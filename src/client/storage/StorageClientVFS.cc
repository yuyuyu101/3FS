#include "StorageClientVFS.h"

#include <algorithm>
#include <boost/core/ignore_unused.hpp>
#include <cassert>
#include <fcntl.h>
#include <folly/Random.h>
#include <folly/logging/xlog.h>
#include <random>
#include <sstream>
#include <string>
#include <system_error>
#include <sys/stat.h>
#include <unistd.h>

#include "common/utils/ExponentialBackoffRetry.h"
#include "common/utils/FaultInjection.h"

#define CHECK_CHAIN_ERROR(chainInfo)                                                           \
  if (chainInfo.error.hasError()) {                                                           \
    XLOGF(WARN, "Inject error {} on chain {}", chainInfo.error.error(), chainId);             \
    co_return makeError(chainInfo.error.error().code(), "fault injection");                    \
  }

namespace hf3fs::storage::client {

namespace fs = std::filesystem;

template <typename T>
static std::vector<T *> randomShuffle(std::span<T> span) {
  std::vector<T *> vec;
  vec.reserve(span.size());
  for (auto &t : span) {
    vec.push_back(&t);
  }

  std::shuffle(vec.begin(), vec.end(), std::mt19937(std::random_device()()));
  return vec;
}

static std::string chunkIdToString(const ChunkId &chunkId) {
  std::stringstream ss;
  for (const auto &byte : chunkId.data()) {
    ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(byte);
  }
  return ss.str();
}

static ChunkId stringToChunkId(const std::string &str) {
  ChunkId chunkId;
  std::vector<uint8_t> data;
  
  for (size_t i = 0; i < str.length(); i += 2) {
    std::string byteString = str.substr(i, 2);
    uint8_t byte = static_cast<uint8_t>(std::stoi(byteString, nullptr, 16));
    data.push_back(byte);
  }
  
  chunkId.set_data(data);
  return chunkId;
}

StorageClientVFS::StorageClientVFS(ClientId clientId, 
                                   const Config &config, 
                                   hf3fs::client::ICommonMgmtdClient &mgmtdClient)
    : StorageClient(clientId, config),
      mgmtdClient_(mgmtdClient),
      fsThreadPool_(std::make_unique<folly::IOThreadPoolExecutor>(kFsThreadPoolSize)) {

  XLOGF(WARN, "========================================================================");
  XLOGF(WARN, "WARNING: StorageClientVFS is being initialized. This is a TEST-ONLY backend!");
  XLOGF(WARN, "         It is NOT recommended for production use.");
  XLOGF(WARN, "         Data will be stored in local filesystem and may be lost.");
  XLOGF(WARN, "========================================================================");
  
  // Create a temporary directory for storing chunks
  baseDir_ = fs::temp_directory_path() / "3fs_vfs_storage";
  
  try {
    fs::create_directories(baseDir_);
  } catch (const std::exception &e) {
    XLOGF(CRITICAL, "Failed to create base directory {}: {}", baseDir_.string(), e.what());
  }
  
  XLOGF(INFO, "StorageClientVFS initialized with base directory: {}", baseDir_.string());
}

StorageClientVFS::~StorageClientVFS() {
  destroyAioContext();
  fsThreadPool_->join();
  
  try {
    // Clean up the base directory when the client is destroyed
    // Uncomment this if you want to remove all data on destruction
    // fs::remove_all(baseDir_);
  } catch (const std::exception &e) {
    XLOGF(WARN, "Failed to clean up base directory {}: {}", baseDir_.string(), e.what());
  }
}

Result<Void> StorageClientVFS::start() {
  return initAioContext();
}

void StorageClientVFS::stop() {
  destroyAioContext();
}

fs::path StorageClientVFS::getChainDir(const ChainId &chainId) const {
  return baseDir_ / std::to_string(chainId);
}

fs::path StorageClientVFS::getChunkPath(const ChainId &chainId, const ChunkId &chunkId) const {
  return getChainDir(chainId) / (chunkIdToString(chunkId) + ".data");
}

fs::path StorageClientVFS::getChunkMetadataPath(const ChainId &chainId, const ChunkId &chunkId) const {
  return getChainDir(chainId) / (chunkIdToString(chunkId) + ".meta");
}

// 同步实现（在线程池中执行）
Result<StorageClientVFS::ChunkMetadata> StorageClientVFS::readChunkMetadataSync(
    const ChainId &chainId, const ChunkId &chunkId) const {
  auto metaPath = getChunkMetadataPath(chainId, chunkId);
  
  std::ifstream metaFile(metaPath, std::ios::binary);
  if (!metaFile) {
    return makeError(StorageClientCode::kChunkNotFound, 
                    "Metadata file not found: " + metaPath.string());
  }
  
  ChunkMetadata metadata;
  if (!metaFile.read(reinterpret_cast<char*>(&metadata), sizeof(metadata))) {
    return makeError(StorageClientCode::kIOError, 
                    "Failed to read metadata from " + metaPath.string());
  }
  
  return metadata;
}

// 异步包装
folly::coro::Task<Result<StorageClientVFS::ChunkMetadata>> StorageClientVFS::readChunkMetadataAsync(
    const ChainId &chainId, const ChunkId &chunkId) const {
  auto future = fsThreadPool_->addFuture([this, chainId, chunkId]() {
    return readChunkMetadataSync(chainId, chunkId);
  });
  co_return co_await folly::coro::toTask(std::move(future));
}

Result<Void> StorageClientVFS::writeChunkMetadataSync(
    const ChainId &chainId, const ChunkId &chunkId, const ChunkMetadata &metadata) {
  auto chainDir = getChainDir(chainId);
  if (!fs::exists(chainDir)) {
    try {
      fs::create_directories(chainDir);
    } catch (const std::exception &e) {
      return makeError(StorageClientCode::kIOError, 
                      "Failed to create chain directory: " + std::string(e.what()));
    }
  }
  
  auto metaPath = getChunkMetadataPath(chainId, chunkId);
  
  std::ofstream metaFile(metaPath, std::ios::binary | std::ios::trunc);
  if (!metaFile) {
    return makeError(StorageClientCode::kIOError, 
                    "Failed to open metadata file for writing: " + metaPath.string());
  }
  
  if (!metaFile.write(reinterpret_cast<const char*>(&metadata), sizeof(metadata))) {
    return makeError(StorageClientCode::kIOError, 
                    "Failed to write metadata to " + metaPath.string());
  }
  
  return Void{};
}

folly::coro::Task<Result<Void>> StorageClientVFS::writeChunkMetadataAsync(
    const ChainId &chainId, const ChunkId &chunkId, const ChunkMetadata &metadata) {
  auto future = fsThreadPool_->addFuture([this, chainId, chunkId, metadata]() {
    return writeChunkMetadataSync(chainId, chunkId, metadata);
  });
  co_return co_await folly::coro::toTask(std::move(future));
}

Result<Void> StorageClientVFS::createChunkSync(
    const ChainId &chainId, const ChunkId &chunkId, uint32_t capacity) {
  auto chainDir = getChainDir(chainId);
  if (!fs::exists(chainDir)) {
    try {
      fs::create_directories(chainDir);
    } catch (const std::exception &e) {
      return makeError(StorageClientCode::kIOError, 
                      "Failed to create chain directory: " + std::string(e.what()));
    }
  }
  
  auto chunkPath = getChunkPath(chainId, chunkId);
  
  // Create an empty file for the chunk
  std::ofstream chunkFile(chunkPath, std::ios::binary | std::ios::trunc);
  if (!chunkFile) {
    return makeError(StorageClientCode::kIOError, 
                    "Failed to create chunk file: " + chunkPath.string());
  }
  
  // Write initial metadata
  ChunkMetadata metadata{capacity, 0};
  return writeChunkMetadataSync(chainId, chunkId, metadata);
}

folly::coro::Task<Result<Void>> StorageClientVFS::createChunkAsync(
    const ChainId &chainId, const ChunkId &chunkId, uint32_t capacity) {
  auto future = fsThreadPool_->addFuture([this, chainId, chunkId, capacity]() {
    return createChunkSync(chainId, chunkId, capacity);
  });
  co_return co_await folly::coro::toTask(std::move(future));
}

std::vector<std::pair<ChunkId, fs::path>> StorageClientVFS::listChunksInRangeSync(
    const fs::path &chainDir, const ChunkIdRange &range, size_t limit) const {
  std::vector<std::pair<ChunkId, fs::path>> result;
  
  if (!fs::exists(chainDir) || !fs::is_directory(chainDir)) {
    return result;
  }
  
  // Collect all chunk data files in the directory
  for (const auto &entry : fs::directory_iterator(chainDir)) {
    if (!entry.is_regular_file() || entry.path().extension() != ".data") {
      continue;
    }
    
    // Extract chunk ID from filename
    std::string filename = entry.path().stem().string();
    ChunkId chunkId = stringToChunkId(filename);
    
    // Check if chunk is in range
    if (chunkId >= range.begin && chunkId < range.end) {
      result.emplace_back(chunkId, entry.path());
      
      if (result.size() >= limit) {
        break;
      }
    }
  }
  
  // Sort by chunk ID in descending order
  std::sort(result.begin(), result.end(), 
            [](const auto &a, const auto &b) { return a.first > b.first; });
  
  return result;
}

folly::coro::Task<std::vector<std::pair<ChunkId, fs::path>>> StorageClientVFS::listChunksInRangeAsync(
    const fs::path &chainDir, const ChunkIdRange &range, size_t limit) const {
  auto future = fsThreadPool_->addFuture([this, chainDir, range, limit]() {
    return listChunksInRangeSync(chainDir, range, limit);
  });
  co_return co_await folly::coro::toTask(std::move(future));
}

CoTryTask<std::vector<std::pair<ChunkId, StorageClientVFS::ChunkMetadata>>> 
StorageClientVFS::doQuery(const ChainId &chainId, const ChunkIdRange &range) {
  auto [chainInfo, chainLock] = getChainInfo(chainId);
  CHECK_CHAIN_ERROR(chainInfo);
  
  std::vector<std::pair<ChunkId, ChunkMetadata>> queryResult;
  auto chainDir = getChainDir(chainId);
  
  // List chunks in the given range
  auto chunks = listChunksInRange(chainDir, range, range.maxNumChunkIdsToProcess);
  
  // Read metadata for each chunk
  for (const auto &[chunkId, chunkPath] : chunks) {
    auto metadataResult = readChunkMetadata(chainId, chunkId);
    if (metadataResult.hasError()) {
      continue;  // Skip if metadata can't be read
    }
    
    queryResult.emplace_back(chunkId, *metadataResult);
  }
  
  co_return queryResult;
}

CoTryTask<uint32_t> StorageClientVFS::processQueryResults(
    const ChainId &chainId,
    const ChunkIdRange &range,
    std::function<CoTryTask<void>(const ChunkId &, const ChunkMetadata &, const fs::path &)> processor,
    bool &moreChunksInRange) {
  if (FAULT_INJECTION()) {
    XLOGF(WARN, "Inject fault on processQueryResults");
    co_return makeError(StorageClientCode::kCommError, "Inject fault");
  }

  const uint32_t numChunksToProcess =
      range.maxNumChunkIdsToProcess ? std::min(range.maxNumChunkIdsToProcess, UINT32_MAX - 1) : (UINT32_MAX - 1);
  const uint32_t maxNumResultsPerQuery = 3U;
  ChunkIdRange currentRange = {range.begin, range.end, 0};
  uint32_t numQueryResults = 0;
  Status status(StatusCode::kOK);

  moreChunksInRange = true;

  while (true) {
    currentRange.maxNumChunkIdsToProcess = std::min(numChunksToProcess - numQueryResults + 1, maxNumResultsPerQuery);

    auto queryResult = co_await doQuery(chainId, currentRange);

    if (UNLIKELY(queryResult.hasError())) {
      status = queryResult.error();
      goto exit;
    }

    for (const auto &[chunkId, metadata] : *queryResult) {
      if (numQueryResults < numChunksToProcess) {
        fs::path chunkPath = getChunkPath(chainId, chunkId);
        auto result = co_await processor(chunkId, metadata, chunkPath);

        if (UNLIKELY(result.hasError())) {
          status = result.error();
          goto exit;
        }
      }

      numQueryResults++;

      if (numQueryResults >= numChunksToProcess + 1) {
        goto exit;
      }
    }

    if (queryResult->size() < currentRange.maxNumChunkIdsToProcess) {
      goto exit;
    } else {
      // there could be more chunks in the range, update range for next query
      const auto &[chunkId, _] = *(queryResult->crbegin());
      currentRange.end = chunkId;
    }
  }

exit:
  if (status.code() != StatusCode::kOK) {
    XLOGF(ERR,
          "Failed to process chunk metadata in range: {}, error {}, {} chunks processed before failure",
          range,
          status,
          numQueryResults);
    co_return makeError(status);
  }

  moreChunksInRange = numQueryResults > numChunksToProcess;

  XLOGF(DBG3, "Processed metadata of {} chunks in range: {}", numQueryResults, range);
  co_return numQueryResults;
}

CoTryTask<void> StorageClientVFS::batchRead(
    std::span<ReadIO> readIOs,
    const flat::UserInfo &userInfo,
    const ReadOptions &options,
    std::vector<ReadIO *> *failedIOs) {
  boost::ignore_unused(userInfo, options);

  XLOGF(DBG, "reads {}", readIOs.size());

  for (auto &readIO : readIOs) {
    XLOGF(DBG, "to read from chunk off {} bytes {}", readIO.offset, readIO.length);

    auto [chainInfo, chainLock] = getChainInfo(readIO.routingTarget.chainId);
    CHECK_CHAIN_ERROR(chainInfo);

    // Get the chunk metadata
    auto metadataResult = readChunkMetadata(readIO.routingTarget.chainId, readIO.chunkId);
    if (metadataResult.hasError()) {
      readIO.result = StorageClientCode::kChunkNotFound;
      if (failedIOs) failedIOs->push_back(&readIO);
      XLOGF(DBG, "chunk not found");
      continue;
    }

    auto chunkPath = getChunkPath(readIO.routingTarget.chainId, readIO.chunkId);
    
    // Open the chunk file
    std::ifstream chunkFile(chunkPath, std::ios::binary);
    if (!chunkFile) {
      readIO.result = StorageClientCode::kIOError;
      if (failedIOs) failedIOs->push_back(&readIO);
      XLOGF(DBG, "failed to open chunk file");
      continue;
    }
    
    // Get file size
    chunkFile.seekg(0, std::ios::end);
    auto fileSize = chunkFile.tellg();
    
    // Calculate how much to read
    auto readSize = fileSize < readIO.offset 
                      ? 0 
                      : std::min(readIO.length, static_cast<uint32_t>(fileSize - readIO.offset));
    
    if (readSize > 0) {
      // Position to the requested offset
      chunkFile.seekg(readIO.offset, std::ios::beg);
      
      // Read the data
      if (!chunkFile.read(reinterpret_cast<char*>(readIO.data), readSize)) {
        readIO.result = StorageClientCode::kIOError;
        if (failedIOs) failedIOs->push_back(&readIO);
        XLOGF(DBG, "failed to read from chunk file");
        continue;
      }
    }
    
    auto metadata = *metadataResult;
    readIO.result = IOResult{readSize, ChunkVer(metadata.version), ChunkVer(metadata.version)};
    XLOGF(DBG, "read size {}", readSize);
  }

  co_return Void{};
}

CoTryTask<void> StorageClientVFS::batchWrite(
    std::span<WriteIO> writeIOs,
    const flat::UserInfo &userInfo,
    const WriteOptions &options,
    std::vector<WriteIO *> *failedIOs) {
  boost::ignore_unused(userInfo, options);

  for (auto writeIO : randomShuffle(writeIOs)) {
    co_await write(*writeIO, userInfo, options);
    if (failedIOs != nullptr && !writeIO->result.lengthInfo) {
      failedIOs->push_back(writeIO);
    }
  }

  co_return Void{};
}

CoTryTask<void> StorageClientVFS::read(
    ReadIO &readIO, 
    const flat::UserInfo &userInfo, 
    const ReadOptions &options) {
  boost::ignore_unused(userInfo, options);

  auto [chainInfo, chainLock] = getChainInfo(readIO.routingTarget.chainId);
  CHECK_CHAIN_ERROR(chainInfo);

  // Get the chunk metadata
  auto metadataResult = readChunkMetadata(readIO.routingTarget.chainId, readIO.chunkId);
  if (metadataResult.hasError()) {
    readIO.result = StorageClientCode::kChunkNotFound;
    XLOGF(DBG, "chunk not found");
    co_return Void{};
  }

  auto chunkPath = getChunkPath(readIO.routingTarget.chainId, readIO.chunkId);
  
  // Open file for direct I/O
  auto fdResult = openFileForDirectIO(chunkPath, true, false);
  if (fdResult.hasError()) {
    readIO.result = fdResult.error();
    co_return Void{};
  }
  int fd = *fdResult;
  
  // Get file size
  auto fileSizeResult = co_await aioGetFileSize(fd);
  if (fileSizeResult.hasError()) {
    close(fd);
    readIO.result = fileSizeResult.error();
    co_return Void{};
  }
  size_t fileSize = *fileSizeResult;

  // Calculate read size
  auto readSize = fileSize < readIO.offset 
                    ? 0 
                    : std::min(readIO.length, static_cast<uint32_t>(fileSize - readIO.offset));

  if (readSize > 0) {
    // Align buffer and size for direct I/O
    void* alignedBuf = alignBuffer(readIO.data);
    size_t alignedSize = alignSize(readSize);
    
    // Perform AIO read
    auto readResult = co_await aioRead(fd, alignedBuf, alignedSize, readIO.offset, 
                                     ChunkVer(metadataResult->version));
    close(fd);
    
    if (readResult.hasError()) {
      readIO.result = readResult.error();
      co_return Void{};
    }
    
    readIO.result = *readResult;
  } else {
    close(fd);
    readIO.result = IOResult{0, ChunkVer(metadataResult->version), ChunkVer(metadataResult->version)};
  }

  co_return Void{};
}

CoTryTask<void> StorageClientVFS::write(
    WriteIO &writeIO,
    const flat::UserInfo &userInfo,
    const WriteOptions &options) {
  boost::ignore_unused(userInfo, options);

  if (writeIO.offset + writeIO.length > writeIO.chunkSize) {
    writeIO.result = StorageClientCode::kInvalidArg;
    co_return Void{};
  }

  auto [chainInfo, chainLock] = getChainInfo(writeIO.routingTarget.chainId);
  CHECK_CHAIN_ERROR(chainInfo);

  const auto &chainId = writeIO.routingTarget.chainId;
  const auto &chunkId = writeIO.chunkId;
  
  // Check if chunk exists - using async version
  auto metadataResult = co_await readChunkMetadataAsync(chainId, chunkId);
  bool newChunk = metadataResult.hasError();
  ChunkMetadata metadata;
  
  if (newChunk) {
    // Create a new chunk - using async version
    auto createResult = co_await createChunkAsync(chainId, chunkId, writeIO.chunkSize);
    if (createResult.hasError()) {
      writeIO.result = createResult.error();
      co_return Void{};
    }
    
    metadata.capacity = writeIO.chunkSize;
    metadata.version = 0;
  } else {
    metadata = *metadataResult;
    
    // Verify chunk size
    if (metadata.capacity != writeIO.chunkSize) {
      writeIO.result = StorageClientCode::kInvalidArg;
      co_return Void{};
    }
  }
  
  auto chunkPath = getChunkPath(chainId, chunkId);
  
  // Open file for direct I/O
  auto fdResult = openFileForDirectIO(chunkPath, false, true);
  if (fdResult.hasError()) {
    writeIO.result = fdResult.error();
    co_return Void{};
  }
  int fd = *fdResult;

  // Align buffer and size for direct I/O
  void* alignedBuf = alignBuffer(writeIO.data);
  size_t alignedSize = alignSize(writeIO.length);
  
  // Perform AIO write
  auto writeResult = co_await aioWrite(fd, alignedBuf, alignedSize, writeIO.offset, 
                                     ChunkVer(metadata.version + 1));
  close(fd);
  
  if (writeResult.hasError()) {
    writeIO.result = writeResult.error();
    co_return Void{};
  }
  
  // Update chunk metadata - using async version
  metadata.version++;
  auto updateResult = co_await writeChunkMetadataAsync(chainId, chunkId, metadata);
  if (updateResult.hasError()) {
    writeIO.result = updateResult.error();
    co_return Void{};
  }
  
  writeIO.result = *writeResult;
  
  co_return Void{};
}

CoTryTask<void> StorageClientVFS::queryLastChunk(
    std::span<QueryLastChunkOp> ops,
    const flat::UserInfo &userInfo,
    const ReadOptions &options,
    std::vector<QueryLastChunkOp *> *failedOps) {
  boost::ignore_unused(userInfo, options);

  FAULT_INJECTION_SET_FACTOR(ops.size());
  for (auto op : randomShuffle(ops)) {
    QueryLastChunkResult queryResult{
        Void{},
        ChunkId(), /*lastChunkId*/
        0 /*lastChunkLen*/,
        0 /*totalChunkLen*/,
        0 /*totalNumChunks*/,
        false /*moreChunksInRange*/,
    };

    auto processChunkData = [&queryResult](const ChunkId &chunkId, 
                                          const ChunkMetadata &metadata,
                                          const fs::path &chunkPath) -> CoTryTask<void> {
      // Get file size
      std::error_code ec;
      auto fileSize = fs::file_size(chunkPath, ec);
      if (ec) {
        fileSize = 0;
      }
      
      if (queryResult.lastChunkId.data().empty() || queryResult.lastChunkId < chunkId) {
        queryResult.lastChunkId = chunkId;
        queryResult.lastChunkLen = fileSize;
      }

      queryResult.totalChunkLen += fileSize;
      queryResult.totalNumChunks++;
      co_return Void{};
    };

    auto processResult = co_await processQueryResults(op->routingTarget.chainId,
                                                      op->chunkRange(),
                                                      processChunkData,
                                                      queryResult.moreChunksInRange);

    if (UNLIKELY(processResult.hasError())) {
      queryResult.statusCode = makeError(processResult.error());
    }

    op->result = queryResult;
  }

  co_return Void{};
}

CoTryTask<void> StorageClientVFS::removeChunks(
    std::span<RemoveChunksOp> ops,
    const flat::UserInfo &userInfo,
    const WriteOptions &options,
    std::vector<RemoveChunksOp *> *failedOps) {
  boost::ignore_unused(userInfo, options, failedOps);

  FAULT_INJECTION_SET_FACTOR(ops.size());
  for (auto op : randomShuffle(ops)) {
    RemoveChunksResult removeRes{Void{}, 0 /*numChunksRemoved*/, false /*moreChunksInRange*/};

    auto processChunkData = [op, &removeRes, this](const ChunkId &chunkId,
                                                  const ChunkMetadata &metadata,
                                                  const fs::path &chunkPath) -> CoTryTask<void> {
      auto [chainInfo, chainLock] = getChainInfo(op->routingTarget.chainId);
      CHECK_CHAIN_ERROR(chainInfo);
      
      auto metaPath = getChunkMetadataPath(op->routingTarget.chainId, chunkId);
      
      // Remove both chunk file and metadata file
      std::error_code ec;
      bool chunkRemoved = fs::remove(chunkPath, ec);
      bool metaRemoved = fs::remove(metaPath, ec);
      
      if (chunkRemoved || metaRemoved) {
        removeRes.numChunksRemoved++;
      }
      
      co_return Void{};
    };

    auto processResult = co_await processQueryResults(op->routingTarget.chainId,
                                                      op->chunkRange(),
                                                      processChunkData,
                                                      removeRes.moreChunksInRange);

    if (UNLIKELY(processResult.hasError())) {
      removeRes.statusCode = makeError(processResult.error());
    }

    op->result = removeRes;
  }

  co_return Void{};
}

CoTryTask<void> StorageClientVFS::truncateChunks(
    std::span<TruncateChunkOp> ops,
    const flat::UserInfo &userInfo,
    const WriteOptions &options,
    std::vector<TruncateChunkOp *> *failedOps) {
  boost::ignore_unused(userInfo, options);

  for (auto op : randomShuffle(ops)) {
    if (op->chunkLen > op->chunkSize) {
      op->result = StorageClientCode::kInvalidArg;
      if (failedOps) failedOps->push_back(op);
      continue;
    }

    auto [chainInfo, chainLock] = getChainInfo(op->routingTarget.chainId);
    CHECK_CHAIN_ERROR(chainInfo);
    
    const auto &chainId = op->routingTarget.chainId;
    const auto &chunkId = op->chunkId;
    
    // Check if chunk exists
    auto metadataResult = readChunkMetadata(chainId, chunkId);
    bool newChunk = metadataResult.hasError();
    ChunkMetadata metadata;
    
    if (newChunk) {
      // Create a new chunk
      auto createResult = createChunk(chainId, chunkId, op->chunkSize);
      if (createResult.hasError()) {
        op->result = createResult.error();
        if (failedOps) failedOps->push_back(op);
        continue;
      }
      
      metadata.capacity = op->chunkSize;
      metadata.version = 0;
    } else {
      metadata = *metadataResult;
      
      // Verify chunk size
      if (metadata.capacity != op->chunkSize) {
        op->result = StorageClientCode::kInvalidArg;
        if (failedOps) failedOps->push_back(op);
        continue;
      }
    }
    
    auto chunkPath = getChunkPath(chainId, chunkId);
    
    // Get current file size
    std::error_code ec;
    auto fileSize = fs::exists(chunkPath, ec) ? fs::file_size(chunkPath, ec) : 0;
    if (ec) {
      fileSize = 0;
    }
    
    size_t newSize = op->onlyExtendChunk && op->chunkLen <= fileSize 
                    ? fileSize 
                    : op->chunkLen;
    
    // Open and truncate the file
    std::fstream chunkFile(chunkPath, std::ios::binary | std::ios::in | std::ios::out | std::ios::trunc);
    if (!chunkFile) {
      op->result = StorageClientCode::kIOError;
      if (failedOps) failedOps->push_back(op);
      continue;
    }
    
    // Resize the file by writing a byte at the desired position
    if (newSize > 0) {
      chunkFile.seekp(newSize - 1);
      chunkFile.put('\0');
    }
    
    // Update chunk metadata
    metadata.version++;
    auto updateResult = writeChunkMetadata(chainId, chunkId, metadata);
    if (updateResult.hasError()) {
      op->result = updateResult.error();
      if (failedOps) failedOps->push_back(op);
      continue;
    }
    
    op->result = IOResult{static_cast<uint32_t>(newSize), 
                          ChunkVer(metadata.version), 
                          ChunkVer(metadata.version)};
  }

  co_return Void{};
}

CoTryTask<SpaceInfoRsp> StorageClientVFS::querySpaceInfo(NodeId node) {
  SpaceInfoRsp rsp;
  // Get information about the filesystem where baseDir_ is located
  try {
    fs::space_info spaceInfo = fs::space(baseDir_);
    
    rsp.spaceInfos.push_back(
        SpaceInfo{baseDir_.string(), 
                  spaceInfo.capacity, 
                  spaceInfo.free, 
                  spaceInfo.available, 
                  std::vector<flat::TargetId>{}, 
                  "vfs"});
  } catch (const std::exception &e) {
    XLOGF(WARN, "Failed to get space info for {}: {}", baseDir_.string(), e.what());
    
    // Provide default values
    rsp.spaceInfos.push_back(
        SpaceInfo{baseDir_.string(), 
                  10ULL << 30,  // 10GB
                  8ULL << 30,   // 8GB free
                  8ULL << 30,   // 8GB available
                  std::vector<flat::TargetId>{}, 
                  "vfs"});
  }
  
  co_return rsp;
}

CoTryTask<CreateTargetRsp> StorageClientVFS::createTarget(NodeId nodeId, const CreateTargetReq &req) {
  co_return CreateTargetRsp{};
}

CoTryTask<OfflineTargetRsp> StorageClientVFS::offlineTarget(NodeId nodeId, const OfflineTargetReq &req) {
  co_return OfflineTargetRsp{};
}

CoTryTask<RemoveTargetRsp> StorageClientVFS::removeTarget(NodeId nodeId, const RemoveTargetReq &req) {
  co_return RemoveTargetRsp{};
}

CoTryTask<std::vector<Result<QueryChunkRsp>>> StorageClientVFS::queryChunk(const QueryChunkReq &req) {
  co_return std::vector<Result<QueryChunkRsp>>{};
}

CoTryTask<ChunkMetaVector> StorageClientVFS::getAllChunkMetadata(
    const ChainId &chainId, const TargetId &targetId) {
  ChunkMetaVector chunkMetaVec;
  auto chainDir = getChainDir(chainId);
  
  if (!fs::exists(chainDir)) {
    co_return chunkMetaVec;
  }
  
  try {
    for (const auto &entry : fs::directory_iterator(chainDir)) {
      if (!entry.is_regular_file() || entry.path().extension() != ".data") {
        continue;
      }
      
      // Extract chunk ID from filename
      std::string filename = entry.path().stem().string();
      ChunkId chunkId = stringToChunkId(filename);
      
      // Read metadata
      auto metadataResult = readChunkMetadata(chainId, chunkId);
      if (metadataResult.hasError()) {
        continue;
      }
      
      auto metadata = *metadataResult;
      
      // Get file size
      std::error_code ec;
      auto fileSize = fs::file_size(entry.path(), ec);
      if (ec) {
        fileSize = 0;
      }
      
      chunkMetaVec.push_back({chunkId,
                              ChunkVer{metadata.version},
                              ChunkVer{metadata.version},
                              ChainVer{},
                              ChunkState::COMMIT,
                              ChecksumInfo{},
                              static_cast<uint32_t>(fileSize)});
    }
  } catch (const std::exception &e) {
    XLOGF(WARN, "Error listing chunks in {}: {}", chainDir.string(), e.what());
  }
  
  co_return chunkMetaVec;
}

CoTask<void> StorageClientVFS::injectErrorOnChain(ChainId chainId, Result<Void> error) {
  auto [chainInfo, chainLock] = getChainInfo(chainId);
  chainInfo.error = error;
  co_return;
}

// AIO implementation methods
Result<Void> StorageClientVFS::initAioContext() {
  if (aioInitialized_) {
    return Void{};
  }

  int ret = io_setup(kAioMaxEvents, &aioContext_.ctx);
  if (ret != 0) {
    return makeError(StorageClientCode::kIOError, 
                    "Failed to initialize AIO context: " + std::string(strerror(-ret)));
  }

  aioInitialized_ = true;
  return Void{};
}

void StorageClientVFS::destroyAioContext() {
  if (!aioInitialized_) {
    return;
  }

  // Wait for all pending I/O operations to complete
  while (aioContext_.inflight.load() > 0) {
    struct io_event events[kAioMaxEvents];
    struct timespec timeout = {0, 100000000}; // 100ms
    
    int ret = io_getevents(aioContext_.ctx, 1, kAioMaxEvents, events, &timeout);
    if (ret > 0) {
      aioContext_.inflight.fetch_sub(ret);
    }
  }

  io_destroy(aioContext_.ctx);
  aioInitialized_ = false;
}

void* StorageClientVFS::alignBuffer(void* buf) {
  uintptr_t addr = reinterpret_cast<uintptr_t>(buf);
  uintptr_t aligned = (addr + kAioAlignmentSize - 1) & ~(kAioAlignmentSize - 1);
  return reinterpret_cast<void*>(aligned);
}

size_t StorageClientVFS::alignSize(size_t size) {
  return (size + kAioAlignmentSize - 1) & ~(kAioAlignmentSize - 1);
}

Result<int> StorageClientVFS::openFileForDirectIO(
    const std::filesystem::path& path, bool forRead, bool create) {
  int flags = O_DIRECT;
  flags |= forRead ? O_RDONLY : O_RDWR;
  if (create) {
    flags |= O_CREAT;
  }

  int fd = open(path.c_str(), flags, 0644);
  if (fd < 0) {
    return makeError(StorageClientCode::kIOError,
                    "Failed to open file for direct I/O: " + std::string(strerror(errno)));
  }

  return fd;
}

CoTryTask<size_t> StorageClientVFS::aioGetFileSize(int fd) {
  struct stat st;
  if (fstat(fd, &st) < 0) {
    co_return makeError(StorageClientCode::kIOError,
                       "Failed to get file size: " + std::string(strerror(errno)));
  }
  co_return st.st_size;
}

CoTryTask<IOResult> StorageClientVFS::aioRead(
    int fd, void* buf, size_t count, off_t offset, ChunkVer version) {
  // Wait if too many I/O operations are in flight
  while (aioContext_.inflight.load() >= kAioMaxIoDepth) {
    struct io_event events[kAioMaxEvents];
    struct timespec timeout = {0, 100000000}; // 100ms
    
    int ret = io_getevents(aioContext_.ctx, 1, kAioMaxEvents, events, &timeout);
    if (ret > 0) {
      aioContext_.inflight.fetch_sub(ret);
    }
  }

  // Prepare AIO control block
  struct iocb cb;
  struct iocb* cbs[1];
  io_prep_pread(&cb, fd, buf, count, offset);
  cbs[0] = &cb;

  // Submit AIO request
  std::unique_lock<std::mutex> lock(aioContext_.mutex);
  int ret = io_submit(aioContext_.ctx, 1, cbs);
  if (ret != 1) {
    co_return makeError(StorageClientCode::kIOError,
                       "Failed to submit AIO read: " + std::string(strerror(-ret)));
  }
  aioContext_.inflight.fetch_add(1);
  lock.unlock();

  // Wait for completion
  struct io_event events[1];
  struct timespec timeout = {kAioTimeoutMs / 1000, (kAioTimeoutMs % 1000) * 1000000};
  
  ret = io_getevents(aioContext_.ctx, 1, 1, events, &timeout);
  aioContext_.inflight.fetch_sub(1);
  
  if (ret != 1) {
    co_return makeError(StorageClientCode::kIOError,
                       "Failed to get AIO read result: " + std::string(strerror(-ret)));
  }

  if (events[0].res < 0) {
    co_return makeError(StorageClientCode::kIOError,
                       "AIO read failed: " + std::string(strerror(-events[0].res)));
  }

  co_return IOResult{static_cast<uint32_t>(events[0].res), version, version};
}

CoTryTask<IOResult> StorageClientVFS::aioWrite(
    int fd, const void* buf, size_t count, off_t offset, ChunkVer version) {
  // Wait if too many I/O operations are in flight
  while (aioContext_.inflight.load() >= kAioMaxIoDepth) {
    struct io_event events[kAioMaxEvents];
    struct timespec timeout = {0, 100000000}; // 100ms
    
    int ret = io_getevents(aioContext_.ctx, 1, kAioMaxEvents, events, &timeout);
    if (ret > 0) {
      aioContext_.inflight.fetch_sub(ret);
    }
  }

  // Prepare AIO control block
  struct iocb cb;
  struct iocb* cbs[1];
  io_prep_pwrite(&cb, fd, const_cast<void*>(buf), count, offset);
  cbs[0] = &cb;

  // Submit AIO request
  std::unique_lock<std::mutex> lock(aioContext_.mutex);
  int ret = io_submit(aioContext_.ctx, 1, cbs);
  if (ret != 1) {
    co_return makeError(StorageClientCode::kIOError,
                       "Failed to submit AIO write: " + std::string(strerror(-ret)));
  }
  aioContext_.inflight.fetch_add(1);
  lock.unlock();

  // Wait for completion
  struct io_event events[1];
  struct timespec timeout = {kAioTimeoutMs / 1000, (kAioTimeoutMs % 1000) * 1000000};
  
  ret = io_getevents(aioContext_.ctx, 1, 1, events, &timeout);
  aioContext_.inflight.fetch_sub(1);
  
  if (ret != 1) {
    co_return makeError(StorageClientCode::kIOError,
                       "Failed to get AIO write result: " + std::string(strerror(-ret)));
  }

  if (events[0].res < 0) {
    co_return makeError(StorageClientCode::kIOError,
                       "AIO write failed: " + std::string(strerror(-events[0].res)));
  }

  co_return IOResult{static_cast<uint32_t>(events[0].res), version, version};
}

}  // namespace hf3fs::storage::client 