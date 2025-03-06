#pragma once

#include <filesystem>
#include <fstream>
#include <mutex>
#include <span>
#include <unordered_map>
#include <vector>
#include <libaio.h>
#include <atomic>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <folly/experimental/coro/Task.h>
#include <folly/experimental/coro/FutureUtil.h>

#include "StorageClient.h"
#include "common/utils/Address.h"
#include "common/utils/Coroutine.h"
#include "common/utils/Result.h"
#include "fbs/storage/Common.h"

namespace hf3fs::storage::client {

/**
 * StorageClientVFS - A StorageClient implementation that uses local filesystem VFS API
 * 
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * !!! WARNING: THIS IS A TEST-ONLY IMPLEMENTATION. NOT FOR PRODUCTION USE !!!
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * 
 * This class implements the StorageClient interface using the local filesystem with AIO and DIO.
 * It's designed for testing storage client functionality without requiring a real storage service.
 * Each chain is represented as a directory, and each chunk as a file within that directory.
 * Metadata for chunks (version, capacity) is stored in separate metadata files.
 * It uses a dedicated thread pool for filesystem operations to avoid blocking the coroutine thread.
 * 
 * Key limitations:
 * - No data replication or fault tolerance
 * - Limited to local filesystem performance
 * - No distributed consistency guarantees
 * - Data may be lost on system crashes
 * 
 * Use cases:
 * - Unit and integration testing
 * - Development and debugging
 * - Demonstrations and examples
 */
class StorageClientVFS : public StorageClient {
 public:
  StorageClientVFS(ClientId clientId, const Config &config, hf3fs::client::ICommonMgmtdClient &mgmtdClient);
  ~StorageClientVFS() override;

  Result<Void> start() override;
  void stop() override;

  hf3fs::client::ICommonMgmtdClient &getMgmtdClient() override { return mgmtdClient_; }

  CoTryTask<void> batchRead(std::span<ReadIO> readIOs,
                            const flat::UserInfo &userInfo,
                            const ReadOptions &options = ReadOptions(),
                            std::vector<ReadIO *> *failedIOs = nullptr) override;

  CoTryTask<void> batchWrite(std::span<WriteIO> writeIOs,
                             const flat::UserInfo &userInfo,
                             const WriteOptions &options = WriteOptions(),
                             std::vector<WriteIO *> *failedIOs = nullptr) override;

  CoTryTask<void> read(ReadIO &readIO,
                       const flat::UserInfo &userInfo,
                       const ReadOptions &options = ReadOptions()) override;

  CoTryTask<void> write(WriteIO &writeIO,
                        const flat::UserInfo &userInfo,
                        const WriteOptions &options = WriteOptions()) override;

  CoTryTask<void> queryLastChunk(std::span<QueryLastChunkOp> ops,
                                 const flat::UserInfo &userInfo,
                                 const ReadOptions &options = ReadOptions(),
                                 std::vector<QueryLastChunkOp *> *failedOps = nullptr) override;

  CoTryTask<void> removeChunks(std::span<RemoveChunksOp> ops,
                               const flat::UserInfo &userInfo,
                               const WriteOptions &options = WriteOptions(),
                               std::vector<RemoveChunksOp *> *failedOps = nullptr) override;

  CoTryTask<void> truncateChunks(std::span<TruncateChunkOp> ops,
                                 const flat::UserInfo &userInfo,
                                 const WriteOptions &options = WriteOptions(),
                                 std::vector<TruncateChunkOp *> *failedOps = nullptr) override;

  CoTryTask<SpaceInfoRsp> querySpaceInfo(NodeId nodeId) override;

  CoTryTask<CreateTargetRsp> createTarget(NodeId nodeId, const CreateTargetReq &req) override;

  CoTryTask<OfflineTargetRsp> offlineTarget(NodeId nodeId, const OfflineTargetReq &req) override;

  CoTryTask<RemoveTargetRsp> removeTarget(NodeId nodeId, const RemoveTargetReq &req) override;

  CoTryTask<std::vector<Result<QueryChunkRsp>>> queryChunk(const QueryChunkReq &req) override;

  CoTryTask<ChunkMetaVector> getAllChunkMetadata(const ChainId &chainId, const TargetId &targetId) override;

  // For testing: inject error for specific chain
  CoTask<void> injectErrorOnChain(ChainId chainId, Result<Void> error);

 private:
  // Structure to hold chunk metadata
  struct ChunkMetadata {
    uint32_t capacity;
    uint32_t version;
  };

  // Structure to hold chain information including error state
  struct ChainInfo {
    std::mutex mutex;
    Result<Void> error = Void{};
  };

  // AIO context and related constants
  static constexpr size_t kAioMaxEvents = 128;
  static constexpr size_t kAioAlignmentSize = 512;  // Typically 512 bytes for most file systems
  static constexpr int kAioTimeoutMs = 5000;        // 5 seconds timeout for AIO operations
  static constexpr size_t kAioMaxIoDepth = 64;      // Maximum number of in-flight I/O operations
  static constexpr size_t kFsThreadPoolSize = 4;    // Thread pool size for filesystem operations

  // Structure for AIO operation context
  struct AioContext {
    io_context_t ctx;
    std::atomic<size_t> inflight{0};
    std::mutex mutex;
    
    AioContext() : ctx(0) {}
  };

  // Base directory for all storage
  std::filesystem::path baseDir_;

  // Get the directory path for a chain
  std::filesystem::path getChainDir(const ChainId &chainId) const;

  // Get the file path for a chunk
  std::filesystem::path getChunkPath(const ChainId &chainId, const ChunkId &chunkId) const;

  // Get the metadata file path for a chunk
  std::filesystem::path getChunkMetadataPath(const ChainId &chainId, const ChunkId &chunkId) const;

  // Read chunk metadata, returns error if the chunk doesn't exist
  Result<ChunkMetadata> readChunkMetadata(const ChainId &chainId, const ChunkId &chunkId) const;

  // Write chunk metadata
  Result<Void> writeChunkMetadata(const ChainId &chainId, const ChunkId &chunkId, const ChunkMetadata &metadata);

  // Create a new chunk with initial metadata
  Result<Void> createChunk(const ChainId &chainId, const ChunkId &chunkId, uint32_t capacity);

  // Get a chain info object and lock it
  std::pair<ChainInfo&, std::unique_lock<std::mutex>> getChainInfo(const ChainId &chainId);

  // Process query to find chunks in a range
  CoTryTask<std::vector<std::pair<ChunkId, ChunkMetadata>>> doQuery(
      const ChainId &chainId, const ChunkIdRange &range);

  // Process the results of a query with a callback function
  CoTryTask<uint32_t> processQueryResults(
      const ChainId &chainId,
      const ChunkIdRange &range,
      std::function<CoTryTask<void>(const ChunkId &, const ChunkMetadata &, const std::filesystem::path &)> processor,
      bool &moreChunksInRange);

  // List chunks in a directory matching a pattern
  std::vector<std::pair<ChunkId, std::filesystem::path>> listChunksInRange(
      const std::filesystem::path &chainDir, const ChunkIdRange &range, size_t limit) const;
    
  // AIO methods
  Result<Void> initAioContext();
  void destroyAioContext();
  
  // AIO operations
  CoTryTask<IOResult> aioRead(int fd, void* buf, size_t count, off_t offset, ChunkVer version);
  CoTryTask<IOResult> aioWrite(int fd, const void* buf, size_t count, off_t offset, ChunkVer version);
  CoTryTask<size_t> aioGetFileSize(int fd);
  
  // Align buffer address and size for direct I/O
  static void* alignBuffer(void* buf);
  static size_t alignSize(size_t size);
  
  // Create or open file with O_DIRECT flag
  Result<int> openFileForDirectIO(const std::filesystem::path& path, bool forRead, bool create = false);

  hf3fs::client::ICommonMgmtdClient &mgmtdClient_;
  std::unordered_map<ChainId, ChainInfo> chainInfos_;
  std::mutex chainInfosMutex_;
  
  // AIO context
  AioContext aioContext_;
  bool aioInitialized_{false};

  // Asynchronous filesystem operations
  folly::coro::Task<Result<ChunkMetadata>> readChunkMetadataAsync(
      const ChainId &chainId, const ChunkId &chunkId) const;
  
  folly::coro::Task<Result<Void>> writeChunkMetadataAsync(
      const ChainId &chainId, const ChunkId &chunkId, const ChunkMetadata &metadata);
  
  folly::coro::Task<Result<Void>> createChunkAsync(
      const ChainId &chainId, const ChunkId &chunkId, uint32_t capacity);
  
  folly::coro::Task<std::vector<std::pair<ChunkId, std::filesystem::path>>> listChunksInRangeAsync(
      const std::filesystem::path &chainDir, const ChunkIdRange &range, size_t limit) const;

  // Synchronous implementations of filesystem operations (executed in thread pool)
  Result<ChunkMetadata> readChunkMetadataSync(
      const ChainId &chainId, const ChunkId &chunkId) const;
  
  Result<Void> writeChunkMetadataSync(
      const ChainId &chainId, const ChunkId &chunkId, const ChunkMetadata &metadata);
  
  Result<Void> createChunkSync(
      const ChainId &chainId, const ChunkId &chunkId, uint32_t capacity);
  
  std::vector<std::pair<ChunkId, std::filesystem::path>> listChunksInRangeSync(
      const std::filesystem::path &chainDir, const ChunkIdRange &range, size_t limit) const;

  // Filesystem operations thread pool
  std::unique_ptr<folly::IOThreadPoolExecutor> fsThreadPool_;
};

}  // namespace hf3fs::storage::client 