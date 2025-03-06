target_add_test(test_storage_client test-fabric-lib)

target_add_test(storage-client-test
  TestStorageClientInterface.cc
  TestStorageClientVFS.cc
  storage-client
  test-lib
)