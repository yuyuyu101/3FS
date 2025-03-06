target_add_lib(storage-client storage-fbs common mgmtd-client)

# Add libaio dependency
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBAIO REQUIRED libaio)

# Add libaio and folly dependencies
target_link_libraries(storage-client
  PRIVATE
    ${LIBAIO_LIBRARIES}
    Folly::folly
)
