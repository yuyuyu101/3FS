include(${PROJECT_SOURCE_DIR}/cmake/GitVersion.cmake)
CheckGitSetup(${PROJECT_SOURCE_DIR})

set(FLATBUFFERS_FLATC_SCHEMA_EXTRA_ARGS --cpp-std=c++17)

target_add_lib(common memory-common version-info folly ibverbs scn::scn clickhouse-cpp-lib-static toml11 libzstd_static 3fs_liburing)
add_dependencies(common MonitorCollectorService-fbs)
target_sources(common PRIVATE utils/Linenoise.c)
target_compile_options(common PRIVATE -Wno-error=deprecated-declarations)

target_add_shared_lib(hf3fs_common_shared memory-common version-info folly ibverbs scn::scn clickhouse-cpp-lib-static toml11 libzstd_static 3fs_liburing)
target_compile_options(hf3fs_common_shared PRIVATE -Wno-error=deprecated-declarations)
add_dependencies(hf3fs_common_shared MonitorCollectorService-fbs)
