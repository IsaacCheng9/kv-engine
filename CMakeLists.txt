cmake_minimum_required(VERSION 3.24)
project(kv_engine VERSION 0.10.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Sanitisers (enable with -DSANITISE=ON)
option(SANITISE "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
if(SANITISE)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()

# Library
add_library(kv_engine
    src/store.cpp
    src/compaction.cpp
    src/engine.cpp
    src/log_file.cpp
    src/memtable.cpp
    src/sstable_reader.cpp
    src/sstable_writer.cpp
    src/wal.cpp
)
target_include_directories(kv_engine PUBLIC src)
target_link_libraries(kv_engine PRIVATE z)

# Compiler warnings (applied to our code only, not dependencies). In release
# mode (used for benchmarks), GCC enables _FORTIFY_SOURCE which marks read()
# with warn_unused_result. We ignore it for now - write_all/read_all helpers
# tracked in the backlog will fix this properly.
target_compile_options(kv_engine PRIVATE -Wall -Wextra -Wpedantic -Werror
                                         -Wno-unused-result)

# GoogleTest
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.16.0
)
FetchContent_MakeAvailable(googletest)

# Tests
enable_testing()
add_executable(kv_engine_tests
    tests/compaction_test.cpp
    tests/engine_concurrency_test.cpp
    tests/engine_test.cpp
    tests/log_file_test.cpp
    tests/memtable_test.cpp
    tests/sstable_reader_test.cpp
    tests/sstable_writer_test.cpp
    tests/store_test.cpp
    tests/wal_test.cpp
)
target_link_libraries(kv_engine_tests PRIVATE kv_engine GTest::gtest_main)
target_compile_options(kv_engine_tests PRIVATE -Wall -Wextra -Wpedantic -Werror)

include(GoogleTest)
gtest_discover_tests(kv_engine_tests)

# Benchmark (enable with -DBENCHMARK=ON)
option(BENCHMARK "Build the benchmark binary" OFF)
if(BENCHMARK)
    add_executable(kv_engine_benchmark benchmarks/kv_engine_benchmark.cpp)
    target_link_libraries(kv_engine_benchmark PRIVATE kv_engine)
    target_compile_options(kv_engine_benchmark PRIVATE -O3 -DNDEBUG)
endif()
