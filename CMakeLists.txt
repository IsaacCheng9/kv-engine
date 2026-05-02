cmake_minimum_required(VERSION 3.24)
project(kv_engine VERSION 0.13.4 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Sanitisers (enable with -DSANITISE=ON)
option(SANITISE "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
if(SANITISE)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()

# Protobuf + gRPC (Homebrew on macOS, libgrpc++-dev on Debian/Ubuntu). Use
# CONFIG mode for both - the legacy FindProtobuf module conflicts with the
# Protobuf targets that gRPC's own config pulls in transitively.
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# Run protoc + grpc_cpp_plugin on each .proto file, emitting generated code
# into ${KV_PROTO_GEN_DIR}/<package-path>/<name>.{pb,grpc.pb}.{h,cc}.
set(KV_PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto_gen)
file(MAKE_DIRECTORY ${KV_PROTO_GEN_DIR})

set(KV_PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/proto/kv/v1/kv.proto)
set(KV_PROTO_SRCS
    ${KV_PROTO_GEN_DIR}/kv/v1/kv.pb.cc
    ${KV_PROTO_GEN_DIR}/kv/v1/kv.grpc.pb.cc
)
set(KV_PROTO_HDRS
    ${KV_PROTO_GEN_DIR}/kv/v1/kv.pb.h
    ${KV_PROTO_GEN_DIR}/kv/v1/kv.grpc.pb.h
)

add_custom_command(
    OUTPUT ${KV_PROTO_SRCS} ${KV_PROTO_HDRS}
    COMMAND $<TARGET_FILE:protobuf::protoc>
    ARGS --grpc_out=${KV_PROTO_GEN_DIR}
         --cpp_out=${KV_PROTO_GEN_DIR}
         -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
         --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
         ${KV_PROTO_FILE}
    DEPENDS ${KV_PROTO_FILE}
    COMMENT "Generating protobuf + gRPC C++ code from kv.proto"
)

# Library wrapping the generated code. Kept separate from kv_engine so the
# engine itself doesn't need to know about gRPC, and so the generated code's
# warnings don't fight the project's -Werror policy.
add_library(kv_proto ${KV_PROTO_SRCS})
target_include_directories(kv_proto PUBLIC ${KV_PROTO_GEN_DIR})
target_link_libraries(kv_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)
# Generated protobuf/gRPC code triggers warnings we don't control. Suppress
# them locally rather than weakening the project-wide warning flags.
target_compile_options(kv_proto PRIVATE -w)

# Library
add_library(kv_engine
    src/compaction.cpp
    src/engine.cpp
    src/io_utils.cpp
    src/log_file.cpp
    src/memtable.cpp
    src/sstable_reader.cpp
    src/sstable_writer.cpp
    src/wal.cpp
    src/bloom_filter.cpp
)
target_include_directories(kv_engine PUBLIC src)
target_link_libraries(kv_engine PRIVATE z)

target_compile_options(kv_engine PRIVATE -Wall -Wextra -Wpedantic -Werror)

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
    tests/bloom_filter_test.cpp
    tests/compaction_test.cpp
    tests/engine_concurrency_test.cpp
    tests/engine_integration_test.cpp
    tests/engine_test.cpp
    tests/io_utils_test.cpp
    tests/log_file_test.cpp
    tests/memtable_test.cpp
    tests/sstable_reader_test.cpp
    tests/sstable_writer_test.cpp
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
