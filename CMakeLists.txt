cmake_minimum_required(VERSION 3.24)
project(kv_engine VERSION 0.2.0 LANGUAGES CXX)

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
    src/log_file.cpp
)
target_include_directories(kv_engine PUBLIC src)

# Compiler warnings (applied to our code only, not dependencies)
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
    tests/log_file_test.cpp
    tests/store_test.cpp
)
target_link_libraries(kv_engine_tests PRIVATE kv_engine GTest::gtest_main)
target_compile_options(kv_engine_tests PRIVATE -Wall -Wextra -Wpedantic -Werror)

include(GoogleTest)
gtest_discover_tests(kv_engine_tests)
