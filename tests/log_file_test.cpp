#include "log_file.hpp"
#include <cstdio>
#include <filesystem>
#include <gtest/gtest.h>
#include <stdexcept>

namespace kv {

namespace {

TEST(LogFileTest, ConstructionWithValidPath) {
  const std::string path = "/tmp/kv_log_file_test";
  EXPECT_NO_THROW(LogFile log_file(path));
  EXPECT_TRUE(std::filesystem::exists(path));
  std::remove(path.c_str());
}

TEST(LogFileTest, ConstructionWithInvalidPath) {
  EXPECT_THROW(LogFile log_file("/non_existent/dir/file.log"),
               std::runtime_error);
}
} // namespace
} // namespace kv
