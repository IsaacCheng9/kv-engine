#include "wal.hpp"
#include <cstdio>
#include <filesystem>
#include <gtest/gtest.h>
#include <stdexcept>

namespace kv {

namespace {

TEST(WALTest, ConstructionWithValidPath) {
  const std::string path = "/tmp/kv_wal_test";
  EXPECT_NO_THROW(WAL wal(path));
  EXPECT_TRUE(std::filesystem::exists(path));
  std::remove(path.c_str());
}

TEST(WALTest, ConstructionWithInvalidPath) {
  EXPECT_THROW(WAL wal("/non_existent/dir/file.wal"), std::runtime_error);
}

TEST(WALTest, LogPutDoesNotThrow) {
  const std::string path = "/tmp/kv_wal_put";
  WAL wal(path);
  EXPECT_NO_THROW(wal.log_put("key1", "value1"));
  std::remove(path.c_str());
}

TEST(WALTest, LogRemoveDoesNotThrow) {
  const std::string path = "/tmp/kv_wal_remove";
  WAL wal(path);
  EXPECT_NO_THROW(wal.log_remove("key1"));
  std::remove(path.c_str());
}

TEST(WALTest, FileGrowsAfterWrites) {
  const std::string path = "/tmp/kv_wal_grow";
  WAL wal(path);
  auto size_before = std::filesystem::file_size(path);
  wal.log_put("key1", "value1");
  auto size_after_put = std::filesystem::file_size(path);
  EXPECT_GT(size_after_put, size_before);

  wal.log_remove("key2");
  auto size_after_remove = std::filesystem::file_size(path);
  EXPECT_GT(size_after_remove, size_after_put);

  std::remove(path.c_str());
}

} // namespace
} // namespace kv
