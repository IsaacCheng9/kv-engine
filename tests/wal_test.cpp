#include "memtable.hpp"
#include "test_utils.hpp"
#include "wal.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <stdexcept>
#include <unistd.h>

namespace kv {

namespace {

TEST(WALTest, ConstructionWithValidPath) {
  const test::TempDir dir;
  const std::string path = dir.file("wal_test");
  EXPECT_NO_THROW(WAL wal(path));
  EXPECT_TRUE(std::filesystem::exists(path));
}

TEST(WALTest, ConstructionWithInvalidPath) {
  EXPECT_THROW(WAL wal("/non_existent/dir/file.wal"), std::runtime_error);
}

TEST(WALTest, LogPutDoesNotThrow) {
  const test::TempDir dir;
  const std::string path = dir.file("wal_put");
  WAL wal(path);
  EXPECT_NO_THROW(wal.log_put("key1", "value1"));
}

TEST(WALTest, LogRemoveDoesNotThrow) {
  const test::TempDir dir;
  const std::string path = dir.file("wal_remove");
  WAL wal(path);
  EXPECT_NO_THROW(wal.log_remove("key1"));
}

TEST(WALTest, FileGrowsAfterWrites) {
  const test::TempDir dir;
  const std::string path = dir.file("wal_grow");
  WAL wal(path);
  auto size_before = std::filesystem::file_size(path);
  wal.log_put("key1", "value1");
  auto size_after_put = std::filesystem::file_size(path);
  EXPECT_GT(size_after_put, size_before);

  wal.log_remove("key2");
  auto size_after_remove = std::filesystem::file_size(path);
  EXPECT_GT(size_after_remove, size_after_put);
}

TEST(WALTest, ReplayGeneratesCorrectMemtable) {
  const test::TempDir dir;
  const std::string path = dir.file("wal_replay");
  {
    WAL wal(path);
    wal.log_put("key1", "value1");
    wal.log_put("key2", "value2");
    wal.log_remove("key1");
  }

  Memtable memtable;
  {
    WAL wal(path);
    wal.replay(memtable);
  }

  EXPECT_FALSE(memtable.get("key1").has_value());
  auto value2 = memtable.get("key2");
  ASSERT_TRUE(value2.has_value());
  EXPECT_EQ(value2.value(), "value2");
}

TEST(WALTest, ReplayOnEmptyFile) {
  const test::TempDir dir;
  const std::string path = dir.file("wal_empty_replay");
  { WAL wal(path); }

  Memtable memtable;
  {
    WAL wal(path);
    EXPECT_NO_THROW(wal.replay(memtable));
  }

  EXPECT_TRUE(memtable.get("anykey").has_value() == false);
}

TEST(WALTest, ReplayWithCorruptedRecord) {
  const test::TempDir dir;
  const std::string path = dir.file("wal_corrupted_replay");
  {
    WAL wal(path);
    wal.log_put("key1", "value1");
    wal.log_put("key2", "value2");
    wal.log_remove("key1");
  }

  // Corrupt the file by truncating the last 10 bytes.
  {
    auto file_size = std::filesystem::file_size(path);
    ASSERT_EQ(truncate(path.c_str(), file_size - 10), 0);
  }

  Memtable memtable;
  {
    WAL wal(path);
    EXPECT_NO_THROW(wal.replay(memtable));
  }

  // We should have successfully replayed the first two records, but not the
  // corrupted remove record.
  auto value1 = memtable.get("key1");
  ASSERT_TRUE(value1.has_value());
  EXPECT_EQ(value1.value(), "value1");
  auto value2 = memtable.get("key2");
  ASSERT_TRUE(value2.has_value());
  EXPECT_EQ(value2.value(), "value2");
}

TEST(WALTest, ClearTruncatesFile) {
  const test::TempDir dir;
  const std::string path = dir.file("wal_clear");
  WAL wal(path);
  wal.log_put("key1", "value1");
  EXPECT_GT(std::filesystem::file_size(path), 0);
  wal.clear();
  EXPECT_EQ(std::filesystem::file_size(path), 0);
}
} // namespace
} // namespace kv
