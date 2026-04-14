#include "memtable.hpp"
#include "sstable_writer.hpp"
#include <cstdio>
#include <filesystem>
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>

namespace kv {

namespace {

TEST(SSTableWriterTest, ConstructionWithValidPath) {
  const std::string path = "/tmp/kv_log_file_test";
  std::remove(path.c_str());
  EXPECT_NO_THROW(SSTableWriter writer(path));
  EXPECT_TRUE(std::filesystem::exists(path));
  std::remove(path.c_str());
}

TEST(SSTableWriterTest, ConstructionWithInvalidPath) {
  EXPECT_THROW(SSTableWriter writer("/non_existent/dir/file.sstable"),
               std::runtime_error);
}

TEST(SSTableWriterTest, WriteMemtableWithoutTombstone) {
  const std::string path = "/tmp/kv_sstable_writer_test";
  std::remove(path.c_str()); // Clean up from previous runs in case of crash.
  SSTableWriter writer(path);
  Memtable memtable;
  memtable.put("key1", "value1");
  memtable.put("key2", "value2");

  EXPECT_NO_THROW(writer.write_memtable(memtable));
  auto file_size = std::filesystem::file_size(path);
  EXPECT_GT(file_size, 0);

  std::remove(path.c_str());
}

TEST(SSTableWriterTest, WriteMemtableWithTombstone) {
  const std::string path = "/tmp/kv_sstable_writer_tombstone_test";
  std::remove(path.c_str()); // Clean up from previous runs in case of crash.
  SSTableWriter writer(path);
  Memtable memtable;
  memtable.put("key1", "value1");
  memtable.remove("key2");

  EXPECT_NO_THROW(writer.write_memtable(memtable));
  auto file_size = std::filesystem::file_size(path);
  EXPECT_GT(file_size, 0);

  std::remove(path.c_str());
}
} // namespace
} // namespace kv
