#include "memtable.hpp"
#include "sstable_writer.hpp"
#include "test_utils.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>

namespace kv {

namespace {

TEST(SSTableWriterTest, ConstructionWithValidPath) {
  const test::TempDir dir;
  const std::string path = dir.file("sstable");
  EXPECT_NO_THROW(SSTableWriter writer(path));
  EXPECT_TRUE(std::filesystem::exists(path));
}

TEST(SSTableWriterTest, ConstructionWithInvalidPath) {
  EXPECT_THROW(SSTableWriter writer("/non_existent/dir/file.sstable"),
               std::runtime_error);
}

TEST(SSTableWriterTest, WriteMemtableWithoutTombstone) {
  const test::TempDir dir;
  const std::string path = dir.file("sstable_writer_test");
  SSTableWriter writer(path);
  Memtable memtable;
  memtable.put("key1", "value1");
  memtable.put("key2", "value2");

  EXPECT_NO_THROW(writer.write_memtable(memtable));
  auto file_size = std::filesystem::file_size(path);
  EXPECT_GT(file_size, 0);
}

TEST(SSTableWriterTest, WriteMemtableWithTombstone) {
  const test::TempDir dir;
  const std::string path = dir.file("sstable_writer_tombstone_test");
  SSTableWriter writer(path);
  Memtable memtable;
  memtable.put("key1", "value1");
  memtable.remove("key2");

  EXPECT_NO_THROW(writer.write_memtable(memtable));
  auto file_size = std::filesystem::file_size(path);
  EXPECT_GT(file_size, 0);
}
} // namespace
} // namespace kv
