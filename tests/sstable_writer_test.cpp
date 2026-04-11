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
  EXPECT_NO_THROW(SSTableWriter writer(path));
  EXPECT_TRUE(std::filesystem::exists(path));
  std::remove(path.c_str());
}

TEST(SSTableWriterTest, ConstructionWithInvalidPath) {
  EXPECT_THROW(SSTableWriter writer("/non_existent/dir/file.sstable"),
               std::runtime_error);
}

TEST(SSTableWriterTest, WriteMemtableDoesNotThrow) {
  const std::string path = "/tmp/kv_sstable_writer_test";
  SSTableWriter writer(path);
  Memtable memtable;
  memtable.put("key1", "value1");
  memtable.put("key2", "value2");

  EXPECT_NO_THROW(writer.write_memtable(memtable));
  auto file_size = std::filesystem::file_size(path);
  // 4 bytes key length + 4 bytes key data + 4 bytes value length + 6 bytes
  // value data = 4 + 4 + 4 + 6 = 18 bytes per key-value pair, so 36 bytes total
  // for 2 pairs.
  EXPECT_EQ(file_size, 36);
}

TEST(SSTableWriterTest, WriteMemtableWithTombstone) {
  const std::string path = "/tmp/kv_sstable_writer_tombstone_test";
  SSTableWriter writer(path);
  Memtable memtable;
  memtable.put("key1", "value1");
  memtable.remove("key2");

  EXPECT_NO_THROW(writer.write_memtable(memtable));
  auto file_size = std::filesystem::file_size(path);
  // key1: 4 bytes key length + 4 bytes key data + 4 bytes value length + 6
  // bytes value data = 18 bytes
  // key2 tombstone: 4 bytes key length + 4 bytes key data + 4 bytes tombstone
  // marker = 12 bytes
  // Total = 30 bytes.
  EXPECT_EQ(file_size, 30);
}
} // namespace
} // namespace kv
