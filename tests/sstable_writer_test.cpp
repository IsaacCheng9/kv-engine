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

TEST(SSTableWriterTest, WriteMemtableWithoutTombstone) {
  const std::string path = "/tmp/kv_sstable_writer_test";
  std::remove(path.c_str()); // Clean up from previous runs in case of crash.
  SSTableWriter writer(path);
  Memtable memtable;
  memtable.put("key1", "value1");
  memtable.put("key2", "value2");

  EXPECT_NO_THROW(writer.write_memtable(memtable));
  auto file_size = std::filesystem::file_size(path);
  // Data: 4 bytes key length + 4 bytes key data + 4 bytes value length + 6
  // bytes value data = 4 + 4 + 4 + 6 = 18 bytes per key-value pair, so 36 bytes
  // Index: 2 entries * (4 + 4 + 8) bytes = 32 bytes (key length + key data +
  // offset)
  // Footer: 8 bytes
  // Total = 36 + 32 + 8 = 76 bytes
  EXPECT_EQ(file_size, 76);

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
  // Data: 4 bytes key length + 4 bytes key data + 4 bytes value length + 6
  // bytes value data = 18 bytes for key1, and 4 bytes key length + 4 bytes key
  // data + 4 bytes tombstone marker = 12 bytes for key2, so 30 bytes total
  // Index: 2 entries * (4 + 4 + 8) bytes = 32 bytes (key length + key data +
  // offset)
  // Footer: 8 bytes
  // Total = 30 + 32 + 8 = 70 bytes
  EXPECT_EQ(file_size, 70);

  std::remove(path.c_str());
}
} // namespace
} // namespace kv
