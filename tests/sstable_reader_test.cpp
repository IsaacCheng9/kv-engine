#include "memtable.hpp"
#include "sstable_reader.hpp"
#include "sstable_writer.hpp"
#include "test_utils.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>

namespace kv {

namespace {

TEST(SSTableReaderTest, ConstructionWithValidPath) {
  const test::TempDir dir;
  const std::string path = dir.file("sstable_reader_test");
  {
    SSTableWriter writer(path);
    Memtable memtable;
    memtable.put("key1", "value1");
    memtable.put("key2", "value2");
    writer.write_memtable(memtable);
  }

  SSTableReader reader(path);
  EXPECT_TRUE(std::filesystem::exists(path));
  auto value1 = reader.get("key1");
  EXPECT_TRUE(value1.has_value());
  EXPECT_EQ(value1.value(), "value1");
  auto value2 = reader.get("key2");
  EXPECT_TRUE(value2.has_value());
  EXPECT_EQ(value2.value(), "value2");
}

TEST(SSTableReaderTest, ConstructionWithInvalidPath) {
  EXPECT_THROW(SSTableReader reader("/non_existent/dir/file.sstable"),
               std::runtime_error);
}

TEST(SSTableReaderTest, GetReturnsNulloptForMissingKey) {
  const test::TempDir dir;
  const std::string path = dir.file("sstable_reader_test_missing_key");
  {
    SSTableWriter writer(path);
    Memtable memtable;
    memtable.put("key1", "value1");
    memtable.put("key2", "value2");
    memtable.put("key3", "value3");
    writer.write_memtable(memtable);
  }

  SSTableReader reader(path);
  EXPECT_EQ(reader.get("missing_key"), std::nullopt);
}

TEST(SSTableReaderTest, GetReturnsValueForExistingKey) {
  const test::TempDir dir;
  const std::string path = dir.file("sstable_reader_test_existing_key");
  {
    SSTableWriter writer(path);
    Memtable memtable;
    memtable.put("key1", "value1");
    memtable.put("key2", "value2");
    memtable.put("key3", "value3");
    memtable.put("key4", "value4");
    writer.write_memtable(memtable);
  }

  SSTableReader reader(path);
  EXPECT_EQ(reader.get("key1"), "value1");
  EXPECT_EQ(reader.get("key2"), "value2");
  EXPECT_EQ(reader.get("key3"), "value3");
  EXPECT_EQ(reader.get("key4"), "value4");
  EXPECT_EQ(reader.get("key5"), std::nullopt);
}

TEST(SSTableReaderTest, GetReturnsTombstoneForDeletedKey) {
  const test::TempDir dir;
  const std::string path = dir.file("sstable_reader_test_deleted_key");
  {
    SSTableWriter writer(path);
    Memtable memtable;
    memtable.put("key1", "value1");
    memtable.put("key2", "value2");
    memtable.put("key3", "value3");
    memtable.remove("key1");
    writer.write_memtable(memtable);
  }

  SSTableReader reader(path);
  auto result = reader.get("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_FALSE(result.value().has_value());
}

} // namespace
} // namespace kv
