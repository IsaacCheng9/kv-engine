#include "memtable.hpp"
#include "sstable_reader.hpp"
#include "sstable_writer.hpp"
#include <cstdio>
#include <filesystem>
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>

namespace kv {

namespace {

TEST(SSTableReaderTest, ConstructionWithValidPath) {
  const std::string path = "/tmp/kv_sstable_reader_test";
  std::remove(path.c_str());
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

  std::remove(path.c_str());
}

TEST(SSTableReaderTest, ConstructionWithInvalidPath) {
  EXPECT_THROW(SSTableReader reader("/non_existent/dir/file.sstable"),
               std::runtime_error);
}

TEST(SSTableReaderTest, GetReturnsNulloptForMissingKey) {
  const std::string path = "/tmp/kv_sstable_reader_test_missing_key";
  std::remove(path.c_str());
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

  std::remove(path.c_str());
}

TEST(SSTableReaderTest, GetReturnsValueForExistingKey) {
  const std::string path = "/tmp/kv_sstable_reader_test_existing_key";
  std::remove(path.c_str());
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

  std::remove(path.c_str());
}

TEST(SSTableReaderTest, GetReturnsTombstoneForDeletedKey) {
  const std::string path = "/tmp/kv_sstable_reader_test_deleted_key";
  std::remove(path.c_str());
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

  std::remove(path.c_str());
}

} // namespace
} // namespace kv
