#include "memtable.hpp"
#include <gtest/gtest.h>
#include <string>

namespace kv {

namespace {

TEST(MemtableTest, PutAndGetMatchesInsertedValues) {
  Memtable memtable;
  memtable.put("key1", "value1");
  memtable.put("key2", "value2");
  EXPECT_EQ(memtable.get("key1"), std::string("value1"));
  EXPECT_EQ(memtable.get("key2"), std::string("value2"));
}

TEST(MemtableTest, GetReturnsNulloptForMissingKeys) {
  Memtable memtable;
  EXPECT_EQ(memtable.get("nonexistent"), std::nullopt);
}

TEST(MemtableTest, PutOverwritesExistingValue) {
  Memtable memtable;
  memtable.put("key1", "value1");
  EXPECT_EQ(memtable.get("key1"), std::string("value1"));
  memtable.put("key1", "new_value");
  EXPECT_EQ(memtable.get("key1"), std::string("new_value"));
}

TEST(MemtableTest, RemoveInsertsNulloptForKey) {
  Memtable memtable;
  memtable.put("key1", "value1");
  memtable.remove("key1");
  EXPECT_EQ(memtable.get("key1"), std::nullopt);
}
} // namespace
} // namespace kv
