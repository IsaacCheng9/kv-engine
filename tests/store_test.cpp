#include "store.hpp"

#include <gtest/gtest.h>

#include <algorithm>

namespace kv {
namespace {

TEST(StoreTest, PutAndGet) {
  Store store;
  store.put("key1", "value1");
  auto result = store.get("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "value1");
}

TEST(StoreTest, GetMissingKey) {
  Store store;
  auto result = store.get("missing");
  EXPECT_FALSE(result.has_value());
}

TEST(StoreTest, PutOverwrite) {
  Store store;
  store.put("key1", "value1");
  store.put("key1", "value2");
  auto result = store.get("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "value2");
}

TEST(StoreTest, Remove) {
  Store store;
  store.put("key1", "value1");
  EXPECT_TRUE(store.remove("key1"));
  EXPECT_FALSE(store.get("key1").has_value());
}

TEST(StoreTest, RemoveMissingKey) {
  Store store;
  EXPECT_FALSE(store.remove("missing"));
}

TEST(StoreTest, Clear) {
  Store store;
  store.put("key1", "value1");
  store.put("key2", "value2");
  store.clear();
  EXPECT_FALSE(store.get("key1").has_value());
  EXPECT_FALSE(store.get("key2").has_value());
  EXPECT_EQ(store.size(), 0);
}

TEST(StoreTest, Size) {
  Store store;
  EXPECT_EQ(store.size(), 0);
  store.put("a", "1");
  store.put("b", "2");
  EXPECT_EQ(store.size(), 2);
  store.remove("a");
  EXPECT_EQ(store.size(), 1);
}

TEST(StoreTest, ContainsFindsKey) {
  Store store;
  store.put("key1", "value1");
  EXPECT_TRUE(store.contains("key1"));
}

TEST(StoreTest, ContainsDoesNotFindMissingKey) {
  Store store;
  EXPECT_FALSE(store.contains("missing"));
}

TEST(StoreTest, GetsAllKeys) {
  Store store;
  store.put("key1", "value1");
  store.put("key2", "value2");
  auto keys = store.keys();
  EXPECT_EQ(keys.size(), 2);
  EXPECT_TRUE(std::ranges::contains(keys, "key1"));
  EXPECT_TRUE(std::ranges::contains(keys, "key2"));
}

TEST(StoreTest, KeysEmptyWhenStoreIsEmpty) {
  Store store;
  auto keys = store.keys();
  EXPECT_TRUE(keys.empty());
}

} // namespace
} // namespace kv
