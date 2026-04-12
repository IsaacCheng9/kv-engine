#include "engine.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <string>

namespace kv {

namespace {

TEST(EngineTest, PutAndGet) {
  std::string temp_dir = std::filesystem::temp_directory_path() /
                         std::filesystem::path("kv_engine_test_put_get");
  std::filesystem::create_directories(temp_dir);

  Engine engine(temp_dir);
  engine.put("key1", "value1");
  engine.put("key2", "value2");

  auto value1 = engine.get("key1");
  EXPECT_TRUE(value1.has_value());
  EXPECT_EQ(value1.value(), "value1");

  auto value2 = engine.get("key2");
  EXPECT_TRUE(value2.has_value());
  EXPECT_EQ(value2.value(), "value2");

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineTest, Remove) {
  std::string temp_dir = std::filesystem::temp_directory_path() /
                         std::filesystem::path("kv_engine_test_remove");
  std::filesystem::create_directories(temp_dir);

  Engine engine(temp_dir);
  engine.put("key1", "value1");
  engine.remove("key1");

  auto value1 = engine.get("key1");
  EXPECT_FALSE(value1.has_value());

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineTest, FlushTriggersOnThreshold) {
  std::string temp_dir = std::filesystem::temp_directory_path() /
                         std::filesystem::path("kv_engine_test_flush");
  std::filesystem::create_directories(temp_dir);

  // Use a small memtable size to trigger flush quickly.
  Engine engine(temp_dir, 10);
  engine.put("key1", "value1");
  engine.put("key2", "value2");

  // Check that at least one SSTable file was created. Our second put should
  // have triggered a flush since the memtable max size is 10 bytes.
  bool sstable_found = false;
  for (const auto &entry : std::filesystem::directory_iterator(temp_dir)) {
    if (entry.path().extension() == ".dat") {
      sstable_found = true;
      break;
    }
  }
  EXPECT_TRUE(sstable_found);

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineTest, WALReplay) {
  std::string temp_dir = std::filesystem::temp_directory_path() /
                         std::filesystem::path("kv_engine_test_wal_replay");
  std::filesystem::create_directories(temp_dir);

  {
    Engine engine(temp_dir);
    engine.put("key1", "value1");
    engine.put("key2", "value2");
    // Don't flush manually - we'll rely on the destructor to flush on close.
  }

  // Create a new engine instance which should replay the WAL and reconstruct
  // the memtable.
  Engine engine(temp_dir);
  auto value1 = engine.get("key1");
  EXPECT_TRUE(value1.has_value());
  EXPECT_EQ(value1.value(), "value1");

  auto value2 = engine.get("key2");
  EXPECT_TRUE(value2.has_value());
  EXPECT_EQ(value2.value(), "value2");

  std::filesystem::remove_all(temp_dir);
}

} // namespace
} // namespace kv
