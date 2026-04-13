#include "engine.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <string>

namespace kv {

namespace {

TEST(EngineTest, PutAndGet) {
  std::string temp_dir = std::filesystem::temp_directory_path() /
                         std::filesystem::path("kv_engine_test_put_get");
  std::filesystem::remove_all(temp_dir);
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
  std::filesystem::remove_all(temp_dir);
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
  std::filesystem::remove_all(temp_dir);
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
  std::filesystem::remove_all(temp_dir);
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

TEST(EngineTest, GetReturnsValueFromSSTableAfterFlush) {
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path(
          "kv_engine_test_get_returns_value_from_sstable_after_flush");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  // Use a small memtable size to trigger flush on first put.
  Engine engine(temp_dir, 1);
  engine.put("key1", "value1");

  // After flush, memtable is empty - get must come from the SSTable.
  auto value1 = engine.get("key1");
  EXPECT_TRUE(value1.has_value());
  EXPECT_EQ(value1.value(), "value1");

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineTest, GetNewerSSTableOverridesOlderSSTableForSameKey) {
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path(
          "kv_engine_test_get_newer_sstable_overrides_older_sstable");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  // Small memtable size to trigger flush on each put.
  Engine engine(temp_dir, 1);
  // Flushes to sstable_0.dat.
  engine.put("key1", "value1");
  // Flushes to sstable_1.dat.
  engine.put("key1", "value2");

  // Get should return the value from the newer SSTable (sstable_1.dat).
  auto value2 = engine.get("key1");
  EXPECT_TRUE(value2.has_value());
  EXPECT_EQ(value2.value(), "value2");

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineTest, FlushingFourTimesTriggersLevelCompaction) {
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path(
          "kv_engine_test_flushing_four_times_triggers_level_compaction");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  // Small memtable size to trigger flush on each put.
  Engine engine(temp_dir, 1);
  engine.put("key1", "value1");
  engine.put("key2", "value2");
  engine.put("key3", "value3");
  engine.put("key4", "value4");

  // After four flushes, we should have triggered compaction of level zero into
  // level one. Check that the L0 files are gone and the L1 file exists.
  bool l0_files_exist = false;
  bool l1_file_exists = false;
  for (const auto &entry : std::filesystem::directory_iterator(temp_dir)) {
    auto filename = entry.path().filename().string();
    if (filename.starts_with("sstable_0_") &&
        entry.path().extension() == ".dat") {
      l0_files_exist = true;
    }
    if (filename.starts_with("sstable_1_") &&
        entry.path().extension() == ".dat") {
      l1_file_exists = true;
    }
  }
  EXPECT_FALSE(l0_files_exist);
  EXPECT_TRUE(l1_file_exists);

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineTest, GetWorksAcrossLevelsAfterCompaction) {
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path(
          "kv_engine_test_get_works_across_levels_after_compaction");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  // Small memtable size to trigger flush on each put.
  Engine engine(temp_dir, 1);
  engine.put("key1", "value1");
  engine.put("key2", "value2");
  engine.put("key3", "value3");
  engine.put("key4", "value4");

  // After compaction, all keys should still be retrievable.
  EXPECT_EQ(engine.get("key1"), "value1");
  EXPECT_EQ(engine.get("key2"), "value2");
  EXPECT_EQ(engine.get("key3"), "value3");
  EXPECT_EQ(engine.get("key4"), "value4");

  std::filesystem::remove_all(temp_dir);
}
} // namespace
} // namespace kv
