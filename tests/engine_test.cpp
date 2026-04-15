#include "engine.hpp"
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <thread>

namespace kv {

namespace {

// Polls for an L1 SSTable file in `dir`, returning true if one appears before
// the deadline. Used to synchronise with the background compaction thread -
// fixed sleeps are flaky because compaction latency varies with sanitiser
// instrumentation (TSan is 5-10x slower than ASan) and CI runner load.
bool wait_for_l1_sstable(
    const std::string &dir,
    std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    for (const auto &entry : std::filesystem::directory_iterator(dir)) {
      auto filename = entry.path().filename().string();
      if (filename.starts_with("sstable_1_") &&
          entry.path().extension() == ".dat") {
        return true;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return false;
}

// Polls for all SSTable and compaction temp files to disappear. Used by tests
// that compact away every live key, where successful compaction leaves no .dat
// files behind at all.
bool wait_for_no_sstable_files(
    const std::string &dir,
    std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    bool found_sstable = false;
    for (const auto &entry : std::filesystem::directory_iterator(dir)) {
      auto filename = entry.path().filename().string();
      if ((filename.starts_with("sstable_") ||
           filename.starts_with("compact_tmp_")) &&
          entry.path().extension() == ".dat") {
        found_sstable = true;
        break;
      }
    }
    if (!found_sstable) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return false;
}

TEST(EngineTest, PutAndGet) {
  std::string temp_dir = std::filesystem::temp_directory_path() /
                         std::filesystem::path("kv_engine_test_put_get");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  {
    Engine engine(temp_dir);
    engine.put("key1", "value1");
    engine.put("key2", "value2");

    auto value1 = engine.get("key1");
    EXPECT_TRUE(value1.has_value());
    EXPECT_EQ(value1.value(), "value1");

    auto value2 = engine.get("key2");
    EXPECT_TRUE(value2.has_value());
    EXPECT_EQ(value2.value(), "value2");
  }

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineTest, Remove) {
  std::string temp_dir = std::filesystem::temp_directory_path() /
                         std::filesystem::path("kv_engine_test_remove");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  {
    Engine engine(temp_dir);
    engine.put("key1", "value1");
    engine.remove("key1");

    auto value1 = engine.get("key1");
    EXPECT_FALSE(value1.has_value());
  }

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineTest, FlushTriggersOnThreshold) {
  std::string temp_dir = std::filesystem::temp_directory_path() /
                         std::filesystem::path("kv_engine_test_flush");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  // Use a small memtable size to trigger flush quickly.
  {
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
  }

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
  {
    Engine engine(temp_dir);
    auto value1 = engine.get("key1");
    EXPECT_TRUE(value1.has_value());
    EXPECT_EQ(value1.value(), "value1");

    auto value2 = engine.get("key2");
    EXPECT_TRUE(value2.has_value());
    EXPECT_EQ(value2.value(), "value2");
  }

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
  {
    Engine engine(temp_dir, 1);
    engine.put("key1", "value1");

    // After flush, memtable is empty - get must come from the SSTable.
    auto value1 = engine.get("key1");
    EXPECT_TRUE(value1.has_value());
    EXPECT_EQ(value1.value(), "value1");
  }

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
  {
    Engine engine(temp_dir, 1);
    // Flushes to sstable_0.dat.
    engine.put("key1", "value1");
    // Flushes to sstable_1.dat.
    engine.put("key1", "value2");

    // Get should return the value from the newer SSTable (sstable_1.dat).
    auto value2 = engine.get("key1");
    EXPECT_TRUE(value2.has_value());
    EXPECT_EQ(value2.value(), "value2");
  }

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
  {
    Engine engine(temp_dir, 1);
    engine.put("key1", "value1");
    engine.put("key2", "value2");
    engine.put("key3", "value3");
    engine.put("key4", "value4");

    // Wait for background compaction to finish before scanning.
    ASSERT_TRUE(wait_for_l1_sstable(temp_dir))
        << "Compaction did not produce an L1 file within the timeout";

    // After four flushes, we should have triggered compaction of level zero
    // into level one. Check that the L0 files are gone and the L1 file exists.
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
  }

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
  {
    Engine engine(temp_dir, 1);
    engine.put("key1", "value1");
    engine.put("key2", "value2");
    engine.put("key3", "value3");
    engine.put("key4", "value4");

    // Wait for background compaction to finish before scanning.
    ASSERT_TRUE(wait_for_l1_sstable(temp_dir))
        << "Compaction did not produce an L1 file within the timeout";

    // After compaction, all keys should still be retrievable.
    EXPECT_EQ(engine.get("key1"), "value1");
    EXPECT_EQ(engine.get("key2"), "value2");
    EXPECT_EQ(engine.get("key3"), "value3");
    EXPECT_EQ(engine.get("key4"), "value4");
  }

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineTest, RepeatedFlushesDoNotLoseNewerLevelZeroFiles) {
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path(
          "kv_engine_test_repeated_flushes_preserve_newer_level_zero_files");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  {
    Engine engine(temp_dir, 1);
    for (int i = 0; i < 32; ++i) {
      engine.put("key" + std::to_string(i), "value" + std::to_string(i));
    }

    // Let the background thread run at least one compaction so the test
    // exercises the post-compaction state.
    ASSERT_TRUE(wait_for_l1_sstable(temp_dir))
        << "Compaction did not produce an L1 file within the timeout";

    for (int i = 0; i < 32; ++i) {
      EXPECT_EQ(engine.get("key" + std::to_string(i)),
                "value" + std::to_string(i));
    }
  }

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineTest, CompactionThatDeletesAllKeysDoesNotPublishEmptyLevelOneFile) {
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path(
          "kv_engine_test_compaction_deletes_all_keys_without_empty_l1");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  {
    Engine engine(temp_dir, 1);
    engine.put("key1", "value1");
    engine.remove("key1");
    engine.remove("key1");
    engine.remove("key1");

    ASSERT_TRUE(wait_for_no_sstable_files(temp_dir))
        << "Compaction did not remove all SSTable files within the timeout";
    EXPECT_EQ(engine.get("key1"), std::nullopt);

    bool sstable_found = false;
    for (const auto &entry : std::filesystem::directory_iterator(temp_dir)) {
      auto filename = entry.path().filename().string();
      if (filename.starts_with("sstable_") &&
          entry.path().extension() == ".dat") {
        sstable_found = true;
        break;
      }
    }
    EXPECT_FALSE(sstable_found);
  }

  {
    Engine reopened_engine(temp_dir, 1);
    EXPECT_EQ(reopened_engine.get("key1"), std::nullopt);
  }

  std::filesystem::remove_all(temp_dir);
}
} // namespace
} // namespace kv
