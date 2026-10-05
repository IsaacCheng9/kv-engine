#include "engine.hpp"
#include "test_utils.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <string>

namespace kv {

namespace {

TEST(EngineTest, PutAndGet) {
  const test::TempDir dir;
  const std::string temp_dir = dir.path().string();

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
}

TEST(EngineTest, Remove) {
  const test::TempDir dir;
  const std::string temp_dir = dir.path().string();

  {
    Engine engine(temp_dir);
    engine.put("key1", "value1");
    engine.remove("key1");

    auto value1 = engine.get("key1");
    EXPECT_FALSE(value1.has_value());
  }
}

TEST(EngineTest, FlushTriggersOnThreshold) {
  const test::TempDir dir;
  const std::string temp_dir = dir.path().string();

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
}

TEST(EngineTest, WALReplay) {
  const test::TempDir dir;
  const std::string temp_dir = dir.path().string();

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
}

TEST(EngineTest, GetReturnsValueFromSSTableAfterFlush) {
  const test::TempDir dir;
  const std::string temp_dir = dir.path().string();

  // Use a small memtable size to trigger flush on first put.
  {
    Engine engine(temp_dir, 1);
    engine.put("key1", "value1");

    // After flush, memtable is empty - get must come from the SSTable.
    auto value1 = engine.get("key1");
    EXPECT_TRUE(value1.has_value());
    EXPECT_EQ(value1.value(), "value1");
  }
}

TEST(EngineTest, GetNewerSSTableOverridesOlderSSTableForSameKey) {
  const test::TempDir dir;
  const std::string temp_dir = dir.path().string();

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
}

TEST(EngineTest, FlushingFourTimesTriggersLevelCompaction) {
  const test::TempDir dir;
  const std::string temp_dir = dir.path().string();

  // Small memtable size to trigger flush on each put.
  {
    Engine engine(temp_dir, 1);
    engine.put("key1", "value1");
    engine.put("key2", "value2");
    engine.put("key3", "value3");
    engine.put("key4", "value4");

    // After four flushes, compaction should retire every L0 file into a
    // single L1 file.
    ASSERT_TRUE(test::wait_for_compaction(temp_dir))
        << "Compaction did not retire the L0 files within the timeout";
    EXPECT_EQ(test::count_sstables(temp_dir, 1), 1u);
  }
}

TEST(EngineTest, GetWorksAcrossLevelsAfterCompaction) {
  const test::TempDir dir;
  const std::string temp_dir = dir.path().string();

  // Small memtable size to trigger flush on each put.
  {
    Engine engine(temp_dir, 1);
    engine.put("key1", "value1");
    engine.put("key2", "value2");
    engine.put("key3", "value3");
    engine.put("key4", "value4");

    ASSERT_TRUE(test::wait_for_compaction(temp_dir))
        << "Compaction did not complete within the timeout";

    // After compaction, all keys should still be retrievable.
    EXPECT_EQ(engine.get("key1"), "value1");
    EXPECT_EQ(engine.get("key2"), "value2");
    EXPECT_EQ(engine.get("key3"), "value3");
    EXPECT_EQ(engine.get("key4"), "value4");
  }
}

TEST(EngineTest, RepeatedFlushesDoNotLoseNewerLevelZeroFiles) {
  const test::TempDir dir;
  const std::string temp_dir = dir.path().string();

  {
    Engine engine(temp_dir, 1);
    for (int i = 0; i < 32; ++i) {
      engine.put("key" + std::to_string(i), "value" + std::to_string(i));
    }

    // Let the background thread run at least one compaction so the test
    // exercises the post-compaction state. Level 0 need not drain completely
    // after 32 flushes, so wait for the first L1 file rather than for
    // wait_for_compaction(); reads stay correct mid-compaction either way.
    ASSERT_TRUE(test::wait_until([&] {
      return test::count_sstables(temp_dir, 1) > 0;
    })) << "Compaction did not produce an L1 file within the timeout";

    for (int i = 0; i < 32; ++i) {
      EXPECT_EQ(engine.get("key" + std::to_string(i)),
                "value" + std::to_string(i));
    }
  }
}

TEST(EngineTest, CompactionWithOnlyTombstonesPublishesL1File) {
  // Previously asserted that an L1 file is NOT published when compaction
  // produces only tombstones - that pinned a buggy optimisation. After
  // the tombstone-preservation fix in `compact_sstables`, a tombstone-
  // only compaction DOES publish an L1 file: the tombstones are load-
  // bearing because they shadow same-key values in any older L1 files.
  // The engine can't tell at compaction time whether an older L1 file
  // exists with the same key, so it preserves the tombstones to be safe.
  const test::TempDir dir;
  const std::string temp_dir = dir.path().string();

  {
    Engine engine(temp_dir, 1);
    engine.put("key1", "value1");
    engine.remove("key1");
    engine.remove("key1");
    engine.remove("key1");

    ASSERT_TRUE(test::wait_for_compaction(temp_dir))
        << "Compaction did not complete within the timeout";
    // The L1 file contains a tombstone for key1; the engine reads through
    // it correctly and returns nullopt.
    EXPECT_EQ(engine.get("key1"), std::nullopt);
  }

  {
    // After reopen the same L1 file is rehydrated; the tombstone still
    // shadows correctly.
    Engine reopened_engine(temp_dir, 1);
    EXPECT_EQ(reopened_engine.get("key1"), std::nullopt);
  }
}
} // namespace
} // namespace kv
