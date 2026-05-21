// End-to-end integration tests for `Engine` that exercise behaviour across
// engine lifetimes - opening, mutating, destroying, and reopening on the same
// data directory. Existing unit tests in `engine_test.cpp` cover single-
// lifetime semantics; these tests cover the persistence and replay surface
// that those tests don't reach.
#include "engine.hpp"
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <thread>

namespace kv {

namespace {

// Polls for an L1 SSTable file in `dir`, returning true if one appears before
// the deadline. Mirrors the helper in engine_test.cpp; duplicated here rather
// than extracted to a shared header to keep test files self-contained.
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

TEST(EngineIntegrationTest, ReopenAfterFlushPreservesLevelZeroData) {
  // Forces an L0 SSTable flush (via tiny memtable) and verifies the reopened
  // engine reads the same value back via the on-disk SSTable - exercising the
  // startup directory scan + reader hydration path, not WAL replay.
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path("kv_engine_integration_reopen_l0");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  {
    Engine engine(temp_dir, 1); // 1-byte memtable forces a flush per put.
    engine.put("key1", "value1");
    engine.put("key2", "value2");
    engine.put("key3", "value3");
  }

  {
    Engine reopened(temp_dir);
    EXPECT_EQ(reopened.get("key1"), "value1");
    EXPECT_EQ(reopened.get("key2"), "value2");
    EXPECT_EQ(reopened.get("key3"), "value3");
  }

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineIntegrationTest, ReopenAfterCompactionPreservesLevelOneData) {
  // Triggers L0 -> L1 compaction by writing four flushes, waits for the L1 file
  // to appear, then reopens and verifies. Catches regressions where L1 files
  // aren't picked up during the startup directory scan.
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path("kv_engine_integration_reopen_l1");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  {
    Engine engine(temp_dir, 1);
    for (int i = 0; i < 4; ++i) {
      engine.put("key" + std::to_string(i), "value" + std::to_string(i));
    }
    ASSERT_TRUE(wait_for_l1_sstable(temp_dir))
        << "Compaction did not produce an L1 file within the timeout";
  }

  {
    Engine reopened_engine(temp_dir);
    for (int i = 0; i < 4; ++i) {
      EXPECT_EQ(reopened_engine.get("key" + std::to_string(i)),
                "value" + std::to_string(i));
    }
  }

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineIntegrationTest, WALReplayAppliesTombstones) {
  // Existing WALReplay test only covers puts. This adds the remove-then-reopen
  // path, catching a bug class where tombstones in the WAL aren't replayed
  // (which would resurrect deleted keys on engine restart).
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path("kv_engine_integration_wal_tombstones");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  {
    Engine engine(temp_dir);
    engine.put("present", "value");
    engine.put("doomed", "value");
    engine.remove("doomed");
  }

  {
    Engine reopened(temp_dir);
    EXPECT_EQ(reopened.get("present"), "value");
    EXPECT_EQ(reopened.get("doomed"), std::nullopt);
  }

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineIntegrationTest, DataSurvivesMultipleReopens) {
  // Open/put/close cycle four times, then verify all keys from all lifetimes
  // are present in the final reopen. Catches state-management bugs that only
  // manifest after multiple lifetime transitions (e.g. an SSTable ID counter
  // that resets, causing collisions across reopens).
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path("kv_engine_integration_multiple_reopens");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  for (int lifetime = 0; lifetime < 4; ++lifetime) {
    // Create a tiny memtable so that each lifetime flushes.
    Engine engine(temp_dir, 1);
    engine.put("lifetime" + std::to_string(lifetime),
               "value" + std::to_string(lifetime));
  }

  Engine final_engine(temp_dir);
  for (int lifetime = 0; lifetime < 4; ++lifetime) {
    EXPECT_EQ(final_engine.get("lifetime" + std::to_string(lifetime)),
              "value" + std::to_string(lifetime));
  }

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineIntegrationTest, TombstoneSurvivesL0ToL1Compaction) {
  // Regression test for a tombstone-drop bug in compact_sstables. The bug:
  //   1. put("doomed", "ghost") -> L0 -> compaction merges into L1 file A
  //      (contains doomed=ghost)
  //   2. remove("doomed") -> tombstone flushed to a new L0 file
  //   3. More puts trigger a second L0->L1 compaction. The merge gates
  //      every write on `if (value.has_value())`, so the tombstone is
  //      dropped from the output. New L1 file B has no `doomed` entry
  //   4. L1 now has file A (doomed=ghost) and file B (no doomed). get
  //      walks L1 newest-first; B says "not in this file, try older",
  //      falls through to A, returns "ghost". Deleted key reappears
  // The fix is to preserve tombstones through L0->L1 compaction, since L1
  // is the bottom level in this engine. With no L2+ to drain into, a
  // dropped tombstone permanently fails to shadow older L1 entries for
  // the same key.
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path("kv_engine_integration_tombstone_l0_l1_compaction");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  auto count_l1 = [&]() {
    std::size_t count = 0;
    for (const auto &entry : std::filesystem::directory_iterator(temp_dir)) {
      auto filename = entry.path().filename().string();
      if (filename.starts_with("sstable_1_") &&
          entry.path().extension() == ".dat") {
        ++count;
      }
    }
    return count;
  };

  Engine engine(temp_dir, 1); // Tiny memtable so every put flushes to L0.

  // Phase 1: write doomed=ghost, fill L0 to 4 files, wait for the first
  // L0->L1 compaction to produce L1 file A containing doomed=ghost.
  engine.put("doomed", "ghost");
  engine.put("a", "1");
  engine.put("b", "2");
  engine.put("c", "3");
  ASSERT_TRUE(wait_for_l1_sstable(temp_dir))
      << "First L0->L1 compaction did not produce an L1 file";
  ASSERT_EQ(engine.get("doomed"), "ghost");
  const std::size_t l1_count_after_phase1 = count_l1();

  // Phase 2: remove doomed, fill L0 again, wait for the second L0->L1
  // compaction. The tombstone is in one of the merged L0 files. With the
  // bug, the merge drops the tombstone from L1 file B.
  engine.remove("doomed");
  engine.put("d", "4");
  engine.put("e", "5");
  engine.put("f", "6");

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (count_l1() <= l1_count_after_phase1 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_GT(count_l1(), l1_count_after_phase1)
      << "Second L0->L1 compaction did not produce an additional L1 file";

  // The bug surfaces here: with the tombstone dropped from the newer L1
  // file, get falls through to the older L1 file and returns "ghost".
  // After the fix, the tombstone is preserved and get returns nullopt.
  EXPECT_EQ(engine.get("doomed"), std::nullopt);

  std::filesystem::remove_all(temp_dir);
}

TEST(EngineIntegrationTest, TombstoneInNewerSSTableShadowsLiveValueInOlder) {
  // put -> flush, remove -> flush (tombstone written to a newer SSTable), then
  // reopen and verify the tombstone shadows the older value.
  std::string temp_dir =
      std::filesystem::temp_directory_path() /
      std::filesystem::path("kv_engine_integration_tombstone_in_newer_sstable_"
                            "shadows_live_value_in_older");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  // Write a key to an SSTable, then remove it and have the tombstone written
  // to a different (newer) SSTable, then get the key.
  {
    Engine engine(temp_dir, 1);
    engine.put("key1", "value1");
    engine.remove("key1");
  }
  {
    Engine reopened_engine(temp_dir);
    EXPECT_EQ(reopened_engine.get("key1"), std::nullopt);
  }

  std::filesystem::remove_all(temp_dir);
}

} // namespace
} // namespace kv
