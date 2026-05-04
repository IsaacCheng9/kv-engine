// Basic correctness tests for Engine::scan and the underlying ScanIterator.
// Concurrent / race tests (writer-during-scan, flush-during-scan,
// compaction-during-scan, mid-scan cancellation) live in
// engine_concurrency_test.cpp.
#include "engine.hpp"
#include <filesystem>
#include <format>
#include <gtest/gtest.h>
#include <string>
#include <utility>
#include <vector>

namespace kv {
namespace {

class EngineScanTest : public ::testing::Test {
protected:
  std::filesystem::path data_dir;

  void SetUp() override {
    auto test_name =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    data_dir =
        std::filesystem::temp_directory_path() /
        std::filesystem::path(std::string("kv_engine_scan_") + test_name);
    std::filesystem::remove_all(data_dir);
    std::filesystem::create_directories(data_dir);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(data_dir, ec);
  }

  // Drain a scan into a vector for easy comparison. Moves out of the iterator
  // so the test reads naturally as "what did we see, in order".
  static std::vector<std::pair<std::string, std::string>>
  drain(ScanIterator it) {
    std::vector<std::pair<std::string, std::string>> out;
    while (auto entry = it.next()) {
      out.emplace_back(std::move(entry->first), std::move(entry->second));
    }
    return out;
  }
};

TEST_F(EngineScanTest, EmptyEngineReturnsNothing) {
  Engine engine(data_dir.string());
  auto results = drain(engine.scan("", "", 0));
  EXPECT_TRUE(results.empty());
}

TEST_F(EngineScanTest, FullScanYieldsAllKeysInSortedOrder) {
  Engine engine(data_dir.string());
  // Insert in non-sorted order to prove the scan returns sorted output rather
  // than insertion order.
  engine.put("banana", "1");
  engine.put("apple", "2");
  engine.put("cherry", "3");

  auto results = drain(engine.scan("", "", 0));
  ASSERT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0], std::make_pair(std::string("apple"), std::string("2")));
  EXPECT_EQ(results[1],
            std::make_pair(std::string("banana"), std::string("1")));
  EXPECT_EQ(results[2],
            std::make_pair(std::string("cherry"), std::string("3")));
}

TEST_F(EngineScanTest, RangeSubsetFiltersByBounds) {
  Engine engine(data_dir.string());
  for (char c = 'a'; c <= 'e'; ++c) {
    engine.put(std::string(1, c), std::string(1, c) + "_value");
  }
  // [b, d) should yield b and c only - start inclusive, end exclusive.
  auto results = drain(engine.scan("b", "d", 0));
  ASSERT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].first, "b");
  EXPECT_EQ(results[1].first, "c");
}

TEST_F(EngineScanTest, EndKeyIsExclusive) {
  Engine engine(data_dir.string());
  engine.put("a", "1");
  engine.put("b", "2");
  engine.put("c", "3");
  // end_key="b" should yield only "a"; "b" itself is at the boundary and
  // excluded by the half-open range convention.
  auto results = drain(engine.scan("", "b", 0));
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].first, "a");
}

TEST_F(EngineScanTest, EmptyRangeReturnsNothing) {
  Engine engine(data_dir.string());
  engine.put("a", "1");
  engine.put("b", "2");
  // start_key == end_key is a half-open empty range - no entry can satisfy
  // both >= start AND < end.
  auto results = drain(engine.scan("a", "a", 0));
  EXPECT_TRUE(results.empty());
}

TEST_F(EngineScanTest, LimitCapsResults) {
  Engine engine(data_dir.string());
  for (int i = 0; i < 10; ++i) {
    engine.put(std::format("key{:02}", i), std::to_string(i));
  }
  auto results = drain(engine.scan("", "", 3));
  ASSERT_EQ(results.size(), 3u);
  EXPECT_EQ(results[0].first, "key00");
  EXPECT_EQ(results[1].first, "key01");
  EXPECT_EQ(results[2].first, "key02");
}

TEST_F(EngineScanTest, TombstoneCollapseSkipsDeletedKeys) {
  Engine engine(data_dir.string());
  engine.put("alive", "1");
  engine.put("doomed", "2");
  engine.put("survives", "3");
  engine.remove("doomed");

  auto results = drain(engine.scan("", "", 0));
  ASSERT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].first, "alive");
  EXPECT_EQ(results[1].first, "survives");
}

TEST_F(EngineScanTest, MergesAcrossMemtableAndSSTable) {
  // Memtable size 16 forces a flush after the first put (which writes
  // ~24 bytes), pushing "alpha" to disk. The second put for "beta" lands
  // in the now-empty memtable.
  Engine engine(data_dir.string(), 16);
  engine.put("alpha", std::string(20, 'a'));
  engine.put("beta", "b");

  auto results = drain(engine.scan("", "", 0));
  ASSERT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0].first, "alpha");
  EXPECT_EQ(results[1].first, "beta");
}

TEST_F(EngineScanTest, NewerMemtableValueShadowsOlderSSTableValue) {
  // Force "key1" into an SSTable, then write a new value for it. The newer
  // memtable version should win - this exercises the heap's source-priority
  // tiebreaker (memtable = source 0 = newest).
  Engine engine(data_dir.string(), 16);
  engine.put("key1", std::string(20, 'x'));
  engine.put("key1", "new_value");

  auto results = drain(engine.scan("", "", 0));
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].first, "key1");
  EXPECT_EQ(results[0].second, "new_value");
}

} // namespace

} // namespace kv
