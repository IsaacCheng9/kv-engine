#include "engine.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <format>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace kv {

namespace {

class EngineConcurrencyTest : public ::testing::Test {
protected:
  std::filesystem::path data_dir;

  void SetUp() override {
    auto test_name =
        ::testing::UnitTest::GetInstance()->current_test_info()->name();
    data_dir = std::filesystem::temp_directory_path() /
               std::filesystem::path(std::string("kv_engine_concurrency_") +
                                     test_name);
    std::filesystem::remove_all(data_dir);
    std::filesystem::create_directories(data_dir);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(data_dir, ec);
  }

  // Drain a scan iterator into a vector. Yields between entries to give
  // concurrent threads a chance to run, exposing races that a single-threaded
  // drain would silently miss.
  static std::vector<std::pair<std::string, std::string>>
  drain(ScanIterator it) {
    std::vector<std::pair<std::string, std::string>> out;
    while (auto entry = it.next()) {
      out.emplace_back(std::move(entry->first), std::move(entry->second));
      std::this_thread::yield();
    }
    return out;
  }
};

TEST_F(EngineConcurrencyTest, ConcurrentPutsArePersisted) {
  constexpr int num_threads = 4;
  constexpr int ops_per_thread = 100;

  std::atomic<int> observed_failures{0};

  Engine engine(data_dir.string(), 1024); // Small memtable to force flushes.
  std::vector<std::thread> workers;
  for (int t = 0; t < num_threads; ++t) {
    workers.emplace_back([&, t] {
      for (int i = 0; i < ops_per_thread; ++i) {
        std::string key = "t" + std::to_string(t) + "_" + std::to_string(i);
        std::string value = "v" + std::to_string(t) + "_" + std::to_string(i);
        engine.put(key, value);
      }
    });
  }
  for (auto &w : workers) {
    w.join();
  }

  // Verify every write is readable.
  for (int t = 0; t < num_threads; ++t) {
    for (int i = 0; i < ops_per_thread; ++i) {
      std::string key = "t" + std::to_string(t) + "_" + std::to_string(i);
      std::string expected = "v" + std::to_string(t) + "_" + std::to_string(i);
      auto got = engine.get(key);
      if (!got.has_value() || got.value() != expected) {
        observed_failures++;
      }
    }
  }

  EXPECT_EQ(observed_failures.load(), 0);
}

TEST_F(EngineConcurrencyTest, ConcurrentReadersDuringWrites) {
  constexpr int num_readers = 4;
  constexpr int num_writes = 200;

  std::atomic<bool> done{false};
  std::atomic<int> read_errors{0};

  Engine engine(data_dir.string(), 1024);

  // Pre-populate some keys so readers have hits.
  for (int i = 0; i < 200; ++i) {
    engine.put("seed_" + std::to_string(i), "seed_value");
  }

  std::vector<std::thread> readers;
  for (int r = 0; r < num_readers; ++r) {
    readers.emplace_back([&] {
      while (!done.load()) {
        for (int i = 0; i < 200; ++i) {
          auto got = engine.get("seed_" + std::to_string(i));
          if (!got.has_value() || got.value() != "seed_value") {
            read_errors++;
          }
        }
        // Yield to give the writer a chance - prevents reader starvation
        // on slow CI runners with few cores.
        std::this_thread::yield();
      }
    });
  }

  // Writer thread.
  std::thread writer([&] {
    for (int i = 0; i < num_writes; ++i) {
      engine.put("w_" + std::to_string(i), "v");
    }
    done.store(true);
  });

  writer.join();
  for (auto &r : readers) {
    r.join();
  }

  EXPECT_EQ(read_errors.load(), 0);
}

TEST_F(EngineConcurrencyTest, ScanIsIsolatedFromConcurrentWrites) {
  constexpr int initial_keys = 50;
  constexpr int num_writes = 100;
  Engine engine(data_dir.string());

  // Pre-populate `initial_keys` keys, then snapshot and store the iterator.
  for (int i = 0; i < initial_keys; ++i) {
    engine.put(std::format("key{:03}", i), std::format("v{}", i));
  }
  auto it = engine.scan("", "", 0);

  // Set up the writer thread, drain, then join.
  std::thread writer([&engine]() {
    for (int i = initial_keys; i < initial_keys + num_writes; ++i) {
      engine.put(std::format("key{:03}", i), std::format("v{}", i));
    }
  });
  auto results = drain(std::move(it));
  writer.join();

  ASSERT_EQ(results.size(), static_cast<std::size_t>(initial_keys));
  for (int i = 0; i < initial_keys; ++i) {
    ASSERT_EQ(results[i].first, std::format("key{:03}", i));
    ASSERT_EQ(results[i].second, std::format("v{}", i));
  }
}

TEST_F(EngineConcurrencyTest, ScanIsIsolatedFromConcurrentFlush) {
  constexpr int initial_keys = 5;
  // Triggers ~2 flushes, below the 4-file compaction threshold.
  constexpr int num_writes = 4;
  // Use a small memtable so that the writer's puts trigger a flush.
  Engine engine(data_dir.string(), 100);
  for (int i = 0; i < initial_keys; ++i) {
    engine.put(std::format("key{:03}", i), std::format("v{}", i));
  }
  auto it = engine.scan("", "", 0);

  // Perform puts with the writer thread that overflow the memtable by using
  // larger values.
  std::thread writer([&engine]() {
    for (int i = initial_keys; i < initial_keys + num_writes; ++i) {
      engine.put(std::format("key{:03}", i), std::string(50, 'x'));
    }
  });
  auto results = drain(std::move(it));
  writer.join();

  ASSERT_EQ(results.size(), static_cast<std::size_t>(initial_keys));
  for (int i = 0; i < initial_keys; ++i) {
    ASSERT_EQ(results[i].first, std::format("key{:03}", i));
    ASSERT_EQ(results[i].second, std::format("v{}", i));
  }
}

// Verifies the iterator's open SSTable file descriptors keep working even
// after compaction has unlinked the underlying L0 files. POSIX semantics:
// when a file is unlinked while an fd is still open, the data persists
// until the fd is closed - so the iterator finishes reading even though
// the paths are gone from anyone else's view of the filesystem.
TEST_F(EngineConcurrencyTest, ScanIsIsolatedFromConcurrentCompaction) {
  constexpr int initial_keys = 3;
  constexpr int writer_key_index = 50;
  Engine engine(data_dir.string(), 50);
  // Each pre-populate put overflows the 50-byte memtable, forcing an
  // immediate flush. Three puts thus create three L0 files. Values use
  // 'a' + i so each pre-populate has a distinct, recognisable pattern.
  for (int i = 0; i < initial_keys; ++i) {
    engine.put(std::format("key{:03}", i), std::string(50, 'a' + i));
  }
  auto it = engine.scan("", "", 0);

  // Capture the three L0 file paths so we can poll for deletion later, and
  // make sure these paths exist.
  std::vector<std::filesystem::path> l0_files;
  for (int i = 0; i < initial_keys; ++i) {
    l0_files.push_back(data_dir / std::format("sstable_0_{}.dat", i));
  }
  for (const auto &path : l0_files) {
    ASSERT_TRUE(std::filesystem::exists(path));
  }

  // Perform one more put with the writer thread to create a 4th L0 file,
  // resulting in a compaction.
  std::thread writer([&engine]() {
    engine.put(std::format("key{:03}", writer_key_index + 1),
               std::string(50, 'x'));
  });
  writer.join();

  // Poll for the 3 original L0 files to be deleted (compaction signal).
  constexpr auto poll_interval = std::chrono::milliseconds(10);
  constexpr auto timeout = std::chrono::seconds(5);
  const auto deadline = std::chrono::steady_clock::now() + timeout;

  bool all_deleted = false;
  while (std::chrono::steady_clock::now() < deadline) {
    all_deleted = std::ranges::all_of(
        l0_files, [](const auto &p) { return !std::filesystem::exists(p); });
    if (all_deleted) {
      break;
    }
    std::this_thread::sleep_for(poll_interval);
  }
  ASSERT_TRUE(all_deleted)
      << "Compaction did not retire L0 files within timeout";

  // Drain the scan iterator to ensure all results are consumed.
  auto results = drain(std::move(it));

  ASSERT_EQ(results.size(), static_cast<std::size_t>(initial_keys));
  for (int i = 0; i < initial_keys; ++i) {
    ASSERT_EQ(results[i].first, std::format("key{:03}", i));
    ASSERT_EQ(results[i].second, std::string(50, 'a' + i));
  }
}

} // namespace

} // namespace kv
