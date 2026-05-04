#include "engine.hpp"

#include <atomic>
#include <filesystem>
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

} // namespace
} // namespace kv
