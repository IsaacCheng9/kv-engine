#include "engine.hpp"

#include <atomic>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

namespace kv {

namespace {

TEST(EngineConcurrencyTest, ConcurrentPutsArePersisted) {
  std::string temp_dir = std::filesystem::temp_directory_path() /
                         std::filesystem::path("kv_engine_stress_puts");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  constexpr int num_threads = 4;
  constexpr int ops_per_thread = 1000;

  std::atomic<int> observed_failures{0};

  {
    Engine engine(temp_dir, 1024); // Small memtable to force flushes.
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
        std::string expected =
            "v" + std::to_string(t) + "_" + std::to_string(i);
        auto got = engine.get(key);
        if (!got.has_value() || got.value() != expected) {
          observed_failures++;
        }
      }
    }
  }

  EXPECT_EQ(observed_failures.load(), 0);
  std::filesystem::remove_all(temp_dir);
}

TEST(EngineConcurrencyTest, ConcurrentReadersDuringWrites) {
  std::string temp_dir = std::filesystem::temp_directory_path() /
                         std::filesystem::path("kv_engine_stress_readers");
  std::filesystem::remove_all(temp_dir);
  std::filesystem::create_directories(temp_dir);

  constexpr int num_readers = 4;
  constexpr int num_writes = 2000;

  std::atomic<bool> done{false};
  std::atomic<int> read_errors{0};

  {
    Engine engine(temp_dir, 1024);

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
  }

  EXPECT_EQ(read_errors.load(), 0);
  std::filesystem::remove_all(temp_dir);
}

} // namespace
} // namespace kv
