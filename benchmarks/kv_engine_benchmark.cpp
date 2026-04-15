// Benchmark harness for the kv-engine.
//
// Measures latency (p50, p99) and throughput for several workloads. Designed
// so a single run before and after a feature lands lets you attribute the
// improvement to that feature.
//
// Scenarios:
//   1. put              - pure write throughput, measures insert cost
//   2. get_memtable     - reads served from the memtable only (fastest path)
//   3. get_sstable      - reads that fall through to SSTables on disk
//   4. get_miss         - negative lookups (keys that were never inserted)
//   5. mixed_50_50      - interleaved reads and writes, production-like
//
// The put and get_sstable scenarios use a small memtable to force many
// flushes, so SSTable code paths actually get exercised. Tune the memtable
// size via the memtable_small / memtable_large constants below.
//
// Output is a Markdown table to stdout, easy to paste into PR descriptions.

#include "engine.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

namespace {

using clock_type = std::chrono::steady_clock;
using ns = std::chrono::nanoseconds;

// Operations per scenario. Uniform 250k across all read/write scenarios gives
// ~1000 samples above any p99 threshold (tight percentile estimates) while
// keeping a full benchmark run under a minute on an M1 Max.
constexpr std::size_t put_ops = 250'000;
constexpr std::size_t get_memtable_ops = 250'000;
constexpr std::size_t get_sstable_ops = 250'000;
constexpr std::size_t get_miss_ops = 250'000;
constexpr std::size_t mixed_ops = 250'000;

// Crash recovery populates the WAL, destroys the engine, and measures the
// time to reconstruct state on a fresh open. Each op does one full populate
// and replay cycle so we can report percentile latencies like the other
// scenarios. A large memtable keeps entries in the WAL rather than flushing
// to SSTables, which is the worst case for replay.
constexpr std::size_t crash_recovery_ops = 20;
constexpr std::size_t crash_recovery_entries_per_op = 5'000;

// Small memtable (64 KiB) forces frequent flushes so SSTable code paths
// actually get exercised. The default 4 MiB memtable would keep everything
// in memory for a 100K-op run.
constexpr std::size_t memtable_small = 64 * 1024;

// Larger memtable (4 MiB) for scenarios that want to stress the in-memory
// path without flush interference.
constexpr std::size_t memtable_large = 4 * 1024 * 1024;

constexpr std::size_t value_bytes = 100;

struct Stats {
  double ops_per_sec;
  double p50_us;
  double p99_us;
  double total_ms;
};

Stats compute_stats(std::vector<uint64_t> &latencies_ns, double total_s) {
  std::sort(latencies_ns.begin(), latencies_ns.end());
  const auto p50 = latencies_ns[latencies_ns.size() * 50 / 100];
  const auto p99 = latencies_ns[latencies_ns.size() * 99 / 100];
  return {
      static_cast<double>(latencies_ns.size()) / total_s,
      static_cast<double>(p50) / 1000.0,
      static_cast<double>(p99) / 1000.0,
      total_s * 1000.0,
  };
}

// Deterministic key generation so put and get phases reference the same keys.
std::string make_key(std::size_t index) {
  char buf[24];
  std::snprintf(buf, sizeof(buf), "key_%012zu", index);
  return std::string(buf);
}

std::string make_value() { return std::string(value_bytes, 'v'); }

std::string fresh_dir(const std::string &name) {
  auto path = std::filesystem::temp_directory_path() / ("kv_bench_" + name);
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  return path.string();
}

// Run a workload of per-op closures, timing each one individually so we can
// report latency percentiles rather than just average throughput.
template <typename Op> Stats run_workload(std::size_t ops, Op op) {
  std::vector<uint64_t> latencies_ns;
  latencies_ns.reserve(ops);

  const auto workload_start = clock_type::now();
  for (std::size_t i = 0; i < ops; ++i) {
    const auto t0 = clock_type::now();
    op(i);
    const auto t1 = clock_type::now();
    latencies_ns.push_back(std::chrono::duration_cast<ns>(t1 - t0).count());
  }
  const auto total_s =
      std::chrono::duration<double>(clock_type::now() - workload_start).count();
  return compute_stats(latencies_ns, total_s);
}

Stats bench_put() {
  const auto dir = fresh_dir("put");
  const auto value = make_value();
  Stats stats;
  {
    kv::Engine engine(dir, memtable_small);
    stats = run_workload(
        put_ops, [&](std::size_t i) { engine.put(make_key(i), value); });
  }
  std::filesystem::remove_all(dir);
  return stats;
}

Stats bench_get_memtable() {
  const auto dir = fresh_dir("get_memtable");
  // Large memtable so nothing flushes - all reads hit the memtable.
  const auto value = make_value();
  Stats stats;
  {
    kv::Engine engine(dir, memtable_large);

    // Pre-load keys.
    for (std::size_t i = 0; i < get_memtable_ops; ++i) {
      engine.put(make_key(i), value);
    }

    stats = run_workload(get_memtable_ops,
                         [&](std::size_t i) { (void)engine.get(make_key(i)); });
  }
  std::filesystem::remove_all(dir);
  return stats;
}

Stats bench_get_sstable() {
  const auto dir = fresh_dir("get_sstable");
  // Small memtable forces frequent flushes - most reads hit SSTables on disk.
  const auto value = make_value();
  Stats stats;
  {
    kv::Engine engine(dir, memtable_small);

    for (std::size_t i = 0; i < get_sstable_ops; ++i) {
      engine.put(make_key(i), value);
    }

    stats = run_workload(get_sstable_ops,
                         [&](std::size_t i) { (void)engine.get(make_key(i)); });
  }
  std::filesystem::remove_all(dir);
  return stats;
}

Stats bench_get_miss() {
  const auto dir = fresh_dir("get_miss");
  // Small memtable so negative lookups must traverse multiple SSTables - this
  // is where bloom filters will shine.
  const auto value = make_value();
  Stats stats;
  {
    kv::Engine engine(dir, memtable_small);

    for (std::size_t i = 0; i < get_miss_ops; ++i) {
      engine.put(make_key(i), value);
    }

    // Query keys that were never inserted - use indices past the inserted
    // range so the keys share the "key_0000000" prefix with stored keys.
    // Without the shared prefix, binary-search comparisons reject on the
    // first byte and the index lookup is artificially cheap, which masks
    // the bloom filter's benefit.
    stats = run_workload(get_miss_ops, [&](std::size_t i) {
      (void)engine.get(make_key(i + get_miss_ops));
    });
  }
  std::filesystem::remove_all(dir);
  return stats;
}

Stats bench_mixed_50_50() {
  const auto dir = fresh_dir("mixed");
  const auto value = make_value();
  // Deterministic PRNG for reproducibility across runs.
  std::mt19937 rng(42);
  std::uniform_int_distribution<std::size_t> key_dist(0, mixed_ops - 1);
  std::uniform_int_distribution<int> op_dist(0, 1);
  Stats stats;
  {
    kv::Engine engine(dir, memtable_small);

    // Seed some keys so early reads have something to find.
    for (std::size_t i = 0; i < mixed_ops / 10; ++i) {
      engine.put(make_key(i), value);
    }

    stats = run_workload(mixed_ops, [&](std::size_t) {
      const auto key = make_key(key_dist(rng));
      if (op_dist(rng) == 0) {
        engine.put(key, value);
      } else {
        (void)engine.get(key);
      }
    });
  }
  std::filesystem::remove_all(dir);
  return stats;
}

// Each op populates a fresh WAL, destroys the engine, and times the full
// replay on reopen. Multiple ops give proper percentile latencies.
Stats bench_crash_recovery() {
  const auto dir = fresh_dir("crash_recovery");
  const auto value = make_value();

  // We only want to time the replay, not the populate step. So we populate
  // outside the timed workload and only time the reopen.
  std::vector<uint64_t> latencies_ns;
  latencies_ns.reserve(crash_recovery_ops);

  const auto workload_start = clock_type::now();
  for (std::size_t op = 0; op < crash_recovery_ops; ++op) {
    // Populate the WAL without flushing. Large memtable means all writes
    // stay in the WAL, maximising replay work.
    {
      kv::Engine engine(dir, memtable_large);
      for (std::size_t i = 0; i < crash_recovery_entries_per_op; ++i) {
        engine.put(make_key(i), value);
      }
    }

    // Reopen triggers WAL replay - this is the bit we actually measure.
    const auto t0 = clock_type::now();
    {
      kv::Engine engine(dir, memtable_large);
    }
    const auto t1 = clock_type::now();
    latencies_ns.push_back(std::chrono::duration_cast<ns>(t1 - t0).count());

    // Clean up so the next op starts fresh.
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
  }
  const auto total_s =
      std::chrono::duration<double>(clock_type::now() - workload_start).count();

  std::filesystem::remove_all(dir);
  return compute_stats(latencies_ns, total_s);
}

void print_markdown_table(
    const std::vector<std::pair<std::string, Stats>> &results) {
  std::cout << "\n";
  std::cout << "| Scenario          | Ops/sec   | p50 (us) | p99 (us) | "
               "Total (ms) |\n";
  std::cout << "|-------------------|-----------|----------|----------|"
               "------------|\n";
  for (const auto &[name, stats] : results) {
    std::cout << "| " << std::left << std::setw(17) << name << " | "
              << std::right << std::setw(9) << std::fixed
              << std::setprecision(0) << stats.ops_per_sec << " | "
              << std::setw(8) << std::setprecision(2) << stats.p50_us << " | "
              << std::setw(8) << std::setprecision(2) << stats.p99_us << " | "
              << std::setw(10) << std::setprecision(2) << stats.total_ms
              << " |\n";
  }
  std::cout << "\n";
}

} // namespace

template <typename Fn>
void run_and_record(const std::string &name, Fn fn,
                    std::vector<std::pair<std::string, Stats>> &results) {
  std::cout << "Running " << name << "...\n" << std::flush;
  auto stats = fn();
  // Use the workload time captured inside the benchmark, not the full
  // function elapsed time - the latter includes scenario setup (pre-loading
  // keys, filling the WAL, etc.) which is not what we're measuring.
  std::cout << "  " << name << " finished in " << std::fixed
            << std::setprecision(2) << stats.total_ms / 1000.0 << "s\n"
            << std::flush;
  results.emplace_back(name, stats);
}

int main() {
  std::cout << "kv-engine benchmark\n";
  std::cout << "  put            (" << put_ops << " ops)\n";
  std::cout << "  get_memtable   (" << get_memtable_ops << " ops)\n";
  std::cout << "  get_sstable    (" << get_sstable_ops << " ops)\n";
  std::cout << "  get_miss       (" << get_miss_ops << " ops)\n";
  std::cout << "  mixed_50_50    (" << mixed_ops << " ops)\n";
  std::cout << "  crash_recovery (" << crash_recovery_ops << " ops)\n\n";

  const auto suite_start = clock_type::now();

  std::vector<std::pair<std::string, Stats>> results;
  run_and_record("put", bench_put, results);
  run_and_record("get_memtable", bench_get_memtable, results);
  run_and_record("get_sstable", bench_get_sstable, results);
  run_and_record("get_miss", bench_get_miss, results);
  run_and_record("mixed_50_50", bench_mixed_50_50, results);
  run_and_record("crash_recovery", bench_crash_recovery, results);

  const auto suite_elapsed =
      std::chrono::duration<double>(clock_type::now() - suite_start).count();
  std::cout << "\nBenchmark suite finished in " << std::fixed
            << std::setprecision(2) << suite_elapsed << "s (includes scenario "
            << "setup + measurement).\n";

  print_markdown_table(results);
  return 0;
}
