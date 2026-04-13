#ifndef KV_ENGINE_ENGINE_HPP
#define KV_ENGINE_ENGINE_HPP

#include "memtable.hpp"
#include "wal.hpp"
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

namespace kv {

class Engine {

public:
  // Defaults to 4MB memtable size, which is common in KV stores.
  explicit Engine(const std::string &data_dir,
                  std::size_t memtable_max_size = 4 * 1024 * 1024);
  ~Engine();
  void put(const std::string &key, const std::string &value);
  std::optional<std::string> get(const std::string &key) const;
  void remove(const std::string &key);

private:
  void flush_if_full();
  std::string data_dir_;
  Memtable memtable_;
  WAL wal_;
  std::vector<std::vector<uint64_t>> level_files_;
  std::vector<uint64_t> next_id_per_level_;
  std::thread compaction_thread_;
  std::mutex compaction_mutex_;
  // Protects shared state accessed by compaction thread (level_files_,
  // next_id_per_level_). Mutable so const methods (like get) can lock it.
  mutable std::shared_mutex state_mutex_;
  std::condition_variable compaction_cv_;
  bool compaction_triggered_ = false;
  bool shutdown_ = false;

  void compact_level_zero();
  // Runs in a background thread to perform compactions when triggered.
  void compaction_loop();
};

} // namespace kv

#endif // KV_ENGINE_ENGINE_HPP
