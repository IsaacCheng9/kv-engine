#ifndef KV_ENGINE_ENGINE_HPP
#define KV_ENGINE_ENGINE_HPP

#include "memtable.hpp"
#include "wal.hpp"
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace kv {

class Engine {

public:
  // Defaults to 4MB memtable size, which is common in KV stores.
  explicit Engine(const std::string &data_dir,
                  std::size_t memtable_max_size = 4 * 1024 * 1024);
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

  void compact_level_zero();
};

} // namespace kv

#endif // KV_ENGINE_ENGINE_HPP
