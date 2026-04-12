#ifndef KV_ENGINE_ENGINE_HPP
#define KV_ENGINE_ENGINE_HPP

#include "memtable.hpp"
#include "wal.hpp"
#include <cstdint>
#include <optional>
#include <string>

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
  std::vector<uint64_t> sstable_ids_;
  uint64_t next_sstable_id_ = 0;
};

} // namespace kv

#endif // KV_ENGINE_ENGINE_HPP
