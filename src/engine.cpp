#include "engine.hpp"
#include "sstable_writer.hpp"
#include <optional>
#include <string>

namespace kv {

Engine::Engine(const std::string &data_dir, std::size_t memtable_max_size)
    : data_dir_(data_dir), memtable_(memtable_max_size),
      wal_(data_dir + "/wal.log") {
  // On startup, replay the WAL to reconstruct the memtable.
  wal_.replay(memtable_);
}

void Engine::flush_if_full() {
  if (memtable_.is_full()) {
    // Use the next available SSTable file name based on an incrementing ID - we
    // do this because timestamp-based names can have collisions in rare cases.
    SSTableWriter writer(data_dir_ + "/sstable_" +
                         std::to_string(next_sstable_id_++) + ".dat");
    writer.write_memtable(memtable_);
    memtable_.clear();
    wal_.clear();
  }
}

void Engine::put(const std::string &key, const std::string &value) {
  wal_.log_put(key, value);
  memtable_.put(key, value);
  flush_if_full();
}

std::optional<std::string> Engine::get(const std::string &key) const {
  return memtable_.get(key);
}

void Engine::remove(const std::string &key) {
  wal_.log_remove(key);
  memtable_.remove(key);
  flush_if_full();
}

} // namespace kv
