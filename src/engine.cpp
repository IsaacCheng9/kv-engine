#include "engine.hpp"
#include "sstable_reader.hpp"
#include "sstable_writer.hpp"
#include <algorithm>
#include <filesystem>
#include <optional>
#include <string>

namespace kv {

Engine::Engine(const std::string &data_dir, std::size_t memtable_max_size)
    : data_dir_(data_dir), memtable_(memtable_max_size),
      wal_(data_dir + "/wal.log") {
  // On startup, replay the WAL to reconstruct the memtable.
  wal_.replay(memtable_);

  // Scan the data directory for SSTable files and load their IDs.
  for (const auto &entry : std::filesystem::directory_iterator(data_dir_)) {
    auto filename = entry.path().filename().string();
    if (filename.starts_with("sstable_") &&
        entry.path().extension() == ".dat") {
      // Extract the ID between "sstable_" and ".dat".
      auto id_str = filename.substr(8, filename.size() - 8 - 4);
      sstable_ids_.push_back(std::stoull(id_str));
    }
  }
  std::sort(sstable_ids_.begin(), sstable_ids_.end());
  // The next SSTable ID is the highest ID + 1.
  if (!sstable_ids_.empty()) {
    next_sstable_id_ = sstable_ids_.back() + 1;
  }
}

void Engine::flush_if_full() {
  if (memtable_.is_full()) {
    // Use the next available SSTable file name based on an incrementing ID - we
    // do this because timestamp-based names can have collisions in rare cases.
    uint64_t new_id = next_sstable_id_++;
    SSTableWriter writer(data_dir_ + "/sstable_" + std::to_string(new_id) +
                         ".dat");
    writer.write_memtable(memtable_);
    memtable_.clear();
    wal_.clear();
    sstable_ids_.push_back(new_id);
  }
}

void Engine::put(const std::string &key, const std::string &value) {
  wal_.log_put(key, value);
  memtable_.put(key, value);
  flush_if_full();
}

std::optional<std::string> Engine::get(const std::string &key) const {
  // Perform multi-level lookups: Memtable -> SSTables (newest to oldest).
  auto memtable_value = memtable_.get(key);
  if (memtable_value.has_value()) {
    return memtable_value;
  }

  for (auto iterator = sstable_ids_.rbegin(); iterator != sstable_ids_.rend();
       ++iterator) {
    SSTableReader reader(data_dir_ + "/sstable_" + std::to_string(*iterator) +
                         ".dat");
    auto sstable_value = reader.get(key);
    if (sstable_value.has_value()) {
      return sstable_value;
    }
  }

  // Key not found in any level.
  return std::nullopt;
}

void Engine::remove(const std::string &key) {
  wal_.log_remove(key);
  memtable_.remove(key);
  flush_if_full();
}

} // namespace kv
