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

      // File names are in format: sstable_<level>_<id>.dat
      // Split the file so we can extract the level and ID.
      auto first_underscore = 8; // Start after 'sstable_'.
      auto second_underscore = filename.find('_', first_underscore);
      auto dot_dat = filename.find('.', second_underscore);

      auto level_str = filename.substr(first_underscore,
                                       second_underscore - first_underscore);
      uint64_t level = std::stoull(level_str);
      auto id_str = filename.substr(second_underscore + 1,
                                    dot_dat - second_underscore - 1);
      uint64_t id = std::stoull(id_str);

      if (level_files_.size() <= level) {
        level_files_.resize(level + 1);
        next_id_per_level_.resize(level + 1, 0);
      }
      level_files_[level].push_back(id);
    }
  }

  // For each level, sort the files and set the next ID to the highest ID + 1.
  for (size_t level = 0; level < level_files_.size(); ++level) {
    std::sort(level_files_[level].begin(), level_files_[level].end());
    if (!level_files_[level].empty()) {
      next_id_per_level_[level] = level_files_[level].back() + 1;
    }
  }
}

void Engine::flush_if_full() {
  if (!memtable_.is_full())
    return;
  if (level_files_.empty()) {
    level_files_.resize(1);
    next_id_per_level_.resize(1, 0);
  }

  // Use the next available SSTable file name based on an incrementing ID -
  // we do this because timestamp-based names can have collisions in rare
  // cases.
  // We always flush to level 0.
  uint64_t new_id = next_id_per_level_[0]++;
  SSTableWriter writer(data_dir_ + "/sstable_0_" + std::to_string(new_id) +
                       ".dat");
  writer.write_memtable(memtable_);
  memtable_.clear();
  wal_.clear();
  level_files_[0].push_back(new_id);

  // Trigger L0 -> L1 compaction if threshold reached.
  if (level_files_[0].size() >= 4) {
    compact_level_zero();
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

  for (std::size_t level = 0; level < level_files_.size(); ++level) {
    for (auto iterator = level_files_[level].rbegin();
         iterator != level_files_[level].rend(); ++iterator) {
      auto sstable_path = data_dir_ + "/sstable_" + std::to_string(level) +
                          "_" + std::to_string(*iterator) + ".dat";
      SSTableReader reader(sstable_path);
      auto sstable_value = reader.get(key);
      if (sstable_value.has_value()) {
        return sstable_value;
      }
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

void Engine::compact_level_zero() {}

} // namespace kv
