#include "engine.hpp"
#include "compaction.hpp"
#include "sstable_reader.hpp"
#include "sstable_writer.hpp"
#include <algorithm>
#include <filesystem>
#include <mutex>
#include <optional>
#include <shared_mutex>
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

  // Start the compaction thread.
  compaction_thread_ = std::thread(&Engine::compaction_loop, this);
}

Engine::~Engine() {
  shutdown_ = true;
  compaction_cv_.notify_all();
  if (compaction_thread_.joinable()) {
    compaction_thread_.join();
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
    std::lock_guard<std::mutex> lock(compaction_mutex_);
    compaction_triggered_ = true;
    compaction_cv_.notify_all();
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

  // Take the shared lock to read the level_files_ state for the compaction
  // thread.
  std::shared_lock<std::shared_mutex> lock(state_mutex_);
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

void Engine::compact_level_zero() {
  // Ensure L1 exists.
  if (level_files_.size() < 2) {
    level_files_.resize(2);
    next_id_per_level_.resize(2, 0);
  }

  // Build the current L0 file paths.
  std::vector<std::string> l0_paths;
  for (auto id : level_files_[0]) {
    l0_paths.push_back(data_dir_ + "/sstable_0_" + std::to_string(id) + ".dat");
  }

  // Fold L0 into L1. Start with the oldest file and merge newer files into it
  // one at a time, writing immediate files to a temp path.
  std::string accumulator = l0_paths[0];
  std::vector<std::string> temp_paths;
  for (std::size_t i = 1; i < l0_paths.size(); ++i) {
    std::string temp_path =
        data_dir_ + "/compact_tmp_" + std::to_string(i) + ".dat";
    compact_sstables(accumulator, l0_paths[i], temp_path);
    temp_paths.push_back(temp_path);
    accumulator = temp_path;
  }

  // Move the final result to the new L1 file.
  uint64_t new_l1_id = next_id_per_level_[1]++;
  std::string l1_path =
      data_dir_ + "/sstable_1_" + std::to_string(new_l1_id) + ".dat";
  std::filesystem::rename(accumulator, l1_path);
  level_files_[1].push_back(new_l1_id);
  level_files_[0].clear();

  // Delete the original L0 files and the temp files, except the one we just
  // renamed.
  for (const auto &path : l0_paths) {
    std::filesystem::remove(path);
  }
  for (const auto &temp_path : temp_paths) {
    if (temp_path != accumulator) {
      std::filesystem::remove(temp_path);
    }
  }
}

void Engine::compaction_loop() {
  while (true) {
    // wait() releases the lock and sleeps, then re-acquires the lock when
    // notified. The predicate guards against spurious wakeups - if we wake
    // without a real signal, we go back to sleep.
    std::unique_lock<std::mutex> lock(compaction_mutex_);
    compaction_cv_.wait(lock,
                        [this] { return compaction_triggered_ || shutdown_; });
    if (shutdown_) {
      break;
    }
    compaction_triggered_ = false;

    // Release the mutex before running compaction. Compaction is slow
    // (seconds), so holding the mutex would block any signals for the next
    // compaction. The shared state it touches (level_files_) is protected
    // separately.
    lock.unlock();

    compact_level_zero();
  }
}

} // namespace kv
