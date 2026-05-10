#ifndef KV_MEMTABLE_HPP
#define KV_MEMTABLE_HPP

#include <map>
#include <optional>
#include <shared_mutex>
#include <string>

namespace kv {

class Memtable {
public:
  explicit Memtable(std::size_t max_size = 4 * 1024 * 1024);

  void put(const std::string &key, const std::string &value);
  [[nodiscard]] std::optional<std::string> get(const std::string &key) const;
  void remove(const std::string &key);
  void clear();

  // Iteration is NOT internally synchronised. The previous version took a
  // shared_lock here but released it at function return, leaving the
  // returned iterator unprotected against concurrent writers - the lock
  // was guarding nothing. Callers must externally exclude writers for the
  // iteration's lifetime (e.g. Engine::flush_if_full holds write_mutex_
  // while iterating to write an SSTable). For callers without an
  // exclusion guarantee, use snapshot() instead.
  [[nodiscard]] auto begin() const { return data_.begin(); }
  [[nodiscard]] auto end() const { return data_.end(); }
  [[nodiscard]] std::size_t size() const {
    std::shared_lock lock(mutex_);
    return data_.size();
  }
  [[nodiscard]] bool is_full() const {
    std::shared_lock lock(mutex_);
    return current_size_ >= max_size_;
  }
  [[nodiscard]] std::map<std::string, std::optional<std::string>>
  snapshot() const;

private:
  std::map<std::string, std::optional<std::string>> data_;
  std::size_t max_size_;
  std::size_t current_size_ = 0;
  mutable std::shared_mutex mutex_;
};
} // namespace kv

#endif
