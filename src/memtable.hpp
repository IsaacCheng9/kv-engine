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
  std::optional<std::string> get(const std::string &key) const;
  void remove(const std::string &key);

  auto begin() const {
    std::shared_lock lock(mutex_);
    return data_.begin();
  }
  auto end() const {
    std::shared_lock lock(mutex_);
    return data_.end();
  }
  std::size_t size() const {
    std::shared_lock lock(mutex_);
    return data_.size();
  }
  bool is_full() const {
    std::shared_lock lock(mutex_);
    return current_size_ >= max_size_;
  }

private:
  std::map<std::string, std::optional<std::string>> data_;
  std::size_t max_size_;
  std::size_t current_size_ = 0;
  mutable std::shared_mutex mutex_;
};
} // namespace kv

#endif