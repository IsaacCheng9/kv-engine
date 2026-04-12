#include "memtable.hpp"
#include <map>
#include <mutex>
#include <optional>
#include <string>

namespace kv {

Memtable::Memtable(std::size_t max_size) : max_size_(max_size) {}

void Memtable::put(const std::string &key, const std::string &value) {
  std::unique_lock lock(mutex_);

  auto it = data_.find(key);
  if (it != data_.end()) {
    if (it->second.has_value()) {
      current_size_ -= it->second.value().size();
    }
  } else {
    current_size_ += key.size();
  }

  data_[key] = value;
  current_size_ += value.size();
}

std::optional<std::string> Memtable::get(const std::string &key) const {
  std::shared_lock lock(mutex_);

  auto it = data_.find(key);
  if (it != data_.end()) {
    return it->second;
  }
  return std::nullopt;
}

void Memtable::remove(const std::string &key) {
  std::unique_lock lock(mutex_);

  auto it = data_.find(key);
  if (it != data_.end() && it->second.has_value()) {
    // Key already existed, so we only need to remove the old value's size.
    current_size_ -= it->second.value().size();
  } else if (it == data_.end()) {
    // Key didn't exist before, but now we set the key with a tombstone, so we
    // need to account for the new key size.
    current_size_ += key.size();
  }

  data_[key] = std::nullopt;
}

void Memtable::clear() {
  std::unique_lock lock(mutex_);
  data_.clear();
  current_size_ = 0;
}
} // namespace kv
