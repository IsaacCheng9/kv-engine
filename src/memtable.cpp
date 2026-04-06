#include "memtable.hpp"
#include <map>
#include <optional>
#include <string>

namespace kv {

void Memtable::put(const std::string &key, const std::string &value) {
  data_[key] = value;
}

std::optional<std::string> Memtable::get(const std::string &key) const {
  auto it = data_.find(key);
  if (it != data_.end()) {
    return it->second;
  }
  return std::nullopt;
}

void Memtable::remove(const std::string &key) { data_[key] = std::nullopt; }
} // namespace kv
