#include "store.hpp"

namespace kv {

void Store::put(const std::string &key, const std::string &value) {
  data_[key] = value;
}

std::optional<std::string> Store::get(const std::string &key) const {
  auto iterator = data_.find(key);
  if (iterator == data_.end()) {
    return std::nullopt;
  }
  return iterator->second;
}

bool Store::remove(const std::string &key) { return data_.erase(key) > 0; }

void Store::clear() { data_.clear(); }

std::size_t Store::size() const { return data_.size(); }

bool Store::contains(const std::string &key) const {
  return data_.find(key) != data_.end();
}

std::vector<std::string> Store::keys() const {
  std::vector<std::string> all_keys;
  all_keys.reserve(data_.size());
  for (const auto &[key, _] : data_) {
    all_keys.push_back(key);
  }
  return all_keys;
}

} // namespace kv
