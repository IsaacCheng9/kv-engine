#ifndef KV_MEMTABLE_HPP
#define KV_MEMTABLE_HPP

#include <map>
#include <optional>
#include <string>

namespace kv {

class Memtable {
public:
  void put(const std::string &key, const std::string &value);
  std::optional<std::string> get(const std::string &key) const;
  void remove(const std::string &key);
  auto begin() const { return data_.begin(); }
  auto end() const { return data_.end(); }
  std::size_t size() const { return data_.size(); }

private:
  std::map<std::string, std::optional<std::string>> data_;
};
} // namespace kv

#endif