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

private:
  std::map<std::string, std::optional<std::string>> data_;
};
} // namespace kv

#endif