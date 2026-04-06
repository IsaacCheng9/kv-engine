#ifndef KV_ENGINE_STORE_HPP
#define KV_ENGINE_STORE_HPP

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
namespace kv {

/// In-memory key-value store. This is a placeholder that will be replaced
/// with an LSM-tree storage engine.
class Store {
public:
  void put(const std::string &key, const std::string &value);
  std::optional<std::string> get(const std::string &key) const;
  bool remove(const std::string &key);
  void clear();
  std::size_t size() const;
  bool contains(const std::string &key) const;
  std::vector<std::string> keys() const;

private:
  std::unordered_map<std::string, std::string> data_;
};

} // namespace kv

#endif // KV_ENGINE_STORE_HPP
