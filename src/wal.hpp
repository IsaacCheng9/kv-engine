#ifndef KV_ENGINE_WAL_HPP
#define KV_ENGINE_WAL_HPP

#include "memtable.hpp"
#include <string>
#include <string_view>

namespace kv {

class WAL {
public:
  explicit WAL(const std::string &path);
  ~WAL();

  void log_put(std::string_view key, std::string_view value);
  void log_remove(std::string_view key);
  void replay(Memtable &memtable);

private:
  int fd_;
};
} // namespace kv

#endif
