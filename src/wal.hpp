#ifndef KV_ENGINE_WAL_HPP
#define KV_ENGINE_WAL_HPP

#include <string>
#include <string_view>

namespace kv {

class Memtable;

class WAL {
public:
  explicit WAL(const std::string &path);
  ~WAL();

  // Non-copyable and non-movable: owns a raw file descriptor, so a copy or
  // move would double-close on destruction.
  WAL(const WAL &) = delete;
  WAL &operator=(const WAL &) = delete;
  WAL(WAL &&) = delete;
  WAL &operator=(WAL &&) = delete;

  void log_put(std::string_view key, std::string_view value);
  void log_remove(std::string_view key);
  void replay(Memtable &memtable);
  void clear();

private:
  int fd_;
};
} // namespace kv

#endif
