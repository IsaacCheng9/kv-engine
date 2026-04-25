#ifndef KV_ENGINE_LOG_FILE_HPP
#define KV_ENGINE_LOG_FILE_HPP

#include <string>
#include <string_view>
#include <vector>

namespace kv {

class LogFile {
public:
  explicit LogFile(const std::string &path);
  ~LogFile();

  // Non-copyable and non-movable: owns a raw file descriptor, so a copy or
  // move would double-close on destruction.
  LogFile(const LogFile &) = delete;
  LogFile &operator=(const LogFile &) = delete;
  LogFile(LogFile &&) = delete;
  LogFile &operator=(LogFile &&) = delete;

  void append(std::string_view data);
  std::vector<std::string> read_entries();

private:
  int fd_;
};
} // namespace kv

#endif
