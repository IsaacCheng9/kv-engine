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

  void append(std::string_view data);
  std::vector<std::string> read_all();

private:
  int fd_;
};
} // namespace kv

#endif
