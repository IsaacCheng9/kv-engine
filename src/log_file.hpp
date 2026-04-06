#ifndef KV_ENGINE_LOG_FILE_HPP
#define KV_ENGINE_LOG_FILE_HPP

#include <string>

namespace kv {
class LogFile {
public:
  explicit LogFile(const std::string &path);
  ~LogFile();

private:
  int fd_;
};
} // namespace kv

#endif