#include "log_file.hpp"
#include <fcntl.h>
#include <stdexcept>
#include <unistd.h>

namespace kv {

LogFile::LogFile(const std::string &path) {
  fd_ = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  // May fail due to invalid path, no permissions, etc.
  if (fd_ == -1) {
    throw std::runtime_error("Failed to open file: " + path);
  }
}

LogFile::~LogFile() { close(fd_); }

} // namespace kv