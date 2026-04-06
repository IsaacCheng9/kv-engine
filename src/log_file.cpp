#include "log_file.hpp"
#include <cstdint>
#include <fcntl.h>
#include <stdexcept>
#include <string_view>
#include <unistd.h>

namespace kv {

LogFile::LogFile(const std::string &path) {
  fd_ = open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  // May fail due to invalid path, no permissions, etc.
  if (fd_ == -1) {
    throw std::runtime_error("Failed to open file: " + path);
  }
}

LogFile::~LogFile() { close(fd_); }

void LogFile::append(std::string_view data) {
  uint32_t length_prefix = data.size();
  auto length_bytes_written = write(fd_, &length_prefix, sizeof(length_prefix));
  if (length_bytes_written == -1) {
    throw std::runtime_error("Failed to write length prefix");
  }

  auto data_bytes_written = write(fd_, data.data(), data.size());
  if (data_bytes_written == -1) {
    throw std::runtime_error("Failed to write data");
  }

  fsync(fd_);
}

} // namespace kv