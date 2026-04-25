#include "log_file.hpp"
#include "io_utils.hpp"
#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <format>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>

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
  write_all(fd_, &length_prefix, sizeof(length_prefix));
  write_all(fd_, data.data(), data.size());
  fsync(fd_);
}

std::vector<std::string> LogFile::read_entries() {
  lseek(fd_, 0, SEEK_SET);
  std::vector<std::string> entries;

  while (true) {
    uint32_t length = 0;
    if (read_all(fd_, &length, sizeof(length)) < sizeof(length)) {
      break;
    }

    std::string entry(length, '\0');
    if (read_all(fd_, entry.data(), entry.size()) < entry.size()) {
      throw std::runtime_error(
          std::format("Truncated read: expected {} bytes", length));
    }
    entries.push_back(std::move(entry));
  }

  return entries;
}

} // namespace kv
