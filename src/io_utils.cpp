#include "io_utils.hpp"
#include <cerrno>
#include <cstddef>
#include <stdexcept>
#include <system_error>
#include <unistd.h>

namespace kv {

void write_all(int fd, const void *buf, std::size_t count) {
  const char *ptr = static_cast<const char *>(buf);
  std::size_t remaining_write = count;

  while (remaining_write > 0) {
    ssize_t result = ::write(fd, ptr, remaining_write);
    if (result == -1) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(), "write failed");
    }
    if (result == 0) {
      throw std::runtime_error("write failed: wrote 0 bytes");
    }

    ptr += result;
    remaining_write -= static_cast<std::size_t>(result);
  }
}

std::size_t read_all(int fd, void *buf, std::size_t count) {
  char *ptr = static_cast<char *>(buf);
  std::size_t total_read = 0;

  while (total_read < count) {
    ssize_t result = ::read(fd, ptr, count - total_read);
    if (result == -1) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(), "read failed");
    }
    if (result == 0) {
      break; // EOF
    }

    ptr += result;
    total_read += static_cast<std::size_t>(result);
  }

  return total_read;
}

void pread_exact(int fd, void *buf, std::size_t count, off_t offset) {
  char *ptr = static_cast<char *>(buf);
  std::size_t total_read = 0;

  while (total_read < count) {
    ssize_t result = ::pread(fd, ptr, count - total_read,
                             offset + static_cast<off_t>(total_read));
    if (result == -1) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(), "pread failed");
    }
    if (result == 0) {
      throw std::runtime_error("pread_exact: unexpected EOF");
    }

    ptr += result;
    total_read += static_cast<std::size_t>(result);
  }
}

} // namespace kv
