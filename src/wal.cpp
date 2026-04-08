#include "wal.hpp"
#include <cstdint>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>

namespace kv {

WAL::WAL(const std::string &path) {
  fd_ = open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  // May fail due to invalid path, no permissions, etc.
  if (fd_ == -1) {
    throw std::runtime_error("Failed to open file: " + path);
  }
}

WAL::~WAL() { close(fd_); }

void WAL::log_put(std::string_view key, std::string_view value) {
  static constexpr uint8_t put_op = 0x01;
  auto put_op_bytes_written = write(fd_, &put_op, sizeof(put_op));
  if (put_op_bytes_written == -1) {
    throw std::runtime_error("Failed to write operation type byte for log put");
  }

  uint32_t key_length_prefix = key.size();
  auto key_length_bytes_written =
      write(fd_, &key_length_prefix, sizeof(key_length_prefix));
  if (key_length_bytes_written == -1) {
    throw std::runtime_error("Failed to write length prefix for key");
  }
  auto key_data_bytes_written = write(fd_, key.data(), key.size());
  if (key_data_bytes_written == -1) {
    throw std::runtime_error("Failed to write data for key");
  }

  uint32_t value_length_prefix = value.size();
  auto value_length_bytes_written =
      write(fd_, &value_length_prefix, sizeof(value_length_prefix));
  if (value_length_bytes_written == -1) {
    throw std::runtime_error("Failed to write length prefix for value");
  }
  auto value_data_bytes_written = write(fd_, value.data(), value.size());
  if (value_data_bytes_written == -1) {
    throw std::runtime_error("Failed to write data for value");
  }

  fsync(fd_);
}

void WAL::log_remove(std::string_view key) {
  static constexpr uint8_t remove_op = 0x02;
  auto remove_op_bytes_written = write(fd_, &remove_op, sizeof(remove_op));
  if (remove_op_bytes_written == -1) {
    throw std::runtime_error(
        "Failed to write operation type byte for log remove");
  }

  uint32_t key_length_prefix = key.size();
  auto key_length_bytes_written =
      write(fd_, &key_length_prefix, sizeof(key_length_prefix));
  if (key_length_bytes_written == -1) {
    throw std::runtime_error("Failed to write length prefix for key");
  }
  auto key_data_bytes_written = write(fd_, key.data(), key.size());
  if (key_data_bytes_written == -1) {
    throw std::runtime_error("Failed to write data for key");
  }

  fsync(fd_);
}

} // namespace kv
