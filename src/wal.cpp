#include "wal.hpp"
#include "memtable.hpp"
#include <cstdint>
#include <fcntl.h>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>
#include <zlib.h>

namespace {
uint32_t update_crc(uint32_t crc, const void *data, std::size_t size) {
  return crc32(crc, static_cast<const Bytef *>(data), size);
}
} // namespace

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

  // Detect corruption. If the process crashes mid-write (e.g. power failure),
  // a record could be partially written - the key might be there but the value
  // is truncated. During replay, the reader recomputes the CRC over the record
  // data and compares it to the stored checksum. If they don't match, the
  // record corrupt and the replay stops there.
  uint32_t checksum = 0;
  checksum = update_crc(checksum, &put_op, sizeof(put_op));
  checksum =
      update_crc(checksum, &key_length_prefix, sizeof(key_length_prefix));
  checksum = update_crc(checksum, key.data(), key.size());
  checksum =
      update_crc(checksum, &value_length_prefix, sizeof(value_length_prefix));
  checksum = update_crc(checksum, value.data(), value.size());
  auto checksum_bytes_written = write(fd_, &checksum, sizeof(checksum));
  if (checksum_bytes_written == -1) {
    throw std::runtime_error("Failed to write checksum for log put");
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

  uint32_t checksum = 0;
  checksum = update_crc(checksum, &remove_op, sizeof(remove_op));
  checksum =
      update_crc(checksum, &key_length_prefix, sizeof(key_length_prefix));
  checksum = update_crc(checksum, key.data(), key.size());
  auto checksum_bytes_written = write(fd_, &checksum, sizeof(checksum));
  if (checksum_bytes_written == -1) {
    throw std::runtime_error("Failed to write checksum for log remove");
  }

  fsync(fd_);
}

void WAL::replay(Memtable &memtable) {
  lseek(fd_, 0, SEEK_SET);

  while (true) {
    // Compute the checksum so we can compare it to the stored checksum at the
    // end of the record. This allows us to detect if a record is truncated due
    // to a crash during write.
    uint32_t checksum = 0;

    uint8_t op_type;
    auto op_type_bytes_read = read(fd_, &op_type, sizeof(op_type));
    if (op_type_bytes_read == 0) {
      break;
    }
    checksum = update_crc(checksum, &op_type, sizeof(op_type));

    uint32_t key_length;
    auto key_length_bytes_read = read(fd_, &key_length, sizeof(key_length));
    if (key_length_bytes_read == 0) {
      break;
    }
    checksum = update_crc(checksum, &key_length, sizeof(key_length));

    std::string key(key_length, '\0');
    auto key_data_bytes_read = read(fd_, key.data(), key.size());
    if (key_data_bytes_read < static_cast<ssize_t>(key_length)) {
      break;
    }
    checksum = update_crc(checksum, key.data(), key.size());

    // Only put operations include a value.
    if (op_type == 0x01) {
      uint32_t value_length;
      auto value_length_bytes_read =
          read(fd_, &value_length, sizeof(value_length));
      if (value_length_bytes_read == 0) {
        break;
      }
      checksum = update_crc(checksum, &value_length, sizeof(value_length));

      std::string value(value_length, '\0');
      auto value_data_bytes_read = read(fd_, value.data(), value.size());
      if (value_data_bytes_read < static_cast<ssize_t>(value_length)) {
        break;
      }
      checksum = update_crc(checksum, value.data(), value.size());

      // Read and verify checksum.
      uint32_t stored_checksum;
      read(fd_, &stored_checksum, sizeof(stored_checksum));
      if (checksum != stored_checksum) {
        std::cerr
            << "WAL replay: checksum mismatch, stopping at corrupt node.\n";
        break;
      }

      memtable.put(std::move(key), std::move(value));
    } else if (op_type == 0x02) {
      // Read and verify checksum.
      uint32_t stored_checksum;
      read(fd_, &stored_checksum, sizeof(stored_checksum));
      if (checksum != stored_checksum) {
        std::cerr
            << "WAL replay: checksum mismatch, stopping at corrupt node.\n";
        break;
      }

      memtable.remove(std::move(key));
    } else {
      throw std::runtime_error("Unknown operation type in WAL: " +
                               std::to_string(op_type));
    }
  }
}
} // namespace kv
