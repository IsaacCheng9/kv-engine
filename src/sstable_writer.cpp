#include "sstable_writer.hpp"
#include "memtable.hpp"
#include <cstdint>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace kv {

SSTableWriter::SSTableWriter(const std::string &path) {
  fd_ = open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
  // May fail due to invalid path, no permissions, etc.
  if (fd_ == -1) {
    throw std::runtime_error("Failed to open file: " + path);
  }
}

SSTableWriter::~SSTableWriter() { close(fd_); }

void SSTableWriter::add_entry(std::string_view key,
                              std::optional<std::string_view> value) {
  uint64_t offset = lseek(fd_, 0, SEEK_CUR);

  uint32_t key_length = key.size();
  auto key_length_bytes_written = write(fd_, &key_length, sizeof(key_length));
  if (key_length_bytes_written == -1) {
    throw std::runtime_error("Failed to write length prefix for key");
  }
  auto key_data_bytes_written = write(fd_, key.data(), key.size());
  if (key_data_bytes_written == -1) {
    throw std::runtime_error("Failed to write data for key");
  }

  if (value.has_value()) {
    uint32_t value_length = value->size();
    auto value_length_bytes_written =
        write(fd_, &value_length, sizeof(value_length));
    if (value_length_bytes_written == -1) {
      throw std::runtime_error("Failed to write length prefix for value");
    }
    auto value_data_bytes_written = write(fd_, value->data(), value->size());
    if (value_data_bytes_written == -1) {
      throw std::runtime_error("Failed to write data for value");
    }
  } else {
    // Use a tombstone marker to indicate that this key was deleted.
    uint32_t tombstone_marker = UINT32_MAX;
    auto bytes_written =
        write(fd_, &tombstone_marker, sizeof(tombstone_marker));
    if (bytes_written == -1) {
      throw std::runtime_error(
          "Failed to write tombstone marker for deleted key");
    }
  }

  key_to_offset_.emplace_back(key, offset);
}

void SSTableWriter::finalise() {
  uint64_t index_offset = lseek(fd_, 0, SEEK_CUR);
  for (const auto &[key, offset] : key_to_offset_) {
    uint32_t key_length = key.size();
    auto key_length_bytes_written = write(fd_, &key_length, sizeof(key_length));
    if (key_length_bytes_written == -1) {
      throw std::runtime_error("Failed to write length prefix for index key");
    }
    auto key_data_bytes_written = write(fd_, key.data(), key.size());
    if (key_data_bytes_written == -1) {
      throw std::runtime_error("Failed to write data for index key");
    }
    auto offset_bytes_written = write(fd_, &offset, sizeof(offset));
    if (offset_bytes_written == -1) {
      throw std::runtime_error("Failed to write offset for index entry");
    }
  }
  write(fd_, &index_offset, sizeof(index_offset));

  fsync(fd_);
  finalised_ = true;
}

void SSTableWriter::write_memtable(const Memtable &memtable) {
  for (const auto &[key, value] : memtable) {
    if (value.has_value()) {
      add_entry(key, std::string_view(*value));
    } else {
      add_entry(key, std::nullopt);
    }
  }
  finalise();
}
} // namespace kv
