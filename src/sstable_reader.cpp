#include "sstable_reader.hpp"
#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <sys/fcntl.h>
#include <sys/types.h>
#include <unistd.h>

namespace kv {

SSTableReader::SSTableReader(const std::string &path) {
  fd_ = open(path.c_str(), O_RDONLY, 0644);
  // May fail due to invalid path, no permissions, etc.
  if (fd_ == -1) {
    throw std::runtime_error("Failed to open file: " + path);
  }

  // The SSTable file layout is:
  // [data entries][index block][footer (8 bytes = offset of index block)]
  // To load the index, read the footer first to find where the index starts,
  // then walk the index entries until we hit the footer position.
  auto file_size = lseek(fd_, 0, SEEK_END);
  lseek(fd_, file_size - sizeof(uint64_t), SEEK_SET);
  uint64_t index_offset;
  read(fd_, &index_offset, sizeof(index_offset));

  lseek(fd_, index_offset, SEEK_SET);
  while (lseek(fd_, 0, SEEK_CUR) <
         file_size - static_cast<off_t>(sizeof(uint64_t))) {
    uint32_t key_length;
    auto key_length_bytes_read = read(fd_, &key_length, sizeof(key_length));
    if (key_length_bytes_read == 0) {
      break;
    }

    std::string key(key_length, '\0');
    auto key_data_bytes_read = read(fd_, key.data(), key.size());
    if (key_data_bytes_read < static_cast<ssize_t>(key_length)) {
      break;
    }

    uint64_t data_offset;
    auto data_offset_bytes_read = read(fd_, &data_offset, sizeof(data_offset));
    if (data_offset_bytes_read == 0) {
      break;
    }

    index_.emplace_back(std::move(key), data_offset);
  }
}

SSTableReader::~SSTableReader() { close(fd_); }

std::optional<std::string> SSTableReader::get(std::string_view key) const {
  // Find the key from the index using binary search.
  auto iterator = std::lower_bound(
      index_.begin(), index_.end(), key,
      [](const auto &entry, const auto &key) { return entry.first < key; });
  if (iterator == index_.end() || iterator->first != key) {
    return std::nullopt;
  }

  uint64_t data_offset = iterator->second;
  lseek(fd_, data_offset, SEEK_SET);

  // Skip the key.
  uint32_t key_length;
  read(fd_, &key_length, sizeof(key_length));
  lseek(fd_, key_length, SEEK_CUR);
  // Read the value if it exists.
  uint32_t value_length;
  read(fd_, &value_length, sizeof(value_length));
  // Key was deleted, so a tombstone marker is written instead of the value.
  if (value_length == UINT32_MAX) {
    return std::nullopt;
  }
  std::string value(value_length, '\0');
  read(fd_, value.data(), value_length);

  return value;
}
} // namespace kv
