#include "sstable_reader.hpp"
#include <algorithm>
#include <cstddef>
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

  // Footer is 24 bytes: 8 bytes for index offset, 8 bytes for Bloom filter
  // offset, and 8 bytes for Bloom filter size.
  auto file_size = lseek(fd_, 0, SEEK_END);
  constexpr std::size_t footer_size = 3 * sizeof(uint64_t);
  const std::size_t footer_start = file_size - footer_size;
  uint64_t index_offset;
  uint64_t bloom_filter_offset;
  uint64_t bloom_filter_size;
  pread(fd_, &index_offset, sizeof(index_offset), footer_start);
  pread(fd_, &bloom_filter_offset, sizeof(bloom_filter_offset),
        footer_start + sizeof(uint64_t));
  pread(fd_, &bloom_filter_size, sizeof(bloom_filter_size),
        footer_start + 2 * sizeof(uint64_t));

  // Load and deserialise the Bloom filter.
  std::vector<uint8_t> bloom_filter_data(bloom_filter_size);
  pread(fd_, bloom_filter_data.data(), bloom_filter_size, bloom_filter_offset);
  bloom_filter_ = BloomFilter::deserialise(bloom_filter_data);

  // Save the offset where the index block starts.
  data_end_ = index_offset;

  std::size_t cursor = index_offset;
  while (cursor < bloom_filter_offset) {
    uint32_t key_length;
    if (pread(fd_, &key_length, sizeof(key_length), cursor) == 0) {
      break;
    }
    cursor += sizeof(key_length);

    std::string key(key_length, '\0');
    auto key_data_bytes_read = pread(fd_, key.data(), key.size(), cursor);
    if (key_data_bytes_read < static_cast<ssize_t>(key_length)) {
      break;
    }
    cursor += key.size();

    uint64_t data_offset;
    auto data_offset_bytes_read =
        pread(fd_, &data_offset, sizeof(data_offset), cursor);
    if (data_offset_bytes_read == 0) {
      break;
    }
    cursor += sizeof(data_offset);

    index_.emplace_back(std::move(key), data_offset);
  }
}

SSTableReader::~SSTableReader() { close(fd_); }

std::optional<std::optional<std::string>>
SSTableReader::get(std::string_view key) {
  // Check the Bloom filter first. If it returns false, we know for sure the key
  // doesn't exist and can skip the index lookup and disk read. If it returns
  // true, the key might exist, so we proceed with the index lookup and disk
  // read to confirm.
  if (!bloom_filter_->might_contain(key)) {
    return std::nullopt;
  }

  // Find the key from the index using binary search.
  auto iterator = std::lower_bound(
      index_.begin(), index_.end(), key,
      [](const auto &entry, const auto &key) { return entry.first < key; });
  if (iterator == index_.end() || iterator->first != key) {
    return std::nullopt;
  }

  std::size_t cursor = iterator->second;

  // Skip the key (4-byte length field + key bytes).
  uint32_t key_length;
  pread(fd_, &key_length, sizeof(key_length), cursor);
  cursor += sizeof(key_length) + key_length;

  uint32_t value_length;
  pread(fd_, &value_length, sizeof(value_length), cursor);
  cursor += sizeof(value_length);
  // Key was deleted, so a tombstone marker is written instead of the value.
  if (value_length == UINT32_MAX) {
    return std::optional<std::string>{std::nullopt};
  }
  std::string value(value_length, '\0');
  pread(fd_, value.data(), value_length, cursor);

  return std::optional<std::string>{value};
}

void SSTableReader::seek_to_first() { read_position_ = 0; }

bool SSTableReader::next_entry(std::string &out_key,
                               std::optional<std::string> &out_value) {
  if (read_position_ >= data_end_) {
    return false;
  }

  uint32_t key_length;
  pread(fd_, &key_length, sizeof(key_length), read_position_);
  read_position_ += sizeof(key_length);

  out_key.resize(key_length);
  pread(fd_, out_key.data(), key_length, read_position_);
  read_position_ += key_length;

  uint32_t value_length;
  pread(fd_, &value_length, sizeof(value_length), read_position_);
  read_position_ += sizeof(value_length);
  // Key was deleted, so a tombstone marker exists instead of the value.
  if (value_length == UINT32_MAX) {
    out_value = std::nullopt;
  } else {
    std::string value(value_length, '\0');
    pread(fd_, value.data(), value_length, read_position_);
    read_position_ += value_length;
    out_value = std::move(value);
  }

  return true;
}

const std::string &SSTableReader::min_key() const {
  return index_.front().first;
}

const std::string &SSTableReader::max_key() const {
  return index_.back().first;
}

} // namespace kv
