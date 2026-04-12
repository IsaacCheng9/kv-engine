#include "sstable_reader.hpp"
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

} // namespace kv
