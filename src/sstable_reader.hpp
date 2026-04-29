#ifndef KV_ENGINE_SSTABLE_READER_HPP
#define KV_ENGINE_SSTABLE_READER_HPP

#include "bloom_filter.hpp"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
namespace kv {

class SSTableReader {
public:
  explicit SSTableReader(const std::string &path);
  ~SSTableReader();

  // Non-copyable and non-movable: owns a raw file descriptor, so a copy or
  // move would double-close on destruction.
  SSTableReader(const SSTableReader &) = delete;
  SSTableReader &operator=(const SSTableReader &) = delete;
  SSTableReader(SSTableReader &&) = delete;
  SSTableReader &operator=(SSTableReader &&) = delete;

  // Returns std::nullopt if the key isn't in the file,
  // std::optional{std::nullopt} if the key is in the file but it's a tombstone,
  // or std::optional{std::optional{"value"}} if the key is in the file and it's
  // a live value.
  [[nodiscard]] std::optional<std::optional<std::string>>
  get(std::string_view key);
  void seek_to_first();
  [[nodiscard]] bool next_entry(std::string &out_key,
                                std::optional<std::string> &out_value);
  [[nodiscard]] const std::string &min_key() const;
  [[nodiscard]] const std::string &max_key() const;

private:
  int fd_;
  std::vector<std::pair<std::string, uint64_t>> index_;
  uint64_t data_end_; // Offset where the index block starts.
  // Sequential read cursor used by seek_to_first() / next_entry() to support
  // compaction-side iteration without relying on the kernel file position
  // (which pread-based get() calls don't touch and must not depend on).
  std::size_t read_position_ = 0;
  // Let the Bloom filter member start empty, then assign it once we have the
  // deserialised filter.
  std::optional<BloomFilter> bloom_filter_;
};
} // namespace kv

#endif
