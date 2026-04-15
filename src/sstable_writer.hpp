#ifndef KV_ENGINE_SSTABLE_WRITER_HPP
#define KV_ENGINE_SSTABLE_WRITER_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kv {

class Memtable;

class SSTableWriter {
public:
  explicit SSTableWriter(const std::string &path);
  ~SSTableWriter();

  // Non-copyable and non-movable: owns a raw file descriptor, so a copy or
  // move would double-close on destruction.
  SSTableWriter(const SSTableWriter &) = delete;
  SSTableWriter &operator=(const SSTableWriter &) = delete;
  SSTableWriter(SSTableWriter &&) = delete;
  SSTableWriter &operator=(SSTableWriter &&) = delete;

  void add_entry(std::string_view key, std::optional<std::string_view> value);
  void finalise();

  void write_memtable(const Memtable &memtable);

private:
  int fd_;
  std::vector<std::pair<std::string, uint64_t>> key_to_offset_;
  bool finalised_ = false;
};
} // namespace kv

#endif
