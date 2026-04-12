#ifndef KV_ENGINE_SSTABLE_READER_HPP
#define KV_ENGINE_SSTABLE_READER_HPP

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

  std::optional<std::string> get(std::string_view key);
  void seek_to_first();
  bool next_entry(std::string &out_key, std::optional<std::string> &out_value);

private:
  int fd_;
  std::vector<std::pair<std::string, uint64_t>> index_;
  uint64_t data_end_; // Offset where the index block starts.
};
} // namespace kv

#endif
