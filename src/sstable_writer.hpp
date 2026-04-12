#ifndef KV_ENGINE_SSTABLE_WRITER_HPP
#define KV_ENGINE_SSTABLE_WRITER_HPP

#include <string>

namespace kv {

class Memtable;

class SSTableWriter {
public:
  explicit SSTableWriter(const std::string &path);
  ~SSTableWriter();

  void write_memtable(const Memtable &memtable);

private:
  int fd_;
};
} // namespace kv

#endif
