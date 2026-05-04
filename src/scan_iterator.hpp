#ifndef KV_ENGINE_SCAN_ITERATOR_HPP
#define KV_ENGINE_SCAN_ITERATOR_HPP

#include "sstable_reader.hpp"
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

namespace kv {

// Iterator over a snapshot of (memtable + SSTables) yielding live (key, value)
// pairs in sorted key order over [start_key, end_key). Tombstones are skipped;
// shadowed older versions of a key are discarded in favour of the newest.
// The snapshot is captured at construction - later writes / flushes /
// compactions don't affect what the iterator yields.
class ScanIterator {
public:
  // Memtable snapshot and pre-opened SSTable readers are moved in. The
  // caller (Engine::scan) takes the snapshot and opens the readers under
  // state_mutex_ so paths can't be unlinked by compaction between listing
  // them and opening them. sstable_readers must be in newest-first order
  // across levels (the memtable is implicitly newer than all of them) and
  // each must already be seeked to its first entry. limit = 0 means
  // unbounded.
  ScanIterator(
      std::map<std::string, std::optional<std::string>> memtable_snapshot,
      std::vector<std::unique_ptr<SSTableReader>> sstable_readers,
      std::string start_key, std::string end_key, uint32_t limit);

  // Returns the next live (key, value) pair, or nullopt when exhausted.
  std::optional<std::pair<std::string, std::string>> next();

private:
  // One in-flight key from one source. Heap is ordered min-key, then
  // newest-source on ties (lower source_index = newer).
  struct HeapEntry {
    std::string key;
    std::optional<std::string> value; // nullopt = tombstone
    std::size_t source_index;         // 0 = memtable, 1+i = sstable_cursors_[i]
  };

  struct HeapEntryCompare {
    bool operator()(const HeapEntry &a, const HeapEntry &b) const {
      if (a.key != b.key)
        return a.key > b.key;
      return a.source_index > b.source_index;
    }
  };

  // Pulls the next entry from the given source and pushes it onto the heap
  // if any remain.
  void advance_source(std::size_t source_index);

  std::map<std::string, std::optional<std::string>> memtable_snapshot_;
  std::map<std::string, std::optional<std::string>>::const_iterator
      memtable_cursor_;

  // Each SSTable cursor owns a fresh reader (not borrowed from the engine's
  // reader cache) so the iterator's sequential read_position_ doesn't race
  // with concurrent point lookups via the cached reader.
  std::vector<std::unique_ptr<SSTableReader>> sstable_cursors_;

  std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapEntryCompare>
      heap_;

  std::string start_key_;
  std::string end_key_;
  uint32_t limit_;
  uint32_t yielded_ = 0;
  bool exhausted_ = false;
};

} // namespace kv

#endif
