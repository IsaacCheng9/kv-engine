#include "scan_iterator.hpp"
#include "sstable_reader.hpp"
#include <memory>

namespace kv {

ScanIterator::ScanIterator(
    std::map<std::string, std::optional<std::string>> memtable_snapshot,
    std::vector<std::string> sstable_paths, std::string start_key,
    std::string end_key, uint32_t limit)
    : memtable_snapshot_(std::move(memtable_snapshot)),
      start_key_(std::move(start_key)), end_key_(std::move(end_key)),
      limit_(limit) {

  // Position the memtable cursor at the first key >= start_key. lower_bound()
  // is O(log n) and gives end() if the start_key is past every key in the map.
  memtable_cursor_ = memtable_snapshot_.lower_bound(start_key_);

  // Open one fresh SSTableReader per snapshot path. Each reader's sequential
  // cursor is private to this scan, so no contention with the engine's cached
  // point-lookup readers (which use pread).
  sstable_cursors_.reserve(sstable_paths.size());
  for (const auto &path : sstable_paths) {
    auto reader = std::make_unique<SSTableReader>(path);
    reader->seek_to_first();
    sstable_cursors_.push_back(std::move(reader));
  }

  // Prime the heap with the first in-range entry from each source.
  // Source 0 = memtable
  // Sources 1 ... N = sstable_cursors_[i - 1]
  // advance_source handles the per-source skip-ahead past start_key_ for
  // SSTables.
  for (std::size_t i = 0; i <= sstable_cursors_.size(); ++i) {
    advance_source(i);
  }
}

void ScanIterator::advance_source(std::size_t source_index) {
  if (source_index == 0) {
    // Memtable source - each call pushes the current entry then steps forward.
    if (memtable_cursor_ == memtable_snapshot_.end()) {
      return;
    }
    heap_.push({memtable_cursor_->first, memtable_cursor_->second, 0});
    ++memtable_cursor_;
    return;
  }

  // SSTable source - the SSTableReader has no seek(key), so on the first
  // call we walk forward from the file's start until we hit start_key.
  // Subsequent calls just consume the next sequential entry (which is
  // already >= start_key by induction).
  auto &reader = sstable_cursors_[source_index - 1];
  std::string key;
  std::optional<std::string> value;
  while (reader->next_entry(key, value)) {
    // Walk forward until we hit the start_key_.
    if (key >= start_key_) {
      heap_.push({std::move(key), std::move(value), source_index});
      return;
    }
  }
  // If we exhausted the reader, we have nothing to push.
}

std::optional<std::pair<std::string, std::string>> ScanIterator::next() {
  if (exhausted_) {
    return std::nullopt;
  }

  while (!heap_.empty()) {
    if (limit_ != 0 && yielded_ >= limit_) {
      exhausted_ = true;
      return std::nullopt;
    }

    HeapEntry top = heap_.top();
    heap_.pop();

    // Perform a range end check. The heap is min-key-ordered, so if even the
    // smallest pending key is past the end_key, no future entry can be in the
    // range either. End is exclusive by spec, so we need >=.
    if (!end_key_.empty() && top.key >= end_key_) {
      exhausted_ = true;
      return std::nullopt;
    }

    // Replenish the source we just popped from if it has more entries.
    advance_source(top.source_index);

    // Drain the shadowed entries. Any other heap entry with the same key as the
    // top is from an older source, as the comparator's tiebreaker pops the
    // newest first, so it's dead. Pop and replenish each shadowed source so we
    // don't re-yield the same key on the next call.
    while (!heap_.empty() && heap_.top().key == top.key) {
      std::size_t shadowed_index = heap_.top().source_index;
      heap_.pop();
      advance_source(shadowed_index);
    }

    // Tombstone collapse: nullopt value means the key is deleted as of the
    // snapshot. Skip it and loop to find the next live key.
    if (!top.value.has_value()) {
      continue;
    }

    ++yielded_;
    return std::make_pair(std::move(top.key), std::move(top.value.value()));
  }

  exhausted_ = true;
  return std::nullopt;
}

} // namespace kv
