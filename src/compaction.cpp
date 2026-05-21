#include "compaction.hpp"
#include "sstable_reader.hpp"
#include "sstable_writer.hpp"
#include <fcntl.h>
#include <optional>
#include <string>
#include <unistd.h>

namespace kv {

struct ReaderState {
  SSTableReader reader;
  std::string key;
  std::optional<std::string> value;
  bool has_entry; // True if key/value are populated.

  void advance() { has_entry = reader.next_entry(key, value); }
};

bool compact_sstables(const std::string &older_path,
                      const std::string &newer_path,
                      const std::string &output_path) {
  SSTableWriter writer(output_path);
  ReaderState older_state{SSTableReader(older_path), "", std::nullopt, false};
  ReaderState newer_state{SSTableReader(newer_path), "", std::nullopt, false};
  older_state.reader.seek_to_first();
  newer_state.reader.seek_to_first();
  older_state.advance();
  newer_state.advance();
  bool wrote_any_entry = false;

  // Convert ReaderState's std::optional<std::string> value to the
  // std::optional<std::string_view> that SSTableWriter::add_entry takes,
  // preserving tombstones (nullopt) rather than dropping them.
  auto as_sv = [](const std::optional<std::string> &v)
      -> std::optional<std::string_view> {
    return v.has_value() ? std::optional<std::string_view>(*v) : std::nullopt;
  };

  while (newer_state.has_entry || older_state.has_entry) {
    if (newer_state.has_entry && older_state.has_entry &&
        newer_state.key == older_state.key) {
      // Same key - newer shadows older. Skip the older entry entirely and
      // write the newer entry, including tombstones: a dropped tombstone
      // would leave older L1 files free to surface the deleted key on
      // read, since this engine has no L2+ to drain the tombstone into.
      writer.add_entry(newer_state.key, as_sv(newer_state.value));
      wrote_any_entry = true;
      newer_state.advance();
      older_state.advance();
    } else if (older_state.has_entry &&
               (!newer_state.has_entry || older_state.key < newer_state.key)) {
      // Older has the smaller key, or newer is exhausted.
      writer.add_entry(older_state.key, as_sv(older_state.value));
      wrote_any_entry = true;
      older_state.advance();
    } else {
      // Newer has the smaller key, or older is exhausted.
      writer.add_entry(newer_state.key, as_sv(newer_state.value));
      wrote_any_entry = true;
      newer_state.advance();
    }
  }

  writer.finalise();
  return wrote_any_entry;
}
} // namespace kv
