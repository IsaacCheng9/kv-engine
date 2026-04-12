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

void compact_sstables(const std::string &older_path,
                      const std::string &newer_path,
                      const std::string &output_path) {
  SSTableWriter writer(output_path);
  ReaderState older_state{SSTableReader(older_path), "", std::nullopt, false};
  ReaderState newer_state{SSTableReader(newer_path), "", std::nullopt, false};
  older_state.reader.seek_to_first();
  newer_state.reader.seek_to_first();
  older_state.advance();
  newer_state.advance();

  while (newer_state.has_entry || older_state.has_entry) {
    if (newer_state.has_entry && older_state.has_entry &&
        newer_state.key == older_state.key) {
      // Same key - newer shadows older. Skip the older entry entirely.
      if (newer_state.value.has_value()) {
        writer.add_entry(newer_state.key, std::string_view(*newer_state.value));
      }
      newer_state.advance();
      older_state.advance();
    } else if (older_state.has_entry &&
               (!newer_state.has_entry || older_state.key < newer_state.key)) {
      // Older has the smaller key, or newer is exhausted.
      if (older_state.value.has_value()) {
        writer.add_entry(older_state.key, std::string_view(*older_state.value));
      }
      older_state.advance();
    } else {
      // Newer has the smaller key, or older is exhausted.
      if (newer_state.value.has_value()) {
        writer.add_entry(newer_state.key, std::string_view(*newer_state.value));
      }
      newer_state.advance();
    }
  }

  writer.finalise();
}
} // namespace kv
