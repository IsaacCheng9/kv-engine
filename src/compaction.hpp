#ifndef KV_ENGINE_COMPACTION_HPP
#define KV_ENGINE_COMPACTION_HPP

#include <string>

namespace kv {

// Merges two sorted SSTables into a single output SSTable.
// Entries from the newer SSTable shadow entries from the older one for the
// same key. Tombstones are dropped - the key is not written to the output.
// Returns true if at least one live entry was written to the output SSTable.
bool compact_sstables(const std::string &older_path,
                      const std::string &newer_path,
                      const std::string &output_path);

} // namespace kv

#endif
