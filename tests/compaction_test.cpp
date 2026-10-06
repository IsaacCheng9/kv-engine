#include "compaction.hpp"
#include "sstable_reader.hpp"
#include "sstable_writer.hpp"
#include "test_utils.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <vector>

namespace kv {

namespace {

TEST(CompactionTest, NewSSTableAddsNewKeys) {
  const test::TempDir dir;
  const std::string older_path = dir.file("older");
  const std::string newer_path = dir.file("newer");
  const std::string output_path = dir.file("output");
  {
    SSTableWriter older_writer(older_path);
    SSTableWriter newer_writer(newer_path);
    older_writer.add_entry("key1", "value1");
    older_writer.add_entry("key2", "value2");
    older_writer.finalise();
    newer_writer.add_entry("key3", "value3");
    newer_writer.add_entry("key4", "value4");
    newer_writer.finalise();
  }

  compact_sstables(older_path, newer_path, output_path);
  EXPECT_TRUE(std::filesystem::exists(output_path));
  auto output_reader = SSTableReader(output_path);
  // All four distinct keys should be present.
  EXPECT_EQ(output_reader.get("key1"), "value1");
  EXPECT_EQ(output_reader.get("key2"), "value2");
  EXPECT_EQ(output_reader.get("key3"), "value3");
  EXPECT_EQ(output_reader.get("key4"), "value4");
}

TEST(CompactionTest, NewSSTableOverridesOlderSSTableForSameKeys) {
  const test::TempDir dir;
  const std::string older_path = dir.file("older");
  const std::string newer_path = dir.file("newer");
  const std::string output_path = dir.file("output");
  {
    SSTableWriter older_writer(older_path);
    SSTableWriter newer_writer(newer_path);
    older_writer.add_entry("key1", "value1");
    older_writer.add_entry("key2", "value2");
    older_writer.finalise();
    newer_writer.add_entry("key1", "value3");
    newer_writer.add_entry("key2", "value4");
    newer_writer.finalise();
  }

  compact_sstables(older_path, newer_path, output_path);
  EXPECT_TRUE(std::filesystem::exists(output_path));
  auto output_reader = SSTableReader(output_path);
  // The newer values from the newer SSTable should be present.
  EXPECT_EQ(output_reader.get("key1"), "value3");
  EXPECT_EQ(output_reader.get("key2"), "value4");
}

TEST(CompactionTest, NewSSTableTombstonesShadowOlderValues) {
  // Tombstones in the newer SSTable must be PRESERVED in the merged
  // output, not dropped. Without this, an older L1 file containing the
  // pre-delete value would surface the deleted key on read (since the
  // engine has no L2+ for the tombstone to drain into).
  const test::TempDir dir;
  const std::string older_path = dir.file("older");
  const std::string newer_path = dir.file("newer");
  const std::string output_path = dir.file("output");
  {
    SSTableWriter older_writer(older_path);
    SSTableWriter newer_writer(newer_path);
    older_writer.add_entry("key1", "value1");
    older_writer.add_entry("key2", "value2");
    older_writer.finalise();
    newer_writer.add_entry("key1", std::nullopt);
    newer_writer.add_entry("key2", std::nullopt);
    newer_writer.finalise();
  }

  compact_sstables(older_path, newer_path, output_path);
  EXPECT_TRUE(std::filesystem::exists(output_path));
  auto output_reader = SSTableReader(output_path);
  // Both keys present in the output AS tombstones (outer optional has
  // value = "key is in this file"; inner optional is nullopt = tombstone).
  auto result1 = output_reader.get("key1");
  ASSERT_TRUE(result1.has_value()) << "key1 should be in the output";
  EXPECT_FALSE(result1->has_value()) << "key1 should be a tombstone";
  auto result2 = output_reader.get("key2");
  ASSERT_TRUE(result2.has_value()) << "key2 should be in the output";
  EXPECT_FALSE(result2->has_value()) << "key2 should be a tombstone";
}

TEST(CompactionTest, DifferentKeyRangesWithInterleavingKeysIsOrdered) {
  const test::TempDir dir;
  const std::string older_path = dir.file("older");
  const std::string newer_path = dir.file("newer");
  const std::string output_path = dir.file("output");
  {
    SSTableWriter older_writer(older_path);
    SSTableWriter newer_writer(newer_path);
    older_writer.add_entry("key1", "value1");
    older_writer.add_entry("key3", "value3");
    older_writer.add_entry("key5", "value5");
    older_writer.finalise();
    newer_writer.add_entry("key2", "value2");
    newer_writer.add_entry("key4", "value4");
    newer_writer.add_entry("key6", "value6");
    newer_writer.finalise();
  }

  compact_sstables(older_path, newer_path, output_path);
  EXPECT_TRUE(std::filesystem::exists(output_path));

  // Walk the output file in sorted order and verify all keys appear in the
  // expected order.
  auto output_reader = SSTableReader(output_path);
  output_reader.seek_to_first();
  std::vector<std::string> keys;
  std::string key;
  std::optional<std::string> value;
  while (output_reader.next_entry(key, value)) {
    keys.push_back(key);
  }
  std::vector<std::string> expected{"key1", "key2", "key3",
                                    "key4", "key5", "key6"};
  EXPECT_EQ(keys, expected);
}
} // namespace
} // namespace kv
