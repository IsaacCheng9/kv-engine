#include "log_file.hpp"
#include "test_utils.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace kv {

namespace {

TEST(LogFileTest, ConstructionWithValidPath) {
  const test::TempDir dir;
  const std::string path = dir.file("log_file_test");
  EXPECT_NO_THROW(LogFile log_file(path));
  EXPECT_TRUE(std::filesystem::exists(path));
}

TEST(LogFileTest, ConstructionWithInvalidPath) {
  EXPECT_THROW(LogFile log_file("/non_existent/dir/file.log"),
               std::runtime_error);
}

TEST(LogFileTest, AppendedEntriesAreReadBack) {
  const test::TempDir dir;
  const std::string path = dir.file("log_file");
  LogFile log_file(path);
  EXPECT_NO_THROW(log_file.append("entry1"));
  EXPECT_NO_THROW(log_file.append("entry2"));
  EXPECT_NO_THROW(log_file.append("entry3"));

  std::vector<std::string> actual_entries = log_file.read_entries();
  std::vector<std::string> expected_entries{"entry1", "entry2", "entry3"};
  EXPECT_EQ(actual_entries, expected_entries);
}

TEST(LogFileTest, ReadsEmptyVectorForEmptyLogs) {
  const test::TempDir dir;
  const std::string path = dir.file("log_file_empty");
  LogFile log_file(path);
  std::vector<std::string> actual_entries = log_file.read_entries();
  EXPECT_TRUE(actual_entries.empty());
}
} // namespace
} // namespace kv
