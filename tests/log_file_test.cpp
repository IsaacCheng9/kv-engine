#include "log_file.hpp"
#include <cstdio>
#include <filesystem>
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace kv {

namespace {

TEST(LogFileTest, ConstructionWithValidPath) {
  const std::string path = "/tmp/kv_log_file_test";
  std::remove(path.c_str());
  EXPECT_NO_THROW(LogFile log_file(path));
  EXPECT_TRUE(std::filesystem::exists(path));
  std::remove(path.c_str());
}

TEST(LogFileTest, ConstructionWithInvalidPath) {
  EXPECT_THROW(LogFile log_file("/non_existent/dir/file.log"),
               std::runtime_error);
}

TEST(LogFileTest, AppendedEntriesAreReadBack) {
  const std::string path = "/tmp/kv_log_file";
  std::remove(path.c_str());
  LogFile log_file(path);
  EXPECT_NO_THROW(log_file.append("entry1"));
  EXPECT_NO_THROW(log_file.append("entry2"));
  EXPECT_NO_THROW(log_file.append("entry3"));

  std::vector<std::string> actual_entries = log_file.read_entries();
  std::vector<std::string> expected_entries{"entry1", "entry2", "entry3"};
  EXPECT_EQ(actual_entries, expected_entries);
  std::remove(path.c_str());
}

TEST(LogFileTest, ReadsEmptyVectorForEmptyLogs) {
  const std::string path = "/tmp/kv_log_file_empty";
  std::remove(path.c_str());
  LogFile log_file(path);
  std::vector<std::string> actual_entries = log_file.read_entries();
  EXPECT_TRUE(actual_entries.empty());
  std::remove(path.c_str());
}
} // namespace
} // namespace kv
