#include "io_utils.hpp"
#include <cstddef>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace kv {

namespace {

TEST(IOUtilsTest, WriteAllAndReadAllRoundTrip) {
  const std::string path = "/tmp/kv_io_utils_round_trip";
  std::remove(path.c_str());

  const std::string payload = "hello, world";

  int write_fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
  ASSERT_NE(write_fd, -1);
  EXPECT_NO_THROW(write_all(write_fd, payload.data(), payload.size()));
  close(write_fd);

  int read_fd = open(path.c_str(), O_RDONLY);
  ASSERT_NE(read_fd, -1);
  std::string buf(payload.size(), '\0');
  std::size_t bytes_read = 0;
  EXPECT_NO_THROW(bytes_read = read_all(read_fd, buf.data(), buf.size()));
  close(read_fd);

  EXPECT_EQ(bytes_read, payload.size());
  EXPECT_EQ(buf, payload);
  std::remove(path.c_str());
}

TEST(IOUtilsTest, ReadAllReturnsFewerBytesAtEof) {
  // File has 8 bytes; ask for 16, should get 8 back without throwing.
  const std::string path = "/tmp/kv_io_utils_eof";
  std::remove(path.c_str());

  const std::string payload = "12345678";

  int write_fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
  ASSERT_NE(write_fd, -1);
  write_all(write_fd, payload.data(), payload.size());
  close(write_fd);

  int read_fd = open(path.c_str(), O_RDONLY);
  ASSERT_NE(read_fd, -1);
  std::vector<char> buf(16);
  std::size_t bytes_read = read_all(read_fd, buf.data(), buf.size());
  close(read_fd);

  EXPECT_EQ(bytes_read, payload.size());
  std::remove(path.c_str());
}

TEST(IOUtilsTest, PReadExactReadsFromOffset) {
  const std::string path = "/tmp/kv_io_utils_pread_offset";
  std::remove(path.c_str());

  const std::string payload = "abcdefgh";

  int write_fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
  ASSERT_NE(write_fd, -1);
  write_all(write_fd, payload.data(), payload.size());
  close(write_fd);

  int read_fd = open(path.c_str(), O_RDONLY);
  ASSERT_NE(read_fd, -1);
  std::string buf(3, '\0');
  EXPECT_NO_THROW(pread_exact(read_fd, buf.data(), buf.size(), 2));
  close(read_fd);

  EXPECT_EQ(buf, "cde");
  std::remove(path.c_str());
}

TEST(IOUtilsTest, PReadExactThrowsOnEof) {
  const std::string path = "/tmp/kv_io_utils_pread_eof";
  std::remove(path.c_str());

  const std::string payload = "12345678";

  int write_fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
  ASSERT_NE(write_fd, -1);
  write_all(write_fd, payload.data(), payload.size());
  close(write_fd);

  int read_fd = open(path.c_str(), O_RDONLY);
  ASSERT_NE(read_fd, -1);
  std::vector<char> buf(6);
  EXPECT_THROW(pread_exact(read_fd, buf.data(), buf.size(), 5),
               std::runtime_error);
  close(read_fd);

  std::remove(path.c_str());
}

TEST(IOUtilsTest, ReadAllReturnsZeroOnEmptyFile) {
  const std::string path = "/tmp/kv_io_utils_empty";
  std::remove(path.c_str());

  int fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
  ASSERT_NE(fd, -1);
  close(fd);

  int read_fd = open(path.c_str(), O_RDONLY);
  ASSERT_NE(read_fd, -1);
  std::vector<char> buf(8);
  std::size_t bytes_read = read_all(read_fd, buf.data(), buf.size());
  close(read_fd);

  EXPECT_EQ(bytes_read, 0u);
  std::remove(path.c_str());
}

TEST(IOUtilsTest, WriteAllZeroBytesIsNoOp) {
  const std::string path = "/tmp/kv_io_utils_write_zero";
  std::remove(path.c_str());

  int fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
  ASSERT_NE(fd, -1);
  EXPECT_NO_THROW(write_all(fd, nullptr, 0));
  close(fd);

  EXPECT_EQ(std::filesystem::file_size(path), 0u);
  std::remove(path.c_str());
}

TEST(IOUtilsTest, ReadAllZeroBytesReturnsZero) {
  const std::string path = "/tmp/kv_io_utils_read_zero";
  std::remove(path.c_str());

  int fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
  ASSERT_NE(fd, -1);
  write_all(fd, "data", 4);
  close(fd);

  int read_fd = open(path.c_str(), O_RDONLY);
  ASSERT_NE(read_fd, -1);
  std::size_t bytes_read = 1; // sentinel.
  EXPECT_NO_THROW(bytes_read = read_all(read_fd, nullptr, 0));
  close(read_fd);

  EXPECT_EQ(bytes_read, 0u);
  std::remove(path.c_str());
}

TEST(IOUtilsTest, WriteAllThrowsSystemErrorOnClosedFd) {
  const std::string path = "/tmp/kv_io_utils_write_closed";
  std::remove(path.c_str());

  int fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
  ASSERT_NE(fd, -1);
  close(fd);

  EXPECT_THROW(write_all(fd, "x", 1), std::system_error);
  std::remove(path.c_str());
}

TEST(IOUtilsTest, ReadAllThrowsSystemErrorOnClosedFd) {
  const std::string path = "/tmp/kv_io_utils_read_closed";
  std::remove(path.c_str());

  int fd = open(path.c_str(), O_RDONLY | O_CREAT, 0644);
  ASSERT_NE(fd, -1);
  close(fd);

  char buf[1];
  // Cast to void to satisfy [[nodiscard]] inside the EXPECT_THROW expansion.
  EXPECT_THROW((void)read_all(fd, buf, sizeof(buf)), std::system_error);
  std::remove(path.c_str());
}

TEST(IOUtilsTest, WriteAllAndReadAllLargeBuffer) {
  // 64 KiB - exercises the loop path even on systems with large kernel I/O
  // buffers, and verifies the pointer-advance arithmetic across iterations.
  const std::string path = "/tmp/kv_io_utils_large";
  std::remove(path.c_str());

  std::vector<char> payload(64 * 1024);
  for (std::size_t i = 0; i < payload.size(); ++i) {
    payload[i] = static_cast<char>(i % 256);
  }

  int write_fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
  ASSERT_NE(write_fd, -1);
  EXPECT_NO_THROW(write_all(write_fd, payload.data(), payload.size()));
  close(write_fd);

  int read_fd = open(path.c_str(), O_RDONLY);
  ASSERT_NE(read_fd, -1);
  std::vector<char> buf(payload.size());
  std::size_t bytes_read = read_all(read_fd, buf.data(), buf.size());
  close(read_fd);

  EXPECT_EQ(bytes_read, payload.size());
  EXPECT_EQ(buf, payload);
  std::remove(path.c_str());
}

} // namespace

} // namespace kv
