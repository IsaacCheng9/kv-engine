#ifndef KV_ENGINE_TESTS_TEST_UTILS_HPP
#define KV_ENGINE_TESTS_TEST_UTILS_HPP

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>

namespace kv::test {

// Owns a uniquely named directory under the system temp directory and removes
// it, with everything inside, on destruction. mkdtemp(3) creates the
// directory atomically with a random suffix, so parallel ctest processes,
// concurrent sanitiser builds, and leftovers from interrupted runs can never
// share or collide with a test's directory.
class TempDir {
public:
  explicit TempDir(std::string_view prefix = "kv_engine_test") {
    auto pattern = (std::filesystem::temp_directory_path() /
                    (std::string(prefix) + "_XXXXXX"))
                       .string();
    if (mkdtemp(pattern.data()) == nullptr) {
      throw std::system_error(errno, std::generic_category(),
                              "mkdtemp failed for " + pattern);
    }
    path_ = pattern;
  }

  ~TempDir() {
    if (!path_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(path_, ec);
    }
  }

  TempDir(const TempDir &) = delete;
  TempDir &operator=(const TempDir &) = delete;

  // Moving transfers ownership; the moved-from object removes nothing.
  TempDir(TempDir &&other) noexcept : path_(std::exchange(other.path_, {})) {}
  TempDir &operator=(TempDir &&other) noexcept {
    std::swap(path_, other.path_);
    return *this;
  }

  [[nodiscard]] const std::filesystem::path &path() const { return path_; }

  // Return the path of `name` inside the directory, for file-level tests.
  [[nodiscard]] std::string file(std::string_view name) const {
    return (path_ / name).string();
  }

private:
  std::filesystem::path path_;
};

// Count the SSTable files at `level` currently on disk in `dir`.
[[nodiscard]] inline std::size_t
count_sstables(const std::filesystem::path &dir, std::size_t level) {
  const auto prefix = std::format("sstable_{}_", level);
  std::size_t count = 0;
  for (const auto &entry : std::filesystem::directory_iterator(dir)) {
    if (entry.path().filename().string().starts_with(prefix) &&
        entry.path().extension() == ".dat") {
      ++count;
    }
  }
  return count;
}

// Poll `condition` until it holds or `timeout` expires, returning whether it
// held. Fixed sleeps are flaky because background work slows down under
// sanitiser instrumentation (TSan is 5-10x slower than ASan) and CI load.
template <typename Condition>
[[nodiscard]] bool
wait_until(Condition condition,
           std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (condition()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return condition();
}

// Wait until an L0-to-L1 compaction has fully completed, leaving at least
// `min_l1_files` L1 SSTables. The engine renames its L1 output into place
// before publishing it and unlinks the L0 inputs and temporary files only
// afterwards, so an L1 file's appearance does not mean the compaction is
// visible to reads. An empty level 0 with no temporary files does. Only use
// this when every L0 file will be compacted - after a multiple of four
// flushes with no further writes.
[[nodiscard]] inline bool wait_for_compaction(const std::filesystem::path &dir,
                                              std::size_t min_l1_files = 1) {
  return wait_until([&] {
    if (count_sstables(dir, 0) != 0 || count_sstables(dir, 1) < min_l1_files) {
      return false;
    }
    for (const auto &entry : std::filesystem::directory_iterator(dir)) {
      if (entry.path().filename().string().starts_with("compact_tmp_")) {
        return false;
      }
    }
    return true;
  });
}

} // namespace kv::test

#endif // KV_ENGINE_TESTS_TEST_UTILS_HPP
