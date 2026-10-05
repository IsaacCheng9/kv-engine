#ifndef KV_ENGINE_TESTS_TEST_UTILS_HPP
#define KV_ENGINE_TESTS_TEST_UTILS_HPP

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
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

} // namespace kv::test

#endif // KV_ENGINE_TESTS_TEST_UTILS_HPP
