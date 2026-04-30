#ifndef KV_ENGINE_IO_UTILS_HPP
#define KV_ENGINE_IO_UTILS_HPP

#include <cstddef>
#include <sys/types.h>

namespace kv {

// Loops on partial writes; retries on EINTR. Throws `std::system_error`
// (with errno) on other errors, or `std::runtime_error` if `write()` returns
// 0 with no progress.
void write_all(int fd, const void *buf, std::size_t count);

// Loops on partial reads; retries on EINTR. Returns the actual bytes read,
// which may be less than `count` on EOF. Throws `std::system_error` (with
// errno) on other errors.
[[nodiscard]] std::size_t read_all(int fd, void *buf, std::size_t count);

// Loops on partial positioned reads; retries on EINTR. Throws
// `std::system_error` (with errno) on errors, or `std::runtime_error` if EOF
// is hit before `count` bytes have been read (indicates a truncated or
// corrupt file).
void pread_exact(int fd, void *buf, std::size_t count, off_t offset);

} // namespace kv

#endif
