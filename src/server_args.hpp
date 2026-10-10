#ifndef KV_ENGINE_SERVER_ARGS_HPP
#define KV_ENGINE_SERVER_ARGS_HPP

#include <expected>
#include <span>
#include <string>
#include <string_view>

namespace kv {

struct ServerArgs {
  std::string data_dir;
  int port = 50051;
};

// Parse kv_engine_server's command-line arguments, excluding the program name.
// On failure, return a message suitable for printing to stderr.
[[nodiscard]] std::expected<ServerArgs, std::string>
parse_server_args(std::span<const std::string_view> args);

} // namespace kv

#endif // KV_ENGINE_SERVER_ARGS_HPP
