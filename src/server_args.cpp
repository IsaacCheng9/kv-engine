#include "server_args.hpp"
#include <charconv>
#include <cstddef>
#include <format>
#include <optional>
#include <system_error>

namespace kv {

namespace {

constexpr std::string_view usage =
    "Usage: kv_engine_server --data-dir <path> [--port <port>]";

// Accept only a whole decimal string in the valid TCP port range; std::stoi
// would accept trailing junk such as "80abc" and throw on non-numeric input.
std::optional<int> parse_port(std::string_view text) {
  int port = 0;
  const auto *end = text.data() + text.size();
  const auto [ptr, ec] = std::from_chars(text.data(), end, port);
  if (ec != std::errc{} || ptr != end || port < 1 || port > 65535) {
    return std::nullopt;
  }
  return port;
}

} // namespace

std::expected<ServerArgs, std::string>
parse_server_args(std::span<const std::string_view> args) {
  ServerArgs result;
  for (std::size_t i = 0; i < args.size(); ++i) {
    const auto arg = args[i];
    if (arg != "--data-dir" && arg != "--port") {
      return std::unexpected(
          std::format("Unknown argument: {}\n{}", arg, usage));
    }
    if (i + 1 == args.size()) {
      return std::unexpected(
          std::format("Missing value for {}\n{}", arg, usage));
    }
    const auto value = args[++i];
    if (arg == "--data-dir") {
      result.data_dir = value;
    } else if (const auto port = parse_port(value)) {
      result.port = *port;
    } else {
      return std::unexpected(std::format(
          "Invalid port '{}': expected an integer from 1 to 65535", value));
    }
  }
  if (result.data_dir.empty()) {
    return std::unexpected(std::string(usage));
  }
  return result;
}

} // namespace kv
