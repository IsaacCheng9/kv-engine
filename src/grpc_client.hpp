#ifndef KV_ENGINE_GRPC_CLIENT_HPP
#define KV_ENGINE_GRPC_CLIENT_HPP

#include "kv/v1/kv.grpc.pb.h"
#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
namespace kv {

// Thrown when an RPC fails. Carries the gRPC status code so callers can tell a
// timeout (DEADLINE_EXCEEDED) or an unreachable server (UNAVAILABLE) apart from
// a rejected request (e.g. INVALID_ARGUMENT).
class KvStoreError : public std::runtime_error {
public:
  KvStoreError(const std::string &message, grpc::StatusCode code)
      : std::runtime_error(message), code_(code) {}

  [[nodiscard]] grpc::StatusCode code() const { return code_; }

private:
  grpc::StatusCode code_;
};

class KvStoreClient {
public:
  // Connects to the server at `target` (e.g. "localhost:50051") over
  // plaintext. Each RPC fails with DEADLINE_EXCEEDED if it does not complete
  // within `deadline`; for scan, the deadline covers the whole stream.
  explicit KvStoreClient(
      const std::string &target,
      std::chrono::milliseconds deadline = std::chrono::seconds(5));

  // Same API surface as Engine - errors are thrown as KvStoreError, and
  // NOT_FOUND on get returns nullopt.
  void put(const std::string &key, const std::string &value);
  [[nodiscard]] std::optional<std::string> get(const std::string &key);
  void remove(const std::string &key);
  [[nodiscard]] std::vector<std::pair<std::string, std::string>>
  scan(const std::string &start_key, const std::string &end_key,
       uint32_t limit);

private:
  void set_deadline(grpc::ClientContext &context) const;

  std::unique_ptr<kv::v1::KvStoreService::Stub> stub_;
  std::chrono::milliseconds deadline_;
};

} // namespace kv

#endif // KV_ENGINE_GRPC_CLIENT_HPP
