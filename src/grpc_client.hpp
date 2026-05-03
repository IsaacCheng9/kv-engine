#ifndef KV_ENGINE_GRPC_CLIENT_HPP
#define KV_ENGINE_GRPC_CLIENT_HPP

#include "kv/v1/kv.grpc.pb.h"
#include <memory>
#include <optional>
#include <string>

namespace kv {

class KvStoreClient {
public:
  // Connects to the server at `target` (e.g. "localhost:50051") over
  // plaintext.
  explicit KvStoreClient(const std::string &target);

  // Same API surface as Engine - errors mapped to exceptions, NOT_FOUND on
  // get returns nullopt.
  void put(const std::string &key, const std::string &value);
  [[nodiscard]] std::optional<std::string> get(const std::string &key);
  void remove(const std::string &key);

private:
  std::unique_ptr<kv::v1::KvStoreService::Stub> stub_;
};

} // namespace kv

#endif // KV_ENGINE_GRPC_CLIENT_HPP
