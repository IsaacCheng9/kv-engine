#include "grpc_server.hpp"

namespace kv {

namespace {
constexpr std::size_t max_key_size = 64 * 1024;     // 64 KiB
constexpr std::size_t max_value_size = 1024 * 1024; // 1 MiB
} // namespace

KvStoreServiceImpl::KvStoreServiceImpl(Engine *engine) : engine_(engine) {}

grpc::Status KvStoreServiceImpl::Put(grpc::ServerContext *,
                                     const kv::v1::PutRequest *request,
                                     kv::v1::PutResponse *) {
  const std::string &key = request->key();
  if (key.empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Key cannot be empty");
  }
  if (key.size() > max_key_size) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Key size exceeds maximum allowed size of " +
                            std::to_string(max_key_size) + " bytes");
  }

  const std::string &value = request->value();
  if (value.size() > max_value_size) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Value size exceeds maximum allowed size of " +
                            std::to_string(max_value_size) + " bytes");
  }

  engine_->put(key, value);
  return grpc::Status::OK;
}

grpc::Status KvStoreServiceImpl::Get(grpc::ServerContext *,
                                     const kv::v1::GetRequest *request,
                                     kv::v1::GetResponse *response) {
  const std::string &key = request->key();
  if (key.empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Key cannot be empty");
  }
  if (key.size() > max_key_size) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Key size exceeds maximum allowed size of " +
                            std::to_string(max_key_size) + " bytes");
  }

  auto value = engine_->get(key);
  if (!value) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND, "Key not found");
  }

  response->set_value(*value);
  return grpc::Status::OK;
}

grpc::Status KvStoreServiceImpl::Delete(grpc::ServerContext *,
                                        const kv::v1::DeleteRequest *request,
                                        kv::v1::DeleteResponse *) {
  const std::string &key = request->key();
  if (key.empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Key cannot be empty");
  }
  if (key.size() > max_key_size) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Key size exceeds maximum allowed size of " +
                            std::to_string(max_key_size) + " bytes");
  }

  engine_->remove(key);
  return grpc::Status::OK;
}

} // namespace kv
