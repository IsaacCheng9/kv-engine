#include "grpc_server.hpp"
#include <optional>

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

  try {
    engine_->put(key, value);
  } catch (const std::exception &e) {
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
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

  std::optional<std::string> value;
  try {
    value = engine_->get(key);
  } catch (const std::exception &e) {
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
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

  try {
    engine_->remove(key);
  } catch (const std::exception &e) {
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
  return grpc::Status::OK;
}

grpc::Status
KvStoreServiceImpl::Scan(grpc::ServerContext *,
                         const kv::v1::ScanRequest *request,
                         grpc::ServerWriter<kv::v1::ScanResponse> *writer) {
  // Empty start_key / end_key are allowed (mean unbounded).
  if (request->start_key().size() > max_key_size) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "start_key size exceeds maximum allowed size of " +
                            std::to_string(max_key_size) + " bytes");
  }
  if (request->end_key().size() > max_key_size) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "end_key size exceeds maximum allowed size of " +
                            std::to_string(max_key_size) + " bytes");
  }

  try {
    auto it = engine_->scan(request->start_key(), request->end_key(),
                            request->limit());

    while (auto entry = it.next()) {
      kv::v1::ScanResponse response;
      response.set_key(std::move(entry->first));
      response.set_value(std::move(entry->second));
      if (!writer->Write(response)) {
        return grpc::Status::CANCELLED;
      }
    }
  } catch (const std::exception &e) {
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }

  return grpc::Status::OK;
}

} // namespace kv
