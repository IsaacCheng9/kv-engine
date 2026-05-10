#include "grpc_server.hpp"
#include <optional>

namespace kv {

namespace {
constexpr std::size_t max_key_size = 64 * 1024;     // 64 KiB
constexpr std::size_t max_value_size = 1024 * 1024; // 1 MiB

// Check a key passed to Put / Get / Delete: must be non-empty and within
// the size cap. Returns OK if valid, otherwise INVALID_ARGUMENT with a
// descriptive message.
grpc::Status validate_key(const std::string &key) {
  if (key.empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Key cannot be empty");
  }
  if (key.size() > max_key_size) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Key size exceeds maximum allowed size of " +
                            std::to_string(max_key_size) + " bytes");
  }
  return grpc::Status::OK;
}

// Generic size check with a customisable field name for the error message.
// Used for Scan's start_key / end_key (empty is allowed) and Put's value.
grpc::Status validate_size(const std::string &field_name, std::size_t actual,
                           std::size_t max) {
  if (actual > max) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        field_name + " size exceeds maximum allowed size of " +
                            std::to_string(max) + " bytes");
  }
  return grpc::Status::OK;
}
} // namespace

KvStoreServiceImpl::KvStoreServiceImpl(Engine *engine) : engine_(engine) {}

grpc::Status KvStoreServiceImpl::Put(grpc::ServerContext *,
                                     const kv::v1::PutRequest *request,
                                     kv::v1::PutResponse *) {
  if (auto status = validate_key(request->key()); !status.ok()) {
    return status;
  }
  if (auto status =
          validate_size("Value", request->value().size(), max_value_size);
      !status.ok()) {
    return status;
  }

  try {
    engine_->put(request->key(), request->value());
  } catch (const std::exception &e) {
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
  return grpc::Status::OK;
}

grpc::Status KvStoreServiceImpl::Get(grpc::ServerContext *,
                                     const kv::v1::GetRequest *request,
                                     kv::v1::GetResponse *response) {
  if (auto status = validate_key(request->key()); !status.ok()) {
    return status;
  }

  std::optional<std::string> value;
  try {
    value = engine_->get(request->key());
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
  if (auto status = validate_key(request->key()); !status.ok()) {
    return status;
  }

  try {
    engine_->remove(request->key());
  } catch (const std::exception &e) {
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
  return grpc::Status::OK;
}

grpc::Status
KvStoreServiceImpl::Scan(grpc::ServerContext *,
                         const kv::v1::ScanRequest *request,
                         grpc::ServerWriter<kv::v1::ScanResponse> *writer) {
  // Empty start_key / end_key are allowed (mean unbounded), so size-only
  // validation rather than validate_key.
  if (auto status =
          validate_size("start_key", request->start_key().size(), max_key_size);
      !status.ok()) {
    return status;
  }
  if (auto status =
          validate_size("end_key", request->end_key().size(), max_key_size);
      !status.ok()) {
    return status;
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
