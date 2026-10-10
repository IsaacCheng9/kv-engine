#include "grpc_client.hpp"
#include <format>
#include <grpcpp/grpcpp.h>
#include <string_view>

namespace kv {

namespace {

[[noreturn]] void throw_rpc_error(std::string_view method,
                                  const grpc::Status &status) {
  throw KvStoreError(std::format("KvStoreClient::{} failed: {} ({})", method,
                                 status.error_message(),
                                 static_cast<int>(status.error_code())),
                     status.error_code());
}

} // namespace

KvStoreClient::KvStoreClient(const std::string &target,
                             std::chrono::milliseconds deadline)
    : stub_(kv::v1::KvStoreService::NewStub(
          grpc::CreateChannel(target, grpc::InsecureChannelCredentials()))),
      deadline_(deadline) {}

void KvStoreClient::set_deadline(grpc::ClientContext &context) const {
  context.set_deadline(std::chrono::system_clock::now() + deadline_);
}

void KvStoreClient::put(const std::string &key, const std::string &value) {
  kv::v1::PutRequest request;
  request.set_key(key);
  request.set_value(value);
  kv::v1::PutResponse response;
  grpc::ClientContext context;
  set_deadline(context);
  grpc::Status status = stub_->Put(&context, request, &response);
  if (!status.ok()) {
    throw_rpc_error("put", status);
  }
}

std::optional<std::string> KvStoreClient::get(const std::string &key) {
  kv::v1::GetRequest request;
  request.set_key(key);
  kv::v1::GetResponse response;
  grpc::ClientContext context;
  set_deadline(context);
  grpc::Status status = stub_->Get(&context, request, &response);
  if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
    return std::nullopt;
  }
  if (!status.ok()) {
    throw_rpc_error("get", status);
  }
  return response.value();
}

void KvStoreClient::remove(const std::string &key) {
  kv::v1::DeleteRequest request;
  request.set_key(key);
  kv::v1::DeleteResponse response;
  grpc::ClientContext context;
  set_deadline(context);
  grpc::Status status = stub_->Delete(&context, request, &response);
  if (!status.ok()) {
    throw_rpc_error("remove", status);
  }
}

std::vector<std::pair<std::string, std::string>>
KvStoreClient::scan(const std::string &start_key, const std::string &end_key,
                    uint32_t limit) {
  kv::v1::ScanRequest request;
  request.set_start_key(start_key);
  request.set_end_key(end_key);
  request.set_limit(limit);

  std::vector<std::pair<std::string, std::string>> result;
  grpc::ClientContext context;
  set_deadline(context);
  kv::v1::ScanResponse response;
  auto reader = stub_->Scan(&context, request);
  while (reader->Read(&response)) {
    result.emplace_back(response.key(), response.value());
  }

  auto status = reader->Finish();
  if (!status.ok()) {
    throw_rpc_error("scan", status);
  }
  return result;
}

} // namespace kv
