#include "grpc_client.hpp"
#include <format>
#include <grpcpp/grpcpp.h>
#include <stdexcept>

namespace kv {

KvStoreClient::KvStoreClient(const std::string &target)
    : stub_(kv::v1::KvStoreService::NewStub(
          grpc::CreateChannel(target, grpc::InsecureChannelCredentials()))) {}

void KvStoreClient::put(const std::string &key, const std::string &value) {
  kv::v1::PutRequest request;
  request.set_key(key);
  request.set_value(value);
  kv::v1::PutResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub_->Put(&context, request, &response);
  if (!status.ok()) {
    throw std::runtime_error(std::format(
        "KvStoreClient::put failed: {} ({})", status.error_message(),
        static_cast<int>(status.error_code())));
  }
}

std::optional<std::string> KvStoreClient::get(const std::string &key) {
  kv::v1::GetRequest request;
  request.set_key(key);
  kv::v1::GetResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub_->Get(&context, request, &response);
  if (status.error_code() == grpc::StatusCode::NOT_FOUND) {
    return std::nullopt;
  }
  if (!status.ok()) {
    throw std::runtime_error(std::format(
        "KvStoreClient::get failed: {} ({})", status.error_message(),
        static_cast<int>(status.error_code())));
  }
  return response.value();
}

void KvStoreClient::remove(const std::string &key) {
  kv::v1::DeleteRequest request;
  request.set_key(key);
  kv::v1::DeleteResponse response;
  grpc::ClientContext context;
  grpc::Status status = stub_->Delete(&context, request, &response);
  if (!status.ok()) {
    throw std::runtime_error(std::format(
        "KvStoreClient::remove failed: {} ({})", status.error_message(),
        static_cast<int>(status.error_code())));
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
  kv::v1::ScanResponse response;
  auto reader = stub_->Scan(&context, request);
  while (reader->Read(&response)) {
    result.emplace_back(response.key(), response.value());
  }

  auto status = reader->Finish();
  if (!status.ok()) {
    throw std::runtime_error(std::format(
        "KvStoreClient::scan failed: {} ({})", status.error_message(),
        static_cast<int>(status.error_code())));
  }
  return result;
}

} // namespace kv
