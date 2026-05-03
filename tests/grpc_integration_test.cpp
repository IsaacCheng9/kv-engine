// End-to-end integration tests that exercise the gRPC server + client over a
// real loopback channel. Each test spins up a fresh in-process server (port 0
// for kernel-allocated free port, fresh temp data dir) via the start_server
// helper and connects a KvStoreClient to it. The ServerHandle destructor
// shuts the server down and cleans the temp dir between tests.
#include "engine.hpp"
#include "grpc_client.hpp"
#include "grpc_server.hpp"
#include <chrono>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>
#include <string>

namespace kv {

namespace {

struct ServerHandle {
  std::filesystem::path data_dir;
  std::unique_ptr<Engine> engine;
  std::unique_ptr<KvStoreServiceImpl> service;
  std::unique_ptr<grpc::Server> server;
  int port = 0;

  ServerHandle() = default;
  ServerHandle(ServerHandle &&) = default;
  ServerHandle &operator=(ServerHandle &&) = default;
  ServerHandle(const ServerHandle &) = delete;
  ServerHandle &operator=(const ServerHandle &) = delete;

  ~ServerHandle() {
    if (server) {
      // Immediate shutdown - tests don't have long-running in-flight RPCs.
      server->Shutdown(std::chrono::system_clock::now());
    }
    server.reset();
    service.reset();
    engine.reset();
    if (!data_dir.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(data_dir, ec);
    }
  }
};

ServerHandle start_server(const std::string &test_name) {
  ServerHandle handle;
  handle.data_dir = std::filesystem::temp_directory_path() /
                    std::filesystem::path("kv_grpc_integration_" + test_name);
  std::filesystem::remove_all(handle.data_dir);
  std::filesystem::create_directories(handle.data_dir);

  handle.engine = std::make_unique<Engine>(handle.data_dir.string());
  handle.service = std::make_unique<KvStoreServiceImpl>(handle.engine.get());

  grpc::ServerBuilder builder;
  builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(),
                           &handle.port);
  builder.RegisterService(handle.service.get());
  handle.server = builder.BuildAndStart();
  return handle;
}

KvStoreClient make_client(const ServerHandle &handle) {
  return KvStoreClient("localhost:" + std::to_string(handle.port));
}

TEST(GrpcIntegrationTest, PutThenGetReturnsValue) {
  auto handle = start_server("put_then_get");
  auto client = make_client(handle);

  client.put("key1", "value1");
  auto value = client.get("key1");

  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, "value1");
}

TEST(GrpcIntegrationTest, RemoveDeletesKey) {
  auto handle = start_server("remove_deletes_key");
  auto client = make_client(handle);

  client.put("doomed", "value");
  client.remove("doomed");

  EXPECT_EQ(client.get("doomed"), std::nullopt);
}

TEST(GrpcIntegrationTest, GetMissingKeyReturnsNullopt) {
  auto handle = start_server("get_missing_key");
  auto client = make_client(handle);

  EXPECT_EQ(client.get("never_inserted"), std::nullopt);
}

TEST(GrpcIntegrationTest, PutOverwritesExistingValue) {
  auto handle = start_server("put_overwrites");
  auto client = make_client(handle);

  client.put("key", "v1");
  client.put("key", "v2");

  auto value = client.get("key");
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, "v2");
}

TEST(GrpcIntegrationTest, EmptyValueIsAllowed) {
  // Distinct from "key absent" - empty value is a real value. The client
  // returned the wrong type for this case at one point; this test pins the
  // contract.
  auto handle = start_server("empty_value");
  auto client = make_client(handle);

  client.put("key", "");
  auto value = client.get("key");

  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, "");
}

TEST(GrpcIntegrationTest, EmptyKeyOnPutFails) {
  auto handle = start_server("empty_key_put");
  auto client = make_client(handle);

  EXPECT_THROW(client.put("", "value"), std::runtime_error);
}

TEST(GrpcIntegrationTest, OversizedKeyOnPutFails) {
  // Server limit is 64 KiB; 64 KiB + 1 should be rejected.
  auto handle = start_server("oversized_key");
  auto client = make_client(handle);

  std::string huge_key(64 * 1024 + 1, 'k');
  EXPECT_THROW(client.put(huge_key, "value"), std::runtime_error);
}

TEST(GrpcIntegrationTest, OversizedValueOnPutFails) {
  // Server limit is 1 MiB; 1 MiB + 1 should be rejected.
  auto handle = start_server("oversized_value");
  auto client = make_client(handle);

  std::string huge_value(1024 * 1024 + 1, 'v');
  EXPECT_THROW(client.put("key", huge_value), std::runtime_error);
}

} // namespace

} // namespace kv
