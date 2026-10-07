// End-to-end integration tests that exercise the gRPC server + client over a
// real loopback channel. Each test spins up a fresh in-process server (port 0
// for kernel-allocated free port, fresh temp data dir) via the start_server
// helper and connects a KvStoreClient to it. The ServerHandle destructor
// shuts the server down and cleans the temp dir between tests.
#include "engine.hpp"
#include "grpc_client.hpp"
#include "grpc_server.hpp"
#include "test_utils.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <format>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <memory>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <vector>

namespace kv {

namespace {

struct ServerHandle {
  // Declared first so it is destroyed last, after the engine releases its
  // files.
  test::TempDir temp_dir;
  std::filesystem::path data_dir;
  std::unique_ptr<Engine> engine;
  std::unique_ptr<KvStoreServiceImpl> service;
  std::unique_ptr<grpc::Server> server;
  int port = 0;

  explicit ServerHandle(const std::string &test_name)
      : temp_dir("kv_grpc_integration_" + test_name),
        data_dir(temp_dir.path()) {}
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
  }
};

ServerHandle start_server(const std::string &test_name) {
  ServerHandle handle(test_name);

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

// Listens on a loopback TCP port without ever accepting or speaking HTTP/2.
// The kernel completes the TCP handshake from the listen backlog, so a client
// connects and then waits indefinitely for the server's reply.
class SilentListener {
public:
  SilentListener() {
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ == -1) {
      throw std::system_error(errno, std::generic_category(), "socket failed");
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t addr_len = sizeof(addr);
    if (bind(fd_, reinterpret_cast<sockaddr *>(&addr), addr_len) == -1 ||
        listen(fd_, 16) == -1 ||
        getsockname(fd_, reinterpret_cast<sockaddr *>(&addr), &addr_len) ==
            -1) {
      const int saved_errno = errno;
      close(fd_);
      throw std::system_error(saved_errno, std::generic_category(),
                              "failed to set up listener");
    }
    port_ = ntohs(addr.sin_port);
  }

  ~SilentListener() { close(fd_); }

  SilentListener(const SilentListener &) = delete;
  SilentListener &operator=(const SilentListener &) = delete;

  [[nodiscard]] std::string target() const {
    return "127.0.0.1:" + std::to_string(port_);
  }

private:
  int fd_;
  int port_;
};

// Run `rpc` and return the status code of the KvStoreError it throws.
template <typename Rpc> grpc::StatusCode rpc_error_code(Rpc rpc) {
  try {
    rpc();
  } catch (const KvStoreError &e) { return e.code(); }
  ADD_FAILURE() << "Expected the RPC to throw KvStoreError";
  return grpc::StatusCode::OK;
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

TEST(GrpcIntegrationTest, OversizedStartKeyOnScanFails) {
  // Same 64 KiB cap as Put, applied to scan range bounds. Empty start_key
  // is valid (means unbounded), so only oversized is tested.
  auto handle = start_server("oversized_scan_start");
  auto client = make_client(handle);

  std::string huge_start(64 * 1024 + 1, 's');
  EXPECT_THROW((void)client.scan(huge_start, "", 0), std::runtime_error);
}

TEST(GrpcIntegrationTest, OversizedEndKeyOnScanFails) {
  auto handle = start_server("oversized_scan_end");
  auto client = make_client(handle);

  std::string huge_end(64 * 1024 + 1, 'e');
  EXPECT_THROW((void)client.scan("", huge_end, 0), std::runtime_error);
}

TEST(GrpcIntegrationTest, ConcurrentClientWritesArePersisted) {
  // 8 client-thread pairs, each doing 100 puts on distinct keys
  // (namespaced by thread id). After all threads join, verify every key is
  // readable with its expected value. Stresses both the gRPC server's
  // request dispatch under multiple simultaneous connections and the
  // engine's write_mutex_ under genuine concurrent load.
  auto handle = start_server("concurrent_writes");

  constexpr int num_threads = 8;
  constexpr int puts_per_thread = 100;

  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
    threads.emplace_back([&handle, thread_id]() {
      // Each thread gets its own client / channel, so the server sees
      // multiple simultaneous TCP connections rather than multiplexed RPCs
      // on one channel.
      auto client = make_client(handle);
      for (int i = 0; i < puts_per_thread; ++i) {
        const std::string key =
            "t" + std::to_string(thread_id) + "_k" + std::to_string(i);
        const std::string value =
            "v" + std::to_string(thread_id) + "_" + std::to_string(i);
        client.put(key, value);
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }

  auto verify_client = make_client(handle);
  for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
    for (int i = 0; i < puts_per_thread; ++i) {
      const std::string key =
          "t" + std::to_string(thread_id) + "_k" + std::to_string(i);
      const std::string expected =
          "v" + std::to_string(thread_id) + "_" + std::to_string(i);
      auto value = verify_client.get(key);
      ASSERT_TRUE(value.has_value())
          << "Missing key " << key << " after concurrent writes";
      EXPECT_EQ(*value, expected);
    }
  }
}

// Mid-scan cancellation: client closes the stream early via
// ClientContext::TryCancel. The server-side handler detects this via
// writer->Write() returning false, returns Status::CANCELLED, and the
// iterator's destructor closes all SSTable fds. Critically, the server must
// remain usable for subsequent calls - a wedged thread or leaked fd would
// manifest as the post-cancel call hanging or failing.
TEST(GrpcIntegrationTest, ScanCancellationLeavesServerUsable) {
  auto handle = start_server("scan_cancellation");

  // Populate enough data that the scan won't complete instantly on loopback.
  // 5000 entries * 200-byte values is ~1 MiB of streaming response - tens of
  // ms even on a fast machine, giving the cancellation a window to land
  // mid-stream rather than after the server has already finished.
  {
    auto populator = make_client(handle);
    for (int i = 0; i < 5000; ++i) {
      populator.put(std::format("key{:05}", i), std::string(200, 'a'));
    }
  }

  // Use the raw stub directly: KvStoreClient::scan() always drains, but we
  // need to cancel mid-stream.
  auto channel = grpc::CreateChannel("localhost:" + std::to_string(handle.port),
                                     grpc::InsecureChannelCredentials());
  auto stub = kv::v1::KvStoreService::NewStub(channel);

  grpc::ClientContext context;
  kv::v1::ScanRequest request;
  auto reader = stub->Scan(&context, request);

  // Read a few entries to confirm the stream is alive, then cancel.
  kv::v1::ScanResponse response;
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(reader->Read(&response));
  }
  context.TryCancel();

  // Drain whatever's already in flight - some responses may have been
  // serialised on the wire before the cancellation reached the server.
  while (reader->Read(&response)) {
    // Discard.
  }

  // CANCELLED if the server detected the cancellation mid-stream; OK if the
  // scan happened to finish before the cancellation propagated. Both are
  // clean outcomes - the test cares that neither path throws or wedges.
  auto status = reader->Finish();
  EXPECT_TRUE(status.error_code() == grpc::StatusCode::CANCELLED ||
              status.error_code() == grpc::StatusCode::OK)
      << "Unexpected status: " << status.error_code() << " "
      << status.error_message();

  // Critical check: a subsequent call must succeed. A wedged server (e.g.
  // leaked iterator state, unjoined thread, blocked mutex) would surface
  // here as a hang or a connection error.
  auto post = make_client(handle);
  post.put("post_cancel", "value");
  auto value = post.get("post_cancel");
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, "value");
}

TEST(GrpcIntegrationTest, UnreachableServerFailsWithUnavailable) {
  // Take a free port and release it, so nothing is listening there.
  std::string target;
  {
    const SilentListener listener;
    target = listener.target();
  }
  KvStoreClient client(target);

  EXPECT_EQ(rpc_error_code([&] { client.put("key", "value"); }),
            grpc::StatusCode::UNAVAILABLE);
}

TEST(GrpcIntegrationTest, UnresponsiveServerFailsWithDeadlineExceeded) {
  const SilentListener listener;
  KvStoreClient client(listener.target(), std::chrono::milliseconds(200));

  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(rpc_error_code([&] { (void)client.get("key"); }),
            grpc::StatusCode::DEADLINE_EXCEEDED);
  // Well under the 5-second default, so the configured deadline applied.
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(3));
}

TEST(GrpcIntegrationTest, UnresponsiveServerFailsStreamingScanWithDeadline) {
  const SilentListener listener;
  KvStoreClient client(listener.target(), std::chrono::milliseconds(200));

  EXPECT_EQ(rpc_error_code([&] { (void)client.scan("", "", 0); }),
            grpc::StatusCode::DEADLINE_EXCEEDED);
}

} // namespace

} // namespace kv
