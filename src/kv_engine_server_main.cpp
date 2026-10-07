#include "engine.hpp"
#include "grpc_server.hpp"
#include "server_args.hpp"
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <print>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {
std::atomic<bool> shutdown_requested{false};

int run_server(const kv::ServerArgs &args) {
  std::filesystem::create_directories(args.data_dir);

  kv::Engine engine(args.data_dir);
  kv::KvStoreServiceImpl service(&engine);

  grpc::ServerBuilder builder;
  builder.AddListeningPort("localhost:" + std::to_string(args.port),
                           grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();

  // Signal handlers can only call async-signal-safe functions, which excludes
  // server->Shutdown() (it allocates, takes locks, does I/O). So the handler
  // just sets an atomic flag, and a watcher thread polls for it and triggers
  // shutdown from a normal context.
  std::thread shutdown_watcher([&] {
    while (!shutdown_requested.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // Give in-flight RPCs 5 seconds to finish, then force shutdown.
    server->Shutdown(std::chrono::system_clock::now() +
                     std::chrono::seconds(5));
  });

  std::println("Server listening on port {}", args.port);
  server->Wait();
  shutdown_watcher.join();
  return 0;
}
} // namespace

extern "C" void on_signal(int) {
  shutdown_requested.store(true, std::memory_order_relaxed);
}

int main(int argc, char *argv[]) {
  const std::vector<std::string_view> raw_args(argv + 1, argv + argc);
  const auto args = kv::parse_server_args(raw_args);
  if (!args) {
    std::println(stderr, "{}", args.error());
    return 1;
  }

  signal(SIGINT, on_signal);
  signal(SIGTERM, on_signal);
  // Report startup failures such as an unusable data directory or corrupt
  // engine files instead of terminating on an uncaught exception.
  try {
    return run_server(*args);
  } catch (const std::exception &e) {
    std::println(stderr, "kv_engine_server failed to start: {}", e.what());
    return 1;
  }
}
