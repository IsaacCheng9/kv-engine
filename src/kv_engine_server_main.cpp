#include "engine.hpp"
#include "grpc_server.hpp"
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <print>
#include <string>
#include <thread>

namespace {
std::atomic<bool> shutdown_requested{false};
}

extern "C" void on_signal(int) {
  shutdown_requested.store(true, std::memory_order_relaxed);
}

int main(int argc, char *argv[]) {
  std::string data_dir;
  int port = 50051;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--data-dir" && i + 1 < argc) {
      data_dir = argv[++i];
    } else if (arg == "--port" && i + 1 < argc) {
      port = std::stoi(argv[++i]);
    } else {
      std::println(stderr, "Unknown argument: {}", arg);
      return 1;
    }
  }
  if (data_dir.empty()) {
    std::println(stderr,
                 "Usage: kv_engine_server --data-dir <path> [--port <port>]");
    return 1;
  }
  std::filesystem::create_directories(data_dir);

  kv::Engine engine(data_dir);
  kv::KvStoreServiceImpl service(&engine);

  grpc::ServerBuilder builder;
  builder.AddListeningPort("localhost:" + std::to_string(port),
                           grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();

  // Signal handlers can only call async-signal-safe functions, which excludes
  // server->Shutdown() (it allocates, takes locks, does I/O). So the handler
  // just sets an atomic flag, and a watcher thread polls for it and triggers
  // shutdown from a normal context.
  signal(SIGINT, on_signal);
  signal(SIGTERM, on_signal);
  std::thread shutdown_watcher([&] {
    while (!shutdown_requested.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // Give in-flight RPCs 5 seconds to finish, then force shutdown.
    server->Shutdown(std::chrono::system_clock::now() +
                     std::chrono::seconds(5));
  });

  std::println("Server listening on port {}", port);
  server->Wait();
  shutdown_watcher.join();
  return 0;
}
