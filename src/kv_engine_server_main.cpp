#include "engine.hpp"
#include "grpc_server.hpp"
#include <cstdio>
#include <filesystem>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <print>
#include <string>

int main(int argc, char *argv[]) {
  std::string data_dir;
  int port = 50051;

  // Parse CLI arguments.
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
  std::println("Server listening on port {}", port);
  server->Wait();
  return 0;
}
