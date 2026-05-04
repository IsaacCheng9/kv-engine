#ifndef KV_ENGINE_GRPC_SERVER_HPP
#define KV_ENGINE_GRPC_SERVER_HPP

#include "engine.hpp"
#include "kv/v1/kv.grpc.pb.h"

namespace kv {

class KvStoreServiceImpl : public kv::v1::KvStoreService::Service {
public:
  explicit KvStoreServiceImpl(Engine *engine);

  grpc::Status Put(grpc::ServerContext *context,
                   const kv::v1::PutRequest *request,
                   kv::v1::PutResponse *response) override;

  grpc::Status Get(grpc::ServerContext *context,
                   const kv::v1::GetRequest *request,
                   kv::v1::GetResponse *response) override;

  grpc::Status Delete(grpc::ServerContext *context,
                      const kv::v1::DeleteRequest *request,
                      kv::v1::DeleteResponse *response) override;

  grpc::Status Scan(grpc::ServerContext *context,
                    const kv::v1::ScanRequest *request,
                    grpc::ServerWriter<kv::v1::ScanResponse> *writer) override;

private:
  Engine *engine_;
};

} // namespace kv

#endif // KV_ENGINE_GRPC_SERVER_HPP
