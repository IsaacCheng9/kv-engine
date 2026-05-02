#include "grpc_server.hpp"

namespace kv {

// TODO: implement KvStoreServiceImpl methods here.
KvStoreServiceImpl::KvStoreServiceImpl(Engine *engine) : engine_(engine) {}

grpc::Status KvStoreServiceImpl::Put(grpc::ServerContext *,
                                     const kv::v1::PutRequest *,
                                     kv::v1::PutResponse *) {
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Not yet implemented");
}

grpc::Status KvStoreServiceImpl::Get(grpc::ServerContext *,
                                     const kv::v1::GetRequest *,
                                     kv::v1::GetResponse *) {
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Not yet implemented");
}

grpc::Status KvStoreServiceImpl::Delete(grpc::ServerContext *,
                                        const kv::v1::DeleteRequest *,
                                        kv::v1::DeleteResponse *) {
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Not yet implemented");
}

} // namespace kv
