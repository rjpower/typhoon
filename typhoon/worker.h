#ifndef TYPHOON_WORKER_H
#define TYPHOON_WORKER_H

#include "typhoon/typhoon.grpc.pb.h"
#include "typhoon/common.h"

#include <grpc++/grpc++.h>

class Worker: public TyphoonWorker::Service {
private:
  TyphoonConfig config_;
  std::vector<WorkerInfo> workers_;
  std::map<int, TyphoonWorker::Stub*> workerForShard_;

public:
  Worker(TyphoonConfig config) : config_(config) {
  }

  virtual ~Worker() {
  }

  void run() {
    std::string server_address("0.0.0.0:50051");
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

    server->Wait();
  }

  void sendToReducers(const std::vector<ShuffleRequest>& data);

  grpc::Status map(grpc::ServerContext* context, const ::MapRequest* request,
      ::MapResponse* response);

  grpc::Status shuffle(grpc::ServerContext* context, const ::ShuffleRequest* request, ::ShuffleResponse* response) {
    return grpc::Status::OK;
  }
};

inline void Worker::sendToReducers(const std::vector<ShuffleRequest>& data) {
  grpc::ClientContext context;
  grpc::CompletionQueue cq;
  std::vector<ShuffleResponse> replies(data.size());
  std::vector<grpc::Status> status(data.size());
  std::vector<std::unique_ptr<grpc::ClientAsyncResponseReader<ShuffleResponse> > > rpcs;
  for (int i = 0; i < data.size(); ++i) {
    TyphoonWorker::Stub* worker = workerForShard_[i];
    auto rpc = worker->Asyncshuffle(&context, data[i], &cq);
    rpcs[i]->Finish(&replies[i], &status[i], (void*) (i));
    rpcs.push_back(std::move(rpc));
  }
  for (int i = 0; i < data.size(); ++i) {
    void* tag;
    bool ok = false;
    cq.Next(&tag, &ok);

    int64_t doneIdx = (int64_t) tag;
    GPR_ASSERT(status[doneIdx].ok());
  }
}

inline grpc::Status Worker::map(grpc::ServerContext* context,
    const ::MapRequest* request, ::MapResponse* response) {
  Store* store = this->config_.store();
  Mapper* mapper = createMapper(this->config_);
  MapInput* mapInput = this->config_.mapInput;
  mapper->mapShard(mapInput, request->shard());
  std::vector<ShuffleRequest> outputs;
  store->partition(outputs);
  sendToReducers(outputs);
  return grpc::Status::OK;
}

#endif
