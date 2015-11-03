#ifndef TYPHOON_WORKER_H
#define TYPHOON_WORKER_H

#include "typhoon/common.h"


class Worker: public TyphoonWorker::Server {
private:
  TyphoonConfig config_;
  std::vector<WorkerInfo> workers_;
  uint32_t numShards_;

public:
  Worker(TyphoonConfig config) : config_(config), numShards_(10) {
    MemStore<int, int> store(new SumCombiner<int>);
  }

  virtual ~Worker() {
  }

  void run() {

  }

  kj::Promise<void> mapShard(MapShardContext context) {
    auto request = context.getParams().getRequest();
    auto inputSpec = request.getInput();

    Store* store = this->config_.store();
    Mapper* mapper = this->config_.mapper(store);
    MapInput* mapInput = this->config_.mapInput;

    mapper->mapShard(mapInput, inputSpec);

    std::vector<ShuffleRequest::Builder> outputs;
    store->partition(outputs);

    return kj::READY_NOW;
  }

  kj::Promise<void> ping(PingContext ctx) {
    return kj::READY_NOW;
  }
};

#endif
