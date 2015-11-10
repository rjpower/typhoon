#ifndef TYPHOON_WORKER_H
#define TYPHOON_WORKER_H

#include <grpc++/grpc++.h>
#include <memory>

#include "typhoon/typhoon.grpc.pb.h"
#include "typhoon/common.h"
#include "typhoon/registry.h"

class Worker: public TyphoonWorker::Service {
private:
  int32_t workerId_;

  struct Peer {
    WorkerInfo info;
    grpc::ClientContext context;
    TyphoonWorker::Stub* client;

    Peer(const WorkerInfo& info) {
      this->info = info;
      this->client = new TyphoonWorker::Stub(grpc::CreateChannel(info.hostport(), grpc::InsecureCredentials()));
    }

    Sink::Ptr makeSink(const std::string& type, const std::string& name) {
      RemoteStore* store = Registry<RemoteStore>::create(type);
      store->setRemote(client, name);
      return Sink::Ptr(reinterpret_cast<Sink*>(store));
    }

    Source::Ptr makeSource(const std::string& type, const std::string& name) {
      RemoteStore* store = Registry<RemoteStore>::create(type);
      store->setRemote(client, name);
      return Source::Ptr(reinterpret_cast<Source*>(store));
    }
  };

  std::map<std::string, Peer*> workers_;
  std::map<std::string, Peer*> workerForObj_;

  std::map<std::string, std::shared_ptr<Task>> tasks_;
  std::map<std::string, std::shared_ptr<Base>> stores_;
  std::map<int64_t, std::shared_ptr<Iterator>> iterators_;

  int64_t iteratorId_;

  Source::Ptr lookupSource(const std::string& type, const std::string& name) {
    if (stores_.find(name) != stores_.end()) {
      return std::dynamic_pointer_cast<Source>(stores_[name]);
    }

    return workerForObj_[name]->makeSource(type, name);
  }

  Sink::Ptr lookupSink(const std::string& type, const std::string& name) {
    if (stores_.find(name) != stores_.end()) {
      return std::dynamic_pointer_cast<Sink>(stores_[name]);
    }

    return workerForObj_[name]->makeSink(type, name);
  }

public:
  Worker() : workerId_(-1) {}

  virtual ~Worker() {
  }

  void run();

  void sendToReducers(const std::vector<ShuffleData>& data);

  grpc::Status start(grpc::ServerContext* context, const ::TaskDescription* request, ::EmptyMessage* response);
  grpc::Status ping(grpc::ServerContext* context, const ::PingRequest* request, ::PingResponse* response);

  grpc::Status assign(::grpc::ServerContext* context, const ::StoreDescription* request, ::EmptyMessage* response) {
    std::shared_ptr<Base> store(Registry<Base>::create(request->type()));
    stores_[request->name()] = store;
    return grpc::Status::OK;
  }

  grpc::Status push(::grpc::ServerContext* context, const ::PushRequest* request, ::PushResponse* response) {
    auto sink = std::dynamic_pointer_cast<Sink>(stores_[request->target()]);
    sink->applyUpdates(request->data());

    return grpc::Status::OK;
  }

  grpc::Status pull(::grpc::ServerContext* context, const ::PullRequest* request, ::PullResponse* response) {
    std::shared_ptr<Iterator> it;

    if (request->iterator() >= 0) {
      it = iterators_[request->iterator()];
      response->set_iterator(request->iterator());
    } else {
      auto source = std::dynamic_pointer_cast<Source>(stores_[request->source()]);
      while (!source->ready()) {
        gpr_log(GPR_INFO, "Waiting for %s to become ready.", request->source().c_str());
        sleep(1);
      }
      it.reset(source->iterator());
      int64_t id = iteratorId_++;
      iterators_[id] = it;
      response->set_iterator(id);
    }

    ShuffleData* data = response->mutable_data();
    it->copy(data, kPullBlockSize);

    return grpc::Status::OK;
  }
};
#endif
