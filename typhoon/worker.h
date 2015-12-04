#ifndef TYPHOON_WORKER_H
#define TYPHOON_WORKER_H

#include <grpc++/grpc++.h>
#include <memory>
#include <thread>

#include "typhoon/typhoon.grpc.pb.h"
#include "typhoon/common.h"
#include "typhoon/registry.h"

static inline void delayedShutdown(int delay) {
  sleep(delay);
  exit(0);
}

struct Peer {
  WorkerInfo info;
  grpc::ClientContext context;
  TyphoonWorker::Stub* client;

  Peer(const WorkerInfo& info) {
    this->info = info;
    this->client = new TyphoonWorker::Stub(grpc::CreateChannel(info.hostport(), grpc::InsecureCredentials()));
  }

  Sink::Ptr makeSink(const std::string& type, const std::string& name) {
    RemoteStore* store = dynamic_cast<RemoteStore*>(Registry<Base>::create(type));
    store->setRemote(client, name);
    return Sink::Ptr(store);
  }

  Source::Ptr makeSource(const std::string& type, const std::string& name) {
    RemoteStore* store = dynamic_cast<RemoteStore*>(Registry<Base>::create(type));
    store->setRemote(client, name);
    return Source::Ptr(store);
  }
};

struct TaskHelper {
  std::vector<std::shared_ptr<Source>> sources;
  std::vector<std::shared_ptr<Sink>> sinks;

  std::shared_ptr<Task> task;
  std::shared_ptr<std::thread> thread;

  ::TaskDescription request;

  bool running;

  TaskHelper() : running(true) {

  }

  void start() {
    running = true;
    thread.reset(new std::thread(&TaskHelper::run, this));
  }

  void wait() {
    thread->join();
  }

private:
  void run() {
    task->initialize(request);
    gpr_log(GPR_INFO, "Task %s started.", request.id().c_str());
    task->run(sources, sinks);

    gpr_log(GPR_INFO, "Flushing sinks for task: %s", request.id().c_str());
    for (auto i : sinks) {
      i->flush();
      // If the output of our task is a store, mark it as available to be read from.
      auto casted = std::dynamic_pointer_cast<Source>(i);
      if (casted != NULL) {
        casted->setReady(true);
      }
    }

    gpr_log(GPR_INFO, "Task %s finished successfully.", request.id().c_str());
    running = false;
    sources.clear();
    sinks.clear();
  }
};

class Worker: public TyphoonWorker::Service {
private:
  int32_t workerId_;

  std::map<std::string, Peer*> workers_;
  std::map<std::string, Peer*> workerForObj_;

  std::map<std::string, std::shared_ptr<TaskHelper>> tasks_;
  std::map<std::string, std::shared_ptr<Base>> stores_;
  std::map<int64_t, std::shared_ptr<Iterator>> iterators_;

  int64_t iteratorId_;

  TyphoonWorker::AsyncService service_;
  std::unique_ptr<grpc::ServerCompletionQueue> cq_;

  Source::Ptr lookupSource(const std::string& type, const std::string& name) {
    if (stores_.find(name) != stores_.end()) {
      return std::dynamic_pointer_cast<Source>(stores_[name]);
    }

    return workerForObj_[name]->makeSource(type, name);
  }

  Sink::Ptr lookupSink(const std::string& type, const std::string& name) {
    gpr_log(GPR_INFO, "Looking up sink: %s", name.c_str());
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

    std::shared_ptr<Source> source = std::dynamic_pointer_cast<Source>(store);
    if (source.get()) {
      source->init(*request);
    }

    std::shared_ptr<Sink> sink = std::dynamic_pointer_cast<Sink>(store);
    if (sink.get()) {
      if (!request->combiner().empty()) {
        sink->setCombiner(Registry<Combiner>::create(request->combiner()));
      }
    } else {
      GPR_ASSERT(request->combiner().empty());
    }


    stores_[request->name()] = store;

    gpr_log(GPR_INFO, "Creating store: %s", request->name().c_str());
    return grpc::Status::OK;
  }

  grpc::Status shutdown(::grpc::ServerContext* context, const ::EmptyMessage* request, ::EmptyMessage* response) {
    new std::thread(&delayedShutdown, 1);
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
