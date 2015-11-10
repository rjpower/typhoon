#include <memory>
#include <vector>
#include <thread>

#include "typhoon/worker.h"

using std::vector;
using std::map;
using std::shared_ptr;

void Worker::run() {
  std::string server_address("0.0.0.0:19999");
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(this);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  char* masterAddress = getenv("MASTER_ADDRESS");
  GPR_ASSERT(masterAddress);
  grpc::ClientContext context;
  auto masterStub = TyphoonMaster::Stub(
      grpc::CreateChannel(masterAddress, grpc::InsecureCredentials()));
  RegisterRequest req;
  req.set_slots(1);
  RegisterResponse resp;
  masterStub.registerWorker(&context, req, &resp);
  workerId_ = resp.id();
  server->Wait();

  MemStore<std::string, int64_t> store;
}

grpc::Status Worker::start(grpc::ServerContext* context, const ::TaskDescription* request, ::EmptyMessage* response) {
  vector<shared_ptr<Source>> sources;
  vector<shared_ptr<Sink>> sinks;

  for (auto i = request->source().begin(); i != request->source().end(); ++i) {
    sources.push_back(this->lookupSource(i->type(), i->name()));
  }

  for (auto i = request->sink().begin(); i != request->sink().end(); ++i) {
    sinks.push_back(this->lookupSink(i->type(), i->name()));
  }

  gpr_log(GPR_INFO, "Starting task: %s", request->id().c_str());

  auto task = std::shared_ptr<Task>(Registry<Task>::create(request->type()));
  task->initialize(*request);
  task->run(sources, sinks);
  tasks_[request->id()] = task;

  for (auto i = request->sink().begin(); i != request->sink().end(); ++i) {
    auto sink = this->lookupSink(i->type(), i->name());

    // If the output of our task is a store, mark it as available to be read from.
    auto casted = std::dynamic_pointer_cast<Source>(sink);
    if (casted != NULL) {
      casted->setReady(true);
    }
  }

  gpr_log(GPR_INFO, "Task %s finished successfully.", request->id().c_str());

  return grpc::Status::OK;
}

grpc::Status Worker::ping(grpc::ServerContext* context,
    const ::PingRequest* request, ::PingResponse* response) {
  for (int i = 0; i < request->worker_size(); ++i) {
    auto worker = request->worker(i);
    if (workers_.find(worker.hostport()) == workers_.end()) {
      workers_[worker.hostport()] = new Peer(worker);
    }
    Peer* p = workers_[worker.hostport()];
    p->info = worker;

    for (auto task: worker.task()) {
    }

    for (auto store: worker.store()) {
      workerForObj_[store] = p;
    }
  }

  for (auto i = tasks_.begin(); i != tasks_.end(); ++i) {
    i->second->status(response->add_task());
  }
  return grpc::Status::OK;
}
