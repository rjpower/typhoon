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
//  builder.RegisterAsyncService(&service_);
  builder.RegisterService(this);
  cq_ = builder.AddCompletionQueue();
  gpr_log(GPR_INFO, "Starting server on port 19999");
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  gpr_log(GPR_INFO, "Started.");

  char* masterAddress = getenv("MASTER_ADDRESS");
  GPR_ASSERT(masterAddress);
  gpr_log(GPR_INFO, "Pinging master... %s", masterAddress);
  while (true) {
    grpc::ClientContext context;
    auto masterStub = TyphoonMaster::Stub(
       grpc::CreateChannel(masterAddress, grpc::InsecureCredentials()));
    RegisterRequest req;
    req.set_host("127.0.0.1");
    req.set_port(19999);
    req.set_slots(4);
    RegisterResponse resp;
    grpc::Status status = masterStub.hello(&context, req, &resp);
    if (!status.ok()) {
      gpr_log(GPR_INFO, "Ping failed: %d %s", status.error_code(), status.error_message().c_str());
      sleep(1);
    } else {
      workerId_ = resp.id();
      break;
    }
  }

  gpr_log(GPR_INFO, "Assigned worker id: %d", workerId_);
  server->Wait();
}

grpc::Status Worker::start(grpc::ServerContext* context, const ::TaskDescription* request, ::EmptyMessage* response) {
  auto helper = std::make_shared<TaskHelper>();
  for (auto i = request->source().begin(); i != request->source().end(); ++i) {
    helper->sources.push_back(this->lookupSource(i->type(), i->name()));
  }

  for (auto i = request->sink().begin(); i != request->sink().end(); ++i) {
    helper->sinks.push_back(this->lookupSink(i->type(), i->name()));
  }

  gpr_log(GPR_INFO, "Starting task: %s", request->id().c_str());
  helper->task = std::shared_ptr<Task>(Registry<Task>::create(request->type()));
  helper->request.CopyFrom(*request);
  helper->start();
  tasks_[request->id()] = helper;
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
    for (auto store: worker.store()) {
      workerForObj_[store] = p;
    }
  }

  for (auto task: tasks_) {
    TaskStatus* status = response->add_task();
    status->set_id(task.first);
    status->set_progress(0);
    status->set_status(task.second->running ?
        Status::ACTIVE : Status::SUCCESS
    );
  }

  return grpc::Status::OK;
}
