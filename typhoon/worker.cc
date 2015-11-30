#include <memory>
#include <vector>
#include <thread>

#include "typhoon/worker.h"

using std::vector;
using std::map;
using std::shared_ptr;

using grpc::ServerContext;
using grpc::ServerCompletionQueue;

class CallData {
public:
  CallData() : thread_(NULL), threaded_(false) {}

  virtual void processRequest() = 0;
  virtual void startRequest() = 0;
  virtual grpc::Status handleRequest() = 0;
  virtual CallData* allocate() = 0;
  void useThreads() {
    threaded_ = true;
  }

protected:
  std::thread* thread_;
  bool threaded_;
};

template <class Request, class Response>
class CallDataT : public CallData {
public:
  virtual ~CallDataT() {}
  CallDataT(
      TyphoonWorker::Service* handler,
      TyphoonWorker::AsyncService* service,
      ServerCompletionQueue *cq
      ) : handler_(handler), service_(service), cq_(cq), responder_(&ctx_), status_(CREATE)  {
  }

  void threadedRequest() {
    grpc::Status result = this->handleRequest();
    responder_.Finish(reply_, result, this);
    gpr_log(GPR_INFO, "Request finished.");
    status_ = FINISH;
  }

  void processRequest() {
    if (status_ == CREATE) {
      this->startRequest();
      status_ = PROCESS;
    } else if (status_ == PROCESS) {
      gpr_log(GPR_INFO, "Processing request...");
      CallDataT<Request, Response>* next = static_cast<CallDataT<Request, Response>*>(this->allocate());
      next->threaded_ = this->threaded_;
      next->processRequest();

      if (threaded_) {
        gpr_log(GPR_INFO, "Spawning thread.");
        thread_ = new std::thread(&CallDataT<Request, Response>::threadedRequest, this);
      } else {
        this->threadedRequest();
      }
    } else {
      GPR_ASSERT(status_ == FINISH);
      delete this;
    }
  }
protected:
  TyphoonWorker::Service* handler_;
  TyphoonWorker::AsyncService* service_;
  ServerCompletionQueue* cq_;
  ServerContext ctx_;

  Request request_;
  Response reply_;

  grpc::ServerAsyncResponseWriter<Response> responder_;

  enum CallStatus { CREATE, PROCESS, FINISH };
  CallStatus status_;  // The current serving state.
};

#define MAKE_HANDLER(name, req, resp)\
class name ## CallData : public CallDataT<req, resp> {\
  public:\
  name ## CallData(TyphoonWorker::Service* handler, TyphoonWorker::AsyncService* service, ServerCompletionQueue *cq) :\
    CallDataT<req, resp>(handler, service, cq) {}\
  void startRequest() { service_->Request ## name (&ctx_, &request_, &responder_, cq_, cq_, this); }\
  grpc::Status handleRequest() { return handler_-> name (&ctx_, &request_, &reply_); }\
  CallDataT<req, resp>* allocate() { return new name ## CallData(handler_, service_, cq_); }\
};

MAKE_HANDLER(ping, PingRequest, PingResponse)
MAKE_HANDLER(push, PushRequest, PushResponse)
MAKE_HANDLER(pull, PullRequest, PullResponse)
MAKE_HANDLER(start, TaskDescription, EmptyMessage)
MAKE_HANDLER(assign, StoreDescription, EmptyMessage)

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

  gpr_log(GPR_INFO, "Pinging master...");
//  char* masterAddress = getenv("MASTER_ADDRESS");
//  GPR_ASSERT(masterAddress);
//  grpc::ClientContext context;
//  auto masterStub = TyphoonMaster::Stub(
//      grpc::CreateChannel(masterAddress, grpc::InsecureCredentials()));
//  RegisterRequest req;
//  req.set_slots(1);
//  RegisterResponse resp;
//  masterStub.registerWorker(&context, req, &resp);
//  workerId_ = resp.id();

  gpr_log(GPR_INFO, "Assigned worker id: %d", workerId_);
  server->Wait();
}

void Worker::handleRPCs() {
  CallData* handlers[5] = {
    new pingCallData(this, &service_, cq_.get()),
    new pushCallData(this, &service_, cq_.get()),
    new pullCallData(this, &service_, cq_.get()),
    new assignCallData(this, &service_, cq_.get()),
    new startCallData(this, &service_, cq_.get())
  };

  handlers[4]->useThreads();

  for (int i = 0; i < 5; ++i) {
    handlers[i]->processRequest();
  }

  gpr_log(GPR_INFO, "Done.");

  void* tag;
  bool ok;
  while (true) {
    cq_->Next(&tag, &ok);
    GPR_ASSERT(ok);
    gpr_log(GPR_INFO, "Got tag!");
    static_cast<CallData*>(tag)->processRequest();
  }
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

    for (auto task: worker.task()) {
    }

    for (auto store: worker.store()) {
      workerForObj_[store] = p;
    }
  }

  for (auto i = tasks_.begin(); i != tasks_.end(); ++i) {
    i->second->task->status(response->add_task());
  }
  return grpc::Status::OK;
}
