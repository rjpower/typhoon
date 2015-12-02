#ifndef TYPHOON_COMMON_H
#define TYPHOON_COMMON_H

#include <vector>
#include <map>
#include <string>
#include <unordered_map>

#include <cstdio>

#include "typhoon/typhoon.pb.h"
#include "typhoon/typhoon.grpc.pb.h"
#include "typhoon/registry.h"
#include "typhoon/colgroup.h"

static const int kShuffleBufferSize = 1e9;
static const int kPullBlockSize = 4096;

class Iterator {
public:
  virtual ~Iterator() {}
  virtual bool copy(ShuffleData* data, size_t maxElements) {
    ColGroup rows;
    while (!next(&rows)) {
      rows.encode(data);
    }
    return true;
  }

  virtual bool next(ColGroup* data) = 0;
};

// Just a placeholder class to allow us to dynamic_cast when needed.
class Base {
public:
  virtual ~Base() {}
};

class Source : virtual public Base {
protected:
  bool ready_;
  FileSplit split_;

public:
  typedef std::shared_ptr<Source> Ptr;
  Source() : ready_(false) {}

  virtual ~Source() {}
  virtual Iterator* iterator() = 0;
  void init(const FileSplit& split) {
    split_.CopyFrom(split);
  }

  void setReady(bool ready) {
    ready_ = ready;
  }

  virtual bool ready() {
    return ready_;
  }
};

class Sink : virtual public Base {
public:
  typedef std::shared_ptr<Sink> Ptr;
  virtual ~Sink() {}

  virtual void applyUpdates(const ShuffleData& r) {
    ColGroup rows;
    rows.decode(r);
    write(rows);
  }

  virtual void write(const ColGroup& rows) = 0;
  virtual void flush() {}
};

class Task {
public:
  virtual ~Task() {

  }
  virtual void run(
      std::vector<Source::Ptr> sources,
      std::vector<Sink::Ptr> sinks) = 0;

  virtual void initialize(const TaskDescription& req) {

  }

  virtual void status(TaskStatus* status) {
    status->set_status(Status::ACTIVE);
  }
};

class RemoteIterator : public Iterator {
  TyphoonWorker::Stub* peer_;
  PullRequest req_;
  PullResponse buffer_;
  int idx_;

  void fetchNextChunk() {
    gpr_log(GPR_INFO, "Fetching chunk from %s. id=%lld", req_.source().c_str(), req_.iterator());
    grpc::ClientContext ctx;
    peer_->pull(&ctx, req_, &buffer_);
    req_.set_iterator(buffer_.iterator());
    gpr_log(GPR_INFO, "Finished fetch.  id=%lld", req_.iterator());
    idx_ = 0;
  }

public:
  RemoteIterator(TyphoonWorker::Stub* peer, const std::string& source) : peer_(peer), idx_(0) {
    req_.set_source(source);
    req_.set_iterator(-1);
    fetchNextChunk();
  }

  bool next(ColGroup* cols) {
    return false;
  }
};

class RemoteStore : public Source, public Sink {
private:
  PushRequest push_;
  TyphoonWorker::Stub* peer_;
  std::string name_;

  void scheduleWrite() {
    grpc::ClientContext ctx;
    PushResponse resp;
    peer_->push(&ctx, push_, &resp);
    push_.Clear();
  }

public:
  void setRemote(TyphoonWorker::Stub* peer, const std::string& name) {
    peer_ = peer;
    name_ = name;
  }

  void write(const ColGroup& cols) {
    cols.encode(push_.mutable_data());
    scheduleWrite();
  }

  Iterator* iterator() {
    return new RemoteIterator(peer_, name_);
  }
};

typedef std::vector<ColGroup> RowBatch;

class MemIterator : public Iterator {
public:
  typename RowBatch::const_iterator cur_, end_;
  MemIterator(const RowBatch& m) {
    cur_ = m.begin();
    end_ = m.end();
  }

  bool next(ColGroup* cols) {
    if (cur_ == end_) {
      return false;
    }
    *cols = *cur_++;
    return true;
  }
};

class MemStore : public Source, public Sink {
private:
  RowBatch m_;

public:
  MemStore() {
  }

  virtual Iterator* iterator() {
    return new MemIterator(m_);
  }

  virtual void write(const ColGroup& rows) {
    m_.push_back(rows);
  }
};

#endif
