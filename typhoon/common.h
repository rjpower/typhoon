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

static const int kShuffleBufferSize = 1e9;
static const int kPullBlockSize = 4096;

// This is very ugly, but serves the purpose of giving us a way to read/write common
// types from our messages without too much pain.
template <class T>
struct DataWrangler {
  static const T& read(const DataList& data, int idx);
  static void write(DataList& data, T value) { data.add_dint32(value); }

  static const int size(const DataList& data);
  static const size_t partition(T value, size_t numPartitions) { return value % numPartitions; }
};

template <>
struct DataWrangler<int32_t> {
  static const int size(const DataList& data) { return data.dint32_size(); }
  static const int32_t read(const DataList& data, int idx) { return data.dint32(idx); }
  static void write(DataList& data, int32_t value) { data.add_dint32(value); }
  static const size_t partition(int32_t value, size_t numPartitions) { return value % numPartitions; }
};

template <>
struct DataWrangler<int64_t> {
  static const int size(const DataList& data) { return data.dint64_size(); }
  static const int64_t read(const DataList& data, int idx) { return data.dint64(idx); }
  static void write(DataList& data, int64_t value) { data.add_dint64(value); }
  static const size_t partition(int64_t value, size_t numPartitions) { return value % numPartitions; }
};

template <>
struct DataWrangler<double> {
  static const int size(const DataList& data) { return data.ddouble_size(); }
  static const double read(const DataList& data, int idx) { return data.ddouble(idx); }
  static void write(DataList& data, double value) { data.add_ddouble(value); }
};

template <>
struct DataWrangler<std::string> {
  static const int size(const DataList& data) { return data.dstring_size(); }
  static const std::string& read(const DataList& data, int idx) { return data.dstring(idx); }
  static void write(DataList& data, const std::string& value) { data.add_dstring(value); }

  static const size_t partition(const std::string& value, size_t numPartitions) {
    // TODO(better hash)
    static std::hash<std::string> hash_fn;
    return hash_fn(value) % numPartitions;
  }
};

class Iterator {
public:
  virtual ~Iterator() {}
  virtual bool copy(ShuffleData* data, size_t maxElements) = 0;
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

class Combiner {
};

class Sink : virtual public Base {
public:
  Combiner* combiner_;
  typedef std::shared_ptr<Sink> Ptr;
  virtual ~Sink() {}

  virtual void applyUpdates(const ShuffleData& r) = 0;
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

template <class K, class V>
class IteratorT : public Iterator {
public:
  virtual bool next(K* k, V* v) = 0;

  void partition(std::vector<ShuffleData>& writers) {
    K k;
    V v;
    while (next(&k, &v)) {
      size_t shard = DataWrangler<K>::partition(k, writers.size());
      DataWrangler<K>::write(*writers[shard].mutable_keys(), k);
      DataWrangler<V>::write(*writers[shard].mutable_values(), v);
    }
  }

  virtual bool copy(ShuffleData* data, size_t maxElements) {
    K k;
    V v;

    DataList* keys = data->mutable_keys();
    DataList* vals = data->mutable_values();

    size_t i = 0;

    while (true) {
      if (maxElements > 0 && i++ >= maxElements) {
        return true;
      }

      if (!this->next(&k, &v)) {
        return false;
      }

      DataWrangler<K>::write(*keys, k);
      DataWrangler<V>::write(*vals, v);
    }

    return true;
  }
};

template <class K, class V>
class SourceT: public Source {
public:
  virtual ~SourceT() {}
};

template <class V>
class CombinerT : public Combiner {
public:
  virtual void combine(V& current, const V& update) = 0;
};

template <class K, class V>
class SinkT : public Sink {
public:
  virtual void write(const K& k, const V& v) = 0;
  virtual V& read(const K& k) {
    abort();
    V* v = NULL;
    return *v;
  }

  void applyUpdates(const ShuffleData& reader) {
    const DataList& keys = reader.keys();
    const DataList& vals = reader.values();

    for (int i = 0; i < DataWrangler<K>::size(keys); ++i) {
      this->write(DataWrangler<K>::read(keys, i), DataWrangler<V>::read(vals, i));
    }
  }
};

class RemoteStore {
public:
  virtual void setRemote(TyphoonWorker::Stub* peer, const std::string& name) = 0;
};

template <class K, class V>
class RemoteIterator : public IteratorT<K, V> {
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

  bool next(K* k, V* v) {
    if (DataWrangler<K>::size(buffer_.data().keys()) == idx_) {
      if (buffer_.done()) {
        return false;
      }

      fetchNextChunk();

      // Handle the (odd) case of an empty, non-first chunk.
      if (buffer_.done() && DataWrangler<K>::size(buffer_.data().keys()) == 0) {
        return false;
      }
    }

    *k = DataWrangler<K>::read(buffer_.data().keys(), idx_);
    *v = DataWrangler<V>::read(buffer_.data().values(), idx_);

    ++idx_;
    return true;
  }
};

// How to handle a typed remote store?
template <class K, class V>
class RemoteStoreT : public SourceT<K, V>, public SinkT<K, V>, public RemoteStore {
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

  void write(const K& k, const V& v) {
    ShuffleData* data = push_.mutable_data();
    DataWrangler<K>::write(*data->mutable_keys(), k);
    DataWrangler<V>::write(*data->mutable_values(), v);

    if (DataWrangler<K>::size(data->keys()) > kShuffleBufferSize) {
      scheduleWrite();
    }
  }

  Iterator* iterator() {
    return new RemoteIterator<K, V>(peer_, name_);
  }
};

template <class K, class V>
class StoreT : public SinkT<K, V>, public SourceT<K, V> {
};

#include "google/dense_hash_map"

template <class K, class V>
using MapT = std::unordered_multimap<K, V>;
//using MapT = google::dense_hash_map<K, V>;

template <class K, class V>
class MemIterator : public IteratorT<K, V> {
public:
  typedef MapT<K, V> Map;
  typename Map::const_iterator cur_, end_;
  MemIterator(const Map& m) {
    cur_ = m.begin();
    end_ = m.end();
  }

  bool next(K* k, V* v) {
    if (cur_ == end_) {
      return false;
    }
    *k = cur_->first;
    *v = cur_->second;
    ++cur_;
    return true;
  }
};

template <class K>
struct EmptyKey {
  static K get() {
    return K();
  }
};

template <>
struct EmptyKey<int64_t> {
  static int64_t get() {
    return -1;
  }
};

template <class K, class V>
class MemStore : public StoreT<K, V> {
private:
  typedef MapT<K, V> Map;
  Map m_;

public:
  MemStore() {
    this->combiner_ = NULL;
//    this->m_.set_empty_key(EmptyKey<K>::get());
  }

  virtual Iterator* iterator() {
    return new MemIterator<K, V>(m_);
  }

  void write(const K& k, const V& v) {
    if (this->combiner_ == NULL) {
      m_.insert(std::make_pair(k, v));
    } else {
      auto res = m_.find(k);
      if (res == m_.end()) {
        m_.insert(std::make_pair(k, v));
      } else {
        static_cast<CombinerT<V>*>(this->combiner_)->combine(res->second, v);
      }
    }
  }
};

#endif
