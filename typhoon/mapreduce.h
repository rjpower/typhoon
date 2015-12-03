#ifndef TYPHOON_MR_H
#define TYPHOON_MR_H

#include "typhoon/common.h"
#include "typhoon/registry.h"

static const int kBatchSize = 512000;

class MapInput {
public:
  MapInput() {
  }
  virtual ~MapInput() {
  }

  virtual FileSplits computeSplits() = 0;
  virtual std::shared_ptr<Source> createSource(const FileSplit& split) = 0;
};

class MapOutput {
public:
};

template <class K, class V>
class Mapper: public Task {
public:
  virtual ~Mapper() {
  }
  virtual void mapShard(Source* input) = 0;

  virtual void run(std::vector<Source::Ptr> sources, std::vector<Sink::Ptr> sinks) {
    sink_ = sinks[0];
    output_.addCol(Column::create("key", TypeUtil<K>::code()));
    output_.addCol(Column::create("value", TypeUtil<V>::code()));
    mapShard(sources[0].get());

    flush();
  }

  void put(const K& k, const V& v) {
    output_.col(0).template as<K>().push(k);
    output_.col(1).template as<V>().push(v);

    if (output_.size() > kBatchSize) {
      sink_->write(output_);
      output_.col(0).clear();
      output_.col(1).clear();
    }
  }

  void flush() {
    sink_->write(output_);
  }

protected:
  ColGroup output_;
  std::shared_ptr<Sink> sink_;
};

class Reducer: public Task, public Combiner {
public:
  virtual void run(std::vector<Source::Ptr> sources,
      std::vector<Sink::Ptr> sinks);
protected:
};

#define REGISTER_REDUCER(Klass)\
  static RegistryHelper<Combiner, Klass> registerCombiner_ ## Klass(#Klass);\
  static RegistryHelper<Task, Klass> registerReducer_ ## Klass(#Klass);

#define REGISTER_MAPPER(Klass)\
  static RegistryHelper<Task, Klass> registerMapper_ ## Klass(#Klass);


#endif
