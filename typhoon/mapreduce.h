#ifndef TYPHOON_MR_H
#define TYPHOON_MR_H

#include "typhoon/common.h"
#include "typhoon/registry.h"

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

class Mapper: public Task {
public:
  virtual ~Mapper() {
  }
  virtual void mapShard(Source* input) = 0;

  virtual void run(std::vector<Source::Ptr> sources, std::vector<Sink::Ptr> sinks) {
    sink_ = sinks[0];
    output_.data.push_back(Column("key"));
    output_.data.push_back(Column("value"));
    mapShard(sources[0].get());

    flush();
  }

  template <class K, class V>
  void put(const K& k, const V& v) {
    output_.data[0].setType(TypeHelper<K>::code());
    output_.data[1].setType(TypeHelper<V>::code());
    output_.data[0].push(k);
    output_.data[1].push(v);
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
  static RegistryHelper<Task, Klass> registerReducer_ ## Klass(#Klass);

#define REGISTER_MAPPER(Klass)\
  static RegistryHelper<Task, Klass> registerMapper_ ## Klass(#Klass);


#endif
