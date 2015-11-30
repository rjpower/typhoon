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
};

class MapOutput {
public:
};

template<class K, class V>
class MapInputT: public MapInput {
public:
  virtual ~MapInputT() {
  }
  virtual std::shared_ptr<Source> createSource(const FileSplit& split) = 0;
};

template<class T>
class SumCombiner: public CombinerT<T> {
  void combine(T& current, const T& update) {
    current += update;
  }
};

template<class KOut, class VOut>
class MapperT: public Task {
public:
  virtual ~MapperT() {
  }
  virtual void mapShard(Source* input) = 0;

  virtual void run(std::vector<Source::Ptr> sources,
      std::vector<Sink::Ptr> sinks) {
    sink_ = std::dynamic_pointer_cast<SinkT<KOut, VOut>>(sinks[0]);
    mapShard(sources[0].get());
  }

  void put(const KOut& k, const VOut& v) {
    sink_->write(k, v);
  }

protected:
  std::shared_ptr<SinkT<KOut, VOut>> sink_;
};

template<class K, class V>
class ReducerT: public Task, public CombinerT<V> {
public:
  virtual void run(std::vector<Source::Ptr> sources,
      std::vector<Sink::Ptr> sinks) {
    MemStore<K, V> memSink;
    memSink.combiner_ = this;

    K k;
    V v;
    for (auto src : sources) {
      gpr_log(GPR_INFO, "Reading from %p.  Ready? %s", src.get(), src->ready());
      while (!src->ready()) {
        gpr_log(GPR_INFO, "Not ready, sleeping...");
        sleep(1);
      }
      gpr_log(GPR_INFO, "Source became ready.");
      auto it = dynamic_cast<IteratorT<K, V>*>(src->iterator());
      while (it->next(&k, &v)) {
        memSink.write(k, v);
      }
    }

    auto sink = std::dynamic_pointer_cast<SinkT<K, V>>(sinks[0]);
    sink->combiner_ = this;
    auto it = dynamic_cast<IteratorT<K, V>*>(memSink.iterator());
    while (it->next(&k, &v)) {
      sink->write(k, v);
    }
  }
protected:
  std::shared_ptr<SinkT<K, V>> sink_;
};

#define REGISTER_REDUCER(Klass, K, V)\
  static RegistryHelper<Task, Klass> registerReducer_ ## Klass(#Klass);\
  static RegistryHelper<Combiner, Klass> registerCombiner_ ## Klass(#Klass);\
  static RegistryHelper<Base, MemStore<K, V>> registerBase_ ## Klass("MemStore<" #K ", " #V ">");

#define REGISTER_MAPPER(Klass, KOut, VOut)\
  static RegistryHelper<Task, Klass> registerTask_ ## Klass(#Klass);\
  static RegistryHelper<Sink, MemStore<KOut, VOut>> registerSink_ ## Klass("MemStore<" #KOut ", " #VOut ">");\
  static RegistryHelper<Base, MemStore<KOut, VOut>> registerBase_ ## Klass("MemStore<" #KOut ", " #VOut ">");\
  static RegistryHelper<RemoteStore, RemoteStoreT<KOut, VOut>> registerRemote_ ## Klass("RemoteStore<" #KOut ", " #VOut ">");

static inline void mapreduce(MapInput* input, MapOutput* output,
    const std::string& mapper, const std::string& reducer) {
}

#endif
