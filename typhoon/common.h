#ifndef TYPHOON_COMMON_H
#define TYPHOON_COMMON_H

#include <vector>
#include <map>
#include <string>
#include <unordered_map>

#include "typhoon/proto.capnp.h"
#include "typhoon/registry.h"

class MapInput {
};

class Combiner {
};

template <class V>
class CombinerT : public Combiner {
public:
  void combine(V& current, const V& update);
};

template <class T>
class SumCombiner: public CombinerT<T> {
  void combine(T& current, const T& update) {
    current += update;
  }
};

class Reader {
};

class Writer {
};


class Store {
public:
  virtual void partition(std::vector<ShuffleRequest::Builder>& writers) = 0;
  virtual void applyUpdates(const ShuffleRequest::Reader& r) = 0;
};

class Mapper {
public:
  virtual void mapShard(MapInput* shard, FileSplit::Reader split) = 0;
};

template <class K, class V>
class ReaderT : public Reader {
public:
  virtual bool next(K* k, V* v);
};

template <class K, class V>
class MapInputT : public MapInput {
public:
  virtual ReaderT<K, V>* createReader(FileSplit::Reader split) = 0;
  virtual FileSplits computeSplits();
};

template <class K, class V>
class WriterT : public Writer {
public:
  virtual void write(const K& k, const V& v) = 0;
};

template <class K, class V>
class StoreT : public Store, public WriterT<K, V> {
public:
  virtual void write(const K& k, const V& v);
  ReaderT<K, V>* reader();
};

template <class K, class V>
class MemStore : public StoreT<K, V> {
private:
  typedef std::unordered_map<K, V> Map;
  CombinerT<V>* combiner_;
  Map m_;

public:
  MemStore(CombinerT<V>* combiner) : combiner_(combiner) {}

  void partition(std::vector<ShuffleRequest::Builder>& writers) {
    std::vector<std::string> output;
    for (typename Map::iterator i = m_.begin(); i != m_.end(); ++i) {
      int partition = 0;
      const K& k = i->first;
      const V& v = i->second;
      output[partition].append((char*)&k, sizeof(k));
      output[partition].append((char*)&v, sizeof(v));
    }
  }

  void applyUpdates(const ShuffleRequest::Reader& reader) {
    kj::ArrayPtr<const kj::byte> bytes = reader.getData();
    const kj::byte* cur = bytes.begin();
    const kj::byte* end = bytes.end();
    while (cur < end) {
      K* k = (K*)cur;
      cur += sizeof(K);
      V* v = (V*)cur;
      cur += sizeof(V);
      write(*k, *v);
    }
  }

  void write(const K& k, const V& v) {
    if (m_.find(k) == m_.end()) {
      m_[k] = v;
    } else {
      combiner_->combine(m_[k], v);
    }
  }
};

template <class KIn, class VIn, class KOut, class VOut>
class MapperT : public Mapper {
public:
  StoreT<KOut, VOut>* output;

  virtual void map(const KIn& k, const VIn& v) {}

  virtual void mapShard(MapInput* inputType, FileSplit::Reader split) {
    KIn k;
    VIn v;
    MapInputT<KIn, VIn>* m = (MapInputT<KIn, VIn>*)inputType;
    ReaderT<KIn, VIn>* r = m->createReader(split);

    while (r->next(&k, &v)) {
      map(k, v);
    }
  }
};

typedef Mapper* (*MapperCreator)(Store* store);
typedef Store* (*StoreCreator)();
typedef Writer* (*OutputCreator)(const std::string& filename);
typedef Combiner* (*CombinerCreator)();

struct TyphoonConfig {
  MapInput* mapInput;
  MapperCreator mapper;
  StoreCreator store;
  OutputCreator output;
  CombinerCreator combiner;
};

#endif
