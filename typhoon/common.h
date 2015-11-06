#ifndef TYPHOON_COMMON_H
#define TYPHOON_COMMON_H

#include <vector>
#include <map>
#include <string>
#include <unordered_map>

#include "typhoon/typhoon.pb.h"
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
  virtual void partition(std::vector<ShuffleRequest>& writers) = 0;
  virtual void applyUpdates(const ShuffleRequest& r) = 0;
};

class Mapper {
public:
  virtual void mapShard(MapInput* shard, const FileSplit& split) = 0;
};

template <class K, class V>
class ReaderT : public Reader {
public:
  virtual bool next(K* k, V* v);
};

template <class K, class V>
class MapInputT : public MapInput {
public:
  virtual ReaderT<K, V>* createReader(const FileSplit& split) = 0;
  virtual FileSplits computeSplits();
};

class WordInput: public MapInputT<uint64_t, std::string> {
public:
  class Reader: public ReaderT<uint64_t, std::string> {
  public:
    FILE* f;

    Reader() {
      f = fopen("./testdata/shakespeare.txt", "r");
    }

    bool next(uint64_t* pos, std::string* word) {
      char buf[256];
      int res = fscanf(f, "%s", buf);

      word->assign(buf);
      if (res == EOF) {
        return false;
      }
      return true;
    }
  };

  virtual ReaderT<uint64_t, std::string>* createReader(const FileSplit& split) {
    return new Reader();
  }

  FileSplits computeSplits() {
    FileSplits splits;
    FileSplit* split = splits.add_splits();
    split->set_filename("./testdata/shakespeare.txt");
    return splits;
  }
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

template <class T>
struct DataReader {
  static const T& read(const DataList& data, int idx);
};

template <>
struct DataReader<int32_t> {
  static const int size(const DataList& data) { return data.dint32_size(); }
  static const int32_t read(const DataList& data, int idx) { return data.dint32(idx); }
};

template <>
struct DataReader<int64_t> {
  static const int size(const DataList& data) { return data.dint64_size(); }
  static const int64_t read(const DataList& data, int idx) { return data.dint64(idx); }
};

template <>
struct DataReader<double> {
  static const int size(const DataList& data) { return data.ddouble_size(); }
  static const double read(const DataList& data, int idx) { return data.ddouble(idx); }
};

template <>
struct DataReader<std::string> {
  static const int size(const DataList& data) { return data.dstring_size(); }
  static const std::string read(const DataList& data, int idx) { return data.dstring(idx); }
};


static inline void writeDatum(DataList& data, const std::string& value) { data.add_dstring(value); }
static inline void writeDatum(DataList& data, int64_t value) { data.add_dint64(value); }
static inline void writeDatum(DataList& data, int32_t value) { data.add_dint32(value); }
static inline void writeDatum(DataList& data, double value) { data.add_ddouble(value); }

template <class K, class V>
class MemStore : public StoreT<K, V> {
private:
  typedef std::unordered_map<K, V> Map;
  CombinerT<V>* combiner_;
  Map m_;

public:
  MemStore(CombinerT<V>* combiner) : combiner_(combiner) {}

  void partition(std::vector<ShuffleRequest>& writers) {
    for (typename Map::iterator i = m_.begin(); i != m_.end(); ++i) {
      int partition = 0;
      const K& k = i->first;
      const V& v = i->second;
      writeDatum(*writers[partition].mutable_keys(), k);
      writeDatum(*writers[partition].mutable_values(), v);
    }
  }

  void applyUpdates(const ShuffleRequest& reader) {
    const DataList& keys = reader.keys();
    const DataList& vals = reader.values();

    for (int i = 0; i < DataReader<K>::size(keys); ++i) {
      this->write(DataReader<K>::read(keys, i), DataReader<V>::read(vals, i));
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

template <class KOut, class VOut>
class MapperT : public Mapper {
public:
  StoreT<KOut, VOut>* output;
  virtual void mapShard(MapInput* inputType, const FileSplit& split) = 0;
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
