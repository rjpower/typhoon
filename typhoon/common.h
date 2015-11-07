#ifndef TYPHOON_COMMON_H
#define TYPHOON_COMMON_H

#include <vector>
#include <map>
#include <string>
#include <unordered_map>

#include "typhoon/typhoon.pb.h"
#include "typhoon/registry.h"

class MapInput {
public:
  MapInput() {}
  virtual ~MapInput() {}
};

class Combiner {
};

template <class V>
class CombinerT : public Combiner {
public:
  virtual void combine(V& current, const V& update) = 0;
};

template <class T>
class SumCombiner: public CombinerT<T> {
  void combine(T& current, const T& update) {
    current += update;
  }
};

class Reader {
public:
  virtual ~Reader() {}
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
  Store* store_;
  virtual ~Mapper() {}
  virtual void mapShard(MapInput* shard, const FileSplit& split) = 0;
};

template <class K, class V>
class ReaderT : public Reader {
public:
  virtual bool next(K* k, V* v) = 0;
};

template <class K, class V>
class MapInputT : public MapInput {
public:
  virtual ~MapInputT() {}
  virtual std::shared_ptr<Reader> createReader(const FileSplit& split) = 0;
  virtual FileSplits computeSplits() = 0;
};

class WordInput: public MapInputT<int64_t, std::string> {
public:
  class WordReader: public ReaderT<int64_t, std::string> {
  public:
    FILE* f;

    WordReader() {
      f = fopen("./testdata/shakespeare.txt", "r");
    }

    virtual ~WordReader() {
      fclose(f);
    }

    bool next(int64_t* pos, std::string* word) {
      char buf[256];
      int res = fscanf(f, "%s", buf);

      word->assign(buf);
      if (res == EOF) {
        return false;
      }
      return true;
    }
  };

  virtual std::shared_ptr<Reader> createReader(const FileSplit& split) {
    return std::make_shared<WordReader>();
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
  virtual void write(const K& k, const V& v) = 0;
};

template <class T>
struct DataWrangler {
  static const T& read(const DataList& data, int idx);
};

template <>
struct DataWrangler<int32_t> {
  static const int size(const DataList& data) { return data.dint32_size(); }
  static const int32_t read(const DataList& data, int idx) { return data.dint32(idx); }
  static void write(DataList& data, int32_t value) { data.add_dint32(value); }
};

template <>
struct DataWrangler<int64_t> {
  static const int size(const DataList& data) { return data.dint64_size(); }
  static const int64_t read(const DataList& data, int idx) { return data.dint64(idx); }
  static void write(DataList& data, int64_t value) { data.add_dint64(value); }
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
  static const std::string read(const DataList& data, int idx) { return data.dstring(idx); }
  static void write(DataList& data, const std::string& value) { data.add_dstring(value); }
};


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
      DataWrangler<K>::write(*writers[partition].mutable_keys(), k);
      DataWrangler<V>::write(*writers[partition].mutable_values(), v);
    }
  }

  void applyUpdates(const ShuffleRequest& reader) {
    const DataList& keys = reader.keys();
    const DataList& vals = reader.values();

    for (int i = 0; i < DataWrangler<K>::size(keys); ++i) {
      this->write(DataWrangler<K>::read(keys, i), DataWrangler<V>::read(vals, i));
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
  virtual ~MapperT() {}
  virtual void mapShard(MapInput* inputType, const FileSplit& split) = 0;

  void put(const KOut& k, const VOut& v) {
    auto typedStore = (StoreT<KOut, VOut>*)store_;
    typedStore->write(k, v);
  }
};

typedef Mapper* (*MapperCreator)();
typedef Store* (*StoreCreator)();
typedef Writer* (*OutputCreator)(const std::string& filename);

struct TyphoonConfig {
  MapInput* mapInput;
  MapperCreator mapper;
  StoreCreator store;
  OutputCreator output;
};

static inline Mapper* createMapper(const TyphoonConfig& config) {
  Store* store = config.store();
  Mapper* mapper = config.mapper();
  mapper->store_ = store;
  return mapper;
}

#endif
