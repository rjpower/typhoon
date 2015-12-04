#ifndef TYPHOON_COLGROUP_H
#define TYPHOON_COLGROUP_H

#include <vector>
#include <string>
#include <memory.h>

#include "typhoon/typhoon.pb.h"
#include "typhoon/typhoon.grpc.pb.h"

enum ColumnType {
  UINT8 = 1,
  UINT16,
  UINT32,
  UINT64,
  INT8,
  INT16,
  INT32,
  INT64,
  STR,
  INVALID = 1024,
};

class Column;
class ColGroup;

struct SortIndex {
  uint64_t idx;
  union {
    uint64_t u64;
    char str[8];
  } hint;
};

template <class T>
struct TypeUtil {
};

template <>
struct TypeUtil<uint64_t> {
  static ColumnType code() { return UINT64; }
  static size_t byteSize(const uint64_t& val) { return sizeof(val); }
  static void hint(SortIndex& idx, const uint64_t& val) {
    idx.hint.u64 = val;
  }
  static bool less(const std::vector<uint64_t>& v, SortIndex a, SortIndex b) {
    return a.hint.u64 < b.hint.u64;
  }
};

template <>
struct TypeUtil<std::string> {
  static ColumnType code() { return STR; }
  static size_t byteSize(const std::string& val) { return val.size(); }
  static bool less(const std::vector<std::string>& v, SortIndex a, SortIndex b) {
    const char* ah = a.hint.str;
    const char* bh = b.hint.str;
    for (size_t i = 0; i < 8; ++i) {
      if (ah[i] < bh[i]) { return true; }
      if (ah[i] > bh[i]) { return false; }
    }
    return v[a.idx] < v[b.idx];
  }
  static void hint(SortIndex& idx, const std::string& val) {
    bzero(idx.hint.str, 8);
    memcpy(idx.hint.str, val.data(), std::min(8ul, val.size()));
  }
};

using IndexVec = std::vector<SortIndex>;

class Combiner {
public:
  virtual ColGroup* init(const ColGroup& input) = 0;
  virtual void combine(
      const ColGroup& src,
      ColGroup* dst,
      IndexVec::const_iterator st,
      IndexVec::const_iterator ed) = 0;
};

class Value {
public:
  virtual std::string toString() = 0;
};

template <class T>
class ColumnT;

class Column {
public:
  std::string name;
  ColumnType type;
  static Column* create(const std::string& name, ColumnType type);

  virtual std::unique_ptr<Value> valueAt(size_t idx) const= 0;

  virtual void clear() = 0;
  virtual void encode(DataList* d) const = 0;
  virtual void decode(const DataList& d) = 0;
  virtual void copy(const Column& src, size_t idx) = 0;
  virtual void merge(const Column& other) = 0;
  virtual std::string toString(size_t idx) const = 0;

  virtual size_t size() const = 0;
  virtual size_t byteSize() const = 0;
  virtual void groupBy(const ColGroup& src, ColGroup* dst, size_t idx, Combiner* c) const = 0;

  template <class T>
  ColumnT<T>& as() {
    return *static_cast<ColumnT<T>*>(this);
  }

  template <class T>
  const ColumnT<T>& as() const {
    return *static_cast<const ColumnT<T>*>(this);
  }
private:
};

template <typename T>
IndexVec argsort(const std::vector<T> &v) {
  IndexVec idx(v.size());
  for (uint64_t i = 0; i != v.size(); ++i) {
    idx[i].idx = i;
    TypeUtil<T>::hint(idx[i], v[i]);
  }

  std::sort(idx.begin(), idx.end(),
       [&v](SortIndex i1, SortIndex i2) {
    return TypeUtil<T>::less(v, i1, i2);
  });
  return idx;
}

static inline std::string toString(const std::string& s) { return s; }
#define PRINTF_CONVERT(Type, Format)\
  static inline std::string toString(const Type& f) {\
    char buf[32];\
    sprintf(buf, "%" # Format, f);\
    return buf;\
  }

PRINTF_CONVERT(int64_t, lld)
PRINTF_CONVERT(int32_t, d)
PRINTF_CONVERT(int16_t, d)
PRINTF_CONVERT(int8_t, d)
PRINTF_CONVERT(uint64_t, llu)
PRINTF_CONVERT(uint32_t, u)
PRINTF_CONVERT(uint16_t, u)
PRINTF_CONVERT(uint8_t, u)
PRINTF_CONVERT(double, f)
PRINTF_CONVERT(float, f)

template <class T>
class ColumnT : public Column {
private:
  typedef std::vector<T> Vec;
  Vec data_;
  size_t bytes_;

public:
  ColumnT(const std::string& name) : bytes_(0) {
    this->name = name;
    this->type = TypeUtil<T>::code();
  }

  Vec& data() { return data_; }
  const Vec& data() const { return data_; }

  typename Vec::iterator begin() { return data_.begin(); }
  typename Vec::iterator end() { return data_.end(); }

  typename Vec::const_iterator begin() const { return data_.begin(); }
  typename Vec::const_iterator end() const { return data_.end(); }

  const T& at(size_t idx) const { return data_[idx]; }
  void push(const T& val) {
    data_.push_back(val);
    bytes_ += TypeUtil<T>::byteSize(val);
  }

  void clear() { data_.clear(); bytes_ = 0; }
  void copy(const Column& src, size_t idx) { push(src.as<T>().data_[idx]); }
  void merge(const Column& other) {
    data_.insert(data_.end(), other.as<T>().begin(), other.as<T>().end());
    bytes_ += other.as<T>().bytes_;
  }

  size_t size() const { return data_.size(); }
  size_t byteSize() const { return bytes_; }

  void groupBy(const ColGroup& src, ColGroup* dst, size_t idx, Combiner* c) const {
     groupBySort(src, dst, idx, c);
//    groupByHash(src, dst, idx, c);
  }

  void groupByHash(const ColGroup& src, ColGroup* dst, size_t idx, Combiner* c) const {
    std::unordered_map<T, std::vector<SortIndex>> groups;
    groups.reserve(data_.size() / 2);
    for (uint64_t i = 0; i < data_.size(); ++i) {
      SortIndex si;
      si.idx = i;
      si.hint.u64 = 0;
      groups[data_[i]].push_back(si);
    }

    for (auto i = groups.begin(); i != groups.end(); ++i) {
      c->combine(src, dst, i->second.begin(), i->second.end());
    }
  }


  void groupBySort(const ColGroup& src, ColGroup* dst, size_t idx, Combiner* c) const {
    IndexVec indices = argsort(data_);
    auto last = indices.begin();

    for (auto i = indices.begin(); i < indices.end(); ++i) {
      if (data_[i->idx] != data_[last->idx]) {
        c->combine(src, dst, last, i);
        last = i;
      }
    }
  }

  void encode(DataList* d) const { abort(); }
  void decode(const DataList& d) { abort(); }
  std::string toString(size_t idx) const {
    return ::toString(data_[idx]);
  }
  std::unique_ptr<Value> valueAt(size_t idx) const {
    return NULL;
  }
};

class ColGroup {
private:
  std::vector<std::shared_ptr<Column>> data;

public:
  ColGroup* groupBy(const std::string& colName, Combiner* combiner) const;

  size_t size() const {
    return data.size() > 0 ? data[0]->size() : 0;
  }

  const Column& col(size_t idx) const { return *data[idx]; }
  Column& col(size_t idx) { return *data[idx]; }

  size_t byteSize() {
    size_t bytes = 0;
    for (size_t i = 0; i < data.size(); ++i) {
      bytes += data[i]->byteSize();
    }
    return bytes;
  }

  void addCol(Column* c) {
    data.push_back(std::shared_ptr<Column>(c));
  }

  void clear() {
    data.clear();
  }

  size_t cols() const {
    return data.size();
  }

  void encode(ShuffleData* out) const {
    for (size_t i = 0; i < data.size(); ++i) {
      out->add_data()->set_name(data[i]->name);
      out->mutable_data(i)->set_type(data[i]->type);
      const Column& c = *data[i];
      c.encode(out->mutable_data(i));
    }
  }

  void decode(const ShuffleData& rows) {
    data.clear();
    for (auto col: rows.data()) {
      Column *c = Column::create(col.name(), (ColumnType)col.type());
      c->decode(col);
    }
  }

  void append(const ColGroup& other) {
    if (data.size() == 0) {
      for (size_t i = 0; i < other.data.size(); ++i) {
        const std::shared_ptr<Column> src = other.data[i];
        data.push_back(
            std::shared_ptr<Column>(Column::create(src->name, src->type))
        );
      }
    }

    GPR_ASSERT(other.data.size() == data.size());

    for (size_t i = 0; i < other.data.size(); ++i) {
      data[i]->merge(*other.data[i]);
    }
  }
};

#endif
