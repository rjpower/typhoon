#ifndef TYPHOON_COLGROUP_H
#define TYPHOON_COLGROUP_H

#include <vector>
#include <string>

#include "typhoon/typhoon.pb.h"
#include "typhoon/typhoon.grpc.pb.h"

enum ColumnType {
  INVALID = -1,
  UINT8 = 1,
  UINT16,
  UINT32,
  UINT64,
  INT8,
  INT16,
  INT32,
  INT64,
  STR,
};

class Column;
class ColGroup;
using IndexVec = std::vector<uint32_t>;

class Combiner {
public:
  virtual ColGroup* init(const ColGroup& input) = 0;
  virtual void combine(
      const Column& src,
      ColGroup* dst,
      IndexVec::const_iterator st,
      IndexVec::const_iterator ed) = 0;
};

struct TypeUtil {
  virtual ColumnType type() = 0;
  virtual std::string toString(const Column& c, size_t idx) = 0;
  virtual void encode(const Column& c, DataList* d) = 0;
  virtual void decode(Column* c, const DataList& d) = 0;
  virtual void alloc(Column* c) = 0;
  virtual void dealloc(Column* c) = 0;
  virtual void clear(Column* c) = 0;
  virtual void copy(const Column& src, Column* dst, size_t idx) = 0;
  virtual void merge(const Column& src, Column* dst) = 0;
  virtual size_t size(const Column& c) = 0;

  virtual void groupBy(Combiner* c) {}
};

class Column {
public:
  std::string name;
  ColumnType type;
  union {
    std::vector<uint8_t>* u8;
    std::vector<uint16_t>* u16;
    std::vector<uint32_t>* u32;
    std::vector<uint64_t>* u64;
    std::vector<int8_t>* i8;
    std::vector<int16_t>* i16;
    std::vector<int32_t>* i32;
    std::vector<int64_t>* i64;
    std::vector<std::string>* str;
  } data;

  Column(const std::string& name) {
    this->name = name;
    this->type = INVALID;
  }

  void setType(ColumnType type);

  // Compute the indices required to shuffle this column into a sorted order.
  void order(IndexVec *indices) const;
  size_t size() const;
  void clear();
  void dealloc();

  template <class T>
  void push(const T& datum);

  template <class T>
  const std::vector<T>& get();
};

TypeUtil& typeFromCode(ColumnType);

template <class T>
struct TypeHelper {
};

#define TYPE_HELPER(CType, TypeCode, Accessor)\
  template <>\
  struct TypeHelper<CType> {\
    static ColumnType code() { return TypeCode; }\
    static std::vector<CType>& data(Column* c) { return *c->data.Accessor; }\
  }

TYPE_HELPER(uint64_t, UINT64, u64);
TYPE_HELPER(std::string, STR, str);

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

class ColGroup {
public:
  std::vector<Column> data;

  void encode(ShuffleData* out) const {
    for (size_t i = 0; i < data.size(); ++i) {
      out->add_data()->set_name(data[i].name);
      out->mutable_data(i)->set_type(data[i].type);
      const Column& c = data[i];
      typeFromCode(c.type).encode(c, out->mutable_data(i));
    }
  }

  void decode(const ShuffleData& rows) {
    data.clear();
    for (auto col: rows.data()) {
      Column c(col.name());
      c.setType((ColumnType)col.type());
      typeFromCode(c.type).decode(&c, col);
    }
  }

  void append(const ColGroup& other) {
    if (data.size() == 0) {
      for (size_t i = 0; i < other.data.size(); ++i) {
        const Column& src = other.data[i];
        Column dst = Column(src.name);
        dst.setType(src.type);
        data.push_back(dst);
      }
    }

    GPR_ASSERT(other.data.size() == data.size());

    for (size_t i = 0; i < other.data.size(); ++i) {
      typeFromCode(data[i].type).merge(other.data[i], &data[i]);
    }
  }
};

template <class T>
void Column::push(const T& datum) {
  return TypeHelper<T>::data(this).push_back(datum);
}

template <class T>
const std::vector<T>& Column::get() {
  return TypeHelper<T>::data(this);
}

inline void Column::setType(ColumnType type) {
  if (type == this->type) {
    return;
  }
  this->dealloc();
  this->type = type;
  typeFromCode(type).alloc(this);
}

template <typename T>
IndexVec argsort(const std::vector<T> &v) {
  IndexVec idx(v.size());
  for (uint32_t i = 0; i != v.size(); ++i) {
    idx[i] = i;
  }

  sort(idx.begin(), idx.end(),
       [&v](size_t i1, size_t i2) {return v[i1] < v[i2];});
  return idx;
}

template <class T>
ColGroup* groupBy(const ColGroup& src, size_t colIdx, Combiner* c) {
  ColGroup* dst = c->init(src);
  const Column& srcCol = src.data[colIdx];
  const std::vector<T> data = TypeHelper<T>::data((Column*)&srcCol);
  IndexVec indices = argsort(data);
  auto last = indices.begin();

  for (auto i = indices.begin(); i < indices.end(); ++i) {
    if (data[*i] != data[*last]) {
      c->combine(srcCol, dst, last, i);
      last = i;
    }
  }

  return dst;
}
#endif
