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

class Combiner {
public:
  virtual Column* init() = 0;
  virtual void combine(const Column& src, Column* dst, const std::vector<std::uint32_t>& indices) = 0;
};

struct TypeUtil {
  virtual std::string toString(const Column& c, size_t idx) = 0;
  virtual void encode(const Column& c, DataList* d) = 0;
  virtual void decode(Column* c, const DataList& d) = 0;
  virtual void alloc(Column* c) = 0;
  virtual void dealloc(Column* c) = 0;
  virtual void clear(Column* c) = 0;
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
  void order(std::vector<uint32_t> *indices) const;
  size_t size() const;
  void clear();
  void dealloc();

  template <class T>
  void push(const T& datum);
};

template <class T>
struct TypeUtilT {};

template <class From>
std::string toString(const From& f);

#define PRINTF_CONVERT(Type, Format)\
  template <>\
  inline std::string toString(const Type& f) {\
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

template <>
inline std::string toString(const std::string& s) {
  return s;
}

#define MAKE_TYPE_UTIL(CType, TypeVal, Accessor)\
template <>\
struct TypeUtilT<CType> : public TypeUtil {\
  static ColumnType colType() { return TypeVal; }\
  static void push(Column* c, const CType& data) { c->data.Accessor->push_back(data); }\
  void dealloc(Column* c) { delete c->data.Accessor; } \
  void clear(Column* c) { c->data.Accessor->clear(); } \
  size_t size(const Column& c) { return c.data.Accessor->size(); }\
  std::string toString(const Column& c, size_t idx) {\
    return ::toString((*c.data.Accessor)[idx]);\
  }\
  void encode(const Column& c, DataList* d) {\
    d->set_name(c.name);\
    for (auto val: *c.data.Accessor) { *d->mutable_ ## Accessor()->Add() = val; }\
  }\
  void decode(Column* c, const DataList& d) {\
    c->name = d.name();\
    c->setType((ColumnType)d.type());\
    for (auto val: d.Accessor()) { c->data.Accessor->push_back(val); }\
  }\
  void alloc(Column* c) {\
    c->type = TypeVal;\
    c->data.Accessor = new std::vector<CType>;\
  }\
}
MAKE_TYPE_UTIL(uint8_t, UINT8, u8);
MAKE_TYPE_UTIL(uint16_t, UINT16, u16);
MAKE_TYPE_UTIL(uint32_t, UINT32, u32);
MAKE_TYPE_UTIL(uint64_t, UINT64, u64);
MAKE_TYPE_UTIL(int8_t, INT8, i8);
MAKE_TYPE_UTIL(int16_t, INT16, i16);
MAKE_TYPE_UTIL(int32_t, INT32, i32);
MAKE_TYPE_UTIL(int64_t, INT64, i64);
MAKE_TYPE_UTIL(std::string, STR, str);

static inline TypeUtil& typeFromCode(ColumnType type) {
  static TypeUtilT<uint8_t> u8;
  static TypeUtilT<uint16_t> u16;
  static TypeUtilT<uint32_t> u32;
  static TypeUtilT<uint64_t> u64;
  static TypeUtilT<int8_t> i8;
  static TypeUtilT<int16_t> i16;
  static TypeUtilT<int32_t> i32;
  static TypeUtilT<int64_t> i64;
  static TypeUtilT<std::string> str;

  switch (type) {
    case UINT8: return u8;
    case UINT16: return u16;
    case UINT32: return u32;
    case UINT64: return u64;
    case INT8: return i8;
    case INT16: return i16;
    case INT32: return i32;
    case INT64: return i64;
    case STR: return str;
    default:
      GPR_ASSERT(false);
  }
}

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
};

template <class T>
void Column::push(const T& datum) {
  TypeUtilT<T>::push(this, datum);
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
std::vector<uint32_t> argsort(const std::vector<T> &v) {
  std::vector<uint32_t> idx(v.size());
  for (uint32_t i = 0; i != v.size(); ++i) {
    idx[i] = i;
  }

  sort(idx.begin(), idx.end(),
       [&v](size_t i1, size_t i2) {return v[i1] < v[i2];});
  return idx;
}

template <class T>
Column* groupBy(const Column& src, const std::vector<T>& data, Combiner* c) {
  Column* dst = c->init();
  std::vector<uint32_t> indices = argsort(data);
  uint32_t last = 0;
  for (size_t i = 1; i < indices.size(); ++i) {
    uint32_t idx = indices[i];
    uint32_t lastIdx = indices[last];
    if (data[idx] != data[lastIdx]) {
//      c->combine(src, dst, indices, last, i);
      last = i;
    }
  }

  return dst;
}
#endif
