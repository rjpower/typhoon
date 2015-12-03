#include "typhoon/colgroup.h"


void Column::order(std::vector<uint32_t> *indices) const {}

size_t Column::size() const {
  if (this->type == INVALID) { return 0; }
  return typeFromCode(this->type).size(*this);
}

void Column::clear() {
  if (this->type == INVALID) { return; }
  return typeFromCode(this->type).clear(this);
}

void Column::dealloc() {
  if (this->type == INVALID) { return; }
  return typeFromCode(this->type).dealloc(this);
}

template <class T>
struct TypeUtilT {};

#define MAKE_TYPE_UTIL(CType, TypeVal, Accessor)\
template <>\
struct TypeUtilT<CType> : public TypeUtil {\
  ColumnType type() { return TypeVal; }\
  void dealloc(Column* c) { delete c->data.Accessor; } \
  void alloc(Column* c) {\
    c->type = TypeVal;\
    c->data.Accessor = new std::vector<CType>;\
  }\
  void clear(Column* c) { c->data.Accessor->clear(); } \
  void copy(const Column& src, Column* dst, size_t idx) {\
    dst->data.Accessor->push_back((*src.data.Accessor)[idx]);\
  }\
  void merge(const Column& src, Column* dst) {\
    dst->data.Accessor->insert(dst->data.Accessor->end(), src.data.Accessor->begin(), src.data.Accessor->end());\
  }\
  size_t size(const Column& c) { return c.data.Accessor->size(); }\
  std::string toString(const Column& c, size_t idx) { return ::toString((*c.data.Accessor)[idx]); }\
  void encode(const Column& c, DataList* d) {\
    d->set_name(c.name);\
    for (auto val: *c.data.Accessor) { *d->mutable_ ## Accessor()->Add() = val; }\
  }\
  void decode(Column* c, const DataList& d) {\
    c->name = d.name();\
    c->setType((ColumnType)d.type());\
    for (auto val: d.Accessor()) { c->data.Accessor->push_back(val); }\
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

TypeUtil& typeFromCode(ColumnType type) {
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
