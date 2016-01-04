#ifndef TYPHOON_TYPES_H
#define TYPHOON_TYPES_H

#include <vector>
#include <string>
#include <memory>

#include "thrust/host_vector.h"

namespace typhoon {

class MemoryBuffer;

template <class T>
using Ptr = std::shared_ptr<T>;

template <class T>
using Vec = thrust::host_vector<T>;

using Str = std::string;

struct StringPiece {
  const char* data;
  size_t len;

  size_t size() const {
    return len;
  }

  StringPiece(const Str& str) : data(str.data()), len(str.size()) {}
  StringPiece() : data(NULL), len(0) {}
  StringPiece(const char* str, size_t sz) : data(str), len(sz) {}

  const char* begin() const { return data; }
  const char* end() const { return data + len; }
};

template <class DstType, class SrcType>
DstType& Cast(SrcType& v) {
  DstType* dst = dynamic_cast<DstType*>(&v);
  assert(dst != NULL);
  return *dst;
}

template <class DstType, class SrcType>
const DstType& Cast(const SrcType& v) {
  const DstType* dst = dynamic_cast<const DstType*>(v);
  assert(dst != NULL);
  return *dst;
}

}
#endif
