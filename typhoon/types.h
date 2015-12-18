#ifndef TYPHOON_TYPES_H
#define TYPHOON_TYPES_H

#include <vector>
#include <string>
#include <memory>

namespace typhoon {

template <class T>
using Ptr = std::shared_ptr<T>;

template <class T>
using Vec = std::vector<T>;

using Str = std::string;

struct StringPiece {
  const char* data;
  size_t len;

  StringPiece(const Str& str) : data(str.data()), len(str.size()) {}
  StringPiece() : data(NULL), len(0) {}
  StringPiece(const char* str, size_t sz) : data(str), len(sz) {}
};


}
#endif
