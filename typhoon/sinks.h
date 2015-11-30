#include "typhoon/common.h"
#include <stdio.h>

template <class From>
std::string to_string(const From& f);

#define PRINTF_CONVERT(Type, Format)\
  template <>\
  inline std::string to_string(const Type& f) {\
    char buf[32];\
    sprintf(buf, "%" # Format, f);\
    return buf;\
  }

PRINTF_CONVERT(int64_t, lld)
PRINTF_CONVERT(int32_t, d)
PRINTF_CONVERT(uint64_t, llu)
PRINTF_CONVERT(uint32_t, u)
PRINTF_CONVERT(double, f)
PRINTF_CONVERT(float, f)

template <>
inline std::string to_string(const std::string& s) {
  return s;
}

template <class K, class V>
class TextSink : public SinkT<K, V> {
private:
  FILE* out_;

public:
  TextSink() {
    out_ = fopen("./testdata/counts.txt", "w");
  }

  virtual ~TextSink() {
    fclose(out_);
  }

  void write(const K& k, const V& v) {
    fprintf(out_, "%s %s\n", to_string(k).c_str(), to_string(v).c_str());
  }

  virtual void flush() {
    fflush(out_);
  }
};
