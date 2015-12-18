#include "typhoon/types.h"

namespace typhoon {

std::string toString(const std::string& s) {
  return s;
}
#define PRINTF_CONVERT(Type, Format)\
  std::string toString(const Type& f) {\
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

}
