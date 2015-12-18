#ifndef TYPHOON_INPUTS_H
#define TYPHOON_INPUTS_H

#include <typhoon/table.h>

namespace typhoon {
Table* NewBlockSource(const std::string& file, int64_t start, int64_t end);
Table* NewLineSource(const std::string& file, int64_t start, int64_t end);
Table* NewWordSource(const std::string& file, int64_t start, int64_t end);
}
#endif
