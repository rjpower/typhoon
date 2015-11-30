#include "typhoon/common.h"
#include "typhoon/registry.h"
#include "typhoon/sinks.h"

RegistryHelper<Base, TextSink<std::string, int64_t>> register_si("TextSink<std::string, int64_t>");
RegistryHelper<Base, TextSink<std::string, std::string>> register_ss("TextSink<std::string, std::string>");
RegistryHelper<Base, TextSink<int64_t, int64_t>> register_ii("TextSink<int64_t, int64_t>");
