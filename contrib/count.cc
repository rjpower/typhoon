#include <memory>

#include "typhoon/mapreduce.h"
#include "typhoon/common.h"
#include "typhoon/worker.h"

class CountMapper: public MapperT<std::string, int64_t> {
public:
  void mapShard(Source* source) {
    gpr_log(GPR_INFO, "Mapping...");
    std::string word;
    int64_t pos;

    auto casted = dynamic_cast<IteratorT<int64_t, std::string>*>(source->iterator());

    int64_t idx = 0;
    while (casted->next(&pos, &word)) {
      if (idx++ % 1000 == 0) {
        gpr_log(GPR_INFO, "Mapping... %d", idx);
      }
      put(word, 1);
    }

    gpr_log(GPR_INFO, "Done...");
  }
};
REGISTER_MAPPER(CountMapper, std::string, int64_t);
