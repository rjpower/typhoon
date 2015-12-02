#include <memory>

#include "typhoon/mapreduce.h"
#include "typhoon/common.h"
#include "typhoon/worker.h"

class CountMapper: public Mapper {
public:
  void mapShard(Source* source) {
    gpr_log(GPR_INFO, "Mapping...");
    auto it = source->iterator();
    ColGroup rows;
    while (it->next(&rows)) {
      this->sink_->write(rows);
    }
    gpr_log(GPR_INFO, "Done...");
  }
};
REGISTER_MAPPER(CountMapper);
