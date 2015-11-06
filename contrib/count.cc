#include "typhoon/master.h"
#include "typhoon/worker.h"
#include "typhoon/common.h"

class CountMapper: public MapperT<std::string, uint64_t> {
public:
  void mapShard(MapInput* inputType, const FileSplit& split) {

  }
};

WordInput input;

TyphoonConfig config = {
  .mapper = [] (Store* store) -> Mapper* { return new CountMapper; },
  .combiner = [] () -> Combiner* { return new SumCombiner<uint64_t>; },
  .mapInput = &input,
};

int main(int argc, char** argv) {
  if (Master::shouldStart(argc, argv)) {
    printf("Starting master..\n");
    Master* m = Master::start(config);
    m->run();
  } else {
    printf("Starting worker..\n");
    Worker* w = new Worker(config);
    w->run();
  }
}
