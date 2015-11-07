#include "typhoon/master.h"
#include "typhoon/worker.h"
#include "typhoon/common.h"

#include <memory>

class CountMapper: public MapperT<std::string, int64_t> {
public:
  void mapShard(MapInput* input, const FileSplit& split) {
    printf("Mapping...\n");
    auto typedInput = dynamic_cast<WordInput*>(input);
    std::shared_ptr<Reader> reader(typedInput->createReader(split));
    std::string word;
    int64_t pos;

    auto casted = std::dynamic_pointer_cast<WordInput::WordReader>(reader);

    while (casted->next(&pos, &word)) {
      printf("Read: %s\n", word.c_str());
      put(word, 1);
    }
  }
};

WordInput input;

TyphoonConfig config = {
  .store = [] () -> Store* {
    return new MemStore<std::string, int64_t>(new SumCombiner<int64_t>);
  },
  .mapper = [] () -> Mapper* { return new CountMapper; },
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
