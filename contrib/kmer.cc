#include "typhoon/master.h"
#include "typhoon/worker.h"
#include "typhoon/common.h"

// TODO - handle reads via jellyfish.
class KMERMapper: public MapperT<uint64_t, uint64_t, std::string, uint32_t> {
public:
  void mapShard(MapInput* inputType, const FileSplit& split) {
    // const char* filename = split.getFilename().cStr();
    // kmers = Jellyfish::magic(filename);
    // for each kmer
    // output.write(kmer, count)
  }
};

struct KMERInput : public MapInputT<uint64_t, uint64_t> {
  virtual ReaderT<uint64_t, uint64_t>* createReader(const FileSplit& split) {
    return NULL;
  }
};

KMERInput input;

TyphoonConfig config = {
  .mapper = [] (Store* store) -> Mapper* { return new KMERMapper; },
  .combiner = [] () -> Combiner* { return new SumCombiner<uint32_t>; },
  .mapInput = &input,
};

int main(int argc, char** argv) {
  if (Master::shouldStart(argc, argv)) {
    Master* m = Master::start(config);
    m->run();
  } else {
    Worker* w = new Worker(config);
    w->run();
  }
}
