#include <memory>

#include "typhoon/mapreduce.h"
#include "typhoon/common.h"
#include "typhoon/worker.h"

class WordIterator : public IteratorT<int64_t, std::string> {
  FILE* f;
  public:
  WordIterator() {
    f = fopen("./testdata/shakespeare.txt", "r");
  }

  virtual ~WordIterator() {
    fclose(f);
  }

  bool next(int64_t* pos, std::string* word) {
    char buf[256];
    int res = fscanf(f, "%s", buf);

    word->assign(buf);
    if (res == EOF) {
      return false;
    }
    return true;
  }
};

class WordSource : public SourceT<int64_t, std::string> {
public:
  bool ready() {
    return true;
  }

  Iterator* iterator() {
    return new WordIterator();
  }
};

class WordInput: public MapInputT<int64_t, std::string> {
public:
  virtual std::shared_ptr<Source> createSource(const FileSplit& split) {
    return std::make_shared<WordSource>();
  }

  FileSplits computeSplits() {
    FileSplits splits;
    FileSplit* split = splits.add_split();
    split->set_filename("./testdata/shakespeare.txt");
    return splits;
  }
};

class CountMapper: public MapperT<std::string, int64_t> {
public:
  void mapShard(Source* source) {
    gpr_log(GPR_INFO, "Mapping...");
    std::string word;
    int64_t pos;

    auto casted = std::shared_ptr<WordIterator>(dynamic_cast<WordIterator*>(source->iterator()));

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

class CountReducer: public ReducerT<std::string, int64_t> {
public:
  void combine(int64_t& cur, const int64_t& next) {
    cur += next;
  }
};

int main(int argc, char** argv) {
  Worker w;
  w.run();
}

REGISTER_REDUCER(CountReducer, std::string, int64_t);
REGISTER_MAPPER(CountMapper, std::string, int64_t);
REGISTER(Base, WordSource);

RegistryHelper<Base, TextSink<std::string, int64_t>> register_("TextSink<std::string, int64_t>");

static CountMapper counter;
