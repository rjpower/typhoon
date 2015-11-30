#include "typhoon/registry.h"
#include "typhoon/common.h"
#include "typhoon/mapreduce.h"

#include <algorithm>

#define LOG_EVERY_N(n, msg...)\
  do {\
    static uint64_t cnt_ = 0;\
    if (cnt_++ % n == 0) {\
      gpr_log(GPR_INFO, msg);\
    }\
  } while(0)

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

class BlockIterator : public IteratorT<int64_t, std::string> {
  FILE* f_;
  std::string filename_;
  uint64_t pos_;
  uint64_t start_;
  uint64_t stop_;
  char buf_[1048576];

  public:
  BlockIterator(const std::string& filename, uint64_t start, uint64_t stop) :
      filename_(filename), pos_(start), start_(start), stop_(stop) {
    f_ = fopen(filename.c_str(), "r");
    fseek(f_, start_, SEEK_SET);
  }

  virtual ~BlockIterator() {
    fclose(f_);
  }

  bool next(int64_t* pos, std::string* word) {
    if (pos_ == stop_) {
      return false;
    }

    uint64_t bufSize = sizeof(buf_) - 1;
    uint64_t bytesToRead = std::min(bufSize, stop_ - pos_);
    int res = fread(buf_, 1, bytesToRead, f_);
    if (res == 0) {
      return false;
    }
    pos_ += res;

    LOG_EVERY_N(10, "Reading... %ld", pos_ - start_);

    *pos = pos_;
    word->assign(buf_);
    return true;
  }
};

template <class T>
class SimpleSource : public SourceT<int64_t, std::string> {
  bool ready() {
    return true;
  }

  Iterator* iterator() {
    return new T(
        this->split_.filename(),
        this->split_.start(),
        this->split_.end()
    );
  }
};

class BlockSource : public SimpleSource<BlockIterator> {
public:
};

class BlockInput: public MapInputT<int64_t, std::string> {
public:
  virtual std::shared_ptr<Source> createSource(const FileSplit& split) {
    return std::make_shared<BlockSource>();
  }

  FileSplits computeSplits() {
    FileSplits splits;
    FileSplit* split = splits.add_split();
    split->set_filename("./testdata/hs_alt_CHM1_1.1.chr1.fa");
    return splits;
  }
};


class LineIterator : public IteratorT<int64_t, std::string> {
private:
  FILE* f_;
   std::string filename_;
   uint64_t pos_;
   uint64_t start_;
   uint64_t stop_;
   char buf_[1048576];

 public:
   LineIterator(const std::string& filename, uint64_t start, uint64_t stop) :
       filename_(filename), pos_(start), start_(start), stop_(stop) {
     f_ = fopen(filename.c_str(), "r");
     fseek(f_, start_, SEEK_SET);
   }

   virtual ~LineIterator() {
     fclose(f_);
   }

  bool next(int64_t* pos, std::string* word) {
    if (pos_ >= stop_) {
      return false;
    }

    char buf[256];
    uint64_t bufSize = sizeof(buf_) - 1;
    uint64_t bytesToRead = std::min(bufSize, stop_ - pos_);
    char* res = fgets(buf_, bytesToRead, f_);
    if (res == NULL) {
      return false;
    }

    *pos = pos_;
    word->assign(buf);
    return true;
  }
};

class LineSource : public SimpleSource<LineIterator> {
public:
};

class LineInput: public MapInputT<int64_t, std::string> {
public:
  virtual std::shared_ptr<Source> createSource(const FileSplit& split) {
    return std::make_shared<LineSource>();
  }

  FileSplits computeSplits() {
    FileSplits splits;
    FileSplit* split = splits.add_split();
    split->set_filename("./testdata/hs_alt_CHM1_1.1.chr1.fa");
    return splits;
  }
};


REGISTER(Base, WordSource);
REGISTER(Base, LineSource);
REGISTER(Base, BlockSource);
