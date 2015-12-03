#include "typhoon/registry.h"
#include "typhoon/common.h"

#include <algorithm>

#define LOG_EVERY_N(n, msg...)\
  do {\
    static uint64_t cnt_ = 0;\
    if (cnt_++ % n == 0) {\
      gpr_log(GPR_INFO, msg);\
    }\
  } while(0)


template <class T>
class SimpleSource : public Source {
  bool ready() {
    return true;
  }

  Iterator* iterator() {
    T* it = new T();
    it->init(
        this->split_.filename(),
        this->split_.start(),
        this->split_.end()
    );
    return it;
  }
};

class FileIterator : public Iterator {
public:
  FileIterator() : f_(NULL), batchSize_(1024) {}

  virtual void init(const std::string& filename, uint64_t start, uint64_t stop) {
    filename_ = filename;
    pos_ = start;
    start_ = start;
    stop_ = stop;
    f_ = fopen(filename.c_str(), "r");
    fseek(f_, start_, SEEK_SET);
  }

  virtual ~FileIterator() {
    fclose(f_);
  }

  bool next(ColGroup* rows) {
    rows->clear();
    rows->addCol(Column::create("offset", UINT64));
    rows->addCol(Column::create("content", STR));

    for (size_t i = 0; i < batchSize_; ++i) {
      if (pos_ >= stop_) {
        break;
      }

      if (!this->read(buf_)) {
        break;
      }

      rows->col(0).as<uint64_t>().push(pos_);
      rows->col(1).as<std::string>().push(buf_);
      pos_ += buf_.size();
    }

    gpr_log(GPR_INFO, "Reading: %llu %llu %d", pos_, stop_, rows->size());
    return rows->size() > 0;
  }
protected:
  virtual bool read(std::string& next) = 0;

  FILE* f_;
  std::string filename_;
  std::string buf_;
  uint64_t batchSize_;
  uint64_t pos_;
  uint64_t start_;
  uint64_t stop_;
};

class WordIterator : public FileIterator {
public:
  bool read(std::string& buf) {
    buf.resize(1024);
    int res = fscanf(f_, "%s", &buf[0]);
    if (res == EOF) {
      return false;
    }
    buf.resize(res);
    return true;
  }

};

class WordSource : public SimpleSource<WordIterator> {};

class BlockIterator : public FileIterator {
public:
  BlockIterator() : FileIterator() {
    batchSize_ = 1;
  }

  bool read(std::string& buf) {
    static const uint64_t kBufSize = 1 << 20;
    buf.resize(kBufSize);
    uint64_t bytesToRead = std::min(kBufSize - 1, stop_ - pos_);
    size_t bytesRead = fread(&buf[0], 1, bytesToRead, f_);
    if (bytesRead == 0) {
      return false;
    }
    buf.resize(bytesRead);
//    gpr_log(GPR_INFO, "Read: %llu, %llu, got %lu %.80s",
//        stop_ - pos_, bytesToRead, bytesRead, buf.c_str());
    return true;
  }
};

class BlockSource : public SimpleSource<BlockIterator> {
public:
};

class LineIterator : public FileIterator {
private:
  bool read(std::string& buf) {
    buf.resize(4096);
    uint64_t bytesToRead = std::min(4095ull, stop_ - pos_);
    char* res = fgets(&buf[0], bytesToRead, f_);
    if (res == NULL) {
      return false;
    }
    buf.resize(res - &buf[0]);
    return true;
  }
};

class LineSource : public SimpleSource<LineIterator> {
public:
};

REGISTER(Base, WordSource);
REGISTER(Base, LineSource);
REGISTER(Base, BlockSource);
