#include <typhoon/table.h>
#include <typhoon/types.h>

#define LOG_EVERY_N(n, msg...)\
  do {\
    static uint64_t cnt_ = 0;\
    if (cnt_++ % n == 0) {\
      gpr_log(GPR_INFO, msg);\
    }\
  } while(0)


namespace typhoon {

template <class T>
class SimpleSource : public Table {
public:
  void init(const Str& file, int64_t start, int64_t end) {
    file_ = file;
    start_ = start;
    end_ = end;
  }

  bool ready() {
    return true;
  }

  TableSchema schema() {
    TableSchema res;
    res.columns.push_back({ "offset", INT64 });
    res.columns.push_back({ "content", STR });
    return res;
  }

  Ptr<TableIterator> iterator(const Vec<Str>& columns) {
    T* it = new T();
    it->init(file_, start_, end_);
    return Ptr<TableIterator>(it);
  }

  size_t length() {
    return 0;
  }

  Vec<Str> columns() {
    Vec<Str> cols = {
        "offset",
        "content"
    };
    return cols;
  }


protected:
  Str file_;
  int64_t start_;
  int64_t end_;
};

class FileIterator : public TableIterator {
public:
  FileIterator() : f_(NULL), batchSize_(1024) {}

  virtual void init(const Str& filename, uint64_t start, uint64_t stop) {
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

  TableBuffer read(size_t numRows) {
    for (size_t i = 0; i < numRows; ++i) {
      if (pos_ >= stop_) {
        break;
      }

      if (!this->_getRow(buf_)) {
        break;
      }

      offset_.push_back(pos_);
      content_.push_back(buf_);
      pos_ += buf_.size();
    }

    rows_.cols = { offset_.data(), content_.data() };
  }

  bool done() {
    return rows_.size() == 0;
  }

  const TableBuffer& value() {
    return rows_;
  }

protected:
  virtual bool _getRow(Str& next) = 0;

  FILE* f_;
  Str filename_;
  Str buf_;
  uint64_t batchSize_;
  uint64_t pos_;
  uint64_t start_;
  uint64_t stop_;

  IntColumn<int64_t> offset_;
  StrColumn content_;

  TableBuffer rows_;
};

class WordIterator : public FileIterator {
public:
  bool _getRow(Str& buf) {
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

  bool _getRow(Str& buf) {
    static const uint64_t kBufSize = 1 << 20;
    buf.resize(kBufSize);
    uint64_t bytesToRead = std::min(kBufSize - 1, stop_ - pos_);
    size_t bytesRead = fread(&buf[0], 1, bytesToRead, f_);
    if (bytesRead == 0) {
      return false;
    }
    buf.resize(bytesRead);
    return true;
  }
};

class BlockSource : public SimpleSource<BlockIterator> {
public:
};

class LineIterator : public FileIterator {
private:
  bool _getRow(Str& buf) {
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

Table* NewBlockSource(const Str& name, int64_t start, int64_t stop) {
  auto b = new BlockSource();
  b->init(name, start, stop);
  return b;
}

}
