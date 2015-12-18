#include <stdlib.h>
#include <assert.h>
#include <typhoon/table.h>

static const size_t kMaxBufferBytes = 10 * 1000 * 1000;

namespace typhoon {

template <class Transformer>
class TransformTable : public Table {
public:
  struct Iterator: public TableIterator {
  public:
    Iterator() {}
    TableBuffer next() {
      Vec<TableBuffer> rows;
      for (auto iter: iters_) {
        rows.push_back(iter.next(32));
      }

      if (rows[0].size() == 0) {
        return TableBuffer::EMPTY;
      }

      // TODO: check row sizes are the same.
      Transformer(rows, &result_);
      return result_;
    }
  private:
    Vec<Ptr<TableIterator>> iters_;
    TableBuffer result_;
  };

  virtual void init(const Vec<Ptr<Table>>& inputs) {
    inputs_ = inputs;
  }

  Ptr<TableIterator> iterator() {
    Vec<Ptr<TableIterator>> iters;
    for (auto input: inputs_) {
      iters.push_back(input.iterator());
    }

    return new Iterator(iters);
  }

private:
  TableBuffer result_;
  Vec<Ptr<Table>> inputs_;
};

ColumnBuffer bufferFromVec(const Vec<int64_t>& data, size_t offset, size_t rows) {
  ColumnBuffer result;
  result.data = (void*)&data[offset];
  result.ptr.i64 = const_cast<int64_t*>(&data[offset]);
  result.bytes = sizeof(int64_t) * rows;
  result.len = rows;
  return result;
}

template <class T>
class IntIterator : public ColumnIterator {
public:
  IntIterator(const Vec<T>& data) : data_(data), pos_(0) {}
  ColumnType type() {
    return INT64;
  }

  ColumnBuffer read(size_t rows) {
    size_t toRead = std::min(rows, data_.size() - pos_);
    if (toRead <= 0) {
      return ColumnBuffer::EMPTY;
    }

    ColumnBuffer result = bufferFromVec(data_, pos_, toRead);
    pos_ += toRead;
    return result;
  }

private:
  const Vec<T>& data_;
  size_t pos_;
};

template<class T>
inline ColumnType IntColumn<T>::type() {
  return INT64;
}

template<class T>
inline void IntColumn<T>::clear() {
  data_.clear();
}

template<class T>
inline void IntColumn<T>::push_back(T obj) {
  data_.push_back(obj);
}

template<class T>
inline Ptr<ColumnIterator> IntColumn<T>::iterator() {
  return Ptr<ColumnIterator>(new IntIterator<T>(data_));
}

template<class T>
ColumnBuffer IntColumn<T>::data() {
  return this->iterator()->read(this->size());
}

class StrIterator : public ColumnIterator {
public:
  StrIterator(const Str& data, const Vec<StringPiece>& ptr) :
    pos_(0), data_(data), ptrs_(ptr) {}
  ColumnType type() {
    return STR;
  }

  ColumnBuffer read(size_t rows) {
    size_t rowsToRead = std::min(rows, ptrs_.size() - rows);
    if (rowsToRead <= 0) {
      return ColumnBuffer::EMPTY;
    }

    ColumnBuffer result;
    result.bytes = data_.size();
    result.data = (void*)&data_[0];
    result.len = rowsToRead;
    result.ptr.str = &ptrs_[pos_];

    pos_ += rowsToRead;

    return result;
  }
private:
  size_t pos_;
  const Str& data_;
  const Vec<StringPiece>& ptrs_;
};

inline void StrColumn::clear() {
  bytes_.clear();
  ptrs_.clear();
  offsets_.clear();
}

inline void StrColumn::push_back(const Str& obj) {
  // We don't handle mutation after we're started iterating at the moment!
  assert(ptrs_.size() == 0);
  offsets_.push_back(std::make_pair(bytes_.size(), obj.size()));
  bytes_ += obj;
}

inline Ptr<ColumnIterator> StrColumn::iterator() {
  if (ptrs_.empty()) {
    ptrs_.resize(offsets_.size());
    for (auto p: offsets_) {
      ptrs_.push_back(StringPiece(&bytes_[0] + p.first, p.second));
    }
  }

  return Ptr<ColumnIterator>(new StrIterator(bytes_, ptrs_));
}

ColumnBuffer StrColumn::data() {
  return this->iterator()->read(this->size());
}

template class IntColumn<int64_t>;

}
