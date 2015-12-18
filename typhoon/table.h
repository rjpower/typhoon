#ifndef TYPHOON_COLUMN_H
#define TYPHOON_COLUMN_H

#include <assert.h>
#include "typhoon/types.h"

namespace typhoon {


// Provide default implementations of order()/groupby()?
class FilterExpr;

enum ColumnType {
  STR = 1,
  INT64 = 2
};

struct ColumnBuffer {
  union {
    const int64_t* i64;
    const StringPiece* str;
  } ptr;

  const void* data;
  size_t bytes;
  size_t len;

  ColumnBuffer clone();

  static ColumnBuffer EMPTY;
};

struct ColumnSchema {
  Str name;
  ColumnType type;
};


class ColumnIterator {
public:
  virtual ColumnType type() = 0;

  // Read `num` rows from the iterator.
  // The iterator retains ownership of the returned data.
  virtual ColumnBuffer read(size_t) = 0;
};

class Column {
public:
  virtual Ptr<Column> filter(FilterExpr*) {
    assert(false);
  }

  virtual Ptr<ColumnIterator> iterator() = 0;

  virtual void order() {
    assert(false);
  }

  virtual void group() {
    assert(false);
  }

  virtual size_t size() = 0;
  virtual ColumnType type() = 0;
};

struct TableBuffer {
  Vec<ColumnBuffer> cols;
  size_t size();

  static TableBuffer EMPTY;
};

struct TableSchema {
  Vec<ColumnSchema> columns;
};

class TableIterator {
public:
  virtual TableBuffer read(size_t) = 0;
};

class Table {
public:
  virtual void setInputs(const Vec<Table>& inputs) {

  }

  virtual Ptr<TableIterator> iterator(const Vec<Str>& columns);

  virtual TableSchema schema() = 0;
};

class StrColumn : public Column {
public:
  Ptr<ColumnIterator> iterator();
  void clear();
  void push_back(const Str& obj);
  ColumnType type();
  ColumnBuffer data();
  size_t size() {
    return ptrs_.size();
  }

private:
  Str bytes_;
  Vec<std::pair<size_t, size_t>> offsets_;
  Vec<StringPiece> ptrs_;
};

template <class T>
class IntColumn : public Column {
public:
  Ptr<ColumnIterator> iterator();
  void clear();
  void push_back(T obj);
  ColumnType type();
  ColumnBuffer data();

  size_t size() {
    return data_.size();
  }
private:
  Vec<T> data_;
};

}

#endif
