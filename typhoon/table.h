#ifndef TYPHOON_COLUMN_H
#define TYPHOON_COLUMN_H

#include <assert.h>
#include "typhoon/types.h"

namespace typhoon {

class FilterExpr;

enum ColumnType {
  STR = 1,
  INT64 = 2
};

struct ColumnSchema {
  Str name;
  ColumnType type;
};

class Column {
public:
  virtual ColumnType type() = 0;
  virtual Str name() = 0;
  virtual Ptr<MemoryBuffer> create_buffer();
};

struct RowSet {
  Vec<Ptr<MemoryBuffer>> cols;
  void spill();
};

class Iterator {
public:
  virtual bool read(Ptr<RowSet> buffer, size_t bytes) = 0;
  virtual Ptr<RowSet> create_rowset() = 0;
};

class Table {
public:
  Ptr<Column> col(const Str&);
  size_t col_idx(const Str&);
  const Vec<Ptr<Column>>& cols();

  Ptr<Table> sort(Str col);
  Ptr<Table> reduce_by_col(Str col);
  Ptr<Table> limit(size_t count);
  Ptr<Table> filter(FilterExpr expr);

  Ptr<Iterator> iterator();
};

class SpillTable: public Table {
private:
  Vec<Ptr<File>> cols_;

public:
};

class TableOp {};

}

#endif
