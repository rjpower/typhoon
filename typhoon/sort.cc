#include "typhoon/types.h"
#include "typhoon/buffer.h"
#include "typhoon/table.h"
#include "typhoon/file.h"

namespace typhoon {

using namespace thrust;

class Arg {
public:
virtual ~Arg() {}
};

class StringArg: public Arg, public Str {
public:
};

class Context {
public:
  Arg* arg(size_t idx);

  size_t spill_threshold;
};

class HeapTable : public Table {
private:
  Vec<Ptr<Table>> groups_;
public:
  HeapTable(const Vec<Ptr<RowSet>> groups);
};


// Compute the stable sort of a table by one or more columns.
class Sort: public TableOp {
public:
  Ptr<Table> apply(Context* ctx, Table* input) {
    auto sort_col = Cast<StringArg>(ctx->arg(0));
    size_t spill_threshold = ctx->spill_threshold;

    auto it = input->iterator();
    auto rows = it->create_rowset();

    Vec<Ptr<RowSet>> spills;
    Vec<size_t> idx;

    while (it->read(rows, 1 << 20)) {
      rows->spill();
    }

    return Ptr<Table>(new HeapTable(spills));
  }
};

}
