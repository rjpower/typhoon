#include <typhoon/mapreduce.h>

class CountReducer: public Reducer {
public:
  ColGroup* init(const ColGroup& input) {
    ColGroup* c = new ColGroup();
    c->addCol(Column::create(input.col(0).name, input.col(0).type));
    c->addCol(Column::create("count", UINT64));
    return c;
  }

  void combine(
      const ColGroup& src,
      ColGroup* dst,
      IndexVec::const_iterator st,
      IndexVec::const_iterator ed) {
    dst->col(0).copy(src.col(0), *st);
    dst->col(1).as<uint64_t>().push(ed - st);
  }
};
REGISTER_REDUCER(CountReducer);

class SumReducer: public Reducer {
public:
  ColGroup* init(const ColGroup& input) {
    ColGroup* c = new ColGroup();
    c->addCol(Column::create(input.col(0).name, input.col(0).type));
    c->addCol(Column::create("count", UINT64));
    return c;
  }

  void combine(
      const ColGroup& src,
      ColGroup* dst,
      IndexVec::const_iterator st,
      IndexVec::const_iterator ed) {
    dst->col(0).copy(src.col(0), *st);
    const ColumnT<uint64_t>& valCol = src.col(1).as<uint64_t>();
    uint64_t sum = 0;
    while (st != ed) {
      sum += valCol.at(*st); ++st;
    }
    dst->col(1).as<uint64_t>().push(sum);
  }
};
REGISTER_REDUCER(SumReducer);

void Reducer::run(std::vector<Source::Ptr> sources, std::vector<Sink::Ptr> sinks) {
  Sink::Ptr sink = sinks[0];
  MemStore buffer;

  for (auto src : sources) {
    gpr_log(GPR_INFO, "Reading from %p.  Ready? %s", src.get(), src->ready());
    while (!src->ready()) {
      gpr_log(GPR_INFO, "Not ready, sleeping...");
      sleep(1);
    }
    gpr_log(GPR_INFO, "Source became ready.");
    auto it = src->iterator();
    ColGroup rows;
    while (it->next(&rows)) {
      gpr_log(GPR_INFO, "Got data: %d", rows.size());
      buffer.write(rows);
    }
  }

  ColGroup* result = buffer.data().groupBy("key", this);
  sink->write(*result);
}
