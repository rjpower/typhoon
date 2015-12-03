#include <typhoon/mapreduce.h>

class CountReducer: public Reducer {
public:
  ColGroup* init(const ColGroup& input) {
    ColGroup* c = new ColGroup();
    c->data.push_back(Column("key"));
    c->data.push_back(Column("count"));
    c->data[0].setType(input.data[0].type);
    c->data[1].setType(UINT64);
    return c;
  }

  void combine(
      const Column& src,
      ColGroup* dst,
      IndexVec::const_iterator st,
      IndexVec::const_iterator ed) {
    typeFromCode(src.type).copy(src, &dst->data[0], *st);
    dst->data[1].push<uint64_t>(ed - st);
  }
};
REGISTER_REDUCER(CountReducer);

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
      gpr_log(GPR_INFO, "Got data: %d", rows.data[0].size());
      buffer.write(rows);
    }
  }


  ColGroup* result = groupBy<std::string>(buffer.data(), 0, this);
  sink->write(*result);
}
