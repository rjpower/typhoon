#include <typhoon/mapreduce.h>

class CountReducer: public Reducer {
public:
  void init(Column* c) { c->setType(UINT64); }

  void combine(
      const Column& src,
      Column* dst,
      const std::vector<uint32_t>& indices) {
    dst->push<uint64_t>(indices.size());
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

  // buffer.groupBy("key", this);
  // combine using "this"
  // output to final sink
  // sink->write(rows);
}
