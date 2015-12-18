#include "typhoon/table.h"
#include "typhoon/inputs.h"

using namespace typhoon;

int main() {
  auto source = Ptr<Table>(NewBlockSource("./testdata/hs_alt_CHM1_1.1_chr1.fa", 0, 100000));
  auto series = Ptr<Table>(NewSeries("content", 4));
  series->connect({source});

  auto combiner = new CountCombiner("content");
  auto groupBy = NewGroupBy("content", combiner);
  groupBy->connect({series});

  auto it = groupBy->iterator();
  while (!it->done()) {
    auto rows = it->value();
    fprintf(stderr, "Result: %d\n", rows.length());
    for (auto name: rows.names()) {
      fprintf(stderr, "Name: %s\n", name.c_str());
    }
    for (auto i = 0; i < rows.length(); ++i) {
      for (auto name: rows.names()) {
        fprintf(stderr, "%s: %s\n",
            name.c_str(),
            rows.columnByName(name)->toString(i).c_str());
      }
    }
    it->next();
  }
}
