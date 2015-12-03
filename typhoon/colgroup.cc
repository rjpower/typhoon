#include "typhoon/colgroup.h"

Column* Column::create(const std::string& name, ColumnType c) {
  switch (c) {
    case UINT64: return new ColumnT<uint64_t>(name);
    case STR: return new ColumnT<std::string>(name);
    default: abort();
  }
  return NULL;
}

ColGroup* ColGroup::groupBy(const std::string& colName, Combiner* combiner) const {
  ColGroup* dst = combiner->init(*this);
  for (size_t i = 0; i < data.size(); ++i) {
    if (data[i]->name == colName) {
      data[i]->groupBy(*this, dst, i, combiner);
      return dst;
    }
  }

  abort();
  return NULL;
}
