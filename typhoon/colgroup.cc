#include "typhoon/colgroup.h"


void Column::order(std::vector<uint32_t> *indices) const {}

size_t Column::size() const {
  if (this->type == INVALID) { return 0; }
  return typeFromCode(this->type).size(*this);
}

void Column::clear() {
  if (this->type == INVALID) { return; }
  return typeFromCode(this->type).clear(this);
}

void Column::dealloc() {
  if (this->type == INVALID) { return; }
  return typeFromCode(this->type).dealloc(this);
}
