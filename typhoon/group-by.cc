#ifndef TYPHOON_GROUPBY_H
#define TYPHOON_GROUPBY_H

#include <typhoon/types.h>

using std::vector;

// GroupBy: Sort an input Table by a set of columns; identical values are
// fused via a combiner function.
template <class T>
class GroupBy : public TableOp {
public:
  void apply(Context* ctx) {
    return sort() + combine();
  }
};

template <class T>
class Combine: public TableOp {
  void apply(Context* ctx) {

  }
};
#endif
