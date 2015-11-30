#include <typhoon/mapreduce.h>

class CountStringReducer: public ReducerT<std::string, int64_t> {
public:
  void combine(int64_t& cur, const int64_t& next) {
    cur += next;
  }
};
REGISTER_REDUCER(CountStringReducer, std::string, int64_t);

class CountIntReducer: public ReducerT<int64_t, int64_t> {
public:
  void combine(int64_t& cur, const int64_t& next) {
    cur += next;
  }
};
REGISTER_REDUCER(CountIntReducer, int64_t, int64_t);
