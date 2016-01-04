#ifndef TYPHOON_MEMORY_BUFFER_H
#define TYPHOON_MEMORY_BUFFER_H

#include "typhoon/types.h"
#include "thrust/sort.h"

namespace typhoon {

class ColumnFile;

class MemoryBuffer {
public:
  // Sort in-place
  MemoryBuffer(size_t);

  // Sort this buffer in place.
  virtual void sort() = 0;

  // Compute a permutation vector from this buffer.
  // Reindexing by this vector will return values in
  // sorted order.
  virtual void sort_idx(Vec<size_t>* idx) = 0;
  virtual Ptr<ColumnFile> spill() = 0;
  virtual Ptr<ColumnFile> spill(const Vec<size_t> permute_idx) = 0;
  virtual void clear() = 0;
  virtual size_t bytes_left() = 0;
  virtual size_t size() = 0;
  virtual bool full() const = 0;
  virtual bool empty() const = 0;
};

template <class T>
class MemoryBufferT {
private:
  Vec<T> data_;
  size_t capacity_;
  typedef thrust::permutation_iterator<
    typename Vec<T>::iterator,
    Vec<size_t>::iterator
  > PermutationVec;

public:
  void sort_idx(Vec<size_t>* idx) {
    idx->assign(
        thrust::make_counting_iterator(0),
        thrust::make_counting_iterator(size())
    );

    thrust::sort_by_key(data_.begin(), data_.end(), idx->begin());
  }

  void sort() {
    std::sort(data_.begin(), data_.end());
  }

  Ptr<ColumnFile> spill() {
    return Ptr<ColumnFile>(NULL);
  }
  
  Ptr<ColumnFile> spill(const Vec<size_t>& permute_idx) {
    return Ptr<ColumnFile>(NULL);
  }

  void append(const Vec<T>& v) {
    std::copy(data_.end(), v.begin(), v.end());
  }

  void clear() {
    return data_.clear();
  }

  size_t size() {
    return data_.size();
  }

  bool empty() const {
    return data_.empty();
  }

  bool full() const {
    return data_.size() == capacity_;
  }

  size_t bytes_left() {
    return capacity_ - data_.size();
  }

  typename Vec<T>::iterator begin() { return data_.begin(); }
  typename Vec<T>::iterator end() { return data_.end(); }

  PermutationVec ordered_begin(const Vec<size_t>& idx) {
    return thrust::make_permutation_iterator(data_.begin(), idx.begin());
  }

  PermutationVec ordered_end(const Vec<size_t>& idx) {
    return thrust::make_permutation_iterator(data_.begin(), idx.end());
  }
};

}
#endif
