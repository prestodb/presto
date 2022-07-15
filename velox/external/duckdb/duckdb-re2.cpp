// See https://raw.githubusercontent.com/duckdb/duckdb/master/LICENSE for licensing information

#include "duckdb.hpp"
#include "duckdb-internal.hpp"
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2008 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Tested by search_test.cc, exhaustive_test.cc, tester.cc

// Prog::SearchBitState is a regular expression search with submatch
// tracking for small regular expressions and texts.  Similarly to
// testing/backtrack.cc, it allocates a bitmap with (count of
// lists) * (length of prog) bits to make sure it never explores the
// same (instruction list, character position) multiple times.  This
// limits the search to run in time linear in the length of the text.
//
// Unlike testing/backtrack.cc, SearchBitState is not recursive
// on the text.
//
// SearchBitState is a fast replacement for the NFA code on small
// regexps and texts when SearchOnePass cannot be used.

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <limits>
#include <utility>




// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2018 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef UTIL_POD_ARRAY_H_
#define UTIL_POD_ARRAY_H_

#include <memory>
#include <type_traits>

namespace duckdb_re2 {

template <typename T>
class PODArray {
 public:
  static_assert(std::is_pod<T>::value,
                "T must be POD");

  PODArray()
      : ptr_() {}
  explicit PODArray(int len)
      : ptr_(std::allocator<T>().allocate(len), Deleter(len)) {}

  T* data() const {
    return ptr_.get();
  }

  int size() const {
    return ptr_.get_deleter().len_;
  }

  T& operator[](int pos) const {
    return ptr_[pos];
  }

 private:
  struct Deleter {
    Deleter()
        : len_(0) {}
    explicit Deleter(int len)
        : len_(len) {}

    void operator()(T* ptr) const {
      std::allocator<T>().deallocate(ptr, len_);
    }

    int len_;
  };

  std::unique_ptr<T[], Deleter> ptr_;
};

}  // namespace duckdb_re2

#endif  // UTIL_POD_ARRAY_H_


// LICENSE_CHANGE_END



// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2007 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef RE2_PROG_H_
#define RE2_PROG_H_

// Compiled representation of regular expressions.
// See regexp.h for the Regexp class, which represents a regular
// expression symbolically.

#include <stdint.h>
#include <functional>
#include <mutex>
#include <string>
#include <vector>
#include <type_traits>






// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2006 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef UTIL_SPARSE_ARRAY_H_
#define UTIL_SPARSE_ARRAY_H_

// DESCRIPTION
//
// SparseArray<T>(m) is a map from integers in [0, m) to T values.
// It requires (sizeof(T)+sizeof(int))*m memory, but it provides
// fast iteration through the elements in the array and fast clearing
// of the array.  The array has a concept of certain elements being
// uninitialized (having no value).
//
// Insertion and deletion are constant time operations.
//
// Allocating the array is a constant time operation
// when memory allocation is a constant time operation.
//
// Clearing the array is a constant time operation (unusual!).
//
// Iterating through the array is an O(n) operation, where n
// is the number of items in the array (not O(m)).
//
// The array iterator visits entries in the order they were first
// inserted into the array.  It is safe to add items to the array while
// using an iterator: the iterator will visit indices added to the array
// during the iteration, but will not re-visit indices whose values
// change after visiting.  Thus SparseArray can be a convenient
// implementation of a work queue.
//
// The SparseArray implementation is NOT thread-safe.  It is up to the
// caller to make sure only one thread is accessing the array.  (Typically
// these arrays are temporary values and used in situations where speed is
// important.)
//
// The SparseArray interface does not present all the usual STL bells and
// whistles.
//
// Implemented with reference to Briggs & Torczon, An Efficient
// Representation for Sparse Sets, ACM Letters on Programming Languages
// and Systems, Volume 2, Issue 1-4 (March-Dec.  1993), pp.  59-69.
//
// Briggs & Torczon popularized this technique, but it had been known
// long before their paper.  They point out that Aho, Hopcroft, and
// Ullman's 1974 Design and Analysis of Computer Algorithms and Bentley's
// 1986 Programming Pearls both hint at the technique in exercises to the
// reader (in Aho & Hopcroft, exercise 2.12; in Bentley, column 1
// exercise 8).
//
// Briggs & Torczon describe a sparse set implementation.  I have
// trivially generalized it to create a sparse array (actually the original
// target of the AHU and Bentley exercises).

// IMPLEMENTATION
//
// SparseArray is an array dense_ and an array sparse_ of identical size.
// At any point, the number of elements in the sparse array is size_.
//
// The array dense_ contains the size_ elements in the sparse array (with
// their indices),
// in the order that the elements were first inserted.  This array is dense:
// the size_ pairs are dense_[0] through dense_[size_-1].
//
// The array sparse_ maps from indices in [0,m) to indices in [0,size_).
// For indices present in the array, dense_[sparse_[i]].index_ == i.
// For indices not present in the array, sparse_ can contain any value at all,
// perhaps outside the range [0, size_) but perhaps not.
//
// The lax requirement on sparse_ values makes clearing the array very easy:
// set size_ to 0.  Lookups are slightly more complicated.
// An index i has a value in the array if and only if:
//   sparse_[i] is in [0, size_) AND
//   dense_[sparse_[i]].index_ == i.
// If both these properties hold, only then it is safe to refer to
//   dense_[sparse_[i]].value_
// as the value associated with index i.
//
// To insert a new entry, set sparse_[i] to size_,
// initialize dense_[size_], and then increment size_.
//
// To make the sparse array as efficient as possible for non-primitive types,
// elements may or may not be destroyed when they are deleted from the sparse
// array through a call to resize(). They immediately become inaccessible, but
// they are only guaranteed to be destroyed when the SparseArray destructor is
// called.
//
// A moved-from SparseArray will be empty.

// Doing this simplifies the logic below.
#ifndef __has_feature
#define __has_feature(x) 0
#endif

#include <assert.h>
#include <stdint.h>
#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
#endif
#include <algorithm>
#include <memory>
#include <utility>



namespace duckdb_re2 {

template<typename Value>
class SparseArray {
 public:
  SparseArray();
  explicit SparseArray(int max_size);
  ~SparseArray();

  // IndexValue pairs: exposed in SparseArray::iterator.
  class IndexValue;

  typedef IndexValue* iterator;
  typedef const IndexValue* const_iterator;

  SparseArray(const SparseArray& src);
  SparseArray(SparseArray&& src);

  SparseArray& operator=(const SparseArray& src);
  SparseArray& operator=(SparseArray&& src);

  // Return the number of entries in the array.
  int size() const {
    return size_;
  }

  // Indicate whether the array is empty.
  int empty() const {
    return size_ == 0;
  }

  // Iterate over the array.
  iterator begin() {
    return dense_.data();
  }
  iterator end() {
    return dense_.data() + size_;
  }

  const_iterator begin() const {
    return dense_.data();
  }
  const_iterator end() const {
    return dense_.data() + size_;
  }

  // Change the maximum size of the array.
  // Invalidates all iterators.
  void resize(int new_max_size);

  // Return the maximum size of the array.
  // Indices can be in the range [0, max_size).
  int max_size() const {
    if (dense_.data() != NULL)
      return dense_.size();
    else
      return 0;
  }

  // Clear the array.
  void clear() {
    size_ = 0;
  }

  // Check whether index i is in the array.
  bool has_index(int i) const;

  // Comparison function for sorting.
  // Can sort the sparse array so that future iterations
  // will visit indices in increasing order using
  // std::sort(arr.begin(), arr.end(), arr.less);
  static bool less(const IndexValue& a, const IndexValue& b);

 public:
  // Set the value at index i to v.
  iterator set(int i, const Value& v) {
    return SetInternal(true, i, v);
  }

  // Set the value at new index i to v.
  // Fast but unsafe: only use if has_index(i) is false.
  iterator set_new(int i, const Value& v) {
    return SetInternal(false, i, v);
  }

  // Set the value at index i to v.
  // Fast but unsafe: only use if has_index(i) is true.
  iterator set_existing(int i, const Value& v) {
    return SetExistingInternal(i, v);
  }

  // Get the value at index i.
  // Fast but unsafe: only use if has_index(i) is true.
  Value& get_existing(int i) {
    assert(has_index(i));
    return dense_[sparse_[i]].value_;
  }
  const Value& get_existing(int i) const {
    assert(has_index(i));
    return dense_[sparse_[i]].value_;
  }

 private:
  iterator SetInternal(bool allow_existing, int i, const Value& v) {
    DebugCheckInvariants();
    if (static_cast<uint32_t>(i) >= static_cast<uint32_t>(max_size())) {
      assert(false && "illegal index");
      // Semantically, end() would be better here, but we already know
      // the user did something stupid, so begin() insulates them from
      // dereferencing an invalid pointer.
      return begin();
    }
    if (!allow_existing) {
      assert(!has_index(i));
      create_index(i);
    } else {
      if (!has_index(i))
        create_index(i);
    }
    return SetExistingInternal(i, v);
  }

  iterator SetExistingInternal(int i, const Value& v) {
    DebugCheckInvariants();
    assert(has_index(i));
    dense_[sparse_[i]].value_ = v;
    DebugCheckInvariants();
    return dense_.data() + sparse_[i];
  }

  // Add the index i to the array.
  // Only use if has_index(i) is known to be false.
  // Since it doesn't set the value associated with i,
  // this function is private, only intended as a helper
  // for other methods.
  void create_index(int i);

  // In debug mode, verify that some invariant properties of the class
  // are being maintained. This is called at the end of the constructor
  // and at the beginning and end of all public non-const member functions.
  void DebugCheckInvariants() const;

  // Initializes memory for elements [min, max).
  void MaybeInitializeMemory(int min, int max) {
#if __has_feature(memory_sanitizer)
    __msan_unpoison(sparse_.data() + min, (max - min) * sizeof sparse_[0]);
#elif defined(RE2_ON_VALGRIND)
    for (int i = min; i < max; i++) {
      sparse_[i] = 0xababababU;
    }
#endif
  }

  int size_ = 0;
  PODArray<int> sparse_;
  PODArray<IndexValue> dense_;
};

template<typename Value>
SparseArray<Value>::SparseArray() = default;

template<typename Value>
SparseArray<Value>::SparseArray(const SparseArray& src)
    : size_(src.size_),
      sparse_(src.max_size()),
      dense_(src.max_size()) {
  std::copy_n(src.sparse_.data(), src.max_size(), sparse_.data());
  std::copy_n(src.dense_.data(), src.max_size(), dense_.data());
}

template<typename Value>
SparseArray<Value>::SparseArray(SparseArray&& src)
    : size_(src.size_),
      sparse_(std::move(src.sparse_)),
      dense_(std::move(src.dense_)) {
  src.size_ = 0;
}

template<typename Value>
SparseArray<Value>& SparseArray<Value>::operator=(const SparseArray& src) {
  // Construct these first for exception safety.
  PODArray<int> a(src.max_size());
  PODArray<IndexValue> b(src.max_size());

  size_ = src.size_;
  sparse_ = std::move(a);
  dense_ = std::move(b);
  std::copy_n(src.sparse_.data(), src.max_size(), sparse_.data());
  std::copy_n(src.dense_.data(), src.max_size(), dense_.data());
  return *this;
}

template<typename Value>
SparseArray<Value>& SparseArray<Value>::operator=(SparseArray&& src) {
  size_ = src.size_;
  sparse_ = std::move(src.sparse_);
  dense_ = std::move(src.dense_);
  src.size_ = 0;
  return *this;
}

// IndexValue pairs: exposed in SparseArray::iterator.
template<typename Value>
class SparseArray<Value>::IndexValue {
 public:
  int index() const { return index_; }
  Value& value() { return value_; }
  const Value& value() const { return value_; }

 private:
  friend class SparseArray;
  int index_;
  Value value_;
};

// Change the maximum size of the array.
// Invalidates all iterators.
template<typename Value>
void SparseArray<Value>::resize(int new_max_size) {
  DebugCheckInvariants();
  if (new_max_size > max_size()) {
    const int old_max_size = max_size();

    // Construct these first for exception safety.
    PODArray<int> a(new_max_size);
    PODArray<IndexValue> b(new_max_size);

    std::copy_n(sparse_.data(), old_max_size, a.data());
    std::copy_n(dense_.data(), old_max_size, b.data());

    sparse_ = std::move(a);
    dense_ = std::move(b);

    MaybeInitializeMemory(old_max_size, new_max_size);
  }
  if (size_ > new_max_size)
    size_ = new_max_size;
  DebugCheckInvariants();
}

// Check whether index i is in the array.
template<typename Value>
bool SparseArray<Value>::has_index(int i) const {
  assert(i >= 0);
  assert(i < max_size());
  if (static_cast<uint32_t>(i) >= static_cast<uint32_t>(max_size())) {
    return false;
  }
  // Unsigned comparison avoids checking sparse_[i] < 0.
  return (uint32_t)sparse_[i] < (uint32_t)size_ &&
         dense_[sparse_[i]].index_ == i;
}

template<typename Value>
void SparseArray<Value>::create_index(int i) {
  assert(!has_index(i));
  assert(size_ < max_size());
  sparse_[i] = size_;
  dense_[size_].index_ = i;
  size_++;
}

template<typename Value> SparseArray<Value>::SparseArray(int max_size) :
    sparse_(max_size), dense_(max_size) {
  MaybeInitializeMemory(size_, max_size);
  DebugCheckInvariants();
}

template<typename Value> SparseArray<Value>::~SparseArray() {
  DebugCheckInvariants();
}

template<typename Value> void SparseArray<Value>::DebugCheckInvariants() const {
  assert(0 <= size_);
  assert(size_ <= max_size());
}

// Comparison function for sorting.
template<typename Value> bool SparseArray<Value>::less(const IndexValue& a,
                                                       const IndexValue& b) {
  return a.index_ < b.index_;
}

}  // namespace duckdb_re2

#endif  // UTIL_SPARSE_ARRAY_H_


// LICENSE_CHANGE_END



// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2006 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef UTIL_SPARSE_SET_H_
#define UTIL_SPARSE_SET_H_

// DESCRIPTION
//
// SparseSet(m) is a set of integers in [0, m).
// It requires sizeof(int)*m memory, but it provides
// fast iteration through the elements in the set and fast clearing
// of the set.
//
// Insertion and deletion are constant time operations.
//
// Allocating the set is a constant time operation
// when memory allocation is a constant time operation.
//
// Clearing the set is a constant time operation (unusual!).
//
// Iterating through the set is an O(n) operation, where n
// is the number of items in the set (not O(m)).
//
// The set iterator visits entries in the order they were first
// inserted into the set.  It is safe to add items to the set while
// using an iterator: the iterator will visit indices added to the set
// during the iteration, but will not re-visit indices whose values
// change after visiting.  Thus SparseSet can be a convenient
// implementation of a work queue.
//
// The SparseSet implementation is NOT thread-safe.  It is up to the
// caller to make sure only one thread is accessing the set.  (Typically
// these sets are temporary values and used in situations where speed is
// important.)
//
// The SparseSet interface does not present all the usual STL bells and
// whistles.
//
// Implemented with reference to Briggs & Torczon, An Efficient
// Representation for Sparse Sets, ACM Letters on Programming Languages
// and Systems, Volume 2, Issue 1-4 (March-Dec.  1993), pp.  59-69.
//
// This is a specialization of sparse array; see sparse_array.h.

// IMPLEMENTATION
//
// See sparse_array.h for implementation details.

// Doing this simplifies the logic below.
#ifndef __has_feature
#define __has_feature(x) 0
#endif

#include <assert.h>
#include <stdint.h>
#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
#endif
#include <algorithm>
#include <memory>
#include <utility>



namespace duckdb_re2 {

template<typename Value>
class SparseSetT {
 public:
  SparseSetT();
  explicit SparseSetT(int max_size);
  ~SparseSetT();

  typedef int* iterator;
  typedef const int* const_iterator;

  // Return the number of entries in the set.
  int size() const {
    return size_;
  }

  // Indicate whether the set is empty.
  int empty() const {
    return size_ == 0;
  }

  // Iterate over the set.
  iterator begin() {
    return dense_.data();
  }
  iterator end() {
    return dense_.data() + size_;
  }

  const_iterator begin() const {
    return dense_.data();
  }
  const_iterator end() const {
    return dense_.data() + size_;
  }

  // Change the maximum size of the set.
  // Invalidates all iterators.
  void resize(int new_max_size);

  // Return the maximum size of the set.
  // Indices can be in the range [0, max_size).
  int max_size() const {
    if (dense_.data() != NULL)
      return dense_.size();
    else
      return 0;
  }

  // Clear the set.
  void clear() {
    size_ = 0;
  }

  // Check whether index i is in the set.
  bool contains(int i) const;

  // Comparison function for sorting.
  // Can sort the sparse set so that future iterations
  // will visit indices in increasing order using
  // std::sort(arr.begin(), arr.end(), arr.less);
  static bool less(int a, int b);

 public:
  // Insert index i into the set.
  iterator insert(int i) {
    return InsertInternal(true, i);
  }

  // Insert index i into the set.
  // Fast but unsafe: only use if contains(i) is false.
  iterator insert_new(int i) {
    return InsertInternal(false, i);
  }

 private:
  iterator InsertInternal(bool allow_existing, int i) {
    DebugCheckInvariants();
    if (static_cast<uint32_t>(i) >= static_cast<uint32_t>(max_size())) {
      assert(false && "illegal index");
      // Semantically, end() would be better here, but we already know
      // the user did something stupid, so begin() insulates them from
      // dereferencing an invalid pointer.
      return begin();
    }
    if (!allow_existing) {
      assert(!contains(i));
      create_index(i);
    } else {
      if (!contains(i))
        create_index(i);
    }
    DebugCheckInvariants();
    return dense_.data() + sparse_[i];
  }

  // Add the index i to the set.
  // Only use if contains(i) is known to be false.
  // This function is private, only intended as a helper
  // for other methods.
  void create_index(int i);

  // In debug mode, verify that some invariant properties of the class
  // are being maintained. This is called at the end of the constructor
  // and at the beginning and end of all public non-const member functions.
  void DebugCheckInvariants() const;

  // Initializes memory for elements [min, max).
  void MaybeInitializeMemory(int min, int max) {
#if __has_feature(memory_sanitizer)
    __msan_unpoison(sparse_.data() + min, (max - min) * sizeof sparse_[0]);
#elif defined(RE2_ON_VALGRIND)
    for (int i = min; i < max; i++) {
      sparse_[i] = 0xababababU;
    }
#endif
  }

  int size_ = 0;
  PODArray<int> sparse_;
  PODArray<int> dense_;
};

template<typename Value>
SparseSetT<Value>::SparseSetT() = default;

// Change the maximum size of the set.
// Invalidates all iterators.
template<typename Value>
void SparseSetT<Value>::resize(int new_max_size) {
  DebugCheckInvariants();
  if (new_max_size > max_size()) {
    const int old_max_size = max_size();

    // Construct these first for exception safety.
    PODArray<int> a(new_max_size);
    PODArray<int> b(new_max_size);

    std::copy_n(sparse_.data(), old_max_size, a.data());
    std::copy_n(dense_.data(), old_max_size, b.data());

    sparse_ = std::move(a);
    dense_ = std::move(b);

    MaybeInitializeMemory(old_max_size, new_max_size);
  }
  if (size_ > new_max_size)
    size_ = new_max_size;
  DebugCheckInvariants();
}

// Check whether index i is in the set.
template<typename Value>
bool SparseSetT<Value>::contains(int i) const {
  assert(i >= 0);
  assert(i < max_size());
  if (static_cast<uint32_t>(i) >= static_cast<uint32_t>(max_size())) {
    return false;
  }
  // Unsigned comparison avoids checking sparse_[i] < 0.
  return (uint32_t)sparse_[i] < (uint32_t)size_ &&
         dense_[sparse_[i]] == i;
}

template<typename Value>
void SparseSetT<Value>::create_index(int i) {
  assert(!contains(i));
  assert(size_ < max_size());
  sparse_[i] = size_;
  dense_[size_] = i;
  size_++;
}

template<typename Value> SparseSetT<Value>::SparseSetT(int max_size) :
    sparse_(max_size), dense_(max_size) {
  MaybeInitializeMemory(size_, max_size);
  DebugCheckInvariants();
}

template<typename Value> SparseSetT<Value>::~SparseSetT() {
  DebugCheckInvariants();
}

template<typename Value> void SparseSetT<Value>::DebugCheckInvariants() const {
  assert(0 <= size_);
  assert(size_ <= max_size());
}

// Comparison function for sorting.
template<typename Value> bool SparseSetT<Value>::less(int a, int b) {
  return a < b;
}

typedef SparseSetT<void> SparseSet;

}  // namespace duckdb_re2

#endif  // UTIL_SPARSE_SET_H_


// LICENSE_CHANGE_END



namespace duckdb_re2 {

// Opcodes for Inst
enum InstOp {
  kInstAlt = 0,      // choose between out_ and out1_
  kInstAltMatch,     // Alt: out_ is [00-FF] and back, out1_ is match; or vice versa.
  kInstByteRange,    // next (possible case-folded) byte must be in [lo_, hi_]
  kInstCapture,      // capturing parenthesis number cap_
  kInstEmptyWidth,   // empty-width special (^ $ ...); bit(s) set in empty_
  kInstMatch,        // found a match!
  kInstNop,          // no-op; occasionally unavoidable
  kInstFail,         // never match; occasionally unavoidable
  kNumInst,
};

// Bit flags for empty-width specials
enum EmptyOp {
  kEmptyBeginLine        = 1<<0,      // ^ - beginning of line
  kEmptyEndLine          = 1<<1,      // $ - end of line
  kEmptyBeginText        = 1<<2,      // \A - beginning of text
  kEmptyEndText          = 1<<3,      // \z - end of text
  kEmptyWordBoundary     = 1<<4,      // \b - word boundary
  kEmptyNonWordBoundary  = 1<<5,      // \B - not \b
  kEmptyAllFlags         = (1<<6)-1,
};

struct inst_byte_range_data_t {             // opcode == kInstByteRange
    uint8_t lo_;       //   byte range is lo_-hi_ inclusive
    uint8_t hi_;       //
    uint16_t hint_foldcase_;  // 15 bits: hint, 1 (low) bit: foldcase
    //   hint to execution engines: the delta to the
    //   next instruction (in the current list) worth
    //   exploring iff this instruction matched; 0
    //   means there are no remaining possibilities,
    //   which is most likely for character classes.
    //   foldcase: A-Z -> a-z before checking range.
} ;

class DFA;
class Regexp;

// Compiled form of regexp program.
class Prog {
 public:
  Prog();
  ~Prog();

  // Single instruction in regexp program.
  class Inst {
   public:
    // See the assertion below for why this is so.
    Inst() = default;

    // Copyable.
    Inst(const Inst&) = default;
    Inst& operator=(const Inst&) = default;

    // Constructors per opcode
    void InitAlt(uint32_t out, uint32_t out1);
    void InitByteRange(int lo, int hi, int foldcase, uint32_t out);
    void InitCapture(int cap, uint32_t out);
    void InitEmptyWidth(EmptyOp empty, uint32_t out);
    void InitMatch(int id);
    void InitNop(uint32_t out);
    void InitFail();

    // Getters
    int id(Prog* p) { return static_cast<int>(this - p->inst_.data()); }
    InstOp opcode() { return static_cast<InstOp>(out_opcode_&7); }
    int last()      { return (out_opcode_>>3)&1; }
    int out()       { return out_opcode_>>4; }
    int out1()      { DCHECK(opcode() == kInstAlt || opcode() == kInstAltMatch); return out1_; }
    int cap()       { DCHECK_EQ(opcode(), kInstCapture); return cap_; }
    int lo()        { DCHECK_EQ(opcode(), kInstByteRange); return inst_byte_range_data_.lo_; }
    int hi()        { DCHECK_EQ(opcode(), kInstByteRange); return inst_byte_range_data_.hi_; }
    int foldcase()  { DCHECK_EQ(opcode(), kInstByteRange); return inst_byte_range_data_.hint_foldcase_&1; }
    int hint()      { DCHECK_EQ(opcode(), kInstByteRange); return inst_byte_range_data_.hint_foldcase_>>1; }
    int match_id()  { DCHECK_EQ(opcode(), kInstMatch); return match_id_; }
    EmptyOp empty() { DCHECK_EQ(opcode(), kInstEmptyWidth); return empty_; }

    bool greedy(Prog* p) {
      DCHECK_EQ(opcode(), kInstAltMatch);
      return p->inst(out())->opcode() == kInstByteRange ||
             (p->inst(out())->opcode() == kInstNop &&
              p->inst(p->inst(out())->out())->opcode() == kInstByteRange);
    }

    // Does this inst (an kInstByteRange) match c?
    inline bool Matches(int c) {
      DCHECK_EQ(opcode(), kInstByteRange);
      if (foldcase() && 'A' <= c && c <= 'Z')
        c += 'a' - 'A';
      return inst_byte_range_data_.lo_ <= c && c <= inst_byte_range_data_.hi_;
    }

    // Returns string representation for debugging.
    std::string Dump();

    // Maximum instruction id.
    // (Must fit in out_opcode_. PatchList/last steal another bit.)
    static const int kMaxInst = (1<<28) - 1;

   private:
    void set_opcode(InstOp opcode) {
      out_opcode_ = (out()<<4) | (last()<<3) | opcode;
    }

    void set_last() {
      out_opcode_ = (out()<<4) | (1<<3) | opcode();
    }

    void set_out(int out) {
      out_opcode_ = (out<<4) | (last()<<3) | opcode();
    }

    void set_out_opcode(int out, InstOp opcode) {
      out_opcode_ = (out<<4) | (last()<<3) | opcode;
    }

    uint32_t out_opcode_;  // 28 bits: out, 1 bit: last, 3 (low) bits: opcode
    union {                // additional instruction arguments:
      uint32_t out1_;      // opcode == kInstAlt
                           //   alternate next instruction

      int32_t cap_;        // opcode == kInstCapture
                           //   Index of capture register (holds text
                           //   position recorded by capturing parentheses).
                           //   For \n (the submatch for the nth parentheses),
                           //   the left parenthesis captures into register 2*n
                           //   and the right one captures into register 2*n+1.

      int32_t match_id_;   // opcode == kInstMatch
                           //   Match ID to identify this match (for duckdb_re2::Set).

      struct inst_byte_range_data_t inst_byte_range_data_;

      EmptyOp empty_;       // opcode == kInstEmptyWidth
                            //   empty_ is bitwise OR of kEmpty* flags above.
    };

    friend class Compiler;
    friend struct PatchList;
    friend class Prog;
  };

  // Inst must be trivial so that we can freely clear it with memset(3).
  // Arrays of Inst are initialised by copying the initial elements with
  // memmove(3) and then clearing any remaining elements with memset(3).
  static_assert(std::is_trivial<Inst>::value, "Inst must be trivial");

  // Whether to anchor the search.
  enum Anchor {
    kUnanchored,  // match anywhere
    kAnchored,    // match only starting at beginning of text
  };

  // Kind of match to look for (for anchor != kFullMatch)
  //
  // kLongestMatch mode finds the overall longest
  // match but still makes its submatch choices the way
  // Perl would, not in the way prescribed by POSIX.
  // The POSIX rules are much more expensive to implement,
  // and no one has needed them.
  //
  // kFullMatch is not strictly necessary -- we could use
  // kLongestMatch and then check the length of the match -- but
  // the matching code can run faster if it knows to consider only
  // full matches.
  enum MatchKind {
    kFirstMatch,     // like Perl, PCRE
    kLongestMatch,   // like egrep or POSIX
    kFullMatch,      // match only entire text; implies anchor==kAnchored
    kManyMatch       // for SearchDFA, records set of matches
  };

  Inst *inst(int id) { return &inst_[id]; }
  int start() { return start_; }
  int start_unanchored() { return start_unanchored_; }
  void set_start(int start) { start_ = start; }
  void set_start_unanchored(int start) { start_unanchored_ = start; }
  int size() { return size_; }
  bool reversed() { return reversed_; }
  void set_reversed(bool reversed) { reversed_ = reversed; }
  int list_count() { return list_count_; }
  int inst_count(InstOp op) { return inst_count_[op]; }
  uint16_t* list_heads() { return list_heads_.data(); }
  void set_dfa_mem(int64_t dfa_mem) { dfa_mem_ = dfa_mem; }
  int64_t dfa_mem() { return dfa_mem_; }
  int flags() { return flags_; }
  void set_flags(int flags) { flags_ = flags; }
  bool anchor_start() { return anchor_start_; }
  void set_anchor_start(bool b) { anchor_start_ = b; }
  bool anchor_end() { return anchor_end_; }
  void set_anchor_end(bool b) { anchor_end_ = b; }
  int bytemap_range() { return bytemap_range_; }
  const uint8_t* bytemap() { return bytemap_; }

  // Lazily computed.
  int first_byte();

  // Returns string representation of program for debugging.
  std::string Dump();
  std::string DumpUnanchored();
  std::string DumpByteMap();

  // Returns the set of kEmpty flags that are in effect at
  // position p within context.
  static uint32_t EmptyFlags(const StringPiece& context, const char* p);

  // Returns whether byte c is a word character: ASCII only.
  // Used by the implementation of \b and \B.
  // This is not right for Unicode, but:
  //   - it's hard to get right in a byte-at-a-time matching world
  //     (the DFA has only one-byte lookahead).
  //   - even if the lookahead were possible, the Progs would be huge.
  // This crude approximation is the same one PCRE uses.
  static bool IsWordChar(uint8_t c) {
    return ('A' <= c && c <= 'Z') ||
           ('a' <= c && c <= 'z') ||
           ('0' <= c && c <= '9') ||
           c == '_';
  }

  // Execution engines.  They all search for the regexp (run the prog)
  // in text, which is in the larger context (used for ^ $ \b etc).
  // Anchor and kind control the kind of search.
  // Returns true if match found, false if not.
  // If match found, fills match[0..nmatch-1] with submatch info.
  // match[0] is overall match, match[1] is first set of parens, etc.
  // If a particular submatch is not matched during the regexp match,
  // it is set to NULL.
  //
  // Matching text == StringPiece(NULL, 0) is treated as any other empty
  // string, but note that on return, it will not be possible to distinguish
  // submatches that matched that empty string from submatches that didn't
  // match anything.  Either way, match[i] == NULL.

  // Search using NFA: can find submatches but kind of slow.
  bool SearchNFA(const StringPiece& text, const StringPiece& context,
                 Anchor anchor, MatchKind kind,
                 StringPiece* match, int nmatch);

  // Search using DFA: much faster than NFA but only finds
  // end of match and can use a lot more memory.
  // Returns whether a match was found.
  // If the DFA runs out of memory, sets *failed to true and returns false.
  // If matches != NULL and kind == kManyMatch and there is a match,
  // SearchDFA fills matches with the match IDs of the final matching state.
  bool SearchDFA(const StringPiece& text, const StringPiece& context,
                 Anchor anchor, MatchKind kind, StringPiece* match0,
                 bool* failed, SparseSet* matches);

  // The callback issued after building each DFA state with BuildEntireDFA().
  // If next is null, then the memory budget has been exhausted and building
  // will halt. Otherwise, the state has been built and next points to an array
  // of bytemap_range()+1 slots holding the next states as per the bytemap and
  // kByteEndText. The number of the state is implied by the callback sequence:
  // the first callback is for state 0, the second callback is for state 1, ...
  // match indicates whether the state is a matching state.
  using DFAStateCallback = std::function<void(const int* next, bool match)>;

  // Build the entire DFA for the given match kind.
  // Usually the DFA is built out incrementally, as needed, which
  // avoids lots of unnecessary work.
  // If cb is not empty, it receives one callback per state built.
  // Returns the number of states built.
  // FOR TESTING OR EXPERIMENTAL PURPOSES ONLY.
  int BuildEntireDFA(MatchKind kind, const DFAStateCallback& cb);

  // Controls whether the DFA should bail out early if the NFA would be faster.
  // FOR TESTING ONLY.
  static void TEST_dfa_should_bail_when_slow(bool b);

  // Compute bytemap.
  void ComputeByteMap();

  // Computes whether all matches must begin with the same first
  // byte, and if so, returns that byte.  If not, returns -1.
  int ComputeFirstByte();

  // Run peep-hole optimizer on program.
  void Optimize();

  // One-pass NFA: only correct if IsOnePass() is true,
  // but much faster than NFA (competitive with PCRE)
  // for those expressions.
  bool IsOnePass();
  bool SearchOnePass(const StringPiece& text, const StringPiece& context,
                     Anchor anchor, MatchKind kind,
                     StringPiece* match, int nmatch);

  // Bit-state backtracking.  Fast on small cases but uses memory
  // proportional to the product of the list count and the text size.
  bool CanBitState() { return list_heads_.data() != NULL; }
  bool SearchBitState(const StringPiece& text, const StringPiece& context,
                      Anchor anchor, MatchKind kind,
                      StringPiece* match, int nmatch);

  static const int kMaxOnePassCapture = 5;  // $0 through $4

  // Backtracking search: the gold standard against which the other
  // implementations are checked.  FOR TESTING ONLY.
  // It allocates a ton of memory to avoid running forever.
  // It is also recursive, so can't use in production (will overflow stacks).
  // The name "Unsafe" here is supposed to be a flag that
  // you should not be using this function.
  bool UnsafeSearchBacktrack(const StringPiece& text,
                             const StringPiece& context,
                             Anchor anchor, MatchKind kind,
                             StringPiece* match, int nmatch);

  // Computes range for any strings matching regexp. The min and max can in
  // some cases be arbitrarily precise, so the caller gets to specify the
  // maximum desired length of string returned.
  //
  // Assuming PossibleMatchRange(&min, &max, N) returns successfully, any
  // string s that is an anchored match for this regexp satisfies
  //   min <= s && s <= max.
  //
  // Note that PossibleMatchRange() will only consider the first copy of an
  // infinitely repeated element (i.e., any regexp element followed by a '*' or
  // '+' operator). Regexps with "{N}" constructions are not affected, as those
  // do not compile down to infinite repetitions.
  //
  // Returns true on success, false on error.
  bool PossibleMatchRange(std::string* min, std::string* max, int maxlen);

  // EXPERIMENTAL! SUBJECT TO CHANGE!
  // Outputs the program fanout into the given sparse array.
  void Fanout(SparseArray<int>* fanout);

  // Compiles a collection of regexps to Prog.  Each regexp will have
  // its own Match instruction recording the index in the output vector.
  static Prog* CompileSet(Regexp* re, RE2::Anchor anchor, int64_t max_mem);

  // Flattens the Prog from "tree" form to "list" form. This is an in-place
  // operation in the sense that the old instructions are lost.
  void Flatten();

  // Walks the Prog; the "successor roots" or predecessors of the reachable
  // instructions are marked in rootmap or predmap/predvec, respectively.
  // reachable and stk are preallocated scratch structures.
  void MarkSuccessors(SparseArray<int>* rootmap,
                      SparseArray<int>* predmap,
                      std::vector<std::vector<int>>* predvec,
                      SparseSet* reachable, std::vector<int>* stk);

  // Walks the Prog from the given "root" instruction; the "dominator root"
  // of the reachable instructions (if such exists) is marked in rootmap.
  // reachable and stk are preallocated scratch structures.
  void MarkDominator(int root, SparseArray<int>* rootmap,
                     SparseArray<int>* predmap,
                     std::vector<std::vector<int>>* predvec,
                     SparseSet* reachable, std::vector<int>* stk);

  // Walks the Prog from the given "root" instruction; the reachable
  // instructions are emitted in "list" form and appended to flat.
  // reachable and stk are preallocated scratch structures.
  void EmitList(int root, SparseArray<int>* rootmap,
                std::vector<Inst>* flat,
                SparseSet* reachable, std::vector<int>* stk);

  // Computes hints for ByteRange instructions in [begin, end).
  void ComputeHints(std::vector<Inst>* flat, int begin, int end);

 private:
  friend class Compiler;

  DFA* GetDFA(MatchKind kind);
  void DeleteDFA(DFA* dfa);

  bool anchor_start_;       // regexp has explicit start anchor
  bool anchor_end_;         // regexp has explicit end anchor
  bool reversed_;           // whether program runs backward over input
  bool did_flatten_;        // has Flatten been called?
  bool did_onepass_;        // has IsOnePass been called?

  int start_;               // entry point for program
  int start_unanchored_;    // unanchored entry point for program
  int size_;                // number of instructions
  int bytemap_range_;       // bytemap_[x] < bytemap_range_
  int first_byte_;          // required first byte for match, or -1 if none
  int flags_;               // regexp parse flags

  int list_count_;                 // count of lists (see above)
  int inst_count_[kNumInst];       // count of instructions by opcode
  PODArray<uint16_t> list_heads_;  // sparse array enumerating list heads
                                   // not populated if size_ is overly large

  PODArray<Inst> inst_;              // pointer to instruction array
  PODArray<uint8_t> onepass_nodes_;  // data for OnePass nodes

  int64_t dfa_mem_;         // Maximum memory for DFAs.
  DFA* dfa_first_;          // DFA cached for kFirstMatch/kManyMatch
  DFA* dfa_longest_;        // DFA cached for kLongestMatch/kFullMatch

  uint8_t bytemap_[256];    // map from input bytes to byte classes

  std::once_flag first_byte_once_;
  std::once_flag dfa_first_once_;
  std::once_flag dfa_longest_once_;

  Prog(const Prog&) = delete;
  Prog& operator=(const Prog&) = delete;
};

}  // namespace duckdb_re2

#endif  // RE2_PROG_H_


// LICENSE_CHANGE_END



namespace duckdb_re2 {

struct Job {
  int id;
  int rle;  // run length encoding
  const char* p;
};

class BitState {
 public:
  explicit BitState(Prog* prog);

  // The usual Search prototype.
  // Can only call Search once per BitState.
  bool Search(const StringPiece& text, const StringPiece& context,
              bool anchored, bool longest,
              StringPiece* submatch, int nsubmatch);

 private:
  inline bool ShouldVisit(int id, const char* p);
  void Push(int id, const char* p);
  void GrowStack();
  bool TrySearch(int id, const char* p);

  // Search parameters
  Prog* prog_;              // program being run
  StringPiece text_;        // text being searched
  StringPiece context_;     // greater context of text being searched
  bool anchored_;           // whether search is anchored at text.begin()
  bool longest_;            // whether search wants leftmost-longest match
  bool endmatch_;           // whether match must end at text.end()
  StringPiece* submatch_;   // submatches to fill in
  int nsubmatch_;           //   # of submatches to fill in

  // Search state
  static const int VisitedBits = 32;
  PODArray<uint32_t> visited_;  // bitmap: (list ID, char*) pairs visited
  PODArray<const char*> cap_;   // capture registers
  PODArray<Job> job_;           // stack of text positions to explore
  int njob_;                    // stack size
};

BitState::BitState(Prog* prog)
  : prog_(prog),
    anchored_(false),
    longest_(false),
    endmatch_(false),
    submatch_(NULL),
    nsubmatch_(0),
    njob_(0) {
}

// Given id, which *must* be a list head, we can look up its list ID.
// Then the question is: Should the search visit the (list ID, p) pair?
// If so, remember that it was visited so that the next time,
// we don't repeat the visit.
bool BitState::ShouldVisit(int id, const char* p) {
  int n = prog_->list_heads()[id] * static_cast<int>(text_.size()+1) +
          static_cast<int>(p-text_.begin());
  if (visited_[n/VisitedBits] & (1 << (n & (VisitedBits-1))))
    return false;
  visited_[n/VisitedBits] |= 1 << (n & (VisitedBits-1));
  return true;
}

// Grow the stack.
void BitState::GrowStack() {
  PODArray<Job> tmp(2*job_.size());
  memmove(tmp.data(), job_.data(), njob_*sizeof job_[0]);
  job_ = std::move(tmp);
}

// Push (id, p) onto the stack, growing it if necessary.
void BitState::Push(int id, const char* p) {
  if (njob_ >= job_.size()) {
    GrowStack();
    if (njob_ >= job_.size()) {
      LOG(DFATAL) << "GrowStack() failed: "
                  << "njob_ = " << njob_ << ", "
                  << "job_.size() = " << job_.size();
      return;
    }
  }

  // If id < 0, it's undoing a Capture,
  // so we mustn't interfere with that.
  if (id >= 0 && njob_ > 0) {
    Job* top = &job_[njob_-1];
    if (id == top->id &&
        p == top->p + top->rle + 1 &&
        top->rle < std::numeric_limits<int>::max()) {
      ++top->rle;
      return;
    }
  }

  Job* top = &job_[njob_++];
  top->id = id;
  top->rle = 0;
  top->p = p;
}

// Try a search from instruction id0 in state p0.
// Return whether it succeeded.
bool BitState::TrySearch(int id0, const char* p0) {
  bool matched = false;
  const char* end = text_.end();
  njob_ = 0;
  // Push() no longer checks ShouldVisit(),
  // so we must perform the check ourselves.
  if (ShouldVisit(id0, p0))
    Push(id0, p0);
  while (njob_ > 0) {
    // Pop job off stack.
    --njob_;
    int id = job_[njob_].id;
    int& rle = job_[njob_].rle;
    const char* p = job_[njob_].p;

    if (id < 0) {
      // Undo the Capture.
      cap_[prog_->inst(-id)->cap()] = p;
      continue;
    }

    if (rle > 0) {
      p += rle;
      // Revivify job on stack.
      --rle;
      ++njob_;
    }

  Loop:
    // Visit id, p.
    Prog::Inst* ip = prog_->inst(id);
    switch (ip->opcode()) {
      default:
        LOG(DFATAL) << "Unexpected opcode: " << ip->opcode();
        return false;

      case kInstFail:
        break;

      case kInstAltMatch:
        if (ip->greedy(prog_)) {
          // out1 is the Match instruction.
          id = ip->out1();
          p = end;
          goto Loop;
        }
        if (longest_) {
          // ip must be non-greedy...
          // out is the Match instruction.
          id = ip->out();
          p = end;
          goto Loop;
        }
        goto Next;

      case kInstByteRange: {
        int c = -1;
        if (p < end)
          c = *p & 0xFF;
        if (!ip->Matches(c))
          goto Next;

        if (ip->hint() != 0)
          Push(id+ip->hint(), p);  // try the next when we're done
        id = ip->out();
        p++;
        goto CheckAndLoop;
      }

      case kInstCapture:
        if (!ip->last())
          Push(id+1, p);  // try the next when we're done

        if (0 <= ip->cap() && ip->cap() < cap_.size()) {
          // Capture p to register, but save old value first.
          Push(-id, cap_[ip->cap()]);  // undo when we're done
          cap_[ip->cap()] = p;
        }

        id = ip->out();
        goto CheckAndLoop;

      case kInstEmptyWidth:
        if (ip->empty() & ~Prog::EmptyFlags(context_, p))
          goto Next;

        if (!ip->last())
          Push(id+1, p);  // try the next when we're done
        id = ip->out();
        goto CheckAndLoop;

      case kInstNop:
        if (!ip->last())
          Push(id+1, p);  // try the next when we're done
        id = ip->out();

      CheckAndLoop:
        // Sanity check: id is the head of its list, which must
        // be the case if id-1 is the last of *its* list. :)
        DCHECK(id == 0 || prog_->inst(id-1)->last());
        if (ShouldVisit(id, p))
          goto Loop;
        break;

      case kInstMatch: {
        if (endmatch_ && p != end)
          goto Next;

        // We found a match.  If the caller doesn't care
        // where the match is, no point going further.
        if (nsubmatch_ == 0)
          return true;

        // Record best match so far.
        // Only need to check end point, because this entire
        // call is only considering one start position.
        matched = true;
        cap_[1] = p;
        if (submatch_[0].data() == NULL ||
            (longest_ && p > submatch_[0].end())) {
          for (int i = 0; i < nsubmatch_; i++)
            submatch_[i] =
                StringPiece(cap_[2 * i],
                            static_cast<size_t>(cap_[2 * i + 1] - cap_[2 * i]));
        }

        // If going for first match, we're done.
        if (!longest_)
          return true;

        // If we used the entire text, no longer match is possible.
        if (p == end)
          return true;

        // Otherwise, continue on in hope of a longer match.
        // Note the absence of the ShouldVisit() check here
        // due to execution remaining in the same list.
      Next:
        if (!ip->last()) {
          id++;
          goto Loop;
        }
        break;
      }
    }
  }
  return matched;
}

// Search text (within context) for prog_.
bool BitState::Search(const StringPiece& text, const StringPiece& context,
                      bool anchored, bool longest,
                      StringPiece* submatch, int nsubmatch) {
  // Search parameters.
  text_ = text;
  context_ = context;
  if (context_.begin() == NULL)
    context_ = text;
  if (prog_->anchor_start() && context_.begin() != text.begin())
    return false;
  if (prog_->anchor_end() && context_.end() != text.end())
    return false;
  anchored_ = anchored || prog_->anchor_start();
  longest_ = longest || prog_->anchor_end();
  endmatch_ = prog_->anchor_end();
  submatch_ = submatch;
  nsubmatch_ = nsubmatch;
  for (int i = 0; i < nsubmatch_; i++)
    submatch_[i] = StringPiece();

  // Allocate scratch space.
  int nvisited = prog_->list_count() * static_cast<int>(text.size()+1);
  nvisited = (nvisited + VisitedBits-1) / VisitedBits;
  visited_ = PODArray<uint32_t>(nvisited);
  memset(visited_.data(), 0, nvisited*sizeof visited_[0]);

  int ncap = 2*nsubmatch;
  if (ncap < 2)
    ncap = 2;
  cap_ = PODArray<const char*>(ncap);
  memset(cap_.data(), 0, ncap*sizeof cap_[0]);

  // When sizeof(Job) == 16, we start with a nice round 1KiB. :)
  job_ = PODArray<Job>(64);

  // Anchored search must start at text.begin().
  if (anchored_) {
    cap_[0] = text.begin();
    return TrySearch(prog_->start(), text.begin());
  }

  // Unanchored search, starting from each possible text position.
  // Notice that we have to try the empty string at the end of
  // the text, so the loop condition is p <= text.end(), not p < text.end().
  // This looks like it's quadratic in the size of the text,
  // but we are not clearing visited_ between calls to TrySearch,
  // so no work is duplicated and it ends up still being linear.
  for (const char* p = text.begin(); p <= text.end(); p++) {
    // Try to use memchr to find the first byte quickly.
    int fb = prog_->first_byte();
    if (fb >= 0 && p < text.end() && (p[0] & 0xFF) != fb) {
      p = reinterpret_cast<const char*>(memchr(p, fb, text.end() - p));
      if (p == NULL)
        p = text.end();
    }

    cap_[0] = p;
    if (TrySearch(prog_->start(), p))  // Match must be leftmost; done.
      return true;
  }
  return false;
}

// Bit-state search.
bool Prog::SearchBitState(const StringPiece& text,
                          const StringPiece& context,
                          Anchor anchor,
                          MatchKind kind,
                          StringPiece* match,
                          int nmatch) {
  // If full match, we ask for an anchored longest match
  // and then check that match[0] == text.
  // So make sure match[0] exists.
  StringPiece sp0;
  if (kind == kFullMatch) {
    anchor = kAnchored;
    if (nmatch < 1) {
      match = &sp0;
      nmatch = 1;
    }
  }

  // Run the search.
  BitState b(this);
  bool anchored = anchor == kAnchored;
  bool longest = kind != kFirstMatch;
  if (!b.Search(text, context, anchored, longest, match, nmatch))
    return false;
  if (kind == kFullMatch && match[0].end() != text.end())
    return false;
  return true;
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2007 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Compile regular expression to Prog.
//
// Prog and Inst are defined in prog.h.
// This file's external interface is just Regexp::CompileToProg.
// The Compiler class defined in this file is private.

#include <stdint.h>
#include <string.h>
#include <unordered_map>
#include <utility>









// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2006 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef RE2_WALKER_INL_H_
#define RE2_WALKER_INL_H_

// Helper class for traversing Regexps without recursion.
// Clients should declare their own subclasses that override
// the PreVisit and PostVisit methods, which are called before
// and after visiting the subexpressions.

// Not quite the Visitor pattern, because (among other things)
// the Visitor pattern is recursive.

#include <stack>




namespace duckdb_re2 {

template<typename T> struct WalkState;

template<typename T> class Regexp::Walker {
 public:
  Walker();
  virtual ~Walker() {
    Reset();
	delete stack_;
  }

  // Virtual method called before visiting re's children.
  // PreVisit passes ownership of its return value to its caller.
  // The Arg* that PreVisit returns will be passed to PostVisit as pre_arg
  // and passed to the child PreVisits and PostVisits as parent_arg.
  // At the top-most Regexp, parent_arg is arg passed to walk.
  // If PreVisit sets *stop to true, the walk does not recurse
  // into the children.  Instead it behaves as though the return
  // value from PreVisit is the return value from PostVisit.
  // The default PreVisit returns parent_arg.
  virtual T PreVisit(Regexp* re, T parent_arg, bool* stop);

  // Virtual method called after visiting re's children.
  // The pre_arg is the T that PreVisit returned.
  // The child_args is a vector of the T that the child PostVisits returned.
  // PostVisit takes ownership of pre_arg.
  // PostVisit takes ownership of the Ts
  // in *child_args, but not the vector itself.
  // PostVisit passes ownership of its return value
  // to its caller.
  // The default PostVisit simply returns pre_arg.
  virtual T PostVisit(Regexp* re, T parent_arg, T pre_arg,
                      T* child_args, int nchild_args);

  // Virtual method called to copy a T,
  // when Walk notices that more than one child is the same re.
  virtual T Copy(T arg);

  // Virtual method called to do a "quick visit" of the re,
  // but not its children.  Only called once the visit budget
  // has been used up and we're trying to abort the walk
  // as quickly as possible.  Should return a value that
  // makes sense for the parent PostVisits still to be run.
  // This function is (hopefully) only called by
  // WalkExponential, but must be implemented by all clients,
  // just in case.
  virtual T ShortVisit(Regexp* re, T parent_arg) = 0;

  // Walks over a regular expression.
  // Top_arg is passed as parent_arg to PreVisit and PostVisit of re.
  // Returns the T returned by PostVisit on re.
  T Walk(Regexp* re, T top_arg);

  // Like Walk, but doesn't use Copy.  This can lead to
  // exponential runtimes on cross-linked Regexps like the
  // ones generated by Simplify.  To help limit this,
  // at most max_visits nodes will be visited and then
  // the walk will be cut off early.
  // If the walk *is* cut off early, ShortVisit(re)
  // will be called on regexps that cannot be fully
  // visited rather than calling PreVisit/PostVisit.
  T WalkExponential(Regexp* re, T top_arg, int max_visits);

  // Clears the stack.  Should never be necessary, since
  // Walk always enters and exits with an empty stack.
  // Logs DFATAL if stack is not already clear.
  void Reset();

  // Returns whether walk was cut off.
  bool stopped_early() { return stopped_early_; }

 private:
  // Walk state for the entire traversal.
  std::stack<WalkState<T> >* stack_;
  bool stopped_early_;
  int max_visits_;

  T WalkInternal(Regexp* re, T top_arg, bool use_copy);

  Walker(const Walker&) = delete;
  Walker& operator=(const Walker&) = delete;
};

template<typename T> T Regexp::Walker<T>::PreVisit(Regexp* re,
                                                   T parent_arg,
                                                   bool* stop) {
  return parent_arg;
}

template<typename T> T Regexp::Walker<T>::PostVisit(Regexp* re,
                                                    T parent_arg,
                                                    T pre_arg,
                                                    T* child_args,
                                                    int nchild_args) {
  return pre_arg;
}

template<typename T> T Regexp::Walker<T>::Copy(T arg) {
  return arg;
}

// State about a single level in the traversal.
template<typename T> struct WalkState {
  WalkState<T>(Regexp* re, T parent)
    : re(re),
      n(-1),
      parent_arg(parent),
      child_args(NULL) { }

  Regexp* re;  // The regexp
  int n;  // The index of the next child to process; -1 means need to PreVisit
  T parent_arg;  // Accumulated arguments.
  T pre_arg;
  T child_arg;  // One-element buffer for child_args.
  T* child_args;
};

template<typename T> Regexp::Walker<T>::Walker() {
  stack_ = new std::stack<WalkState<T> >;
  stopped_early_ = false;
}

// Clears the stack.  Should never be necessary, since
// Walk always enters and exits with an empty stack.
// Logs DFATAL if stack is not already clear.
template<typename T> void Regexp::Walker<T>::Reset() {
  if (stack_ && stack_->size() > 0) {
    LOG(DFATAL) << "Stack not empty.";
    while (stack_->size() > 0) {
      delete stack_->top().child_args;
      stack_->pop();
    }
  }
}

template<typename T> T Regexp::Walker<T>::WalkInternal(Regexp* re, T top_arg,
                                                       bool use_copy) {
  Reset();

  if (re == NULL) {
    LOG(DFATAL) << "Walk NULL";
    return top_arg;
  }

  stack_->push(WalkState<T>(re, top_arg));

  WalkState<T>* s;
  for (;;) {
    T t;
    s = &stack_->top();
    Regexp* re = s->re;
    switch (s->n) {
      case -1: {
        if (--max_visits_ < 0) {
          stopped_early_ = true;
          t = ShortVisit(re, s->parent_arg);
          break;
        }
        bool stop = false;
        s->pre_arg = PreVisit(re, s->parent_arg, &stop);
        if (stop) {
          t = s->pre_arg;
          break;
        }
        s->n = 0;
        s->child_args = NULL;
        if (re->nsub_ == 1)
          s->child_args = &s->child_arg;
        else if (re->nsub_ > 1)
          s->child_args = new T[re->nsub_];
        FALLTHROUGH_INTENDED;
      }
      default: {
        if (re->nsub_ > 0) {
          Regexp** sub = re->sub();
          if (s->n < re->nsub_) {
            if (use_copy && s->n > 0 && sub[s->n - 1] == sub[s->n]) {
              s->child_args[s->n] = Copy(s->child_args[s->n - 1]);
              s->n++;
            } else {
              stack_->push(WalkState<T>(sub[s->n], s->pre_arg));
            }
            continue;
          }
        }

        t = PostVisit(re, s->parent_arg, s->pre_arg, s->child_args, s->n);
        if (re->nsub_ > 1)
          delete[] s->child_args;
        break;
      }
    }

    // We've finished stack_->top().
    // Update next guy down.
    stack_->pop();
    if (stack_->size() == 0)
      return t;
    s = &stack_->top();
    if (s->child_args != NULL)
      s->child_args[s->n] = t;
    else
      s->child_arg = t;
    s->n++;
  }
}

template<typename T> T Regexp::Walker<T>::Walk(Regexp* re, T top_arg) {
  // Without the exponential walking behavior,
  // this budget should be more than enough for any
  // regexp, and yet not enough to get us in trouble
  // as far as CPU time.
  max_visits_ = 1000000;
  return WalkInternal(re, top_arg, true);
}

template<typename T> T Regexp::Walker<T>::WalkExponential(Regexp* re, T top_arg,
                                                          int max_visits) {
  max_visits_ = max_visits;
  return WalkInternal(re, top_arg, false);
}

}  // namespace duckdb_re2

#endif  // RE2_WALKER_INL_H_


// LICENSE_CHANGE_END


namespace duckdb_re2 {

// List of pointers to Inst* that need to be filled in (patched).
// Because the Inst* haven't been filled in yet,
// we can use the Inst* word to hold the list's "next" pointer.
// It's kind of sleazy, but it works well in practice.
// See http://swtch.com/~rsc/regexp/regexp1.html for inspiration.
//
// Because the out and out1 fields in Inst are no longer pointers,
// we can't use pointers directly here either.  Instead, p refers
// to inst_[p>>1].out (p&1 == 0) or inst_[p>>1].out1 (p&1 == 1).
// p == 0 represents the NULL list.  This is okay because instruction #0
// is always the fail instruction, which never appears on a list.

struct PatchList {
  uint32_t p;

  // Returns patch list containing just p.
  static PatchList Mk(uint32_t p);

  // Patches all the entries on l to have value v.
  // Caller must not ever use patch list again.
  static void Patch(Prog::Inst *inst0, PatchList l, uint32_t v);

  // Deref returns the next pointer pointed at by p.
  static PatchList Deref(Prog::Inst *inst0, PatchList l);

  // Appends two patch lists and returns result.
  static PatchList Append(Prog::Inst *inst0, PatchList l1, PatchList l2);
};

static PatchList nullPatchList = { 0 };

// Returns patch list containing just p.
PatchList PatchList::Mk(uint32_t p) {
  PatchList l;
  l.p = p;
  return l;
}

// Returns the next pointer pointed at by l.
PatchList PatchList::Deref(Prog::Inst* inst0, PatchList l) {
  Prog::Inst* ip = &inst0[l.p>>1];
  if (l.p&1)
    l.p = ip->out1();
  else
    l.p = ip->out();
  return l;
}

// Patches all the entries on l to have value v.
void PatchList::Patch(Prog::Inst *inst0, PatchList l, uint32_t val) {
  while (l.p != 0) {
    Prog::Inst* ip = &inst0[l.p>>1];
    if (l.p&1) {
      l.p = ip->out1();
      ip->out1_ = val;
    } else {
      l.p = ip->out();
      ip->set_out(val);
    }
  }
}

// Appends two patch lists and returns result.
PatchList PatchList::Append(Prog::Inst* inst0, PatchList l1, PatchList l2) {
  if (l1.p == 0)
    return l2;
  if (l2.p == 0)
    return l1;

  PatchList l = l1;
  for (;;) {
    PatchList next = PatchList::Deref(inst0, l);
    if (next.p == 0)
      break;
    l = next;
  }

  Prog::Inst* ip = &inst0[l.p>>1];
  if (l.p&1)
    ip->out1_ = l2.p;
  else
    ip->set_out(l2.p);

  return l1;
}

// Compiled program fragment.
struct Frag {
  uint32_t begin;
  PatchList end;

  Frag() : begin(0) { end.p = 0; }  // needed so Frag can go in vector
  Frag(uint32_t begin, PatchList end) : begin(begin), end(end) {}
};

// Input encodings.
enum Encoding {
  kEncodingUTF8 = 1,  // UTF-8 (0-10FFFF)
  kEncodingLatin1,    // Latin-1 (0-FF)
};

class Compiler : public Regexp::Walker<Frag> {
 public:
  explicit Compiler();
  ~Compiler();

  // Compiles Regexp to a new Prog.
  // Caller is responsible for deleting Prog when finished with it.
  // If reversed is true, compiles for walking over the input
  // string backward (reverses all concatenations).
  static Prog *Compile(Regexp* re, bool reversed, int64_t max_mem);

  // Compiles alternation of all the re to a new Prog.
  // Each re has a match with an id equal to its index in the vector.
  static Prog* CompileSet(Regexp* re, RE2::Anchor anchor, int64_t max_mem);

  // Interface for Regexp::Walker, which helps traverse the Regexp.
  // The walk is purely post-recursive: given the machines for the
  // children, PostVisit combines them to create the machine for
  // the current node.  The child_args are Frags.
  // The Compiler traverses the Regexp parse tree, visiting
  // each node in depth-first order.  It invokes PreVisit before
  // visiting the node's children and PostVisit after visiting
  // the children.
  Frag PreVisit(Regexp* re, Frag parent_arg, bool* stop);
  Frag PostVisit(Regexp* re, Frag parent_arg, Frag pre_arg, Frag* child_args,
                 int nchild_args);
  Frag ShortVisit(Regexp* re, Frag parent_arg);
  Frag Copy(Frag arg);

  // Given fragment a, returns a+ or a+?; a* or a*?; a? or a??
  Frag Plus(Frag a, bool nongreedy);
  Frag Star(Frag a, bool nongreedy);
  Frag Quest(Frag a, bool nongreedy);

  // Given fragment a, returns (a) capturing as \n.
  Frag Capture(Frag a, int n);

  // Given fragments a and b, returns ab; a|b
  Frag Cat(Frag a, Frag b);
  Frag Alt(Frag a, Frag b);

  // Returns a fragment that can't match anything.
  Frag NoMatch();

  // Returns a fragment that matches the empty string.
  Frag Match(int32_t id);

  // Returns a no-op fragment.
  Frag Nop();

  // Returns a fragment matching the byte range lo-hi.
  Frag ByteRange(int lo, int hi, bool foldcase);

  // Returns a fragment matching an empty-width special op.
  Frag EmptyWidth(EmptyOp op);

  // Adds n instructions to the program.
  // Returns the index of the first one.
  // Returns -1 if no more instructions are available.
  int AllocInst(int n);

  // Rune range compiler.

  // Begins a new alternation.
  void BeginRange();

  // Adds a fragment matching the rune range lo-hi.
  void AddRuneRange(Rune lo, Rune hi, bool foldcase);
  void AddRuneRangeLatin1(Rune lo, Rune hi, bool foldcase);
  void AddRuneRangeUTF8(Rune lo, Rune hi, bool foldcase);
  void Add_80_10ffff();

  // New suffix that matches the byte range lo-hi, then goes to next.
  int UncachedRuneByteSuffix(uint8_t lo, uint8_t hi, bool foldcase, int next);
  int CachedRuneByteSuffix(uint8_t lo, uint8_t hi, bool foldcase, int next);

  // Returns true iff the suffix is cached.
  bool IsCachedRuneByteSuffix(int id);

  // Adds a suffix to alternation.
  void AddSuffix(int id);

  // Adds a suffix to the trie starting from the given root node.
  // Returns zero iff allocating an instruction fails. Otherwise, returns
  // the current root node, which might be different from what was given.
  int AddSuffixRecursive(int root, int id);

  // Finds the trie node for the given suffix. Returns a Frag in order to
  // distinguish between pointing at the root node directly (end.p == 0)
  // and pointing at an Alt's out1 or out (end.p&1 == 1 or 0, respectively).
  Frag FindByteRange(int root, int id);

  // Compares two ByteRanges and returns true iff they are equal.
  bool ByteRangeEqual(int id1, int id2);

  // Returns the alternation of all the added suffixes.
  Frag EndRange();

  // Single rune.
  Frag Literal(Rune r, bool foldcase);

  void Setup(Regexp::ParseFlags, int64_t, RE2::Anchor);
  Prog* Finish();

  // Returns .* where dot = any byte
  Frag DotStar();

 private:
  Prog* prog_;         // Program being built.
  bool failed_;        // Did we give up compiling?
  Encoding encoding_;  // Input encoding
  bool reversed_;      // Should program run backward over text?

  PODArray<Prog::Inst> inst_;
  int ninst_;          // Number of instructions used.
  int max_ninst_;      // Maximum number of instructions.

  int64_t max_mem_;    // Total memory budget.

  std::unordered_map<uint64_t, int> rune_cache_;
  Frag rune_range_;

  RE2::Anchor anchor_;  // anchor mode for RE2::Set

  Compiler(const Compiler&) = delete;
  Compiler& operator=(const Compiler&) = delete;
};

Compiler::Compiler() {
  prog_ = new Prog();
  failed_ = false;
  encoding_ = kEncodingUTF8;
  reversed_ = false;
  ninst_ = 0;
  max_ninst_ = 1;  // make AllocInst for fail instruction okay
  max_mem_ = 0;
  int fail = AllocInst(1);
  inst_[fail].InitFail();
  max_ninst_ = 0;  // Caller must change
}

Compiler::~Compiler() {
  delete prog_;
}

int Compiler::AllocInst(int n) {
  if (failed_ || ninst_ + n > max_ninst_) {
    failed_ = true;
    return -1;
  }

  if (ninst_ + n > inst_.size()) {
    int cap = inst_.size();
    if (cap == 0)
      cap = 8;
    while (ninst_ + n > cap)
      cap *= 2;
    PODArray<Prog::Inst> inst(cap);
    if (inst_.data() != NULL)
      memmove(inst.data(), inst_.data(), ninst_*sizeof inst_[0]);
    memset(inst.data() + ninst_, 0, (cap - ninst_)*sizeof inst_[0]);
    inst_ = std::move(inst);
  }
  int id = ninst_;
  ninst_ += n;
  return id;
}

// These routines are somewhat hard to visualize in text --
// see http://swtch.com/~rsc/regexp/regexp1.html for
// pictures explaining what is going on here.

// Returns an unmatchable fragment.
Frag Compiler::NoMatch() {
  return Frag(0, nullPatchList);
}

// Is a an unmatchable fragment?
static bool IsNoMatch(Frag a) {
  return a.begin == 0;
}

// Given fragments a and b, returns fragment for ab.
Frag Compiler::Cat(Frag a, Frag b) {
  if (IsNoMatch(a) || IsNoMatch(b))
    return NoMatch();

  // Elide no-op.
  Prog::Inst* begin = &inst_[a.begin];
  if (begin->opcode() == kInstNop &&
      a.end.p == (a.begin << 1) &&
      begin->out() == 0) {
    // in case refs to a somewhere
    PatchList::Patch(inst_.data(), a.end, b.begin);
    return b;
  }

  // To run backward over string, reverse all concatenations.
  if (reversed_) {
    PatchList::Patch(inst_.data(), b.end, a.begin);
    return Frag(b.begin, a.end);
  }

  PatchList::Patch(inst_.data(), a.end, b.begin);
  return Frag(a.begin, b.end);
}

// Given fragments for a and b, returns fragment for a|b.
Frag Compiler::Alt(Frag a, Frag b) {
  // Special case for convenience in loops.
  if (IsNoMatch(a))
    return b;
  if (IsNoMatch(b))
    return a;

  int id = AllocInst(1);
  if (id < 0)
    return NoMatch();

  inst_[id].InitAlt(a.begin, b.begin);
  return Frag(id, PatchList::Append(inst_.data(), a.end, b.end));
}

// When capturing submatches in like-Perl mode, a kOpAlt Inst
// treats out_ as the first choice, out1_ as the second.
//
// For *, +, and ?, if out_ causes another repetition,
// then the operator is greedy.  If out1_ is the repetition
// (and out_ moves forward), then the operator is non-greedy.

// Given a fragment a, returns a fragment for a* or a*? (if nongreedy)
Frag Compiler::Star(Frag a, bool nongreedy) {
  int id = AllocInst(1);
  if (id < 0)
    return NoMatch();
  inst_[id].InitAlt(0, 0);
  PatchList::Patch(inst_.data(), a.end, id);
  if (nongreedy) {
    inst_[id].out1_ = a.begin;
    return Frag(id, PatchList::Mk(id << 1));
  } else {
    inst_[id].set_out(a.begin);
    return Frag(id, PatchList::Mk((id << 1) | 1));
  }
}

// Given a fragment for a, returns a fragment for a+ or a+? (if nongreedy)
Frag Compiler::Plus(Frag a, bool nongreedy) {
  // a+ is just a* with a different entry point.
  Frag f = Star(a, nongreedy);
  return Frag(a.begin, f.end);
}

// Given a fragment for a, returns a fragment for a? or a?? (if nongreedy)
Frag Compiler::Quest(Frag a, bool nongreedy) {
  if (IsNoMatch(a))
    return Nop();
  int id = AllocInst(1);
  if (id < 0)
    return NoMatch();
  PatchList pl;
  if (nongreedy) {
    inst_[id].InitAlt(0, a.begin);
    pl = PatchList::Mk(id << 1);
  } else {
    inst_[id].InitAlt(a.begin, 0);
    pl = PatchList::Mk((id << 1) | 1);
  }
  return Frag(id, PatchList::Append(inst_.data(), pl, a.end));
}

// Returns a fragment for the byte range lo-hi.
Frag Compiler::ByteRange(int lo, int hi, bool foldcase) {
  int id = AllocInst(1);
  if (id < 0)
    return NoMatch();
  inst_[id].InitByteRange(lo, hi, foldcase, 0);
  return Frag(id, PatchList::Mk(id << 1));
}

// Returns a no-op fragment.  Sometimes unavoidable.
Frag Compiler::Nop() {
  int id = AllocInst(1);
  if (id < 0)
    return NoMatch();
  inst_[id].InitNop(0);
  return Frag(id, PatchList::Mk(id << 1));
}

// Returns a fragment that signals a match.
Frag Compiler::Match(int32_t match_id) {
  int id = AllocInst(1);
  if (id < 0)
    return NoMatch();
  inst_[id].InitMatch(match_id);
  return Frag(id, nullPatchList);
}

// Returns a fragment matching a particular empty-width op (like ^ or $)
Frag Compiler::EmptyWidth(EmptyOp empty) {
  int id = AllocInst(1);
  if (id < 0)
    return NoMatch();
  inst_[id].InitEmptyWidth(empty, 0);
  return Frag(id, PatchList::Mk(id << 1));
}

// Given a fragment a, returns a fragment with capturing parens around a.
Frag Compiler::Capture(Frag a, int n) {
  if (IsNoMatch(a))
    return NoMatch();
  int id = AllocInst(2);
  if (id < 0)
    return NoMatch();
  inst_[id].InitCapture(2*n, a.begin);
  inst_[id+1].InitCapture(2*n+1, 0);
  PatchList::Patch(inst_.data(), a.end, id+1);

  return Frag(id, PatchList::Mk((id+1) << 1));
}

// A Rune is a name for a Unicode code point.
// Returns maximum rune encoded by UTF-8 sequence of length len.
static int MaxRune(int len) {
  int b;  // number of Rune bits in len-byte UTF-8 sequence (len < UTFmax)
  if (len == 1)
    b = 7;
  else
    b = 8-(len+1) + 6*(len-1);
  return (1<<b) - 1;   // maximum Rune for b bits.
}

// The rune range compiler caches common suffix fragments,
// which are very common in UTF-8 (e.g., [80-bf]).
// The fragment suffixes are identified by their start
// instructions.  NULL denotes the eventual end match.
// The Frag accumulates in rune_range_.  Caching common
// suffixes reduces the UTF-8 "." from 32 to 24 instructions,
// and it reduces the corresponding one-pass NFA from 16 nodes to 8.

void Compiler::BeginRange() {
  rune_cache_.clear();
  rune_range_.begin = 0;
  rune_range_.end = nullPatchList;
}

int Compiler::UncachedRuneByteSuffix(uint8_t lo, uint8_t hi, bool foldcase,
                                     int next) {
  Frag f = ByteRange(lo, hi, foldcase);
  if (next != 0) {
    PatchList::Patch(inst_.data(), f.end, next);
  } else {
    rune_range_.end = PatchList::Append(inst_.data(), rune_range_.end, f.end);
  }
  return f.begin;
}

static uint64_t MakeRuneCacheKey(uint8_t lo, uint8_t hi, bool foldcase,
                                 int next) {
  return (uint64_t)next << 17 |
         (uint64_t)lo   <<  9 |
         (uint64_t)hi   <<  1 |
         (uint64_t)foldcase;
}

int Compiler::CachedRuneByteSuffix(uint8_t lo, uint8_t hi, bool foldcase,
                                   int next) {
  uint64_t key = MakeRuneCacheKey(lo, hi, foldcase, next);
  std::unordered_map<uint64_t, int>::const_iterator it = rune_cache_.find(key);
  if (it != rune_cache_.end())
    return it->second;
  int id = UncachedRuneByteSuffix(lo, hi, foldcase, next);
  rune_cache_[key] = id;
  return id;
}

bool Compiler::IsCachedRuneByteSuffix(int id) {
  uint8_t lo = inst_[id].inst_byte_range_data_.lo_;
  uint8_t hi = inst_[id].inst_byte_range_data_.hi_;
  bool foldcase = inst_[id].foldcase() != 0;
  int next = inst_[id].out();

  uint64_t key = MakeRuneCacheKey(lo, hi, foldcase, next);
  return rune_cache_.find(key) != rune_cache_.end();
}

void Compiler::AddSuffix(int id) {
  if (failed_)
    return;

  if (rune_range_.begin == 0) {
    rune_range_.begin = id;
    return;
  }

  if (encoding_ == kEncodingUTF8) {
    // Build a trie in order to reduce fanout.
    rune_range_.begin = AddSuffixRecursive(rune_range_.begin, id);
    return;
  }

  int alt = AllocInst(1);
  if (alt < 0) {
    rune_range_.begin = 0;
    return;
  }
  inst_[alt].InitAlt(rune_range_.begin, id);
  rune_range_.begin = alt;
}

int Compiler::AddSuffixRecursive(int root, int id) {
  DCHECK(inst_[root].opcode() == kInstAlt ||
         inst_[root].opcode() == kInstByteRange);

  Frag f = FindByteRange(root, id);
  if (IsNoMatch(f)) {
    int alt = AllocInst(1);
    if (alt < 0)
      return 0;
    inst_[alt].InitAlt(root, id);
    return alt;
  }

  int br;
  if (f.end.p == 0)
    br = root;
  else if (f.end.p&1)
    br = inst_[f.begin].out1();
  else
    br = inst_[f.begin].out();

  if (IsCachedRuneByteSuffix(br)) {
    // We can't fiddle with cached suffixes, so make a clone of the head.
    int byterange = AllocInst(1);
    if (byterange < 0)
      return 0;
    inst_[byterange].InitByteRange(inst_[br].lo(), inst_[br].hi(),
                                   inst_[br].foldcase(), inst_[br].out());

    // Ensure that the parent points to the clone, not to the original.
    // Note that this could leave the head unreachable except via the cache.
    br = byterange;
    if (f.end.p == 0)
      root = br;
    else if (f.end.p&1)
      inst_[f.begin].out1_ = br;
    else
      inst_[f.begin].set_out(br);
  }

  int out = inst_[id].out();
  if (!IsCachedRuneByteSuffix(id)) {
    // The head should be the instruction most recently allocated, so free it
    // instead of leaving it unreachable.
    DCHECK_EQ(id, ninst_-1);
    inst_[id].out_opcode_ = 0;
    inst_[id].out1_ = 0;
    ninst_--;
  }

  out = AddSuffixRecursive(inst_[br].out(), out);
  if (out == 0)
    return 0;

  inst_[br].set_out(out);
  return root;
}

bool Compiler::ByteRangeEqual(int id1, int id2) {
  return inst_[id1].lo() == inst_[id2].lo() &&
         inst_[id1].hi() == inst_[id2].hi() &&
         inst_[id1].foldcase() == inst_[id2].foldcase();
}

Frag Compiler::FindByteRange(int root, int id) {
  if (inst_[root].opcode() == kInstByteRange) {
    if (ByteRangeEqual(root, id))
      return Frag(root, nullPatchList);
    else
      return NoMatch();
  }

  while (inst_[root].opcode() == kInstAlt) {
    int out1 = inst_[root].out1();
    if (ByteRangeEqual(out1, id))
      return Frag(root, PatchList::Mk((root << 1) | 1));

    // CharClass is a sorted list of ranges, so if out1 of the root Alt wasn't
    // what we're looking for, then we can stop immediately. Unfortunately, we
    // can't short-circuit the search in reverse mode.
    if (!reversed_)
      return NoMatch();

    int out = inst_[root].out();
    if (inst_[out].opcode() == kInstAlt)
      root = out;
    else if (ByteRangeEqual(out, id))
      return Frag(root, PatchList::Mk(root << 1));
    else
      return NoMatch();
  }

  LOG(DFATAL) << "should never happen";
  return NoMatch();
}

Frag Compiler::EndRange() {
  return rune_range_;
}

// Converts rune range lo-hi into a fragment that recognizes
// the bytes that would make up those runes in the current
// encoding (Latin 1 or UTF-8).
// This lets the machine work byte-by-byte even when
// using multibyte encodings.

void Compiler::AddRuneRange(Rune lo, Rune hi, bool foldcase) {
  switch (encoding_) {
    default:
    case kEncodingUTF8:
      AddRuneRangeUTF8(lo, hi, foldcase);
      break;
    case kEncodingLatin1:
      AddRuneRangeLatin1(lo, hi, foldcase);
      break;
  }
}

void Compiler::AddRuneRangeLatin1(Rune lo, Rune hi, bool foldcase) {
  // Latin-1 is easy: runes *are* bytes.
  if (lo > hi || lo > 0xFF)
    return;
  if (hi > 0xFF)
    hi = 0xFF;
  AddSuffix(UncachedRuneByteSuffix(static_cast<uint8_t>(lo),
                                   static_cast<uint8_t>(hi), foldcase, 0));
}

// Table describing how to make a UTF-8 matching machine
// for the rune range 80-10FFFF (Runeself-Runemax).
// This range happens frequently enough (for example /./ and /[^a-z]/)
// and the rune_cache_ map is slow enough that this is worth
// special handling.  Makes compilation of a small expression
// with a dot in it about 10% faster.
// The * in the comments below mark whole sequences.
static struct ByteRangeProg {
  int next;
  int lo;
  int hi;
} prog_80_10ffff[] = {
  // Two-byte
  { -1, 0x80, 0xBF, },  // 0:  80-BF
  {  0, 0xC2, 0xDF, },  // 1:  C2-DF 80-BF*

  // Three-byte
  {  0, 0xA0, 0xBF, },  // 2:  A0-BF 80-BF
  {  2, 0xE0, 0xE0, },  // 3:  E0 A0-BF 80-BF*
  {  0, 0x80, 0xBF, },  // 4:  80-BF 80-BF
  {  4, 0xE1, 0xEF, },  // 5:  E1-EF 80-BF 80-BF*

  // Four-byte
  {  4, 0x90, 0xBF, },  // 6:  90-BF 80-BF 80-BF
  {  6, 0xF0, 0xF0, },  // 7:  F0 90-BF 80-BF 80-BF*
  {  4, 0x80, 0xBF, },  // 8:  80-BF 80-BF 80-BF
  {  8, 0xF1, 0xF3, },  // 9: F1-F3 80-BF 80-BF 80-BF*
  {  4, 0x80, 0x8F, },  // 10: 80-8F 80-BF 80-BF
  { 10, 0xF4, 0xF4, },  // 11: F4 80-8F 80-BF 80-BF*
};

void Compiler::Add_80_10ffff() {
  int inst[arraysize(prog_80_10ffff)] = { 0 }; // does not need to be initialized; silences gcc warning
  for (int i = 0; i < arraysize(prog_80_10ffff); i++) {
    const ByteRangeProg& p = prog_80_10ffff[i];
    int next = 0;
    if (p.next >= 0)
      next = inst[p.next];
    inst[i] = UncachedRuneByteSuffix(static_cast<uint8_t>(p.lo),
                                     static_cast<uint8_t>(p.hi), false, next);
    if ((p.lo & 0xC0) != 0x80)
      AddSuffix(inst[i]);
  }
}

void Compiler::AddRuneRangeUTF8(Rune lo, Rune hi, bool foldcase) {
  if (lo > hi)
    return;

  // Pick off 80-10FFFF as a common special case
  // that can bypass the slow rune_cache_.
  if (lo == 0x80 && hi == 0x10ffff && !reversed_) {
    Add_80_10ffff();
    return;
  }

  // Split range into same-length sized ranges.
  for (int i = 1; i < UTFmax; i++) {
    Rune max = MaxRune(i);
    if (lo <= max && max < hi) {
      AddRuneRangeUTF8(lo, max, foldcase);
      AddRuneRangeUTF8(max+1, hi, foldcase);
      return;
    }
  }

  // ASCII range is always a special case.
  if (hi < Runeself) {
    AddSuffix(UncachedRuneByteSuffix(static_cast<uint8_t>(lo),
                                     static_cast<uint8_t>(hi), foldcase, 0));
    return;
  }

  // Split range into sections that agree on leading bytes.
  for (int i = 1; i < UTFmax; i++) {
    uint32_t m = (1<<(6*i)) - 1;  // last i bytes of a UTF-8 sequence
    if ((lo & ~m) != (hi & ~m)) {
      if ((lo & m) != 0) {
        AddRuneRangeUTF8(lo, lo|m, foldcase);
        AddRuneRangeUTF8((lo|m)+1, hi, foldcase);
        return;
      }
      if ((hi & m) != m) {
        AddRuneRangeUTF8(lo, (hi&~m)-1, foldcase);
        AddRuneRangeUTF8(hi&~m, hi, foldcase);
        return;
      }
    }
  }

  // Finally.  Generate byte matching equivalent for lo-hi.
  uint8_t ulo[UTFmax], uhi[UTFmax];
  int n = runetochar(reinterpret_cast<char*>(ulo), &lo);
  int m = runetochar(reinterpret_cast<char*>(uhi), &hi);
  (void)m;  // USED(m)
  DCHECK_EQ(n, m);

  // The logic below encodes this thinking:
  //
  // 1. When we have built the whole suffix, we know that it cannot
  // possibly be a suffix of anything longer: in forward mode, nothing
  // else can occur before the leading byte; in reverse mode, nothing
  // else can occur after the last continuation byte or else the leading
  // byte would have to change. Thus, there is no benefit to caching
  // the first byte of the suffix whereas there is a cost involved in
  // cloning it if it begins a common prefix, which is fairly likely.
  //
  // 2. Conversely, the last byte of the suffix cannot possibly be a
  // prefix of anything because next == 0, so we will never want to
  // clone it, but it is fairly likely to be a common suffix. Perhaps
  // more so in reverse mode than in forward mode because the former is
  // "converging" towards lower entropy, but caching is still worthwhile
  // for the latter in cases such as 80-BF.
  //
  // 3. Handling the bytes between the first and the last is less
  // straightforward and, again, the approach depends on whether we are
  // "converging" towards lower entropy: in forward mode, a single byte
  // is unlikely to be part of a common suffix whereas a byte range
  // is more likely so; in reverse mode, a byte range is unlikely to
  // be part of a common suffix whereas a single byte is more likely
  // so. The same benefit versus cost argument applies here.
  int id = 0;
  if (reversed_) {
    for (int i = 0; i < n; i++) {
      // In reverse UTF-8 mode: cache the leading byte; don't cache the last
      // continuation byte; cache anything else iff it's a single byte (XX-XX).
      if (i == 0 || (ulo[i] == uhi[i] && i != n-1))
        id = CachedRuneByteSuffix(ulo[i], uhi[i], false, id);
      else
        id = UncachedRuneByteSuffix(ulo[i], uhi[i], false, id);
    }
  } else {
    for (int i = n-1; i >= 0; i--) {
      // In forward UTF-8 mode: don't cache the leading byte; cache the last
      // continuation byte; cache anything else iff it's a byte range (XX-YY).
      if (i == n-1 || (ulo[i] < uhi[i] && i != 0))
        id = CachedRuneByteSuffix(ulo[i], uhi[i], false, id);
      else
        id = UncachedRuneByteSuffix(ulo[i], uhi[i], false, id);
    }
  }
  AddSuffix(id);
}

// Should not be called.
Frag Compiler::Copy(Frag arg) {
  // We're using WalkExponential; there should be no copying.
  LOG(DFATAL) << "Compiler::Copy called!";
  failed_ = true;
  return NoMatch();
}

// Visits a node quickly; called once WalkExponential has
// decided to cut this walk short.
Frag Compiler::ShortVisit(Regexp* re, Frag) {
  failed_ = true;
  return NoMatch();
}

// Called before traversing a node's children during the walk.
Frag Compiler::PreVisit(Regexp* re, Frag, bool* stop) {
  // Cut off walk if we've already failed.
  if (failed_)
    *stop = true;

  return Frag();  // not used by caller
}

Frag Compiler::Literal(Rune r, bool foldcase) {
  switch (encoding_) {
    default:
      return Frag();

    case kEncodingLatin1:
      return ByteRange(r, r, foldcase);

    case kEncodingUTF8: {
      if (r < Runeself)  // Make common case fast.
        return ByteRange(r, r, foldcase);
      uint8_t buf[UTFmax];
      int n = runetochar(reinterpret_cast<char*>(buf), &r);
      Frag f = ByteRange((uint8_t)buf[0], buf[0], false);
      for (int i = 1; i < n; i++)
        f = Cat(f, ByteRange((uint8_t)buf[i], buf[i], false));
      return f;
    }
  }
}

// Called after traversing the node's children during the walk.
// Given their frags, build and return the frag for this re.
Frag Compiler::PostVisit(Regexp* re, Frag, Frag, Frag* child_frags,
                         int nchild_frags) {
  // If a child failed, don't bother going forward, especially
  // since the child_frags might contain Frags with NULLs in them.
  if (failed_)
    return NoMatch();

  // Given the child fragments, return the fragment for this node.
  switch (re->op()) {
    case kRegexpRepeat:
      // Should not see; code at bottom of function will print error
      break;

    case kRegexpNoMatch:
      return NoMatch();

    case kRegexpEmptyMatch:
      return Nop();

    case kRegexpHaveMatch: {
      Frag f = Match(re->match_id());
      if (anchor_ == RE2::ANCHOR_BOTH) {
        // Append \z or else the subexpression will effectively be unanchored.
        // Complemented by the UNANCHORED case in CompileSet().
        f = Cat(EmptyWidth(kEmptyEndText), f);
      }
      return f;
    }

    case kRegexpConcat: {
      Frag f = child_frags[0];
      for (int i = 1; i < nchild_frags; i++)
        f = Cat(f, child_frags[i]);
      return f;
    }

    case kRegexpAlternate: {
      Frag f = child_frags[0];
      for (int i = 1; i < nchild_frags; i++)
        f = Alt(f, child_frags[i]);
      return f;
    }

    case kRegexpStar:
      return Star(child_frags[0], (re->parse_flags()&Regexp::NonGreedy) != 0);

    case kRegexpPlus:
      return Plus(child_frags[0], (re->parse_flags()&Regexp::NonGreedy) != 0);

    case kRegexpQuest:
      return Quest(child_frags[0], (re->parse_flags()&Regexp::NonGreedy) != 0);

    case kRegexpLiteral:
      return Literal(re->rune(), (re->parse_flags()&Regexp::FoldCase) != 0);

    case kRegexpLiteralString: {
      // Concatenation of literals.
      if (re->nrunes() == 0)
        return Nop();
      Frag f;
      for (int i = 0; i < re->nrunes(); i++) {
        Frag f1 = Literal(re->runes()[i],
                          (re->parse_flags()&Regexp::FoldCase) != 0);
        if (i == 0)
          f = f1;
        else
          f = Cat(f, f1);
      }
      return f;
    }

    case kRegexpAnyChar:
      BeginRange();
      AddRuneRange(0, Runemax, false);
      return EndRange();

    case kRegexpAnyByte:
      return ByteRange(0x00, 0xFF, false);

    case kRegexpCharClass: {
      CharClass* cc = re->cc();
      if (cc->empty()) {
        // This can't happen.
        LOG(DFATAL) << "No ranges in char class";
        failed_ = true;
        return NoMatch();
      }

      // ASCII case-folding optimization: if the char class
      // behaves the same on A-Z as it does on a-z,
      // discard any ranges wholly contained in A-Z
      // and mark the other ranges as foldascii.
      // This reduces the size of a program for
      // (?i)abc from 3 insts per letter to 1 per letter.
      bool foldascii = cc->FoldsASCII();

      // Character class is just a big OR of the different
      // character ranges in the class.
      BeginRange();
      for (CharClass::iterator i = cc->begin(); i != cc->end(); ++i) {
        // ASCII case-folding optimization (see above).
        if (foldascii && 'A' <= i->lo && i->hi <= 'Z')
          continue;

        // If this range contains all of A-Za-z or none of it,
        // the fold flag is unnecessary; don't bother.
        bool fold = foldascii;
        if ((i->lo <= 'A' && 'z' <= i->hi) || i->hi < 'A' || 'z' < i->lo ||
            ('Z' < i->lo && i->hi < 'a'))
          fold = false;

        AddRuneRange(i->lo, i->hi, fold);
      }
      return EndRange();
    }

    case kRegexpCapture:
      // If this is a non-capturing parenthesis -- (?:foo) --
      // just use the inner expression.
      if (re->cap() < 0)
        return child_frags[0];
      return Capture(child_frags[0], re->cap());

    case kRegexpBeginLine:
      return EmptyWidth(reversed_ ? kEmptyEndLine : kEmptyBeginLine);

    case kRegexpEndLine:
      return EmptyWidth(reversed_ ? kEmptyBeginLine : kEmptyEndLine);

    case kRegexpBeginText:
      return EmptyWidth(reversed_ ? kEmptyEndText : kEmptyBeginText);

    case kRegexpEndText:
      return EmptyWidth(reversed_ ? kEmptyBeginText : kEmptyEndText);

    case kRegexpWordBoundary:
      return EmptyWidth(kEmptyWordBoundary);

    case kRegexpNoWordBoundary:
      return EmptyWidth(kEmptyNonWordBoundary);
  }
  LOG(DFATAL) << "Missing case in Compiler: " << re->op();
  failed_ = true;
  return NoMatch();
}

// Is this regexp required to start at the beginning of the text?
// Only approximate; can return false for complicated regexps like (\Aa|\Ab),
// but handles (\A(a|b)).  Could use the Walker to write a more exact one.
static bool IsAnchorStart(Regexp** pre, int depth) {
  Regexp* re = *pre;
  Regexp* sub;
  // The depth limit makes sure that we don't overflow
  // the stack on a deeply nested regexp.  As the comment
  // above says, IsAnchorStart is conservative, so returning
  // a false negative is okay.  The exact limit is somewhat arbitrary.
  if (re == NULL || depth >= 4)
    return false;
  switch (re->op()) {
    default:
      break;
    case kRegexpConcat:
      if (re->nsub() > 0) {
        sub = re->sub()[0]->Incref();
        if (IsAnchorStart(&sub, depth+1)) {
          PODArray<Regexp*> subcopy(re->nsub());
          subcopy[0] = sub;  // already have reference
          for (int i = 1; i < re->nsub(); i++)
            subcopy[i] = re->sub()[i]->Incref();
          *pre = Regexp::Concat(subcopy.data(), re->nsub(), re->parse_flags());
          re->Decref();
          return true;
        }
        sub->Decref();
      }
      break;
    case kRegexpCapture:
      sub = re->sub()[0]->Incref();
      if (IsAnchorStart(&sub, depth+1)) {
        *pre = Regexp::Capture(sub, re->parse_flags(), re->cap());
        re->Decref();
        return true;
      }
      sub->Decref();
      break;
    case kRegexpBeginText:
      *pre = Regexp::LiteralString(NULL, 0, re->parse_flags());
      re->Decref();
      return true;
  }
  return false;
}

// Is this regexp required to start at the end of the text?
// Only approximate; can return false for complicated regexps like (a\z|b\z),
// but handles ((a|b)\z).  Could use the Walker to write a more exact one.
static bool IsAnchorEnd(Regexp** pre, int depth) {
  Regexp* re = *pre;
  Regexp* sub;
  // The depth limit makes sure that we don't overflow
  // the stack on a deeply nested regexp.  As the comment
  // above says, IsAnchorEnd is conservative, so returning
  // a false negative is okay.  The exact limit is somewhat arbitrary.
  if (re == NULL || depth >= 4)
    return false;
  switch (re->op()) {
    default:
      break;
    case kRegexpConcat:
      if (re->nsub() > 0) {
        sub = re->sub()[re->nsub() - 1]->Incref();
        if (IsAnchorEnd(&sub, depth+1)) {
          PODArray<Regexp*> subcopy(re->nsub());
          subcopy[re->nsub() - 1] = sub;  // already have reference
          for (int i = 0; i < re->nsub() - 1; i++)
            subcopy[i] = re->sub()[i]->Incref();
          *pre = Regexp::Concat(subcopy.data(), re->nsub(), re->parse_flags());
          re->Decref();
          return true;
        }
        sub->Decref();
      }
      break;
    case kRegexpCapture:
      sub = re->sub()[0]->Incref();
      if (IsAnchorEnd(&sub, depth+1)) {
        *pre = Regexp::Capture(sub, re->parse_flags(), re->cap());
        re->Decref();
        return true;
      }
      sub->Decref();
      break;
    case kRegexpEndText:
      *pre = Regexp::LiteralString(NULL, 0, re->parse_flags());
      re->Decref();
      return true;
  }
  return false;
}

void Compiler::Setup(Regexp::ParseFlags flags, int64_t max_mem,
                     RE2::Anchor anchor) {
  prog_->set_flags(flags);

  if (flags & Regexp::Latin1)
    encoding_ = kEncodingLatin1;
  max_mem_ = max_mem;
  if (max_mem <= 0) {
    max_ninst_ = 100000;  // more than enough
  } else if (static_cast<size_t>(max_mem) <= sizeof(Prog)) {
    // No room for anything.
    max_ninst_ = 0;
  } else {
    int64_t m = (max_mem - sizeof(Prog)) / sizeof(Prog::Inst);
    // Limit instruction count so that inst->id() fits nicely in an int.
    // SparseArray also assumes that the indices (inst->id()) are ints.
    // The call to WalkExponential uses 2*max_ninst_ below,
    // and other places in the code use 2 or 3 * prog->size().
    // Limiting to 2^24 should avoid overflow in those places.
    // (The point of allowing more than 32 bits of memory is to
    // have plenty of room for the DFA states, not to use it up
    // on the program.)
    if (m >= 1<<24)
      m = 1<<24;

    // Inst imposes its own limit (currently bigger than 2^24 but be safe).
    if (m > Prog::Inst::kMaxInst)
      m = Prog::Inst::kMaxInst;

    max_ninst_ = static_cast<int>(m);
  }

  anchor_ = anchor;
}

// Compiles re, returning program.
// Caller is responsible for deleting prog_.
// If reversed is true, compiles a program that expects
// to run over the input string backward (reverses all concatenations).
// The reversed flag is also recorded in the returned program.
Prog* Compiler::Compile(Regexp* re, bool reversed, int64_t max_mem) {
  Compiler c;
  c.Setup(re->parse_flags(), max_mem, RE2::UNANCHORED /* unused */);
  c.reversed_ = reversed;

  // Simplify to remove things like counted repetitions
  // and character classes like \d.
  Regexp* sre = re->Simplify();
  if (sre == NULL)
    return NULL;

  // Record whether prog is anchored, removing the anchors.
  // (They get in the way of other optimizations.)
  bool is_anchor_start = IsAnchorStart(&sre, 0);
  bool is_anchor_end = IsAnchorEnd(&sre, 0);

  // Generate fragment for entire regexp.
  Frag all = c.WalkExponential(sre, Frag(), 2*c.max_ninst_);
  sre->Decref();
  if (c.failed_)
    return NULL;

  // Success!  Finish by putting Match node at end, and record start.
  // Turn off c.reversed_ (if it is set) to force the remaining concatenations
  // to behave normally.
  c.reversed_ = false;
  all = c.Cat(all, c.Match(0));

  c.prog_->set_reversed(reversed);
  if (c.prog_->reversed()) {
    c.prog_->set_anchor_start(is_anchor_end);
    c.prog_->set_anchor_end(is_anchor_start);
  } else {
    c.prog_->set_anchor_start(is_anchor_start);
    c.prog_->set_anchor_end(is_anchor_end);
  }

  c.prog_->set_start(all.begin);
  if (!c.prog_->anchor_start()) {
    // Also create unanchored version, which starts with a .*? loop.
    all = c.Cat(c.DotStar(), all);
  }
  c.prog_->set_start_unanchored(all.begin);

  // Hand ownership of prog_ to caller.
  return c.Finish();
}

Prog* Compiler::Finish() {
  if (failed_)
    return NULL;

  if (prog_->start() == 0 && prog_->start_unanchored() == 0) {
    // No possible matches; keep Fail instruction only.
    ninst_ = 1;
  }

  // Hand off the array to Prog.
  prog_->inst_ = std::move(inst_);
  prog_->size_ = ninst_;

  prog_->Optimize();
  prog_->Flatten();
  prog_->ComputeByteMap();

  // Record remaining memory for DFA.
  if (max_mem_ <= 0) {
    prog_->set_dfa_mem(1<<20);
  } else {
    int64_t m = max_mem_ - sizeof(Prog);
    m -= prog_->size_*sizeof(Prog::Inst);  // account for inst_
    if (prog_->CanBitState())
      m -= prog_->size_*sizeof(uint16_t);  // account for list_heads_
    if (m < 0)
      m = 0;
    prog_->set_dfa_mem(m);
  }

  Prog* p = prog_;
  prog_ = NULL;
  return p;
}

// Converts Regexp to Prog.
Prog* Regexp::CompileToProg(int64_t max_mem) {
  return Compiler::Compile(this, false, max_mem);
}

Prog* Regexp::CompileToReverseProg(int64_t max_mem) {
  return Compiler::Compile(this, true, max_mem);
}

Frag Compiler::DotStar() {
  return Star(ByteRange(0x00, 0xff, false), true);
}

// Compiles RE set to Prog.
Prog* Compiler::CompileSet(Regexp* re, RE2::Anchor anchor, int64_t max_mem) {
  Compiler c;
  c.Setup(re->parse_flags(), max_mem, anchor);

  Regexp* sre = re->Simplify();
  if (sre == NULL)
    return NULL;

  Frag all = c.WalkExponential(sre, Frag(), 2*c.max_ninst_);
  sre->Decref();
  if (c.failed_)
    return NULL;

  c.prog_->set_anchor_start(true);
  c.prog_->set_anchor_end(true);

  if (anchor == RE2::UNANCHORED) {
    // Prepend .* or else the expression will effectively be anchored.
    // Complemented by the ANCHOR_BOTH case in PostVisit().
    all = c.Cat(c.DotStar(), all);
  }
  c.prog_->set_start(all.begin);
  c.prog_->set_start_unanchored(all.begin);

  Prog* prog = c.Finish();
  if (prog == NULL)
    return NULL;

  // Make sure DFA has enough memory to operate,
  // since we're not going to fall back to the NFA.
  bool dfa_failed = false;
  StringPiece sp = "hello, world";
  prog->SearchDFA(sp, sp, Prog::kAnchored, Prog::kManyMatch,
                  NULL, &dfa_failed, NULL);
  if (dfa_failed) {
    delete prog;
    return NULL;
  }

  return prog;
}

Prog* Prog::CompileSet(Regexp* re, RE2::Anchor anchor, int64_t max_mem) {
  return Compiler::CompileSet(re, anchor, max_mem);
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2008 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// A DFA (deterministic finite automaton)-based regular expression search.
//
// The DFA search has two main parts: the construction of the automaton,
// which is represented by a graph of State structures, and the execution
// of the automaton over a given input string.
//
// The basic idea is that the State graph is constructed so that the
// execution can simply start with a state s, and then for each byte c in
// the input string, execute "s = s->next[c]", checking at each point whether
// the current s represents a matching state.
//
// The simple explanation just given does convey the essence of this code,
// but it omits the details of how the State graph gets constructed as well
// as some performance-driven optimizations to the execution of the automaton.
// All these details are explained in the comments for the code following
// the definition of class DFA.
//
// See http://swtch.com/~rsc/regexp/ for a very bare-bones equivalent.

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <atomic>
#include <deque>
#include <mutex>
#include <new>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>




// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2016 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef UTIL_MIX_H_
#define UTIL_MIX_H_

#include <stddef.h>
#include <limits>

namespace duckdb_re2 {

// Silence "truncation of constant value" warning for kMul in 32-bit mode.
// Since this is a header file, push and then pop to limit the scope.
#ifdef _MSC_VER
//#pragma warning(push)
//#pragma warning(disable: 4309)
#endif

class HashMix {
 public:
  HashMix() : hash_(1) {}
  explicit HashMix(size_t val) : hash_(val + 83) {}
  void Mix(size_t val) {
    static const size_t kMul = static_cast<size_t>(0xdc3eb94af8ab4c93ULL);
    hash_ *= kMul;
    hash_ = ((hash_ << 19) |
             (hash_ >> (std::numeric_limits<size_t>::digits - 19))) + val;
  }
  size_t get() const { return hash_; }
 private:
  size_t hash_;
};

#ifdef _MSC_VER
//#pragma warning(pop)
#endif

}  // namespace duckdb_re2

#endif  // UTIL_MIX_H_


// LICENSE_CHANGE_END



// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2007 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef UTIL_MUTEX_H_
#define UTIL_MUTEX_H_

/*
 * A simple mutex wrapper, supporting locks and read-write locks.
 * You should assume the locks are *not* re-entrant.
 */

#if !defined(_WIN32)
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#include <unistd.h>
//#if defined(_POSIX_READER_WRITER_LOCKS) && _POSIX_READER_WRITER_LOCKS > 0
//#define MUTEX_IS_PTHREAD_RWLOCK
//#endif
#endif

#if defined(MUTEX_IS_PTHREAD_RWLOCK)
#error We really dont want to include pthread
#include <pthread.h>
#include <stdlib.h>
typedef pthread_rwlock_t MutexType;
#else
#include <mutex>
typedef std::mutex MutexType;
#endif

namespace duckdb_re2 {

class Mutex {
 public:
  inline Mutex();
  inline ~Mutex();
  inline void Lock();    // Block if needed until free then acquire exclusively
  inline void Unlock();  // Release a lock acquired via Lock()
  // Note that on systems that don't support read-write locks, these may
  // be implemented as synonyms to Lock() and Unlock().  So you can use
  // these for efficiency, but don't use them anyplace where being able
  // to do shared reads is necessary to avoid deadlock.
  inline void ReaderLock();   // Block until free or shared then acquire a share
  inline void ReaderUnlock(); // Release a read share of this Mutex
  inline void WriterLock() { Lock(); }     // Acquire an exclusive lock
  inline void WriterUnlock() { Unlock(); } // Release a lock from WriterLock()

 private:
  MutexType mutex_;

  // Catch the error of writing Mutex when intending MutexLock.
  Mutex(Mutex *ignored);

  Mutex(const Mutex&) = delete;
  Mutex& operator=(const Mutex&) = delete;
};

#if defined(MUTEX_IS_PTHREAD_RWLOCK)

#define SAFE_PTHREAD(fncall)    \
  do {                          \
    if ((fncall) != 0) abort(); \
  } while (0)

Mutex::Mutex()             { SAFE_PTHREAD(pthread_rwlock_init(&mutex_, NULL)); }
Mutex::~Mutex()            { SAFE_PTHREAD(pthread_rwlock_destroy(&mutex_)); }
void Mutex::Lock()         { SAFE_PTHREAD(pthread_rwlock_wrlock(&mutex_)); }
void Mutex::Unlock()       { SAFE_PTHREAD(pthread_rwlock_unlock(&mutex_)); }
void Mutex::ReaderLock()   { SAFE_PTHREAD(pthread_rwlock_rdlock(&mutex_)); }
void Mutex::ReaderUnlock() { SAFE_PTHREAD(pthread_rwlock_unlock(&mutex_)); }

#undef SAFE_PTHREAD

#else

Mutex::Mutex()             { }
Mutex::~Mutex()            { }
void Mutex::Lock()         { mutex_.lock(); }
void Mutex::Unlock()       { mutex_.unlock(); }
void Mutex::ReaderLock()   { Lock(); }  // C++11 doesn't have std::shared_mutex.
void Mutex::ReaderUnlock() { Unlock(); }

#endif

// --------------------------------------------------------------------------
// Some helper classes

// MutexLock(mu) acquires mu when constructed and releases it when destroyed.
class MutexLock {
 public:
  explicit MutexLock(Mutex *mu) : mu_(mu) { mu_->Lock(); }
  ~MutexLock() { mu_->Unlock(); }
 private:
  Mutex * const mu_;

  MutexLock(const MutexLock&) = delete;
  MutexLock& operator=(const MutexLock&) = delete;
};

// ReaderMutexLock and WriterMutexLock do the same, for rwlocks
class ReaderMutexLock {
 public:
  explicit ReaderMutexLock(Mutex *mu) : mu_(mu) { mu_->ReaderLock(); }
  ~ReaderMutexLock() { mu_->ReaderUnlock(); }
 private:
  Mutex * const mu_;

  ReaderMutexLock(const ReaderMutexLock&) = delete;
  ReaderMutexLock& operator=(const ReaderMutexLock&) = delete;
};

class WriterMutexLock {
 public:
  explicit WriterMutexLock(Mutex *mu) : mu_(mu) { mu_->WriterLock(); }
  ~WriterMutexLock() { mu_->WriterUnlock(); }
 private:
  Mutex * const mu_;

  WriterMutexLock(const WriterMutexLock&) = delete;
  WriterMutexLock& operator=(const WriterMutexLock&) = delete;
};

// Catch bug where variable name is omitted, e.g. MutexLock (&mu);
#define MutexLock(x) static_assert(false, "MutexLock declaration missing variable name")
#define ReaderMutexLock(x) static_assert(false, "ReaderMutexLock declaration missing variable name")
#define WriterMutexLock(x) static_assert(false, "WriterMutexLock declaration missing variable name")

}  // namespace duckdb_re2

#endif  // UTIL_MUTEX_H_


// LICENSE_CHANGE_END





// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2016 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef UTIL_STRUTIL_H_
#define UTIL_STRUTIL_H_

#include <string>




namespace duckdb_re2 {

std::string CEscape(const StringPiece& src);
void PrefixSuccessor(std::string* prefix);
std::string StringPrintf(const char* format, ...);
void SStringPrintf(std::string* dst, const char* format, ...);
void StringAppendF(std::string* dst, const char* format, ...);

}  // namespace duckdb_re2

#endif  // UTIL_STRUTIL_H_


// LICENSE_CHANGE_END




// Silence "zero-sized array in struct/union" warning for DFA::State::next_.
#ifdef _MSC_VER
//#pragma warning(disable: 4200)
#endif

namespace duckdb_re2 {

#if !defined(__linux__)  /* only Linux seems to have memrchr */
static void* memrchr(const void* s, int c, size_t n) {
  const unsigned char* p = (const unsigned char*)s;
  for (p += n; n > 0; n--)
    if (*--p == c)
      return (void*)p;

  return NULL;
}
#endif

// Controls whether the DFA should bail out early if the NFA would be faster.
static bool dfa_should_bail_when_slow = true;

// A DFA implementation of a regular expression program.
// Since this is entirely a forward declaration mandated by C++,
// some of the comments here are better understood after reading
// the comments in the sections that follow the DFA definition.
class DFA {
 public:
  DFA(Prog* prog, Prog::MatchKind kind, int64_t max_mem);
  ~DFA();
  bool ok() const { return !init_failed_; }
  Prog::MatchKind kind() { return kind_; }

  // Searches for the regular expression in text, which is considered
  // as a subsection of context for the purposes of interpreting flags
  // like ^ and $ and \A and \z.
  // Returns whether a match was found.
  // If a match is found, sets *ep to the end point of the best match in text.
  // If "anchored", the match must begin at the start of text.
  // If "want_earliest_match", the match that ends first is used, not
  //   necessarily the best one.
  // If "run_forward" is true, the DFA runs from text.begin() to text.end().
  //   If it is false, the DFA runs from text.end() to text.begin(),
  //   returning the leftmost end of the match instead of the rightmost one.
  // If the DFA cannot complete the search (for example, if it is out of
  //   memory), it sets *failed and returns false.
  bool Search(const StringPiece& text, const StringPiece& context,
              bool anchored, bool want_earliest_match, bool run_forward,
              bool* failed, const char** ep, SparseSet* matches);

  // Builds out all states for the entire DFA.
  // If cb is not empty, it receives one callback per state built.
  // Returns the number of states built.
  // FOR TESTING OR EXPERIMENTAL PURPOSES ONLY.
  int BuildAllStates(const Prog::DFAStateCallback& cb);

  // Computes min and max for matching strings.  Won't return strings
  // bigger than maxlen.
  bool PossibleMatchRange(std::string* min, std::string* max, int maxlen);

  // These data structures are logically private, but C++ makes it too
  // difficult to mark them as such.
  class RWLocker;
  class StateSaver;
  class Workq;

  // A single DFA state.  The DFA is represented as a graph of these
  // States, linked by the next_ pointers.  If in state s and reading
  // byte c, the next state should be s->next_[c].
  struct State {
    inline bool IsMatch() const { return (flag_ & kFlagMatch) != 0; }
    void SaveMatch(std::vector<int>* v);

    int* inst_;         // Instruction pointers in the state.
    int ninst_;         // # of inst_ pointers.
    uint32_t flag_;     // Empty string bitfield flags in effect on the way
                        // into this state, along with kFlagMatch if this
                        // is a matching state.

// Work around the bug affecting flexible array members in GCC 6.x (for x >= 1).
// (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=70932)
    std::atomic<State*> next_[1];    // Outgoing arrows from State,
                        // one per input byte class
  };

  // Marks separate thread groups of different priority
  // in the work queue when in leftmost-longest matching mode.
  constexpr static int32_t Mark = -1;
  // Separates the match IDs from the instructions in inst_.
  // Used only for "many match" DFA states.
  constexpr static int32_t MatchSep = -2;

  enum {
    kByteEndText = 256,         // imaginary byte at end of text

    kFlagEmptyMask = 0xFF,      // State.flag_: bits holding kEmptyXXX flags
    kFlagMatch = 0x0100,        // State.flag_: this is a matching state
    kFlagLastWord = 0x0200,     // State.flag_: last byte was a word char
    kFlagNeedShift = 16,        // needed kEmpty bits are or'ed in shifted left
  };

  struct StateHash {
    size_t operator()(const State* a) const {
      DCHECK(a != NULL);
      HashMix mix(a->flag_);
      for (int i = 0; i < a->ninst_; i++)
        mix.Mix(a->inst_[i]);
      mix.Mix(0);
      return mix.get();
    }
  };

  struct StateEqual {
    bool operator()(const State* a, const State* b) const {
      DCHECK(a != NULL);
      DCHECK(b != NULL);
      if (a == b)
        return true;
      if (a->flag_ != b->flag_)
        return false;
      if (a->ninst_ != b->ninst_)
        return false;
      for (int i = 0; i < a->ninst_; i++)
        if (a->inst_[i] != b->inst_[i])
          return false;
      return true;
    }
  };

  typedef std::unordered_set<State*, StateHash, StateEqual> StateSet;

 private:
  // Special "first_byte" values for a state.  (Values >= 0 denote actual bytes.)
  enum {
    kFbUnknown = -1,   // No analysis has been performed.
    kFbNone = -2,      // The first-byte trick cannot be used.
  };

  enum {
    // Indices into start_ for unanchored searches.
    // Add kStartAnchored for anchored searches.
    kStartBeginText = 0,          // text at beginning of context
    kStartBeginLine = 2,          // text at beginning of line
    kStartAfterWordChar = 4,      // text follows a word character
    kStartAfterNonWordChar = 6,   // text follows non-word character
    kMaxStart = 8,

    kStartAnchored = 1,
  };

  // Resets the DFA State cache, flushing all saved State* information.
  // Releases and reacquires cache_mutex_ via cache_lock, so any
  // State* existing before the call are not valid after the call.
  // Use a StateSaver to preserve important states across the call.
  // cache_mutex_.r <= L < mutex_
  // After: cache_mutex_.w <= L < mutex_
  void ResetCache(RWLocker* cache_lock);

  // Looks up and returns the State corresponding to a Workq.
  // L >= mutex_
  State* WorkqToCachedState(Workq* q, Workq* mq, uint32_t flag);

  // Looks up and returns a State matching the inst, ninst, and flag.
  // L >= mutex_
  State* CachedState(int* inst, int ninst, uint32_t flag);

  // Clear the cache entirely.
  // Must hold cache_mutex_.w or be in destructor.
  void ClearCache();

  // Converts a State into a Workq: the opposite of WorkqToCachedState.
  // L >= mutex_
  void StateToWorkq(State* s, Workq* q);

  // Runs a State on a given byte, returning the next state.
  State* RunStateOnByteUnlocked(State*, int);  // cache_mutex_.r <= L < mutex_
  State* RunStateOnByte(State*, int);          // L >= mutex_

  // Runs a Workq on a given byte followed by a set of empty-string flags,
  // producing a new Workq in nq.  If a match instruction is encountered,
  // sets *ismatch to true.
  // L >= mutex_
  void RunWorkqOnByte(Workq* q, Workq* nq,
                      int c, uint32_t flag, bool* ismatch);

  // Runs a Workq on a set of empty-string flags, producing a new Workq in nq.
  // L >= mutex_
  void RunWorkqOnEmptyString(Workq* q, Workq* nq, uint32_t flag);

  // Adds the instruction id to the Workq, following empty arrows
  // according to flag.
  // L >= mutex_
  void AddToQueue(Workq* q, int id, uint32_t flag);

  // For debugging, returns a text representation of State.
  static std::string DumpState(State* state);

  // For debugging, returns a text representation of a Workq.
  static std::string DumpWorkq(Workq* q);

  // Search parameters
  struct SearchParams {
    SearchParams(const StringPiece& text, const StringPiece& context,
                 RWLocker* cache_lock)
      : text(text), context(context),
        anchored(false),
        want_earliest_match(false),
        run_forward(false),
        start(NULL),
        first_byte(kFbUnknown),
        cache_lock(cache_lock),
        failed(false),
        ep(NULL),
        matches(NULL) { }

    StringPiece text;
    StringPiece context;
    bool anchored;
    bool want_earliest_match;
    bool run_forward;
    State* start;
    int first_byte;
    RWLocker *cache_lock;
    bool failed;     // "out" parameter: whether search gave up
    const char* ep;  // "out" parameter: end pointer for match
    SparseSet* matches;

   private:
    SearchParams(const SearchParams&) = delete;
    SearchParams& operator=(const SearchParams&) = delete;
  };

  // Before each search, the parameters to Search are analyzed by
  // AnalyzeSearch to determine the state in which to start and the
  // "first_byte" for that state, if any.
  struct StartInfo {
    StartInfo() : start(NULL), first_byte(kFbUnknown) {}
    State* start;
    std::atomic<int> first_byte;
  };

  // Fills in params->start and params->first_byte using
  // the other search parameters.  Returns true on success,
  // false on failure.
  // cache_mutex_.r <= L < mutex_
  bool AnalyzeSearch(SearchParams* params);
  bool AnalyzeSearchHelper(SearchParams* params, StartInfo* info,
                           uint32_t flags);

  // The generic search loop, inlined to create specialized versions.
  // cache_mutex_.r <= L < mutex_
  // Might unlock and relock cache_mutex_ via params->cache_lock.
  inline bool InlinedSearchLoop(SearchParams* params,
                                bool have_first_byte,
                                bool want_earliest_match,
                                bool run_forward);

  // The specialized versions of InlinedSearchLoop.  The three letters
  // at the ends of the name denote the true/false values used as the
  // last three parameters of InlinedSearchLoop.
  // cache_mutex_.r <= L < mutex_
  // Might unlock and relock cache_mutex_ via params->cache_lock.
  bool SearchFFF(SearchParams* params);
  bool SearchFFT(SearchParams* params);
  bool SearchFTF(SearchParams* params);
  bool SearchFTT(SearchParams* params);
  bool SearchTFF(SearchParams* params);
  bool SearchTFT(SearchParams* params);
  bool SearchTTF(SearchParams* params);
  bool SearchTTT(SearchParams* params);

  // The main search loop: calls an appropriate specialized version of
  // InlinedSearchLoop.
  // cache_mutex_.r <= L < mutex_
  // Might unlock and relock cache_mutex_ via params->cache_lock.
  bool FastSearchLoop(SearchParams* params);

  // For debugging, a slow search loop that calls InlinedSearchLoop
  // directly -- because the booleans passed are not constants, the
  // loop is not specialized like the SearchFFF etc. versions, so it
  // runs much more slowly.  Useful only for debugging.
  // cache_mutex_.r <= L < mutex_
  // Might unlock and relock cache_mutex_ via params->cache_lock.
  bool SlowSearchLoop(SearchParams* params);

  // Looks up bytes in bytemap_ but handles case c == kByteEndText too.
  int ByteMap(int c) {
    if (c == kByteEndText)
      return prog_->bytemap_range();
    return prog_->bytemap()[c];
  }

  // Constant after initialization.
  Prog* prog_;              // The regular expression program to run.
  Prog::MatchKind kind_;    // The kind of DFA.
  bool init_failed_;        // initialization failed (out of memory)

  Mutex mutex_;  // mutex_ >= cache_mutex_.r

  // Scratch areas, protected by mutex_.
  Workq* q0_;             // Two pre-allocated work queues.
  Workq* q1_;
  PODArray<int> stack_;   // Pre-allocated stack for AddToQueue

  // State* cache.  Many threads use and add to the cache simultaneously,
  // holding cache_mutex_ for reading and mutex_ (above) when adding.
  // If the cache fills and needs to be discarded, the discarding is done
  // while holding cache_mutex_ for writing, to avoid interrupting other
  // readers.  Any State* pointers are only valid while cache_mutex_
  // is held.
  Mutex cache_mutex_;
  int64_t mem_budget_;     // Total memory budget for all States.
  int64_t state_budget_;   // Amount of memory remaining for new States.
  StateSet state_cache_;   // All States computed so far.
  StartInfo start_[kMaxStart];
};

// Shorthand for casting to uint8_t*.
static inline const uint8_t* BytePtr(const void* v) {
  return reinterpret_cast<const uint8_t*>(v);
}

// Work queues

// Internally, the DFA uses a sparse array of
// program instruction pointers as a work queue.
// In leftmost longest mode, marks separate sections
// of workq that started executing at different
// locations in the string (earlier locations first).
class DFA::Workq : public SparseSet {
 public:
  // Constructor: n is number of normal slots, maxmark number of mark slots.
  Workq(int n, int maxmark) :
    SparseSet(n+maxmark),
    n_(n),
    maxmark_(maxmark),
    nextmark_(n),
    last_was_mark_(true) {
  }

  bool is_mark(int i) { return i >= n_; }

  int maxmark() { return maxmark_; }

  void clear() {
    SparseSet::clear();
    nextmark_ = n_;
  }

  void mark() {
    if (last_was_mark_)
      return;
    last_was_mark_ = false;
    SparseSet::insert_new(nextmark_++);
  }

  int size() {
    return n_ + maxmark_;
  }

  void insert(int id) {
    if (contains(id))
      return;
    insert_new(id);
  }

  void insert_new(int id) {
    last_was_mark_ = false;
    SparseSet::insert_new(id);
  }

 private:
  int n_;                // size excluding marks
  int maxmark_;          // maximum number of marks
  int nextmark_;         // id of next mark
  bool last_was_mark_;   // last inserted was mark

  Workq(const Workq&) = delete;
  Workq& operator=(const Workq&) = delete;
};

DFA::DFA(Prog* prog, Prog::MatchKind kind, int64_t max_mem)
  : prog_(prog),
    kind_(kind),
    init_failed_(false),
    q0_(NULL),
    q1_(NULL),
    mem_budget_(max_mem) {
  int nmark = 0;
  if (kind_ == Prog::kLongestMatch)
    nmark = prog_->size();
  // See DFA::AddToQueue() for why this is so.
  int nstack = prog_->inst_count(kInstCapture) +
               prog_->inst_count(kInstEmptyWidth) +
               prog_->inst_count(kInstNop) +
               nmark + 1;  // + 1 for start inst

  // Account for space needed for DFA, q0, q1, stack.
  mem_budget_ -= sizeof(DFA);
  mem_budget_ -= (prog_->size() + nmark) *
                 (sizeof(int)+sizeof(int)) * 2;  // q0, q1
  mem_budget_ -= nstack * sizeof(int);  // stack
  if (mem_budget_ < 0) {
    init_failed_ = true;
    return;
  }

  state_budget_ = mem_budget_;

  // Make sure there is a reasonable amount of working room left.
  // At minimum, the search requires room for two states in order
  // to limp along, restarting frequently.  We'll get better performance
  // if there is room for a larger number of states, say 20.
  // Note that a state stores list heads only, so we use the program
  // list count for the upper bound, not the program size.
  int nnext = prog_->bytemap_range() + 1;  // + 1 for kByteEndText slot
  int64_t one_state = sizeof(State) + nnext*sizeof(std::atomic<State*>) +
                      (prog_->list_count()+nmark)*sizeof(int);
  if (state_budget_ < 20*one_state) {
    init_failed_ = true;
    return;
  }

  q0_ = new Workq(prog_->size(), nmark);
  q1_ = new Workq(prog_->size(), nmark);
  stack_ = PODArray<int>(nstack);
}

DFA::~DFA() {
  delete q0_;
  delete q1_;
  ClearCache();
}

// In the DFA state graph, s->next[c] == NULL means that the
// state has not yet been computed and needs to be.  We need
// a different special value to signal that s->next[c] is a
// state that can never lead to a match (and thus the search
// can be called off).  Hence DeadState.
#define DeadState reinterpret_cast<State*>(1)

// Signals that the rest of the string matches no matter what it is.
#define FullMatchState reinterpret_cast<State*>(2)

#define SpecialStateMax FullMatchState

// Debugging printouts

// For debugging, returns a string representation of the work queue.
std::string DFA::DumpWorkq(Workq* q) {
  std::string s;
  const char* sep = "";
  for (Workq::iterator it = q->begin(); it != q->end(); ++it) {
    if (q->is_mark(*it)) {
      StringAppendF(&s, "|");
      sep = "";
    } else {
      StringAppendF(&s, "%s%d", sep, *it);
      sep = ",";
    }
  }
  return s;
}

// For debugging, returns a string representation of the state.
std::string DFA::DumpState(State* state) {
  if (state == NULL)
    return "_";
  if (state == DeadState)
    return "X";
  if (state == FullMatchState)
    return "*";
  std::string s;
  const char* sep = "";
  StringAppendF(&s, "(%p)", state);
  for (int i = 0; i < state->ninst_; i++) {
    if (state->inst_[i] == Mark) {
      StringAppendF(&s, "|");
      sep = "";
    } else if (state->inst_[i] == MatchSep) {
      StringAppendF(&s, "||");
      sep = "";
    } else {
      StringAppendF(&s, "%s%d", sep, state->inst_[i]);
      sep = ",";
    }
  }
  StringAppendF(&s, " flag=%#x", state->flag_);
  return s;
}

//////////////////////////////////////////////////////////////////////
//
// DFA state graph construction.
//
// The DFA state graph is a heavily-linked collection of State* structures.
// The state_cache_ is a set of all the State structures ever allocated,
// so that if the same state is reached by two different paths,
// the same State structure can be used.  This reduces allocation
// requirements and also avoids duplication of effort across the two
// identical states.
//
// A State is defined by an ordered list of instruction ids and a flag word.
//
// The choice of an ordered list of instructions differs from a typical
// textbook DFA implementation, which would use an unordered set.
// Textbook descriptions, however, only care about whether
// the DFA matches, not where it matches in the text.  To decide where the
// DFA matches, we need to mimic the behavior of the dominant backtracking
// implementations like PCRE, which try one possible regular expression
// execution, then another, then another, stopping when one of them succeeds.
// The DFA execution tries these many executions in parallel, representing
// each by an instruction id.  These pointers are ordered in the State.inst_
// list in the same order that the executions would happen in a backtracking
// search: if a match is found during execution of inst_[2], inst_[i] for i>=3
// can be discarded.
//
// Textbooks also typically do not consider context-aware empty string operators
// like ^ or $.  These are handled by the flag word, which specifies the set
// of empty-string operators that should be matched when executing at the
// current text position.  These flag bits are defined in prog.h.
// The flag word also contains two DFA-specific bits: kFlagMatch if the state
// is a matching state (one that reached a kInstMatch in the program)
// and kFlagLastWord if the last processed byte was a word character, for the
// implementation of \B and \b.
//
// The flag word also contains, shifted up 16 bits, the bits looked for by
// any kInstEmptyWidth instructions in the state.  These provide a useful
// summary indicating when new flags might be useful.
//
// The permanent representation of a State's instruction ids is just an array,
// but while a state is being analyzed, these instruction ids are represented
// as a Workq, which is an array that allows iteration in insertion order.

// NOTE(rsc): The choice of State construction determines whether the DFA
// mimics backtracking implementations (so-called leftmost first matching) or
// traditional DFA implementations (so-called leftmost longest matching as
// prescribed by POSIX).  This implementation chooses to mimic the
// backtracking implementations, because we want to replace PCRE.  To get
// POSIX behavior, the states would need to be considered not as a simple
// ordered list of instruction ids, but as a list of unordered sets of instruction
// ids.  A match by a state in one set would inhibit the running of sets
// farther down the list but not other instruction ids in the same set.  Each
// set would correspond to matches beginning at a given point in the string.
// This is implemented by separating different sets with Mark pointers.

// Looks in the State cache for a State matching q, flag.
// If one is found, returns it.  If one is not found, allocates one,
// inserts it in the cache, and returns it.
// If mq is not null, MatchSep and the match IDs in mq will be appended
// to the State.
DFA::State* DFA::WorkqToCachedState(Workq* q, Workq* mq, uint32_t flag) {
  //mutex_.AssertHeld();

  // Construct array of instruction ids for the new state.
  // Only ByteRange, EmptyWidth, and Match instructions are useful to keep:
  // those are the only operators with any effect in
  // RunWorkqOnEmptyString or RunWorkqOnByte.
  int* inst = new int[q->size()];
  int n = 0;
  uint32_t needflags = 0;  // flags needed by kInstEmptyWidth instructions
  bool sawmatch = false;   // whether queue contains guaranteed kInstMatch
  bool sawmark = false;    // whether queue contains a Mark
  for (Workq::iterator it = q->begin(); it != q->end(); ++it) {
    int id = *it;
    if (sawmatch && (kind_ == Prog::kFirstMatch || q->is_mark(id)))
      break;
    if (q->is_mark(id)) {
      if (n > 0 && inst[n-1] != Mark) {
        sawmark = true;
        inst[n++] = Mark;
      }
      continue;
    }
    Prog::Inst* ip = prog_->inst(id);
    switch (ip->opcode()) {
      case kInstAltMatch:
        // This state will continue to a match no matter what
        // the rest of the input is.  If it is the highest priority match
        // being considered, return the special FullMatchState
        // to indicate that it's all matches from here out.
        if (kind_ != Prog::kManyMatch &&
            (kind_ != Prog::kFirstMatch ||
             (it == q->begin() && ip->greedy(prog_))) &&
            (kind_ != Prog::kLongestMatch || !sawmark) &&
            (flag & kFlagMatch)) {
          delete[] inst;
          return FullMatchState;
        }
        FALLTHROUGH_INTENDED;
      default:
        // Record iff id is the head of its list, which must
        // be the case if id-1 is the last of *its* list. :)
        if (prog_->inst(id-1)->last())
          inst[n++] = *it;
        if (ip->opcode() == kInstEmptyWidth)
          needflags |= ip->empty();
        if (ip->opcode() == kInstMatch && !prog_->anchor_end())
          sawmatch = true;
        break;
    }
  }
  DCHECK_LE(n, q->size());
  if (n > 0 && inst[n-1] == Mark)
    n--;

  // If there are no empty-width instructions waiting to execute,
  // then the extra flag bits will not be used, so there is no
  // point in saving them.  (Discarding them reduces the number
  // of distinct states.)
  if (needflags == 0)
    flag &= kFlagMatch;

  // NOTE(rsc): The code above cannot do flag &= needflags,
  // because if the right flags were present to pass the current
  // kInstEmptyWidth instructions, new kInstEmptyWidth instructions
  // might be reached that in turn need different flags.
  // The only sure thing is that if there are no kInstEmptyWidth
  // instructions at all, no flags will be needed.
  // We could do the extra work to figure out the full set of
  // possibly needed flags by exploring past the kInstEmptyWidth
  // instructions, but the check above -- are any flags needed
  // at all? -- handles the most common case.  More fine-grained
  // analysis can only be justified by measurements showing that
  // too many redundant states are being allocated.

  // If there are no Insts in the list, it's a dead state,
  // which is useful to signal with a special pointer so that
  // the execution loop can stop early.  This is only okay
  // if the state is *not* a matching state.
  if (n == 0 && flag == 0) {
    delete[] inst;
    return DeadState;
  }

  // If we're in longest match mode, the state is a sequence of
  // unordered state sets separated by Marks.  Sort each set
  // to canonicalize, to reduce the number of distinct sets stored.
  if (kind_ == Prog::kLongestMatch) {
    int* ip = inst;
    int* ep = ip + n;
    while (ip < ep) {
      int* markp = ip;
      while (markp < ep && *markp != Mark)
        markp++;
      std::sort(ip, markp);
      if (markp < ep)
        markp++;
      ip = markp;
    }
  }

  // Append MatchSep and the match IDs in mq if necessary.
  if (mq != NULL) {
    inst[n++] = MatchSep;
    for (Workq::iterator i = mq->begin(); i != mq->end(); ++i) {
      int id = *i;
      Prog::Inst* ip = prog_->inst(id);
      if (ip->opcode() == kInstMatch)
        inst[n++] = ip->match_id();
    }
  }

  // Save the needed empty-width flags in the top bits for use later.
  flag |= needflags << kFlagNeedShift;

  State* state = CachedState(inst, n, flag);
  delete[] inst;
  return state;
}

// Looks in the State cache for a State matching inst, ninst, flag.
// If one is found, returns it.  If one is not found, allocates one,
// inserts it in the cache, and returns it.
DFA::State* DFA::CachedState(int* inst, int ninst, uint32_t flag) {
  //mutex_.AssertHeld();

  // Look in the cache for a pre-existing state.
  // We have to initialise the struct like this because otherwise
  // MSVC will complain about the flexible array member. :(
  State state;
  state.inst_ = inst;
  state.ninst_ = ninst;
  state.flag_ = flag;
  StateSet::iterator it = state_cache_.find(&state);
  if (it != state_cache_.end()) {
    return *it;
  }

  // Must have enough memory for new state.
  // In addition to what we're going to allocate,
  // the state cache hash table seems to incur about 40 bytes per
  // State*, empirically.
  const int kStateCacheOverhead = 40;
  int nnext = prog_->bytemap_range() + 1;  // + 1 for kByteEndText slot
  int mem = sizeof(State) + nnext*sizeof(std::atomic<State*>) +
            ninst*sizeof(int);
  if (mem_budget_ < mem + kStateCacheOverhead) {
    mem_budget_ = -1;
    return NULL;
  }
  mem_budget_ -= mem + kStateCacheOverhead;

  // Allocate new state along with room for next_ and inst_.
  char* space = std::allocator<char>().allocate(mem);
  State* s = new (space) State;
  (void) new (s->next_) std::atomic<State*>[nnext];
  // Work around a unfortunate bug in older versions of libstdc++.
  // (https://gcc.gnu.org/bugzilla/show_bug.cgi?id=64658)
  for (int i = 0; i < nnext; i++)
    (void) new (s->next_ + i) std::atomic<State*>(NULL);
  s->inst_ = new (s->next_ + nnext) int[ninst];
  memmove(s->inst_, inst, ninst*sizeof s->inst_[0]);
  s->ninst_ = ninst;
  s->flag_ = flag;
  // Put state in cache and return it.
  state_cache_.insert(s);
  return s;
}

// Clear the cache.  Must hold cache_mutex_.w or be in destructor.
void DFA::ClearCache() {
  StateSet::iterator begin = state_cache_.begin();
  StateSet::iterator end = state_cache_.end();
  while (begin != end) {
    StateSet::iterator tmp = begin;
    ++begin;
    // Deallocate the blob of memory that we allocated in DFA::CachedState().
    // We recompute mem in order to benefit from sized delete where possible.
    int ninst = (*tmp)->ninst_;
    int nnext = prog_->bytemap_range() + 1;  // + 1 for kByteEndText slot
    int mem = sizeof(State) + nnext*sizeof(std::atomic<State*>) +
              ninst*sizeof(int);
    std::allocator<char>().deallocate(reinterpret_cast<char*>(*tmp), mem);
  }
  state_cache_.clear();
}

// Copies insts in state s to the work queue q.
void DFA::StateToWorkq(State* s, Workq* q) {
  q->clear();
  for (int i = 0; i < s->ninst_; i++) {
    if (s->inst_[i] == Mark) {
      q->mark();
    } else if (s->inst_[i] == MatchSep) {
      // Nothing after this is an instruction!
      break;
    } else {
      // Explore from the head of the list.
      AddToQueue(q, s->inst_[i], s->flag_ & kFlagEmptyMask);
    }
  }
}

// Adds ip to the work queue, following empty arrows according to flag.
void DFA::AddToQueue(Workq* q, int id, uint32_t flag) {

  // Use stack_ to hold our stack of instructions yet to process.
  // It was preallocated as follows:
  //   one entry per Capture;
  //   one entry per EmptyWidth; and
  //   one entry per Nop.
  // This reflects the maximum number of stack pushes that each can
  // perform. (Each instruction can be processed at most once.)
  // When using marks, we also added nmark == prog_->size().
  // (Otherwise, nmark == 0.)
  int* stk = stack_.data();
  int nstk = 0;

  stk[nstk++] = id;
  while (nstk > 0) {
    DCHECK_LE(nstk, stack_.size());
    id = stk[--nstk];

  Loop:
    if (id == Mark) {
      q->mark();
      continue;
    }

    if (id == 0)
      continue;

    // If ip is already on the queue, nothing to do.
    // Otherwise add it.  We don't actually keep all the
    // ones that get added, but adding all of them here
    // increases the likelihood of q->contains(id),
    // reducing the amount of duplicated work.
    if (q->contains(id))
      continue;
    q->insert_new(id);

    // Process instruction.
    Prog::Inst* ip = prog_->inst(id);
    switch (ip->opcode()) {
      default:
        LOG(DFATAL) << "unhandled opcode: " << ip->opcode();
        break;

      case kInstByteRange:  // just save these on the queue
      case kInstMatch:
        if (ip->last())
          break;
        id = id+1;
        goto Loop;

      case kInstCapture:    // DFA treats captures as no-ops.
      case kInstNop:
        if (!ip->last())
          stk[nstk++] = id+1;

        // If this instruction is the [00-FF]* loop at the beginning of
        // a leftmost-longest unanchored search, separate with a Mark so
        // that future threads (which will start farther to the right in
        // the input string) are lower priority than current threads.
        if (ip->opcode() == kInstNop && q->maxmark() > 0 &&
            id == prog_->start_unanchored() && id != prog_->start())
          stk[nstk++] = Mark;
        id = ip->out();
        goto Loop;

      case kInstAltMatch:
        DCHECK(!ip->last());
        id = id+1;
        goto Loop;

      case kInstEmptyWidth:
        if (!ip->last())
          stk[nstk++] = id+1;

        // Continue on if we have all the right flag bits.
        if (ip->empty() & ~flag)
          break;
        id = ip->out();
        goto Loop;
    }
  }
}

// Running of work queues.  In the work queue, order matters:
// the queue is sorted in priority order.  If instruction i comes before j,
// then the instructions that i produces during the run must come before
// the ones that j produces.  In order to keep this invariant, all the
// work queue runners have to take an old queue to process and then
// also a new queue to fill in.  It's not acceptable to add to the end of
// an existing queue, because new instructions will not end up in the
// correct position.

// Runs the work queue, processing the empty strings indicated by flag.
// For example, flag == kEmptyBeginLine|kEmptyEndLine means to match
// both ^ and $.  It is important that callers pass all flags at once:
// processing both ^ and $ is not the same as first processing only ^
// and then processing only $.  Doing the two-step sequence won't match
// ^$^$^$ but processing ^ and $ simultaneously will (and is the behavior
// exhibited by existing implementations).
void DFA::RunWorkqOnEmptyString(Workq* oldq, Workq* newq, uint32_t flag) {
  newq->clear();
  for (Workq::iterator i = oldq->begin(); i != oldq->end(); ++i) {
    if (oldq->is_mark(*i))
      AddToQueue(newq, Mark, flag);
    else
      AddToQueue(newq, *i, flag);
  }
}

// Runs the work queue, processing the single byte c followed by any empty
// strings indicated by flag.  For example, c == 'a' and flag == kEmptyEndLine,
// means to match c$.  Sets the bool *ismatch to true if the end of the
// regular expression program has been reached (the regexp has matched).
void DFA::RunWorkqOnByte(Workq* oldq, Workq* newq,
                         int c, uint32_t flag, bool* ismatch) {
  //mutex_.AssertHeld();

  newq->clear();
  for (Workq::iterator i = oldq->begin(); i != oldq->end(); ++i) {
    if (oldq->is_mark(*i)) {
      if (*ismatch)
        return;
      newq->mark();
      continue;
    }
    int id = *i;
    Prog::Inst* ip = prog_->inst(id);
    switch (ip->opcode()) {
      default:
        LOG(DFATAL) << "unhandled opcode: " << ip->opcode();
        break;

      case kInstFail:        // never succeeds
      case kInstCapture:     // already followed
      case kInstNop:         // already followed
      case kInstAltMatch:    // already followed
      case kInstEmptyWidth:  // already followed
        break;

      case kInstByteRange:   // can follow if c is in range
        if (ip->Matches(c))
          AddToQueue(newq, ip->out(), flag);
        break;

      case kInstMatch:
        if (prog_->anchor_end() && c != kByteEndText &&
            kind_ != Prog::kManyMatch)
          break;
        *ismatch = true;
        if (kind_ == Prog::kFirstMatch) {
          // Can stop processing work queue since we found a match.
          return;
        }
        break;
    }
  }
}

// Processes input byte c in state, returning new state.
// Caller does not hold mutex.
DFA::State* DFA::RunStateOnByteUnlocked(State* state, int c) {
  // Keep only one RunStateOnByte going
  // even if the DFA is being run by multiple threads.
  MutexLock l(&mutex_);
  return RunStateOnByte(state, c);
}

// Processes input byte c in state, returning new state.
DFA::State* DFA::RunStateOnByte(State* state, int c) {
  //mutex_.AssertHeld();

  if (state <= SpecialStateMax) {
    if (state == FullMatchState) {
      // It is convenient for routines like PossibleMatchRange
      // if we implement RunStateOnByte for FullMatchState:
      // once you get into this state you never get out,
      // so it's pretty easy.
      return FullMatchState;
    }
    if (state == DeadState) {
      LOG(DFATAL) << "DeadState in RunStateOnByte";
      return NULL;
    }
    if (state == NULL) {
      LOG(DFATAL) << "NULL state in RunStateOnByte";
      return NULL;
    }
    LOG(DFATAL) << "Unexpected special state in RunStateOnByte";
    return NULL;
  }

  // If someone else already computed this, return it.
  State* ns = state->next_[ByteMap(c)].load(std::memory_order_relaxed);
  if (ns != NULL)
    return ns;

  // Convert state into Workq.
  StateToWorkq(state, q0_);

  // Flags marking the kinds of empty-width things (^ $ etc)
  // around this byte.  Before the byte we have the flags recorded
  // in the State structure itself.  After the byte we have
  // nothing yet (but that will change: read on).
  uint32_t needflag = state->flag_ >> kFlagNeedShift;
  uint32_t beforeflag = state->flag_ & kFlagEmptyMask;
  uint32_t oldbeforeflag = beforeflag;
  uint32_t afterflag = 0;

  if (c == '\n') {
    // Insert implicit $ and ^ around \n
    beforeflag |= kEmptyEndLine;
    afterflag |= kEmptyBeginLine;
  }

  if (c == kByteEndText) {
    // Insert implicit $ and \z before the fake "end text" byte.
    beforeflag |= kEmptyEndLine | kEmptyEndText;
  }

  // The state flag kFlagLastWord says whether the last
  // byte processed was a word character.  Use that info to
  // insert empty-width (non-)word boundaries.
  bool islastword = (state->flag_ & kFlagLastWord) != 0;
  bool isword = c != kByteEndText && Prog::IsWordChar(static_cast<uint8_t>(c));
  if (isword == islastword)
    beforeflag |= kEmptyNonWordBoundary;
  else
    beforeflag |= kEmptyWordBoundary;

  // Okay, finally ready to run.
  // Only useful to rerun on empty string if there are new, useful flags.
  if (beforeflag & ~oldbeforeflag & needflag) {
    RunWorkqOnEmptyString(q0_, q1_, beforeflag);
    using std::swap;
    swap(q0_, q1_);
  }
  bool ismatch = false;
  RunWorkqOnByte(q0_, q1_, c, afterflag, &ismatch);
  using std::swap;
  swap(q0_, q1_);

  // Save afterflag along with ismatch and isword in new state.
  uint32_t flag = afterflag;
  if (ismatch)
    flag |= kFlagMatch;
  if (isword)
    flag |= kFlagLastWord;

  if (ismatch && kind_ == Prog::kManyMatch)
    ns = WorkqToCachedState(q0_, q1_, flag);
  else
    ns = WorkqToCachedState(q0_, NULL, flag);

  // Flush ns before linking to it.
  // Write barrier before updating state->next_ so that the
  // main search loop can proceed without any locking, for speed.
  // (Otherwise it would need one mutex operation per input byte.)
  state->next_[ByteMap(c)].store(ns, std::memory_order_release);
  return ns;
}


//////////////////////////////////////////////////////////////////////
// DFA cache reset.

// Reader-writer lock helper.
//
// The DFA uses a reader-writer mutex to protect the state graph itself.
// Traversing the state graph requires holding the mutex for reading,
// and discarding the state graph and starting over requires holding the
// lock for writing.  If a search needs to expand the graph but is out
// of memory, it will need to drop its read lock and then acquire the
// write lock.  Since it cannot then atomically downgrade from write lock
// to read lock, it runs the rest of the search holding the write lock.
// (This probably helps avoid repeated contention, but really the decision
// is forced by the Mutex interface.)  It's a bit complicated to keep
// track of whether the lock is held for reading or writing and thread
// that through the search, so instead we encapsulate it in the RWLocker
// and pass that around.

class DFA::RWLocker {
 public:
  explicit RWLocker(Mutex* mu);
  ~RWLocker();

  // If the lock is only held for reading right now,
  // drop the read lock and re-acquire for writing.
  // Subsequent calls to LockForWriting are no-ops.
  // Notice that the lock is *released* temporarily.
  void LockForWriting();

 private:
  Mutex* mu_;
  bool writing_;

  RWLocker(const RWLocker&) = delete;
  RWLocker& operator=(const RWLocker&) = delete;
};

DFA::RWLocker::RWLocker(Mutex* mu) : mu_(mu), writing_(false) {
  mu_->ReaderLock();
}

// This function is marked as NO_THREAD_SAFETY_ANALYSIS because the annotations
// does not support lock upgrade.
void DFA::RWLocker::LockForWriting() NO_THREAD_SAFETY_ANALYSIS {
  if (!writing_) {
    mu_->ReaderUnlock();
    mu_->WriterLock();
    writing_ = true;
  }
}

DFA::RWLocker::~RWLocker() {
  if (!writing_)
    mu_->ReaderUnlock();
  else
    mu_->WriterUnlock();
}


// When the DFA's State cache fills, we discard all the states in the
// cache and start over.  Many threads can be using and adding to the
// cache at the same time, so we synchronize using the cache_mutex_
// to keep from stepping on other threads.  Specifically, all the
// threads using the current cache hold cache_mutex_ for reading.
// When a thread decides to flush the cache, it drops cache_mutex_
// and then re-acquires it for writing.  That ensures there are no
// other threads accessing the cache anymore.  The rest of the search
// runs holding cache_mutex_ for writing, avoiding any contention
// with or cache pollution caused by other threads.

void DFA::ResetCache(RWLocker* cache_lock) {
  // Re-acquire the cache_mutex_ for writing (exclusive use).
  cache_lock->LockForWriting();

  // Clear the cache, reset the memory budget.
  for (int i = 0; i < kMaxStart; i++) {
    start_[i].start = NULL;
    start_[i].first_byte.store(kFbUnknown, std::memory_order_relaxed);
  }
  ClearCache();
  mem_budget_ = state_budget_;
}

// Typically, a couple States do need to be preserved across a cache
// reset, like the State at the current point in the search.
// The StateSaver class helps keep States across cache resets.
// It makes a copy of the state's guts outside the cache (before the reset)
// and then can be asked, after the reset, to recreate the State
// in the new cache.  For example, in a DFA method ("this" is a DFA):
//
//   StateSaver saver(this, s);
//   ResetCache(cache_lock);
//   s = saver.Restore();
//
// The saver should always have room in the cache to re-create the state,
// because resetting the cache locks out all other threads, and the cache
// is known to have room for at least a couple states (otherwise the DFA
// constructor fails).

class DFA::StateSaver {
 public:
  explicit StateSaver(DFA* dfa, State* state);
  ~StateSaver();

  // Recreates and returns a state equivalent to the
  // original state passed to the constructor.
  // Returns NULL if the cache has filled, but
  // since the DFA guarantees to have room in the cache
  // for a couple states, should never return NULL
  // if used right after ResetCache.
  State* Restore();

 private:
  DFA* dfa_;         // the DFA to use
  int* inst_;        // saved info from State
  int ninst_;
  uint32_t flag_;
  bool is_special_;  // whether original state was special
  State* special_;   // if is_special_, the original state

  StateSaver(const StateSaver&) = delete;
  StateSaver& operator=(const StateSaver&) = delete;
};

DFA::StateSaver::StateSaver(DFA* dfa, State* state) {
  dfa_ = dfa;
  if (state <= SpecialStateMax) {
    inst_ = NULL;
    ninst_ = 0;
    flag_ = 0;
    is_special_ = true;
    special_ = state;
    return;
  }
  is_special_ = false;
  special_ = NULL;
  flag_ = state->flag_;
  ninst_ = state->ninst_;
  inst_ = new int[ninst_];
  memmove(inst_, state->inst_, ninst_*sizeof inst_[0]);
}

DFA::StateSaver::~StateSaver() {
  if (!is_special_)
    delete[] inst_;
}

DFA::State* DFA::StateSaver::Restore() {
  if (is_special_)
    return special_;
  MutexLock l(&dfa_->mutex_);
  State* s = dfa_->CachedState(inst_, ninst_, flag_);
  if (s == NULL)
    LOG(DFATAL) << "StateSaver failed to restore state.";
  return s;
}


//////////////////////////////////////////////////////////////////////
//
// DFA execution.
//
// The basic search loop is easy: start in a state s and then for each
// byte c in the input, s = s->next[c].
//
// This simple description omits a few efficiency-driven complications.
//
// First, the State graph is constructed incrementally: it is possible
// that s->next[c] is null, indicating that that state has not been
// fully explored.  In this case, RunStateOnByte must be invoked to
// determine the next state, which is cached in s->next[c] to save
// future effort.  An alternative reason for s->next[c] to be null is
// that the DFA has reached a so-called "dead state", in which any match
// is no longer possible.  In this case RunStateOnByte will return NULL
// and the processing of the string can stop early.
//
// Second, a 256-element pointer array for s->next_ makes each State
// quite large (2kB on 64-bit machines).  Instead, dfa->bytemap_[]
// maps from bytes to "byte classes" and then next_ only needs to have
// as many pointers as there are byte classes.  A byte class is simply a
// range of bytes that the regexp never distinguishes between.
// A regexp looking for a[abc] would have four byte ranges -- 0 to 'a'-1,
// 'a', 'b' to 'c', and 'c' to 0xFF.  The bytemap slows us a little bit
// but in exchange we typically cut the size of a State (and thus our
// memory footprint) by about 5-10x.  The comments still refer to
// s->next[c] for simplicity, but code should refer to s->next_[bytemap_[c]].
//
// Third, it is common for a DFA for an unanchored match to begin in a
// state in which only one particular byte value can take the DFA to a
// different state.  That is, s->next[c] != s for only one c.  In this
// situation, the DFA can do better than executing the simple loop.
// Instead, it can call memchr to search very quickly for the byte c.
// Whether the start state has this property is determined during a
// pre-compilation pass, and if so, the byte b is passed to the search
// loop as the "first_byte" argument, along with a boolean "have_first_byte".
//
// Fourth, the desired behavior is to search for the leftmost-best match
// (approximately, the same one that Perl would find), which is not
// necessarily the match ending earliest in the string.  Each time a
// match is found, it must be noted, but the DFA must continue on in
// hope of finding a higher-priority match.  In some cases, the caller only
// cares whether there is any match at all, not which one is found.
// The "want_earliest_match" flag causes the search to stop at the first
// match found.
//
// Fifth, one algorithm that uses the DFA needs it to run over the
// input string backward, beginning at the end and ending at the beginning.
// Passing false for the "run_forward" flag causes the DFA to run backward.
//
// The checks for these last three cases, which in a naive implementation
// would be performed once per input byte, slow the general loop enough
// to merit specialized versions of the search loop for each of the
// eight possible settings of the three booleans.  Rather than write
// eight different functions, we write one general implementation and then
// inline it to create the specialized ones.
//
// Note that matches are delayed by one byte, to make it easier to
// accommodate match conditions depending on the next input byte (like $ and \b).
// When s->next[c]->IsMatch(), it means that there is a match ending just
// *before* byte c.

// The generic search loop.  Searches text for a match, returning
// the pointer to the end of the chosen match, or NULL if no match.
// The bools are equal to the same-named variables in params, but
// making them function arguments lets the inliner specialize
// this function to each combination (see two paragraphs above).
inline bool DFA::InlinedSearchLoop(SearchParams* params,
                                   bool have_first_byte,
                                   bool want_earliest_match,
                                   bool run_forward) {
  State* start = params->start;
  const uint8_t* bp = BytePtr(params->text.begin());  // start of text
  const uint8_t* p = bp;                              // text scanning point
  const uint8_t* ep = BytePtr(params->text.end());    // end of text
  const uint8_t* resetp = NULL;                       // p at last cache reset
  if (!run_forward) {
    using std::swap;
    swap(p, ep);
  }

  const uint8_t* bytemap = prog_->bytemap();
  const uint8_t* lastmatch = NULL;   // most recent matching position in text
  bool matched = false;

  State* s = start;

  if (s->IsMatch()) {
    matched = true;
    lastmatch = p;
    if (params->matches != NULL && kind_ == Prog::kManyMatch) {
      for (int i = s->ninst_ - 1; i >= 0; i--) {
        int id = s->inst_[i];
        if (id == MatchSep)
          break;
        params->matches->insert(id);
      }
    }
    if (want_earliest_match) {
      params->ep = reinterpret_cast<const char*>(lastmatch);
      return true;
    }
  }

  while (p != ep) {
    if (have_first_byte && s == start) {
      // In start state, only way out is to find first_byte,
      // so use optimized assembly in memchr to skip ahead.
      // If first_byte isn't found, we can skip to the end
      // of the string.
      if (run_forward) {
        if ((p = BytePtr(memchr(p, params->first_byte, ep - p))) == NULL) {
          p = ep;
          break;
        }
      } else {
        if ((p = BytePtr(memrchr(ep, params->first_byte, p - ep))) == NULL) {
          p = ep;
          break;
        }
        p++;
      }
    }

    int c;
    if (run_forward)
      c = *p++;
    else
      c = *--p;

    // Note that multiple threads might be consulting
    // s->next_[bytemap[c]] simultaneously.
    // RunStateOnByte takes care of the appropriate locking,
    // including a memory barrier so that the unlocked access
    // (sometimes known as "double-checked locking") is safe.
    // The alternative would be either one DFA per thread
    // or one mutex operation per input byte.
    //
    // ns == DeadState means the state is known to be dead
    // (no more matches are possible).
    // ns == NULL means the state has not yet been computed
    // (need to call RunStateOnByteUnlocked).
    // RunStateOnByte returns ns == NULL if it is out of memory.
    // ns == FullMatchState means the rest of the string matches.
    //
    // Okay to use bytemap[] not ByteMap() here, because
    // c is known to be an actual byte and not kByteEndText.

    State* ns = s->next_[bytemap[c]].load(std::memory_order_acquire);
    if (ns == NULL) {
      ns = RunStateOnByteUnlocked(s, c);
      if (ns == NULL) {
        // After we reset the cache, we hold cache_mutex exclusively,
        // so if resetp != NULL, it means we filled the DFA state
        // cache with this search alone (without any other threads).
        // Benchmarks show that doing a state computation on every
        // byte runs at about 0.2 MB/s, while the NFA (nfa.cc) can do the
        // same at about 2 MB/s.  Unless we're processing an average
        // of 10 bytes per state computation, fail so that RE2 can
        // fall back to the NFA.
        if (dfa_should_bail_when_slow && resetp != NULL &&
            static_cast<size_t>(p - resetp) < 10*state_cache_.size()) {
          params->failed = true;
          return false;
        }
        resetp = p;

        // Prepare to save start and s across the reset.
        StateSaver save_start(this, start);
        StateSaver save_s(this, s);

        // Discard all the States in the cache.
        ResetCache(params->cache_lock);

        // Restore start and s so we can continue.
        if ((start = save_start.Restore()) == NULL ||
            (s = save_s.Restore()) == NULL) {
          // Restore already did LOG(DFATAL).
          params->failed = true;
          return false;
        }
        ns = RunStateOnByteUnlocked(s, c);
        if (ns == NULL) {
          LOG(DFATAL) << "RunStateOnByteUnlocked failed after ResetCache";
          params->failed = true;
          return false;
        }
      }
    }
    if (ns <= SpecialStateMax) {
      if (ns == DeadState) {
        params->ep = reinterpret_cast<const char*>(lastmatch);
        return matched;
      }
      // FullMatchState
      params->ep = reinterpret_cast<const char*>(ep);
      return true;
    }

    s = ns;
    if (s->IsMatch()) {
      matched = true;
      // The DFA notices the match one byte late,
      // so adjust p before using it in the match.
      if (run_forward)
        lastmatch = p - 1;
      else
        lastmatch = p + 1;
      if (params->matches != NULL && kind_ == Prog::kManyMatch) {
        for (int i = s->ninst_ - 1; i >= 0; i--) {
          int id = s->inst_[i];
          if (id == MatchSep)
            break;
          params->matches->insert(id);
        }
      }
      if (want_earliest_match) {
        params->ep = reinterpret_cast<const char*>(lastmatch);
        return true;
      }
    }
  }

  // Process one more byte to see if it triggers a match.
  // (Remember, matches are delayed one byte.)
  int lastbyte;
  if (run_forward) {
    if (params->text.end() == params->context.end())
      lastbyte = kByteEndText;
    else
      lastbyte = params->text.end()[0] & 0xFF;
  } else {
    if (params->text.begin() == params->context.begin())
      lastbyte = kByteEndText;
    else
      lastbyte = params->text.begin()[-1] & 0xFF;
  }

  State* ns = s->next_[ByteMap(lastbyte)].load(std::memory_order_acquire);
  if (ns == NULL) {
    ns = RunStateOnByteUnlocked(s, lastbyte);
    if (ns == NULL) {
      StateSaver save_s(this, s);
      ResetCache(params->cache_lock);
      if ((s = save_s.Restore()) == NULL) {
        params->failed = true;
        return false;
      }
      ns = RunStateOnByteUnlocked(s, lastbyte);
      if (ns == NULL) {
        LOG(DFATAL) << "RunStateOnByteUnlocked failed after Reset";
        params->failed = true;
        return false;
      }
    }
  }
  if (ns <= SpecialStateMax) {
    if (ns == DeadState) {
      params->ep = reinterpret_cast<const char*>(lastmatch);
      return matched;
    }
    // FullMatchState
    params->ep = reinterpret_cast<const char*>(ep);
    return true;
  }

  s = ns;
  if (s->IsMatch()) {
    matched = true;
    lastmatch = p;
    if (params->matches != NULL && kind_ == Prog::kManyMatch) {
      for (int i = s->ninst_ - 1; i >= 0; i--) {
        int id = s->inst_[i];
        if (id == MatchSep)
          break;
        params->matches->insert(id);
      }
    }
  }

  params->ep = reinterpret_cast<const char*>(lastmatch);
  return matched;
}

// Inline specializations of the general loop.
bool DFA::SearchFFF(SearchParams* params) {
  return InlinedSearchLoop(params, 0, 0, 0);
}
bool DFA::SearchFFT(SearchParams* params) {
  return InlinedSearchLoop(params, 0, 0, 1);
}
bool DFA::SearchFTF(SearchParams* params) {
  return InlinedSearchLoop(params, 0, 1, 0);
}
bool DFA::SearchFTT(SearchParams* params) {
  return InlinedSearchLoop(params, 0, 1, 1);
}
bool DFA::SearchTFF(SearchParams* params) {
  return InlinedSearchLoop(params, 1, 0, 0);
}
bool DFA::SearchTFT(SearchParams* params) {
  return InlinedSearchLoop(params, 1, 0, 1);
}
bool DFA::SearchTTF(SearchParams* params) {
  return InlinedSearchLoop(params, 1, 1, 0);
}
bool DFA::SearchTTT(SearchParams* params) {
  return InlinedSearchLoop(params, 1, 1, 1);
}

// For debugging, calls the general code directly.
bool DFA::SlowSearchLoop(SearchParams* params) {
  return InlinedSearchLoop(params,
                           params->first_byte >= 0,
                           params->want_earliest_match,
                           params->run_forward);
}

// For performance, calls the appropriate specialized version
// of InlinedSearchLoop.
bool DFA::FastSearchLoop(SearchParams* params) {
  // Because the methods are private, the Searches array
  // cannot be declared at top level.
  static bool (DFA::*Searches[])(SearchParams*) = {
    &DFA::SearchFFF,
    &DFA::SearchFFT,
    &DFA::SearchFTF,
    &DFA::SearchFTT,
    &DFA::SearchTFF,
    &DFA::SearchTFT,
    &DFA::SearchTTF,
    &DFA::SearchTTT,
  };

  bool have_first_byte = params->first_byte >= 0;
  int index = 4 * have_first_byte +
              2 * params->want_earliest_match +
              1 * params->run_forward;
  return (this->*Searches[index])(params);
}


// The discussion of DFA execution above ignored the question of how
// to determine the initial state for the search loop.  There are two
// factors that influence the choice of start state.
//
// The first factor is whether the search is anchored or not.
// The regexp program (Prog*) itself has
// two different entry points: one for anchored searches and one for
// unanchored searches.  (The unanchored version starts with a leading ".*?"
// and then jumps to the anchored one.)
//
// The second factor is where text appears in the larger context, which
// determines which empty-string operators can be matched at the beginning
// of execution.  If text is at the very beginning of context, \A and ^ match.
// Otherwise if text is at the beginning of a line, then ^ matches.
// Otherwise it matters whether the character before text is a word character
// or a non-word character.
//
// The two cases (unanchored vs not) and four cases (empty-string flags)
// combine to make the eight cases recorded in the DFA's begin_text_[2],
// begin_line_[2], after_wordchar_[2], and after_nonwordchar_[2] cached
// StartInfos.  The start state for each is filled in the first time it
// is used for an actual search.

// Examines text, context, and anchored to determine the right start
// state for the DFA search loop.  Fills in params and returns true on success.
// Returns false on failure.
bool DFA::AnalyzeSearch(SearchParams* params) {
  const StringPiece& text = params->text;
  const StringPiece& context = params->context;

  // Sanity check: make sure that text lies within context.
  if (text.begin() < context.begin() || text.end() > context.end()) {
    LOG(DFATAL) << "context does not contain text";
    params->start = DeadState;
    return true;
  }

  // Determine correct search type.
  int start;
  uint32_t flags;
  if (params->run_forward) {
    if (text.begin() == context.begin()) {
      start = kStartBeginText;
      flags = kEmptyBeginText|kEmptyBeginLine;
    } else if (text.begin()[-1] == '\n') {
      start = kStartBeginLine;
      flags = kEmptyBeginLine;
    } else if (Prog::IsWordChar(text.begin()[-1] & 0xFF)) {
      start = kStartAfterWordChar;
      flags = kFlagLastWord;
    } else {
      start = kStartAfterNonWordChar;
      flags = 0;
    }
  } else {
    if (text.end() == context.end()) {
      start = kStartBeginText;
      flags = kEmptyBeginText|kEmptyBeginLine;
    } else if (text.end()[0] == '\n') {
      start = kStartBeginLine;
      flags = kEmptyBeginLine;
    } else if (Prog::IsWordChar(text.end()[0] & 0xFF)) {
      start = kStartAfterWordChar;
      flags = kFlagLastWord;
    } else {
      start = kStartAfterNonWordChar;
      flags = 0;
    }
  }
  if (params->anchored)
    start |= kStartAnchored;
  StartInfo* info = &start_[start];

  // Try once without cache_lock for writing.
  // Try again after resetting the cache
  // (ResetCache will relock cache_lock for writing).
  if (!AnalyzeSearchHelper(params, info, flags)) {
    ResetCache(params->cache_lock);
    if (!AnalyzeSearchHelper(params, info, flags)) {
      LOG(DFATAL) << "Failed to analyze start state.";
      params->failed = true;
      return false;
    }
  }

  params->start = info->start;
  params->first_byte = info->first_byte.load(std::memory_order_acquire);

  return true;
}

// Fills in info if needed.  Returns true on success, false on failure.
bool DFA::AnalyzeSearchHelper(SearchParams* params, StartInfo* info,
                              uint32_t flags) {
  // Quick check.
  int fb = info->first_byte.load(std::memory_order_acquire);
  if (fb != kFbUnknown)
    return true;

  MutexLock l(&mutex_);
  fb = info->first_byte.load(std::memory_order_relaxed);
  if (fb != kFbUnknown)
    return true;

  q0_->clear();
  AddToQueue(q0_,
             params->anchored ? prog_->start() : prog_->start_unanchored(),
             flags);
  info->start = WorkqToCachedState(q0_, NULL, flags);
  if (info->start == NULL)
    return false;

  if (info->start == DeadState) {
    // Synchronize with "quick check" above.
    info->first_byte.store(kFbNone, std::memory_order_release);
    return true;
  }

  if (info->start == FullMatchState) {
    // Synchronize with "quick check" above.
    info->first_byte.store(kFbNone, std::memory_order_release);  // will be ignored
    return true;
  }

  // Even if we have a first_byte, we cannot use it when anchored and,
  // less obviously, we cannot use it when we are going to need flags.
  // This trick works only when there is a single byte that leads to a
  // different state!
  int first_byte = prog_->first_byte();
  if (first_byte == -1 ||
      params->anchored ||
      info->start->flag_ >> kFlagNeedShift != 0)
    first_byte = kFbNone;

  // Synchronize with "quick check" above.
  info->first_byte.store(first_byte, std::memory_order_release);
  return true;
}

// The actual DFA search: calls AnalyzeSearch and then FastSearchLoop.
bool DFA::Search(const StringPiece& text,
                 const StringPiece& context,
                 bool anchored,
                 bool want_earliest_match,
                 bool run_forward,
                 bool* failed,
                 const char** epp,
                 SparseSet* matches) {
  *epp = NULL;
  if (!ok()) {
    *failed = true;
    return false;
  }
  *failed = false;

  RWLocker l(&cache_mutex_);
  SearchParams params(text, context, &l);
  params.anchored = anchored;
  params.want_earliest_match = want_earliest_match;
  params.run_forward = run_forward;
  params.matches = matches;

  if (!AnalyzeSearch(&params)) {
    *failed = true;
    return false;
  }
  if (params.start == DeadState)
    return false;
  if (params.start == FullMatchState) {
    if (run_forward == want_earliest_match)
      *epp = text.begin();
    else
      *epp = text.end();
    return true;
  }
  bool ret = FastSearchLoop(&params);
  if (params.failed) {
    *failed = true;
    return false;
  }
  *epp = params.ep;
  return ret;
}

DFA* Prog::GetDFA(MatchKind kind) {
  // For a forward DFA, half the memory goes to each DFA.
  // However, if it is a "many match" DFA, then there is
  // no counterpart with which the memory must be shared.
  //
  // For a reverse DFA, all the memory goes to the
  // "longest match" DFA, because RE2 never does reverse
  // "first match" searches.
  if (kind == kFirstMatch) {
    std::call_once(dfa_first_once_, [](Prog* prog) {
      prog->dfa_first_ = new DFA(prog, kFirstMatch, prog->dfa_mem_ / 2);
    }, this);
    return dfa_first_;
  } else if (kind == kManyMatch) {
    std::call_once(dfa_first_once_, [](Prog* prog) {
      prog->dfa_first_ = new DFA(prog, kManyMatch, prog->dfa_mem_);
    }, this);
    return dfa_first_;
  } else {
    std::call_once(dfa_longest_once_, [](Prog* prog) {
      if (!prog->reversed_)
        prog->dfa_longest_ = new DFA(prog, kLongestMatch, prog->dfa_mem_ / 2);
      else
        prog->dfa_longest_ = new DFA(prog, kLongestMatch, prog->dfa_mem_);
    }, this);
    return dfa_longest_;
  }
}

void Prog::DeleteDFA(DFA* dfa) {
  delete dfa;
}

// Executes the regexp program to search in text,
// which itself is inside the larger context.  (As a convenience,
// passing a NULL context is equivalent to passing text.)
// Returns true if a match is found, false if not.
// If a match is found, fills in match0->end() to point at the end of the match
// and sets match0->begin() to text.begin(), since the DFA can't track
// where the match actually began.
//
// This is the only external interface (class DFA only exists in this file).
//
bool Prog::SearchDFA(const StringPiece& text, const StringPiece& const_context,
                     Anchor anchor, MatchKind kind, StringPiece* match0,
                     bool* failed, SparseSet* matches) {
  *failed = false;

  StringPiece context = const_context;
  if (context.begin() == NULL)
    context = text;
  bool carat = anchor_start();
  bool dollar = anchor_end();
  if (reversed_) {
    using std::swap;
    swap(carat, dollar);
  }
  if (carat && context.begin() != text.begin())
    return false;
  if (dollar && context.end() != text.end())
    return false;

  // Handle full match by running an anchored longest match
  // and then checking if it covers all of text.
  bool anchored = anchor == kAnchored || anchor_start() || kind == kFullMatch;
  bool endmatch = false;
  if (kind == kManyMatch) {
    // This is split out in order to avoid clobbering kind.
  } else if (kind == kFullMatch || anchor_end()) {
    endmatch = true;
    kind = kLongestMatch;
  }

  // If the caller doesn't care where the match is (just whether one exists),
  // then we can stop at the very first match we find, the so-called
  // "earliest match".
  bool want_earliest_match = false;
  if (kind == kManyMatch) {
    // This is split out in order to avoid clobbering kind.
    if (matches == NULL) {
      want_earliest_match = true;
    }
  } else if (match0 == NULL && !endmatch) {
    want_earliest_match = true;
    kind = kLongestMatch;
  }

  DFA* dfa = GetDFA(kind);
  const char* ep;
  bool matched = dfa->Search(text, context, anchored,
                             want_earliest_match, !reversed_,
                             failed, &ep, matches);
  if (*failed)
    return false;
  if (!matched)
    return false;
  if (endmatch && ep != (reversed_ ? text.begin() : text.end()))
    return false;

  // If caller cares, record the boundary of the match.
  // We only know where it ends, so use the boundary of text
  // as the beginning.
  if (match0) {
    if (reversed_)
      *match0 = StringPiece(ep, static_cast<size_t>(text.end() - ep));
    else
      *match0 =
          StringPiece(text.begin(), static_cast<size_t>(ep - text.begin()));
  }
  return true;
}

// Build out all states in DFA.  Returns number of states.
int DFA::BuildAllStates(const Prog::DFAStateCallback& cb) {
  if (!ok())
    return 0;

  // Pick out start state for unanchored search
  // at beginning of text.
  RWLocker l(&cache_mutex_);
  SearchParams params(StringPiece(), StringPiece(), &l);
  params.anchored = false;
  if (!AnalyzeSearch(&params) ||
      params.start == NULL ||
      params.start == DeadState)
    return 0;

  // Add start state to work queue.
  // Note that any State* that we handle here must point into the cache,
  // so we can simply depend on pointer-as-a-number hashing and equality.
  std::unordered_map<State*, int> m;
  std::deque<State*> q;
  m.emplace(params.start, static_cast<int>(m.size()));
  q.push_back(params.start);

  // Compute the input bytes needed to cover all of the next pointers.
  int nnext = prog_->bytemap_range() + 1;  // + 1 for kByteEndText slot
  std::vector<int> input(nnext);
  for (int c = 0; c < 256; c++) {
    int b = prog_->bytemap()[c];
    while (c < 256-1 && prog_->bytemap()[c+1] == b)
      c++;
    input[b] = c;
  }
  input[prog_->bytemap_range()] = kByteEndText;

  // Scratch space for the output.
  std::vector<int> output(nnext);

  // Flood to expand every state.
  bool oom = false;
  while (!q.empty()) {
    State* s = q.front();
    q.pop_front();
    for (int c : input) {
      State* ns = RunStateOnByteUnlocked(s, c);
      if (ns == NULL) {
        oom = true;
        break;
      }
      if (ns == DeadState) {
        output[ByteMap(c)] = -1;
        continue;
      }
      if (m.find(ns) == m.end()) {
        m.emplace(ns, static_cast<int>(m.size()));
        q.push_back(ns);
      }
      output[ByteMap(c)] = m[ns];
    }
    if (cb)
      cb(oom ? NULL : output.data(),
         s == FullMatchState || s->IsMatch());
    if (oom)
      break;
  }

  return static_cast<int>(m.size());
}

// Build out all states in DFA for kind.  Returns number of states.
int Prog::BuildEntireDFA(MatchKind kind, const DFAStateCallback& cb) {
  return GetDFA(kind)->BuildAllStates(cb);
}

void Prog::TEST_dfa_should_bail_when_slow(bool b) {
  dfa_should_bail_when_slow = b;
}

// Computes min and max for matching string.
// Won't return strings bigger than maxlen.
bool DFA::PossibleMatchRange(std::string* min, std::string* max, int maxlen) {
  if (!ok())
    return false;

  // NOTE: if future users of PossibleMatchRange want more precision when
  // presented with infinitely repeated elements, consider making this a
  // parameter to PossibleMatchRange.
  static int kMaxEltRepetitions = 0;

  // Keep track of the number of times we've visited states previously. We only
  // revisit a given state if it's part of a repeated group, so if the value
  // portion of the map tuple exceeds kMaxEltRepetitions we bail out and set
  // |*max| to |PrefixSuccessor(*max)|.
  //
  // Also note that previously_visited_states[UnseenStatePtr] will, in the STL
  // tradition, implicitly insert a '0' value at first use. We take advantage
  // of that property below.
  std::unordered_map<State*, int> previously_visited_states;

  // Pick out start state for anchored search at beginning of text.
  RWLocker l(&cache_mutex_);
  SearchParams params(StringPiece(), StringPiece(), &l);
  params.anchored = true;
  if (!AnalyzeSearch(&params))
    return false;
  if (params.start == DeadState) {  // No matching strings
    *min = "";
    *max = "";
    return true;
  }
  if (params.start == FullMatchState)  // Every string matches: no max
    return false;

  // The DFA is essentially a big graph rooted at params.start,
  // and paths in the graph correspond to accepted strings.
  // Each node in the graph has potentially 256+1 arrows
  // coming out, one for each byte plus the magic end of
  // text character kByteEndText.

  // To find the smallest possible prefix of an accepted
  // string, we just walk the graph preferring to follow
  // arrows with the lowest bytes possible.  To find the
  // largest possible prefix, we follow the largest bytes
  // possible.

  // The test for whether there is an arrow from s on byte j is
  //    ns = RunStateOnByteUnlocked(s, j);
  //    if (ns == NULL)
  //      return false;
  //    if (ns != DeadState && ns->ninst > 0)
  // The RunStateOnByteUnlocked call asks the DFA to build out the graph.
  // It returns NULL only if the DFA has run out of memory,
  // in which case we can't be sure of anything.
  // The second check sees whether there was graph built
  // and whether it is interesting graph.  Nodes might have
  // ns->ninst == 0 if they exist only to represent the fact
  // that a match was found on the previous byte.

  // Build minimum prefix.
  State* s = params.start;
  min->clear();
  MutexLock lock(&mutex_);
  for (int i = 0; i < maxlen; i++) {
    if (previously_visited_states[s] > kMaxEltRepetitions)
      break;
    previously_visited_states[s]++;

    // Stop if min is a match.
    State* ns = RunStateOnByte(s, kByteEndText);
    if (ns == NULL)  // DFA out of memory
      return false;
    if (ns != DeadState && (ns == FullMatchState || ns->IsMatch()))
      break;

    // Try to extend the string with low bytes.
    bool extended = false;
    for (int j = 0; j < 256; j++) {
      ns = RunStateOnByte(s, j);
      if (ns == NULL)  // DFA out of memory
        return false;
      if (ns == FullMatchState ||
          (ns > SpecialStateMax && ns->ninst_ > 0)) {
        extended = true;
        min->append(1, static_cast<char>(j));
        s = ns;
        break;
      }
    }
    if (!extended)
      break;
  }

  // Build maximum prefix.
  previously_visited_states.clear();
  s = params.start;
  max->clear();
  for (int i = 0; i < maxlen; i++) {
    if (previously_visited_states[s] > kMaxEltRepetitions)
      break;
    previously_visited_states[s] += 1;

    // Try to extend the string with high bytes.
    bool extended = false;
    for (int j = 255; j >= 0; j--) {
      State* ns = RunStateOnByte(s, j);
      if (ns == NULL)
        return false;
      if (ns == FullMatchState ||
          (ns > SpecialStateMax && ns->ninst_ > 0)) {
        extended = true;
        max->append(1, static_cast<char>(j));
        s = ns;
        break;
      }
    }
    if (!extended) {
      // Done, no need for PrefixSuccessor.
      return true;
    }
  }

  // Stopped while still adding to *max - round aaaaaaaaaa... to aaaa...b
  PrefixSuccessor(max);

  // If there are no bytes left, we have no way to say "there is no maximum
  // string".  We could make the interface more complicated and be able to
  // return "there is no maximum but here is a minimum", but that seems like
  // overkill -- the most common no-max case is all possible strings, so not
  // telling the caller that the empty string is the minimum match isn't a
  // great loss.
  if (max->empty())
    return false;

  return true;
}

// PossibleMatchRange for a Prog.
bool Prog::PossibleMatchRange(std::string* min, std::string* max, int maxlen) {
  // Have to use dfa_longest_ to get all strings for full matches.
  // For example, (a|aa) never matches aa in first-match mode.
  return GetDFA(kLongestMatch)->PossibleMatchRange(min, max, maxlen);
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2009 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.



// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2009 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef RE2_FILTERED_RE2_H_
#define RE2_FILTERED_RE2_H_

// The class FilteredRE2 is used as a wrapper to multiple RE2 regexps.
// It provides a prefilter mechanism that helps in cutting down the
// number of regexps that need to be actually searched.
//
// By design, it does not include a string matching engine. This is to
// allow the user of the class to use their favorite string match
// engine. The overall flow is: Add all the regexps using Add, then
// Compile the FilteredRE2. The compile returns strings that need to
// be matched. Note that all returned strings are lowercase. For
// applying regexps to a search text, the caller does the string
// matching using the strings returned. When doing the string match,
// note that the caller has to do that on lower cased version of the
// search text. Then call FirstMatch or AllMatches with a vector of
// indices of strings that were found in the text to get the actual
// regexp matches.

#include <string>
#include <vector>



namespace duckdb_re2 {

class PrefilterTree;

class FilteredRE2 {
 public:
  FilteredRE2();
  explicit FilteredRE2(int min_atom_len);
  ~FilteredRE2();

  // Uses RE2 constructor to create a RE2 object (re). Returns
  // re->error_code(). If error_code is other than NoError, then re is
  // deleted and not added to re2_vec_.
  RE2::ErrorCode Add(const StringPiece& pattern,
                     const RE2::Options& options,
                     int *id);

  // Prepares the regexps added by Add for filtering.  Returns a set
  // of strings that the caller should check for in candidate texts.
  // The returned strings are lowercased. When doing string matching,
  // the search text should be lowercased first to find matching
  // strings from the set of strings returned by Compile.  Call after
  // all Add calls are done.
  void Compile(std::vector<std::string>* strings_to_match);

  // Returns the index of the first matching regexp.
  // Returns -1 on no match. Can be called prior to Compile.
  // Does not do any filtering: simply tries to Match the
  // regexps in a loop.
  int SlowFirstMatch(const StringPiece& text) const;

  // Returns the index of the first matching regexp.
  // Returns -1 on no match. Compile has to be called before
  // calling this.
  int FirstMatch(const StringPiece& text,
                 const std::vector<int>& atoms) const;

  // Returns the indices of all matching regexps, after first clearing
  // matched_regexps.
  bool AllMatches(const StringPiece& text,
                  const std::vector<int>& atoms,
                  std::vector<int>* matching_regexps) const;

  // Returns the indices of all potentially matching regexps after first
  // clearing potential_regexps.
  // A regexp is potentially matching if it passes the filter.
  // If a regexp passes the filter it may still not match.
  // A regexp that does not pass the filter is guaranteed to not match.
  void AllPotentials(const std::vector<int>& atoms,
                     std::vector<int>* potential_regexps) const;

  // The number of regexps added.
  int NumRegexps() const { return static_cast<int>(re2_vec_.size()); }

 private:

  // Get the individual RE2 objects. Useful for testing.
  RE2* GetRE2(int regexpid) const { return re2_vec_[regexpid]; }

  // Print prefilter.
  void PrintPrefilter(int regexpid);

  // Useful for testing and debugging.
  void RegexpsGivenStrings(const std::vector<int>& matched_atoms,
                           std::vector<int>* passed_regexps);

  // All the regexps in the FilteredRE2.
  std::vector<RE2*> re2_vec_;

  // Has the FilteredRE2 been compiled using Compile()
  bool compiled_;

  // An AND-OR tree of string atoms used for filtering regexps.
  PrefilterTree* prefilter_tree_;

  FilteredRE2(const FilteredRE2&) = delete;
  FilteredRE2& operator=(const FilteredRE2&) = delete;
};

}  // namespace duckdb_re2

#endif  // RE2_FILTERED_RE2_H_


// LICENSE_CHANGE_END


#include <stddef.h>
#include <string>





// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2009 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef RE2_PREFILTER_H_
#define RE2_PREFILTER_H_

// Prefilter is the class used to extract string guards from regexps.
// Rather than using Prefilter class directly, use FilteredRE2.
// See filtered_re2.h

#include <set>
#include <string>
#include <vector>




namespace duckdb_re2 {

class RE2;

class Regexp;

class Prefilter {
  // Instead of using Prefilter directly, use FilteredRE2; see filtered_re2.h
 public:
  enum Op {
    ALL = 0,  // Everything matches
    NONE,  // Nothing matches
    ATOM,  // The string atom() must match
    AND,   // All in subs() must match
    OR,   // One of subs() must match
  };

  explicit Prefilter(Op op);
  ~Prefilter();

  Op op() { return op_; }
  const std::string& atom() const { return atom_; }
  void set_unique_id(int id) { unique_id_ = id; }
  int unique_id() const { return unique_id_; }

  // The children of the Prefilter node.
  std::vector<Prefilter*>* subs() {
    DCHECK(op_ == AND || op_ == OR);
    return subs_;
  }

  // Set the children vector. Prefilter takes ownership of subs and
  // subs_ will be deleted when Prefilter is deleted.
  void set_subs(std::vector<Prefilter*>* subs) { subs_ = subs; }

  // Given a RE2, return a Prefilter. The caller takes ownership of
  // the Prefilter and should deallocate it. Returns NULL if Prefilter
  // cannot be formed.
  static Prefilter* FromRE2(const RE2* re2);

  // Returns a readable debug string of the prefilter.
  std::string DebugString() const;

 private:
  class Info;

  // Combines two prefilters together to create an AND. The passed
  // Prefilters will be part of the returned Prefilter or deleted.
  static Prefilter* And(Prefilter* a, Prefilter* b);

  // Combines two prefilters together to create an OR. The passed
  // Prefilters will be part of the returned Prefilter or deleted.
  static Prefilter* Or(Prefilter* a, Prefilter* b);

  // Generalized And/Or
  static Prefilter* AndOr(Op op, Prefilter* a, Prefilter* b);

  static Prefilter* FromRegexp(Regexp* a);

  static Prefilter* FromString(const std::string& str);

  static Prefilter* OrStrings(std::set<std::string>* ss);

  static Info* BuildInfo(Regexp* re);

  Prefilter* Simplify();

  // Kind of Prefilter.
  Op op_;

  // Sub-matches for AND or OR Prefilter.
  std::vector<Prefilter*>* subs_;

  // Actual string to match in leaf node.
  std::string atom_;

  // If different prefilters have the same string atom, or if they are
  // structurally the same (e.g., OR of same atom strings) they are
  // considered the same unique nodes. This is the id for each unique
  // node. This field is populated with a unique id for every node,
  // and -1 for duplicate nodes.
  int unique_id_;

  Prefilter(const Prefilter&) = delete;
  Prefilter& operator=(const Prefilter&) = delete;
};

}  // namespace duckdb_re2

#endif  // RE2_PREFILTER_H_


// LICENSE_CHANGE_END



// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2009 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef RE2_PREFILTER_TREE_H_
#define RE2_PREFILTER_TREE_H_

// The PrefilterTree class is used to form an AND-OR tree of strings
// that would trigger each regexp. The 'prefilter' of each regexp is
// added tp PrefilterTree, and then PrefilterTree is used to find all
// the unique strings across the prefilters. During search, by using
// matches from a string matching engine, PrefilterTree deduces the
// set of regexps that are to be triggered. The 'string matching
// engine' itself is outside of this class, and the caller can use any
// favorite engine. PrefilterTree provides a set of strings (called
// atoms) that the user of this class should use to do the string
// matching.

#include <map>
#include <string>
#include <vector>





namespace duckdb_re2 {

class PrefilterTree {
 public:
  PrefilterTree();
  explicit PrefilterTree(int min_atom_len);
  ~PrefilterTree();

  // Adds the prefilter for the next regexp. Note that we assume that
  // Add called sequentially for all regexps. All Add calls
  // must precede Compile.
  void Add(Prefilter* prefilter);

  // The Compile returns a vector of string in atom_vec.
  // Call this after all the prefilters are added through Add.
  // No calls to Add after Compile are allowed.
  // The caller should use the returned set of strings to do string matching.
  // Each time a string matches, the corresponding index then has to be
  // and passed to RegexpsGivenStrings below.
  void Compile(std::vector<std::string>* atom_vec);

  // Given the indices of the atoms that matched, returns the indexes
  // of regexps that should be searched.  The matched_atoms should
  // contain all the ids of string atoms that were found to match the
  // content. The caller can use any string match engine to perform
  // this function. This function is thread safe.
  void RegexpsGivenStrings(const std::vector<int>& matched_atoms,
                           std::vector<int>* regexps) const;

  // Print debug prefilter. Also prints unique ids associated with
  // nodes of the prefilter of the regexp.
  void PrintPrefilter(int regexpid);

 private:
  typedef SparseArray<int> IntMap;
  typedef std::map<int, int> StdIntMap;
  typedef std::map<std::string, Prefilter*> NodeMap;

  // Each unique node has a corresponding Entry that helps in
  // passing the matching trigger information along the tree.
  struct Entry {
   public:
    // How many children should match before this node triggers the
    // parent. For an atom and an OR node, this is 1 and for an AND
    // node, it is the number of unique children.
    int propagate_up_at_count;

    // When this node is ready to trigger the parent, what are the indices
    // of the parent nodes to trigger. The reason there may be more than
    // one is because of sharing. For example (abc | def) and (xyz | def)
    // are two different nodes, but they share the atom 'def'. So when
    // 'def' matches, it triggers two parents, corresponding to the two
    // different OR nodes.
    StdIntMap* parents;

    // When this node is ready to trigger the parent, what are the
    // regexps that are triggered.
    std::vector<int> regexps;
  };

  // Returns true if the prefilter node should be kept.
  bool KeepNode(Prefilter* node) const;

  // This function assigns unique ids to various parts of the
  // prefilter, by looking at if these nodes are already in the
  // PrefilterTree.
  void AssignUniqueIds(NodeMap* nodes, std::vector<std::string>* atom_vec);

  // Given the matching atoms, find the regexps to be triggered.
  void PropagateMatch(const std::vector<int>& atom_ids,
                      IntMap* regexps) const;

  // Returns the prefilter node that has the same NodeString as this
  // node. For the canonical node, returns node.
  Prefilter* CanonicalNode(NodeMap* nodes, Prefilter* node);

  // A string that uniquely identifies the node. Assumes that the
  // children of node has already been assigned unique ids.
  std::string NodeString(Prefilter* node) const;

  // Recursively constructs a readable prefilter string.
  std::string DebugNodeString(Prefilter* node) const;

  // Used for debugging.
  void PrintDebugInfo(NodeMap* nodes);

  // These are all the nodes formed by Compile. Essentially, there is
  // one node for each unique atom and each unique AND/OR node.
  std::vector<Entry> entries_;

  // indices of regexps that always pass through the filter (since we
  // found no required literals in these regexps).
  std::vector<int> unfiltered_;

  // vector of Prefilter for all regexps.
  std::vector<Prefilter*> prefilter_vec_;

  // Atom index in returned strings to entry id mapping.
  std::vector<int> atom_index_to_id_;

  // Has the prefilter tree been compiled.
  bool compiled_;

  // Strings less than this length are not stored as atoms.
  const int min_atom_len_;

  PrefilterTree(const PrefilterTree&) = delete;
  PrefilterTree& operator=(const PrefilterTree&) = delete;
};

}  // namespace

#endif  // RE2_PREFILTER_TREE_H_


// LICENSE_CHANGE_END


namespace duckdb_re2 {

FilteredRE2::FilteredRE2()
    : compiled_(false),
      prefilter_tree_(new PrefilterTree()) {
}

FilteredRE2::FilteredRE2(int min_atom_len)
    : compiled_(false),
      prefilter_tree_(new PrefilterTree(min_atom_len)) {
}

FilteredRE2::~FilteredRE2() {
  for (size_t i = 0; i < re2_vec_.size(); i++)
    delete re2_vec_[i];
  delete prefilter_tree_;
}

RE2::ErrorCode FilteredRE2::Add(const StringPiece& pattern,
                                const RE2::Options& options, int* id) {
  RE2* re = new RE2(pattern, options);
  RE2::ErrorCode code = re->error_code();

  if (!re->ok()) {
    if (options.log_errors()) {
      LOG(ERROR) << "Couldn't compile regular expression, skipping: "
                 << re << " due to error " << re->error();
    }
    delete re;
  } else {
    *id = static_cast<int>(re2_vec_.size());
    re2_vec_.push_back(re);
  }

  return code;
}

void FilteredRE2::Compile(std::vector<std::string>* atoms) {
  if (compiled_) {
    LOG(ERROR) << "Compile called already.";
    return;
  }

  if (re2_vec_.empty()) {
    LOG(ERROR) << "Compile called before Add.";
    return;
  }

  for (size_t i = 0; i < re2_vec_.size(); i++) {
    Prefilter* prefilter = Prefilter::FromRE2(re2_vec_[i]);
    prefilter_tree_->Add(prefilter);
  }
  atoms->clear();
  prefilter_tree_->Compile(atoms);
  compiled_ = true;
}

int FilteredRE2::SlowFirstMatch(const StringPiece& text) const {
  for (size_t i = 0; i < re2_vec_.size(); i++)
    if (RE2::PartialMatch(text, *re2_vec_[i]))
      return static_cast<int>(i);
  return -1;
}

int FilteredRE2::FirstMatch(const StringPiece& text,
                            const std::vector<int>& atoms) const {
  if (!compiled_) {
    LOG(DFATAL) << "FirstMatch called before Compile.";
    return -1;
  }
  std::vector<int> regexps;
  prefilter_tree_->RegexpsGivenStrings(atoms, &regexps);
  for (size_t i = 0; i < regexps.size(); i++)
    if (RE2::PartialMatch(text, *re2_vec_[regexps[i]]))
      return regexps[i];
  return -1;
}

bool FilteredRE2::AllMatches(
    const StringPiece& text,
    const std::vector<int>& atoms,
    std::vector<int>* matching_regexps) const {
  matching_regexps->clear();
  std::vector<int> regexps;
  prefilter_tree_->RegexpsGivenStrings(atoms, &regexps);
  for (size_t i = 0; i < regexps.size(); i++)
    if (RE2::PartialMatch(text, *re2_vec_[regexps[i]]))
      matching_regexps->push_back(regexps[i]);
  return !matching_regexps->empty();
}

void FilteredRE2::AllPotentials(
    const std::vector<int>& atoms,
    std::vector<int>* potential_regexps) const {
  prefilter_tree_->RegexpsGivenStrings(atoms, potential_regexps);
}

void FilteredRE2::RegexpsGivenStrings(const std::vector<int>& matched_atoms,
                                      std::vector<int>* passed_regexps) {
  prefilter_tree_->RegexpsGivenStrings(matched_atoms, passed_regexps);
}

void FilteredRE2::PrintPrefilter(int regexpid) {
  prefilter_tree_->PrintPrefilter(regexpid);
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2008 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Determine whether this library should match PCRE exactly
// for a particular Regexp.  (If so, the testing framework can
// check that it does.)
//
// This library matches PCRE except in these cases:
//   * the regexp contains a repetition of an empty string,
//     like (a*)* or (a*)+.  In this case, PCRE will treat
//     the repetition sequence as ending with an empty string,
//     while this library does not.
//   * Perl and PCRE differ on whether \v matches \n.
//     For historical reasons, this library implements the Perl behavior.
//   * Perl and PCRE allow $ in one-line mode to match either the very
//     end of the text or just before a \n at the end of the text.
//     This library requires it to match only the end of the text.
//   * Similarly, Perl and PCRE do not allow ^ in multi-line mode to
//     match the end of the text if the last character is a \n.
//     This library does allow it.
//
// Regexp::MimicsPCRE checks for any of these conditions.






namespace duckdb_re2 {

// Returns whether re might match an empty string.
static bool CanBeEmptyString(Regexp *re);

// Walker class to compute whether library handles a regexp
// exactly as PCRE would.  See comment at top for conditions.

class PCREWalker : public Regexp::Walker<bool> {
 public:
  PCREWalker() {}
  bool PostVisit(Regexp* re, bool parent_arg, bool pre_arg, bool* child_args,
                 int nchild_args);

  bool ShortVisit(Regexp* re, bool a) {
    // Should never be called: we use Walk not WalkExponential.
    LOG(DFATAL) << "EmptyStringWalker::ShortVisit called";
    return a;
  }
};

// Called after visiting each of re's children and accumulating
// the return values in child_args.  So child_args contains whether
// this library mimics PCRE for those subexpressions.
bool PCREWalker::PostVisit(Regexp* re, bool parent_arg, bool pre_arg,
                           bool* child_args, int nchild_args) {
  // If children failed, so do we.
  for (int i = 0; i < nchild_args; i++)
    if (!child_args[i])
      return false;

  // Otherwise look for other reasons to fail.
  switch (re->op()) {
    // Look for repeated empty string.
    case kRegexpStar:
    case kRegexpPlus:
    case kRegexpQuest:
      if (CanBeEmptyString(re->sub()[0]))
        return false;
      break;
    case kRegexpRepeat:
      if (re->max() == -1 && CanBeEmptyString(re->sub()[0]))
        return false;
      break;

    // Look for \v
    case kRegexpLiteral:
      if (re->rune() == '\v')
        return false;
      break;

    // Look for $ in single-line mode.
    case kRegexpEndText:
    case kRegexpEmptyMatch:
      if (re->parse_flags() & Regexp::WasDollar)
        return false;
      break;

    // Look for ^ in multi-line mode.
    case kRegexpBeginLine:
      // No condition: in single-line mode ^ becomes kRegexpBeginText.
      return false;

    default:
      break;
  }

  // Not proven guilty.
  return true;
}

// Returns whether this regexp's behavior will mimic PCRE's exactly.
bool Regexp::MimicsPCRE() {
  PCREWalker w;
  return w.Walk(this, true);
}


// Walker class to compute whether a Regexp can match an empty string.
// It is okay to overestimate.  For example, \b\B cannot match an empty
// string, because \b and \B are mutually exclusive, but this isn't
// that smart and will say it can.  Spurious empty strings
// will reduce the number of regexps we sanity check against PCRE,
// but they won't break anything.

class EmptyStringWalker : public Regexp::Walker<bool> {
 public:
  EmptyStringWalker() { }
  bool PostVisit(Regexp* re, bool parent_arg, bool pre_arg,
                 bool* child_args, int nchild_args);

  bool ShortVisit(Regexp* re, bool a) {
    // Should never be called: we use Walk not WalkExponential.
    LOG(DFATAL) << "EmptyStringWalker::ShortVisit called";
    return a;
  }

 private:
  EmptyStringWalker(const EmptyStringWalker&) = delete;
  EmptyStringWalker& operator=(const EmptyStringWalker&) = delete;
};

// Called after visiting re's children.  child_args contains the return
// value from each of the children's PostVisits (i.e., whether each child
// can match an empty string).  Returns whether this clause can match an
// empty string.
bool EmptyStringWalker::PostVisit(Regexp* re, bool parent_arg, bool pre_arg,
                                  bool* child_args, int nchild_args) {
  switch (re->op()) {
    case kRegexpNoMatch:               // never empty
    case kRegexpLiteral:
    case kRegexpAnyChar:
    case kRegexpAnyByte:
    case kRegexpCharClass:
    case kRegexpLiteralString:
      return false;

    case kRegexpEmptyMatch:            // always empty
    case kRegexpBeginLine:             // always empty, when they match
    case kRegexpEndLine:
    case kRegexpNoWordBoundary:
    case kRegexpWordBoundary:
    case kRegexpBeginText:
    case kRegexpEndText:
    case kRegexpStar:                  // can always be empty
    case kRegexpQuest:
    case kRegexpHaveMatch:
      return true;

    case kRegexpConcat:                // can be empty if all children can
      for (int i = 0; i < nchild_args; i++)
        if (!child_args[i])
          return false;
      return true;

    case kRegexpAlternate:             // can be empty if any child can
      for (int i = 0; i < nchild_args; i++)
        if (child_args[i])
          return true;
      return false;

    case kRegexpPlus:                  // can be empty if the child can
    case kRegexpCapture:
      return child_args[0];

    case kRegexpRepeat:                // can be empty if child can or is x{0}
      return child_args[0] || re->min() == 0;
  }
  return false;
}

// Returns whether re can match an empty string.
static bool CanBeEmptyString(Regexp* re) {
  EmptyStringWalker w;
  return w.Walk(re, true);
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2006-2007 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Tested by search_test.cc.
//
// Prog::SearchNFA, an NFA search.
// This is an actual NFA like the theorists talk about,
// not the pseudo-NFA found in backtracking regexp implementations.
//
// IMPLEMENTATION
//
// This algorithm is a variant of one that appeared in Rob Pike's sam editor,
// which is a variant of the one described in Thompson's 1968 CACM paper.
// See http://swtch.com/~rsc/regexp/ for various history.  The main feature
// over the DFA implementation is that it tracks submatch boundaries.
//
// When the choice of submatch boundaries is ambiguous, this particular
// implementation makes the same choices that traditional backtracking
// implementations (in particular, Perl and PCRE) do.
// Note that unlike in Perl and PCRE, this algorithm *cannot* take exponential
// time in the length of the input.
//
// Like Thompson's original machine and like the DFA implementation, this
// implementation notices a match only once it is one byte past it.

#include <stdio.h>
#include <string.h>
#include <algorithm>
#include <string>
#include <utility>
#include <vector>









namespace duckdb_re2 {

class NFA {
 public:
  NFA(Prog* prog);
  ~NFA();

  // Searches for a matching string.
  //   * If anchored is true, only considers matches starting at offset.
  //     Otherwise finds lefmost match at or after offset.
  //   * If longest is true, returns the longest match starting
  //     at the chosen start point.  Otherwise returns the so-called
  //     left-biased match, the one traditional backtracking engines
  //     (like Perl and PCRE) find.
  // Records submatch boundaries in submatch[1..nsubmatch-1].
  // Submatch[0] is the entire match.  When there is a choice in
  // which text matches each subexpression, the submatch boundaries
  // are chosen to match what a backtracking implementation would choose.
  bool Search(const StringPiece& text, const StringPiece& context,
              bool anchored, bool longest,
              StringPiece* submatch, int nsubmatch);

 private:
  struct Thread {
    union {
      int ref;
      Thread* next;  // when on free list
    };
    const char** capture;
  };

  // State for explicit stack in AddToThreadq.
  struct AddState {
    int id;     // Inst to process
    Thread* t;  // if not null, set t0 = t before processing id
  };

  // Threadq is a list of threads.  The list is sorted by the order
  // in which Perl would explore that particular state -- the earlier
  // choices appear earlier in the list.
  typedef SparseArray<Thread*> Threadq;

  inline Thread* AllocThread();
  inline Thread* Incref(Thread* t);
  inline void Decref(Thread* t);

  // Follows all empty arrows from id0 and enqueues all the states reached.
  // Enqueues only the ByteRange instructions that match byte c.
  // context is used (with p) for evaluating empty-width specials.
  // p is the current input position, and t0 is the current thread.
  void AddToThreadq(Threadq* q, int id0, int c, const StringPiece& context,
                    const char* p, Thread* t0);

  // Run runq on byte c, appending new states to nextq.
  // Updates matched_ and match_ as new, better matches are found.
  // context is used (with p) for evaluating empty-width specials.
  // p is the position of byte c in the input string for AddToThreadq;
  // p-1 will be used when processing Match instructions.
  // Frees all the threads on runq.
  // If there is a shortcut to the end, returns that shortcut.
  int Step(Threadq* runq, Threadq* nextq, int c, const StringPiece& context,
           const char* p);

  // Returns text version of capture information, for debugging.
  std::string FormatCapture(const char** capture);

  inline void CopyCapture(const char** dst, const char** src);

  Prog* prog_;                // underlying program
  int start_;                 // start instruction in program
  int ncapture_;              // number of submatches to track
  bool longest_;              // whether searching for longest match
  bool endmatch_;             // whether match must end at text.end()
  const char* btext_;         // beginning of text being matched (for FormatSubmatch)
  const char* etext_;         // end of text being matched (for endmatch_)
  Threadq q0_, q1_;           // pre-allocated for Search.
  PODArray<AddState> stack_;  // pre-allocated for AddToThreadq
  Thread* free_threads_;      // free list
  const char** match_;        // best match so far
  bool matched_;              // any match so far?

  NFA(const NFA&) = delete;
  NFA& operator=(const NFA&) = delete;
};

NFA::NFA(Prog* prog) {
  prog_ = prog;
  start_ = prog_->start();
  ncapture_ = 0;
  longest_ = false;
  endmatch_ = false;
  btext_ = NULL;
  etext_ = NULL;
  q0_.resize(prog_->size());
  q1_.resize(prog_->size());
  // See NFA::AddToThreadq() for why this is so.
  int nstack = 2*prog_->inst_count(kInstCapture) +
               prog_->inst_count(kInstEmptyWidth) +
               prog_->inst_count(kInstNop) + 1;  // + 1 for start inst
  stack_ = PODArray<AddState>(nstack);
  free_threads_ = NULL;
  match_ = NULL;
  matched_ = false;
}

NFA::~NFA() {
  delete[] match_;
  Thread* next;
  for (Thread* t = free_threads_; t; t = next) {
    next = t->next;
    delete[] t->capture;
    delete t;
  }
}

NFA::Thread* NFA::AllocThread() {
  Thread* t = free_threads_;
  if (t == NULL) {
    t = new Thread;
    t->ref = 1;
    t->capture = new const char*[ncapture_];
    return t;
  }
  free_threads_ = t->next;
  t->ref = 1;
  return t;
}

NFA::Thread* NFA::Incref(Thread* t) {
  DCHECK(t != NULL);
  t->ref++;
  return t;
}

void NFA::Decref(Thread* t) {
  if (t == NULL)
    return;
  t->ref--;
  if (t->ref > 0)
    return;
  DCHECK_EQ(t->ref, 0);
  t->next = free_threads_;
  free_threads_ = t;
}

void NFA::CopyCapture(const char** dst, const char** src) {
  for (int i = 0; i < ncapture_; i+=2) {
    dst[i] = src[i];
    dst[i+1] = src[i+1];
  }
}

// Follows all empty arrows from id0 and enqueues all the states reached.
// Enqueues only the ByteRange instructions that match byte c.
// context is used (with p) for evaluating empty-width specials.
// p is the current input position, and t0 is the current thread.
void NFA::AddToThreadq(Threadq* q, int id0, int c, const StringPiece& context,
                       const char* p, Thread* t0) {
  if (id0 == 0)
    return;

  // Use stack_ to hold our stack of instructions yet to process.
  // It was preallocated as follows:
  //   two entries per Capture;
  //   one entry per EmptyWidth; and
  //   one entry per Nop.
  // This reflects the maximum number of stack pushes that each can
  // perform. (Each instruction can be processed at most once.)
  AddState* stk = stack_.data();
  int nstk = 0;

  stk[nstk++] = {id0, NULL};
  while (nstk > 0) {
    DCHECK_LE(nstk, stack_.size());
    AddState a = stk[--nstk];

  Loop:
    if (a.t != NULL) {
      // t0 was a thread that we allocated and copied in order to
      // record the capture, so we must now decref it.
      Decref(t0);
      t0 = a.t;
    }

    int id = a.id;
    if (id == 0)
      continue;
    if (q->has_index(id)) {
      continue;
    }

    // Create entry in q no matter what.  We might fill it in below,
    // or we might not.  Even if not, it is necessary to have it,
    // so that we don't revisit id0 during the recursion.
    q->set_new(id, NULL);
    Thread** tp = &q->get_existing(id);
    int j;
    Thread* t;
    Prog::Inst* ip = prog_->inst(id);
    switch (ip->opcode()) {
    default:
      LOG(DFATAL) << "unhandled " << ip->opcode() << " in AddToThreadq";
      break;

    case kInstFail:
      break;

    case kInstAltMatch:
      // Save state; will pick up at next byte.
      t = Incref(t0);
      *tp = t;

      DCHECK(!ip->last());
      a = {id+1, NULL};
      goto Loop;

    case kInstNop:
      if (!ip->last())
        stk[nstk++] = {id+1, NULL};

      // Continue on.
      a = {ip->out(), NULL};
      goto Loop;

    case kInstCapture:
      if (!ip->last())
        stk[nstk++] = {id+1, NULL};

      if ((j=ip->cap()) < ncapture_) {
        // Push a dummy whose only job is to restore t0
        // once we finish exploring this possibility.
        stk[nstk++] = {0, t0};

        // Record capture.
        t = AllocThread();
        CopyCapture(t->capture, t0->capture);
        t->capture[j] = p;
        t0 = t;
      }
      a = {ip->out(), NULL};
      goto Loop;

    case kInstByteRange:
      if (!ip->Matches(c))
        goto Next;

      // Save state; will pick up at next byte.
      t = Incref(t0);
      *tp = t;
      if (ip->hint() == 0)
        break;
      a = {id+ip->hint(), NULL};
      goto Loop;

    case kInstMatch:
      // Save state; will pick up at next byte.
      t = Incref(t0);
      *tp = t;
    Next:
      if (ip->last())
        break;
      a = {id+1, NULL};
      goto Loop;

    case kInstEmptyWidth:
      if (!ip->last())
        stk[nstk++] = {id+1, NULL};

      // Continue on if we have all the right flag bits.
      if (ip->empty() & ~Prog::EmptyFlags(context, p))
        break;
      a = {ip->out(), NULL};
      goto Loop;
    }
  }
}

// Run runq on byte c, appending new states to nextq.
// Updates matched_ and match_ as new, better matches are found.
// context is used (with p) for evaluating empty-width specials.
// p is the position of byte c in the input string for AddToThreadq;
// p-1 will be used when processing Match instructions.
// Frees all the threads on runq.
// If there is a shortcut to the end, returns that shortcut.
int NFA::Step(Threadq* runq, Threadq* nextq, int c, const StringPiece& context,
              const char* p) {
  nextq->clear();

  for (Threadq::iterator i = runq->begin(); i != runq->end(); ++i) {
    Thread* t = i->value();
    if (t == NULL)
      continue;

    if (longest_) {
      // Can skip any threads started after our current best match.
      if (matched_ && match_[0] < t->capture[0]) {
        Decref(t);
        continue;
      }
    }

    int id = i->index();
    Prog::Inst* ip = prog_->inst(id);

    switch (ip->opcode()) {
      default:
        // Should only see the values handled below.
        LOG(DFATAL) << "Unhandled " << ip->opcode() << " in step";
        break;

      case kInstByteRange:
        AddToThreadq(nextq, ip->out(), c, context, p, t);
        break;

      case kInstAltMatch:
        if (i != runq->begin())
          break;
        // The match is ours if we want it.
        if (ip->greedy(prog_) || longest_) {
          CopyCapture(match_, t->capture);
          matched_ = true;

          Decref(t);
          for (++i; i != runq->end(); ++i)
            Decref(i->value());
          runq->clear();
          if (ip->greedy(prog_))
            return ip->out1();
          return ip->out();
        }
        break;

      case kInstMatch: {
        // Avoid invoking undefined behavior when p happens
        // to be null - and p-1 would be meaningless anyway.
        if (p == NULL)
          break;

        if (endmatch_ && p-1 != etext_)
          break;

        if (longest_) {
          // Leftmost-longest mode: save this match only if
          // it is either farther to the left or at the same
          // point but longer than an existing match.
          if (!matched_ || t->capture[0] < match_[0] ||
              (t->capture[0] == match_[0] && p-1 > match_[1])) {
            CopyCapture(match_, t->capture);
            match_[1] = p-1;
            matched_ = true;
          }
        } else {
          // Leftmost-biased mode: this match is by definition
          // better than what we've already found (see next line).
          CopyCapture(match_, t->capture);
          match_[1] = p-1;
          matched_ = true;

          // Cut off the threads that can only find matches
          // worse than the one we just found: don't run the
          // rest of the current Threadq.
          Decref(t);
          for (++i; i != runq->end(); ++i)
            Decref(i->value());
          runq->clear();
          return 0;
        }
        break;
      }
    }
    Decref(t);
  }
  runq->clear();
  return 0;
}

std::string NFA::FormatCapture(const char** capture) {
  std::string s;
  for (int i = 0; i < ncapture_; i+=2) {
    if (capture[i] == NULL)
      StringAppendF(&s, "(?,?)");
    else if (capture[i+1] == NULL)
      StringAppendF(&s, "(%d,?)", (int)(capture[i] - btext_));
    else
      StringAppendF(&s, "(%d,%d)",
                    (int)(capture[i] - btext_),
                    (int)(capture[i+1] - btext_));
  }
  return s;
}

bool NFA::Search(const StringPiece& text, const StringPiece& const_context,
            bool anchored, bool longest,
            StringPiece* submatch, int nsubmatch) {
  if (start_ == 0)
    return false;

  StringPiece context = const_context;
  if (context.begin() == NULL)
    context = text;

  // Sanity check: make sure that text lies within context.
  if (text.begin() < context.begin() || text.end() > context.end()) {
    LOG(DFATAL) << "context does not contain text";
    return false;
  }

  if (prog_->anchor_start() && context.begin() != text.begin())
    return false;
  if (prog_->anchor_end() && context.end() != text.end())
    return false;
  anchored |= prog_->anchor_start();
  if (prog_->anchor_end()) {
    longest = true;
    endmatch_ = true;
    etext_ = text.end();
  }

  if (nsubmatch < 0) {
    LOG(DFATAL) << "Bad args: nsubmatch=" << nsubmatch;
    return false;
  }

  // Save search parameters.
  ncapture_ = 2*nsubmatch;
  longest_ = longest;

  if (nsubmatch == 0) {
    // We need to maintain match[0], both to distinguish the
    // longest match (if longest is true) and also to tell
    // whether we've seen any matches at all.
    ncapture_ = 2;
  }

  match_ = new const char*[ncapture_];
  matched_ = false;

  // For debugging prints.
  btext_ = context.begin();

  // Set up search.
  Threadq* runq = &q0_;
  Threadq* nextq = &q1_;
  runq->clear();
  nextq->clear();
  memset(&match_[0], 0, ncapture_*sizeof match_[0]);

  // Loop over the text, stepping the machine.
  for (const char* p = text.begin();; p++) {
    // This is a no-op the first time around the loop because runq is empty.
    int id = Step(runq, nextq, p < text.end() ? p[0] & 0xFF : -1, context, p);
    DCHECK_EQ(runq->size(), 0);
    using std::swap;
    swap(nextq, runq);
    nextq->clear();
    if (id != 0) {
      // We're done: full match ahead.
      p = text.end();
      for (;;) {
        Prog::Inst* ip = prog_->inst(id);
        switch (ip->opcode()) {
          default:
            LOG(DFATAL) << "Unexpected opcode in short circuit: " << ip->opcode();
            break;

          case kInstCapture:
            if (ip->cap() < ncapture_)
              match_[ip->cap()] = p;
            id = ip->out();
            continue;

          case kInstNop:
            id = ip->out();
            continue;

          case kInstMatch:
            match_[1] = p;
            matched_ = true;
            break;
        }
        break;
      }
      break;
    }

    if (p > text.end())
      break;

    // Start a new thread if there have not been any matches.
    // (No point in starting a new thread if there have been
    // matches, since it would be to the right of the match
    // we already found.)
    if (!matched_ && (!anchored || p == text.begin())) {
      // If there's a required first byte for an unanchored search
      // and we're not in the middle of any possible matches,
      // use memchr to search for the byte quickly.
      int fb = prog_->first_byte();
      if (!anchored && runq->size() == 0 &&
          fb >= 0 && p < text.end() && (p[0] & 0xFF) != fb) {
        p = reinterpret_cast<const char*>(memchr(p, fb, text.end() - p));
        if (p == NULL) {
          p = text.end();
        }
      }

      Thread* t = AllocThread();
      CopyCapture(t->capture, match_);
      t->capture[0] = p;
      AddToThreadq(runq, start_, p < text.end() ? p[0] & 0xFF : -1, context, p,
                   t);
      Decref(t);
    }

    // If all the threads have died, stop early.
    if (runq->size() == 0) {
      break;
    }
  }

  for (Threadq::iterator i = runq->begin(); i != runq->end(); ++i)
    Decref(i->value());

  if (matched_) {
    for (int i = 0; i < nsubmatch; i++)
      submatch[i] =
          StringPiece(match_[2 * i],
                      static_cast<size_t>(match_[2 * i + 1] - match_[2 * i]));
    return true;
  }
  return false;
}

// Computes whether all successful matches have a common first byte,
// and if so, returns that byte.  If not, returns -1.
int Prog::ComputeFirstByte() {
  int b = -1;
  SparseSet q(size());
  q.insert(start());
  for (SparseSet::iterator it = q.begin(); it != q.end(); ++it) {
    int id = *it;
    Prog::Inst* ip = inst(id);
    switch (ip->opcode()) {
      default:
        LOG(DFATAL) << "unhandled " << ip->opcode() << " in ComputeFirstByte";
        break;

      case kInstMatch:
        // The empty string matches: no first byte.
        return -1;

      case kInstByteRange:
        if (!ip->last())
          q.insert(id+1);

        // Must match only a single byte
        if (ip->lo() != ip->hi())
          return -1;
        if (ip->foldcase() && 'a' <= ip->lo() && ip->lo() <= 'z')
          return -1;
        // If we haven't seen any bytes yet, record it;
        // otherwise must match the one we saw before.
        if (b == -1)
          b = ip->lo();
        else if (b != ip->lo())
          return -1;
        break;

      case kInstNop:
      case kInstCapture:
      case kInstEmptyWidth:
        if (!ip->last())
          q.insert(id+1);

        // Continue on.
        // Ignore ip->empty() flags for kInstEmptyWidth
        // in order to be as conservative as possible
        // (assume all possible empty-width flags are true).
        if (ip->out())
          q.insert(ip->out());
        break;

      case kInstAltMatch:
        DCHECK(!ip->last());
        q.insert(id+1);
        break;

      case kInstFail:
        break;
    }
  }
  return b;
}

bool
Prog::SearchNFA(const StringPiece& text, const StringPiece& context,
                Anchor anchor, MatchKind kind,
                StringPiece* match, int nmatch) {
  NFA nfa(this);
  StringPiece sp;
  if (kind == kFullMatch) {
    anchor = kAnchored;
    if (nmatch == 0) {
      match = &sp;
      nmatch = 1;
    }
  }
  if (!nfa.Search(text, context, anchor == kAnchored, kind != kFirstMatch, match, nmatch))
    return false;
  if (kind == kFullMatch && match[0].end() != text.end())
    return false;
  return true;
}

// For each instruction i in the program reachable from the start, compute the
// number of instructions reachable from i by following only empty transitions
// and record that count as fanout[i].
//
// fanout holds the results and is also the work queue for the outer iteration.
// reachable holds the reached nodes for the inner iteration.
void Prog::Fanout(SparseArray<int>* fanout) {
  DCHECK_EQ(fanout->max_size(), size());
  SparseSet reachable(size());
  fanout->clear();
  fanout->set_new(start(), 0);
  for (SparseArray<int>::iterator i = fanout->begin(); i != fanout->end(); ++i) {
    int* count = &i->value();
    reachable.clear();
    reachable.insert(i->index());
    for (SparseSet::iterator j = reachable.begin(); j != reachable.end(); ++j) {
      int id = *j;
      Prog::Inst* ip = inst(id);
      switch (ip->opcode()) {
        default:
          LOG(DFATAL) << "unhandled " << ip->opcode() << " in Prog::Fanout()";
          break;

        case kInstByteRange:
          if (!ip->last())
            reachable.insert(id+1);

          (*count)++;
          if (!fanout->has_index(ip->out())) {
            fanout->set_new(ip->out(), 0);
          }
          break;

        case kInstAltMatch:
          DCHECK(!ip->last());
          reachable.insert(id+1);
          break;

        case kInstCapture:
        case kInstEmptyWidth:
        case kInstNop:
          if (!ip->last())
            reachable.insert(id+1);

          reachable.insert(ip->out());
          break;

        case kInstMatch:
          if (!ip->last())
            reachable.insert(id+1);
          break;

        case kInstFail:
          break;
      }
    }
  }
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2008 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Tested by search_test.cc.
//
// Prog::SearchOnePass is an efficient implementation of
// regular expression search with submatch tracking for
// what I call "one-pass regular expressions".  (An alternate
// name might be "backtracking-free regular expressions".)
//
// One-pass regular expressions have the property that
// at each input byte during an anchored match, there may be
// multiple alternatives but only one can proceed for any
// given input byte.
//
// For example, the regexp /x*yx*/ is one-pass: you read
// x's until a y, then you read the y, then you keep reading x's.
// At no point do you have to guess what to do or back up
// and try a different guess.
//
// On the other hand, /x*x/ is not one-pass: when you're
// looking at an input "x", it's not clear whether you should
// use it to extend the x* or as the final x.
//
// More examples: /([^ ]*) (.*)/ is one-pass; /(.*) (.*)/ is not.
// /(\d+)-(\d+)/ is one-pass; /(\d+).(\d+)/ is not.
//
// A simple intuition for identifying one-pass regular expressions
// is that it's always immediately obvious when a repetition ends.
// It must also be immediately obvious which branch of an | to take:
//
// /x(y|z)/ is one-pass, but /(xy|xz)/ is not.
//
// The NFA-based search in nfa.cc does some bookkeeping to
// avoid the need for backtracking and its associated exponential blowup.
// But if we have a one-pass regular expression, there is no
// possibility of backtracking, so there is no need for the
// extra bookkeeping.  Hence, this code.
//
// On a one-pass regular expression, the NFA code in nfa.cc
// runs at about 1/20 of the backtracking-based PCRE speed.
// In contrast, the code in this file runs at about the same
// speed as PCRE.
//
// One-pass regular expressions get used a lot when RE is
// used for parsing simple strings, so it pays off to
// notice them and handle them efficiently.
//
// See also Anne Brüggemann-Klein and Derick Wood,
// "One-unambiguous regular languages", Information and Computation 142(2).

#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <map>
#include <string>
#include <vector>










// Silence "zero-sized array in struct/union" warning for OneState::action.
#ifdef _MSC_VER
//#pragma warning(disable: 4200)
#endif

namespace duckdb_re2 {

// The key insight behind this implementation is that the
// non-determinism in an NFA for a one-pass regular expression
// is contained.  To explain what that means, first a
// refresher about what regular expression programs look like
// and how the usual NFA execution runs.
//
// In a regular expression program, only the kInstByteRange
// instruction processes an input byte c and moves on to the
// next byte in the string (it does so if c is in the given range).
// The kInstByteRange instructions correspond to literal characters
// and character classes in the regular expression.
//
// The kInstAlt instructions are used as wiring to connect the
// kInstByteRange instructions together in interesting ways when
// implementing | + and *.
// The kInstAlt instruction forks execution, like a goto that
// jumps to ip->out() and ip->out1() in parallel.  Each of the
// resulting computation paths is called a thread.
//
// The other instructions -- kInstEmptyWidth, kInstMatch, kInstCapture --
// are interesting in their own right but like kInstAlt they don't
// advance the input pointer.  Only kInstByteRange does.
//
// The automaton execution in nfa.cc runs all the possible
// threads of execution in lock-step over the input.  To process
// a particular byte, each thread gets run until it either dies
// or finds a kInstByteRange instruction matching the byte.
// If the latter happens, the thread stops just past the
// kInstByteRange instruction (at ip->out()) and waits for
// the other threads to finish processing the input byte.
// Then, once all the threads have processed that input byte,
// the whole process repeats.  The kInstAlt state instruction
// might create new threads during input processing, but no
// matter what, all the threads stop after a kInstByteRange
// and wait for the other threads to "catch up".
// Running in lock step like this ensures that the NFA reads
// the input string only once.
//
// Each thread maintains its own set of capture registers
// (the string positions at which it executed the kInstCapture
// instructions corresponding to capturing parentheses in the
// regular expression).  Repeated copying of the capture registers
// is the main performance bottleneck in the NFA implementation.
//
// A regular expression program is "one-pass" if, no matter what
// the input string, there is only one thread that makes it
// past a kInstByteRange instruction at each input byte.  This means
// that there is in some sense only one active thread throughout
// the execution.  Other threads might be created during the
// processing of an input byte, but they are ephemeral: only one
// thread is left to start processing the next input byte.
// This is what I meant above when I said the non-determinism
// was "contained".
//
// To execute a one-pass regular expression program, we can build
// a DFA (no non-determinism) that has at most as many states as
// the NFA (compare this to the possibly exponential number of states
// in the general case).  Each state records, for each possible
// input byte, the next state along with the conditions required
// before entering that state -- empty-width flags that must be true
// and capture operations that must be performed.  It also records
// whether a set of conditions required to finish a match at that
// point in the input rather than process the next byte.

// A state in the one-pass NFA - just an array of actions indexed
// by the bytemap_[] of the next input byte.  (The bytemap
// maps next input bytes into equivalence classes, to reduce
// the memory footprint.)
struct OneState {
  uint32_t matchcond;   // conditions to match right now.
  uint32_t action[1];
};

// The uint32_t conditions in the action are a combination of
// condition and capture bits and the next state.  The bottom 16 bits
// are the condition and capture bits, and the top 16 are the index of
// the next state.
//
// Bits 0-5 are the empty-width flags from prog.h.
// Bit 6 is kMatchWins, which means the match takes
// priority over moving to next in a first-match search.
// The remaining bits mark capture registers that should
// be set to the current input position.  The capture bits
// start at index 2, since the search loop can take care of
// cap[0], cap[1] (the overall match position).
// That means we can handle up to 5 capturing parens: $1 through $4, plus $0.
// No input position can satisfy both kEmptyWordBoundary
// and kEmptyNonWordBoundary, so we can use that as a sentinel
// instead of needing an extra bit.

static const int    kIndexShift   = 16;  // number of bits below index
static const int    kEmptyShift   = 6;   // number of empty flags in prog.h
static const int    kRealCapShift = kEmptyShift + 1;
static const int    kRealMaxCap   = (kIndexShift - kRealCapShift) / 2 * 2;

// Parameters used to skip over cap[0], cap[1].
static const int    kCapShift     = kRealCapShift - 2;
static const int    kMaxCap       = kRealMaxCap + 2;

static const uint32_t kMatchWins  = 1 << kEmptyShift;
static const uint32_t kCapMask    = ((1 << kRealMaxCap) - 1) << kRealCapShift;

static const uint32_t kImpossible = kEmptyWordBoundary | kEmptyNonWordBoundary;

// Check, at compile time, that prog.h agrees with math above.
// This function is never called.
void OnePass_Checks() {
  static_assert((1<<kEmptyShift)-1 == kEmptyAllFlags,
                "kEmptyShift disagrees with kEmptyAllFlags");
  // kMaxCap counts pointers, kMaxOnePassCapture counts pairs.
  static_assert(kMaxCap == Prog::kMaxOnePassCapture*2,
                "kMaxCap disagrees with kMaxOnePassCapture");
}

static bool Satisfy(uint32_t cond, const StringPiece& context, const char* p) {
  uint32_t satisfied = Prog::EmptyFlags(context, p);
  if (cond & kEmptyAllFlags & ~satisfied)
    return false;
  return true;
}

// Apply the capture bits in cond, saving p to the appropriate
// locations in cap[].
static void ApplyCaptures(uint32_t cond, const char* p,
                          const char** cap, int ncap) {
  for (int i = 2; i < ncap; i++)
    if (cond & (1 << kCapShift << i))
      cap[i] = p;
}

// Computes the OneState* for the given nodeindex.
static inline OneState* IndexToNode(uint8_t* nodes, int statesize,
                                    int nodeindex) {
  return reinterpret_cast<OneState*>(nodes + statesize*nodeindex);
}

bool Prog::SearchOnePass(const StringPiece& text,
                         const StringPiece& const_context,
                         Anchor anchor, MatchKind kind,
                         StringPiece* match, int nmatch) {
  if (anchor != kAnchored && kind != kFullMatch) {
    LOG(DFATAL) << "Cannot use SearchOnePass for unanchored matches.";
    return false;
  }

  // Make sure we have at least cap[1],
  // because we use it to tell if we matched.
  int ncap = 2*nmatch;
  if (ncap < 2)
    ncap = 2;

  const char* cap[kMaxCap];
  for (int i = 0; i < ncap; i++)
    cap[i] = NULL;

  const char* matchcap[kMaxCap];
  for (int i = 0; i < ncap; i++)
    matchcap[i] = NULL;

  StringPiece context = const_context;
  if (context.begin() == NULL)
    context = text;
  if (anchor_start() && context.begin() != text.begin())
    return false;
  if (anchor_end() && context.end() != text.end())
    return false;
  if (anchor_end())
    kind = kFullMatch;

  uint8_t* nodes = onepass_nodes_.data();
  int statesize = sizeof(OneState) + bytemap_range()*sizeof(uint32_t);
  // start() is always mapped to the zeroth OneState.
  OneState* state = IndexToNode(nodes, statesize, 0);
  uint8_t* bytemap = bytemap_;
  const char* bp = text.begin();
  const char* ep = text.end();
  const char* p;
  bool matched = false;
  matchcap[0] = bp;
  cap[0] = bp;
  uint32_t nextmatchcond = state->matchcond;
  for (p = bp; p < ep; p++) {
    int c = bytemap[*p & 0xFF];
    uint32_t matchcond = nextmatchcond;
    uint32_t cond = state->action[c];

    // Determine whether we can reach act->next.
    // If so, advance state and nextmatchcond.
    if ((cond & kEmptyAllFlags) == 0 || Satisfy(cond, context, p)) {
      uint32_t nextindex = cond >> kIndexShift;
      state = IndexToNode(nodes, statesize, nextindex);
      nextmatchcond = state->matchcond;
    } else {
      state = NULL;
      nextmatchcond = kImpossible;
    }

    // This code section is carefully tuned.
    // The goto sequence is about 10% faster than the
    // obvious rewrite as a large if statement in the
    // ASCIIMatchRE2 and DotMatchRE2 benchmarks.

    // Saving the match capture registers is expensive.
    // Is this intermediate match worth thinking about?

    // Not if we want a full match.
    if (kind == kFullMatch)
      goto skipmatch;

    // Not if it's impossible.
    if (matchcond == kImpossible)
      goto skipmatch;

    // Not if the possible match is beaten by the certain
    // match at the next byte.  When this test is useless
    // (e.g., HTTPPartialMatchRE2) it slows the loop by
    // about 10%, but when it avoids work (e.g., DotMatchRE2),
    // it cuts the loop execution by about 45%.
    if ((cond & kMatchWins) == 0 && (nextmatchcond & kEmptyAllFlags) == 0)
      goto skipmatch;

    // Finally, the match conditions must be satisfied.
    if ((matchcond & kEmptyAllFlags) == 0 || Satisfy(matchcond, context, p)) {
      for (int i = 2; i < 2*nmatch; i++)
        matchcap[i] = cap[i];
      if (nmatch > 1 && (matchcond & kCapMask))
        ApplyCaptures(matchcond, p, matchcap, ncap);
      matchcap[1] = p;
      matched = true;

      // If we're in longest match mode, we have to keep
      // going and see if we find a longer match.
      // In first match mode, we can stop if the match
      // takes priority over the next state for this input byte.
      // That bit is per-input byte and thus in cond, not matchcond.
      if (kind == kFirstMatch && (cond & kMatchWins))
        goto done;
    }

  skipmatch:
    if (state == NULL)
      goto done;
    if ((cond & kCapMask) && nmatch > 1)
      ApplyCaptures(cond, p, cap, ncap);
  }

  // Look for match at end of input.
  {
    uint32_t matchcond = state->matchcond;
    if (matchcond != kImpossible &&
        ((matchcond & kEmptyAllFlags) == 0 || Satisfy(matchcond, context, p))) {
      if (nmatch > 1 && (matchcond & kCapMask))
        ApplyCaptures(matchcond, p, cap, ncap);
      for (int i = 2; i < ncap; i++)
        matchcap[i] = cap[i];
      matchcap[1] = p;
      matched = true;
    }
  }

done:
  if (!matched)
    return false;
  for (int i = 0; i < nmatch; i++)
    match[i] =
        StringPiece(matchcap[2 * i],
                    static_cast<size_t>(matchcap[2 * i + 1] - matchcap[2 * i]));
  return true;
}


// Analysis to determine whether a given regexp program is one-pass.

// If ip is not on workq, adds ip to work queue and returns true.
// If ip is already on work queue, does nothing and returns false.
// If ip is NULL, does nothing and returns true (pretends to add it).
typedef SparseSet Instq;
static bool AddQ(Instq *q, int id) {
  if (id == 0)
    return true;
  if (q->contains(id))
    return false;
  q->insert(id);
  return true;
}

struct InstCond {
  int id;
  uint32_t cond;
};

// Returns whether this is a one-pass program; that is,
// returns whether it is safe to use SearchOnePass on this program.
// These conditions must be true for any instruction ip:
//
//   (1) for any other Inst nip, there is at most one input-free
//       path from ip to nip.
//   (2) there is at most one kInstByte instruction reachable from
//       ip that matches any particular byte c.
//   (3) there is at most one input-free path from ip to a kInstMatch
//       instruction.
//
// This is actually just a conservative approximation: it might
// return false when the answer is true, when kInstEmptyWidth
// instructions are involved.
// Constructs and saves corresponding one-pass NFA on success.
bool Prog::IsOnePass() {
  if (did_onepass_)
    return onepass_nodes_.data() != NULL;
  did_onepass_ = true;

  if (start() == 0)  // no match
    return false;

  // Steal memory for the one-pass NFA from the overall DFA budget.
  // Willing to use at most 1/4 of the DFA budget (heuristic).
  // Limit max node count to 65000 as a conservative estimate to
  // avoid overflowing 16-bit node index in encoding.
  int maxnodes = 2 + inst_count(kInstByteRange);
  int statesize = sizeof(OneState) + bytemap_range()*sizeof(uint32_t);
  if (maxnodes >= 65000 || dfa_mem_ / 4 / statesize < maxnodes)
    return false;

  // Flood the graph starting at the start state, and check
  // that in each reachable state, each possible byte leads
  // to a unique next state.
  int stacksize = inst_count(kInstCapture) +
                  inst_count(kInstEmptyWidth) +
                  inst_count(kInstNop) + 1;  // + 1 for start inst
  PODArray<InstCond> stack(stacksize);

  int size = this->size();
  PODArray<int> nodebyid(size);  // indexed by ip
  memset(nodebyid.data(), 0xFF, size*sizeof nodebyid[0]);

  // Originally, nodes was a uint8_t[maxnodes*statesize], but that was
  // unnecessarily optimistic: why allocate a large amount of memory
  // upfront for a large program when it is unlikely to be one-pass?
  std::vector<uint8_t> nodes;

  Instq tovisit(size), workq(size);
  AddQ(&tovisit, start());
  nodebyid[start()] = 0;
  int nalloc = 1;
  nodes.insert(nodes.end(), statesize, 0);
  for (Instq::iterator it = tovisit.begin(); it != tovisit.end(); ++it) {
    int id = *it;
    int nodeindex = nodebyid[id];
    OneState* node = IndexToNode(nodes.data(), statesize, nodeindex);

    // Flood graph using manual stack, filling in actions as found.
    // Default is none.
    for (int b = 0; b < bytemap_range_; b++)
      node->action[b] = kImpossible;
    node->matchcond = kImpossible;

    workq.clear();
    bool matched = false;
    int nstack = 0;
    stack[nstack].id = id;
    stack[nstack++].cond = 0;
    while (nstack > 0) {
      int id = stack[--nstack].id;
      uint32_t cond = stack[nstack].cond;

    Loop:
      Prog::Inst* ip = inst(id);
      switch (ip->opcode()) {
        default:
          LOG(DFATAL) << "unhandled opcode: " << ip->opcode();
          break;

        case kInstAltMatch:
          // TODO(rsc): Ignoring kInstAltMatch optimization.
          // Should implement it in this engine, but it's subtle.
          DCHECK(!ip->last());
          // If already on work queue, (1) is violated: bail out.
          if (!AddQ(&workq, id+1))
            goto fail;
          id = id+1;
          goto Loop;

        case kInstByteRange: {
          int nextindex = nodebyid[ip->out()];
          if (nextindex == -1) {
            if (nalloc >= maxnodes) {
              goto fail;
            }
            nextindex = nalloc;
            AddQ(&tovisit, ip->out());
            nodebyid[ip->out()] = nalloc;
            nalloc++;
            nodes.insert(nodes.end(), statesize, 0);
            // Update node because it might have been invalidated.
            node = IndexToNode(nodes.data(), statesize, nodeindex);
          }
          for (int c = ip->lo(); c <= ip->hi(); c++) {
            int b = bytemap_[c];
            // Skip any bytes immediately after c that are also in b.
            while (c < 256-1 && bytemap_[c+1] == b)
              c++;
            uint32_t act = node->action[b];
            uint32_t newact = (nextindex << kIndexShift) | cond;
            if (matched)
              newact |= kMatchWins;
            if ((act & kImpossible) == kImpossible) {
              node->action[b] = newact;
            } else if (act != newact) {
              goto fail;
            }
          }
          if (ip->foldcase()) {
            Rune lo = std::max<Rune>(ip->lo(), 'a') + 'A' - 'a';
            Rune hi = std::min<Rune>(ip->hi(), 'z') + 'A' - 'a';
            for (int c = lo; c <= hi; c++) {
              int b = bytemap_[c];
              // Skip any bytes immediately after c that are also in b.
              while (c < 256-1 && bytemap_[c+1] == b)
                c++;
              uint32_t act = node->action[b];
              uint32_t newact = (nextindex << kIndexShift) | cond;
              if (matched)
                newact |= kMatchWins;
              if ((act & kImpossible) == kImpossible) {
                node->action[b] = newact;
              } else if (act != newact) {
                goto fail;
              }
            }
          }

          if (ip->last())
            break;
          // If already on work queue, (1) is violated: bail out.
          if (!AddQ(&workq, id+1))
            goto fail;
          id = id+1;
          goto Loop;
        }

        case kInstCapture:
        case kInstEmptyWidth:
        case kInstNop:
          if (!ip->last()) {
            // If already on work queue, (1) is violated: bail out.
            if (!AddQ(&workq, id+1))
              goto fail;
            stack[nstack].id = id+1;
            stack[nstack++].cond = cond;
          }

          if (ip->opcode() == kInstCapture && ip->cap() < kMaxCap)
            cond |= (1 << kCapShift) << ip->cap();
          if (ip->opcode() == kInstEmptyWidth)
            cond |= ip->empty();

          // kInstCapture and kInstNop always proceed to ip->out().
          // kInstEmptyWidth only sometimes proceeds to ip->out(),
          // but as a conservative approximation we assume it always does.
          // We could be a little more precise by looking at what c
          // is, but that seems like overkill.

          // If already on work queue, (1) is violated: bail out.
          if (!AddQ(&workq, ip->out())) {
            goto fail;
          }
          id = ip->out();
          goto Loop;

        case kInstMatch:
          if (matched) {
            // (3) is violated
            goto fail;
          }
          matched = true;
          node->matchcond = cond;

          if (ip->last())
            break;
          // If already on work queue, (1) is violated: bail out.
          if (!AddQ(&workq, id+1))
            goto fail;
          id = id+1;
          goto Loop;

        case kInstFail:
          break;
      }
    }
  }
  dfa_mem_ -= nalloc*statesize;
  onepass_nodes_ = PODArray<uint8_t>(nalloc*statesize);
  memmove(onepass_nodes_.data(), nodes.data(), nalloc*statesize);
  return true;

fail:
  return false;
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2006 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Regular expression parser.

// The parser is a simple precedence-based parser with a
// manual stack.  The parsing work is done by the methods
// of the ParseState class.  The Regexp::Parse function is
// essentially just a lexer that calls the ParseState method
// for each token.

// The parser recognizes POSIX extended regular expressions
// excluding backreferences, collating elements, and collating
// classes.  It also allows the empty string as a regular expression
// and recognizes the Perl escape sequences \d, \s, \w, \D, \S, and \W.
// See regexp.h for rationale.

#include <ctype.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <map>
#include <string>
#include <vector>










// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2008 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef RE2_UNICODE_CASEFOLD_H_
#define RE2_UNICODE_CASEFOLD_H_

// Unicode case folding tables.

// The Unicode case folding tables encode the mapping from one Unicode point
// to the next largest Unicode point with equivalent folding.  The largest
// point wraps back to the first.  For example, the tables map:
//
//     'A' -> 'a'
//     'a' -> 'A'
//
//     'K' -> 'k'
//     'k' -> 'K'  (Kelvin symbol)
//     'K' -> 'K'
//
// Like everything Unicode, these tables are big.  If we represent the table
// as a sorted list of uint32_t pairs, it has 2049 entries and is 16 kB.
// Most table entries look like the ones around them:
// 'A' maps to 'A'+32, 'B' maps to 'B'+32, etc.
// Instead of listing all the pairs explicitly, we make a list of ranges
// and deltas, so that the table entries for 'A' through 'Z' can be represented
// as a single entry { 'A', 'Z', +32 }.
//
// In addition to blocks that map to each other (A-Z mapping to a-z)
// there are blocks of pairs that individually map to each other
// (for example, 0100<->0101, 0102<->0103, 0104<->0105, ...).
// For those, the special delta value EvenOdd marks even/odd pairs
// (if even, add 1; if odd, subtract 1), and OddEven marks odd/even pairs.
//
// In this form, the table has 274 entries, about 3kB.  If we were to split
// the table into one for 16-bit codes and an overflow table for larger ones,
// we could get it down to about 1.5kB, but that's not worth the complexity.
//
// The grouped form also allows for efficient fold range calculations
// rather than looping one character at a time.

#include <stdint.h>




namespace duckdb_re2 {

enum {
  EvenOdd = 1,
  OddEven = -1,
  EvenOddSkip = 1<<30,
  OddEvenSkip,
};

struct CaseFold {
  Rune lo;
  Rune hi;
  int32_t delta;
};

extern const CaseFold unicode_casefold[];
extern const int num_unicode_casefold;

extern const CaseFold unicode_tolower[];
extern const int num_unicode_tolower;

// Returns the CaseFold* in the tables that contains rune.
// If rune is not in the tables, returns the first CaseFold* after rune.
// If rune is larger than any value in the tables, returns NULL.
extern const CaseFold* LookupCaseFold(const CaseFold*, int, Rune rune);

// Returns the result of applying the fold f to the rune r.
extern Rune ApplyFold(const CaseFold *f, Rune r);

}  // namespace duckdb_re2

#endif  // RE2_UNICODE_CASEFOLD_H_


// LICENSE_CHANGE_END



// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2008 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef RE2_UNICODE_GROUPS_H_
#define RE2_UNICODE_GROUPS_H_

// Unicode character groups.

// The codes get split into ranges of 16-bit codes
// and ranges of 32-bit codes.  It would be simpler
// to use only 32-bit ranges, but these tables are large
// enough to warrant extra care.
//
// Using just 32-bit ranges gives 27 kB of data.
// Adding 16-bit ranges gives 18 kB of data.
// Adding an extra table of 16-bit singletons would reduce
// to 16.5 kB of data but make the data harder to use;
// we don't bother.

#include <stdint.h>




namespace duckdb_re2 {

struct URange16
{
  uint16_t lo;
  uint16_t hi;
};

struct URange32
{
  Rune lo;
  Rune hi;
};

struct UGroup
{
  const char *name;
  int sign;  // +1 for [abc], -1 for [^abc]
  const URange16 *r16;
  int nr16;
  const URange32 *r32;
  int nr32;
};

// Named by property or script name (e.g., "Nd", "N", "Han").
// Negated groups are not included.
extern const UGroup unicode_groups[];
extern const int num_unicode_groups;

// Named by POSIX name (e.g., "[:alpha:]", "[:^lower:]").
// Negated groups are included.
extern const UGroup posix_groups[];
extern const int num_posix_groups;

// Named by Perl name (e.g., "\\d", "\\D").
// Negated groups are included.
extern const UGroup perl_groups[];
extern const int num_perl_groups;

}  // namespace duckdb_re2

#endif  // RE2_UNICODE_GROUPS_H_


// LICENSE_CHANGE_END



// #if defined(RE2_USE_ICU)
// #include "unicode/uniset.h"
// #include "unicode/unistr.h"
// #include "unicode/utypes.h"
// #endif

namespace duckdb_re2 {

// Reduce the maximum repeat count by an order of magnitude when fuzzing.
#ifdef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION
static const int kMaxRepeat = 100;
#else
static const int kMaxRepeat = 1000;
#endif

// Regular expression parse state.
// The list of parsed regexps so far is maintained as a vector of
// Regexp pointers called the stack.  Left parenthesis and vertical
// bar markers are also placed on the stack, as Regexps with
// non-standard opcodes.
// Scanning a left parenthesis causes the parser to push a left parenthesis
// marker on the stack.
// Scanning a vertical bar causes the parser to pop the stack until it finds a
// vertical bar or left parenthesis marker (not popping the marker),
// concatenate all the popped results, and push them back on
// the stack (DoConcatenation).
// Scanning a right parenthesis causes the parser to act as though it
// has seen a vertical bar, which then leaves the top of the stack in the
// form LeftParen regexp VerticalBar regexp VerticalBar ... regexp VerticalBar.
// The parser pops all this off the stack and creates an alternation of the
// regexps (DoAlternation).

class Regexp::ParseState {
 public:
  ParseState(ParseFlags flags, const StringPiece& whole_regexp,
             RegexpStatus* status);
  ~ParseState();

  ParseFlags flags() { return flags_; }
  int rune_max() { return rune_max_; }

  // Parse methods.  All public methods return a bool saying
  // whether parsing should continue.  If a method returns
  // false, it has set fields in *status_, and the parser
  // should return NULL.

  // Pushes the given regular expression onto the stack.
  // Could check for too much memory used here.
  bool PushRegexp(Regexp* re);

  // Pushes the literal rune r onto the stack.
  bool PushLiteral(Rune r);

  // Pushes a regexp with the given op (and no args) onto the stack.
  bool PushSimpleOp(RegexpOp op);

  // Pushes a ^ onto the stack.
  bool PushCarat();

  // Pushes a \b (word == true) or \B (word == false) onto the stack.
  bool PushWordBoundary(bool word);

  // Pushes a $ onto the stack.
  bool PushDollar();

  // Pushes a . onto the stack
  bool PushDot();

  // Pushes a repeat operator regexp onto the stack.
  // A valid argument for the operator must already be on the stack.
  // s is the name of the operator, for use in error messages.
  bool PushRepeatOp(RegexpOp op, const StringPiece& s, bool nongreedy);

  // Pushes a repetition regexp onto the stack.
  // A valid argument for the operator must already be on the stack.
  bool PushRepetition(int min, int max, const StringPiece& s, bool nongreedy);

  // Checks whether a particular regexp op is a marker.
  bool IsMarker(RegexpOp op);

  // Processes a left parenthesis in the input.
  // Pushes a marker onto the stack.
  bool DoLeftParen(const StringPiece& name);
  bool DoLeftParenNoCapture();

  // Processes a vertical bar in the input.
  bool DoVerticalBar();

  // Processes a right parenthesis in the input.
  bool DoRightParen();

  // Processes the end of input, returning the final regexp.
  Regexp* DoFinish();

  // Finishes the regexp if necessary, preparing it for use
  // in a more complicated expression.
  // If it is a CharClassBuilder, converts into a CharClass.
  Regexp* FinishRegexp(Regexp*);

  // These routines don't manipulate the parse stack
  // directly, but they do need to look at flags_.
  // ParseCharClass also manipulates the internals of Regexp
  // while creating *out_re.

  // Parse a character class into *out_re.
  // Removes parsed text from s.
  bool ParseCharClass(StringPiece* s, Regexp** out_re,
                      RegexpStatus* status);

  // Parse a character class character into *rp.
  // Removes parsed text from s.
  bool ParseCCCharacter(StringPiece* s, Rune *rp,
                        const StringPiece& whole_class,
                        RegexpStatus* status);

  // Parse a character class range into rr.
  // Removes parsed text from s.
  bool ParseCCRange(StringPiece* s, RuneRange* rr,
                    const StringPiece& whole_class,
                    RegexpStatus* status);

  // Parse a Perl flag set or non-capturing group from s.
  bool ParsePerlFlags(StringPiece* s);


  // Finishes the current concatenation,
  // collapsing it into a single regexp on the stack.
  void DoConcatenation();

  // Finishes the current alternation,
  // collapsing it to a single regexp on the stack.
  void DoAlternation();

  // Generalized DoAlternation/DoConcatenation.
  void DoCollapse(RegexpOp op);

  // Maybe concatenate Literals into LiteralString.
  bool MaybeConcatString(int r, ParseFlags flags);

private:
  ParseFlags flags_;
  StringPiece whole_regexp_;
  RegexpStatus* status_;
  Regexp* stacktop_;
  int ncap_;  // number of capturing parens seen
  int rune_max_;  // maximum char value for this encoding

  ParseState(const ParseState&) = delete;
  ParseState& operator=(const ParseState&) = delete;
};

// Pseudo-operators - only on parse stack.
const RegexpOp kLeftParen = static_cast<RegexpOp>(kMaxRegexpOp+1);
const RegexpOp kVerticalBar = static_cast<RegexpOp>(kMaxRegexpOp+2);

Regexp::ParseState::ParseState(ParseFlags flags,
                               const StringPiece& whole_regexp,
                               RegexpStatus* status)
  : flags_(flags), whole_regexp_(whole_regexp),
    status_(status), stacktop_(NULL), ncap_(0) {
  if (flags_ & Latin1)
    rune_max_ = 0xFF;
  else
    rune_max_ = Runemax;
}

// Cleans up by freeing all the regexps on the stack.
Regexp::ParseState::~ParseState() {
  Regexp* next;
  for (Regexp* re = stacktop_; re != NULL; re = next) {
    next = re->down_;
    re->down_ = NULL;
    if (re->op() == kLeftParen)
      delete re->capture_.name_;
    re->Decref();
  }
}

// Finishes the regexp if necessary, preparing it for use in
// a more complex expression.
// If it is a CharClassBuilder, converts into a CharClass.
Regexp* Regexp::ParseState::FinishRegexp(Regexp* re) {
  if (re == NULL)
    return NULL;
  re->down_ = NULL;

  if (re->op_ == kRegexpCharClass && re->char_class_.ccb_ != NULL) {
    CharClassBuilder* ccb = re->char_class_.ccb_;
    re->char_class_.ccb_ = NULL;
    re->char_class_.cc_ = ccb->GetCharClass();
    delete ccb;
  }

  return re;
}

// Pushes the given regular expression onto the stack.
// Could check for too much memory used here.
bool Regexp::ParseState::PushRegexp(Regexp* re) {
  MaybeConcatString(-1, NoParseFlags);

  // Special case: a character class of one character is just
  // a literal.  This is a common idiom for escaping
  // single characters (e.g., [.] instead of \.), and some
  // analysis does better with fewer character classes.
  // Similarly, [Aa] can be rewritten as a literal A with ASCII case folding.
  auto ccb = re->char_class_.ccb_;
  if (re->op_ == kRegexpCharClass && ccb != NULL) {
	  ccb->RemoveAbove(rune_max_);
    if (ccb->size() == 1) {
      Rune r = ccb->begin()->lo;
      re->Decref();
      re = new Regexp(kRegexpLiteral, flags_);
      re->rune_ = r;
    } else if (ccb->size() == 2) {
      Rune r = ccb->begin()->lo;
      if ('A' <= r && r <= 'Z' && ccb->Contains(r + 'a' - 'A')) {
        re->Decref();
        re = new Regexp(kRegexpLiteral, flags_ | FoldCase);
        re->rune_ = r + 'a' - 'A';
      }
    }
  }

  if (!IsMarker(re->op()))
    re->simple_ = re->ComputeSimple();
  re->down_ = stacktop_;
  stacktop_ = re;
  return true;
}

// Searches the case folding tables and returns the CaseFold* that contains r.
// If there isn't one, returns the CaseFold* with smallest f->lo bigger than r.
// If there isn't one, returns NULL.
const CaseFold* LookupCaseFold(const CaseFold *f, int n, Rune r) {
  const CaseFold* ef = f + n;

  // Binary search for entry containing r.
  while (n > 0) {
    int m = n/2;
    if (f[m].lo <= r && r <= f[m].hi)
      return &f[m];
    if (r < f[m].lo) {
      n = m;
    } else {
      f += m+1;
      n -= m+1;
    }
  }

  // There is no entry that contains r, but f points
  // where it would have been.  Unless f points at
  // the end of the array, it points at the next entry
  // after r.
  if (f < ef)
    return f;

  // No entry contains r; no entry contains runes > r.
  return NULL;
}

// Returns the result of applying the fold f to the rune r.
Rune ApplyFold(const CaseFold *f, Rune r) {
  switch (f->delta) {
    default:
      return r + f->delta;

    case EvenOddSkip:  // even <-> odd but only applies to every other
      if ((r - f->lo) % 2)
        return r;
      FALLTHROUGH_INTENDED;
    case EvenOdd:  // even <-> odd
      if (r%2 == 0)
        return r + 1;
      return r - 1;

    case OddEvenSkip:  // odd <-> even but only applies to every other
      if ((r - f->lo) % 2)
        return r;
      FALLTHROUGH_INTENDED;
    case OddEven:  // odd <-> even
      if (r%2 == 1)
        return r + 1;
      return r - 1;
  }
}

// Returns the next Rune in r's folding cycle (see unicode_casefold.h).
// Examples:
//   CycleFoldRune('A') = 'a'
//   CycleFoldRune('a') = 'A'
//
//   CycleFoldRune('K') = 'k'
//   CycleFoldRune('k') = 0x212A (Kelvin)
//   CycleFoldRune(0x212A) = 'K'
//
//   CycleFoldRune('?') = '?'
Rune CycleFoldRune(Rune r) {
  const CaseFold* f = LookupCaseFold(unicode_casefold, num_unicode_casefold, r);
  if (f == NULL || r < f->lo)
    return r;
  return ApplyFold(f, r);
}

// Add lo-hi to the class, along with their fold-equivalent characters.
// If lo-hi is already in the class, assume that the fold-equivalent
// chars are there too, so there's no work to do.
static void AddFoldedRange(CharClassBuilder* cc, Rune lo, Rune hi, int depth) {
  // AddFoldedRange calls itself recursively for each rune in the fold cycle.
  // Most folding cycles are small: there aren't any bigger than four in the
  // current Unicode tables.  make_unicode_casefold.py checks that
  // the cycles are not too long, and we double-check here using depth.
  if (depth > 10) {
    LOG(DFATAL) << "AddFoldedRange recurses too much.";
    return;
  }

  if (!cc->AddRange(lo, hi))  // lo-hi was already there? we're done
    return;

  while (lo <= hi) {
    const CaseFold* f = LookupCaseFold(unicode_casefold, num_unicode_casefold, lo);
    if (f == NULL)  // lo has no fold, nor does anything above lo
      break;
    if (lo < f->lo) {  // lo has no fold; next rune with a fold is f->lo
      lo = f->lo;
      continue;
    }

    // Add in the result of folding the range lo - f->hi
    // and that range's fold, recursively.
    Rune lo1 = lo;
    Rune hi1 = std::min<Rune>(hi, f->hi);
    switch (f->delta) {
      default:
        lo1 += f->delta;
        hi1 += f->delta;
        break;
      case EvenOdd:
        if (lo1%2 == 1)
          lo1--;
        if (hi1%2 == 0)
          hi1++;
        break;
      case OddEven:
        if (lo1%2 == 0)
          lo1--;
        if (hi1%2 == 1)
          hi1++;
        break;
    }
    AddFoldedRange(cc, lo1, hi1, depth+1);

    // Pick up where this fold left off.
    lo = f->hi + 1;
  }
}

// Pushes the literal rune r onto the stack.
bool Regexp::ParseState::PushLiteral(Rune r) {
  // Do case folding if needed.
  if ((flags_ & FoldCase) && CycleFoldRune(r) != r) {
    Regexp* re = new Regexp(kRegexpCharClass, flags_ & ~FoldCase);
    re->char_class_.ccb_ = new CharClassBuilder;
    Rune r1 = r;
    do {
      if (!(flags_ & NeverNL) || r != '\n') {
        re->char_class_.ccb_->AddRange(r, r);
      }
      r = CycleFoldRune(r);
    } while (r != r1);
    return PushRegexp(re);
  }

  // Exclude newline if applicable.
  if ((flags_ & NeverNL) && r == '\n')
    return PushRegexp(new Regexp(kRegexpNoMatch, flags_));

  // No fancy stuff worked.  Ordinary literal.
  if (MaybeConcatString(r, flags_))
    return true;

  Regexp* re = new Regexp(kRegexpLiteral, flags_);
  re->rune_ = r;
  return PushRegexp(re);
}

// Pushes a ^ onto the stack.
bool Regexp::ParseState::PushCarat() {
  if (flags_ & OneLine) {
    return PushSimpleOp(kRegexpBeginText);
  }
  return PushSimpleOp(kRegexpBeginLine);
}

// Pushes a \b or \B onto the stack.
bool Regexp::ParseState::PushWordBoundary(bool word) {
  if (word)
    return PushSimpleOp(kRegexpWordBoundary);
  return PushSimpleOp(kRegexpNoWordBoundary);
}

// Pushes a $ onto the stack.
bool Regexp::ParseState::PushDollar() {
  if (flags_ & OneLine) {
    // Clumsy marker so that MimicsPCRE() can tell whether
    // this kRegexpEndText was a $ and not a \z.
    Regexp::ParseFlags oflags = flags_;
    flags_ = flags_ | WasDollar;
    bool ret = PushSimpleOp(kRegexpEndText);
    flags_ = oflags;
    return ret;
  }
  return PushSimpleOp(kRegexpEndLine);
}

// Pushes a . onto the stack.
bool Regexp::ParseState::PushDot() {
  if ((flags_ & DotNL) && !(flags_ & NeverNL))
    return PushSimpleOp(kRegexpAnyChar);
  // Rewrite . into [^\n]
  Regexp* re = new Regexp(kRegexpCharClass, flags_ & ~FoldCase);
  re->char_class_.ccb_ = new CharClassBuilder;
  re->char_class_.ccb_->AddRange(0, '\n' - 1);
  re->char_class_.ccb_->AddRange('\n' + 1, rune_max_);
  return PushRegexp(re);
}

// Pushes a regexp with the given op (and no args) onto the stack.
bool Regexp::ParseState::PushSimpleOp(RegexpOp op) {
  Regexp* re = new Regexp(op, flags_);
  return PushRegexp(re);
}

// Pushes a repeat operator regexp onto the stack.
// A valid argument for the operator must already be on the stack.
// The char c is the name of the operator, for use in error messages.
bool Regexp::ParseState::PushRepeatOp(RegexpOp op, const StringPiece& s,
                                      bool nongreedy) {
  if (stacktop_ == NULL || IsMarker(stacktop_->op())) {
    status_->set_code(kRegexpRepeatArgument);
    status_->set_error_arg(s);
    return false;
  }
  Regexp::ParseFlags fl = flags_;
  if (nongreedy)
    fl = fl ^ NonGreedy;

  // Squash **, ++ and ??. Regexp::Star() et al. handle this too, but
  // they're mostly for use during simplification, not during parsing.
  if (op == stacktop_->op() && fl == stacktop_->parse_flags())
    return true;

  // Squash *+, *?, +*, +?, ?* and ?+. They all squash to *, so because
  // op is a repeat, we just have to check that stacktop_->op() is too,
  // then adjust stacktop_.
  if ((stacktop_->op() == kRegexpStar ||
       stacktop_->op() == kRegexpPlus ||
       stacktop_->op() == kRegexpQuest) &&
      fl == stacktop_->parse_flags()) {
    stacktop_->op_ = kRegexpStar;
    return true;
  }

  Regexp* re = new Regexp(op, fl);
  re->AllocSub(1);
  re->down_ = stacktop_->down_;
  re->sub()[0] = FinishRegexp(stacktop_);
  re->simple_ = re->ComputeSimple();
  stacktop_ = re;
  return true;
}

// RepetitionWalker reports whether the repetition regexp is valid.
// Valid means that the combination of the top-level repetition
// and any inner repetitions does not exceed n copies of the
// innermost thing.
// This rewalks the regexp tree and is called for every repetition,
// so we have to worry about inducing quadratic behavior in the parser.
// We avoid this by only using RepetitionWalker when min or max >= 2.
// In that case the depth of any >= 2 nesting can only get to 9 without
// triggering a parse error, so each subtree can only be rewalked 9 times.
class RepetitionWalker : public Regexp::Walker<int> {
 public:
  RepetitionWalker() {}
  virtual int PreVisit(Regexp* re, int parent_arg, bool* stop);
  virtual int PostVisit(Regexp* re, int parent_arg, int pre_arg,
                        int* child_args, int nchild_args);
  virtual int ShortVisit(Regexp* re, int parent_arg);

 private:
  RepetitionWalker(const RepetitionWalker&) = delete;
  RepetitionWalker& operator=(const RepetitionWalker&) = delete;
};

int RepetitionWalker::PreVisit(Regexp* re, int parent_arg, bool* stop) {
  int arg = parent_arg;
  if (re->op() == kRegexpRepeat) {
    int m = re->max();
    if (m < 0) {
      m = re->min();
    }
    if (m > 0) {
      arg /= m;
    }
  }
  return arg;
}

int RepetitionWalker::PostVisit(Regexp* re, int parent_arg, int pre_arg,
                                int* child_args, int nchild_args) {
  int arg = pre_arg;
  for (int i = 0; i < nchild_args; i++) {
    if (child_args[i] < arg) {
      arg = child_args[i];
    }
  }
  return arg;
}

int RepetitionWalker::ShortVisit(Regexp* re, int parent_arg) {
  // This should never be called, since we use Walk and not
  // WalkExponential.
  LOG(DFATAL) << "RepetitionWalker::ShortVisit called";
  return 0;
}

// Pushes a repetition regexp onto the stack.
// A valid argument for the operator must already be on the stack.
bool Regexp::ParseState::PushRepetition(int min, int max,
                                        const StringPiece& s,
                                        bool nongreedy) {
  if ((max != -1 && max < min) || min > kMaxRepeat || max > kMaxRepeat) {
    status_->set_code(kRegexpRepeatSize);
    status_->set_error_arg(s);
    return false;
  }
  if (stacktop_ == NULL || IsMarker(stacktop_->op())) {
    status_->set_code(kRegexpRepeatArgument);
    status_->set_error_arg(s);
    return false;
  }
  Regexp::ParseFlags fl = flags_;
  if (nongreedy)
    fl = fl ^ NonGreedy;
  Regexp* re = new Regexp(kRegexpRepeat, fl);
  re->repeat_.min_ = min;
  re->repeat_.max_ = max;
  re->AllocSub(1);
  re->down_ = stacktop_->down_;
  re->sub()[0] = FinishRegexp(stacktop_);
  re->simple_ = re->ComputeSimple();
  stacktop_ = re;
  if (min >= 2 || max >= 2) {
    RepetitionWalker w;
    if (w.Walk(stacktop_, kMaxRepeat) == 0) {
      status_->set_code(kRegexpRepeatSize);
      status_->set_error_arg(s);
      return false;
    }
  }
  return true;
}

// Checks whether a particular regexp op is a marker.
bool Regexp::ParseState::IsMarker(RegexpOp op) {
  return op >= kLeftParen;
}

// Processes a left parenthesis in the input.
// Pushes a marker onto the stack.
bool Regexp::ParseState::DoLeftParen(const StringPiece& name) {
  Regexp* re = new Regexp(kLeftParen, flags_);
  re->capture_.cap_ = ++ncap_;
  if (name.data() != NULL)
    re->capture_.name_ = new std::string(name);
  return PushRegexp(re);
}

// Pushes a non-capturing marker onto the stack.
bool Regexp::ParseState::DoLeftParenNoCapture() {
  Regexp* re = new Regexp(kLeftParen, flags_);
  re->capture_.cap_ = -1;
  return PushRegexp(re);
}

// Processes a vertical bar in the input.
bool Regexp::ParseState::DoVerticalBar() {
  MaybeConcatString(-1, NoParseFlags);
  DoConcatenation();

  // Below the vertical bar is a list to alternate.
  // Above the vertical bar is a list to concatenate.
  // We just did the concatenation, so either swap
  // the result below the vertical bar or push a new
  // vertical bar on the stack.
  Regexp* r1;
  Regexp* r2;
  if ((r1 = stacktop_) != NULL &&
      (r2 = r1->down_) != NULL &&
      r2->op() == kVerticalBar) {
    Regexp* r3;
    if ((r3 = r2->down_) != NULL &&
        (r1->op() == kRegexpAnyChar || r3->op() == kRegexpAnyChar)) {
      // AnyChar is above or below the vertical bar. Let it subsume
      // the other when the other is Literal, CharClass or AnyChar.
      if (r3->op() == kRegexpAnyChar &&
          (r1->op() == kRegexpLiteral ||
           r1->op() == kRegexpCharClass ||
           r1->op() == kRegexpAnyChar)) {
        // Discard r1.
        stacktop_ = r2;
        r1->Decref();
        return true;
      }
      if (r1->op() == kRegexpAnyChar &&
          (r3->op() == kRegexpLiteral ||
           r3->op() == kRegexpCharClass ||
           r3->op() == kRegexpAnyChar)) {
        // Rearrange the stack and discard r3.
        r1->down_ = r3->down_;
        r2->down_ = r1;
        stacktop_ = r2;
        r3->Decref();
        return true;
      }
    }
    // Swap r1 below vertical bar (r2).
    r1->down_ = r2->down_;
    r2->down_ = r1;
    stacktop_ = r2;
    return true;
  }
  return PushSimpleOp(kVerticalBar);
}

// Processes a right parenthesis in the input.
bool Regexp::ParseState::DoRightParen() {
  // Finish the current concatenation and alternation.
  DoAlternation();

  // The stack should be: LeftParen regexp
  // Remove the LeftParen, leaving the regexp,
  // parenthesized.
  Regexp* r1;
  Regexp* r2;
  if ((r1 = stacktop_) == NULL ||
      (r2 = r1->down_) == NULL ||
      r2->op() != kLeftParen) {
    status_->set_code(kRegexpMissingParen);
    status_->set_error_arg(whole_regexp_);
    return false;
  }

  // Pop off r1, r2.  Will Decref or reuse below.
  stacktop_ = r2->down_;

  // Restore flags from when paren opened.
  Regexp* re = r2;
  flags_ = re->parse_flags();

  // Rewrite LeftParen as capture if needed.
  if (re->capture_.cap_ > 0) {
    re->op_ = kRegexpCapture;
    // re->cap_ is already set
    re->AllocSub(1);
    re->sub()[0] = FinishRegexp(r1);
    re->simple_ = re->ComputeSimple();
  } else {
    re->Decref();
    re = r1;
  }
  return PushRegexp(re);
}

// Processes the end of input, returning the final regexp.
Regexp* Regexp::ParseState::DoFinish() {
  DoAlternation();
  Regexp* re = stacktop_;
  if (re != NULL && re->down_ != NULL) {
    status_->set_code(kRegexpMissingParen);
    status_->set_error_arg(whole_regexp_);
    return NULL;
  }
  stacktop_ = NULL;
  return FinishRegexp(re);
}

// Returns the leading regexp that re starts with.
// The returned Regexp* points into a piece of re,
// so it must not be used after the caller calls re->Decref().
Regexp* Regexp::LeadingRegexp(Regexp* re) {
  if (re->op() == kRegexpEmptyMatch)
    return NULL;
  if (re->op() == kRegexpConcat && re->nsub() >= 2) {
    Regexp** sub = re->sub();
    if (sub[0]->op() == kRegexpEmptyMatch)
      return NULL;
    return sub[0];
  }
  return re;
}

// Removes LeadingRegexp(re) from re and returns what's left.
// Consumes the reference to re and may edit it in place.
// If caller wants to hold on to LeadingRegexp(re),
// must have already Incref'ed it.
Regexp* Regexp::RemoveLeadingRegexp(Regexp* re) {
  if (re->op() == kRegexpEmptyMatch)
    return re;
  if (re->op() == kRegexpConcat && re->nsub() >= 2) {
    Regexp** sub = re->sub();
    if (sub[0]->op() == kRegexpEmptyMatch)
      return re;
    sub[0]->Decref();
    sub[0] = NULL;
    if (re->nsub() == 2) {
      // Collapse concatenation to single regexp.
      Regexp* nre = sub[1];
      sub[1] = NULL;
      re->Decref();
      return nre;
    }
    // 3 or more -> 2 or more.
    re->nsub_--;
    memmove(sub, sub + 1, re->nsub_ * sizeof sub[0]);
    return re;
  }
  Regexp::ParseFlags pf = re->parse_flags();
  re->Decref();
  return new Regexp(kRegexpEmptyMatch, pf);
}

// Returns the leading string that re starts with.
// The returned Rune* points into a piece of re,
// so it must not be used after the caller calls re->Decref().
Rune* Regexp::LeadingString(Regexp* re, int *nrune,
                            Regexp::ParseFlags *flags) {
  while (re->op() == kRegexpConcat && re->nsub() > 0)
    re = re->sub()[0];

  *flags = static_cast<Regexp::ParseFlags>(re->parse_flags_ & Regexp::FoldCase);

  if (re->op() == kRegexpLiteral) {
    *nrune = 1;
    return &re->rune_;
  }

  if (re->op() == kRegexpLiteralString) {
    *nrune = re->literal_string_.nrunes_;
    return re->literal_string_.runes_;
  }

  *nrune = 0;
  return NULL;
}

// Removes the first n leading runes from the beginning of re.
// Edits re in place.
void Regexp::RemoveLeadingString(Regexp* re, int n) {
  // Chase down concats to find first string.
  // For regexps generated by parser, nested concats are
  // flattened except when doing so would overflow the 16-bit
  // limit on the size of a concatenation, so we should never
  // see more than two here.
  Regexp* stk[4];
  int d = 0;
  while (re->op() == kRegexpConcat) {
    if (d < arraysize(stk))
      stk[d++] = re;
    re = re->sub()[0];
  }

  // Remove leading string from re.
  if (re->op() == kRegexpLiteral) {
    re->rune_ = 0;
    re->op_ = kRegexpEmptyMatch;
  } else if (re->op() == kRegexpLiteralString) {
    if (n >= re->literal_string_.nrunes_) {
      delete[] re->literal_string_.runes_;
      re->literal_string_.runes_ = NULL;
      re->literal_string_.nrunes_ = 0;
      re->op_ = kRegexpEmptyMatch;
    } else if (n == re->literal_string_.nrunes_ - 1) {
      Rune rune = re->literal_string_.runes_[re->literal_string_.nrunes_ - 1];
      delete[] re->literal_string_.runes_;
      re->literal_string_.runes_ = NULL;
      re->literal_string_.nrunes_ = 0;
      re->rune_ = rune;
      re->op_ = kRegexpLiteral;
    } else {
      re->literal_string_.nrunes_ -= n;
      memmove(re->literal_string_.runes_, re->literal_string_.runes_ + n, re->literal_string_.nrunes_ * sizeof re->literal_string_.runes_[0]);
    }
  }

  // If re is now empty, concatenations might simplify too.
  while (d-- > 0) {
    re = stk[d];
    Regexp** sub = re->sub();
    if (sub[0]->op() == kRegexpEmptyMatch) {
      sub[0]->Decref();
      sub[0] = NULL;
      // Delete first element of concat.
      switch (re->nsub()) {
        case 0:
        case 1:
          // Impossible.
          LOG(DFATAL) << "Concat of " << re->nsub();
          re->submany_ = NULL;
          re->op_ = kRegexpEmptyMatch;
          break;

        case 2: {
          // Replace re with sub[1].
          Regexp* old = sub[1];
          sub[1] = NULL;
          re->Swap(old);
          old->Decref();
          break;
        }

        default:
          // Slide down.
          re->nsub_--;
          memmove(sub, sub + 1, re->nsub_ * sizeof sub[0]);
          break;
      }
    }
  }
}

// In the context of factoring alternations, a Splice is: a factored prefix or
// merged character class computed by one iteration of one round of factoring;
// the span of subexpressions of the alternation to be "spliced" (i.e. removed
// and replaced); and, for a factored prefix, the number of suffixes after any
// factoring that might have subsequently been performed on them. For a merged
// character class, there are no suffixes, of course, so the field is ignored.
struct Splice {
  Splice(Regexp* prefix, Regexp** sub, int nsub)
      : prefix(prefix),
        sub(sub),
        nsub(nsub),
        nsuffix(-1) {}

  Regexp* prefix;
  Regexp** sub;
  int nsub;
  int nsuffix;
};

// Named so because it is used to implement an explicit stack, a Frame is: the
// span of subexpressions of the alternation to be factored; the current round
// of factoring; any Splices computed; and, for a factored prefix, an iterator
// to the next Splice to be factored (i.e. in another Frame) because suffixes.
struct Frame {
  Frame(Regexp** sub, int nsub)
      : sub(sub),
        nsub(nsub),
        round(0) {}

  Regexp** sub;
  int nsub;
  int round;
  std::vector<Splice> splices;
  int spliceidx;
};

// Bundled into a class for friend access to Regexp without needing to declare
// (or define) Splice in regexp.h.
class FactorAlternationImpl {
 public:
  static void Round1(Regexp** sub, int nsub,
                     Regexp::ParseFlags flags,
                     std::vector<Splice>* splices);
  static void Round2(Regexp** sub, int nsub,
                     Regexp::ParseFlags flags,
                     std::vector<Splice>* splices);
  static void Round3(Regexp** sub, int nsub,
                     Regexp::ParseFlags flags,
                     std::vector<Splice>* splices);
};

// Factors common prefixes from alternation.
// For example,
//     ABC|ABD|AEF|BCX|BCY
// simplifies to
//     A(B(C|D)|EF)|BC(X|Y)
// and thence to
//     A(B[CD]|EF)|BC[XY]
//
// Rewrites sub to contain simplified list to alternate and returns
// the new length of sub.  Adjusts reference counts accordingly
// (incoming sub[i] decremented, outgoing sub[i] incremented).
int Regexp::FactorAlternation(Regexp** sub, int nsub, ParseFlags flags) {
  std::vector<Frame> stk;
  stk.emplace_back(sub, nsub);

  for (;;) {
    auto& sub = stk.back().sub;
    auto& nsub = stk.back().nsub;
    auto& round = stk.back().round;
    auto& splices = stk.back().splices;
    auto& spliceidx = stk.back().spliceidx;

    if (splices.empty()) {
      // Advance to the next round of factoring. Note that this covers
      // the initialised state: when splices is empty and round is 0.
      round++;
    } else if (spliceidx < static_cast<int>(splices.size())) {
      // We have at least one more Splice to factor. Recurse logically.
      stk.emplace_back(splices[spliceidx].sub, splices[spliceidx].nsub);
      continue;
    } else {
      // We have no more Splices to factor. Apply them.
      auto iter = splices.begin();
      int out = 0;
      for (int i = 0; i < nsub; ) {
        // Copy until we reach where the next Splice begins.
        while (sub + i < iter->sub)
          sub[out++] = sub[i++];
        switch (round) {
          case 1:
          case 2: {
            // Assemble the Splice prefix and the suffixes.
            Regexp* re[2];
            re[0] = iter->prefix;
            re[1] = Regexp::AlternateNoFactor(iter->sub, iter->nsuffix, flags);
            sub[out++] = Regexp::Concat(re, 2, flags);
            i += iter->nsub;
            break;
          }
          case 3:
            // Just use the Splice prefix.
            sub[out++] = iter->prefix;
            i += iter->nsub;
            break;
          default:
            LOG(DFATAL) << "unknown round: " << round;
            break;
        }
        // If we are done, copy until the end of sub.
        if (++iter == splices.end()) {
          while (i < nsub)
            sub[out++] = sub[i++];
        }
      }
      splices.clear();
      nsub = out;
      // Advance to the next round of factoring.
      round++;
    }

    switch (round) {
      case 1:
        FactorAlternationImpl::Round1(sub, nsub, flags, &splices);
        break;
      case 2:
        FactorAlternationImpl::Round2(sub, nsub, flags, &splices);
        break;
      case 3:
        FactorAlternationImpl::Round3(sub, nsub, flags, &splices);
        break;
      case 4:
        if (stk.size() == 1) {
          // We are at the top of the stack. Just return.
          return nsub;
        } else {
          // Pop the stack and set the number of suffixes.
          // (Note that references will be invalidated!)
          int nsuffix = nsub;
          stk.pop_back();
          stk.back().splices[stk.back().spliceidx].nsuffix = nsuffix;
          ++stk.back().spliceidx;
          continue;
        }
      default:
        LOG(DFATAL) << "unknown round: " << round;
        break;
    }

    // Set spliceidx depending on whether we have Splices to factor.
    if (splices.empty() || round == 3) {
      spliceidx = static_cast<int>(splices.size());
    } else {
      spliceidx = 0;
    }
  }
}

void FactorAlternationImpl::Round1(Regexp** sub, int nsub,
                                   Regexp::ParseFlags flags,
                                   std::vector<Splice>* splices) {
  // Round 1: Factor out common literal prefixes.
  int start = 0;
  Rune* rune = NULL;
  int nrune = 0;
  Regexp::ParseFlags runeflags = Regexp::NoParseFlags;
  for (int i = 0; i <= nsub; i++) {
    // Invariant: sub[start:i] consists of regexps that all
    // begin with rune[0:nrune].
    Rune* rune_i = NULL;
    int nrune_i = 0;
    Regexp::ParseFlags runeflags_i = Regexp::NoParseFlags;
    if (i < nsub) {
      rune_i = Regexp::LeadingString(sub[i], &nrune_i, &runeflags_i);
      if (runeflags_i == runeflags) {
        int same = 0;
        while (same < nrune && same < nrune_i && rune[same] == rune_i[same])
          same++;
        if (same > 0) {
          // Matches at least one rune in current range.  Keep going around.
          nrune = same;
          continue;
        }
      }
    }

    // Found end of a run with common leading literal string:
    // sub[start:i] all begin with rune[0:nrune],
    // but sub[i] does not even begin with rune[0].
    if (i == start) {
      // Nothing to do - first iteration.
    } else if (i == start+1) {
      // Just one: don't bother factoring.
    } else {
      Regexp* prefix = Regexp::LiteralString(rune, nrune, runeflags);
      for (int j = start; j < i; j++)
        Regexp::RemoveLeadingString(sub[j], nrune);
      splices->emplace_back(prefix, sub + start, i - start);
    }

    // Prepare for next iteration (if there is one).
    if (i < nsub) {
      start = i;
      rune = rune_i;
      nrune = nrune_i;
      runeflags = runeflags_i;
    }
  }
}

void FactorAlternationImpl::Round2(Regexp** sub, int nsub,
                                   Regexp::ParseFlags flags,
                                   std::vector<Splice>* splices) {
  // Round 2: Factor out common simple prefixes,
  // just the first piece of each concatenation.
  // This will be good enough a lot of the time.
  //
  // Complex subexpressions (e.g. involving quantifiers)
  // are not safe to factor because that collapses their
  // distinct paths through the automaton, which affects
  // correctness in some cases.
  int start = 0;
  Regexp* first = NULL;
  for (int i = 0; i <= nsub; i++) {
    // Invariant: sub[start:i] consists of regexps that all
    // begin with first.
    Regexp* first_i = NULL;
    if (i < nsub) {
      first_i = Regexp::LeadingRegexp(sub[i]);
      if (first != NULL &&
          // first must be an empty-width op
          // OR a char class, any char or any byte
          // OR a fixed repeat of a literal, char class, any char or any byte.
          (first->op() == kRegexpBeginLine ||
           first->op() == kRegexpEndLine ||
           first->op() == kRegexpWordBoundary ||
           first->op() == kRegexpNoWordBoundary ||
           first->op() == kRegexpBeginText ||
           first->op() == kRegexpEndText ||
           first->op() == kRegexpCharClass ||
           first->op() == kRegexpAnyChar ||
           first->op() == kRegexpAnyByte ||
           (first->op() == kRegexpRepeat &&
            first->min() == first->max() &&
            (first->sub()[0]->op() == kRegexpLiteral ||
             first->sub()[0]->op() == kRegexpCharClass ||
             first->sub()[0]->op() == kRegexpAnyChar ||
             first->sub()[0]->op() == kRegexpAnyByte))) &&
          Regexp::Equal(first, first_i))
        continue;
    }

    // Found end of a run with common leading regexp:
    // sub[start:i] all begin with first,
    // but sub[i] does not.
    if (i == start) {
      // Nothing to do - first iteration.
    } else if (i == start+1) {
      // Just one: don't bother factoring.
    } else {
      Regexp* prefix = first->Incref();
      for (int j = start; j < i; j++)
        sub[j] = Regexp::RemoveLeadingRegexp(sub[j]);
      splices->emplace_back(prefix, sub + start, i - start);
    }

    // Prepare for next iteration (if there is one).
    if (i < nsub) {
      start = i;
      first = first_i;
    }
  }
}

void FactorAlternationImpl::Round3(Regexp** sub, int nsub,
                                   Regexp::ParseFlags flags,
                                   std::vector<Splice>* splices) {
  // Round 3: Merge runs of literals and/or character classes.
  int start = 0;
  Regexp* first = NULL;
  for (int i = 0; i <= nsub; i++) {
    // Invariant: sub[start:i] consists of regexps that all
    // are either literals (i.e. runes) or character classes.
    Regexp* first_i = NULL;
    if (i < nsub) {
      first_i = sub[i];
      if (first != NULL &&
          (first->op() == kRegexpLiteral ||
           first->op() == kRegexpCharClass) &&
          (first_i->op() == kRegexpLiteral ||
           first_i->op() == kRegexpCharClass))
        continue;
    }

    // Found end of a run of Literal/CharClass:
    // sub[start:i] all are either one or the other,
    // but sub[i] is not.
    if (i == start) {
      // Nothing to do - first iteration.
    } else if (i == start+1) {
      // Just one: don't bother factoring.
    } else {
      CharClassBuilder ccb;
      for (int j = start; j < i; j++) {
        Regexp* re = sub[j];
        if (re->op() == kRegexpCharClass) {
          CharClass* cc = re->cc();
          for (CharClass::iterator it = cc->begin(); it != cc->end(); ++it)
            ccb.AddRange(it->lo, it->hi);
        } else if (re->op() == kRegexpLiteral) {
          ccb.AddRangeFlags(re->rune(), re->rune(), re->parse_flags());
        } else {
          LOG(DFATAL) << "RE2: unexpected op: " << re->op() << " "
                      << re->ToString();
        }
        re->Decref();
      }
      Regexp* re = Regexp::NewCharClass(ccb.GetCharClass(), flags);
      splices->emplace_back(re, sub + start, i - start);
    }

    // Prepare for next iteration (if there is one).
    if (i < nsub) {
      start = i;
      first = first_i;
    }
  }
}

// Collapse the regexps on top of the stack, down to the
// first marker, into a new op node (op == kRegexpAlternate
// or op == kRegexpConcat).
void Regexp::ParseState::DoCollapse(RegexpOp op) {
  // Scan backward to marker, counting children of composite.
  int n = 0;
  Regexp* next = NULL;
  Regexp* sub;
  for (sub = stacktop_; sub != NULL && !IsMarker(sub->op()); sub = next) {
    next = sub->down_;
    if (sub->op_ == op)
      n += sub->nsub_;
    else
      n++;
  }

  // If there's just one child, leave it alone.
  // (Concat of one thing is that one thing; alternate of one thing is same.)
  if (stacktop_ != NULL && stacktop_->down_ == next)
    return;

  // Construct op (alternation or concatenation), flattening op of op.
  PODArray<Regexp*> subs(n);
  next = NULL;
  int i = n;
  for (sub = stacktop_; sub != NULL && !IsMarker(sub->op()); sub = next) {
    next = sub->down_;
    if (sub->op_ == op) {
      Regexp** sub_subs = sub->sub();
      for (int k = sub->nsub_ - 1; k >= 0; k--)
        subs[--i] = sub_subs[k]->Incref();
      sub->Decref();
    } else {
      subs[--i] = FinishRegexp(sub);
    }
  }

  Regexp* re = ConcatOrAlternate(op, subs.data(), n, flags_, true);
  re->simple_ = re->ComputeSimple();
  re->down_ = next;
  stacktop_ = re;
}

// Finishes the current concatenation,
// collapsing it into a single regexp on the stack.
void Regexp::ParseState::DoConcatenation() {
  Regexp* r1 = stacktop_;
  if (r1 == NULL || IsMarker(r1->op())) {
    // empty concatenation is special case
    Regexp* re = new Regexp(kRegexpEmptyMatch, flags_);
    PushRegexp(re);
  }
  DoCollapse(kRegexpConcat);
}

// Finishes the current alternation,
// collapsing it to a single regexp on the stack.
void Regexp::ParseState::DoAlternation() {
  DoVerticalBar();
  // Now stack top is kVerticalBar.
  Regexp* r1 = stacktop_;
  stacktop_ = r1->down_;
  r1->Decref();
  DoCollapse(kRegexpAlternate);
}

// Incremental conversion of concatenated literals into strings.
// If top two elements on stack are both literal or string,
// collapse into single string.
// Don't walk down the stack -- the parser calls this frequently
// enough that below the bottom two is known to be collapsed.
// Only called when another regexp is about to be pushed
// on the stack, so that the topmost literal is not being considered.
// (Otherwise ab* would turn into (ab)*.)
// If r >= 0, consider pushing a literal r on the stack.
// Return whether that happened.
bool Regexp::ParseState::MaybeConcatString(int r, ParseFlags flags) {
  Regexp* re1;
  Regexp* re2;
  if ((re1 = stacktop_) == NULL || (re2 = re1->down_) == NULL)
    return false;

  if (re1->op_ != kRegexpLiteral && re1->op_ != kRegexpLiteralString)
    return false;
  if (re2->op_ != kRegexpLiteral && re2->op_ != kRegexpLiteralString)
    return false;
  if ((re1->parse_flags_ & FoldCase) != (re2->parse_flags_ & FoldCase))
    return false;

  if (re2->op_ == kRegexpLiteral) {
    // convert into string
    Rune rune = re2->rune_;
    re2->op_ = kRegexpLiteralString;
    re2->literal_string_.nrunes_ = 0;
    re2->literal_string_.runes_ = NULL;
    re2->AddRuneToString(rune);
  }

  // push re1 into re2.
  if (re1->op_ == kRegexpLiteral) {
    re2->AddRuneToString(re1->rune_);
  } else {
    for (int i = 0; i < re1->literal_string_.nrunes_; i++)
      re2->AddRuneToString(re1->literal_string_.runes_[i]);
    re1->literal_string_.nrunes_ = 0;
    delete[] re1->literal_string_.runes_;
    re1->literal_string_.runes_ = NULL;
  }

  // reuse re1 if possible
  if (r >= 0) {
    re1->op_ = kRegexpLiteral;
    re1->rune_ = r;
    re1->parse_flags_ = static_cast<uint16_t>(flags);
    return true;
  }

  stacktop_ = re2;
  re1->Decref();
  return false;
}

// Lexing routines.

// Parses a decimal integer, storing it in *np.
// Sets *s to span the remainder of the string.
static bool ParseInteger(StringPiece* s, int* np) {
  if (s->size() == 0 || !isdigit((*s)[0] & 0xFF))
    return false;
  // Disallow leading zeros.
  if (s->size() >= 2 && (*s)[0] == '0' && isdigit((*s)[1] & 0xFF))
    return false;
  int n = 0;
  int c;
  while (s->size() > 0 && isdigit(c = (*s)[0] & 0xFF)) {
    // Avoid overflow.
    if (n >= 100000000)
      return false;
    n = n*10 + c - '0';
    s->remove_prefix(1);  // digit
  }
  *np = n;
  return true;
}

// Parses a repetition suffix like {1,2} or {2} or {2,}.
// Sets *s to span the remainder of the string on success.
// Sets *lo and *hi to the given range.
// In the case of {2,}, the high number is unbounded;
// sets *hi to -1 to signify this.
// {,2} is NOT a valid suffix.
// The Maybe in the name signifies that the regexp parse
// doesn't fail even if ParseRepetition does, so the StringPiece
// s must NOT be edited unless MaybeParseRepetition returns true.
static bool MaybeParseRepetition(StringPiece* sp, int* lo, int* hi) {
  StringPiece s = *sp;
  if (s.size() == 0 || s[0] != '{')
    return false;
  s.remove_prefix(1);  // '{'
  if (!ParseInteger(&s, lo))
    return false;
  if (s.size() == 0)
    return false;
  if (s[0] == ',') {
    s.remove_prefix(1);  // ','
    if (s.size() == 0)
      return false;
    if (s[0] == '}') {
      // {2,} means at least 2
      *hi = -1;
    } else {
      // {2,4} means 2, 3, or 4.
      if (!ParseInteger(&s, hi))
        return false;
    }
  } else {
    // {2} means exactly two
    *hi = *lo;
  }
  if (s.size() == 0 || s[0] != '}')
    return false;
  s.remove_prefix(1);  // '}'
  *sp = s;
  return true;
}

// Removes the next Rune from the StringPiece and stores it in *r.
// Returns number of bytes removed from sp.
// Behaves as though there is a terminating NUL at the end of sp.
// Argument order is backwards from usual Google style
// but consistent with chartorune.
static int StringPieceToRune(Rune *r, StringPiece *sp, RegexpStatus* status) {
  // fullrune() takes int, not size_t. However, it just looks
  // at the leading byte and treats any length >= 4 the same.
  if (fullrune(sp->data(), static_cast<int>(std::min(size_t{4}, sp->size())))) {
    int n = chartorune(r, sp->data());
    // Some copies of chartorune have a bug that accepts
    // encodings of values in (10FFFF, 1FFFFF] as valid.
    // Those values break the character class algorithm,
    // which assumes Runemax is the largest rune.
    if (*r > Runemax) {
      n = 1;
      *r = Runeerror;
    }
    if (!(n == 1 && *r == Runeerror)) {  // no decoding error
      sp->remove_prefix(n);
      return n;
    }
  }

  status->set_code(kRegexpBadUTF8);
  status->set_error_arg(StringPiece());
  return -1;
}

// Return whether name is valid UTF-8.
// If not, set status to kRegexpBadUTF8.
static bool IsValidUTF8(const StringPiece& s, RegexpStatus* status) {
  StringPiece t = s;
  Rune r;
  while (t.size() > 0) {
    if (StringPieceToRune(&r, &t, status) < 0)
      return false;
  }
  return true;
}

// Is c a hex digit?
static int IsHex(int c) {
  return ('0' <= c && c <= '9') ||
         ('A' <= c && c <= 'F') ||
         ('a' <= c && c <= 'f');
}

// Convert hex digit to value.
static int UnHex(int c) {
  if ('0' <= c && c <= '9')
    return c - '0';
  if ('A' <= c && c <= 'F')
    return c - 'A' + 10;
  if ('a' <= c && c <= 'f')
    return c - 'a' + 10;
  LOG(DFATAL) << "Bad hex digit " << c;
  return 0;
}

// Parse an escape sequence (e.g., \n, \{).
// Sets *s to span the remainder of the string.
// Sets *rp to the named character.
static bool ParseEscape(StringPiece* s, Rune* rp,
                        RegexpStatus* status, int rune_max) {
  const char* begin = s->begin();
  if (s->size() < 1 || (*s)[0] != '\\') {
    // Should not happen - caller always checks.
    status->set_code(kRegexpInternalError);
    status->set_error_arg(StringPiece());
    return false;
  }
  if (s->size() < 2) {
    status->set_code(kRegexpTrailingBackslash);
    status->set_error_arg(StringPiece());
    return false;
  }
  Rune c, c1;
  s->remove_prefix(1);  // backslash
  if (StringPieceToRune(&c, s, status) < 0)
    return false;
  int code;
  switch (c) {
    default:
      if (c < Runeself && !isalpha(c) && !isdigit(c)) {
        // Escaped non-word characters are always themselves.
        // PCRE is not quite so rigorous: it accepts things like
        // \q, but we don't.  We once rejected \_, but too many
        // programs and people insist on using it, so allow \_.
        *rp = c;
        return true;
      }
      goto BadEscape;

    // Octal escapes.
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
      // Single non-zero octal digit is a backreference; not supported.
      if (s->size() == 0 || (*s)[0] < '0' || (*s)[0] > '7')
        goto BadEscape;
      FALLTHROUGH_INTENDED;
    case '0':
      // consume up to three octal digits; already have one.
      code = c - '0';
      if (s->size() > 0 && '0' <= (c = (*s)[0]) && c <= '7') {
        code = code * 8 + c - '0';
        s->remove_prefix(1);  // digit
        if (s->size() > 0) {
          c = (*s)[0];
          if ('0' <= c && c <= '7') {
            code = code * 8 + c - '0';
            s->remove_prefix(1);  // digit
          }
        }
      }
      if (code > rune_max)
        goto BadEscape;
      *rp = code;
      return true;

    // Hexadecimal escapes
    case 'x':
      if (s->size() == 0)
        goto BadEscape;
      if (StringPieceToRune(&c, s, status) < 0)
        return false;
      if (c == '{') {
        // Any number of digits in braces.
        // Update n as we consume the string, so that
        // the whole thing gets shown in the error message.
        // Perl accepts any text at all; it ignores all text
        // after the first non-hex digit.  We require only hex digits,
        // and at least one.
        if (StringPieceToRune(&c, s, status) < 0)
          return false;
        int nhex = 0;
        code = 0;
        while (IsHex(c)) {
          nhex++;
          code = code * 16 + UnHex(c);
          if (code > rune_max)
            goto BadEscape;
          if (s->size() == 0)
            goto BadEscape;
          if (StringPieceToRune(&c, s, status) < 0)
            return false;
        }
        if (c != '}' || nhex == 0)
          goto BadEscape;
        *rp = code;
        return true;
      }
      // Easy case: two hex digits.
      if (s->size() == 0)
        goto BadEscape;
      if (StringPieceToRune(&c1, s, status) < 0)
        return false;
      if (!IsHex(c) || !IsHex(c1))
        goto BadEscape;
      *rp = UnHex(c) * 16 + UnHex(c1);
      return true;

    // C escapes.
    case 'n':
      *rp = '\n';
      return true;
    case 'r':
      *rp = '\r';
      return true;
    case 't':
      *rp = '\t';
      return true;

    // Less common C escapes.
    case 'a':
      *rp = '\a';
      return true;
    case 'f':
      *rp = '\f';
      return true;
    case 'v':
      *rp = '\v';
      return true;

    // This code is disabled to avoid misparsing
    // the Perl word-boundary \b as a backspace
    // when in POSIX regexp mode.  Surprisingly,
    // in Perl, \b means word-boundary but [\b]
    // means backspace.  We don't support that:
    // if you want a backspace embed a literal
    // backspace character or use \x08.
    //
    // case 'b':
    //   *rp = '\b';
    //   return true;
  }

  LOG(DFATAL) << "Not reached in ParseEscape.";

BadEscape:
  // Unrecognized escape sequence.
  status->set_code(kRegexpBadEscape);
  status->set_error_arg(
      StringPiece(begin, static_cast<size_t>(s->begin() - begin)));
  return false;
}

// Add a range to the character class, but exclude newline if asked.
// Also handle case folding.
void CharClassBuilder::AddRangeFlags(
    Rune lo, Rune hi, Regexp::ParseFlags parse_flags) {

  // Take out \n if the flags say so.
  bool cutnl = !(parse_flags & Regexp::ClassNL) ||
               (parse_flags & Regexp::NeverNL);
  if (cutnl && lo <= '\n' && '\n' <= hi) {
    if (lo < '\n')
      AddRangeFlags(lo, '\n' - 1, parse_flags);
    if (hi > '\n')
      AddRangeFlags('\n' + 1, hi, parse_flags);
    return;
  }

  // If folding case, add fold-equivalent characters too.
  if (parse_flags & Regexp::FoldCase)
    AddFoldedRange(this, lo, hi, 0);
  else
    AddRange(lo, hi);
}

// Look for a group with the given name.
static const UGroup* LookupGroup(const StringPiece& name,
                                 const UGroup *groups, int ngroups) {
  // Simple name lookup.
  for (int i = 0; i < ngroups; i++)
    if (StringPiece(groups[i].name) == name)
      return &groups[i];
  return NULL;
}

// Look for a POSIX group with the given name (e.g., "[:^alpha:]")
static const UGroup* LookupPosixGroup(const StringPiece& name) {
  return LookupGroup(name, posix_groups, num_posix_groups);
}

static const UGroup* LookupPerlGroup(const StringPiece& name) {
  return LookupGroup(name, perl_groups, num_perl_groups);
}

#if !defined(RE2_USE_ICU)
// Fake UGroup containing all Runes
static URange16 any16[] = { { 0, 65535 } };
static URange32 any32[] = { { 65536, Runemax } };
static UGroup anygroup = { "Any", +1, any16, 1, any32, 1 };

// Look for a Unicode group with the given name (e.g., "Han")
static const UGroup* LookupUnicodeGroup(const StringPiece& name) {
  // Special case: "Any" means any.
  if (name == StringPiece("Any"))
    return &anygroup;
  return LookupGroup(name, unicode_groups, num_unicode_groups);
}
#endif

// Add a UGroup or its negation to the character class.
static void AddUGroup(CharClassBuilder *cc, const UGroup *g, int sign,
                      Regexp::ParseFlags parse_flags) {
  if (sign == +1) {
    for (int i = 0; i < g->nr16; i++) {
      cc->AddRangeFlags(g->r16[i].lo, g->r16[i].hi, parse_flags);
    }
    for (int i = 0; i < g->nr32; i++) {
      cc->AddRangeFlags(g->r32[i].lo, g->r32[i].hi, parse_flags);
    }
  } else {
    if (parse_flags & Regexp::FoldCase) {
      // Normally adding a case-folded group means
      // adding all the extra fold-equivalent runes too.
      // But if we're adding the negation of the group,
      // we have to exclude all the runes that are fold-equivalent
      // to what's already missing.  Too hard, so do in two steps.
      CharClassBuilder ccb1;
      AddUGroup(&ccb1, g, +1, parse_flags);
      // If the flags say to take out \n, put it in, so that negating will take it out.
      // Normally AddRangeFlags does this, but we're bypassing AddRangeFlags.
      bool cutnl = !(parse_flags & Regexp::ClassNL) ||
                   (parse_flags & Regexp::NeverNL);
      if (cutnl) {
        ccb1.AddRange('\n', '\n');
      }
      ccb1.Negate();
      cc->AddCharClass(&ccb1);
      return;
    }
    int next = 0;
    for (int i = 0; i < g->nr16; i++) {
      if (next < g->r16[i].lo)
        cc->AddRangeFlags(next, g->r16[i].lo - 1, parse_flags);
      next = g->r16[i].hi + 1;
    }
    for (int i = 0; i < g->nr32; i++) {
      if (next < g->r32[i].lo)
        cc->AddRangeFlags(next, g->r32[i].lo - 1, parse_flags);
      next = g->r32[i].hi + 1;
    }
    if (next <= Runemax)
      cc->AddRangeFlags(next, Runemax, parse_flags);
  }
}

// Maybe parse a Perl character class escape sequence.
// Only recognizes the Perl character classes (\d \s \w \D \S \W),
// not the Perl empty-string classes (\b \B \A \Z \z).
// On success, sets *s to span the remainder of the string
// and returns the corresponding UGroup.
// The StringPiece must *NOT* be edited unless the call succeeds.
const UGroup* MaybeParsePerlCCEscape(StringPiece* s, Regexp::ParseFlags parse_flags) {
  if (!(parse_flags & Regexp::PerlClasses))
    return NULL;
  if (s->size() < 2 || (*s)[0] != '\\')
    return NULL;
  // Could use StringPieceToRune, but there aren't
  // any non-ASCII Perl group names.
  StringPiece name(s->begin(), 2);
  const UGroup *g = LookupPerlGroup(name);
  if (g == NULL)
    return NULL;
  s->remove_prefix(name.size());
  return g;
}

enum ParseStatus {
  kParseOk,  // Did some parsing.
  kParseError,  // Found an error.
  kParseNothing,  // Decided not to parse.
};

// Maybe parses a Unicode character group like \p{Han} or \P{Han}
// (the latter is a negated group).
ParseStatus ParseUnicodeGroup(StringPiece* s, Regexp::ParseFlags parse_flags,
                              CharClassBuilder *cc,
                              RegexpStatus* status) {
  // Decide whether to parse.
  if (!(parse_flags & Regexp::UnicodeGroups))
    return kParseNothing;
  if (s->size() < 2 || (*s)[0] != '\\')
    return kParseNothing;
  Rune c = (*s)[1];
  if (c != 'p' && c != 'P')
    return kParseNothing;

  // Committed to parse.  Results:
  int sign = +1;  // -1 = negated char class
  if (c == 'P')
    sign = -sign;
  StringPiece seq = *s;  // \p{Han} or \pL
  StringPiece name;  // Han or L
  s->remove_prefix(2);  // '\\', 'p'

  if (!StringPieceToRune(&c, s, status))
    return kParseError;
  if (c != '{') {
    // Name is the bit of string we just skipped over for c.
    const char* p = seq.begin() + 2;
    name = StringPiece(p, static_cast<size_t>(s->begin() - p));
  } else {
    // Name is in braces. Look for closing }
    size_t end = s->find('}', 0);
    if (end == StringPiece::npos) {
      if (!IsValidUTF8(seq, status))
        return kParseError;
      status->set_code(kRegexpBadCharRange);
      status->set_error_arg(seq);
      return kParseError;
    }
    name = StringPiece(s->begin(), end);  // without '}'
    s->remove_prefix(end + 1);  // with '}'
    if (!IsValidUTF8(name, status))
      return kParseError;
  }

  // Chop seq where s now begins.
  seq = StringPiece(seq.begin(), static_cast<size_t>(s->begin() - seq.begin()));

  if (name.size() > 0 && name[0] == '^') {
    sign = -sign;
    name.remove_prefix(1);  // '^'
  }

#if !defined(RE2_USE_ICU)
  // Look up the group in the RE2 Unicode data.
  const UGroup *g = LookupUnicodeGroup(name);
  if (g == NULL) {
    status->set_code(kRegexpBadCharRange);
    status->set_error_arg(seq);
    return kParseError;
  }

  AddUGroup(cc, g, sign, parse_flags);
#else
  // Look up the group in the ICU Unicode data. Because ICU provides full
  // Unicode properties support, this could be more than a lookup by name.
  ::icu::UnicodeString ustr = ::icu::UnicodeString::fromUTF8(
      std::string("\\p{") + std::string(name) + std::string("}"));
  UErrorCode uerr = U_ZERO_ERROR;
  ::icu::UnicodeSet uset(ustr, uerr);
  if (U_FAILURE(uerr)) {
    status->set_code(kRegexpBadCharRange);
    status->set_error_arg(seq);
    return kParseError;
  }

  // Convert the UnicodeSet to a URange32 and UGroup that we can add.
  int nr = uset.getRangeCount();
  URange32* r = new URange32[nr];
  for (int i = 0; i < nr; i++) {
    r[i].lo = uset.getRangeStart(i);
    r[i].hi = uset.getRangeEnd(i);
  }
  UGroup g = {"", +1, 0, 0, r, nr};
  AddUGroup(cc, &g, sign, parse_flags);
  delete[] r;
#endif

  return kParseOk;
}

// Parses a character class name like [:alnum:].
// Sets *s to span the remainder of the string.
// Adds the ranges corresponding to the class to ranges.
static ParseStatus ParseCCName(StringPiece* s, Regexp::ParseFlags parse_flags,
                               CharClassBuilder *cc,
                               RegexpStatus* status) {
  // Check begins with [:
  const char* p = s->data();
  const char* ep = s->data() + s->size();
  if (ep - p < 2 || p[0] != '[' || p[1] != ':')
    return kParseNothing;

  // Look for closing :].
  const char* q;
  for (q = p+2; q <= ep-2 && (*q != ':' || *(q+1) != ']'); q++)
    ;

  // If no closing :], then ignore.
  if (q > ep-2)
    return kParseNothing;

  // Got it.  Check that it's valid.
  q += 2;
  StringPiece name(p, static_cast<size_t>(q - p));

  const UGroup *g = LookupPosixGroup(name);
  if (g == NULL) {
    status->set_code(kRegexpBadCharRange);
    status->set_error_arg(name);
    return kParseError;
  }

  s->remove_prefix(name.size());
  AddUGroup(cc, g, g->sign, parse_flags);
  return kParseOk;
}

// Parses a character inside a character class.
// There are fewer special characters here than in the rest of the regexp.
// Sets *s to span the remainder of the string.
// Sets *rp to the character.
bool Regexp::ParseState::ParseCCCharacter(StringPiece* s, Rune *rp,
                                          const StringPiece& whole_class,
                                          RegexpStatus* status) {
  if (s->size() == 0) {
    status->set_code(kRegexpMissingBracket);
    status->set_error_arg(whole_class);
    return false;
  }

  // Allow regular escape sequences even though
  // many need not be escaped in this context.
  if (s->size() >= 1 && (*s)[0] == '\\')
    return ParseEscape(s, rp, status, rune_max_);

  // Otherwise take the next rune.
  return StringPieceToRune(rp, s, status) >= 0;
}

// Parses a character class character, or, if the character
// is followed by a hyphen, parses a character class range.
// For single characters, rr->lo == rr->hi.
// Sets *s to span the remainder of the string.
// Sets *rp to the character.
bool Regexp::ParseState::ParseCCRange(StringPiece* s, RuneRange* rr,
                                      const StringPiece& whole_class,
                                      RegexpStatus* status) {
  StringPiece os = *s;
  if (!ParseCCCharacter(s, &rr->lo, whole_class, status))
    return false;
  // [a-] means (a|-), so check for final ].
  if (s->size() >= 2 && (*s)[0] == '-' && (*s)[1] != ']') {
    s->remove_prefix(1);  // '-'
    if (!ParseCCCharacter(s, &rr->hi, whole_class, status))
      return false;
    if (rr->hi < rr->lo) {
      status->set_code(kRegexpBadCharRange);
      status->set_error_arg(
          StringPiece(os.data(), static_cast<size_t>(s->data() - os.data())));
      return false;
    }
  } else {
    rr->hi = rr->lo;
  }
  return true;
}

// Parses a possibly-negated character class expression like [^abx-z[:digit:]].
// Sets *s to span the remainder of the string.
// Sets *out_re to the regexp for the class.
bool Regexp::ParseState::ParseCharClass(StringPiece* s,
                                        Regexp** out_re,
                                        RegexpStatus* status) {
  StringPiece whole_class = *s;
  if (s->size() == 0 || (*s)[0] != '[') {
    // Caller checked this.
    status->set_code(kRegexpInternalError);
    status->set_error_arg(StringPiece());
    return false;
  }
  bool negated = false;
  Regexp* re = new Regexp(kRegexpCharClass, flags_ & ~FoldCase);
  re->char_class_.ccb_ = new CharClassBuilder;
  s->remove_prefix(1);  // '['
  if (s->size() > 0 && (*s)[0] == '^') {
    s->remove_prefix(1);  // '^'
    negated = true;
    if (!(flags_ & ClassNL) || (flags_ & NeverNL)) {
      // If NL can't match implicitly, then pretend
      // negated classes include a leading \n.
      re->char_class_.ccb_->AddRange('\n', '\n');
    }
  }
  bool first = true;  // ] is okay as first char in class
  while (s->size() > 0 && ((*s)[0] != ']' || first)) {
    // - is only okay unescaped as first or last in class.
    // Except that Perl allows - anywhere.
    if ((*s)[0] == '-' && !first && !(flags_&PerlX) &&
        (s->size() == 1 || (*s)[1] != ']')) {
      StringPiece t = *s;
      t.remove_prefix(1);  // '-'
      Rune r;
      int n = StringPieceToRune(&r, &t, status);
      if (n < 0) {
        re->Decref();
        return false;
      }
      status->set_code(kRegexpBadCharRange);
      status->set_error_arg(StringPiece(s->data(), 1+n));
      re->Decref();
      return false;
    }
    first = false;

    // Look for [:alnum:] etc.
    if (s->size() > 2 && (*s)[0] == '[' && (*s)[1] == ':') {
      switch (ParseCCName(s, flags_, re->char_class_.ccb_, status)) {
        case kParseOk:
          continue;
        case kParseError:
          re->Decref();
          return false;
        case kParseNothing:
          break;
      }
    }

    // Look for Unicode character group like \p{Han}
    if (s->size() > 2 &&
        (*s)[0] == '\\' &&
        ((*s)[1] == 'p' || (*s)[1] == 'P')) {
      switch (ParseUnicodeGroup(s, flags_, re->char_class_.ccb_, status)) {
        case kParseOk:
          continue;
        case kParseError:
          re->Decref();
          return false;
        case kParseNothing:
          break;
      }
    }

    // Look for Perl character class symbols (extension).
    const UGroup *g = MaybeParsePerlCCEscape(s, flags_);
    if (g != NULL) {
      AddUGroup(re->char_class_.ccb_, g, g->sign, flags_);
      continue;
    }

    // Otherwise assume single character or simple range.
    RuneRange rr;
    if (!ParseCCRange(s, &rr, whole_class, status)) {
      re->Decref();
      return false;
    }
    // AddRangeFlags is usually called in response to a class like
    // \p{Foo} or [[:foo:]]; for those, it filters \n out unless
    // Regexp::ClassNL is set.  In an explicit range or singleton
    // like we just parsed, we do not filter \n out, so set ClassNL
    // in the flags.
    re->char_class_.ccb_->AddRangeFlags(rr.lo, rr.hi, flags_ | Regexp::ClassNL);
  }
  if (s->size() == 0) {
    status->set_code(kRegexpMissingBracket);
    status->set_error_arg(whole_class);
    re->Decref();
    return false;
  }
  s->remove_prefix(1);  // ']'

  if (negated)
    re->char_class_.ccb_->Negate();

  *out_re = re;
  return true;
}

// Is this a valid capture name?  [A-Za-z0-9_]+
// PCRE limits names to 32 bytes.
// Python rejects names starting with digits.
// We don't enforce either of those.
static bool IsValidCaptureName(const StringPiece& name) {
  if (name.size() == 0)
    return false;
  for (size_t i = 0; i < name.size(); i++) {
    int c = name[i];
    if (('0' <= c && c <= '9') ||
        ('a' <= c && c <= 'z') ||
        ('A' <= c && c <= 'Z') ||
        c == '_')
      continue;
    return false;
  }
  return true;
}

// Parses a Perl flag setting or non-capturing group or both,
// like (?i) or (?: or (?i:.  Removes from s, updates parse state.
// The caller must check that s begins with "(?".
// Returns true on success.  If the Perl flag is not
// well-formed or not supported, sets status_ and returns false.
bool Regexp::ParseState::ParsePerlFlags(StringPiece* s) {
  StringPiece t = *s;

  // Caller is supposed to check this.
  if (!(flags_ & PerlX) || t.size() < 2 || t[0] != '(' || t[1] != '?') {
    LOG(DFATAL) << "Bad call to ParseState::ParsePerlFlags";
    status_->set_code(kRegexpInternalError);
    return false;
  }

  t.remove_prefix(2);  // "(?"

  // Check for named captures, first introduced in Python's regexp library.
  // As usual, there are three slightly different syntaxes:
  //
  //   (?P<name>expr)   the original, introduced by Python
  //   (?<name>expr)    the .NET alteration, adopted by Perl 5.10
  //   (?'name'expr)    another .NET alteration, adopted by Perl 5.10
  //
  // Perl 5.10 gave in and implemented the Python version too,
  // but they claim that the last two are the preferred forms.
  // PCRE and languages based on it (specifically, PHP and Ruby)
  // support all three as well.  EcmaScript 4 uses only the Python form.
  //
  // In both the open source world (via Code Search) and the
  // Google source tree, (?P<expr>name) is the dominant form,
  // so that's the one we implement.  One is enough.
  if (t.size() > 2 && t[0] == 'P' && t[1] == '<') {
    // Pull out name.
    size_t end = t.find('>', 2);
    if (end == StringPiece::npos) {
      if (!IsValidUTF8(*s, status_))
        return false;
      status_->set_code(kRegexpBadNamedCapture);
      status_->set_error_arg(*s);
      return false;
    }

    // t is "P<name>...", t[end] == '>'
    StringPiece capture(t.begin()-2, end+3);  // "(?P<name>"
    StringPiece name(t.begin()+2, end-2);     // "name"
    if (!IsValidUTF8(name, status_))
      return false;
    if (!IsValidCaptureName(name)) {
      status_->set_code(kRegexpBadNamedCapture);
      status_->set_error_arg(capture);
      return false;
    }

    if (!DoLeftParen(name)) {
      // DoLeftParen's failure set status_.
      return false;
    }

    s->remove_prefix(static_cast<size_t>(capture.end() - s->begin()));
    return true;
  }

  bool negated = false;
  bool sawflags = false;
  int nflags = flags_;
  Rune c;
  for (bool done = false; !done; ) {
    if (t.size() == 0)
      goto BadPerlOp;
    if (StringPieceToRune(&c, &t, status_) < 0)
      return false;
    switch (c) {
      default:
        goto BadPerlOp;

      // Parse flags.
      case 'i':
        sawflags = true;
        if (negated)
          nflags &= ~FoldCase;
        else
          nflags |= FoldCase;
        break;

      case 'm':  // opposite of our OneLine
        sawflags = true;
        if (negated)
          nflags |= OneLine;
        else
          nflags &= ~OneLine;
        break;

      case 's':
        sawflags = true;
        if (negated)
          nflags &= ~DotNL;
        else
          nflags |= DotNL;
        break;

      case 'U':
        sawflags = true;
        if (negated)
          nflags &= ~NonGreedy;
        else
          nflags |= NonGreedy;
        break;

      // Negation
      case '-':
        if (negated)
          goto BadPerlOp;
        negated = true;
        sawflags = false;
        break;

      // Open new group.
      case ':':
        if (!DoLeftParenNoCapture()) {
          // DoLeftParenNoCapture's failure set status_.
          return false;
        }
        done = true;
        break;

      // Finish flags.
      case ')':
        done = true;
        break;
    }
  }

  if (negated && !sawflags)
    goto BadPerlOp;

  flags_ = static_cast<Regexp::ParseFlags>(nflags);
  *s = t;
  return true;

BadPerlOp:
  status_->set_code(kRegexpBadPerlOp);
  status_->set_error_arg(
      StringPiece(s->begin(), static_cast<size_t>(t.begin() - s->begin())));
  return false;
}

// Converts latin1 (assumed to be encoded as Latin1 bytes)
// into UTF8 encoding in string.
// Can't use EncodingUtils::EncodeLatin1AsUTF8 because it is
// deprecated and because it rejects code points 0x80-0x9F.
void ConvertLatin1ToUTF8(const StringPiece& latin1, std::string* utf) {
  char buf[UTFmax];

  utf->clear();
  for (size_t i = 0; i < latin1.size(); i++) {
    Rune r = latin1[i] & 0xFF;
    int n = runetochar(buf, &r);
    utf->append(buf, n);
  }
}

// Parses the regular expression given by s,
// returning the corresponding Regexp tree.
// The caller must Decref the return value when done with it.
// Returns NULL on error.
Regexp* Regexp::Parse(const StringPiece& s, ParseFlags global_flags,
                      RegexpStatus* status) {
  // Make status non-NULL (easier on everyone else).
  RegexpStatus xstatus;
  if (status == NULL)
    status = &xstatus;

  ParseState ps(global_flags, s, status);
  StringPiece t = s;

  // Convert regexp to UTF-8 (easier on the rest of the parser).
  if (global_flags & Latin1) {
    std::string* tmp = new std::string;
    ConvertLatin1ToUTF8(t, tmp);
    status->set_tmp(tmp);
    t = *tmp;
  }

  if (global_flags & Literal) {
    // Special parse loop for literal string.
    while (t.size() > 0) {
      Rune r;
      if (StringPieceToRune(&r, &t, status) < 0)
        return NULL;
      if (!ps.PushLiteral(r))
        return NULL;
    }
    return ps.DoFinish();
  }

  StringPiece lastunary = StringPiece();
  while (t.size() > 0) {
    StringPiece isunary = StringPiece();
    switch (t[0]) {
      default: {
        Rune r;
        if (StringPieceToRune(&r, &t, status) < 0)
          return NULL;
        if (!ps.PushLiteral(r))
          return NULL;
        break;
      }

      case '(':
        // "(?" introduces Perl escape.
        if ((ps.flags() & PerlX) && (t.size() >= 2 && t[1] == '?')) {
          // Flag changes and non-capturing groups.
          if (!ps.ParsePerlFlags(&t))
            return NULL;
          break;
        }
        if (ps.flags() & NeverCapture) {
          if (!ps.DoLeftParenNoCapture())
            return NULL;
        } else {
          if (!ps.DoLeftParen(StringPiece()))
            return NULL;
        }
        t.remove_prefix(1);  // '('
        break;

      case '|':
        if (!ps.DoVerticalBar())
          return NULL;
        t.remove_prefix(1);  // '|'
        break;

      case ')':
        if (!ps.DoRightParen())
          return NULL;
        t.remove_prefix(1);  // ')'
        break;

      case '^':  // Beginning of line.
        if (!ps.PushCarat())
          return NULL;
        t.remove_prefix(1);  // '^'
        break;

      case '$':  // End of line.
        if (!ps.PushDollar())
          return NULL;
        t.remove_prefix(1);  // '$'
        break;

      case '.':  // Any character (possibly except newline).
        if (!ps.PushDot())
          return NULL;
        t.remove_prefix(1);  // '.'
        break;

      case '[': {  // Character class.
        Regexp* re;
        if (!ps.ParseCharClass(&t, &re, status))
          return NULL;
        if (!ps.PushRegexp(re))
          return NULL;
        break;
      }

      case '*': {  // Zero or more.
        RegexpOp op;
        op = kRegexpStar;
        goto Rep;
      case '+':  // One or more.
        op = kRegexpPlus;
        goto Rep;
      case '?':  // Zero or one.
        op = kRegexpQuest;
        goto Rep;
      Rep:
        StringPiece opstr = t;
        bool nongreedy = false;
        t.remove_prefix(1);  // '*' or '+' or '?'
        if (ps.flags() & PerlX) {
          if (t.size() > 0 && t[0] == '?') {
            nongreedy = true;
            t.remove_prefix(1);  // '?'
          }
          if (lastunary.size() > 0) {
            // In Perl it is not allowed to stack repetition operators:
            //   a** is a syntax error, not a double-star.
            // (and a++ means something else entirely, which we don't support!)
            status->set_code(kRegexpRepeatOp);
            status->set_error_arg(StringPiece(
                lastunary.begin(),
                static_cast<size_t>(t.begin() - lastunary.begin())));
            return NULL;
          }
        }
        opstr = StringPiece(opstr.data(),
                            static_cast<size_t>(t.data() - opstr.data()));
        if (!ps.PushRepeatOp(op, opstr, nongreedy))
          return NULL;
        isunary = opstr;
        break;
      }

      case '{': {  // Counted repetition.
        int lo, hi;
        StringPiece opstr = t;
        if (!MaybeParseRepetition(&t, &lo, &hi)) {
          // Treat like a literal.
          if (!ps.PushLiteral('{'))
            return NULL;
          t.remove_prefix(1);  // '{'
          break;
        }
        bool nongreedy = false;
        if (ps.flags() & PerlX) {
          if (t.size() > 0 && t[0] == '?') {
            nongreedy = true;
            t.remove_prefix(1);  // '?'
          }
          if (lastunary.size() > 0) {
            // Not allowed to stack repetition operators.
            status->set_code(kRegexpRepeatOp);
            status->set_error_arg(StringPiece(
                lastunary.begin(),
                static_cast<size_t>(t.begin() - lastunary.begin())));
            return NULL;
          }
        }
        opstr = StringPiece(opstr.data(),
                            static_cast<size_t>(t.data() - opstr.data()));
        if (!ps.PushRepetition(lo, hi, opstr, nongreedy))
          return NULL;
        isunary = opstr;
        break;
      }

      case '\\': {  // Escaped character or Perl sequence.
        // \b and \B: word boundary or not
        if ((ps.flags() & Regexp::PerlB) &&
            t.size() >= 2 && (t[1] == 'b' || t[1] == 'B')) {
          if (!ps.PushWordBoundary(t[1] == 'b'))
            return NULL;
          t.remove_prefix(2);  // '\\', 'b'
          break;
        }

        if ((ps.flags() & Regexp::PerlX) && t.size() >= 2) {
          if (t[1] == 'A') {
            if (!ps.PushSimpleOp(kRegexpBeginText))
              return NULL;
            t.remove_prefix(2);  // '\\', 'A'
            break;
          }
          if (t[1] == 'z') {
            if (!ps.PushSimpleOp(kRegexpEndText))
              return NULL;
            t.remove_prefix(2);  // '\\', 'z'
            break;
          }
          // Do not recognize \Z, because this library can't
          // implement the exact Perl/PCRE semantics.
          // (This library treats "(?-m)$" as \z, even though
          // in Perl and PCRE it is equivalent to \Z.)

          if (t[1] == 'C') {  // \C: any byte [sic]
            if (!ps.PushSimpleOp(kRegexpAnyByte))
              return NULL;
            t.remove_prefix(2);  // '\\', 'C'
            break;
          }

          if (t[1] == 'Q') {  // \Q ... \E: the ... is always literals
            t.remove_prefix(2);  // '\\', 'Q'
            while (t.size() > 0) {
              if (t.size() >= 2 && t[0] == '\\' && t[1] == 'E') {
                t.remove_prefix(2);  // '\\', 'E'
                break;
              }
              Rune r;
              if (StringPieceToRune(&r, &t, status) < 0)
                return NULL;
              if (!ps.PushLiteral(r))
                return NULL;
            }
            break;
          }
        }

        if (t.size() >= 2 && (t[1] == 'p' || t[1] == 'P')) {
          Regexp* re = new Regexp(kRegexpCharClass, ps.flags() & ~FoldCase);
          re->char_class_.ccb_ = new CharClassBuilder;
          switch (ParseUnicodeGroup(&t, ps.flags(), re->char_class_.ccb_, status)) {
            case kParseOk:
              if (!ps.PushRegexp(re))
                return NULL;
              goto Break2;
            case kParseError:
              re->Decref();
              return NULL;
            case kParseNothing:
              re->Decref();
              break;
          }
        }

        const UGroup *g = MaybeParsePerlCCEscape(&t, ps.flags());
        if (g != NULL) {
          Regexp* re = new Regexp(kRegexpCharClass, ps.flags() & ~FoldCase);
          re->char_class_.ccb_ = new CharClassBuilder;
          AddUGroup(re->char_class_.ccb_, g, g->sign, ps.flags());
          if (!ps.PushRegexp(re))
            return NULL;
          break;
        }

        Rune r;
        if (!ParseEscape(&t, &r, status, ps.rune_max()))
          return NULL;
        if (!ps.PushLiteral(r))
          return NULL;
        break;
      }
    }
  Break2:
    lastunary = isunary;
  }
  return ps.DoFinish();
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// GENERATED BY make_perl_groups.pl; DO NOT EDIT.
// make_perl_groups.pl >perl_groups.cc



namespace duckdb_re2 {

static const URange16 code1[] = {  /* \d */
	{ 0x30, 0x39 },
};
static const URange16 code2[] = {  /* \s */
	{ 0x9, 0xa },
	{ 0xc, 0xd },
	{ 0x20, 0x20 },
};
static const URange16 code3[] = {  /* \w */
	{ 0x30, 0x39 },
	{ 0x41, 0x5a },
	{ 0x5f, 0x5f },
	{ 0x61, 0x7a },
};
const UGroup perl_groups[] = {
	{ "\\d", +1, code1, 1 , nullptr, 0},
	{ "\\D", -1, code1, 1 , nullptr, 0},
	{ "\\s", +1, code2, 3 , nullptr, 0},
	{ "\\S", -1, code2, 3 , nullptr, 0},
	{ "\\w", +1, code3, 4 , nullptr, 0},
	{ "\\W", -1, code3, 4 , nullptr, 0},
};
const int num_perl_groups = 6;
static const URange16 code4[] = {  /* [:alnum:] */
	{ 0x30, 0x39 },
	{ 0x41, 0x5a },
	{ 0x61, 0x7a },
};
static const URange16 code5[] = {  /* [:alpha:] */
	{ 0x41, 0x5a },
	{ 0x61, 0x7a },
};
static const URange16 code6[] = {  /* [:ascii:] */
	{ 0x0, 0x7f },
};
static const URange16 code7[] = {  /* [:blank:] */
	{ 0x9, 0x9 },
	{ 0x20, 0x20 },
};
static const URange16 code8[] = {  /* [:cntrl:] */
	{ 0x0, 0x1f },
	{ 0x7f, 0x7f },
};
static const URange16 code9[] = {  /* [:digit:] */
	{ 0x30, 0x39 },
};
static const URange16 code10[] = {  /* [:graph:] */
	{ 0x21, 0x7e },
};
static const URange16 code11[] = {  /* [:lower:] */
	{ 0x61, 0x7a },
};
static const URange16 code12[] = {  /* [:print:] */
	{ 0x20, 0x7e },
};
static const URange16 code13[] = {  /* [:punct:] */
	{ 0x21, 0x2f },
	{ 0x3a, 0x40 },
	{ 0x5b, 0x60 },
	{ 0x7b, 0x7e },
};
static const URange16 code14[] = {  /* [:space:] */
	{ 0x9, 0xd },
	{ 0x20, 0x20 },
};
static const URange16 code15[] = {  /* [:upper:] */
	{ 0x41, 0x5a },
};
static const URange16 code16[] = {  /* [:word:] */
	{ 0x30, 0x39 },
	{ 0x41, 0x5a },
	{ 0x5f, 0x5f },
	{ 0x61, 0x7a },
};
static const URange16 code17[] = {  /* [:xdigit:] */
	{ 0x30, 0x39 },
	{ 0x41, 0x46 },
	{ 0x61, 0x66 },
};
const UGroup posix_groups[] = {
	{ "[:alnum:]", +1, code4, 3 , nullptr, 0},
	{ "[:^alnum:]", -1, code4, 3 , nullptr, 0},
	{ "[:alpha:]", +1, code5, 2 , nullptr, 0},
	{ "[:^alpha:]", -1, code5, 2 , nullptr, 0},
	{ "[:ascii:]", +1, code6, 1 , nullptr, 0},
	{ "[:^ascii:]", -1, code6, 1 , nullptr, 0},
	{ "[:blank:]", +1, code7, 2 , nullptr, 0},
	{ "[:^blank:]", -1, code7, 2 , nullptr, 0},
	{ "[:cntrl:]", +1, code8, 2 , nullptr, 0},
	{ "[:^cntrl:]", -1, code8, 2 , nullptr, 0},
	{ "[:digit:]", +1, code9, 1 , nullptr, 0},
	{ "[:^digit:]", -1, code9, 1 , nullptr, 0},
	{ "[:graph:]", +1, code10, 1 , nullptr, 0},
	{ "[:^graph:]", -1, code10, 1 , nullptr, 0},
	{ "[:lower:]", +1, code11, 1 , nullptr, 0},
	{ "[:^lower:]", -1, code11, 1 , nullptr, 0},
	{ "[:print:]", +1, code12, 1 , nullptr, 0},
	{ "[:^print:]", -1, code12, 1 , nullptr, 0},
	{ "[:punct:]", +1, code13, 4 , nullptr, 0},
	{ "[:^punct:]", -1, code13, 4 , nullptr, 0},
	{ "[:space:]", +1, code14, 2 , nullptr, 0},
	{ "[:^space:]", -1, code14, 2 , nullptr, 0},
	{ "[:upper:]", +1, code15, 1 , nullptr, 0},
	{ "[:^upper:]", -1, code15, 1, nullptr, 0},
	{ "[:word:]", +1, code16, 4 , nullptr, 0},
	{ "[:^word:]", -1, code16, 4 , nullptr, 0},
	{ "[:xdigit:]", +1, code17, 3 , nullptr, 0},
	{ "[:^xdigit:]", -1, code17, 3 , nullptr, 0},
};
const int num_posix_groups = 28;

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2009 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.



#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>









namespace duckdb_re2 {

typedef std::set<std::string>::iterator SSIter;
typedef std::set<std::string>::const_iterator ConstSSIter;

// Initializes a Prefilter, allocating subs_ as necessary.
Prefilter::Prefilter(Op op) {
  op_ = op;
  subs_ = NULL;
  if (op_ == AND || op_ == OR)
    subs_ = new std::vector<Prefilter*>;
}

// Destroys a Prefilter.
Prefilter::~Prefilter() {
  if (subs_) {
    for (size_t i = 0; i < subs_->size(); i++)
      delete (*subs_)[i];
    delete subs_;
    subs_ = NULL;
  }
}

// Simplify if the node is an empty Or or And.
Prefilter* Prefilter::Simplify() {
  if (op_ != AND && op_ != OR) {
    return this;
  }

  // Nothing left in the AND/OR.
  if (subs_->empty()) {
    if (op_ == AND)
      op_ = ALL;  // AND of nothing is true
    else
      op_ = NONE;  // OR of nothing is false

    return this;
  }

  // Just one subnode: throw away wrapper.
  if (subs_->size() == 1) {
    Prefilter* a = (*subs_)[0];
    subs_->clear();
    delete this;
    return a->Simplify();
  }

  return this;
}

// Combines two Prefilters together to create an "op" (AND or OR).
// The passed Prefilters will be part of the returned Prefilter or deleted.
// Does lots of work to avoid creating unnecessarily complicated structures.
Prefilter* Prefilter::AndOr(Op op, Prefilter* a, Prefilter* b) {
  // If a, b can be rewritten as op, do so.
  a = a->Simplify();
  b = b->Simplify();

  // Canonicalize: a->op <= b->op.
  if (a->op() > b->op()) {
    Prefilter* t = a;
    a = b;
    b = t;
  }

  // Trivial cases.
  //    ALL AND b = b
  //    NONE OR b = b
  //    ALL OR b   = ALL
  //    NONE AND b = NONE
  // Don't need to look at b, because of canonicalization above.
  // ALL and NONE are smallest opcodes.
  if (a->op() == ALL || a->op() == NONE) {
    if ((a->op() == ALL && op == AND) ||
        (a->op() == NONE && op == OR)) {
      delete a;
      return b;
    } else {
      delete b;
      return a;
    }
  }

  // If a and b match op, merge their contents.
  if (a->op() == op && b->op() == op) {
    for (size_t i = 0; i < b->subs()->size(); i++) {
      Prefilter* bb = (*b->subs())[i];
      a->subs()->push_back(bb);
    }
    b->subs()->clear();
    delete b;
    return a;
  }

  // If a already has the same op as the op that is under construction
  // add in b (similarly if b already has the same op, add in a).
  if (b->op() == op) {
    Prefilter* t = a;
    a = b;
    b = t;
  }
  if (a->op() == op) {
    a->subs()->push_back(b);
    return a;
  }

  // Otherwise just return the op.
  Prefilter* c = new Prefilter(op);
  c->subs()->push_back(a);
  c->subs()->push_back(b);
  return c;
}

Prefilter* Prefilter::And(Prefilter* a, Prefilter* b) {
  return AndOr(AND, a, b);
}

Prefilter* Prefilter::Or(Prefilter* a, Prefilter* b) {
  return AndOr(OR, a, b);
}

static void SimplifyStringSet(std::set<std::string> *ss) {
  // Now make sure that the strings aren't redundant.  For example, if
  // we know "ab" is a required string, then it doesn't help at all to
  // know that "abc" is also a required string, so delete "abc". This
  // is because, when we are performing a string search to filter
  // regexps, matching ab will already allow this regexp to be a
  // candidate for match, so further matching abc is redundant.

  for (SSIter i = ss->begin(); i != ss->end(); ++i) {
    SSIter j = i;
    ++j;
    while (j != ss->end()) {
      // Increment j early so that we can erase the element it points to.
      SSIter old_j = j;
      ++j;
      if (old_j->find(*i) != std::string::npos)
        ss->erase(old_j);
    }
  }
}

Prefilter* Prefilter::OrStrings(std::set<std::string>* ss) {
  SimplifyStringSet(ss);
  Prefilter* or_prefilter = NULL;
  if (!ss->empty()) {
    or_prefilter = new Prefilter(NONE);
    for (SSIter i = ss->begin(); i != ss->end(); ++i)
      or_prefilter = Or(or_prefilter, FromString(*i));
  }
  return or_prefilter;
}

static Rune ToLowerRune(Rune r) {
  if (r < Runeself) {
    if ('A' <= r && r <= 'Z')
      r += 'a' - 'A';
    return r;
  }

  const CaseFold *f = LookupCaseFold(unicode_tolower, num_unicode_tolower, r);
  if (f == NULL || r < f->lo)
    return r;
  return ApplyFold(f, r);
}

static Rune ToLowerRuneLatin1(Rune r) {
  if ('A' <= r && r <= 'Z')
    r += 'a' - 'A';
  return r;
}

Prefilter* Prefilter::FromString(const std::string& str) {
  Prefilter* m = new Prefilter(Prefilter::ATOM);
  m->atom_ = str;
  return m;
}

// Information about a regexp used during computation of Prefilter.
// Can be thought of as information about the set of strings matching
// the given regular expression.
class Prefilter::Info {
 public:
  Info();
  ~Info();

  // More constructors.  They delete their Info* arguments.
  static Info* Alt(Info* a, Info* b);
  static Info* Concat(Info* a, Info* b);
  static Info* And(Info* a, Info* b);
  static Info* Star(Info* a);
  static Info* Plus(Info* a);
  static Info* Quest(Info* a);
  static Info* EmptyString();
  static Info* NoMatch();
  static Info* AnyCharOrAnyByte();
  static Info* CClass(CharClass* cc, bool latin1);
  static Info* Literal(Rune r);
  static Info* LiteralLatin1(Rune r);
  static Info* AnyMatch();

  // Format Info as a string.
  std::string ToString();

  // Caller takes ownership of the Prefilter.
  Prefilter* TakeMatch();

  std::set<std::string>& exact() { return exact_; }

  bool is_exact() const { return is_exact_; }

  class Walker;

 private:
  std::set<std::string> exact_;

  // When is_exact_ is true, the strings that match
  // are placed in exact_. When it is no longer an exact
  // set of strings that match this RE, then is_exact_
  // is false and the match_ contains the required match
  // criteria.
  bool is_exact_;

  // Accumulated Prefilter query that any
  // match for this regexp is guaranteed to match.
  Prefilter* match_;
};


Prefilter::Info::Info()
  : is_exact_(false),
    match_(NULL) {
}

Prefilter::Info::~Info() {
  delete match_;
}

Prefilter* Prefilter::Info::TakeMatch() {
  if (is_exact_) {
    match_ = Prefilter::OrStrings(&exact_);
    is_exact_ = false;
  }
  Prefilter* m = match_;
  match_ = NULL;
  return m;
}

// Format a Info in string form.
std::string Prefilter::Info::ToString() {
  if (is_exact_) {
    int n = 0;
    std::string s;
    for (SSIter i = exact_.begin(); i != exact_.end(); ++i) {
      if (n++ > 0)
        s += ",";
      s += *i;
    }
    return s;
  }

  if (match_)
    return match_->DebugString();

  return "";
}

// Add the strings from src to dst.
static void CopyIn(const std::set<std::string>& src,
                   std::set<std::string>* dst) {
  for (ConstSSIter i = src.begin(); i != src.end(); ++i)
    dst->insert(*i);
}

// Add the cross-product of a and b to dst.
// (For each string i in a and j in b, add i+j.)
static void CrossProduct(const std::set<std::string>& a,
                         const std::set<std::string>& b,
                         std::set<std::string>* dst) {
  for (ConstSSIter i = a.begin(); i != a.end(); ++i)
    for (ConstSSIter j = b.begin(); j != b.end(); ++j)
      dst->insert(*i + *j);
}

// Concats a and b. Requires that both are exact sets.
// Forms an exact set that is a crossproduct of a and b.
Prefilter::Info* Prefilter::Info::Concat(Info* a, Info* b) {
  if (a == NULL)
    return b;
  DCHECK(a->is_exact_);
  DCHECK(b && b->is_exact_);
  Info *ab = new Info();

  CrossProduct(a->exact_, b->exact_, &ab->exact_);
  ab->is_exact_ = true;

  delete a;
  delete b;
  return ab;
}

// Constructs an inexact Info for ab given a and b.
// Used only when a or b is not exact or when the
// exact cross product is likely to be too big.
Prefilter::Info* Prefilter::Info::And(Info* a, Info* b) {
  if (a == NULL)
    return b;
  if (b == NULL)
    return a;

  Info *ab = new Info();

  ab->match_ = Prefilter::And(a->TakeMatch(), b->TakeMatch());
  ab->is_exact_ = false;
  delete a;
  delete b;
  return ab;
}

// Constructs Info for a|b given a and b.
Prefilter::Info* Prefilter::Info::Alt(Info* a, Info* b) {
  Info *ab = new Info();

  if (a->is_exact_ && b->is_exact_) {
    CopyIn(a->exact_, &ab->exact_);
    CopyIn(b->exact_, &ab->exact_);
    ab->is_exact_ = true;
  } else {
    // Either a or b has is_exact_ = false. If the other
    // one has is_exact_ = true, we move it to match_ and
    // then create a OR of a,b. The resulting Info has
    // is_exact_ = false.
    ab->match_ = Prefilter::Or(a->TakeMatch(), b->TakeMatch());
    ab->is_exact_ = false;
  }

  delete a;
  delete b;
  return ab;
}

// Constructs Info for a? given a.
Prefilter::Info* Prefilter::Info::Quest(Info *a) {
  Info *ab = new Info();

  ab->is_exact_ = false;
  ab->match_ = new Prefilter(ALL);
  delete a;
  return ab;
}

// Constructs Info for a* given a.
// Same as a? -- not much to do.
Prefilter::Info* Prefilter::Info::Star(Info *a) {
  return Quest(a);
}

// Constructs Info for a+ given a. If a was exact set, it isn't
// anymore.
Prefilter::Info* Prefilter::Info::Plus(Info *a) {
  Info *ab = new Info();

  ab->match_ = a->TakeMatch();
  ab->is_exact_ = false;

  delete a;
  return ab;
}

static std::string RuneToString(Rune r) {
  char buf[UTFmax];
  int n = runetochar(buf, &r);
  return std::string(buf, n);
}

static std::string RuneToStringLatin1(Rune r) {
  char c = r & 0xff;
  return std::string(&c, 1);
}

// Constructs Info for literal rune.
Prefilter::Info* Prefilter::Info::Literal(Rune r) {
  Info* info = new Info();
  info->exact_.insert(RuneToString(ToLowerRune(r)));
  info->is_exact_ = true;
  return info;
}

// Constructs Info for literal rune for Latin1 encoded string.
Prefilter::Info* Prefilter::Info::LiteralLatin1(Rune r) {
  Info* info = new Info();
  info->exact_.insert(RuneToStringLatin1(ToLowerRuneLatin1(r)));
  info->is_exact_ = true;
  return info;
}

// Constructs Info for dot (any character) or \C (any byte).
Prefilter::Info* Prefilter::Info::AnyCharOrAnyByte() {
  Prefilter::Info* info = new Prefilter::Info();
  info->match_ = new Prefilter(ALL);
  return info;
}

// Constructs Prefilter::Info for no possible match.
Prefilter::Info* Prefilter::Info::NoMatch() {
  Prefilter::Info* info = new Prefilter::Info();
  info->match_ = new Prefilter(NONE);
  return info;
}

// Constructs Prefilter::Info for any possible match.
// This Prefilter::Info is valid for any regular expression,
// since it makes no assertions whatsoever about the
// strings being matched.
Prefilter::Info* Prefilter::Info::AnyMatch() {
  Prefilter::Info *info = new Prefilter::Info();
  info->match_ = new Prefilter(ALL);
  return info;
}

// Constructs Prefilter::Info for just the empty string.
Prefilter::Info* Prefilter::Info::EmptyString() {
  Prefilter::Info* info = new Prefilter::Info();
  info->is_exact_ = true;
  info->exact_.insert("");
  return info;
}

// Constructs Prefilter::Info for a character class.
typedef CharClass::iterator CCIter;
Prefilter::Info* Prefilter::Info::CClass(CharClass *cc,
                                         bool latin1) {
  // If the class is too large, it's okay to overestimate.
  if (cc->size() > 10)
    return AnyCharOrAnyByte();

  Prefilter::Info *a = new Prefilter::Info();
  for (CCIter i = cc->begin(); i != cc->end(); ++i)
    for (Rune r = i->lo; r <= i->hi; r++) {
      if (latin1) {
        a->exact_.insert(RuneToStringLatin1(ToLowerRuneLatin1(r)));
      } else {
        a->exact_.insert(RuneToString(ToLowerRune(r)));
      }
    }


  a->is_exact_ = true;

  return a;
}

class Prefilter::Info::Walker : public Regexp::Walker<Prefilter::Info*> {
 public:
  Walker(bool latin1) : latin1_(latin1) {}

  virtual Info* PostVisit(
      Regexp* re, Info* parent_arg,
      Info* pre_arg,
      Info** child_args, int nchild_args);

  virtual Info* ShortVisit(
      Regexp* re,
      Info* parent_arg);

  bool latin1() { return latin1_; }
 private:
  bool latin1_;

  Walker(const Walker&) = delete;
  Walker& operator=(const Walker&) = delete;
};

Prefilter::Info* Prefilter::BuildInfo(Regexp* re) {
  bool latin1 = (re->parse_flags() & Regexp::Latin1) != 0;
  Prefilter::Info::Walker w(latin1);
  Prefilter::Info* info = w.WalkExponential(re, NULL, 100000);

  if (w.stopped_early()) {
    delete info;
    return NULL;
  }

  return info;
}

Prefilter::Info* Prefilter::Info::Walker::ShortVisit(
    Regexp* re, Prefilter::Info* parent_arg) {
  return AnyMatch();
}

// Constructs the Prefilter::Info for the given regular expression.
// Assumes re is simplified.
Prefilter::Info* Prefilter::Info::Walker::PostVisit(
    Regexp* re, Prefilter::Info* parent_arg,
    Prefilter::Info* pre_arg, Prefilter::Info** child_args,
    int nchild_args) {
  Prefilter::Info *info;
  switch (re->op()) {
    default:
    case kRegexpRepeat:
      LOG(DFATAL) << "Bad regexp op " << re->op();
      info = EmptyString();
      break;

    case kRegexpNoMatch:
      info = NoMatch();
      break;

    // These ops match the empty string:
    case kRegexpEmptyMatch:      // anywhere
    case kRegexpBeginLine:       // at beginning of line
    case kRegexpEndLine:         // at end of line
    case kRegexpBeginText:       // at beginning of text
    case kRegexpEndText:         // at end of text
    case kRegexpWordBoundary:    // at word boundary
    case kRegexpNoWordBoundary:  // not at word boundary
      info = EmptyString();
      break;

    case kRegexpLiteral:
      if (latin1()) {
        info = LiteralLatin1(re->rune());
      }
      else {
        info = Literal(re->rune());
      }
      break;

    case kRegexpLiteralString:
      if (re->nrunes() == 0) {
        info = NoMatch();
        break;
      }
      if (latin1()) {
        info = LiteralLatin1(re->runes()[0]);
        for (int i = 1; i < re->nrunes(); i++) {
          info = Concat(info, LiteralLatin1(re->runes()[i]));
        }
      } else {
        info = Literal(re->runes()[0]);
        for (int i = 1; i < re->nrunes(); i++) {
          info = Concat(info, Literal(re->runes()[i]));
        }
      }
      break;

    case kRegexpConcat: {
      // Accumulate in info.
      // Exact is concat of recent contiguous exact nodes.
      info = NULL;
      Info* exact = NULL;
      for (int i = 0; i < nchild_args; i++) {
        Info* ci = child_args[i];  // child info
        if (!ci->is_exact() ||
            (exact && ci->exact().size() * exact->exact().size() > 16)) {
          // Exact run is over.
          info = And(info, exact);
          exact = NULL;
          // Add this child's info.
          info = And(info, ci);
        } else {
          // Append to exact run.
          exact = Concat(exact, ci);
        }
      }
      info = And(info, exact);
    }
      break;

    case kRegexpAlternate:
      info = child_args[0];
      for (int i = 1; i < nchild_args; i++)
        info = Alt(info, child_args[i]);
      break;

    case kRegexpStar:
      info = Star(child_args[0]);
      break;

    case kRegexpQuest:
      info = Quest(child_args[0]);
      break;

    case kRegexpPlus:
      info = Plus(child_args[0]);
      break;

    case kRegexpAnyChar:
    case kRegexpAnyByte:
      // Claim nothing, except that it's not empty.
      info = AnyCharOrAnyByte();
      break;

    case kRegexpCharClass:
      info = CClass(re->cc(), latin1());
      break;

    case kRegexpCapture:
      // These don't affect the set of matching strings.
      info = child_args[0];
      break;
  }

  return info;
}


Prefilter* Prefilter::FromRegexp(Regexp* re) {
  if (re == NULL)
    return NULL;

  Regexp* simple = re->Simplify();
  Prefilter::Info *info = BuildInfo(simple);

  simple->Decref();
  if (info == NULL)
    return NULL;

  Prefilter* m = info->TakeMatch();

  delete info;
  return m;
}

std::string Prefilter::DebugString() const {
  switch (op_) {
    default:
      LOG(DFATAL) << "Bad op in Prefilter::DebugString: " << op_;
      return StringPrintf("op%d", op_);
    case NONE:
      return "*no-matches*";
    case ATOM:
      return atom_;
    case ALL:
      return "";
    case AND: {
      std::string s = "";
      for (size_t i = 0; i < subs_->size(); i++) {
        if (i > 0)
          s += " ";
        Prefilter* sub = (*subs_)[i];
        s += sub ? sub->DebugString() : "<nil>";
      }
      return s;
    }
    case OR: {
      std::string s = "(";
      for (size_t i = 0; i < subs_->size(); i++) {
        if (i > 0)
          s += "|";
        Prefilter* sub = (*subs_)[i];
        s += sub ? sub->DebugString() : "<nil>";
      }
      s += ")";
      return s;
    }
  }
}

Prefilter* Prefilter::FromRE2(const RE2* re2) {
  if (re2 == NULL)
    return NULL;

  Regexp* regexp = re2->Regexp();
  if (regexp == NULL)
    return NULL;

  return FromRegexp(regexp);
}


}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2009 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.



#include <stddef.h>
#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>







namespace duckdb_re2 {

PrefilterTree::PrefilterTree()
    : compiled_(false),
      min_atom_len_(3) {
}

PrefilterTree::PrefilterTree(int min_atom_len)
    : compiled_(false),
      min_atom_len_(min_atom_len) {
}

PrefilterTree::~PrefilterTree() {
  for (size_t i = 0; i < prefilter_vec_.size(); i++)
    delete prefilter_vec_[i];

  for (size_t i = 0; i < entries_.size(); i++)
    delete entries_[i].parents;
}

void PrefilterTree::Add(Prefilter* prefilter) {
  if (compiled_) {
    LOG(DFATAL) << "Add called after Compile.";
    return;
  }
  if (prefilter != NULL && !KeepNode(prefilter)) {
    delete prefilter;
    prefilter = NULL;
  }

  prefilter_vec_.push_back(prefilter);
}

void PrefilterTree::Compile(std::vector<std::string>* atom_vec) {
  if (compiled_) {
    LOG(DFATAL) << "Compile called already.";
    return;
  }

  // Some legacy users of PrefilterTree call Compile() before
  // adding any regexps and expect Compile() to have no effect.
  if (prefilter_vec_.empty())
    return;

  compiled_ = true;

  // TODO(junyer): Use std::unordered_set<Prefilter*> instead?
  NodeMap nodes;
  AssignUniqueIds(&nodes, atom_vec);

  // Identify nodes that are too common among prefilters and are
  // triggering too many parents. Then get rid of them if possible.
  // Note that getting rid of a prefilter node simply means they are
  // no longer necessary for their parent to trigger; that is, we do
  // not miss out on any regexps triggering by getting rid of a
  // prefilter node.
  for (size_t i = 0; i < entries_.size(); i++) {
    StdIntMap* parents = entries_[i].parents;
    if (parents->size() > 8) {
      // This one triggers too many things. If all the parents are AND
      // nodes and have other things guarding them, then get rid of
      // this trigger. TODO(vsri): Adjust the threshold appropriately,
      // make it a function of total number of nodes?
      bool have_other_guard = true;
      for (StdIntMap::iterator it = parents->begin();
           it != parents->end(); ++it) {
        have_other_guard = have_other_guard &&
            (entries_[it->first].propagate_up_at_count > 1);
      }

      if (have_other_guard) {
        for (StdIntMap::iterator it = parents->begin();
             it != parents->end(); ++it)
          entries_[it->first].propagate_up_at_count -= 1;

        parents->clear();  // Forget the parents
      }
    }
  }
}

Prefilter* PrefilterTree::CanonicalNode(NodeMap* nodes, Prefilter* node) {
  std::string node_string = NodeString(node);
  std::map<std::string, Prefilter*>::iterator iter = nodes->find(node_string);
  if (iter == nodes->end())
    return NULL;
  return (*iter).second;
}

std::string PrefilterTree::NodeString(Prefilter* node) const {
  // Adding the operation disambiguates AND/OR/atom nodes.
  std::string s = StringPrintf("%d", node->op()) + ":";
  if (node->op() == Prefilter::ATOM) {
    s += node->atom();
  } else {
    for (size_t i = 0; i < node->subs()->size(); i++) {
      if (i > 0)
        s += ',';
      s += StringPrintf("%d", (*node->subs())[i]->unique_id());
    }
  }
  return s;
}

bool PrefilterTree::KeepNode(Prefilter* node) const {
  if (node == NULL)
    return false;

  switch (node->op()) {
    default:
      LOG(DFATAL) << "Unexpected op in KeepNode: " << node->op();
      return false;

    case Prefilter::ALL:
    case Prefilter::NONE:
      return false;

    case Prefilter::ATOM:
      return node->atom().size() >= static_cast<size_t>(min_atom_len_);

    case Prefilter::AND: {
      int j = 0;
      std::vector<Prefilter*>* subs = node->subs();
      for (size_t i = 0; i < subs->size(); i++)
        if (KeepNode((*subs)[i]))
          (*subs)[j++] = (*subs)[i];
        else
          delete (*subs)[i];

      subs->resize(j);
      return j > 0;
    }

    case Prefilter::OR:
      for (size_t i = 0; i < node->subs()->size(); i++)
        if (!KeepNode((*node->subs())[i]))
          return false;
      return true;
  }
}

void PrefilterTree::AssignUniqueIds(NodeMap* nodes,
                                    std::vector<std::string>* atom_vec) {
  atom_vec->clear();

  // Build vector of all filter nodes, sorted topologically
  // from top to bottom in v.
  std::vector<Prefilter*> v;

  // Add the top level nodes of each regexp prefilter.
  for (size_t i = 0; i < prefilter_vec_.size(); i++) {
    Prefilter* f = prefilter_vec_[i];
    if (f == NULL)
      unfiltered_.push_back(static_cast<int>(i));

    // We push NULL also on to v, so that we maintain the
    // mapping of index==regexpid for level=0 prefilter nodes.
    v.push_back(f);
  }

  // Now add all the descendant nodes.
  for (size_t i = 0; i < v.size(); i++) {
    Prefilter* f = v[i];
    if (f == NULL)
      continue;
    if (f->op() == Prefilter::AND || f->op() == Prefilter::OR) {
      const std::vector<Prefilter*>& subs = *f->subs();
      for (size_t j = 0; j < subs.size(); j++)
        v.push_back(subs[j]);
    }
  }

  // Identify unique nodes.
  int unique_id = 0;
  for (int i = static_cast<int>(v.size()) - 1; i >= 0; i--) {
    Prefilter *node = v[i];
    if (node == NULL)
      continue;
    node->set_unique_id(-1);
    Prefilter* canonical = CanonicalNode(nodes, node);
    if (canonical == NULL) {
      // Any further nodes that have the same node string
      // will find this node as the canonical node.
      nodes->emplace(NodeString(node), node);
      if (node->op() == Prefilter::ATOM) {
        atom_vec->push_back(node->atom());
        atom_index_to_id_.push_back(unique_id);
      }
      node->set_unique_id(unique_id++);
    } else {
      node->set_unique_id(canonical->unique_id());
    }
  }
  entries_.resize(nodes->size());

  // Create parent StdIntMap for the entries.
  for (int i = static_cast<int>(v.size()) - 1; i >= 0; i--) {
    Prefilter* prefilter = v[i];
    if (prefilter == NULL)
      continue;

    if (CanonicalNode(nodes, prefilter) != prefilter)
      continue;

    Entry* entry = &entries_[prefilter->unique_id()];
    entry->parents = new StdIntMap();
  }

  // Fill the entries.
  for (int i = static_cast<int>(v.size()) - 1; i >= 0; i--) {
    Prefilter* prefilter = v[i];
    if (prefilter == NULL)
      continue;

    if (CanonicalNode(nodes, prefilter) != prefilter)
      continue;

    Entry* entry = &entries_[prefilter->unique_id()];

    switch (prefilter->op()) {
      default:
      case Prefilter::ALL:
        LOG(DFATAL) << "Unexpected op: " << prefilter->op();
        return;

      case Prefilter::ATOM:
        entry->propagate_up_at_count = 1;
        break;

      case Prefilter::OR:
      case Prefilter::AND: {
        std::set<int> uniq_child;
        for (size_t j = 0; j < prefilter->subs()->size(); j++) {
          Prefilter* child = (*prefilter->subs())[j];
          Prefilter* canonical = CanonicalNode(nodes, child);
          if (canonical == NULL) {
            LOG(DFATAL) << "Null canonical node";
            return;
          }
          int child_id = canonical->unique_id();
          uniq_child.insert(child_id);
          // To the child, we want to add to parent indices.
          Entry* child_entry = &entries_[child_id];
          if (child_entry->parents->find(prefilter->unique_id()) ==
              child_entry->parents->end()) {
            (*child_entry->parents)[prefilter->unique_id()] = 1;
          }
        }
        entry->propagate_up_at_count = prefilter->op() == Prefilter::AND
                                           ? static_cast<int>(uniq_child.size())
                                           : 1;

        break;
      }
    }
  }

  // For top level nodes, populate regexp id.
  for (size_t i = 0; i < prefilter_vec_.size(); i++) {
    if (prefilter_vec_[i] == NULL)
      continue;
    int id = CanonicalNode(nodes, prefilter_vec_[i])->unique_id();
    DCHECK_LE(0, id);
    Entry* entry = &entries_[id];
    entry->regexps.push_back(static_cast<int>(i));
  }
}

// Functions for triggering during search.
void PrefilterTree::RegexpsGivenStrings(
    const std::vector<int>& matched_atoms,
    std::vector<int>* regexps) const {
  regexps->clear();
  if (!compiled_) {
    // Some legacy users of PrefilterTree call Compile() before
    // adding any regexps and expect Compile() to have no effect.
    // This kludge is a counterpart to that kludge.
    if (prefilter_vec_.empty())
      return;

    LOG(ERROR) << "RegexpsGivenStrings called before Compile.";
    for (size_t i = 0; i < prefilter_vec_.size(); i++)
      regexps->push_back(static_cast<int>(i));
  } else {
    IntMap regexps_map(static_cast<int>(prefilter_vec_.size()));
    std::vector<int> matched_atom_ids;
    for (size_t j = 0; j < matched_atoms.size(); j++)
      matched_atom_ids.push_back(atom_index_to_id_[matched_atoms[j]]);
    PropagateMatch(matched_atom_ids, &regexps_map);
    for (IntMap::iterator it = regexps_map.begin();
         it != regexps_map.end();
         ++it)
      regexps->push_back(it->index());

    regexps->insert(regexps->end(), unfiltered_.begin(), unfiltered_.end());
  }
  std::sort(regexps->begin(), regexps->end());
}

void PrefilterTree::PropagateMatch(const std::vector<int>& atom_ids,
                                   IntMap* regexps) const {
  IntMap count(static_cast<int>(entries_.size()));
  IntMap work(static_cast<int>(entries_.size()));
  for (size_t i = 0; i < atom_ids.size(); i++)
    work.set(atom_ids[i], 1);
  for (IntMap::iterator it = work.begin(); it != work.end(); ++it) {
    const Entry& entry = entries_[it->index()];
    // Record regexps triggered.
    for (size_t i = 0; i < entry.regexps.size(); i++)
      regexps->set(entry.regexps[i], 1);
    int c;
    // Pass trigger up to parents.
    for (StdIntMap::iterator it = entry.parents->begin();
         it != entry.parents->end();
         ++it) {
      int j = it->first;
      const Entry& parent = entries_[j];
      // Delay until all the children have succeeded.
      if (parent.propagate_up_at_count > 1) {
        if (count.has_index(j)) {
          c = count.get_existing(j) + 1;
          count.set_existing(j, c);
        } else {
          c = 1;
          count.set_new(j, c);
        }
        if (c < parent.propagate_up_at_count)
          continue;
      }
      // Trigger the parent.
      work.set(j, 1);
    }
  }
}

// Debugging help.
void PrefilterTree::PrintPrefilter(int regexpid) {
  LOG(ERROR) << DebugNodeString(prefilter_vec_[regexpid]);
}

void PrefilterTree::PrintDebugInfo(NodeMap* nodes) {
  LOG(ERROR) << "#Unique Atoms: " << atom_index_to_id_.size();
  LOG(ERROR) << "#Unique Nodes: " << entries_.size();

  for (size_t i = 0; i < entries_.size(); i++) {
    StdIntMap* parents = entries_[i].parents;
    const std::vector<int>& regexps = entries_[i].regexps;
    LOG(ERROR) << "EntryId: " << i
               << " N: " << parents->size() << " R: " << regexps.size();
    for (StdIntMap::iterator it = parents->begin(); it != parents->end(); ++it)
      LOG(ERROR) << it->first;
  }
  LOG(ERROR) << "Map:";
  for (std::map<std::string, Prefilter*>::const_iterator iter = nodes->begin();
       iter != nodes->end(); ++iter)
    LOG(ERROR) << "NodeId: " << (*iter).second->unique_id()
               << " Str: " << (*iter).first;
}

std::string PrefilterTree::DebugNodeString(Prefilter* node) const {
  std::string node_string = "";
  if (node->op() == Prefilter::ATOM) {
    DCHECK(!node->atom().empty());
    node_string += node->atom();
  } else {
    // Adding the operation disambiguates AND and OR nodes.
    node_string +=  node->op() == Prefilter::AND ? "AND" : "OR";
    node_string += "(";
    for (size_t i = 0; i < node->subs()->size(); i++) {
      if (i > 0)
        node_string += ',';
      node_string += StringPrintf("%d", (*node->subs())[i]->unique_id());
      node_string += ":";
      node_string += DebugNodeString((*node->subs())[i]);
    }
    node_string += ")";
  }
  return node_string;
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2007 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Compiled regular expression representation.
// Tested by compile_test.cc



#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <memory>
#include <utility>






// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2016 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef RE2_BITMAP256_H_
#define RE2_BITMAP256_H_

#ifdef _MSC_VER
#include <intrin.h>
#endif
#include <stdint.h>
#include <string.h>




namespace duckdb_re2 {

class Bitmap256 {
 public:
  Bitmap256() {
    Clear();
  }

  // Clears all of the bits.
  void Clear() {
    memset(words_, 0, sizeof words_);
  }

  // Tests the bit with index c.
  bool Test(int c) const {
    DCHECK_GE(c, 0);
    DCHECK_LE(c, 255);

    return (words_[c / 64] & (1ULL << (c % 64))) != 0;
  }

  // Sets the bit with index c.
  void Set(int c) {
    DCHECK_GE(c, 0);
    DCHECK_LE(c, 255);

    words_[c / 64] |= (1ULL << (c % 64));
  }

  // Finds the next non-zero bit with index >= c.
  // Returns -1 if no such bit exists.
  int FindNextSetBit(int c) const;

 private:
  // Finds the least significant non-zero bit in n.
  static int FindLSBSet(uint64_t n) {
    DCHECK_NE(n, 0);

#if defined(__GNUC__)
    return __builtin_ctzll(n);
#elif defined(_MSC_VER) && defined(_M_X64)
    unsigned long c;
    _BitScanForward64(&c, n);
    return static_cast<int>(c);
#elif defined(_MSC_VER) && defined(_M_IX86)
    unsigned long c;
    if (static_cast<uint32_t>(n) != 0) {
      _BitScanForward(&c, static_cast<uint32_t>(n));
      return static_cast<int>(c);
    } else {
      _BitScanForward(&c, static_cast<uint32_t>(n >> 32));
      return static_cast<int>(c) + 32;
    }
#else
    int c = 63;
    for (int shift = 1 << 5; shift != 0; shift >>= 1) {
      uint64_t word = n << shift;
      if (word != 0) {
        n = word;
        c -= shift;
      }
    }
    return c;
#endif
  }

  uint64_t words_[4];
};

int Bitmap256::FindNextSetBit(int c) const {
  DCHECK_GE(c, 0);
  DCHECK_LE(c, 255);

  // Check the word that contains the bit. Mask out any lower bits.
  int i = c / 64;
  uint64_t word = words_[i] & (~0ULL << (c % 64));
  if (word != 0)
    return (i * 64) + FindLSBSet(word);

  // Check any following words.
  i++;
  switch (i) {
    case 1:
      if (words_[1] != 0)
        return (1 * 64) + FindLSBSet(words_[1]);
      FALLTHROUGH_INTENDED;
    case 2:
      if (words_[2] != 0)
        return (2 * 64) + FindLSBSet(words_[2]);
      FALLTHROUGH_INTENDED;
    case 3:
      if (words_[3] != 0)
        return (3 * 64) + FindLSBSet(words_[3]);
      FALLTHROUGH_INTENDED;
    default:
      return -1;
  }
}

}  // namespace duckdb_re2

#endif  // RE2_BITMAP256_H_


// LICENSE_CHANGE_END



namespace duckdb_re2 {

// Constructors per Inst opcode

void Prog::Inst::InitAlt(uint32_t out, uint32_t out1) {
  DCHECK_EQ(out_opcode_, 0);
  set_out_opcode(out, kInstAlt);
  out1_ = out1;
}

void Prog::Inst::InitByteRange(int lo, int hi, int foldcase, uint32_t out) {
  DCHECK_EQ(out_opcode_, 0);
  set_out_opcode(out, kInstByteRange);
    inst_byte_range_data_.lo_ = lo & 0xFF;
    inst_byte_range_data_.hi_ = hi & 0xFF;
    inst_byte_range_data_.hint_foldcase_ = foldcase&1;
}

void Prog::Inst::InitCapture(int cap, uint32_t out) {
  DCHECK_EQ(out_opcode_, 0);
  set_out_opcode(out, kInstCapture);
  cap_ = cap;
}

void Prog::Inst::InitEmptyWidth(EmptyOp empty, uint32_t out) {
  DCHECK_EQ(out_opcode_, 0);
  set_out_opcode(out, kInstEmptyWidth);
  empty_ = empty;
}

void Prog::Inst::InitMatch(int32_t id) {
  DCHECK_EQ(out_opcode_, 0);
  set_opcode(kInstMatch);
  match_id_ = id;
}

void Prog::Inst::InitNop(uint32_t out) {
  DCHECK_EQ(out_opcode_, 0);
  set_opcode(kInstNop);
}

void Prog::Inst::InitFail() {
  DCHECK_EQ(out_opcode_, 0);
  set_opcode(kInstFail);
}

std::string Prog::Inst::Dump() {
  switch (opcode()) {
    default:
      return StringPrintf("opcode %d", static_cast<int>(opcode()));

    case kInstAlt:
      return StringPrintf("alt -> %d | %d", out(), out1_);

    case kInstAltMatch:
      return StringPrintf("altmatch -> %d | %d", out(), out1_);

    case kInstByteRange:
      return StringPrintf("byte%s [%02x-%02x] %d -> %d",
                          foldcase() ? "/i" : "",
                          inst_byte_range_data_.lo_, inst_byte_range_data_.hi_, hint(), out());

    case kInstCapture:
      return StringPrintf("capture %d -> %d", cap_, out());

    case kInstEmptyWidth:
      return StringPrintf("emptywidth %#x -> %d",
                          static_cast<int>(empty_), out());

    case kInstMatch:
      return StringPrintf("match! %d", match_id());

    case kInstNop:
      return StringPrintf("nop -> %d", out());

    case kInstFail:
      return StringPrintf("fail");
  }
}

Prog::Prog()
  : anchor_start_(false),
    anchor_end_(false),
    reversed_(false),
    did_flatten_(false),
    did_onepass_(false),
    start_(0),
    start_unanchored_(0),
    size_(0),
    bytemap_range_(0),
    first_byte_(-1),
    flags_(0),
    list_count_(0),
    dfa_mem_(0),
    dfa_first_(NULL),
    dfa_longest_(NULL) {
}

Prog::~Prog() {
  DeleteDFA(dfa_longest_);
  DeleteDFA(dfa_first_);
}

typedef SparseSet Workq;

static inline void AddToQueue(Workq* q, int id) {
  if (id != 0)
    q->insert(id);
}

static std::string ProgToString(Prog* prog, Workq* q) {
  std::string s;
  for (Workq::iterator i = q->begin(); i != q->end(); ++i) {
    int id = *i;
    Prog::Inst* ip = prog->inst(id);
    StringAppendF(&s, "%d. %s\n", id, ip->Dump().c_str());
    AddToQueue(q, ip->out());
    if (ip->opcode() == kInstAlt || ip->opcode() == kInstAltMatch)
      AddToQueue(q, ip->out1());
  }
  return s;
}

static std::string FlattenedProgToString(Prog* prog, int start) {
  std::string s;
  for (int id = start; id < prog->size(); id++) {
    Prog::Inst* ip = prog->inst(id);
    if (ip->last())
      StringAppendF(&s, "%d. %s\n", id, ip->Dump().c_str());
    else
      StringAppendF(&s, "%d+ %s\n", id, ip->Dump().c_str());
  }
  return s;
}

std::string Prog::Dump() {
  if (did_flatten_)
    return FlattenedProgToString(this, start_);

  Workq q(size_);
  AddToQueue(&q, start_);
  return ProgToString(this, &q);
}

std::string Prog::DumpUnanchored() {
  if (did_flatten_)
    return FlattenedProgToString(this, start_unanchored_);

  Workq q(size_);
  AddToQueue(&q, start_unanchored_);
  return ProgToString(this, &q);
}

std::string Prog::DumpByteMap() {
  std::string map;
  for (int c = 0; c < 256; c++) {
    int b = bytemap_[c];
    int lo = c;
    while (c < 256-1 && bytemap_[c+1] == b)
      c++;
    int hi = c;
    StringAppendF(&map, "[%02x-%02x] -> %d\n", lo, hi, b);
  }
  return map;
}

int Prog::first_byte() {
  std::call_once(first_byte_once_, [](Prog* prog) {
    prog->first_byte_ = prog->ComputeFirstByte();
  }, this);
  return first_byte_;
}

static bool IsMatch(Prog*, Prog::Inst*);

// Peep-hole optimizer.
void Prog::Optimize() {
  Workq q(size_);

  // Eliminate nops.  Most are taken out during compilation
  // but a few are hard to avoid.
  q.clear();
  AddToQueue(&q, start_);
  for (Workq::iterator i = q.begin(); i != q.end(); ++i) {
    int id = *i;

    Inst* ip = inst(id);
    int j = ip->out();
    Inst* jp;
    while (j != 0 && (jp=inst(j))->opcode() == kInstNop) {
      j = jp->out();
    }
    ip->set_out(j);
    AddToQueue(&q, ip->out());

    if (ip->opcode() == kInstAlt) {
      j = ip->out1();
      while (j != 0 && (jp=inst(j))->opcode() == kInstNop) {
        j = jp->out();
      }
      ip->out1_ = j;
      AddToQueue(&q, ip->out1());
    }
  }

  // Insert kInstAltMatch instructions
  // Look for
  //   ip: Alt -> j | k
  //	  j: ByteRange [00-FF] -> ip
  //    k: Match
  // or the reverse (the above is the greedy one).
  // Rewrite Alt to AltMatch.
  q.clear();
  AddToQueue(&q, start_);
  for (Workq::iterator i = q.begin(); i != q.end(); ++i) {
    int id = *i;
    Inst* ip = inst(id);
    AddToQueue(&q, ip->out());
    if (ip->opcode() == kInstAlt)
      AddToQueue(&q, ip->out1());

    if (ip->opcode() == kInstAlt) {
      Inst* j = inst(ip->out());
      Inst* k = inst(ip->out1());
      if (j->opcode() == kInstByteRange && j->out() == id &&
          j->lo() == 0x00 && j->hi() == 0xFF &&
          IsMatch(this, k)) {
        ip->set_opcode(kInstAltMatch);
        continue;
      }
      if (IsMatch(this, j) &&
          k->opcode() == kInstByteRange && k->out() == id &&
          k->lo() == 0x00 && k->hi() == 0xFF) {
        ip->set_opcode(kInstAltMatch);
      }
    }
  }
}

// Is ip a guaranteed match at end of text, perhaps after some capturing?
static bool IsMatch(Prog* prog, Prog::Inst* ip) {
  for (;;) {
    switch (ip->opcode()) {
      default:
        LOG(DFATAL) << "Unexpected opcode in IsMatch: " << ip->opcode();
        return false;

      case kInstAlt:
      case kInstAltMatch:
      case kInstByteRange:
      case kInstFail:
      case kInstEmptyWidth:
        return false;

      case kInstCapture:
      case kInstNop:
        ip = prog->inst(ip->out());
        break;

      case kInstMatch:
        return true;
    }
  }
}

uint32_t Prog::EmptyFlags(const StringPiece& text, const char* p) {
  int flags = 0;

  // ^ and \A
  if (p == text.begin())
    flags |= kEmptyBeginText | kEmptyBeginLine;
  else if (p[-1] == '\n')
    flags |= kEmptyBeginLine;

  // $ and \z
  if (p == text.end())
    flags |= kEmptyEndText | kEmptyEndLine;
  else if (p < text.end() && p[0] == '\n')
    flags |= kEmptyEndLine;

  // \b and \B
  if (p == text.begin() && p == text.end()) {
    // no word boundary here
  } else if (p == text.begin()) {
    if (IsWordChar(p[0]))
      flags |= kEmptyWordBoundary;
  } else if (p == text.end()) {
    if (IsWordChar(p[-1]))
      flags |= kEmptyWordBoundary;
  } else {
    if (IsWordChar(p[-1]) != IsWordChar(p[0]))
      flags |= kEmptyWordBoundary;
  }
  if (!(flags & kEmptyWordBoundary))
    flags |= kEmptyNonWordBoundary;

  return flags;
}

// ByteMapBuilder implements a coloring algorithm.
//
// The first phase is a series of "mark and merge" batches: we mark one or more
// [lo-hi] ranges, then merge them into our internal state. Batching is not for
// performance; rather, it means that the ranges are treated indistinguishably.
//
// Internally, the ranges are represented using a bitmap that stores the splits
// and a vector that stores the colors; both of them are indexed by the ranges'
// last bytes. Thus, in order to merge a [lo-hi] range, we split at lo-1 and at
// hi (if not already split), then recolor each range in between. The color map
// (i.e. from the old color to the new color) is maintained for the lifetime of
// the batch and so underpins this somewhat obscure approach to set operations.
//
// The second phase builds the bytemap from our internal state: we recolor each
// range, then store the new color (which is now the byte class) in each of the
// corresponding array elements. Finally, we output the number of byte classes.
class ByteMapBuilder {
 public:
  ByteMapBuilder() {
    // Initial state: the [0-255] range has color 256.
    // This will avoid problems during the second phase,
    // in which we assign byte classes numbered from 0.
    splits_.Set(255);
    colors_[255] = 256;
    nextcolor_ = 257;
  }

  void Mark(int lo, int hi);
  void Merge();
  void Build(uint8_t* bytemap, int* bytemap_range);

 private:
  int Recolor(int oldcolor);

  Bitmap256 splits_;
  int colors_[256];
  int nextcolor_;
  std::vector<std::pair<int, int>> colormap_;
  std::vector<std::pair<int, int>> ranges_;

  ByteMapBuilder(const ByteMapBuilder&) = delete;
  ByteMapBuilder& operator=(const ByteMapBuilder&) = delete;
};

void ByteMapBuilder::Mark(int lo, int hi) {
  DCHECK_GE(lo, 0);
  DCHECK_GE(hi, 0);
  DCHECK_LE(lo, 255);
  DCHECK_LE(hi, 255);
  DCHECK_LE(lo, hi);

  // Ignore any [0-255] ranges. They cause us to recolor every range, which
  // has no effect on the eventual result and is therefore a waste of time.
  if (lo == 0 && hi == 255)
    return;

  ranges_.emplace_back(lo, hi);
}

void ByteMapBuilder::Merge() {
  for (std::vector<std::pair<int, int>>::const_iterator it = ranges_.begin();
       it != ranges_.end();
       ++it) {
    int lo = it->first-1;
    int hi = it->second;

    if (0 <= lo && !splits_.Test(lo)) {
      splits_.Set(lo);
      int next = splits_.FindNextSetBit(lo+1);
      colors_[lo] = colors_[next];
    }
    if (!splits_.Test(hi)) {
      splits_.Set(hi);
      int next = splits_.FindNextSetBit(hi+1);
      colors_[hi] = colors_[next];
    }

    int c = lo+1;
    while (c < 256) {
      int next = splits_.FindNextSetBit(c);
      colors_[next] = Recolor(colors_[next]);
      if (next == hi)
        break;
      c = next+1;
    }
  }
  colormap_.clear();
  ranges_.clear();
}

void ByteMapBuilder::Build(uint8_t* bytemap, int* bytemap_range) {
  // Assign byte classes numbered from 0.
  nextcolor_ = 0;

  int c = 0;
  while (c < 256) {
    int next = splits_.FindNextSetBit(c);
    uint8_t b = static_cast<uint8_t>(Recolor(colors_[next]));
    while (c <= next) {
      bytemap[c] = b;
      c++;
    }
  }

  *bytemap_range = nextcolor_;
}

int ByteMapBuilder::Recolor(int oldcolor) {
  // Yes, this is a linear search. There can be at most 256
  // colors and there will typically be far fewer than that.
  // Also, we need to consider keys *and* values in order to
  // avoid recoloring a given range more than once per batch.
  std::vector<std::pair<int, int>>::const_iterator it =
      std::find_if(colormap_.begin(), colormap_.end(),
                   [=](const std::pair<int, int>& kv) -> bool {
                     return kv.first == oldcolor || kv.second == oldcolor;
                   });
  if (it != colormap_.end())
    return it->second;
  int newcolor = nextcolor_;
  nextcolor_++;
  colormap_.emplace_back(oldcolor, newcolor);
  return newcolor;
}

void Prog::ComputeByteMap() {
  // Fill in bytemap with byte classes for the program.
  // Ranges of bytes that are treated indistinguishably
  // will be mapped to a single byte class.
  ByteMapBuilder builder;

  // Don't repeat the work for ^ and $.
  bool marked_line_boundaries = false;
  // Don't repeat the work for \b and \B.
  bool marked_word_boundaries = false;

  for (int id = 0; id < size(); id++) {
    Inst* ip = inst(id);
    if (ip->opcode() == kInstByteRange) {
      int lo = ip->lo();
      int hi = ip->hi();
      builder.Mark(lo, hi);
      if (ip->foldcase() && lo <= 'z' && hi >= 'a') {
        int foldlo = lo;
        int foldhi = hi;
        if (foldlo < 'a')
          foldlo = 'a';
        if (foldhi > 'z')
          foldhi = 'z';
        if (foldlo <= foldhi) {
          foldlo += 'A' - 'a';
          foldhi += 'A' - 'a';
          builder.Mark(foldlo, foldhi);
        }
      }
      // If this Inst is not the last Inst in its list AND the next Inst is
      // also a ByteRange AND the Insts have the same out, defer the merge.
      if (!ip->last() &&
          inst(id+1)->opcode() == kInstByteRange &&
          ip->out() == inst(id+1)->out())
        continue;
      builder.Merge();
    } else if (ip->opcode() == kInstEmptyWidth) {
      if (ip->empty() & (kEmptyBeginLine|kEmptyEndLine) &&
          !marked_line_boundaries) {
        builder.Mark('\n', '\n');
        builder.Merge();
        marked_line_boundaries = true;
      }
      if (ip->empty() & (kEmptyWordBoundary|kEmptyNonWordBoundary) &&
          !marked_word_boundaries) {
        // We require two batches here: the first for ranges that are word
        // characters, the second for ranges that are not word characters.
        for (bool isword : {true, false}) {
          int j;
          for (int i = 0; i < 256; i = j) {
            for (j = i + 1; j < 256 &&
                            Prog::IsWordChar(static_cast<uint8_t>(i)) ==
                                Prog::IsWordChar(static_cast<uint8_t>(j));
                 j++)
              ;
            if (Prog::IsWordChar(static_cast<uint8_t>(i)) == isword)
              builder.Mark(i, j - 1);
          }
          builder.Merge();
        }
        marked_word_boundaries = true;
      }
    }
  }

  builder.Build(bytemap_, &bytemap_range_);
}

// Prog::Flatten() implements a graph rewriting algorithm.
//
// The overall process is similar to epsilon removal, but retains some epsilon
// transitions: those from Capture and EmptyWidth instructions; and those from
// nullable subexpressions. (The latter avoids quadratic blowup in transitions
// in the worst case.) It might be best thought of as Alt instruction elision.
//
// In conceptual terms, it divides the Prog into "trees" of instructions, then
// traverses the "trees" in order to produce "lists" of instructions. A "tree"
// is one or more instructions that grow from one "root" instruction to one or
// more "leaf" instructions; if a "tree" has exactly one instruction, then the
// "root" is also the "leaf". In most cases, a "root" is the successor of some
// "leaf" (i.e. the "leaf" instruction's out() returns the "root" instruction)
// and is considered a "successor root". A "leaf" can be a ByteRange, Capture,
// EmptyWidth or Match instruction. However, this is insufficient for handling
// nested nullable subexpressions correctly, so in some cases, a "root" is the
// dominator of the instructions reachable from some "successor root" (i.e. it
// has an unreachable predecessor) and is considered a "dominator root". Since
// only Alt instructions can be "dominator roots" (other instructions would be
// "leaves"), only Alt instructions are required to be marked as predecessors.
//
// Dividing the Prog into "trees" comprises two passes: marking the "successor
// roots" and the predecessors; and marking the "dominator roots". Sorting the
// "successor roots" by their bytecode offsets enables iteration in order from
// greatest to least during the second pass; by working backwards in this case
// and flooding the graph no further than "leaves" and already marked "roots",
// it becomes possible to mark "dominator roots" without doing excessive work.
//
// Traversing the "trees" is just iterating over the "roots" in order of their
// marking and flooding the graph no further than "leaves" and "roots". When a
// "leaf" is reached, the instruction is copied with its successor remapped to
// its "root" number. When a "root" is reached, a Nop instruction is generated
// with its successor remapped similarly. As each "list" is produced, its last
// instruction is marked as such. After all of the "lists" have been produced,
// a pass over their instructions remaps their successors to bytecode offsets.
void Prog::Flatten() {
  if (did_flatten_)
    return;
  did_flatten_ = true;

  // Scratch structures. It's important that these are reused by functions
  // that we call in loops because they would thrash the heap otherwise.
  SparseSet reachable(size());
  std::vector<int> stk;
  stk.reserve(size());

  // First pass: Marks "successor roots" and predecessors.
  // Builds the mapping from inst-ids to root-ids.
  SparseArray<int> rootmap(size());
  SparseArray<int> predmap(size());
  std::vector<std::vector<int>> predvec;
  MarkSuccessors(&rootmap, &predmap, &predvec, &reachable, &stk);

  // Second pass: Marks "dominator roots".
  SparseArray<int> sorted(rootmap);
  std::sort(sorted.begin(), sorted.end(), sorted.less);
  for (SparseArray<int>::const_iterator i = sorted.end() - 1;
       i != sorted.begin();
       --i) {
    if (i->index() != start_unanchored() && i->index() != start())
      MarkDominator(i->index(), &rootmap, &predmap, &predvec, &reachable, &stk);
  }

  // Third pass: Emits "lists". Remaps outs to root-ids.
  // Builds the mapping from root-ids to flat-ids.
  std::vector<int> flatmap(rootmap.size());
  std::vector<Inst> flat;
  flat.reserve(size());
  for (SparseArray<int>::const_iterator i = rootmap.begin();
       i != rootmap.end();
       ++i) {
    flatmap[i->value()] = static_cast<int>(flat.size());
    EmitList(i->index(), &rootmap, &flat, &reachable, &stk);
    flat.back().set_last();
    // We have the bounds of the "list", so this is the
    // most convenient point at which to compute hints.
    ComputeHints(&flat, flatmap[i->value()], static_cast<int>(flat.size()));
  }

  list_count_ = static_cast<int>(flatmap.size());
  for (int i = 0; i < kNumInst; i++)
    inst_count_[i] = 0;

  // Fourth pass: Remaps outs to flat-ids.
  // Counts instructions by opcode.
  for (int id = 0; id < static_cast<int>(flat.size()); id++) {
    Inst* ip = &flat[id];
    if (ip->opcode() != kInstAltMatch)  // handled in EmitList()
      ip->set_out(flatmap[ip->out()]);
    inst_count_[ip->opcode()]++;
  }

  int total = 0;
  for (int i = 0; i < kNumInst; i++)
    total += inst_count_[i];
  DCHECK_EQ(total, static_cast<int>(flat.size()));

  // Remap start_unanchored and start.
  if (start_unanchored() == 0) {
    DCHECK_EQ(start(), 0);
  } else if (start_unanchored() == start()) {
    set_start_unanchored(flatmap[1]);
    set_start(flatmap[1]);
  } else {
    set_start_unanchored(flatmap[1]);
    set_start(flatmap[2]);
  }

  // Finally, replace the old instructions with the new instructions.
  size_ = static_cast<int>(flat.size());
  inst_ = PODArray<Inst>(size_);
  memmove(inst_.data(), flat.data(), size_*sizeof inst_[0]);

  // Populate the list heads for BitState.
  // 512 instructions limits the memory footprint to 1KiB.
  if (size_ <= 512) {
    list_heads_ = PODArray<uint16_t>(size_);
    // 0xFF makes it more obvious if we try to look up a non-head.
    memset(list_heads_.data(), 0xFF, size_*sizeof list_heads_[0]);
    for (int i = 0; i < list_count_; ++i)
      list_heads_[flatmap[i]] = i;
  }
}

void Prog::MarkSuccessors(SparseArray<int>* rootmap,
                          SparseArray<int>* predmap,
                          std::vector<std::vector<int>>* predvec,
                          SparseSet* reachable, std::vector<int>* stk) {
  // Mark the kInstFail instruction.
  rootmap->set_new(0, rootmap->size());

  // Mark the start_unanchored and start instructions.
  if (!rootmap->has_index(start_unanchored()))
    rootmap->set_new(start_unanchored(), rootmap->size());
  if (!rootmap->has_index(start()))
    rootmap->set_new(start(), rootmap->size());

  reachable->clear();
  stk->clear();
  stk->push_back(start_unanchored());
  while (!stk->empty()) {
    int id = stk->back();
    stk->pop_back();
  Loop:
    if (reachable->contains(id))
      continue;
    reachable->insert_new(id);

    Inst* ip = inst(id);
    switch (ip->opcode()) {
      default:
        LOG(DFATAL) << "unhandled opcode: " << ip->opcode();
        break;

      case kInstAltMatch:
      case kInstAlt:
        // Mark this instruction as a predecessor of each out.
        for (int out : {ip->out(), ip->out1()}) {
          if (!predmap->has_index(out)) {
            predmap->set_new(out, static_cast<int>(predvec->size()));
            predvec->emplace_back();
          }
          (*predvec)[predmap->get_existing(out)].emplace_back(id);
        }
        stk->push_back(ip->out1());
        id = ip->out();
        goto Loop;

      case kInstByteRange:
      case kInstCapture:
      case kInstEmptyWidth:
        // Mark the out of this instruction as a "root".
        if (!rootmap->has_index(ip->out()))
          rootmap->set_new(ip->out(), rootmap->size());
        id = ip->out();
        goto Loop;

      case kInstNop:
        id = ip->out();
        goto Loop;

      case kInstMatch:
      case kInstFail:
        break;
    }
  }
}

void Prog::MarkDominator(int root, SparseArray<int>* rootmap,
                         SparseArray<int>* predmap,
                         std::vector<std::vector<int>>* predvec,
                         SparseSet* reachable, std::vector<int>* stk) {
  reachable->clear();
  stk->clear();
  stk->push_back(root);
  while (!stk->empty()) {
    int id = stk->back();
    stk->pop_back();
  Loop:
    if (reachable->contains(id))
      continue;
    reachable->insert_new(id);

    if (id != root && rootmap->has_index(id)) {
      // We reached another "tree" via epsilon transition.
      continue;
    }

    Inst* ip = inst(id);
    switch (ip->opcode()) {
      default:
        LOG(DFATAL) << "unhandled opcode: " << ip->opcode();
        break;

      case kInstAltMatch:
      case kInstAlt:
        stk->push_back(ip->out1());
        id = ip->out();
        goto Loop;

      case kInstByteRange:
      case kInstCapture:
      case kInstEmptyWidth:
        break;

      case kInstNop:
        id = ip->out();
        goto Loop;

      case kInstMatch:
      case kInstFail:
        break;
    }
  }

  for (SparseSet::const_iterator i = reachable->begin();
       i != reachable->end();
       ++i) {
    int id = *i;
    if (predmap->has_index(id)) {
      for (int pred : (*predvec)[predmap->get_existing(id)]) {
        if (!reachable->contains(pred)) {
          // id has a predecessor that cannot be reached from root!
          // Therefore, id must be a "root" too - mark it as such.
          if (!rootmap->has_index(id))
            rootmap->set_new(id, rootmap->size());
        }
      }
    }
  }
}

void Prog::EmitList(int root, SparseArray<int>* rootmap,
                    std::vector<Inst>* flat,
                    SparseSet* reachable, std::vector<int>* stk) {
  reachable->clear();
  stk->clear();
  stk->push_back(root);
  while (!stk->empty()) {
    int id = stk->back();
    stk->pop_back();
  Loop:
    if (reachable->contains(id))
      continue;
    reachable->insert_new(id);

    if (id != root && rootmap->has_index(id)) {
      // We reached another "tree" via epsilon transition. Emit a kInstNop
      // instruction so that the Prog does not become quadratically larger.
      flat->emplace_back();
      flat->back().set_opcode(kInstNop);
      flat->back().set_out(rootmap->get_existing(id));
      continue;
    }

    Inst* ip = inst(id);
    switch (ip->opcode()) {
      default:
        LOG(DFATAL) << "unhandled opcode: " << ip->opcode();
        break;

      case kInstAltMatch:
        flat->emplace_back();
        flat->back().set_opcode(kInstAltMatch);
        flat->back().set_out(static_cast<int>(flat->size()));
        flat->back().out1_ = static_cast<uint32_t>(flat->size())+1;
        FALLTHROUGH_INTENDED;

      case kInstAlt:
        stk->push_back(ip->out1());
        id = ip->out();
        goto Loop;

      case kInstByteRange:
      case kInstCapture:
      case kInstEmptyWidth:
        flat->emplace_back();
        memmove(&flat->back(), ip, sizeof *ip);
        flat->back().set_out(rootmap->get_existing(ip->out()));
        break;

      case kInstNop:
        id = ip->out();
        goto Loop;

      case kInstMatch:
      case kInstFail:
        flat->emplace_back();
        memmove(&flat->back(), ip, sizeof *ip);
        break;
    }
  }
}

// For each ByteRange instruction in [begin, end), computes a hint to execution
// engines: the delta to the next instruction (in flat) worth exploring iff the
// current instruction matched.
//
// Implements a coloring algorithm related to ByteMapBuilder, but in this case,
// colors are instructions and recoloring ranges precisely identifies conflicts
// between instructions. Iterating backwards over [begin, end) is guaranteed to
// identify the nearest conflict (if any) with only linear complexity.
void Prog::ComputeHints(std::vector<Inst>* flat, int begin, int end) {
  Bitmap256 splits;
  int colors[256];

  bool dirty = false;
  for (int id = end; id >= begin; --id) {
    if (id == end ||
        (*flat)[id].opcode() != kInstByteRange) {
      if (dirty) {
        dirty = false;
        splits.Clear();
      }
      splits.Set(255);
      colors[255] = id;
      // At this point, the [0-255] range is colored with id.
      // Thus, hints cannot point beyond id; and if id == end,
      // hints that would have pointed to id will be 0 instead.
      continue;
    }
    dirty = true;

    // We recolor the [lo-hi] range with id. Note that first ratchets backwards
    // from end to the nearest conflict (if any) during recoloring.
    int first = end;
    auto Recolor = [&](int lo, int hi) {
      // Like ByteMapBuilder, we split at lo-1 and at hi.
      --lo;

      if (0 <= lo && !splits.Test(lo)) {
        splits.Set(lo);
        int next = splits.FindNextSetBit(lo+1);
        colors[lo] = colors[next];
      }
      if (!splits.Test(hi)) {
        splits.Set(hi);
        int next = splits.FindNextSetBit(hi+1);
        colors[hi] = colors[next];
      }

      int c = lo+1;
      while (c < 256) {
        int next = splits.FindNextSetBit(c);
        // Ratchet backwards...
        first = std::min(first, colors[next]);
        // Recolor with id - because it's the new nearest conflict!
        colors[next] = id;
        if (next == hi)
          break;
        c = next+1;
      }
    };

    Inst* ip = &(*flat)[id];
    int lo = ip->lo();
    int hi = ip->hi();
    Recolor(lo, hi);
    if (ip->foldcase() && lo <= 'z' && hi >= 'a') {
      int foldlo = lo;
      int foldhi = hi;
      if (foldlo < 'a')
        foldlo = 'a';
      if (foldhi > 'z')
        foldhi = 'z';
      if (foldlo <= foldhi) {
        foldlo += 'A' - 'a';
        foldhi += 'A' - 'a';
        Recolor(foldlo, foldhi);
      }
    }

    if (first != end) {
      uint16_t hint = static_cast<uint16_t>(std::min(first - id, 32767));
      ip->inst_byte_range_data_.hint_foldcase_ |= hint<<1;
    }
  }
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2003-2009 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Regular expression interface RE2.
//
// Originally the PCRE C++ wrapper, but adapted to use
// the new automata-based regular expression engines.



#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include <iterator>
#include <mutex>
#include <string>
#include <utility>
#include <vector>









namespace duckdb_re2 {

// Maximum number of args we can set
static const int kMaxArgs = 16;
static const int kVecSize = 1+kMaxArgs;

const int RE2::Options::kDefaultMaxMem;  // initialized in re2.h

RE2::Options::Options(RE2::CannedOptions opt)
  : encoding_(opt == RE2::Latin1 ? EncodingLatin1 : EncodingUTF8),
    posix_syntax_(opt == RE2::POSIX),
    longest_match_(opt == RE2::POSIX),
    log_errors_(opt != RE2::Quiet),
    max_mem_(kDefaultMaxMem),
    literal_(false),
    never_nl_(false),
    dot_nl_(false),
    never_capture_(false),
    case_sensitive_(true),
    perl_classes_(false),
    word_boundary_(false),
    one_line_(false) {
}

// static empty objects for use as const references.
// To avoid global constructors, allocated in RE2::Init().
static const std::string* empty_string;
static const std::map<std::string, int>* empty_named_groups;
static const std::map<int, std::string>* empty_group_names;

// Converts from Regexp error code to RE2 error code.
// Maybe some day they will diverge.  In any event, this
// hides the existence of Regexp from RE2 users.
static RE2::ErrorCode RegexpErrorToRE2(duckdb_re2::RegexpStatusCode code) {
  switch (code) {
    case duckdb_re2::kRegexpSuccess:
      return RE2::NoError;
    case duckdb_re2::kRegexpInternalError:
      return RE2::ErrorInternal;
    case duckdb_re2::kRegexpBadEscape:
      return RE2::ErrorBadEscape;
    case duckdb_re2::kRegexpBadCharClass:
      return RE2::ErrorBadCharClass;
    case duckdb_re2::kRegexpBadCharRange:
      return RE2::ErrorBadCharRange;
    case duckdb_re2::kRegexpMissingBracket:
      return RE2::ErrorMissingBracket;
    case duckdb_re2::kRegexpMissingParen:
      return RE2::ErrorMissingParen;
    case duckdb_re2::kRegexpTrailingBackslash:
      return RE2::ErrorTrailingBackslash;
    case duckdb_re2::kRegexpRepeatArgument:
      return RE2::ErrorRepeatArgument;
    case duckdb_re2::kRegexpRepeatSize:
      return RE2::ErrorRepeatSize;
    case duckdb_re2::kRegexpRepeatOp:
      return RE2::ErrorRepeatOp;
    case duckdb_re2::kRegexpBadPerlOp:
      return RE2::ErrorBadPerlOp;
    case duckdb_re2::kRegexpBadUTF8:
      return RE2::ErrorBadUTF8;
    case duckdb_re2::kRegexpBadNamedCapture:
      return RE2::ErrorBadNamedCapture;
  }
  return RE2::ErrorInternal;
}

static std::string trunc(const StringPiece& pattern) {
  if (pattern.size() < 100)
    return std::string(pattern);
  return std::string(pattern.substr(0, 100)) + "...";
}


RE2::RE2(const char* pattern) {
  Init(pattern, DefaultOptions);
}

RE2::RE2(const std::string& pattern) {
  Init(pattern, DefaultOptions);
}

RE2::RE2(const StringPiece& pattern) {
  Init(pattern, DefaultOptions);
}

RE2::RE2(const StringPiece& pattern, const Options& options) {
  Init(pattern, options);
}

int RE2::Options::ParseFlags() const {
  int flags = Regexp::ClassNL;
  switch (encoding()) {
    default:
      if (log_errors())
        LOG(ERROR) << "Unknown encoding " << encoding();
      break;
    case RE2::Options::EncodingUTF8:
      break;
    case RE2::Options::EncodingLatin1:
      flags |= Regexp::Latin1;
      break;
  }

  if (!posix_syntax())
    flags |= Regexp::LikePerl;

  if (literal())
    flags |= Regexp::Literal;

  if (never_nl())
    flags |= Regexp::NeverNL;

  if (dot_nl())
    flags |= Regexp::DotNL;

  if (never_capture())
    flags |= Regexp::NeverCapture;

  if (!case_sensitive())
    flags |= Regexp::FoldCase;

  if (perl_classes())
    flags |= Regexp::PerlClasses;

  if (word_boundary())
    flags |= Regexp::PerlB;

  if (one_line())
    flags |= Regexp::OneLine;

  return flags;
}

void RE2::Init(const StringPiece& pattern, const Options& options) {
  static std::once_flag empty_once;
  std::call_once(empty_once, []() {
    empty_string = new std::string;
    empty_named_groups = new std::map<std::string, int>;
    empty_group_names = new std::map<int, std::string>;
  });

  pattern_ = std::string(pattern);
  options_.Copy(options);
  entire_regexp_ = NULL;
  suffix_regexp_ = NULL;
  prog_ = NULL;
  num_captures_ = -1;
  rprog_ = NULL;
  error_ = empty_string;
  error_code_ = NoError;
  named_groups_ = NULL;
  group_names_ = NULL;

  RegexpStatus status;
  entire_regexp_ = Regexp::Parse(
    pattern_,
    static_cast<Regexp::ParseFlags>(options_.ParseFlags()),
    &status);
  if (entire_regexp_ == NULL) {
    if (options_.log_errors()) {
      LOG(ERROR) << "Error parsing '" << trunc(pattern_) << "': "
                 << status.Text();
    }
    error_ = new std::string(status.Text());
    error_code_ = RegexpErrorToRE2(status.code());
    error_arg_ = std::string(status.error_arg());
    return;
  }

  duckdb_re2::Regexp* suffix;
  if (entire_regexp_->RequiredPrefix(&prefix_, &prefix_foldcase_, &suffix))
    suffix_regexp_ = suffix;
  else
    suffix_regexp_ = entire_regexp_->Incref();

  // Two thirds of the memory goes to the forward Prog,
  // one third to the reverse prog, because the forward
  // Prog has two DFAs but the reverse prog has one.
  prog_ = suffix_regexp_->CompileToProg(options_.max_mem()*2/3);
  if (prog_ == NULL) {
    if (options_.log_errors())
      LOG(ERROR) << "Error compiling '" << trunc(pattern_) << "'";
    error_ = new std::string("pattern too large - compile failed");
    error_code_ = RE2::ErrorPatternTooLarge;
    return;
  }

  // We used to compute this lazily, but it's used during the
  // typical control flow for a match call, so we now compute
  // it eagerly, which avoids the overhead of std::once_flag.
  num_captures_ = suffix_regexp_->NumCaptures();

  // Could delay this until the first match call that
  // cares about submatch information, but the one-pass
  // machine's memory gets cut from the DFA memory budget,
  // and that is harder to do if the DFA has already
  // been built.
  is_one_pass_ = prog_->IsOnePass();
}

// Returns rprog_, computing it if needed.
duckdb_re2::Prog* RE2::ReverseProg() const {
  std::call_once(rprog_once_, [](const RE2* re) {
    re->rprog_ =
        re->suffix_regexp_->CompileToReverseProg(re->options_.max_mem() / 3);
    if (re->rprog_ == NULL) {
      if (re->options_.log_errors())
        LOG(ERROR) << "Error reverse compiling '" << trunc(re->pattern_) << "'";
      re->error_ =
          new std::string("pattern too large - reverse compile failed");
      re->error_code_ = RE2::ErrorPatternTooLarge;
    }
  }, this);
  return rprog_;
}

RE2::~RE2() {
  if (suffix_regexp_)
    suffix_regexp_->Decref();
  if (entire_regexp_)
    entire_regexp_->Decref();
  delete prog_;
  delete rprog_;
  if (error_ != empty_string)
    delete error_;
  if (named_groups_ != NULL && named_groups_ != empty_named_groups)
    delete named_groups_;
  if (group_names_ != NULL &&  group_names_ != empty_group_names)
    delete group_names_;
}

int RE2::ProgramSize() const {
  if (prog_ == NULL)
    return -1;
  return prog_->size();
}

int RE2::ReverseProgramSize() const {
  if (prog_ == NULL)
    return -1;
  Prog* prog = ReverseProg();
  if (prog == NULL)
    return -1;
  return prog->size();
}

static int Fanout(Prog* prog, std::map<int, int>* histogram) {
  SparseArray<int> fanout(prog->size());
  prog->Fanout(&fanout);
  histogram->clear();
  for (SparseArray<int>::iterator i = fanout.begin(); i != fanout.end(); ++i) {
    // TODO(junyer): Optimise this?
    int bucket = 0;
    while (1 << bucket < i->value()) {
      bucket++;
    }
    (*histogram)[bucket]++;
  }
  return histogram->rbegin()->first;
}

int RE2::ProgramFanout(std::map<int, int>* histogram) const {
  if (prog_ == NULL)
    return -1;
  return Fanout(prog_, histogram);
}

int RE2::ReverseProgramFanout(std::map<int, int>* histogram) const {
  if (prog_ == NULL)
    return -1;
  Prog* prog = ReverseProg();
  if (prog == NULL)
    return -1;
  return Fanout(prog, histogram);
}

// Returns named_groups_, computing it if needed.
const std::map<std::string, int>& RE2::NamedCapturingGroups() const {
  std::call_once(named_groups_once_, [](const RE2* re) {
    if (re->suffix_regexp_ != NULL)
      re->named_groups_ = re->suffix_regexp_->NamedCaptures();
    if (re->named_groups_ == NULL)
      re->named_groups_ = empty_named_groups;
  }, this);
  return *named_groups_;
}

// Returns group_names_, computing it if needed.
const std::map<int, std::string>& RE2::CapturingGroupNames() const {
  std::call_once(group_names_once_, [](const RE2* re) {
    if (re->suffix_regexp_ != NULL)
      re->group_names_ = re->suffix_regexp_->CaptureNames();
    if (re->group_names_ == NULL)
      re->group_names_ = empty_group_names;
  }, this);
  return *group_names_;
}

/***** Convenience interfaces *****/

bool RE2::FullMatchN(const StringPiece& text, const RE2& re,
                     const Arg* const args[], int n) {
  return re.DoMatch(text, ANCHOR_BOTH, NULL, args, n);
}

bool RE2::PartialMatchN(const StringPiece& text, const RE2& re,
                        const Arg* const args[], int n) {
  return re.DoMatch(text, UNANCHORED, NULL, args, n);
}

bool RE2::ConsumeN(StringPiece* input, const RE2& re,
                   const Arg* const args[], int n) {
  size_t consumed;
  if (re.DoMatch(*input, ANCHOR_START, &consumed, args, n)) {
    input->remove_prefix(consumed);
    return true;
  } else {
    return false;
  }
}

bool RE2::FindAndConsumeN(StringPiece* input, const RE2& re,
                          const Arg* const args[], int n) {
  size_t consumed;
  if (re.DoMatch(*input, UNANCHORED, &consumed, args, n)) {
    input->remove_prefix(consumed);
    return true;
  } else {
    return false;
  }
}

bool RE2::Replace(std::string* str,
                  const RE2& re,
                  const StringPiece& rewrite) {
  StringPiece vec[kVecSize];
  int nvec = 1 + MaxSubmatch(rewrite);
  if (nvec > arraysize(vec))
    return false;
  if (!re.Match(*str, 0, str->size(), UNANCHORED, vec, nvec))
    return false;

  std::string s;
  if (!re.Rewrite(&s, rewrite, vec, nvec))
    return false;

  assert(vec[0].begin() >= str->data());
  assert(vec[0].end() <= str->data()+str->size());
  str->replace(vec[0].data() - str->data(), vec[0].size(), s);
  return true;
}

int RE2::GlobalReplace(std::string* str,
                       const RE2& re,
                       const StringPiece& rewrite) {
  StringPiece vec[kVecSize];
  int nvec = 1 + MaxSubmatch(rewrite);
  if (nvec > arraysize(vec))
    return false;

  const char* p = str->data();
  const char* ep = p + str->size();
  const char* lastend = NULL;
  std::string out;
  int count = 0;
#ifdef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION
  // Iterate just once when fuzzing. Otherwise, we easily get bogged down
  // and coverage is unlikely to improve despite significant expense.
  while (p == str->data()) {
#else
  while (p <= ep) {
#endif
    if (!re.Match(*str, static_cast<size_t>(p - str->data()),
                  str->size(), UNANCHORED, vec, nvec))
      break;
    if (p < vec[0].begin())
      out.append(p, vec[0].begin() - p);
    if (vec[0].begin() == lastend && vec[0].size() == 0) {
      // Disallow empty match at end of last match: skip ahead.
      //
      // fullrune() takes int, not ptrdiff_t. However, it just looks
      // at the leading byte and treats any length >= 4 the same.
      if (re.options().encoding() == RE2::Options::EncodingUTF8 &&
          fullrune(p, static_cast<int>(std::min(ptrdiff_t{4}, ep - p)))) {
        // re is in UTF-8 mode and there is enough left of str
        // to allow us to advance by up to UTFmax bytes.
        Rune r;
        int n = chartorune(&r, p);
        // Some copies of chartorune have a bug that accepts
        // encodings of values in (10FFFF, 1FFFFF] as valid.
        if (r > Runemax) {
          n = 1;
          r = Runeerror;
        }
        if (!(n == 1 && r == Runeerror)) {  // no decoding error
          out.append(p, n);
          p += n;
          continue;
        }
      }
      // Most likely, re is in Latin-1 mode. If it is in UTF-8 mode,
      // we fell through from above and the GIGO principle applies.
      if (p < ep)
        out.append(p, 1);
      p++;
      continue;
    }
    re.Rewrite(&out, rewrite, vec, nvec);
    p = vec[0].end();
    lastend = p;
    count++;
  }

  if (count == 0)
    return 0;

  if (p < ep)
    out.append(p, ep - p);
  using std::swap;
  swap(out, *str);
  return count;
}

bool RE2::Extract(const StringPiece& text,
                  const RE2& re,
                  const StringPiece& rewrite,
                  std::string* out) {
  StringPiece vec[kVecSize];
  int nvec = 1 + MaxSubmatch(rewrite);
  if (nvec > arraysize(vec))
    return false;

  if (!re.Match(text, 0, text.size(), UNANCHORED, vec, nvec))
    return false;

  out->clear();
  return re.Rewrite(out, rewrite, vec, nvec);
}

std::string RE2::QuoteMeta(const StringPiece& unquoted) {
  std::string result;
  result.reserve(unquoted.size() << 1);

  // Escape any ascii character not in [A-Za-z_0-9].
  //
  // Note that it's legal to escape a character even if it has no
  // special meaning in a regular expression -- so this function does
  // that.  (This also makes it identical to the perl function of the
  // same name except for the null-character special case;
  // see `perldoc -f quotemeta`.)
  for (size_t ii = 0; ii < unquoted.size(); ++ii) {
    // Note that using 'isalnum' here raises the benchmark time from
    // 32ns to 58ns:
    if ((unquoted[ii] < 'a' || unquoted[ii] > 'z') &&
        (unquoted[ii] < 'A' || unquoted[ii] > 'Z') &&
        (unquoted[ii] < '0' || unquoted[ii] > '9') &&
        unquoted[ii] != '_' &&
        // If this is the part of a UTF8 or Latin1 character, we need
        // to copy this byte without escaping.  Experimentally this is
        // what works correctly with the regexp library.
        !(unquoted[ii] & 128)) {
      if (unquoted[ii] == '\0') {  // Special handling for null chars.
        // Note that this special handling is not strictly required for RE2,
        // but this quoting is required for other regexp libraries such as
        // PCRE.
        // Can't use "\\0" since the next character might be a digit.
        result += "\\x00";
        continue;
      }
      result += '\\';
    }
    result += unquoted[ii];
  }

  return result;
}

bool RE2::PossibleMatchRange(std::string* min, std::string* max,
                             int maxlen) const {
  if (prog_ == NULL)
    return false;

  int n = static_cast<int>(prefix_.size());
  if (n > maxlen)
    n = maxlen;

  // Determine initial min max from prefix_ literal.
  *min = prefix_.substr(0, n);
  *max = prefix_.substr(0, n);
  if (prefix_foldcase_) {
    // prefix is ASCII lowercase; change *min to uppercase.
    for (int i = 0; i < n; i++) {
      char& c = (*min)[i];
      if ('a' <= c && c <= 'z')
        c += 'A' - 'a';
    }
  }

  // Add to prefix min max using PossibleMatchRange on regexp.
  std::string dmin, dmax;
  maxlen -= n;
  if (maxlen > 0 && prog_->PossibleMatchRange(&dmin, &dmax, maxlen)) {
    min->append(dmin);
    max->append(dmax);
  } else if (!max->empty()) {
    // prog_->PossibleMatchRange has failed us,
    // but we still have useful information from prefix_.
    // Round up *max to allow any possible suffix.
    PrefixSuccessor(max);
  } else {
    // Nothing useful.
    *min = "";
    *max = "";
    return false;
  }

  return true;
}

// Avoid possible locale nonsense in standard strcasecmp.
// The string a is known to be all lowercase.
static int ascii_strcasecmp(const char* a, const char* b, size_t len) {
  const char* ae = a + len;

  for (; a < ae; a++, b++) {
    uint8_t x = *a;
    uint8_t y = *b;
    if ('A' <= y && y <= 'Z')
      y += 'a' - 'A';
    if (x != y)
      return x - y;
  }
  return 0;
}


/***** Actual matching and rewriting code *****/

bool RE2::Match(const StringPiece& text,
                size_t startpos,
                size_t endpos,
                Anchor re_anchor,
                StringPiece* submatch,
                int nsubmatch) const {
  if (!ok()) {
    if (options_.log_errors())
      LOG(ERROR) << "Invalid RE2: " << *error_;
    return false;
  }

  if (startpos > endpos || endpos > text.size()) {
    if (options_.log_errors())
      LOG(ERROR) << "RE2: invalid startpos, endpos pair. ["
                 << "startpos: " << startpos << ", "
                 << "endpos: " << endpos << ", "
                 << "text size: " << text.size() << "]";
    return false;
  }

  StringPiece subtext = text;
  subtext.remove_prefix(startpos);
  subtext.remove_suffix(text.size() - endpos);

  // Use DFAs to find exact location of match, filter out non-matches.

  // Don't ask for the location if we won't use it.
  // SearchDFA can do extra optimizations in that case.
  StringPiece match;
  StringPiece* matchp = &match;
  if (nsubmatch == 0)
    matchp = NULL;

  int ncap = 1 + NumberOfCapturingGroups();
  if (ncap > nsubmatch)
    ncap = nsubmatch;

  // If the regexp is anchored explicitly, must not be in middle of text.
  if (prog_->anchor_start() && startpos != 0)
    return false;

  // If the regexp is anchored explicitly, update re_anchor
  // so that we can potentially fall into a faster case below.
  if (prog_->anchor_start() && prog_->anchor_end())
    re_anchor = ANCHOR_BOTH;
  else if (prog_->anchor_start() && re_anchor != ANCHOR_BOTH)
    re_anchor = ANCHOR_START;

  // Check for the required prefix, if any.
  size_t prefixlen = 0;
  if (!prefix_.empty()) {
    if (startpos != 0)
      return false;
    prefixlen = prefix_.size();
    if (prefixlen > subtext.size())
      return false;
    if (prefix_foldcase_) {
      if (ascii_strcasecmp(&prefix_[0], subtext.data(), prefixlen) != 0)
        return false;
    } else {
      if (memcmp(&prefix_[0], subtext.data(), prefixlen) != 0)
        return false;
    }
    subtext.remove_prefix(prefixlen);
    // If there is a required prefix, the anchor must be at least ANCHOR_START.
    if (re_anchor != ANCHOR_BOTH)
      re_anchor = ANCHOR_START;
  }

  Prog::Anchor anchor = Prog::kUnanchored;
  Prog::MatchKind kind = Prog::kFirstMatch;
  if (options_.longest_match())
    kind = Prog::kLongestMatch;
  bool skipped_test = false;

  bool can_one_pass = (is_one_pass_ && ncap <= Prog::kMaxOnePassCapture);

  // BitState allocates a bitmap of size prog_->list_count() * text.size().
  // It also allocates a stack of 3-word structures which could potentially
  // grow as large as prog_->list_count() * text.size(), but in practice is
  // much smaller.
  const int kMaxBitStateBitmapSize = 256*1024;  // bitmap size <= max (bits)
  bool can_bit_state = prog_->CanBitState();
  size_t bit_state_text_max = kMaxBitStateBitmapSize / prog_->list_count();

  bool dfa_failed = false;
  switch (re_anchor) {
    default:
    case UNANCHORED: {
      if (!prog_->SearchDFA(subtext, text, anchor, kind,
                            matchp, &dfa_failed, NULL)) {
        if (dfa_failed) {
          if (options_.log_errors())
            LOG(ERROR) << "DFA out of memory: size " << prog_->size() << ", "
                       << "bytemap range " << prog_->bytemap_range() << ", "
                       << "list count " << prog_->list_count();
          // Fall back to NFA below.
          skipped_test = true;
          break;
        }
        return false;
      }
      if (matchp == NULL)  // Matched.  Don't care where
        return true;
      // SearchDFA set match[0].end() but didn't know where the
      // match started.  Run the regexp backward from match[0].end()
      // to find the longest possible match -- that's where it started.
      Prog* prog = ReverseProg();
      if (prog == NULL)
        return false;
      if (!prog->SearchDFA(match, text, Prog::kAnchored,
                           Prog::kLongestMatch, &match, &dfa_failed, NULL)) {
        if (dfa_failed) {
          if (options_.log_errors())
            LOG(ERROR) << "DFA out of memory: size " << prog->size() << ", "
                       << "bytemap range " << prog->bytemap_range() << ", "
                       << "list count " << prog->list_count();
          // Fall back to NFA below.
          skipped_test = true;
          break;
        }
        if (options_.log_errors())
          LOG(ERROR) << "SearchDFA inconsistency";
        return false;
      }
      break;
    }

    case ANCHOR_BOTH:
    case ANCHOR_START:
      if (re_anchor == ANCHOR_BOTH)
        kind = Prog::kFullMatch;
      anchor = Prog::kAnchored;

      // If only a small amount of text and need submatch
      // information anyway and we're going to use OnePass or BitState
      // to get it, we might as well not even bother with the DFA:
      // OnePass or BitState will be fast enough.
      // On tiny texts, OnePass outruns even the DFA, and
      // it doesn't have the shared state and occasional mutex that
      // the DFA does.
      if (can_one_pass && text.size() <= 4096 &&
          (ncap > 1 || text.size() <= 8)) {
        skipped_test = true;
        break;
      }
      if (can_bit_state && text.size() <= bit_state_text_max && ncap > 1) {
        skipped_test = true;
        break;
      }
      if (!prog_->SearchDFA(subtext, text, anchor, kind,
                            &match, &dfa_failed, NULL)) {
        if (dfa_failed) {
          if (options_.log_errors())
            LOG(ERROR) << "DFA out of memory: size " << prog_->size() << ", "
                       << "bytemap range " << prog_->bytemap_range() << ", "
                       << "list count " << prog_->list_count();
          // Fall back to NFA below.
          skipped_test = true;
          break;
        }
        return false;
      }
      break;
  }

  if (!skipped_test && ncap <= 1) {
    // We know exactly where it matches.  That's enough.
    if (ncap == 1)
      submatch[0] = match;
  } else {
    StringPiece subtext1;
    if (skipped_test) {
      // DFA ran out of memory or was skipped:
      // need to search in entire original text.
      subtext1 = subtext;
    } else {
      // DFA found the exact match location:
      // let NFA run an anchored, full match search
      // to find submatch locations.
      subtext1 = match;
      anchor = Prog::kAnchored;
      kind = Prog::kFullMatch;
    }

    if (can_one_pass && anchor != Prog::kUnanchored) {
      if (!prog_->SearchOnePass(subtext1, text, anchor, kind, submatch, ncap)) {
        if (!skipped_test && options_.log_errors())
          LOG(ERROR) << "SearchOnePass inconsistency";
        return false;
      }
    } else if (can_bit_state && subtext1.size() <= bit_state_text_max) {
      if (!prog_->SearchBitState(subtext1, text, anchor,
                                 kind, submatch, ncap)) {
        if (!skipped_test && options_.log_errors())
          LOG(ERROR) << "SearchBitState inconsistency";
        return false;
      }
    } else {
      if (!prog_->SearchNFA(subtext1, text, anchor, kind, submatch, ncap)) {
        if (!skipped_test && options_.log_errors())
          LOG(ERROR) << "SearchNFA inconsistency";
        return false;
      }
    }
  }

  // Adjust overall match for required prefix that we stripped off.
  if (prefixlen > 0 && nsubmatch > 0)
    submatch[0] = StringPiece(submatch[0].data() - prefixlen,
                              submatch[0].size() + prefixlen);

  // Zero submatches that don't exist in the regexp.
  for (int i = ncap; i < nsubmatch; i++)
    submatch[i] = StringPiece();
  return true;
}

// Internal matcher - like Match() but takes Args not StringPieces.
bool RE2::DoMatch(const StringPiece& text,
                  Anchor re_anchor,
                  size_t* consumed,
                  const Arg* const* args,
                  int n) const {
  if (!ok()) {
    if (options_.log_errors())
      LOG(ERROR) << "Invalid RE2: " << *error_;
    return false;
  }

  if (NumberOfCapturingGroups() < n) {
    // RE has fewer capturing groups than number of Arg pointers passed in.
    return false;
  }

  // Count number of capture groups needed.
  int nvec;
  if (n == 0 && consumed == NULL)
    nvec = 0;
  else
    nvec = n+1;

  StringPiece* vec;
  StringPiece stkvec[kVecSize];
  StringPiece* heapvec = NULL;

  if (nvec <= arraysize(stkvec)) {
    vec = stkvec;
  } else {
    vec = new StringPiece[nvec];
    heapvec = vec;
  }

  if (!Match(text, 0, text.size(), re_anchor, vec, nvec)) {
    delete[] heapvec;
    return false;
  }

  if (consumed != NULL)
    *consumed = static_cast<size_t>(vec[0].end() - text.begin());

  if (n == 0 || args == NULL) {
    // We are not interested in results
    delete[] heapvec;
    return true;
  }

  // If we got here, we must have matched the whole pattern.
  for (int i = 0; i < n; i++) {
    const StringPiece& s = vec[i+1];
    if (!args[i]->Parse(s.data(), s.size())) {
      // TODO: Should we indicate what the error was?
      delete[] heapvec;
      return false;
    }
  }

  delete[] heapvec;
  return true;
}

// Checks that the rewrite string is well-formed with respect to this
// regular expression.
bool RE2::CheckRewriteString(const StringPiece& rewrite,
                             std::string* error) const {
  int max_token = -1;
  for (const char *s = rewrite.data(), *end = s + rewrite.size();
       s < end; s++) {
    int c = *s;
    if (c != '\\') {
      continue;
    }
    if (++s == end) {
      *error = "Rewrite schema error: '\\' not allowed at end.";
      return false;
    }
    c = *s;
    if (c == '\\') {
      continue;
    }
    if (!isdigit(c)) {
      *error = "Rewrite schema error: "
               "'\\' must be followed by a digit or '\\'.";
      return false;
    }
    int n = (c - '0');
    if (max_token < n) {
      max_token = n;
    }
  }

  if (max_token > NumberOfCapturingGroups()) {
    SStringPrintf(error, "Rewrite schema requests %d matches, "
                  "but the regexp only has %d parenthesized subexpressions.",
                  max_token, NumberOfCapturingGroups());
    return false;
  }
  return true;
}

// Returns the maximum submatch needed for the rewrite to be done by Replace().
// E.g. if rewrite == "foo \\2,\\1", returns 2.
int RE2::MaxSubmatch(const StringPiece& rewrite) {
  int max = 0;
  for (const char *s = rewrite.data(), *end = s + rewrite.size();
       s < end; s++) {
    if (*s == '\\') {
      s++;
      int c = (s < end) ? *s : -1;
      if (isdigit(c)) {
        int n = (c - '0');
        if (n > max)
          max = n;
      }
    }
  }
  return max;
}

// Append the "rewrite" string, with backslash subsitutions from "vec",
// to string "out".
bool RE2::Rewrite(std::string* out,
                  const StringPiece& rewrite,
                  const StringPiece* vec,
                  int veclen) const {
  for (const char *s = rewrite.data(), *end = s + rewrite.size();
       s < end; s++) {
    if (*s != '\\') {
      out->push_back(*s);
      continue;
    }
    s++;
    int c = (s < end) ? *s : -1;
    if (isdigit(c)) {
      int n = (c - '0');
      if (n >= veclen) {
        if (options_.log_errors()) {
          LOG(ERROR) << "requested group " << n
                     << " in regexp " << rewrite.data();
        }
        return false;
      }
      StringPiece snip = vec[n];
      if (snip.size() > 0)
        out->append(snip.data(), snip.size());
    } else if (c == '\\') {
      out->push_back('\\');
    } else {
      if (options_.log_errors())
        LOG(ERROR) << "invalid rewrite pattern: " << rewrite.data();
      return false;
    }
  }
  return true;
}

/***** Parsers for various types *****/

bool RE2::Arg::parse_null(const char* str, size_t n, void* dest) {
  // We fail if somebody asked us to store into a non-NULL void* pointer
  return (dest == NULL);
}

bool RE2::Arg::parse_string(const char* str, size_t n, void* dest) {
  if (dest == NULL) return true;
  reinterpret_cast<std::string*>(dest)->assign(str, n);
  return true;
}

bool RE2::Arg::parse_stringpiece(const char* str, size_t n, void* dest) {
  if (dest == NULL) return true;
  *(reinterpret_cast<StringPiece*>(dest)) = StringPiece(str, n);
  return true;
}

bool RE2::Arg::parse_char(const char* str, size_t n, void* dest) {
  if (n != 1) return false;
  if (dest == NULL) return true;
  *(reinterpret_cast<char*>(dest)) = str[0];
  return true;
}

bool RE2::Arg::parse_schar(const char* str, size_t n, void* dest) {
  if (n != 1) return false;
  if (dest == NULL) return true;
  *(reinterpret_cast<signed char*>(dest)) = str[0];
  return true;
}

bool RE2::Arg::parse_uchar(const char* str, size_t n, void* dest) {
  if (n != 1) return false;
  if (dest == NULL) return true;
  *(reinterpret_cast<unsigned char*>(dest)) = str[0];
  return true;
}

// Largest number spec that we are willing to parse
static const int kMaxNumberLength = 32;

// REQUIRES "buf" must have length at least nbuf.
// Copies "str" into "buf" and null-terminates.
// Overwrites *np with the new length.
static const char* TerminateNumber(char* buf, size_t nbuf, const char* str,
                                   size_t* np, bool accept_spaces) {
  size_t n = *np;
  if (n == 0) return "";
  if (n > 0 && isspace(*str)) {
    // We are less forgiving than the strtoxxx() routines and do not
    // allow leading spaces. We do allow leading spaces for floats.
    if (!accept_spaces) {
      return "";
    }
    while (n > 0 && isspace(*str)) {
      n--;
      str++;
    }
  }

  // Although buf has a fixed maximum size, we can still handle
  // arbitrarily large integers correctly by omitting leading zeros.
  // (Numbers that are still too long will be out of range.)
  // Before deciding whether str is too long,
  // remove leading zeros with s/000+/00/.
  // Leaving the leading two zeros in place means that
  // we don't change 0000x123 (invalid) into 0x123 (valid).
  // Skip over leading - before replacing.
  bool neg = false;
  if (n >= 1 && str[0] == '-') {
    neg = true;
    n--;
    str++;
  }

  if (n >= 3 && str[0] == '0' && str[1] == '0') {
    while (n >= 3 && str[2] == '0') {
      n--;
      str++;
    }
  }

  if (neg) {  // make room in buf for -
    n++;
    str--;
  }

  if (n > nbuf-1) return "";

  memmove(buf, str, n);
  if (neg) {
    buf[0] = '-';
  }
  buf[n] = '\0';
  *np = n;
  return buf;
}

bool RE2::Arg::parse_long_radix(const char* str,
                                size_t n,
                                void* dest,
                                int radix) {
  if (n == 0) return false;
  char buf[kMaxNumberLength+1];
  str = TerminateNumber(buf, sizeof buf, str, &n, false);
  char* end;
  errno = 0;
  long r = strtol(str, &end, radix);
  if (end != str + n) return false;   // Leftover junk
  if (errno) return false;
  if (dest == NULL) return true;
  *(reinterpret_cast<long*>(dest)) = r;
  return true;
}

bool RE2::Arg::parse_ulong_radix(const char* str,
                                 size_t n,
                                 void* dest,
                                 int radix) {
  if (n == 0) return false;
  char buf[kMaxNumberLength+1];
  str = TerminateNumber(buf, sizeof buf, str, &n, false);
  if (str[0] == '-') {
    // strtoul() will silently accept negative numbers and parse
    // them.  This module is more strict and treats them as errors.
    return false;
  }

  char* end;
  errno = 0;
  unsigned long r = strtoul(str, &end, radix);
  if (end != str + n) return false;   // Leftover junk
  if (errno) return false;
  if (dest == NULL) return true;
  *(reinterpret_cast<unsigned long*>(dest)) = r;
  return true;
}

bool RE2::Arg::parse_short_radix(const char* str,
                                 size_t n,
                                 void* dest,
                                 int radix) {
  long r;
  if (!parse_long_radix(str, n, &r, radix)) return false;  // Could not parse
  if ((short)r != r) return false;                         // Out of range
  if (dest == NULL) return true;
  *(reinterpret_cast<short*>(dest)) = (short)r;
  return true;
}

bool RE2::Arg::parse_ushort_radix(const char* str,
                                  size_t n,
                                  void* dest,
                                  int radix) {
  unsigned long r;
  if (!parse_ulong_radix(str, n, &r, radix)) return false;  // Could not parse
  if ((unsigned short)r != r) return false;                 // Out of range
  if (dest == NULL) return true;
  *(reinterpret_cast<unsigned short*>(dest)) = (unsigned short)r;
  return true;
}

bool RE2::Arg::parse_int_radix(const char* str,
                               size_t n,
                               void* dest,
                               int radix) {
  long r;
  if (!parse_long_radix(str, n, &r, radix)) return false;  // Could not parse
  if ((int)r != r) return false;                           // Out of range
  if (dest == NULL) return true;
  *(reinterpret_cast<int*>(dest)) = (int)r;
  return true;
}

bool RE2::Arg::parse_uint_radix(const char* str,
                                size_t n,
                                void* dest,
                                int radix) {
  unsigned long r;
  if (!parse_ulong_radix(str, n, &r, radix)) return false;  // Could not parse
  if ((unsigned int)r != r) return false;                   // Out of range
  if (dest == NULL) return true;
  *(reinterpret_cast<unsigned int*>(dest)) = (unsigned int)r;
  return true;
}

bool RE2::Arg::parse_longlong_radix(const char* str,
                                    size_t n,
                                    void* dest,
                                    int radix) {
  if (n == 0) return false;
  char buf[kMaxNumberLength+1];
  str = TerminateNumber(buf, sizeof buf, str, &n, false);
  char* end;
  errno = 0;
  long long r = strtoll(str, &end, radix);
  if (end != str + n) return false;   // Leftover junk
  if (errno) return false;
  if (dest == NULL) return true;
  *(reinterpret_cast<long long*>(dest)) = r;
  return true;
}

bool RE2::Arg::parse_ulonglong_radix(const char* str,
                                     size_t n,
                                     void* dest,
                                     int radix) {
  if (n == 0) return false;
  char buf[kMaxNumberLength+1];
  str = TerminateNumber(buf, sizeof buf, str, &n, false);
  if (str[0] == '-') {
    // strtoull() will silently accept negative numbers and parse
    // them.  This module is more strict and treats them as errors.
    return false;
  }
  char* end;
  errno = 0;
  unsigned long long r = strtoull(str, &end, radix);
  if (end != str + n) return false;   // Leftover junk
  if (errno) return false;
  if (dest == NULL) return true;
  *(reinterpret_cast<unsigned long long*>(dest)) = r;
  return true;
}

static bool parse_double_float(const char* str, size_t n, bool isfloat,
                               void* dest) {
  if (n == 0) return false;
  static const int kMaxLength = 200;
  char buf[kMaxLength+1];
  str = TerminateNumber(buf, sizeof buf, str, &n, true);
  char* end;
  errno = 0;
  double r;
  if (isfloat) {
    r = strtof(str, &end);
  } else {
    r = strtod(str, &end);
  }
  if (end != str + n) return false;   // Leftover junk
  if (errno) return false;
  if (dest == NULL) return true;
  if (isfloat) {
    *(reinterpret_cast<float*>(dest)) = (float)r;
  } else {
    *(reinterpret_cast<double*>(dest)) = r;
  }
  return true;
}

bool RE2::Arg::parse_double(const char* str, size_t n, void* dest) {
  return parse_double_float(str, n, false, dest);
}

bool RE2::Arg::parse_float(const char* str, size_t n, void* dest) {
  return parse_double_float(str, n, true, dest);
}

#define DEFINE_INTEGER_PARSER(name)                                            \
  bool RE2::Arg::parse_##name(const char* str, size_t n, void* dest) {         \
    return parse_##name##_radix(str, n, dest, 10);                             \
  }                                                                            \
  bool RE2::Arg::parse_##name##_hex(const char* str, size_t n, void* dest) {   \
    return parse_##name##_radix(str, n, dest, 16);                             \
  }                                                                            \
  bool RE2::Arg::parse_##name##_octal(const char* str, size_t n, void* dest) { \
    return parse_##name##_radix(str, n, dest, 8);                              \
  }                                                                            \
  bool RE2::Arg::parse_##name##_cradix(const char* str, size_t n,              \
                                       void* dest) {                           \
    return parse_##name##_radix(str, n, dest, 0);                              \
  }

DEFINE_INTEGER_PARSER(short)
DEFINE_INTEGER_PARSER(ushort)
DEFINE_INTEGER_PARSER(int)
DEFINE_INTEGER_PARSER(uint)
DEFINE_INTEGER_PARSER(long)
DEFINE_INTEGER_PARSER(ulong)
DEFINE_INTEGER_PARSER(longlong)
DEFINE_INTEGER_PARSER(ulonglong)

#undef DEFINE_INTEGER_PARSER

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2006 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Regular expression representation.
// Tested by parse_test.cc



#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <map>
#include <mutex>
#include <string>
#include <vector>








namespace duckdb_re2 {

// Constructor.  Allocates vectors as appropriate for operator.
Regexp::Regexp(RegexpOp op, ParseFlags parse_flags)
  : op_(static_cast<uint8_t>(op)),
    simple_(false),
    parse_flags_(static_cast<uint16_t>(parse_flags)),
    ref_(1),
    nsub_(0),
    down_(NULL) {
  subone_ = NULL;
  memset(the_union_, 0, sizeof the_union_);
}

// Destructor.  Assumes already cleaned up children.
// Private: use Decref() instead of delete to destroy Regexps.
// Can't call Decref on the sub-Regexps here because
// that could cause arbitrarily deep recursion, so
// required Decref() to have handled them for us.
Regexp::~Regexp() {
  if (nsub_ > 0)
    LOG(DFATAL) << "Regexp not destroyed.";

  switch (op_) {
    default:
      break;
    case kRegexpCapture:
      delete capture_.name_;
      break;
    case kRegexpLiteralString:
      delete[] literal_string_.runes_;
      break;
    case kRegexpCharClass:
      if (char_class_.cc_)
          char_class_.cc_->Delete();
      delete char_class_.ccb_;
      break;
  }
}

// If it's possible to destroy this regexp without recurring,
// do so and return true.  Else return false.
bool Regexp::QuickDestroy() {
  if (nsub_ == 0) {
    delete this;
    return true;
  }
  return false;
}

// Lazily allocated.
static Mutex* ref_mutex;
static std::map<Regexp*, int>* ref_map;

int Regexp::Ref() {
  if (ref_ < kMaxRef)
    return ref_;

  MutexLock l(ref_mutex);
  return (*ref_map)[this];
}

// Increments reference count, returns object as convenience.
Regexp* Regexp::Incref() {
  if (ref_ >= kMaxRef-1) {
    static std::once_flag ref_once;
    std::call_once(ref_once, []() {
      ref_mutex = new Mutex;
      ref_map = new std::map<Regexp*, int>;
    });

    // Store ref count in overflow map.
    MutexLock l(ref_mutex);
    if (ref_ == kMaxRef) {
      // already overflowed
      (*ref_map)[this]++;
    } else {
      // overflowing now
      (*ref_map)[this] = kMaxRef;
      ref_ = kMaxRef;
    }
    return this;
  }

  ref_++;
  return this;
}

// Decrements reference count and deletes this object if count reaches 0.
void Regexp::Decref() {
  if (ref_ == kMaxRef) {
    // Ref count is stored in overflow map.
    MutexLock l(ref_mutex);
    int r = (*ref_map)[this] - 1;
    if (r < kMaxRef) {
      ref_ = static_cast<uint16_t>(r);
      ref_map->erase(this);
    } else {
      (*ref_map)[this] = r;
    }
    return;
  }
  ref_--;
  if (ref_ == 0)
    Destroy();
}

// Deletes this object; ref count has count reached 0.
void Regexp::Destroy() {
  if (QuickDestroy())
    return;

  // Handle recursive Destroy with explicit stack
  // to avoid arbitrarily deep recursion on process stack [sigh].
  down_ = NULL;
  Regexp* stack = this;
  while (stack != NULL) {
    Regexp* re = stack;
    stack = re->down_;
    if (re->ref_ != 0)
      LOG(DFATAL) << "Bad reference count " << re->ref_;
    if (re->nsub_ > 0) {
      Regexp** subs = re->sub();
      for (int i = 0; i < re->nsub_; i++) {
        Regexp* sub = subs[i];
        if (sub == NULL)
          continue;
        if (sub->ref_ == kMaxRef)
          sub->Decref();
        else
          --sub->ref_;
        if (sub->ref_ == 0 && !sub->QuickDestroy()) {
          sub->down_ = stack;
          stack = sub;
        }
      }
      if (re->nsub_ > 1)
        delete[] subs;
      re->nsub_ = 0;
    }
    delete re;
  }
}

void Regexp::AddRuneToString(Rune r) {
  DCHECK(op_ == kRegexpLiteralString);
  if (literal_string_.nrunes_ == 0) {
    // start with 8
      literal_string_.runes_ = new Rune[8];
  } else if (literal_string_.nrunes_ >= 8 && (literal_string_.nrunes_ & (literal_string_.nrunes_ - 1)) == 0) {
    // double on powers of two
    Rune *old = literal_string_.runes_;
      literal_string_.runes_ = new Rune[literal_string_.nrunes_ * 2];
    for (int i = 0; i < literal_string_.nrunes_; i++)
        literal_string_.runes_[i] = old[i];
    delete[] old;
  }

    literal_string_.runes_[literal_string_.nrunes_++] = r;
}

Regexp* Regexp::HaveMatch(int match_id, ParseFlags flags) {
  Regexp* re = new Regexp(kRegexpHaveMatch, flags);
  re->match_id_ = match_id;
  return re;
}

Regexp* Regexp::StarPlusOrQuest(RegexpOp op, Regexp* sub, ParseFlags flags) {
  // Squash **, ++ and ??.
  if (op == sub->op() && flags == sub->parse_flags())
    return sub;

  // Squash *+, *?, +*, +?, ?* and ?+. They all squash to *, so because
  // op is Star/Plus/Quest, we just have to check that sub->op() is too.
  if ((sub->op() == kRegexpStar ||
       sub->op() == kRegexpPlus ||
       sub->op() == kRegexpQuest) &&
      flags == sub->parse_flags()) {
    // If sub is Star, no need to rewrite it.
    if (sub->op() == kRegexpStar)
      return sub;

    // Rewrite sub to Star.
    Regexp* re = new Regexp(kRegexpStar, flags);
    re->AllocSub(1);
    re->sub()[0] = sub->sub()[0]->Incref();
    sub->Decref();  // We didn't consume the reference after all.
    return re;
  }

  Regexp* re = new Regexp(op, flags);
  re->AllocSub(1);
  re->sub()[0] = sub;
  return re;
}

Regexp* Regexp::Plus(Regexp* sub, ParseFlags flags) {
  return StarPlusOrQuest(kRegexpPlus, sub, flags);
}

Regexp* Regexp::Star(Regexp* sub, ParseFlags flags) {
  return StarPlusOrQuest(kRegexpStar, sub, flags);
}

Regexp* Regexp::Quest(Regexp* sub, ParseFlags flags) {
  return StarPlusOrQuest(kRegexpQuest, sub, flags);
}

Regexp* Regexp::ConcatOrAlternate(RegexpOp op, Regexp** sub, int nsub,
                                  ParseFlags flags, bool can_factor) {
  if (nsub == 1)
    return sub[0];

  if (nsub == 0) {
    if (op == kRegexpAlternate)
      return new Regexp(kRegexpNoMatch, flags);
    else
      return new Regexp(kRegexpEmptyMatch, flags);
  }

  Regexp** subcopy = NULL;
  if (op == kRegexpAlternate && can_factor) {
    // Going to edit sub; make a copy so we don't step on caller.
    subcopy = new Regexp*[nsub];
    memmove(subcopy, sub, nsub * sizeof sub[0]);
    sub = subcopy;
    nsub = FactorAlternation(sub, nsub, flags);
    if (nsub == 1) {
      Regexp* re = sub[0];
      delete[] subcopy;
      return re;
    }
  }

  if (nsub > kMaxNsub) {
    // Too many subexpressions to fit in a single Regexp.
    // Make a two-level tree.  Two levels gets us to 65535^2.
    int nbigsub = (nsub+kMaxNsub-1)/kMaxNsub;
    Regexp* re = new Regexp(op, flags);
    re->AllocSub(nbigsub);
    Regexp** subs = re->sub();
    for (int i = 0; i < nbigsub - 1; i++)
      subs[i] = ConcatOrAlternate(op, sub+i*kMaxNsub, kMaxNsub, flags, false);
    subs[nbigsub - 1] = ConcatOrAlternate(op, sub+(nbigsub-1)*kMaxNsub,
                                          nsub - (nbigsub-1)*kMaxNsub, flags,
                                          false);
    delete[] subcopy;
    return re;
  }

  Regexp* re = new Regexp(op, flags);
  re->AllocSub(nsub);
  Regexp** subs = re->sub();
  for (int i = 0; i < nsub; i++)
    subs[i] = sub[i];

  delete[] subcopy;
  return re;
}

Regexp* Regexp::Concat(Regexp** sub, int nsub, ParseFlags flags) {
  return ConcatOrAlternate(kRegexpConcat, sub, nsub, flags, false);
}

Regexp* Regexp::Alternate(Regexp** sub, int nsub, ParseFlags flags) {
  return ConcatOrAlternate(kRegexpAlternate, sub, nsub, flags, true);
}

Regexp* Regexp::AlternateNoFactor(Regexp** sub, int nsub, ParseFlags flags) {
  return ConcatOrAlternate(kRegexpAlternate, sub, nsub, flags, false);
}

Regexp* Regexp::Capture(Regexp* sub, ParseFlags flags, int cap) {
  Regexp* re = new Regexp(kRegexpCapture, flags);
  re->AllocSub(1);
  re->sub()[0] = sub;
  re->capture_.cap_ = cap;
  return re;
}

Regexp* Regexp::Repeat(Regexp* sub, ParseFlags flags, int min, int max) {
  Regexp* re = new Regexp(kRegexpRepeat, flags);
  re->AllocSub(1);
  re->sub()[0] = sub;
  re->repeat_.min_ = min;
  re->repeat_.max_ = max;
  return re;
}

Regexp* Regexp::NewLiteral(Rune rune, ParseFlags flags) {
  Regexp* re = new Regexp(kRegexpLiteral, flags);
  re->rune_ = rune;
  return re;
}

Regexp* Regexp::LiteralString(Rune* runes, int nrunes, ParseFlags flags) {
  if (nrunes <= 0)
    return new Regexp(kRegexpEmptyMatch, flags);
  if (nrunes == 1)
    return NewLiteral(runes[0], flags);
  Regexp* re = new Regexp(kRegexpLiteralString, flags);
  for (int i = 0; i < nrunes; i++)
    re->AddRuneToString(runes[i]);
  return re;
}

Regexp* Regexp::NewCharClass(CharClass* cc, ParseFlags flags) {
  Regexp* re = new Regexp(kRegexpCharClass, flags);
  re->char_class_.cc_ = cc;
  return re;
}

void Regexp::Swap(Regexp* that) {
  // Regexp is not trivially copyable, so we cannot freely copy it with
  // memmove(3), but swapping objects like so is safe for our purposes.
  char tmp[sizeof *this];
  void* vthis = reinterpret_cast<void*>(this);
  void* vthat = reinterpret_cast<void*>(that);
  memmove(tmp, vthis, sizeof *this);
  memmove(vthis, vthat, sizeof *this);
  memmove(vthat, tmp, sizeof *this);
}

// Tests equality of all top-level structure but not subregexps.
static bool TopEqual(Regexp* a, Regexp* b) {
  if (a->op() != b->op())
    return false;

  switch (a->op()) {
    case kRegexpNoMatch:
    case kRegexpEmptyMatch:
    case kRegexpAnyChar:
    case kRegexpAnyByte:
    case kRegexpBeginLine:
    case kRegexpEndLine:
    case kRegexpWordBoundary:
    case kRegexpNoWordBoundary:
    case kRegexpBeginText:
      return true;

    case kRegexpEndText:
      // The parse flags remember whether it's \z or (?-m:$),
      // which matters when testing against PCRE.
      return ((a->parse_flags() ^ b->parse_flags()) & Regexp::WasDollar) == 0;

    case kRegexpLiteral:
      return a->rune() == b->rune() &&
             ((a->parse_flags() ^ b->parse_flags()) & Regexp::FoldCase) == 0;

    case kRegexpLiteralString:
      return a->nrunes() == b->nrunes() &&
             ((a->parse_flags() ^ b->parse_flags()) & Regexp::FoldCase) == 0 &&
             memcmp(a->runes(), b->runes(),
                    a->nrunes() * sizeof a->runes()[0]) == 0;

    case kRegexpAlternate:
    case kRegexpConcat:
      return a->nsub() == b->nsub();

    case kRegexpStar:
    case kRegexpPlus:
    case kRegexpQuest:
      return ((a->parse_flags() ^ b->parse_flags()) & Regexp::NonGreedy) == 0;

    case kRegexpRepeat:
      return ((a->parse_flags() ^ b->parse_flags()) & Regexp::NonGreedy) == 0 &&
             a->min() == b->min() &&
             a->max() == b->max();

    case kRegexpCapture:
      return a->cap() == b->cap() && a->name() == b->name();

    case kRegexpHaveMatch:
      return a->match_id() == b->match_id();

    case kRegexpCharClass: {
      CharClass* acc = a->cc();
      CharClass* bcc = b->cc();
      return acc->size() == bcc->size() &&
             acc->end() - acc->begin() == bcc->end() - bcc->begin() &&
             memcmp(acc->begin(), bcc->begin(),
                    (acc->end() - acc->begin()) * sizeof acc->begin()[0]) == 0;
    }
  }

  LOG(DFATAL) << "Unexpected op in Regexp::Equal: " << a->op();
  return 0;
}

bool Regexp::Equal(Regexp* a, Regexp* b) {
  if (a == NULL || b == NULL)
    return a == b;

  if (!TopEqual(a, b))
    return false;

  // Fast path:
  // return without allocating vector if there are no subregexps.
  switch (a->op()) {
    case kRegexpAlternate:
    case kRegexpConcat:
    case kRegexpStar:
    case kRegexpPlus:
    case kRegexpQuest:
    case kRegexpRepeat:
    case kRegexpCapture:
      break;

    default:
      return true;
  }

  // Committed to doing real work.
  // The stack (vector) has pairs of regexps waiting to
  // be compared.  The regexps are only equal if
  // all the pairs end up being equal.
  std::vector<Regexp*> stk;

  for (;;) {
    // Invariant: TopEqual(a, b) == true.
    Regexp* a2;
    Regexp* b2;
    switch (a->op()) {
      default:
        break;
      case kRegexpAlternate:
      case kRegexpConcat:
        for (int i = 0; i < a->nsub(); i++) {
          a2 = a->sub()[i];
          b2 = b->sub()[i];
          if (!TopEqual(a2, b2))
            return false;
          stk.push_back(a2);
          stk.push_back(b2);
        }
        break;

      case kRegexpStar:
      case kRegexpPlus:
      case kRegexpQuest:
      case kRegexpRepeat:
      case kRegexpCapture:
        a2 = a->sub()[0];
        b2 = b->sub()[0];
        if (!TopEqual(a2, b2))
          return false;
        // Really:
        //   stk.push_back(a2);
        //   stk.push_back(b2);
        //   break;
        // but faster to assign directly and loop.
        a = a2;
        b = b2;
        continue;
    }

    size_t n = stk.size();
    if (n == 0)
      break;

    DCHECK_GE(n, 2);
    a = stk[n-2];
    b = stk[n-1];
    stk.resize(n-2);
  }

  return true;
}

// Keep in sync with enum RegexpStatusCode in regexp.h
static const char *kErrorStrings[] = {
  "no error",
  "unexpected error",
  "invalid escape sequence",
  "invalid character class",
  "invalid character class range",
  "missing ]",
  "missing )",
  "trailing \\",
  "no argument for repetition operator",
  "invalid repetition size",
  "bad repetition operator",
  "invalid perl operator",
  "invalid UTF-8",
  "invalid named capture group",
};

std::string RegexpStatus::CodeText(enum RegexpStatusCode code) {
  if (code < 0 || code >= arraysize(kErrorStrings))
    code = kRegexpInternalError;
  return kErrorStrings[code];
}

std::string RegexpStatus::Text() const {
  if (error_arg_.empty())
    return CodeText(code_);
  std::string s;
  s.append(CodeText(code_));
  s.append(": ");
  s.append(error_arg_.data(), error_arg_.size());
  return s;
}

void RegexpStatus::Copy(const RegexpStatus& status) {
  code_ = status.code_;
  error_arg_ = status.error_arg_;
}

typedef int Ignored;  // Walker<void> doesn't exist

// Walker subclass to count capturing parens in regexp.
class NumCapturesWalker : public Regexp::Walker<Ignored> {
 public:
  NumCapturesWalker() : ncapture_(0) {}
  int ncapture() { return ncapture_; }

  virtual Ignored PreVisit(Regexp* re, Ignored ignored, bool* stop) {
    if (re->op() == kRegexpCapture)
      ncapture_++;
    return ignored;
  }
  virtual Ignored ShortVisit(Regexp* re, Ignored ignored) {
    // Should never be called: we use Walk not WalkExponential.
    LOG(DFATAL) << "NumCapturesWalker::ShortVisit called";
    return ignored;
  }

 private:
  int ncapture_;

  NumCapturesWalker(const NumCapturesWalker&) = delete;
  NumCapturesWalker& operator=(const NumCapturesWalker&) = delete;
};

int Regexp::NumCaptures() {
  NumCapturesWalker w;
  w.Walk(this, 0);
  return w.ncapture();
}

// Walker class to build map of named capture groups and their indices.
class NamedCapturesWalker : public Regexp::Walker<Ignored> {
 public:
  NamedCapturesWalker() : map_(NULL) {}
  ~NamedCapturesWalker() { delete map_; }

  std::map<std::string, int>* TakeMap() {
    std::map<std::string, int>* m = map_;
    map_ = NULL;
    return m;
  }

  Ignored PreVisit(Regexp* re, Ignored ignored, bool* stop) {
    if (re->op() == kRegexpCapture && re->name() != NULL) {
      // Allocate map once we find a name.
      if (map_ == NULL)
        map_ = new std::map<std::string, int>;

      // Record first occurrence of each name.
      // (The rule is that if you have the same name
      // multiple times, only the leftmost one counts.)
      if (map_->find(*re->name()) == map_->end())
        (*map_)[*re->name()] = re->cap();
    }
    return ignored;
  }

  virtual Ignored ShortVisit(Regexp* re, Ignored ignored) {
    // Should never be called: we use Walk not WalkExponential.
    LOG(DFATAL) << "NamedCapturesWalker::ShortVisit called";
    return ignored;
  }

 private:
  std::map<std::string, int>* map_;

  NamedCapturesWalker(const NamedCapturesWalker&) = delete;
  NamedCapturesWalker& operator=(const NamedCapturesWalker&) = delete;
};

std::map<std::string, int>* Regexp::NamedCaptures() {
  NamedCapturesWalker w;
  w.Walk(this, 0);
  return w.TakeMap();
}

// Walker class to build map from capture group indices to their names.
class CaptureNamesWalker : public Regexp::Walker<Ignored> {
 public:
  CaptureNamesWalker() : map_(NULL) {}
  ~CaptureNamesWalker() { delete map_; }

  std::map<int, std::string>* TakeMap() {
    std::map<int, std::string>* m = map_;
    map_ = NULL;
    return m;
  }

  Ignored PreVisit(Regexp* re, Ignored ignored, bool* stop) {
    if (re->op() == kRegexpCapture && re->name() != NULL) {
      // Allocate map once we find a name.
      if (map_ == NULL)
        map_ = new std::map<int, std::string>;

      (*map_)[re->cap()] = *re->name();
    }
    return ignored;
  }

  virtual Ignored ShortVisit(Regexp* re, Ignored ignored) {
    // Should never be called: we use Walk not WalkExponential.
    LOG(DFATAL) << "CaptureNamesWalker::ShortVisit called";
    return ignored;
  }

 private:
  std::map<int, std::string>* map_;

  CaptureNamesWalker(const CaptureNamesWalker&) = delete;
  CaptureNamesWalker& operator=(const CaptureNamesWalker&) = delete;
};

std::map<int, std::string>* Regexp::CaptureNames() {
  CaptureNamesWalker w;
  w.Walk(this, 0);
  return w.TakeMap();
}

// Determines whether regexp matches must be anchored
// with a fixed string prefix.  If so, returns the prefix and
// the regexp that remains after the prefix.  The prefix might
// be ASCII case-insensitive.
bool Regexp::RequiredPrefix(std::string* prefix, bool* foldcase,
                            Regexp** suffix) {
  // No need for a walker: the regexp must be of the form
  // 1. some number of ^ anchors
  // 2. a literal char or string
  // 3. the rest
  prefix->clear();
  *foldcase = false;
  *suffix = NULL;
  if (op_ != kRegexpConcat)
    return false;

  // Some number of anchors, then a literal or concatenation.
  int i = 0;
  Regexp** sub = this->sub();
  while (i < nsub_ && sub[i]->op_ == kRegexpBeginText)
    i++;
  if (i == 0 || i >= nsub_)
    return false;

  Regexp* re = sub[i];
  switch (re->op_) {
    default:
      return false;

    case kRegexpLiteralString:
      // Convert to string in proper encoding.
      if (re->parse_flags() & Latin1) {
        prefix->resize(re->literal_string_.nrunes_);
        for (int j = 0; j < re->literal_string_.nrunes_; j++)
          (*prefix)[j] = static_cast<char>(re->literal_string_.runes_[j]);
      } else {
        // Convert to UTF-8 in place.
        // Assume worst-case space and then trim.
        prefix->resize(re->literal_string_.nrunes_ * UTFmax);
        char *p = &(*prefix)[0];
        for (int j = 0; j < re->literal_string_.nrunes_; j++) {
          Rune r = re->literal_string_.runes_[j];
          if (r < Runeself)
            *p++ = static_cast<char>(r);
          else
            p += runetochar(p, &r);
        }
        prefix->resize(p - &(*prefix)[0]);
      }
      break;

    case kRegexpLiteral:
      if ((re->parse_flags() & Latin1) || re->rune_ < Runeself) {
        prefix->append(1, static_cast<char>(re->rune_));
      } else {
        char buf[UTFmax];
        prefix->append(buf, runetochar(buf, &re->rune_));
      }
      break;
  }
  *foldcase = (sub[i]->parse_flags() & FoldCase) != 0;
  i++;

  // The rest.
  if (i < nsub_) {
    for (int j = i; j < nsub_; j++)
      sub[j]->Incref();
    re = Concat(sub + i, nsub_ - i, parse_flags());
  } else {
    re = new Regexp(kRegexpEmptyMatch, parse_flags());
  }
  *suffix = re;
  return true;
}

// Character class builder is a balanced binary tree (STL set)
// containing non-overlapping, non-abutting RuneRanges.
// The less-than operator used in the tree treats two
// ranges as equal if they overlap at all, so that
// lookups for a particular Rune are possible.

CharClassBuilder::CharClassBuilder() {
  nrunes_ = 0;
  upper_ = 0;
  lower_ = 0;
}

// Add lo-hi to the class; return whether class got bigger.
bool CharClassBuilder::AddRange(Rune lo, Rune hi) {
  if (hi < lo)
    return false;

  if (lo <= 'z' && hi >= 'A') {
    // Overlaps some alpha, maybe not all.
    // Update bitmaps telling which ASCII letters are in the set.
    Rune lo1 = std::max<Rune>(lo, 'A');
    Rune hi1 = std::min<Rune>(hi, 'Z');
    if (lo1 <= hi1)
      upper_ |= ((1 << (hi1 - lo1 + 1)) - 1) << (lo1 - 'A');

    lo1 = std::max<Rune>(lo, 'a');
    hi1 = std::min<Rune>(hi, 'z');
    if (lo1 <= hi1)
      lower_ |= ((1 << (hi1 - lo1 + 1)) - 1) << (lo1 - 'a');
  }

  {  // Check whether lo, hi is already in the class.
    iterator it = ranges_.find(RuneRange(lo, lo));
    if (it != end() && it->lo <= lo && hi <= it->hi)
      return false;
  }

  // Look for a range abutting lo on the left.
  // If it exists, take it out and increase our range.
  if (lo > 0) {
    iterator it = ranges_.find(RuneRange(lo-1, lo-1));
    if (it != end()) {
      lo = it->lo;
      if (it->hi > hi)
        hi = it->hi;
      nrunes_ -= it->hi - it->lo + 1;
      ranges_.erase(it);
    }
  }

  // Look for a range abutting hi on the right.
  // If it exists, take it out and increase our range.
  if (hi < Runemax) {
    iterator it = ranges_.find(RuneRange(hi+1, hi+1));
    if (it != end()) {
      hi = it->hi;
      nrunes_ -= it->hi - it->lo + 1;
      ranges_.erase(it);
    }
  }

  // Look for ranges between lo and hi.  Take them out.
  // This is only safe because the set has no overlapping ranges.
  // We've already removed any ranges abutting lo and hi, so
  // any that overlap [lo, hi] must be contained within it.
  for (;;) {
    iterator it = ranges_.find(RuneRange(lo, hi));
    if (it == end())
      break;
    nrunes_ -= it->hi - it->lo + 1;
    ranges_.erase(it);
  }

  // Finally, add [lo, hi].
  nrunes_ += hi - lo + 1;
  ranges_.insert(RuneRange(lo, hi));
  return true;
}

void CharClassBuilder::AddCharClass(CharClassBuilder *cc) {
  for (iterator it = cc->begin(); it != cc->end(); ++it)
    AddRange(it->lo, it->hi);
}

bool CharClassBuilder::Contains(Rune r) {
  return ranges_.find(RuneRange(r, r)) != end();
}

// Does the character class behave the same on A-Z as on a-z?
bool CharClassBuilder::FoldsASCII() {
  return ((upper_ ^ lower_) & AlphaMask) == 0;
}

CharClassBuilder* CharClassBuilder::Copy() {
  CharClassBuilder* cc = new CharClassBuilder;
  for (iterator it = begin(); it != end(); ++it)
    cc->ranges_.insert(RuneRange(it->lo, it->hi));
  cc->upper_ = upper_;
  cc->lower_ = lower_;
  cc->nrunes_ = nrunes_;
  return cc;
}



void CharClassBuilder::RemoveAbove(Rune r) {
  if (r >= Runemax)
    return;

  if (r < 'z') {
    if (r < 'a')
      lower_ = 0;
    else
      lower_ &= AlphaMask >> ('z' - r);
  }

  if (r < 'Z') {
    if (r < 'A')
      upper_ = 0;
    else
      upper_ &= AlphaMask >> ('Z' - r);
  }

  for (;;) {

    iterator it = ranges_.find(RuneRange(r + 1, Runemax));
    if (it == end())
      break;
    RuneRange rr = *it;
    ranges_.erase(it);
    nrunes_ -= rr.hi - rr.lo + 1;
    if (rr.lo <= r) {
      rr.hi = r;
      ranges_.insert(rr);
      nrunes_ += rr.hi - rr.lo + 1;
    }
  }
}

void CharClassBuilder::Negate() {
  // Build up negation and then copy in.
  // Could edit ranges in place, but C++ won't let me.
  std::vector<RuneRange> v;
  v.reserve(ranges_.size() + 1);

  // In negation, first range begins at 0, unless
  // the current class begins at 0.
  iterator it = begin();
  if (it == end()) {
    v.push_back(RuneRange(0, Runemax));
  } else {
    int nextlo = 0;
    if (it->lo == 0) {
      nextlo = it->hi + 1;
      ++it;
    }
    for (; it != end(); ++it) {
      v.push_back(RuneRange(nextlo, it->lo - 1));
      nextlo = it->hi + 1;
    }
    if (nextlo <= Runemax)
      v.push_back(RuneRange(nextlo, Runemax));
  }

  ranges_.clear();
  for (size_t i = 0; i < v.size(); i++)
    ranges_.insert(v[i]);

  upper_ = AlphaMask & ~upper_;
  lower_ = AlphaMask & ~lower_;
  nrunes_ = Runemax+1 - nrunes_;
}

// Character class is a sorted list of ranges.
// The ranges are allocated in the same block as the header,
// necessitating a special allocator and Delete method.

CharClass* CharClass::New(int maxranges) {
  CharClass* cc;
  uint8_t* data = new uint8_t[sizeof *cc + maxranges*sizeof cc->ranges_[0]];
  cc = reinterpret_cast<CharClass*>(data);
  cc->ranges_ = reinterpret_cast<RuneRange*>(data + sizeof *cc);
  cc->nranges_ = 0;
  cc->folds_ascii_ = false;
  cc->nrunes_ = 0;
  return cc;
}

void CharClass::Delete() {
  uint8_t* data = reinterpret_cast<uint8_t*>(this);
  delete[] data;
}

CharClass* CharClass::Negate() {
  CharClass* cc = CharClass::New(nranges_+1);
  cc->folds_ascii_ = folds_ascii_;
  cc->nrunes_ = Runemax + 1 - nrunes_;
  int n = 0;
  int nextlo = 0;
  for (CharClass::iterator it = begin(); it != end(); ++it) {
    if (it->lo == nextlo) {
      nextlo = it->hi + 1;
    } else {
      cc->ranges_[n++] = RuneRange(nextlo, it->lo - 1);
      nextlo = it->hi + 1;
    }
  }
  if (nextlo <= Runemax)
    cc->ranges_[n++] = RuneRange(nextlo, Runemax);
  cc->nranges_ = n;
  return cc;
}

bool CharClass::Contains(Rune r) {
  RuneRange* rr = ranges_;
  int n = nranges_;
  while (n > 0) {
    int m = n/2;
    if (rr[m].hi < r) {
      rr += m+1;
      n -= m+1;
    } else if (r < rr[m].lo) {
      n = m;
    } else {  // rr[m].lo <= r && r <= rr[m].hi
      return true;
    }
  }
  return false;
}

CharClass* CharClassBuilder::GetCharClass() {
  CharClass* cc = CharClass::New(static_cast<int>(ranges_.size()));
  int n = 0;
  for (iterator it = begin(); it != end(); ++it)
    cc->ranges_[n++] = *it;
  cc->nranges_ = n;
  DCHECK_LE(n, static_cast<int>(ranges_.size()));
  cc->nrunes_ = nrunes_;
  cc->folds_ascii_ = FoldsASCII();
  return cc;
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2010 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.



// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2010 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef RE2_SET_H_
#define RE2_SET_H_

#include <string>
#include <utility>
#include <vector>



namespace duckdb_re2 {
class Prog;
class Regexp;
}  // namespace duckdb_re2

namespace duckdb_re2 {

// An RE2::Set represents a collection of regexps that can
// be searched for simultaneously.
class RE2::Set {
 public:
  enum ErrorKind {
    kNoError = 0,
    kNotCompiled,   // The set is not compiled.
    kOutOfMemory,   // The DFA ran out of memory.
    kInconsistent,  // The result is inconsistent. This should never happen.
  };

  struct ErrorInfo {
    ErrorKind kind;
  };

  Set(const RE2::Options& options, RE2::Anchor anchor);
  ~Set();

  // Adds pattern to the set using the options passed to the constructor.
  // Returns the index that will identify the regexp in the output of Match(),
  // or -1 if the regexp cannot be parsed.
  // Indices are assigned in sequential order starting from 0.
  // Errors do not increment the index; if error is not NULL, *error will hold
  // the error message from the parser.
  int Add(const StringPiece& pattern, std::string* error);

  // Compiles the set in preparation for matching.
  // Returns false if the compiler runs out of memory.
  // Add() must not be called again after Compile().
  // Compile() must be called before Match().
  bool Compile();

  // Returns true if text matches at least one of the regexps in the set.
  // Fills v (if not NULL) with the indices of the matching regexps.
  // Callers must not expect v to be sorted.
  bool Match(const StringPiece& text, std::vector<int>* v) const;

  // As above, but populates error_info (if not NULL) when none of the regexps
  // in the set matched. This can inform callers when DFA execution fails, for
  // example, because they might wish to handle that case differently.
  bool Match(const StringPiece& text, std::vector<int>* v,
             ErrorInfo* error_info) const;

 private:
  typedef std::pair<std::string, duckdb_re2::Regexp*> Elem;

  RE2::Options options_;
  RE2::Anchor anchor_;
  std::vector<Elem> elem_;
  duckdb_re2::Prog* prog_;
  bool compiled_;
  int size_;

  Set(const Set&) = delete;
  Set& operator=(const Set&) = delete;
};

}  // namespace duckdb_re2

#endif  // RE2_SET_H_


// LICENSE_CHANGE_END


#include <stddef.h>
#include <algorithm>
#include <memory>









namespace duckdb_re2 {

RE2::Set::Set(const RE2::Options& options, RE2::Anchor anchor) {
  options_.Copy(options);
  options_.set_never_capture(true);  // might unblock some optimisations
  anchor_ = anchor;
  prog_ = NULL;
  compiled_ = false;
  size_ = 0;
}

RE2::Set::~Set() {
  for (size_t i = 0; i < elem_.size(); i++)
    elem_[i].second->Decref();
  delete prog_;
}

int RE2::Set::Add(const StringPiece& pattern, std::string* error) {
  if (compiled_) {
    LOG(DFATAL) << "RE2::Set::Add() called after compiling";
    return -1;
  }

  Regexp::ParseFlags pf = static_cast<Regexp::ParseFlags>(
    options_.ParseFlags());
  RegexpStatus status;
  duckdb_re2::Regexp* re = Regexp::Parse(pattern, pf, &status);
  if (re == NULL) {
    if (error != NULL)
      *error = status.Text();
    if (options_.log_errors())
      LOG(ERROR) << "Error parsing '" << pattern << "': " << status.Text();
    return -1;
  }

  // Concatenate with match index and push on vector.
  int n = static_cast<int>(elem_.size());
  duckdb_re2::Regexp* m = duckdb_re2::Regexp::HaveMatch(n, pf);
  if (re->op() == kRegexpConcat) {
    int nsub = re->nsub();
    PODArray<duckdb_re2::Regexp*> sub(nsub + 1);
    for (int i = 0; i < nsub; i++)
      sub[i] = re->sub()[i]->Incref();
    sub[nsub] = m;
    re->Decref();
    re = duckdb_re2::Regexp::Concat(sub.data(), nsub + 1, pf);
  } else {
    duckdb_re2::Regexp* sub[2];
    sub[0] = re;
    sub[1] = m;
    re = duckdb_re2::Regexp::Concat(sub, 2, pf);
  }
  elem_.emplace_back(std::string(pattern), re);
  return n;
}

bool RE2::Set::Compile() {
  if (compiled_) {
    LOG(DFATAL) << "RE2::Set::Compile() called more than once";
    return false;
  }
  compiled_ = true;
  size_ = static_cast<int>(elem_.size());

  // Sort the elements by their patterns. This is good enough for now
  // until we have a Regexp comparison function. (Maybe someday...)
  std::sort(elem_.begin(), elem_.end(),
            [](const Elem& a, const Elem& b) -> bool {
              return a.first < b.first;
            });

  PODArray<duckdb_re2::Regexp*> sub(size_);
  for (int i = 0; i < size_; i++)
    sub[i] = elem_[i].second;
  elem_.clear();
  elem_.shrink_to_fit();

  Regexp::ParseFlags pf = static_cast<Regexp::ParseFlags>(
    options_.ParseFlags());
  duckdb_re2::Regexp* re = duckdb_re2::Regexp::Alternate(sub.data(), size_, pf);

  prog_ = Prog::CompileSet(re, anchor_, options_.max_mem());
  re->Decref();
  return prog_ != NULL;
}

bool RE2::Set::Match(const StringPiece& text, std::vector<int>* v) const {
  return Match(text, v, NULL);
}

bool RE2::Set::Match(const StringPiece& text, std::vector<int>* v,
                     ErrorInfo* error_info) const {
  if (!compiled_) {
    LOG(DFATAL) << "RE2::Set::Match() called before compiling";
    if (error_info != NULL)
      error_info->kind = kNotCompiled;
    return false;
  }
  bool dfa_failed = false;
  std::unique_ptr<SparseSet> matches;
  if (v != NULL) {
    matches.reset(new SparseSet(size_));
    v->clear();
  }
  bool ret = prog_->SearchDFA(text, text, Prog::kAnchored, Prog::kManyMatch,
                              NULL, &dfa_failed, matches.get());
  if (dfa_failed) {
    if (options_.log_errors())
      LOG(ERROR) << "DFA out of memory: size " << prog_->size() << ", "
                 << "bytemap range " << prog_->bytemap_range() << ", "
                 << "list count " << prog_->list_count();
    if (error_info != NULL)
      error_info->kind = kOutOfMemory;
    return false;
  }
  if (ret == false) {
    if (error_info != NULL)
      error_info->kind = kNoError;
    return false;
  }
  if (v != NULL) {
    if (matches->empty()) {
      LOG(DFATAL) << "RE2::Set::Match() matched, but no matches returned?!";
      if (error_info != NULL)
        error_info->kind = kInconsistent;
      return false;
    }
    v->assign(matches->begin(), matches->end());
  }
  if (error_info != NULL)
    error_info->kind = kNoError;
  return true;
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2006 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Rewrite POSIX and other features in re
// to use simple extended regular expression features.
// Also sort and simplify character classes.

#include <string>








namespace duckdb_re2 {

// Parses the regexp src and then simplifies it and sets *dst to the
// string representation of the simplified form.  Returns true on success.
// Returns false and sets *error (if error != NULL) on error.
bool Regexp::SimplifyRegexp(const StringPiece& src, ParseFlags flags,
                            std::string* dst, RegexpStatus* status) {
  Regexp* re = Parse(src, flags, status);
  if (re == NULL)
    return false;
  Regexp* sre = re->Simplify();
  re->Decref();
  if (sre == NULL) {
    // Should not happen, since Simplify never fails.
    LOG(ERROR) << "Simplify failed on " << src;
    if (status) {
      status->set_code(kRegexpInternalError);
      status->set_error_arg(src);
    }
    return false;
  }
  *dst = sre->ToString();
  sre->Decref();
  return true;
}

// Assuming the simple_ flags on the children are accurate,
// is this Regexp* simple?
bool Regexp::ComputeSimple() {
  Regexp** subs;
  auto ccb = char_class_.ccb_;
  auto cc = char_class_.cc_;

  switch (op_) {
    case kRegexpNoMatch:
    case kRegexpEmptyMatch:
    case kRegexpLiteral:
    case kRegexpLiteralString:
    case kRegexpBeginLine:
    case kRegexpEndLine:
    case kRegexpBeginText:
    case kRegexpWordBoundary:
    case kRegexpNoWordBoundary:
    case kRegexpEndText:
    case kRegexpAnyChar:
    case kRegexpAnyByte:
    case kRegexpHaveMatch:
      return true;
    case kRegexpConcat:
    case kRegexpAlternate:
      // These are simple as long as the subpieces are simple.
      subs = sub();
      for (int i = 0; i < nsub_; i++)
        if (!subs[i]->simple())
          return false;
      return true;
    case kRegexpCharClass:
      // Simple as long as the char class is not empty, not full.
      if (ccb != NULL)
        return !ccb->empty() && !ccb->full();
      return !cc->empty() && !cc->full();
    case kRegexpCapture:
      subs = sub();
      return subs[0]->simple();
    case kRegexpStar:
    case kRegexpPlus:
    case kRegexpQuest:
      subs = sub();
      if (!subs[0]->simple())
        return false;
      switch (subs[0]->op_) {
        case kRegexpStar:
        case kRegexpPlus:
        case kRegexpQuest:
        case kRegexpEmptyMatch:
        case kRegexpNoMatch:
          return false;
        default:
          break;
      }
      return true;
    case kRegexpRepeat:
      return false;
  }
  LOG(DFATAL) << "Case not handled in ComputeSimple: " << op_;
  return false;
}

// Walker subclass used by Simplify.
// Coalesces runs of star/plus/quest/repeat of the same literal along with any
// occurrences of that literal into repeats of that literal. It also works for
// char classes, any char and any byte.
// PostVisit creates the coalesced result, which should then be simplified.
class CoalesceWalker : public Regexp::Walker<Regexp*> {
 public:
  CoalesceWalker() {}
  virtual Regexp* PostVisit(Regexp* re, Regexp* parent_arg, Regexp* pre_arg,
                            Regexp** child_args, int nchild_args);
  virtual Regexp* Copy(Regexp* re);
  virtual Regexp* ShortVisit(Regexp* re, Regexp* parent_arg);

 private:
  // These functions are declared inside CoalesceWalker so that
  // they can edit the private fields of the Regexps they construct.

  // Returns true if r1 and r2 can be coalesced. In particular, ensures that
  // the parse flags are consistent. (They will not be checked again later.)
  static bool CanCoalesce(Regexp* r1, Regexp* r2);

  // Coalesces *r1ptr and *r2ptr. In most cases, the array elements afterwards
  // will be empty match and the coalesced op. In other cases, where part of a
  // literal string was removed to be coalesced, the array elements afterwards
  // will be the coalesced op and the remainder of the literal string.
  static void DoCoalesce(Regexp** r1ptr, Regexp** r2ptr);

  CoalesceWalker(const CoalesceWalker&) = delete;
  CoalesceWalker& operator=(const CoalesceWalker&) = delete;
};

// Walker subclass used by Simplify.
// The simplify walk is purely post-recursive: given the simplified children,
// PostVisit creates the simplified result.
// The child_args are simplified Regexp*s.
class SimplifyWalker : public Regexp::Walker<Regexp*> {
 public:
  SimplifyWalker() {}
  virtual Regexp* PreVisit(Regexp* re, Regexp* parent_arg, bool* stop);
  virtual Regexp* PostVisit(Regexp* re, Regexp* parent_arg, Regexp* pre_arg,
                            Regexp** child_args, int nchild_args);
  virtual Regexp* Copy(Regexp* re);
  virtual Regexp* ShortVisit(Regexp* re, Regexp* parent_arg);

 private:
  // These functions are declared inside SimplifyWalker so that
  // they can edit the private fields of the Regexps they construct.

  // Creates a concatenation of two Regexp, consuming refs to re1 and re2.
  // Caller must Decref return value when done with it.
  static Regexp* Concat2(Regexp* re1, Regexp* re2, Regexp::ParseFlags flags);

  // Simplifies the expression re{min,max} in terms of *, +, and ?.
  // Returns a new regexp.  Does not edit re.  Does not consume reference to re.
  // Caller must Decref return value when done with it.
  static Regexp* SimplifyRepeat(Regexp* re, int min, int max,
                                Regexp::ParseFlags parse_flags);

  // Simplifies a character class by expanding any named classes
  // into rune ranges.  Does not edit re.  Does not consume ref to re.
  // Caller must Decref return value when done with it.
  static Regexp* SimplifyCharClass(Regexp* re);

  SimplifyWalker(const SimplifyWalker&) = delete;
  SimplifyWalker& operator=(const SimplifyWalker&) = delete;
};

// Simplifies a regular expression, returning a new regexp.
// The new regexp uses traditional Unix egrep features only,
// plus the Perl (?:) non-capturing parentheses.
// Otherwise, no POSIX or Perl additions.  The new regexp
// captures exactly the same subexpressions (with the same indices)
// as the original.
// Does not edit current object.
// Caller must Decref() return value when done with it.

Regexp* Regexp::Simplify() {
  CoalesceWalker cw;
  Regexp* cre = cw.Walk(this, NULL);
  if (cre == NULL)
    return cre;
  SimplifyWalker sw;
  Regexp* sre = sw.Walk(cre, NULL);
  cre->Decref();
  return sre;
}

// Utility function for PostVisit implementations that compares re->sub() with
// child_args to determine whether any child_args changed. In the common case,
// where nothing changed, calls Decref() for all child_args and returns false,
// so PostVisit must return re->Incref(). Otherwise, returns true.
static bool ChildArgsChanged(Regexp* re, Regexp** child_args) {
  for (int i = 0; i < re->nsub(); i++) {
    Regexp* sub = re->sub()[i];
    Regexp* newsub = child_args[i];
    if (newsub != sub)
      return true;
  }
  for (int i = 0; i < re->nsub(); i++) {
    Regexp* newsub = child_args[i];
    newsub->Decref();
  }
  return false;
}

Regexp* CoalesceWalker::Copy(Regexp* re) {
  return re->Incref();
}

Regexp* CoalesceWalker::ShortVisit(Regexp* re, Regexp* parent_arg) {
  // This should never be called, since we use Walk and not
  // WalkExponential.
  LOG(DFATAL) << "CoalesceWalker::ShortVisit called";
  return re->Incref();
}

Regexp* CoalesceWalker::PostVisit(Regexp* re,
                                  Regexp* parent_arg,
                                  Regexp* pre_arg,
                                  Regexp** child_args,
                                  int nchild_args) {
  if (re->nsub() == 0)
    return re->Incref();

  if (re->op() != kRegexpConcat) {
    if (!ChildArgsChanged(re, child_args))
      return re->Incref();

    // Something changed. Build a new op.
    Regexp* nre = new Regexp(re->op(), re->parse_flags());
    nre->AllocSub(re->nsub());
    Regexp** nre_subs = nre->sub();
    for (int i = 0; i < re->nsub(); i++)
      nre_subs[i] = child_args[i];
    // Repeats and Captures have additional data that must be copied.
    if (re->op() == kRegexpRepeat) {
      nre->repeat_.min_ = re->min();
      nre->repeat_.max_ = re->max();
    } else if (re->op() == kRegexpCapture) {
      nre->capture_.cap_ = re->cap();
    }
    return nre;
  }

  bool can_coalesce = false;
  for (int i = 0; i < re->nsub(); i++) {
    if (i+1 < re->nsub() &&
        CanCoalesce(child_args[i], child_args[i+1])) {
      can_coalesce = true;
      break;
    }
  }
  if (!can_coalesce) {
    if (!ChildArgsChanged(re, child_args))
      return re->Incref();

    // Something changed. Build a new op.
    Regexp* nre = new Regexp(re->op(), re->parse_flags());
    nre->AllocSub(re->nsub());
    Regexp** nre_subs = nre->sub();
    for (int i = 0; i < re->nsub(); i++)
      nre_subs[i] = child_args[i];
    return nre;
  }

  for (int i = 0; i < re->nsub(); i++) {
    if (i+1 < re->nsub() &&
        CanCoalesce(child_args[i], child_args[i+1]))
      DoCoalesce(&child_args[i], &child_args[i+1]);
  }
  // Determine how many empty matches were left by DoCoalesce.
  int n = 0;
  for (int i = n; i < re->nsub(); i++) {
    if (child_args[i]->op() == kRegexpEmptyMatch)
      n++;
  }
  // Build a new op.
  Regexp* nre = new Regexp(re->op(), re->parse_flags());
  nre->AllocSub(re->nsub() - n);
  Regexp** nre_subs = nre->sub();
  for (int i = 0, j = 0; i < re->nsub(); i++) {
    if (child_args[i]->op() == kRegexpEmptyMatch) {
      child_args[i]->Decref();
      continue;
    }
    nre_subs[j] = child_args[i];
    j++;
  }
  return nre;
}

bool CoalesceWalker::CanCoalesce(Regexp* r1, Regexp* r2) {
  // r1 must be a star/plus/quest/repeat of a literal, char class, any char or
  // any byte.
  if ((r1->op() == kRegexpStar ||
       r1->op() == kRegexpPlus ||
       r1->op() == kRegexpQuest ||
       r1->op() == kRegexpRepeat) &&
      (r1->sub()[0]->op() == kRegexpLiteral ||
       r1->sub()[0]->op() == kRegexpCharClass ||
       r1->sub()[0]->op() == kRegexpAnyChar ||
       r1->sub()[0]->op() == kRegexpAnyByte)) {
    // r2 must be a star/plus/quest/repeat of the same literal, char class,
    // any char or any byte.
    if ((r2->op() == kRegexpStar ||
         r2->op() == kRegexpPlus ||
         r2->op() == kRegexpQuest ||
         r2->op() == kRegexpRepeat) &&
        Regexp::Equal(r1->sub()[0], r2->sub()[0]) &&
        // The parse flags must be consistent.
        ((r1->parse_flags() & Regexp::NonGreedy) ==
         (r2->parse_flags() & Regexp::NonGreedy))) {
      return true;
    }
    // ... OR an occurrence of that literal, char class, any char or any byte
    if (Regexp::Equal(r1->sub()[0], r2)) {
      return true;
    }
    // ... OR a literal string that begins with that literal.
    if (r1->sub()[0]->op() == kRegexpLiteral &&
        r2->op() == kRegexpLiteralString &&
        r2->runes()[0] == r1->sub()[0]->rune() &&
        // The parse flags must be consistent.
        ((r1->sub()[0]->parse_flags() & Regexp::FoldCase) ==
         (r2->parse_flags() & Regexp::FoldCase))) {
      return true;
    }
  }
  return false;
}

void CoalesceWalker::DoCoalesce(Regexp** r1ptr, Regexp** r2ptr) {
  Regexp* r1 = *r1ptr;
  Regexp* r2 = *r2ptr;

  Regexp* nre = Regexp::Repeat(
      r1->sub()[0]->Incref(), r1->parse_flags(), 0, 0);

  switch (r1->op()) {
    case kRegexpStar:
      nre->repeat_.min_ = 0;
      nre->repeat_.max_ = -1;
      break;

    case kRegexpPlus:
      nre->repeat_.min_ = 1;
      nre->repeat_.max_ = -1;
      break;

    case kRegexpQuest:
      nre->repeat_.min_ = 0;
      nre->repeat_.max_ = 1;
      break;

    case kRegexpRepeat:
      nre->repeat_.min_ = r1->min();
      nre->repeat_.max_ = r1->max();
      break;

    default:
      LOG(DFATAL) << "DoCoalesce failed: r1->op() is " << r1->op();
      nre->Decref();
      return;
  }

  switch (r2->op()) {
    case kRegexpStar:
      nre->repeat_.max_ = -1;
      goto LeaveEmpty;

    case kRegexpPlus:
      nre->repeat_.min_++;
      nre->repeat_.max_ = -1;
      goto LeaveEmpty;

    case kRegexpQuest:
      if (nre->max() != -1)
        nre->repeat_.max_++;
      goto LeaveEmpty;

    case kRegexpRepeat:
      nre->repeat_.min_ += r2->min();
      if (r2->max() == -1)
        nre->repeat_.max_ = -1;
      else if (nre->max() != -1)
        nre->repeat_.max_ += r2->max();
      goto LeaveEmpty;

    case kRegexpLiteral:
    case kRegexpCharClass:
    case kRegexpAnyChar:
    case kRegexpAnyByte:
      nre->repeat_.min_++;
      if (nre->max() != -1)
        nre->repeat_.max_++;
      goto LeaveEmpty;

    LeaveEmpty:
      *r1ptr = new Regexp(kRegexpEmptyMatch, Regexp::NoParseFlags);
      *r2ptr = nre;
      break;

    case kRegexpLiteralString: {
      Rune r = r1->sub()[0]->rune();
      // Determine how much of the literal string is removed.
      // We know that we have at least one rune. :)
      int n = 1;
      while (n < r2->nrunes() && r2->runes()[n] == r)
        n++;
      nre->repeat_.min_ += n;
      if (nre->max() != -1)
        nre->repeat_.max_ += n;
      if (n == r2->nrunes())
        goto LeaveEmpty;
      *r1ptr = nre;
      *r2ptr = Regexp::LiteralString(
          &r2->runes()[n], r2->nrunes() - n, r2->parse_flags());
      break;
    }

    default:
      LOG(DFATAL) << "DoCoalesce failed: r2->op() is " << r2->op();
      nre->Decref();
      return;
  }

  r1->Decref();
  r2->Decref();
}

Regexp* SimplifyWalker::Copy(Regexp* re) {
  return re->Incref();
}

Regexp* SimplifyWalker::ShortVisit(Regexp* re, Regexp* parent_arg) {
  // This should never be called, since we use Walk and not
  // WalkExponential.
  LOG(DFATAL) << "SimplifyWalker::ShortVisit called";
  return re->Incref();
}

Regexp* SimplifyWalker::PreVisit(Regexp* re, Regexp* parent_arg, bool* stop) {
  if (re->simple()) {
    *stop = true;
    return re->Incref();
  }
  return NULL;
}

Regexp* SimplifyWalker::PostVisit(Regexp* re,
                                  Regexp* parent_arg,
                                  Regexp* pre_arg,
                                  Regexp** child_args,
                                  int nchild_args) {
  switch (re->op()) {
    case kRegexpNoMatch:
    case kRegexpEmptyMatch:
    case kRegexpLiteral:
    case kRegexpLiteralString:
    case kRegexpBeginLine:
    case kRegexpEndLine:
    case kRegexpBeginText:
    case kRegexpWordBoundary:
    case kRegexpNoWordBoundary:
    case kRegexpEndText:
    case kRegexpAnyChar:
    case kRegexpAnyByte:
    case kRegexpHaveMatch:
      // All these are always simple.
      re->simple_ = true;
      return re->Incref();

    case kRegexpConcat:
    case kRegexpAlternate: {
      // These are simple as long as the subpieces are simple.
      if (!ChildArgsChanged(re, child_args)) {
        re->simple_ = true;
        return re->Incref();
      }
      Regexp* nre = new Regexp(re->op(), re->parse_flags());
      nre->AllocSub(re->nsub());
      Regexp** nre_subs = nre->sub();
      for (int i = 0; i < re->nsub(); i++)
        nre_subs[i] = child_args[i];
      nre->simple_ = true;
      return nre;
    }

    case kRegexpCapture: {
      Regexp* newsub = child_args[0];
      if (newsub == re->sub()[0]) {
        newsub->Decref();
        re->simple_ = true;
        return re->Incref();
      }
      Regexp* nre = new Regexp(kRegexpCapture, re->parse_flags());
      nre->AllocSub(1);
      nre->sub()[0] = newsub;
      nre->capture_.cap_ = re->cap();
      nre->simple_ = true;
      return nre;
    }

    case kRegexpStar:
    case kRegexpPlus:
    case kRegexpQuest: {
      Regexp* newsub = child_args[0];
      // Special case: repeat the empty string as much as
      // you want, but it's still the empty string.
      if (newsub->op() == kRegexpEmptyMatch)
        return newsub;

      // These are simple as long as the subpiece is simple.
      if (newsub == re->sub()[0]) {
        newsub->Decref();
        re->simple_ = true;
        return re->Incref();
      }

      // These are also idempotent if flags are constant.
      if (re->op() == newsub->op() &&
          re->parse_flags() == newsub->parse_flags())
        return newsub;

      Regexp* nre = new Regexp(re->op(), re->parse_flags());
      nre->AllocSub(1);
      nre->sub()[0] = newsub;
      nre->simple_ = true;
      return nre;
    }

    case kRegexpRepeat: {
      Regexp* newsub = child_args[0];
      // Special case: repeat the empty string as much as
      // you want, but it's still the empty string.
      if (newsub->op() == kRegexpEmptyMatch)
        return newsub;

      Regexp* nre = SimplifyRepeat(newsub, re->repeat_.min_, re->repeat_.max_,
                                   re->parse_flags());
      newsub->Decref();
      nre->simple_ = true;
      return nre;
    }

    case kRegexpCharClass: {
      Regexp* nre = SimplifyCharClass(re);
      nre->simple_ = true;
      return nre;
    }
  }

  LOG(ERROR) << "Simplify case not handled: " << re->op();
  return re->Incref();
}

// Creates a concatenation of two Regexp, consuming refs to re1 and re2.
// Returns a new Regexp, handing the ref to the caller.
Regexp* SimplifyWalker::Concat2(Regexp* re1, Regexp* re2,
                                Regexp::ParseFlags parse_flags) {
  Regexp* re = new Regexp(kRegexpConcat, parse_flags);
  re->AllocSub(2);
  Regexp** subs = re->sub();
  subs[0] = re1;
  subs[1] = re2;
  return re;
}

// Simplifies the expression re{min,max} in terms of *, +, and ?.
// Returns a new regexp.  Does not edit re.  Does not consume reference to re.
// Caller must Decref return value when done with it.
// The result will *not* necessarily have the right capturing parens
// if you call ToString() and re-parse it: (x){2} becomes (x)(x),
// but in the Regexp* representation, both (x) are marked as $1.
Regexp* SimplifyWalker::SimplifyRepeat(Regexp* re, int min, int max,
                                       Regexp::ParseFlags f) {
  // x{n,} means at least n matches of x.
  if (max == -1) {
    // Special case: x{0,} is x*
    if (min == 0)
      return Regexp::Star(re->Incref(), f);

    // Special case: x{1,} is x+
    if (min == 1)
      return Regexp::Plus(re->Incref(), f);

    // General case: x{4,} is xxxx+
    PODArray<Regexp*> nre_subs(min);
    for (int i = 0; i < min-1; i++)
      nre_subs[i] = re->Incref();
    nre_subs[min-1] = Regexp::Plus(re->Incref(), f);
    return Regexp::Concat(nre_subs.data(), min, f);
  }

  // Special case: (x){0} matches only empty string.
  if (min == 0 && max == 0)
    return new Regexp(kRegexpEmptyMatch, f);

  // Special case: x{1} is just x.
  if (min == 1 && max == 1)
    return re->Incref();

  // General case: x{n,m} means n copies of x and m copies of x?.
  // The machine will do less work if we nest the final m copies,
  // so that x{2,5} = xx(x(x(x)?)?)?

  // Build leading prefix: xx.  Capturing only on the last one.
  Regexp* nre = NULL;
  if (min > 0) {
    PODArray<Regexp*> nre_subs(min);
    for (int i = 0; i < min; i++)
      nre_subs[i] = re->Incref();
    nre = Regexp::Concat(nre_subs.data(), min, f);
  }

  // Build and attach suffix: (x(x(x)?)?)?
  if (max > min) {
    Regexp* suf = Regexp::Quest(re->Incref(), f);
    for (int i = min+1; i < max; i++)
      suf = Regexp::Quest(Concat2(re->Incref(), suf, f), f);
    if (nre == NULL)
      nre = suf;
    else
      nre = Concat2(nre, suf, f);
  }

  if (nre == NULL) {
    // Some degenerate case, like min > max, or min < max < 0.
    // This shouldn't happen, because the parser rejects such regexps.
    LOG(DFATAL) << "Malformed repeat " << re->ToString() << " " << min << " " << max;
    return new Regexp(kRegexpNoMatch, f);
  }

  return nre;
}

// Simplifies a character class.
// Caller must Decref return value when done with it.
Regexp* SimplifyWalker::SimplifyCharClass(Regexp* re) {
  CharClass* cc = re->cc();

  // Special cases
  if (cc->empty())
    return new Regexp(kRegexpNoMatch, re->parse_flags());
  if (cc->full())
    return new Regexp(kRegexpAnyChar, re->parse_flags());

  return re->Incref();
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2004 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.



#include <ostream>



namespace duckdb_re2 {

const StringPiece::size_type StringPiece::npos;  // initialized in stringpiece.h

StringPiece::size_type StringPiece::copy(char* buf, size_type n,
                                         size_type pos) const {
  size_type ret = std::min(size_ - pos, n);
  memcpy(buf, data_ + pos, ret);
  return ret;
}

StringPiece StringPiece::substr(size_type pos, size_type n) const {
  if (pos > size_) pos = size_;
  if (n > size_ - pos) n = size_ - pos;
  return StringPiece(data_ + pos, n);
}

StringPiece::size_type StringPiece::find(const StringPiece& s,
                                         size_type pos) const {
  if (pos > size_) return npos;
  const_pointer result = std::search(data_ + pos, data_ + size_,
                                     s.data_, s.data_ + s.size_);
  size_type xpos = result - data_;
  return xpos + s.size_ <= size_ ? xpos : npos;
}

StringPiece::size_type StringPiece::find(char c, size_type pos) const {
  if (size_ <= 0 || pos >= size_) return npos;
  const_pointer result = std::find(data_ + pos, data_ + size_, c);
  return result != data_ + size_ ? result - data_ : npos;
}

StringPiece::size_type StringPiece::rfind(const StringPiece& s,
                                          size_type pos) const {
  if (size_ < s.size_) return npos;
  if (s.size_ == 0) return std::min(size_, pos);
  const_pointer last = data_ + std::min(size_ - s.size_, pos) + s.size_;
  const_pointer result = std::find_end(data_, last, s.data_, s.data_ + s.size_);
  return result != last ? result - data_ : npos;
}

StringPiece::size_type StringPiece::rfind(char c, size_type pos) const {
  if (size_ <= 0) return npos;
  for (size_t i = std::min(pos + 1, size_); i != 0;) {
    if (data_[--i] == c) return i;
  }
  return npos;
}

std::ostream& operator<<(std::ostream& o, const StringPiece& p) {
  o.write(p.data(), p.size());
  return o;
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 2006 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Format a regular expression structure as a string.
// Tested by parse_test.cc

#include <string.h>
#include <string>








namespace duckdb_re2 {

enum {
  PrecAtom,
  PrecUnary,
  PrecConcat,
  PrecAlternate,
  PrecEmpty,
  PrecParen,
  PrecToplevel,
};

// Helper function.  See description below.
static void AppendCCRange(std::string* t, Rune lo, Rune hi);

// Walker to generate string in s_.
// The arg pointers are actually integers giving the
// context precedence.
// The child_args are always NULL.
class ToStringWalker : public Regexp::Walker<int> {
 public:
  explicit ToStringWalker(std::string* t) : t_(t) {}

  virtual int PreVisit(Regexp* re, int parent_arg, bool* stop);
  virtual int PostVisit(Regexp* re, int parent_arg, int pre_arg,
                        int* child_args, int nchild_args);
  virtual int ShortVisit(Regexp* re, int parent_arg) {
    return 0;
  }

 private:
  std::string* t_;  // The string the walker appends to.

  ToStringWalker(const ToStringWalker&) = delete;
  ToStringWalker& operator=(const ToStringWalker&) = delete;
};

std::string Regexp::ToString() {
  std::string t;
  ToStringWalker w(&t);
  w.WalkExponential(this, PrecToplevel, 100000);
  if (w.stopped_early())
    t += " [truncated]";
  return t;
}

// Visits re before children are processed.
// Appends ( if needed and passes new precedence to children.
int ToStringWalker::PreVisit(Regexp* re, int parent_arg, bool* stop) {
  int prec = parent_arg;
  int nprec = PrecAtom;

  switch (re->op()) {
    case kRegexpNoMatch:
    case kRegexpEmptyMatch:
    case kRegexpLiteral:
    case kRegexpAnyChar:
    case kRegexpAnyByte:
    case kRegexpBeginLine:
    case kRegexpEndLine:
    case kRegexpBeginText:
    case kRegexpEndText:
    case kRegexpWordBoundary:
    case kRegexpNoWordBoundary:
    case kRegexpCharClass:
    case kRegexpHaveMatch:
      nprec = PrecAtom;
      break;

    case kRegexpConcat:
    case kRegexpLiteralString:
      if (prec < PrecConcat)
        t_->append("(?:");
      nprec = PrecConcat;
      break;

    case kRegexpAlternate:
      if (prec < PrecAlternate)
        t_->append("(?:");
      nprec = PrecAlternate;
      break;

    case kRegexpCapture:
      t_->append("(");
      if (re->cap() == 0)
        LOG(DFATAL) << "kRegexpCapture cap() == 0";
      if (re->name()) {
        t_->append("?P<");
        t_->append(*re->name());
        t_->append(">");
      }
      nprec = PrecParen;
      break;

    case kRegexpStar:
    case kRegexpPlus:
    case kRegexpQuest:
    case kRegexpRepeat:
      if (prec < PrecUnary)
        t_->append("(?:");
      // The subprecedence here is PrecAtom instead of PrecUnary
      // because PCRE treats two unary ops in a row as a parse error.
      nprec = PrecAtom;
      break;
  }

  return nprec;
}

static void AppendLiteral(std::string *t, Rune r, bool foldcase) {
  if (r != 0 && r < 0x80 && strchr("(){}[]*+?|.^$\\", r)) {
    t->append(1, '\\');
    t->append(1, static_cast<char>(r));
  } else if (foldcase && 'a' <= r && r <= 'z') {
    r -= 'a' - 'A';
    t->append(1, '[');
    t->append(1, static_cast<char>(r));
    t->append(1, static_cast<char>(r) + 'a' - 'A');
    t->append(1, ']');
  } else {
    AppendCCRange(t, r, r);
  }
}

// Visits re after children are processed.
// For childless regexps, all the work is done here.
// For regexps with children, append any unary suffixes or ).
int ToStringWalker::PostVisit(Regexp* re, int parent_arg, int pre_arg,
                              int* child_args, int nchild_args) {
  int prec = parent_arg;
  switch (re->op()) {
    case kRegexpNoMatch:
      // There's no simple symbol for "no match", but
      // [^0-Runemax] excludes everything.
      t_->append("[^\\x00-\\x{10ffff}]");
      break;

    case kRegexpEmptyMatch:
      // Append (?:) to make empty string visible,
      // unless this is already being parenthesized.
      if (prec < PrecEmpty)
        t_->append("(?:)");
      break;

    case kRegexpLiteral:
      AppendLiteral(t_, re->rune(),
                    (re->parse_flags() & Regexp::FoldCase) != 0);
      break;

    case kRegexpLiteralString:
      for (int i = 0; i < re->nrunes(); i++)
        AppendLiteral(t_, re->runes()[i],
                      (re->parse_flags() & Regexp::FoldCase) != 0);
      if (prec < PrecConcat)
        t_->append(")");
      break;

    case kRegexpConcat:
      if (prec < PrecConcat)
        t_->append(")");
      break;

    case kRegexpAlternate:
      // Clumsy but workable: the children all appended |
      // at the end of their strings, so just remove the last one.
      if ((*t_)[t_->size()-1] == '|')
        t_->erase(t_->size()-1);
      else
        LOG(DFATAL) << "Bad final char: " << t_;
      if (prec < PrecAlternate)
        t_->append(")");
      break;

    case kRegexpStar:
      t_->append("*");
      if (re->parse_flags() & Regexp::NonGreedy)
        t_->append("?");
      if (prec < PrecUnary)
        t_->append(")");
      break;

    case kRegexpPlus:
      t_->append("+");
      if (re->parse_flags() & Regexp::NonGreedy)
        t_->append("?");
      if (prec < PrecUnary)
        t_->append(")");
      break;

    case kRegexpQuest:
      t_->append("?");
      if (re->parse_flags() & Regexp::NonGreedy)
        t_->append("?");
      if (prec < PrecUnary)
        t_->append(")");
      break;

    case kRegexpRepeat:
      if (re->max() == -1)
        t_->append(StringPrintf("{%d,}", re->min()));
      else if (re->min() == re->max())
        t_->append(StringPrintf("{%d}", re->min()));
      else
        t_->append(StringPrintf("{%d,%d}", re->min(), re->max()));
      if (re->parse_flags() & Regexp::NonGreedy)
        t_->append("?");
      if (prec < PrecUnary)
        t_->append(")");
      break;

    case kRegexpAnyChar:
      t_->append(".");
      break;

    case kRegexpAnyByte:
      t_->append("\\C");
      break;

    case kRegexpBeginLine:
      t_->append("^");
      break;

    case kRegexpEndLine:
      t_->append("$");
      break;

    case kRegexpBeginText:
      t_->append("(?-m:^)");
      break;

    case kRegexpEndText:
      if (re->parse_flags() & Regexp::WasDollar)
        t_->append("(?-m:$)");
      else
        t_->append("\\z");
      break;

    case kRegexpWordBoundary:
      t_->append("\\b");
      break;

    case kRegexpNoWordBoundary:
      t_->append("\\B");
      break;

    case kRegexpCharClass: {
      if (re->cc()->size() == 0) {
        t_->append("[^\\x00-\\x{10ffff}]");
        break;
      }
      t_->append("[");
      // Heuristic: show class as negated if it contains the
      // non-character 0xFFFE.
      CharClass* cc = re->cc();
      if (cc->Contains(0xFFFE)) {
        cc = cc->Negate();
        t_->append("^");
      }
      for (CharClass::iterator i = cc->begin(); i != cc->end(); ++i)
        AppendCCRange(t_, i->lo, i->hi);
      if (cc != re->cc())
        cc->Delete();
      t_->append("]");
      break;
    }

    case kRegexpCapture:
      t_->append(")");
      break;

    case kRegexpHaveMatch:
      // There's no syntax accepted by the parser to generate
      // this node (it is generated by RE2::Set) so make something
      // up that is readable but won't compile.
      t_->append("(?HaveMatch:%d)", re->match_id());
      break;
  }

  // If the parent is an alternation, append the | for it.
  if (prec == PrecAlternate)
    t_->append("|");

  return 0;
}

// Appends a rune for use in a character class to the string t.
static void AppendCCChar(std::string* t, Rune r) {
  if (0x20 <= r && r <= 0x7E) {
    if (strchr("[]^-\\", r))
      t->append("\\");
    t->append(1, static_cast<char>(r));
    return;
  }
  switch (r) {
    default:
      break;

    case '\r':
      t->append("\\r");
      return;

    case '\t':
      t->append("\\t");
      return;

    case '\n':
      t->append("\\n");
      return;

    case '\f':
      t->append("\\f");
      return;
  }

  if (r < 0x100) {
    StringAppendF(t, "\\x%02x", static_cast<int>(r));
    return;
  }
  StringAppendF(t, "\\x{%x}", static_cast<int>(r));
}

static void AppendCCRange(std::string* t, Rune lo, Rune hi) {
  if (lo > hi)
    return;
  AppendCCChar(t, lo);
  if (lo < hi) {
    t->append("-");
    AppendCCChar(t, hi);
  }
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list


// GENERATED BY make_unicode_casefold.py; DO NOT EDIT.
// make_unicode_casefold.py >unicode_casefold.cc



namespace duckdb_re2 {


// 1374 groups, 2778 pairs, 349 ranges
const CaseFold unicode_casefold[] = {
	{ 65, 90, 32 },
	{ 97, 106, -32 },
	{ 107, 107, 8383 },
	{ 108, 114, -32 },
	{ 115, 115, 268 },
	{ 116, 122, -32 },
	{ 181, 181, 743 },
	{ 192, 214, 32 },
	{ 216, 222, 32 },
	{ 223, 223, 7615 },
	{ 224, 228, -32 },
	{ 229, 229, 8262 },
	{ 230, 246, -32 },
	{ 248, 254, -32 },
	{ 255, 255, 121 },
	{ 256, 303, EvenOdd },
	{ 306, 311, EvenOdd },
	{ 313, 328, OddEven },
	{ 330, 375, EvenOdd },
	{ 376, 376, -121 },
	{ 377, 382, OddEven },
	{ 383, 383, -300 },
	{ 384, 384, 195 },
	{ 385, 385, 210 },
	{ 386, 389, EvenOdd },
	{ 390, 390, 206 },
	{ 391, 392, OddEven },
	{ 393, 394, 205 },
	{ 395, 396, OddEven },
	{ 398, 398, 79 },
	{ 399, 399, 202 },
	{ 400, 400, 203 },
	{ 401, 402, OddEven },
	{ 403, 403, 205 },
	{ 404, 404, 207 },
	{ 405, 405, 97 },
	{ 406, 406, 211 },
	{ 407, 407, 209 },
	{ 408, 409, EvenOdd },
	{ 410, 410, 163 },
	{ 412, 412, 211 },
	{ 413, 413, 213 },
	{ 414, 414, 130 },
	{ 415, 415, 214 },
	{ 416, 421, EvenOdd },
	{ 422, 422, 218 },
	{ 423, 424, OddEven },
	{ 425, 425, 218 },
	{ 428, 429, EvenOdd },
	{ 430, 430, 218 },
	{ 431, 432, OddEven },
	{ 433, 434, 217 },
	{ 435, 438, OddEven },
	{ 439, 439, 219 },
	{ 440, 441, EvenOdd },
	{ 444, 445, EvenOdd },
	{ 447, 447, 56 },
	{ 452, 452, EvenOdd },
	{ 453, 453, OddEven },
	{ 454, 454, -2 },
	{ 455, 455, OddEven },
	{ 456, 456, EvenOdd },
	{ 457, 457, -2 },
	{ 458, 458, EvenOdd },
	{ 459, 459, OddEven },
	{ 460, 460, -2 },
	{ 461, 476, OddEven },
	{ 477, 477, -79 },
	{ 478, 495, EvenOdd },
	{ 497, 497, OddEven },
	{ 498, 498, EvenOdd },
	{ 499, 499, -2 },
	{ 500, 501, EvenOdd },
	{ 502, 502, -97 },
	{ 503, 503, -56 },
	{ 504, 543, EvenOdd },
	{ 544, 544, -130 },
	{ 546, 563, EvenOdd },
	{ 570, 570, 10795 },
	{ 571, 572, OddEven },
	{ 573, 573, -163 },
	{ 574, 574, 10792 },
	{ 575, 576, 10815 },
	{ 577, 578, OddEven },
	{ 579, 579, -195 },
	{ 580, 580, 69 },
	{ 581, 581, 71 },
	{ 582, 591, EvenOdd },
	{ 592, 592, 10783 },
	{ 593, 593, 10780 },
	{ 594, 594, 10782 },
	{ 595, 595, -210 },
	{ 596, 596, -206 },
	{ 598, 599, -205 },
	{ 601, 601, -202 },
	{ 603, 603, -203 },
	{ 604, 604, 42319 },
	{ 608, 608, -205 },
	{ 609, 609, 42315 },
	{ 611, 611, -207 },
	{ 613, 613, 42280 },
	{ 614, 614, 42308 },
	{ 616, 616, -209 },
	{ 617, 617, -211 },
	{ 618, 618, 42308 },
	{ 619, 619, 10743 },
	{ 620, 620, 42305 },
	{ 623, 623, -211 },
	{ 625, 625, 10749 },
	{ 626, 626, -213 },
	{ 629, 629, -214 },
	{ 637, 637, 10727 },
	{ 640, 640, -218 },
	{ 643, 643, -218 },
	{ 647, 647, 42282 },
	{ 648, 648, -218 },
	{ 649, 649, -69 },
	{ 650, 651, -217 },
	{ 652, 652, -71 },
	{ 658, 658, -219 },
	{ 669, 669, 42261 },
	{ 670, 670, 42258 },
	{ 837, 837, 84 },
	{ 880, 883, EvenOdd },
	{ 886, 887, EvenOdd },
	{ 891, 893, 130 },
	{ 895, 895, 116 },
	{ 902, 902, 38 },
	{ 904, 906, 37 },
	{ 908, 908, 64 },
	{ 910, 911, 63 },
	{ 913, 929, 32 },
	{ 931, 931, 31 },
	{ 932, 939, 32 },
	{ 940, 940, -38 },
	{ 941, 943, -37 },
	{ 945, 945, -32 },
	{ 946, 946, 30 },
	{ 947, 948, -32 },
	{ 949, 949, 64 },
	{ 950, 951, -32 },
	{ 952, 952, 25 },
	{ 953, 953, 7173 },
	{ 954, 954, 54 },
	{ 955, 955, -32 },
	{ 956, 956, -775 },
	{ 957, 959, -32 },
	{ 960, 960, 22 },
	{ 961, 961, 48 },
	{ 962, 962, EvenOdd },
	{ 963, 965, -32 },
	{ 966, 966, 15 },
	{ 967, 968, -32 },
	{ 969, 969, 7517 },
	{ 970, 971, -32 },
	{ 972, 972, -64 },
	{ 973, 974, -63 },
	{ 975, 975, 8 },
	{ 976, 976, -62 },
	{ 977, 977, 35 },
	{ 981, 981, -47 },
	{ 982, 982, -54 },
	{ 983, 983, -8 },
	{ 984, 1007, EvenOdd },
	{ 1008, 1008, -86 },
	{ 1009, 1009, -80 },
	{ 1010, 1010, 7 },
	{ 1011, 1011, -116 },
	{ 1012, 1012, -92 },
	{ 1013, 1013, -96 },
	{ 1015, 1016, OddEven },
	{ 1017, 1017, -7 },
	{ 1018, 1019, EvenOdd },
	{ 1021, 1023, -130 },
	{ 1024, 1039, 80 },
	{ 1040, 1071, 32 },
	{ 1072, 1073, -32 },
	{ 1074, 1074, 6222 },
	{ 1075, 1075, -32 },
	{ 1076, 1076, 6221 },
	{ 1077, 1085, -32 },
	{ 1086, 1086, 6212 },
	{ 1087, 1088, -32 },
	{ 1089, 1090, 6210 },
	{ 1091, 1097, -32 },
	{ 1098, 1098, 6204 },
	{ 1099, 1103, -32 },
	{ 1104, 1119, -80 },
	{ 1120, 1122, EvenOdd },
	{ 1123, 1123, 6180 },
	{ 1124, 1153, EvenOdd },
	{ 1162, 1215, EvenOdd },
	{ 1216, 1216, 15 },
	{ 1217, 1230, OddEven },
	{ 1231, 1231, -15 },
	{ 1232, 1327, EvenOdd },
	{ 1329, 1366, 48 },
	{ 1377, 1414, -48 },
	{ 4256, 4293, 7264 },
	{ 4295, 4295, 7264 },
	{ 4301, 4301, 7264 },
	{ 4304, 4346, 3008 },
	{ 4349, 4351, 3008 },
	{ 5024, 5103, 38864 },
	{ 5104, 5109, 8 },
	{ 5112, 5117, -8 },
	{ 7296, 7296, -6254 },
	{ 7297, 7297, -6253 },
	{ 7298, 7298, -6244 },
	{ 7299, 7299, -6242 },
	{ 7300, 7300, EvenOdd },
	{ 7301, 7301, -6243 },
	{ 7302, 7302, -6236 },
	{ 7303, 7303, -6181 },
	{ 7304, 7304, 35266 },
	{ 7312, 7354, -3008 },
	{ 7357, 7359, -3008 },
	{ 7545, 7545, 35332 },
	{ 7549, 7549, 3814 },
	{ 7680, 7776, EvenOdd },
	{ 7777, 7777, 58 },
	{ 7778, 7829, EvenOdd },
	{ 7835, 7835, -59 },
	{ 7838, 7838, -7615 },
	{ 7840, 7935, EvenOdd },
	{ 7936, 7943, 8 },
	{ 7944, 7951, -8 },
	{ 7952, 7957, 8 },
	{ 7960, 7965, -8 },
	{ 7968, 7975, 8 },
	{ 7976, 7983, -8 },
	{ 7984, 7991, 8 },
	{ 7992, 7999, -8 },
	{ 8000, 8005, 8 },
	{ 8008, 8013, -8 },
	{ 8017, 8017, 8 },
	{ 8019, 8019, 8 },
	{ 8021, 8021, 8 },
	{ 8023, 8023, 8 },
	{ 8025, 8025, -8 },
	{ 8027, 8027, -8 },
	{ 8029, 8029, -8 },
	{ 8031, 8031, -8 },
	{ 8032, 8039, 8 },
	{ 8040, 8047, -8 },
	{ 8048, 8049, 74 },
	{ 8050, 8053, 86 },
	{ 8054, 8055, 100 },
	{ 8056, 8057, 128 },
	{ 8058, 8059, 112 },
	{ 8060, 8061, 126 },
	{ 8064, 8071, 8 },
	{ 8072, 8079, -8 },
	{ 8080, 8087, 8 },
	{ 8088, 8095, -8 },
	{ 8096, 8103, 8 },
	{ 8104, 8111, -8 },
	{ 8112, 8113, 8 },
	{ 8115, 8115, 9 },
	{ 8120, 8121, -8 },
	{ 8122, 8123, -74 },
	{ 8124, 8124, -9 },
	{ 8126, 8126, -7289 },
	{ 8131, 8131, 9 },
	{ 8136, 8139, -86 },
	{ 8140, 8140, -9 },
	{ 8144, 8145, 8 },
	{ 8152, 8153, -8 },
	{ 8154, 8155, -100 },
	{ 8160, 8161, 8 },
	{ 8165, 8165, 7 },
	{ 8168, 8169, -8 },
	{ 8170, 8171, -112 },
	{ 8172, 8172, -7 },
	{ 8179, 8179, 9 },
	{ 8184, 8185, -128 },
	{ 8186, 8187, -126 },
	{ 8188, 8188, -9 },
	{ 8486, 8486, -7549 },
	{ 8490, 8490, -8415 },
	{ 8491, 8491, -8294 },
	{ 8498, 8498, 28 },
	{ 8526, 8526, -28 },
	{ 8544, 8559, 16 },
	{ 8560, 8575, -16 },
	{ 8579, 8580, OddEven },
	{ 9398, 9423, 26 },
	{ 9424, 9449, -26 },
	{ 11264, 11310, 48 },
	{ 11312, 11358, -48 },
	{ 11360, 11361, EvenOdd },
	{ 11362, 11362, -10743 },
	{ 11363, 11363, -3814 },
	{ 11364, 11364, -10727 },
	{ 11365, 11365, -10795 },
	{ 11366, 11366, -10792 },
	{ 11367, 11372, OddEven },
	{ 11373, 11373, -10780 },
	{ 11374, 11374, -10749 },
	{ 11375, 11375, -10783 },
	{ 11376, 11376, -10782 },
	{ 11378, 11379, EvenOdd },
	{ 11381, 11382, OddEven },
	{ 11390, 11391, -10815 },
	{ 11392, 11491, EvenOdd },
	{ 11499, 11502, OddEven },
	{ 11506, 11507, EvenOdd },
	{ 11520, 11557, -7264 },
	{ 11559, 11559, -7264 },
	{ 11565, 11565, -7264 },
	{ 42560, 42570, EvenOdd },
	{ 42571, 42571, -35267 },
	{ 42572, 42605, EvenOdd },
	{ 42624, 42651, EvenOdd },
	{ 42786, 42799, EvenOdd },
	{ 42802, 42863, EvenOdd },
	{ 42873, 42876, OddEven },
	{ 42877, 42877, -35332 },
	{ 42878, 42887, EvenOdd },
	{ 42891, 42892, OddEven },
	{ 42893, 42893, -42280 },
	{ 42896, 42899, EvenOdd },
	{ 42902, 42921, EvenOdd },
	{ 42922, 42922, -42308 },
	{ 42923, 42923, -42319 },
	{ 42924, 42924, -42315 },
	{ 42925, 42925, -42305 },
	{ 42926, 42926, -42308 },
	{ 42928, 42928, -42258 },
	{ 42929, 42929, -42282 },
	{ 42930, 42930, -42261 },
	{ 42931, 42931, 928 },
	{ 42932, 42937, EvenOdd },
	{ 43859, 43859, -928 },
	{ 43888, 43967, -38864 },
	{ 65313, 65338, 32 },
	{ 65345, 65370, -32 },
	{ 66560, 66599, 40 },
	{ 66600, 66639, -40 },
	{ 66736, 66771, 40 },
	{ 66776, 66811, -40 },
	{ 68736, 68786, 64 },
	{ 68800, 68850, -64 },
	{ 71840, 71871, 32 },
	{ 71872, 71903, -32 },
	{ 93760, 93791, 32 },
	{ 93792, 93823, -32 },
	{ 125184, 125217, 34 },
	{ 125218, 125251, -34 },
};
const int num_unicode_casefold = 349;

// 1374 groups, 1404 pairs, 194 ranges
const CaseFold unicode_tolower[] = {
	{ 65, 90, 32 },
	{ 181, 181, 775 },
	{ 192, 214, 32 },
	{ 216, 222, 32 },
	{ 256, 302, EvenOddSkip },
	{ 306, 310, EvenOddSkip },
	{ 313, 327, OddEvenSkip },
	{ 330, 374, EvenOddSkip },
	{ 376, 376, -121 },
	{ 377, 381, OddEvenSkip },
	{ 383, 383, -268 },
	{ 385, 385, 210 },
	{ 386, 388, EvenOddSkip },
	{ 390, 390, 206 },
	{ 391, 391, OddEven },
	{ 393, 394, 205 },
	{ 395, 395, OddEven },
	{ 398, 398, 79 },
	{ 399, 399, 202 },
	{ 400, 400, 203 },
	{ 401, 401, OddEven },
	{ 403, 403, 205 },
	{ 404, 404, 207 },
	{ 406, 406, 211 },
	{ 407, 407, 209 },
	{ 408, 408, EvenOdd },
	{ 412, 412, 211 },
	{ 413, 413, 213 },
	{ 415, 415, 214 },
	{ 416, 420, EvenOddSkip },
	{ 422, 422, 218 },
	{ 423, 423, OddEven },
	{ 425, 425, 218 },
	{ 428, 428, EvenOdd },
	{ 430, 430, 218 },
	{ 431, 431, OddEven },
	{ 433, 434, 217 },
	{ 435, 437, OddEvenSkip },
	{ 439, 439, 219 },
	{ 440, 440, EvenOdd },
	{ 444, 444, EvenOdd },
	{ 452, 452, 2 },
	{ 453, 453, OddEven },
	{ 455, 455, 2 },
	{ 456, 456, EvenOdd },
	{ 458, 458, 2 },
	{ 459, 475, OddEvenSkip },
	{ 478, 494, EvenOddSkip },
	{ 497, 497, 2 },
	{ 498, 500, EvenOddSkip },
	{ 502, 502, -97 },
	{ 503, 503, -56 },
	{ 504, 542, EvenOddSkip },
	{ 544, 544, -130 },
	{ 546, 562, EvenOddSkip },
	{ 570, 570, 10795 },
	{ 571, 571, OddEven },
	{ 573, 573, -163 },
	{ 574, 574, 10792 },
	{ 577, 577, OddEven },
	{ 579, 579, -195 },
	{ 580, 580, 69 },
	{ 581, 581, 71 },
	{ 582, 590, EvenOddSkip },
	{ 837, 837, 116 },
	{ 880, 882, EvenOddSkip },
	{ 886, 886, EvenOdd },
	{ 895, 895, 116 },
	{ 902, 902, 38 },
	{ 904, 906, 37 },
	{ 908, 908, 64 },
	{ 910, 911, 63 },
	{ 913, 929, 32 },
	{ 931, 939, 32 },
	{ 962, 962, EvenOdd },
	{ 975, 975, 8 },
	{ 976, 976, -30 },
	{ 977, 977, -25 },
	{ 981, 981, -15 },
	{ 982, 982, -22 },
	{ 984, 1006, EvenOddSkip },
	{ 1008, 1008, -54 },
	{ 1009, 1009, -48 },
	{ 1012, 1012, -60 },
	{ 1013, 1013, -64 },
	{ 1015, 1015, OddEven },
	{ 1017, 1017, -7 },
	{ 1018, 1018, EvenOdd },
	{ 1021, 1023, -130 },
	{ 1024, 1039, 80 },
	{ 1040, 1071, 32 },
	{ 1120, 1152, EvenOddSkip },
	{ 1162, 1214, EvenOddSkip },
	{ 1216, 1216, 15 },
	{ 1217, 1229, OddEvenSkip },
	{ 1232, 1326, EvenOddSkip },
	{ 1329, 1366, 48 },
	{ 4256, 4293, 7264 },
	{ 4295, 4295, 7264 },
	{ 4301, 4301, 7264 },
	{ 5112, 5117, -8 },
	{ 7296, 7296, -6222 },
	{ 7297, 7297, -6221 },
	{ 7298, 7298, -6212 },
	{ 7299, 7300, -6210 },
	{ 7301, 7301, -6211 },
	{ 7302, 7302, -6204 },
	{ 7303, 7303, -6180 },
	{ 7304, 7304, 35267 },
	{ 7312, 7354, -3008 },
	{ 7357, 7359, -3008 },
	{ 7680, 7828, EvenOddSkip },
	{ 7835, 7835, -58 },
	{ 7838, 7838, -7615 },
	{ 7840, 7934, EvenOddSkip },
	{ 7944, 7951, -8 },
	{ 7960, 7965, -8 },
	{ 7976, 7983, -8 },
	{ 7992, 7999, -8 },
	{ 8008, 8013, -8 },
	{ 8025, 8025, -8 },
	{ 8027, 8027, -8 },
	{ 8029, 8029, -8 },
	{ 8031, 8031, -8 },
	{ 8040, 8047, -8 },
	{ 8072, 8079, -8 },
	{ 8088, 8095, -8 },
	{ 8104, 8111, -8 },
	{ 8120, 8121, -8 },
	{ 8122, 8123, -74 },
	{ 8124, 8124, -9 },
	{ 8126, 8126, -7173 },
	{ 8136, 8139, -86 },
	{ 8140, 8140, -9 },
	{ 8152, 8153, -8 },
	{ 8154, 8155, -100 },
	{ 8168, 8169, -8 },
	{ 8170, 8171, -112 },
	{ 8172, 8172, -7 },
	{ 8184, 8185, -128 },
	{ 8186, 8187, -126 },
	{ 8188, 8188, -9 },
	{ 8486, 8486, -7517 },
	{ 8490, 8490, -8383 },
	{ 8491, 8491, -8262 },
	{ 8498, 8498, 28 },
	{ 8544, 8559, 16 },
	{ 8579, 8579, OddEven },
	{ 9398, 9423, 26 },
	{ 11264, 11310, 48 },
	{ 11360, 11360, EvenOdd },
	{ 11362, 11362, -10743 },
	{ 11363, 11363, -3814 },
	{ 11364, 11364, -10727 },
	{ 11367, 11371, OddEvenSkip },
	{ 11373, 11373, -10780 },
	{ 11374, 11374, -10749 },
	{ 11375, 11375, -10783 },
	{ 11376, 11376, -10782 },
	{ 11378, 11378, EvenOdd },
	{ 11381, 11381, OddEven },
	{ 11390, 11391, -10815 },
	{ 11392, 11490, EvenOddSkip },
	{ 11499, 11501, OddEvenSkip },
	{ 11506, 11506, EvenOdd },
	{ 42560, 42604, EvenOddSkip },
	{ 42624, 42650, EvenOddSkip },
	{ 42786, 42798, EvenOddSkip },
	{ 42802, 42862, EvenOddSkip },
	{ 42873, 42875, OddEvenSkip },
	{ 42877, 42877, -35332 },
	{ 42878, 42886, EvenOddSkip },
	{ 42891, 42891, OddEven },
	{ 42893, 42893, -42280 },
	{ 42896, 42898, EvenOddSkip },
	{ 42902, 42920, EvenOddSkip },
	{ 42922, 42922, -42308 },
	{ 42923, 42923, -42319 },
	{ 42924, 42924, -42315 },
	{ 42925, 42925, -42305 },
	{ 42926, 42926, -42308 },
	{ 42928, 42928, -42258 },
	{ 42929, 42929, -42282 },
	{ 42930, 42930, -42261 },
	{ 42931, 42931, 928 },
	{ 42932, 42936, EvenOddSkip },
	{ 43888, 43967, -38864 },
	{ 65313, 65338, 32 },
	{ 66560, 66599, 40 },
	{ 66736, 66771, 40 },
	{ 68736, 68786, 64 },
	{ 71840, 71871, 32 },
	{ 93760, 93791, 32 },
	{ 125184, 125217, 34 },
};
const int num_unicode_tolower = 194;



} // namespace duckdb_re2




// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list


// GENERATED BY make_unicode_groups.py; DO NOT EDIT.
// make_unicode_groups.py >unicode_groups.cc



namespace duckdb_re2 {


static const URange16 Ps_range16[] = {
	{ 40, 40 },
	{ 91, 91 },
	{ 123, 123 },
	{ 3898, 3898 },
	{ 3900, 3900 },
	{ 5787, 5787 },
	{ 8218, 8218 },
	{ 8222, 8222 },
	{ 8261, 8261 },
	{ 8317, 8317 },
	{ 8333, 8333 },
	{ 8968, 8968 },
	{ 8970, 8970 },
	{ 9001, 9001 },
	{ 10088, 10088 },
	{ 10090, 10090 },
	{ 10092, 10092 },
	{ 10094, 10094 },
	{ 10096, 10096 },
	{ 10098, 10098 },
	{ 10100, 10100 },
	{ 10181, 10181 },
	{ 10214, 10214 },
	{ 10216, 10216 },
	{ 10218, 10218 },
	{ 10220, 10220 },
	{ 10222, 10222 },
	{ 10627, 10627 },
	{ 10629, 10629 },
	{ 10631, 10631 },
	{ 10633, 10633 },
	{ 10635, 10635 },
	{ 10637, 10637 },
	{ 10639, 10639 },
	{ 10641, 10641 },
	{ 10643, 10643 },
	{ 10645, 10645 },
	{ 10647, 10647 },
	{ 10712, 10712 },
	{ 10714, 10714 },
	{ 10748, 10748 },
	{ 11810, 11810 },
	{ 11812, 11812 },
	{ 11814, 11814 },
	{ 11816, 11816 },
	{ 11842, 11842 },
	{ 12296, 12296 },
	{ 12298, 12298 },
	{ 12300, 12300 },
	{ 12302, 12302 },
	{ 12304, 12304 },
	{ 12308, 12308 },
	{ 12310, 12310 },
	{ 12312, 12312 },
	{ 12314, 12314 },
	{ 12317, 12317 },
	{ 64831, 64831 },
	{ 65047, 65047 },
	{ 65077, 65077 },
	{ 65079, 65079 },
	{ 65081, 65081 },
	{ 65083, 65083 },
	{ 65085, 65085 },
	{ 65087, 65087 },
	{ 65089, 65089 },
	{ 65091, 65091 },
	{ 65095, 65095 },
	{ 65113, 65113 },
	{ 65115, 65115 },
	{ 65117, 65117 },
	{ 65288, 65288 },
	{ 65339, 65339 },
	{ 65371, 65371 },
	{ 65375, 65375 },
	{ 65378, 65378 },
};
static const URange16 Nl_range16[] = {
	{ 5870, 5872 },
	{ 8544, 8578 },
	{ 8581, 8584 },
	{ 12295, 12295 },
	{ 12321, 12329 },
	{ 12344, 12346 },
	{ 42726, 42735 },
};
static const URange32 Nl_range32[] = {
	{ 65856, 65908 },
	{ 66369, 66369 },
	{ 66378, 66378 },
	{ 66513, 66517 },
	{ 74752, 74862 },
};
static const URange16 No_range16[] = {
	{ 178, 179 },
	{ 185, 185 },
	{ 188, 190 },
	{ 2548, 2553 },
	{ 2930, 2935 },
	{ 3056, 3058 },
	{ 3192, 3198 },
	{ 3416, 3422 },
	{ 3440, 3448 },
	{ 3882, 3891 },
	{ 4969, 4988 },
	{ 6128, 6137 },
	{ 6618, 6618 },
	{ 8304, 8304 },
	{ 8308, 8313 },
	{ 8320, 8329 },
	{ 8528, 8543 },
	{ 8585, 8585 },
	{ 9312, 9371 },
	{ 9450, 9471 },
	{ 10102, 10131 },
	{ 11517, 11517 },
	{ 12690, 12693 },
	{ 12832, 12841 },
	{ 12872, 12879 },
	{ 12881, 12895 },
	{ 12928, 12937 },
	{ 12977, 12991 },
	{ 43056, 43061 },
};
static const URange32 No_range32[] = {
	{ 65799, 65843 },
	{ 65909, 65912 },
	{ 65930, 65931 },
	{ 66273, 66299 },
	{ 66336, 66339 },
	{ 67672, 67679 },
	{ 67705, 67711 },
	{ 67751, 67759 },
	{ 67835, 67839 },
	{ 67862, 67867 },
	{ 68028, 68029 },
	{ 68032, 68047 },
	{ 68050, 68095 },
	{ 68160, 68168 },
	{ 68221, 68222 },
	{ 68253, 68255 },
	{ 68331, 68335 },
	{ 68440, 68447 },
	{ 68472, 68479 },
	{ 68521, 68527 },
	{ 68858, 68863 },
	{ 69216, 69246 },
	{ 69405, 69414 },
	{ 69457, 69460 },
	{ 69714, 69733 },
	{ 70113, 70132 },
	{ 71482, 71483 },
	{ 71914, 71922 },
	{ 72794, 72812 },
	{ 93019, 93025 },
	{ 93824, 93846 },
	{ 119520, 119539 },
	{ 119648, 119672 },
	{ 125127, 125135 },
	{ 126065, 126123 },
	{ 126125, 126127 },
	{ 126129, 126132 },
	{ 127232, 127244 },
};
static const URange16 Lo_range16[] = {
	{ 170, 170 },
	{ 186, 186 },
	{ 443, 443 },
	{ 448, 451 },
	{ 660, 660 },
	{ 1488, 1514 },
	{ 1519, 1522 },
	{ 1568, 1599 },
	{ 1601, 1610 },
	{ 1646, 1647 },
	{ 1649, 1747 },
	{ 1749, 1749 },
	{ 1774, 1775 },
	{ 1786, 1788 },
	{ 1791, 1791 },
	{ 1808, 1808 },
	{ 1810, 1839 },
	{ 1869, 1957 },
	{ 1969, 1969 },
	{ 1994, 2026 },
	{ 2048, 2069 },
	{ 2112, 2136 },
	{ 2144, 2154 },
	{ 2208, 2228 },
	{ 2230, 2237 },
	{ 2308, 2361 },
	{ 2365, 2365 },
	{ 2384, 2384 },
	{ 2392, 2401 },
	{ 2418, 2432 },
	{ 2437, 2444 },
	{ 2447, 2448 },
	{ 2451, 2472 },
	{ 2474, 2480 },
	{ 2482, 2482 },
	{ 2486, 2489 },
	{ 2493, 2493 },
	{ 2510, 2510 },
	{ 2524, 2525 },
	{ 2527, 2529 },
	{ 2544, 2545 },
	{ 2556, 2556 },
	{ 2565, 2570 },
	{ 2575, 2576 },
	{ 2579, 2600 },
	{ 2602, 2608 },
	{ 2610, 2611 },
	{ 2613, 2614 },
	{ 2616, 2617 },
	{ 2649, 2652 },
	{ 2654, 2654 },
	{ 2674, 2676 },
	{ 2693, 2701 },
	{ 2703, 2705 },
	{ 2707, 2728 },
	{ 2730, 2736 },
	{ 2738, 2739 },
	{ 2741, 2745 },
	{ 2749, 2749 },
	{ 2768, 2768 },
	{ 2784, 2785 },
	{ 2809, 2809 },
	{ 2821, 2828 },
	{ 2831, 2832 },
	{ 2835, 2856 },
	{ 2858, 2864 },
	{ 2866, 2867 },
	{ 2869, 2873 },
	{ 2877, 2877 },
	{ 2908, 2909 },
	{ 2911, 2913 },
	{ 2929, 2929 },
	{ 2947, 2947 },
	{ 2949, 2954 },
	{ 2958, 2960 },
	{ 2962, 2965 },
	{ 2969, 2970 },
	{ 2972, 2972 },
	{ 2974, 2975 },
	{ 2979, 2980 },
	{ 2984, 2986 },
	{ 2990, 3001 },
	{ 3024, 3024 },
	{ 3077, 3084 },
	{ 3086, 3088 },
	{ 3090, 3112 },
	{ 3114, 3129 },
	{ 3133, 3133 },
	{ 3160, 3162 },
	{ 3168, 3169 },
	{ 3200, 3200 },
	{ 3205, 3212 },
	{ 3214, 3216 },
	{ 3218, 3240 },
	{ 3242, 3251 },
	{ 3253, 3257 },
	{ 3261, 3261 },
	{ 3294, 3294 },
	{ 3296, 3297 },
	{ 3313, 3314 },
	{ 3333, 3340 },
	{ 3342, 3344 },
	{ 3346, 3386 },
	{ 3389, 3389 },
	{ 3406, 3406 },
	{ 3412, 3414 },
	{ 3423, 3425 },
	{ 3450, 3455 },
	{ 3461, 3478 },
	{ 3482, 3505 },
	{ 3507, 3515 },
	{ 3517, 3517 },
	{ 3520, 3526 },
	{ 3585, 3632 },
	{ 3634, 3635 },
	{ 3648, 3653 },
	{ 3713, 3714 },
	{ 3716, 3716 },
	{ 3719, 3720 },
	{ 3722, 3722 },
	{ 3725, 3725 },
	{ 3732, 3735 },
	{ 3737, 3743 },
	{ 3745, 3747 },
	{ 3749, 3749 },
	{ 3751, 3751 },
	{ 3754, 3755 },
	{ 3757, 3760 },
	{ 3762, 3763 },
	{ 3773, 3773 },
	{ 3776, 3780 },
	{ 3804, 3807 },
	{ 3840, 3840 },
	{ 3904, 3911 },
	{ 3913, 3948 },
	{ 3976, 3980 },
	{ 4096, 4138 },
	{ 4159, 4159 },
	{ 4176, 4181 },
	{ 4186, 4189 },
	{ 4193, 4193 },
	{ 4197, 4198 },
	{ 4206, 4208 },
	{ 4213, 4225 },
	{ 4238, 4238 },
	{ 4352, 4680 },
	{ 4682, 4685 },
	{ 4688, 4694 },
	{ 4696, 4696 },
	{ 4698, 4701 },
	{ 4704, 4744 },
	{ 4746, 4749 },
	{ 4752, 4784 },
	{ 4786, 4789 },
	{ 4792, 4798 },
	{ 4800, 4800 },
	{ 4802, 4805 },
	{ 4808, 4822 },
	{ 4824, 4880 },
	{ 4882, 4885 },
	{ 4888, 4954 },
	{ 4992, 5007 },
	{ 5121, 5740 },
	{ 5743, 5759 },
	{ 5761, 5786 },
	{ 5792, 5866 },
	{ 5873, 5880 },
	{ 5888, 5900 },
	{ 5902, 5905 },
	{ 5920, 5937 },
	{ 5952, 5969 },
	{ 5984, 5996 },
	{ 5998, 6000 },
	{ 6016, 6067 },
	{ 6108, 6108 },
	{ 6176, 6210 },
	{ 6212, 6264 },
	{ 6272, 6276 },
	{ 6279, 6312 },
	{ 6314, 6314 },
	{ 6320, 6389 },
	{ 6400, 6430 },
	{ 6480, 6509 },
	{ 6512, 6516 },
	{ 6528, 6571 },
	{ 6576, 6601 },
	{ 6656, 6678 },
	{ 6688, 6740 },
	{ 6917, 6963 },
	{ 6981, 6987 },
	{ 7043, 7072 },
	{ 7086, 7087 },
	{ 7098, 7141 },
	{ 7168, 7203 },
	{ 7245, 7247 },
	{ 7258, 7287 },
	{ 7401, 7404 },
	{ 7406, 7409 },
	{ 7413, 7414 },
	{ 8501, 8504 },
	{ 11568, 11623 },
	{ 11648, 11670 },
	{ 11680, 11686 },
	{ 11688, 11694 },
	{ 11696, 11702 },
	{ 11704, 11710 },
	{ 11712, 11718 },
	{ 11720, 11726 },
	{ 11728, 11734 },
	{ 11736, 11742 },
	{ 12294, 12294 },
	{ 12348, 12348 },
	{ 12353, 12438 },
	{ 12447, 12447 },
	{ 12449, 12538 },
	{ 12543, 12543 },
	{ 12549, 12591 },
	{ 12593, 12686 },
	{ 12704, 12730 },
	{ 12784, 12799 },
	{ 13312, 19893 },
	{ 19968, 40943 },
	{ 40960, 40980 },
	{ 40982, 42124 },
	{ 42192, 42231 },
	{ 42240, 42507 },
	{ 42512, 42527 },
	{ 42538, 42539 },
	{ 42606, 42606 },
	{ 42656, 42725 },
	{ 42895, 42895 },
	{ 42999, 42999 },
	{ 43003, 43009 },
	{ 43011, 43013 },
	{ 43015, 43018 },
	{ 43020, 43042 },
	{ 43072, 43123 },
	{ 43138, 43187 },
	{ 43250, 43255 },
	{ 43259, 43259 },
	{ 43261, 43262 },
	{ 43274, 43301 },
	{ 43312, 43334 },
	{ 43360, 43388 },
	{ 43396, 43442 },
	{ 43488, 43492 },
	{ 43495, 43503 },
	{ 43514, 43518 },
	{ 43520, 43560 },
	{ 43584, 43586 },
	{ 43588, 43595 },
	{ 43616, 43631 },
	{ 43633, 43638 },
	{ 43642, 43642 },
	{ 43646, 43695 },
	{ 43697, 43697 },
	{ 43701, 43702 },
	{ 43705, 43709 },
	{ 43712, 43712 },
	{ 43714, 43714 },
	{ 43739, 43740 },
	{ 43744, 43754 },
	{ 43762, 43762 },
	{ 43777, 43782 },
	{ 43785, 43790 },
	{ 43793, 43798 },
	{ 43808, 43814 },
	{ 43816, 43822 },
	{ 43968, 44002 },
	{ 44032, 55203 },
	{ 55216, 55238 },
	{ 55243, 55291 },
	{ 63744, 64109 },
	{ 64112, 64217 },
	{ 64285, 64285 },
	{ 64287, 64296 },
	{ 64298, 64310 },
	{ 64312, 64316 },
	{ 64318, 64318 },
	{ 64320, 64321 },
	{ 64323, 64324 },
	{ 64326, 64433 },
	{ 64467, 64829 },
	{ 64848, 64911 },
	{ 64914, 64967 },
	{ 65008, 65019 },
	{ 65136, 65140 },
	{ 65142, 65276 },
	{ 65382, 65391 },
	{ 65393, 65437 },
	{ 65440, 65470 },
	{ 65474, 65479 },
	{ 65482, 65487 },
	{ 65490, 65495 },
	{ 65498, 65500 },
};
static const URange32 Lo_range32[] = {
	{ 65536, 65547 },
	{ 65549, 65574 },
	{ 65576, 65594 },
	{ 65596, 65597 },
	{ 65599, 65613 },
	{ 65616, 65629 },
	{ 65664, 65786 },
	{ 66176, 66204 },
	{ 66208, 66256 },
	{ 66304, 66335 },
	{ 66349, 66368 },
	{ 66370, 66377 },
	{ 66384, 66421 },
	{ 66432, 66461 },
	{ 66464, 66499 },
	{ 66504, 66511 },
	{ 66640, 66717 },
	{ 66816, 66855 },
	{ 66864, 66915 },
	{ 67072, 67382 },
	{ 67392, 67413 },
	{ 67424, 67431 },
	{ 67584, 67589 },
	{ 67592, 67592 },
	{ 67594, 67637 },
	{ 67639, 67640 },
	{ 67644, 67644 },
	{ 67647, 67669 },
	{ 67680, 67702 },
	{ 67712, 67742 },
	{ 67808, 67826 },
	{ 67828, 67829 },
	{ 67840, 67861 },
	{ 67872, 67897 },
	{ 67968, 68023 },
	{ 68030, 68031 },
	{ 68096, 68096 },
	{ 68112, 68115 },
	{ 68117, 68119 },
	{ 68121, 68149 },
	{ 68192, 68220 },
	{ 68224, 68252 },
	{ 68288, 68295 },
	{ 68297, 68324 },
	{ 68352, 68405 },
	{ 68416, 68437 },
	{ 68448, 68466 },
	{ 68480, 68497 },
	{ 68608, 68680 },
	{ 68864, 68899 },
	{ 69376, 69404 },
	{ 69415, 69415 },
	{ 69424, 69445 },
	{ 69635, 69687 },
	{ 69763, 69807 },
	{ 69840, 69864 },
	{ 69891, 69926 },
	{ 69956, 69956 },
	{ 69968, 70002 },
	{ 70006, 70006 },
	{ 70019, 70066 },
	{ 70081, 70084 },
	{ 70106, 70106 },
	{ 70108, 70108 },
	{ 70144, 70161 },
	{ 70163, 70187 },
	{ 70272, 70278 },
	{ 70280, 70280 },
	{ 70282, 70285 },
	{ 70287, 70301 },
	{ 70303, 70312 },
	{ 70320, 70366 },
	{ 70405, 70412 },
	{ 70415, 70416 },
	{ 70419, 70440 },
	{ 70442, 70448 },
	{ 70450, 70451 },
	{ 70453, 70457 },
	{ 70461, 70461 },
	{ 70480, 70480 },
	{ 70493, 70497 },
	{ 70656, 70708 },
	{ 70727, 70730 },
	{ 70784, 70831 },
	{ 70852, 70853 },
	{ 70855, 70855 },
	{ 71040, 71086 },
	{ 71128, 71131 },
	{ 71168, 71215 },
	{ 71236, 71236 },
	{ 71296, 71338 },
	{ 71424, 71450 },
	{ 71680, 71723 },
	{ 71935, 71935 },
	{ 72192, 72192 },
	{ 72203, 72242 },
	{ 72250, 72250 },
	{ 72272, 72272 },
	{ 72284, 72323 },
	{ 72326, 72329 },
	{ 72349, 72349 },
	{ 72384, 72440 },
	{ 72704, 72712 },
	{ 72714, 72750 },
	{ 72768, 72768 },
	{ 72818, 72847 },
	{ 72960, 72966 },
	{ 72968, 72969 },
	{ 72971, 73008 },
	{ 73030, 73030 },
	{ 73056, 73061 },
	{ 73063, 73064 },
	{ 73066, 73097 },
	{ 73112, 73112 },
	{ 73440, 73458 },
	{ 73728, 74649 },
	{ 74880, 75075 },
	{ 77824, 78894 },
	{ 82944, 83526 },
	{ 92160, 92728 },
	{ 92736, 92766 },
	{ 92880, 92909 },
	{ 92928, 92975 },
	{ 93027, 93047 },
	{ 93053, 93071 },
	{ 93952, 94020 },
	{ 94032, 94032 },
	{ 94208, 100337 },
	{ 100352, 101106 },
	{ 110592, 110878 },
	{ 110960, 111355 },
	{ 113664, 113770 },
	{ 113776, 113788 },
	{ 113792, 113800 },
	{ 113808, 113817 },
	{ 124928, 125124 },
	{ 126464, 126467 },
	{ 126469, 126495 },
	{ 126497, 126498 },
	{ 126500, 126500 },
	{ 126503, 126503 },
	{ 126505, 126514 },
	{ 126516, 126519 },
	{ 126521, 126521 },
	{ 126523, 126523 },
	{ 126530, 126530 },
	{ 126535, 126535 },
	{ 126537, 126537 },
	{ 126539, 126539 },
	{ 126541, 126543 },
	{ 126545, 126546 },
	{ 126548, 126548 },
	{ 126551, 126551 },
	{ 126553, 126553 },
	{ 126555, 126555 },
	{ 126557, 126557 },
	{ 126559, 126559 },
	{ 126561, 126562 },
	{ 126564, 126564 },
	{ 126567, 126570 },
	{ 126572, 126578 },
	{ 126580, 126583 },
	{ 126585, 126588 },
	{ 126590, 126590 },
	{ 126592, 126601 },
	{ 126603, 126619 },
	{ 126625, 126627 },
	{ 126629, 126633 },
	{ 126635, 126651 },
	{ 131072, 173782 },
	{ 173824, 177972 },
	{ 177984, 178205 },
	{ 178208, 183969 },
	{ 183984, 191456 },
	{ 194560, 195101 },
};
static const URange16 Ll_range16[] = {
	{ 97, 122 },
	{ 181, 181 },
	{ 223, 246 },
	{ 248, 255 },
	{ 257, 257 },
	{ 259, 259 },
	{ 261, 261 },
	{ 263, 263 },
	{ 265, 265 },
	{ 267, 267 },
	{ 269, 269 },
	{ 271, 271 },
	{ 273, 273 },
	{ 275, 275 },
	{ 277, 277 },
	{ 279, 279 },
	{ 281, 281 },
	{ 283, 283 },
	{ 285, 285 },
	{ 287, 287 },
	{ 289, 289 },
	{ 291, 291 },
	{ 293, 293 },
	{ 295, 295 },
	{ 297, 297 },
	{ 299, 299 },
	{ 301, 301 },
	{ 303, 303 },
	{ 305, 305 },
	{ 307, 307 },
	{ 309, 309 },
	{ 311, 312 },
	{ 314, 314 },
	{ 316, 316 },
	{ 318, 318 },
	{ 320, 320 },
	{ 322, 322 },
	{ 324, 324 },
	{ 326, 326 },
	{ 328, 329 },
	{ 331, 331 },
	{ 333, 333 },
	{ 335, 335 },
	{ 337, 337 },
	{ 339, 339 },
	{ 341, 341 },
	{ 343, 343 },
	{ 345, 345 },
	{ 347, 347 },
	{ 349, 349 },
	{ 351, 351 },
	{ 353, 353 },
	{ 355, 355 },
	{ 357, 357 },
	{ 359, 359 },
	{ 361, 361 },
	{ 363, 363 },
	{ 365, 365 },
	{ 367, 367 },
	{ 369, 369 },
	{ 371, 371 },
	{ 373, 373 },
	{ 375, 375 },
	{ 378, 378 },
	{ 380, 380 },
	{ 382, 384 },
	{ 387, 387 },
	{ 389, 389 },
	{ 392, 392 },
	{ 396, 397 },
	{ 402, 402 },
	{ 405, 405 },
	{ 409, 411 },
	{ 414, 414 },
	{ 417, 417 },
	{ 419, 419 },
	{ 421, 421 },
	{ 424, 424 },
	{ 426, 427 },
	{ 429, 429 },
	{ 432, 432 },
	{ 436, 436 },
	{ 438, 438 },
	{ 441, 442 },
	{ 445, 447 },
	{ 454, 454 },
	{ 457, 457 },
	{ 460, 460 },
	{ 462, 462 },
	{ 464, 464 },
	{ 466, 466 },
	{ 468, 468 },
	{ 470, 470 },
	{ 472, 472 },
	{ 474, 474 },
	{ 476, 477 },
	{ 479, 479 },
	{ 481, 481 },
	{ 483, 483 },
	{ 485, 485 },
	{ 487, 487 },
	{ 489, 489 },
	{ 491, 491 },
	{ 493, 493 },
	{ 495, 496 },
	{ 499, 499 },
	{ 501, 501 },
	{ 505, 505 },
	{ 507, 507 },
	{ 509, 509 },
	{ 511, 511 },
	{ 513, 513 },
	{ 515, 515 },
	{ 517, 517 },
	{ 519, 519 },
	{ 521, 521 },
	{ 523, 523 },
	{ 525, 525 },
	{ 527, 527 },
	{ 529, 529 },
	{ 531, 531 },
	{ 533, 533 },
	{ 535, 535 },
	{ 537, 537 },
	{ 539, 539 },
	{ 541, 541 },
	{ 543, 543 },
	{ 545, 545 },
	{ 547, 547 },
	{ 549, 549 },
	{ 551, 551 },
	{ 553, 553 },
	{ 555, 555 },
	{ 557, 557 },
	{ 559, 559 },
	{ 561, 561 },
	{ 563, 569 },
	{ 572, 572 },
	{ 575, 576 },
	{ 578, 578 },
	{ 583, 583 },
	{ 585, 585 },
	{ 587, 587 },
	{ 589, 589 },
	{ 591, 659 },
	{ 661, 687 },
	{ 881, 881 },
	{ 883, 883 },
	{ 887, 887 },
	{ 891, 893 },
	{ 912, 912 },
	{ 940, 974 },
	{ 976, 977 },
	{ 981, 983 },
	{ 985, 985 },
	{ 987, 987 },
	{ 989, 989 },
	{ 991, 991 },
	{ 993, 993 },
	{ 995, 995 },
	{ 997, 997 },
	{ 999, 999 },
	{ 1001, 1001 },
	{ 1003, 1003 },
	{ 1005, 1005 },
	{ 1007, 1011 },
	{ 1013, 1013 },
	{ 1016, 1016 },
	{ 1019, 1020 },
	{ 1072, 1119 },
	{ 1121, 1121 },
	{ 1123, 1123 },
	{ 1125, 1125 },
	{ 1127, 1127 },
	{ 1129, 1129 },
	{ 1131, 1131 },
	{ 1133, 1133 },
	{ 1135, 1135 },
	{ 1137, 1137 },
	{ 1139, 1139 },
	{ 1141, 1141 },
	{ 1143, 1143 },
	{ 1145, 1145 },
	{ 1147, 1147 },
	{ 1149, 1149 },
	{ 1151, 1151 },
	{ 1153, 1153 },
	{ 1163, 1163 },
	{ 1165, 1165 },
	{ 1167, 1167 },
	{ 1169, 1169 },
	{ 1171, 1171 },
	{ 1173, 1173 },
	{ 1175, 1175 },
	{ 1177, 1177 },
	{ 1179, 1179 },
	{ 1181, 1181 },
	{ 1183, 1183 },
	{ 1185, 1185 },
	{ 1187, 1187 },
	{ 1189, 1189 },
	{ 1191, 1191 },
	{ 1193, 1193 },
	{ 1195, 1195 },
	{ 1197, 1197 },
	{ 1199, 1199 },
	{ 1201, 1201 },
	{ 1203, 1203 },
	{ 1205, 1205 },
	{ 1207, 1207 },
	{ 1209, 1209 },
	{ 1211, 1211 },
	{ 1213, 1213 },
	{ 1215, 1215 },
	{ 1218, 1218 },
	{ 1220, 1220 },
	{ 1222, 1222 },
	{ 1224, 1224 },
	{ 1226, 1226 },
	{ 1228, 1228 },
	{ 1230, 1231 },
	{ 1233, 1233 },
	{ 1235, 1235 },
	{ 1237, 1237 },
	{ 1239, 1239 },
	{ 1241, 1241 },
	{ 1243, 1243 },
	{ 1245, 1245 },
	{ 1247, 1247 },
	{ 1249, 1249 },
	{ 1251, 1251 },
	{ 1253, 1253 },
	{ 1255, 1255 },
	{ 1257, 1257 },
	{ 1259, 1259 },
	{ 1261, 1261 },
	{ 1263, 1263 },
	{ 1265, 1265 },
	{ 1267, 1267 },
	{ 1269, 1269 },
	{ 1271, 1271 },
	{ 1273, 1273 },
	{ 1275, 1275 },
	{ 1277, 1277 },
	{ 1279, 1279 },
	{ 1281, 1281 },
	{ 1283, 1283 },
	{ 1285, 1285 },
	{ 1287, 1287 },
	{ 1289, 1289 },
	{ 1291, 1291 },
	{ 1293, 1293 },
	{ 1295, 1295 },
	{ 1297, 1297 },
	{ 1299, 1299 },
	{ 1301, 1301 },
	{ 1303, 1303 },
	{ 1305, 1305 },
	{ 1307, 1307 },
	{ 1309, 1309 },
	{ 1311, 1311 },
	{ 1313, 1313 },
	{ 1315, 1315 },
	{ 1317, 1317 },
	{ 1319, 1319 },
	{ 1321, 1321 },
	{ 1323, 1323 },
	{ 1325, 1325 },
	{ 1327, 1327 },
	{ 1376, 1416 },
	{ 4304, 4346 },
	{ 4349, 4351 },
	{ 5112, 5117 },
	{ 7296, 7304 },
	{ 7424, 7467 },
	{ 7531, 7543 },
	{ 7545, 7578 },
	{ 7681, 7681 },
	{ 7683, 7683 },
	{ 7685, 7685 },
	{ 7687, 7687 },
	{ 7689, 7689 },
	{ 7691, 7691 },
	{ 7693, 7693 },
	{ 7695, 7695 },
	{ 7697, 7697 },
	{ 7699, 7699 },
	{ 7701, 7701 },
	{ 7703, 7703 },
	{ 7705, 7705 },
	{ 7707, 7707 },
	{ 7709, 7709 },
	{ 7711, 7711 },
	{ 7713, 7713 },
	{ 7715, 7715 },
	{ 7717, 7717 },
	{ 7719, 7719 },
	{ 7721, 7721 },
	{ 7723, 7723 },
	{ 7725, 7725 },
	{ 7727, 7727 },
	{ 7729, 7729 },
	{ 7731, 7731 },
	{ 7733, 7733 },
	{ 7735, 7735 },
	{ 7737, 7737 },
	{ 7739, 7739 },
	{ 7741, 7741 },
	{ 7743, 7743 },
	{ 7745, 7745 },
	{ 7747, 7747 },
	{ 7749, 7749 },
	{ 7751, 7751 },
	{ 7753, 7753 },
	{ 7755, 7755 },
	{ 7757, 7757 },
	{ 7759, 7759 },
	{ 7761, 7761 },
	{ 7763, 7763 },
	{ 7765, 7765 },
	{ 7767, 7767 },
	{ 7769, 7769 },
	{ 7771, 7771 },
	{ 7773, 7773 },
	{ 7775, 7775 },
	{ 7777, 7777 },
	{ 7779, 7779 },
	{ 7781, 7781 },
	{ 7783, 7783 },
	{ 7785, 7785 },
	{ 7787, 7787 },
	{ 7789, 7789 },
	{ 7791, 7791 },
	{ 7793, 7793 },
	{ 7795, 7795 },
	{ 7797, 7797 },
	{ 7799, 7799 },
	{ 7801, 7801 },
	{ 7803, 7803 },
	{ 7805, 7805 },
	{ 7807, 7807 },
	{ 7809, 7809 },
	{ 7811, 7811 },
	{ 7813, 7813 },
	{ 7815, 7815 },
	{ 7817, 7817 },
	{ 7819, 7819 },
	{ 7821, 7821 },
	{ 7823, 7823 },
	{ 7825, 7825 },
	{ 7827, 7827 },
	{ 7829, 7837 },
	{ 7839, 7839 },
	{ 7841, 7841 },
	{ 7843, 7843 },
	{ 7845, 7845 },
	{ 7847, 7847 },
	{ 7849, 7849 },
	{ 7851, 7851 },
	{ 7853, 7853 },
	{ 7855, 7855 },
	{ 7857, 7857 },
	{ 7859, 7859 },
	{ 7861, 7861 },
	{ 7863, 7863 },
	{ 7865, 7865 },
	{ 7867, 7867 },
	{ 7869, 7869 },
	{ 7871, 7871 },
	{ 7873, 7873 },
	{ 7875, 7875 },
	{ 7877, 7877 },
	{ 7879, 7879 },
	{ 7881, 7881 },
	{ 7883, 7883 },
	{ 7885, 7885 },
	{ 7887, 7887 },
	{ 7889, 7889 },
	{ 7891, 7891 },
	{ 7893, 7893 },
	{ 7895, 7895 },
	{ 7897, 7897 },
	{ 7899, 7899 },
	{ 7901, 7901 },
	{ 7903, 7903 },
	{ 7905, 7905 },
	{ 7907, 7907 },
	{ 7909, 7909 },
	{ 7911, 7911 },
	{ 7913, 7913 },
	{ 7915, 7915 },
	{ 7917, 7917 },
	{ 7919, 7919 },
	{ 7921, 7921 },
	{ 7923, 7923 },
	{ 7925, 7925 },
	{ 7927, 7927 },
	{ 7929, 7929 },
	{ 7931, 7931 },
	{ 7933, 7933 },
	{ 7935, 7943 },
	{ 7952, 7957 },
	{ 7968, 7975 },
	{ 7984, 7991 },
	{ 8000, 8005 },
	{ 8016, 8023 },
	{ 8032, 8039 },
	{ 8048, 8061 },
	{ 8064, 8071 },
	{ 8080, 8087 },
	{ 8096, 8103 },
	{ 8112, 8116 },
	{ 8118, 8119 },
	{ 8126, 8126 },
	{ 8130, 8132 },
	{ 8134, 8135 },
	{ 8144, 8147 },
	{ 8150, 8151 },
	{ 8160, 8167 },
	{ 8178, 8180 },
	{ 8182, 8183 },
	{ 8458, 8458 },
	{ 8462, 8463 },
	{ 8467, 8467 },
	{ 8495, 8495 },
	{ 8500, 8500 },
	{ 8505, 8505 },
	{ 8508, 8509 },
	{ 8518, 8521 },
	{ 8526, 8526 },
	{ 8580, 8580 },
	{ 11312, 11358 },
	{ 11361, 11361 },
	{ 11365, 11366 },
	{ 11368, 11368 },
	{ 11370, 11370 },
	{ 11372, 11372 },
	{ 11377, 11377 },
	{ 11379, 11380 },
	{ 11382, 11387 },
	{ 11393, 11393 },
	{ 11395, 11395 },
	{ 11397, 11397 },
	{ 11399, 11399 },
	{ 11401, 11401 },
	{ 11403, 11403 },
	{ 11405, 11405 },
	{ 11407, 11407 },
	{ 11409, 11409 },
	{ 11411, 11411 },
	{ 11413, 11413 },
	{ 11415, 11415 },
	{ 11417, 11417 },
	{ 11419, 11419 },
	{ 11421, 11421 },
	{ 11423, 11423 },
	{ 11425, 11425 },
	{ 11427, 11427 },
	{ 11429, 11429 },
	{ 11431, 11431 },
	{ 11433, 11433 },
	{ 11435, 11435 },
	{ 11437, 11437 },
	{ 11439, 11439 },
	{ 11441, 11441 },
	{ 11443, 11443 },
	{ 11445, 11445 },
	{ 11447, 11447 },
	{ 11449, 11449 },
	{ 11451, 11451 },
	{ 11453, 11453 },
	{ 11455, 11455 },
	{ 11457, 11457 },
	{ 11459, 11459 },
	{ 11461, 11461 },
	{ 11463, 11463 },
	{ 11465, 11465 },
	{ 11467, 11467 },
	{ 11469, 11469 },
	{ 11471, 11471 },
	{ 11473, 11473 },
	{ 11475, 11475 },
	{ 11477, 11477 },
	{ 11479, 11479 },
	{ 11481, 11481 },
	{ 11483, 11483 },
	{ 11485, 11485 },
	{ 11487, 11487 },
	{ 11489, 11489 },
	{ 11491, 11492 },
	{ 11500, 11500 },
	{ 11502, 11502 },
	{ 11507, 11507 },
	{ 11520, 11557 },
	{ 11559, 11559 },
	{ 11565, 11565 },
	{ 42561, 42561 },
	{ 42563, 42563 },
	{ 42565, 42565 },
	{ 42567, 42567 },
	{ 42569, 42569 },
	{ 42571, 42571 },
	{ 42573, 42573 },
	{ 42575, 42575 },
	{ 42577, 42577 },
	{ 42579, 42579 },
	{ 42581, 42581 },
	{ 42583, 42583 },
	{ 42585, 42585 },
	{ 42587, 42587 },
	{ 42589, 42589 },
	{ 42591, 42591 },
	{ 42593, 42593 },
	{ 42595, 42595 },
	{ 42597, 42597 },
	{ 42599, 42599 },
	{ 42601, 42601 },
	{ 42603, 42603 },
	{ 42605, 42605 },
	{ 42625, 42625 },
	{ 42627, 42627 },
	{ 42629, 42629 },
	{ 42631, 42631 },
	{ 42633, 42633 },
	{ 42635, 42635 },
	{ 42637, 42637 },
	{ 42639, 42639 },
	{ 42641, 42641 },
	{ 42643, 42643 },
	{ 42645, 42645 },
	{ 42647, 42647 },
	{ 42649, 42649 },
	{ 42651, 42651 },
	{ 42787, 42787 },
	{ 42789, 42789 },
	{ 42791, 42791 },
	{ 42793, 42793 },
	{ 42795, 42795 },
	{ 42797, 42797 },
	{ 42799, 42801 },
	{ 42803, 42803 },
	{ 42805, 42805 },
	{ 42807, 42807 },
	{ 42809, 42809 },
	{ 42811, 42811 },
	{ 42813, 42813 },
	{ 42815, 42815 },
	{ 42817, 42817 },
	{ 42819, 42819 },
	{ 42821, 42821 },
	{ 42823, 42823 },
	{ 42825, 42825 },
	{ 42827, 42827 },
	{ 42829, 42829 },
	{ 42831, 42831 },
	{ 42833, 42833 },
	{ 42835, 42835 },
	{ 42837, 42837 },
	{ 42839, 42839 },
	{ 42841, 42841 },
	{ 42843, 42843 },
	{ 42845, 42845 },
	{ 42847, 42847 },
	{ 42849, 42849 },
	{ 42851, 42851 },
	{ 42853, 42853 },
	{ 42855, 42855 },
	{ 42857, 42857 },
	{ 42859, 42859 },
	{ 42861, 42861 },
	{ 42863, 42863 },
	{ 42865, 42872 },
	{ 42874, 42874 },
	{ 42876, 42876 },
	{ 42879, 42879 },
	{ 42881, 42881 },
	{ 42883, 42883 },
	{ 42885, 42885 },
	{ 42887, 42887 },
	{ 42892, 42892 },
	{ 42894, 42894 },
	{ 42897, 42897 },
	{ 42899, 42901 },
	{ 42903, 42903 },
	{ 42905, 42905 },
	{ 42907, 42907 },
	{ 42909, 42909 },
	{ 42911, 42911 },
	{ 42913, 42913 },
	{ 42915, 42915 },
	{ 42917, 42917 },
	{ 42919, 42919 },
	{ 42921, 42921 },
	{ 42927, 42927 },
	{ 42933, 42933 },
	{ 42935, 42935 },
	{ 42937, 42937 },
	{ 43002, 43002 },
	{ 43824, 43866 },
	{ 43872, 43877 },
	{ 43888, 43967 },
	{ 64256, 64262 },
	{ 64275, 64279 },
	{ 65345, 65370 },
};
static const URange32 Ll_range32[] = {
	{ 66600, 66639 },
	{ 66776, 66811 },
	{ 68800, 68850 },
	{ 71872, 71903 },
	{ 93792, 93823 },
	{ 119834, 119859 },
	{ 119886, 119892 },
	{ 119894, 119911 },
	{ 119938, 119963 },
	{ 119990, 119993 },
	{ 119995, 119995 },
	{ 119997, 120003 },
	{ 120005, 120015 },
	{ 120042, 120067 },
	{ 120094, 120119 },
	{ 120146, 120171 },
	{ 120198, 120223 },
	{ 120250, 120275 },
	{ 120302, 120327 },
	{ 120354, 120379 },
	{ 120406, 120431 },
	{ 120458, 120485 },
	{ 120514, 120538 },
	{ 120540, 120545 },
	{ 120572, 120596 },
	{ 120598, 120603 },
	{ 120630, 120654 },
	{ 120656, 120661 },
	{ 120688, 120712 },
	{ 120714, 120719 },
	{ 120746, 120770 },
	{ 120772, 120777 },
	{ 120779, 120779 },
	{ 125218, 125251 },
};
static const URange16 Lm_range16[] = {
	{ 688, 705 },
	{ 710, 721 },
	{ 736, 740 },
	{ 748, 748 },
	{ 750, 750 },
	{ 884, 884 },
	{ 890, 890 },
	{ 1369, 1369 },
	{ 1600, 1600 },
	{ 1765, 1766 },
	{ 2036, 2037 },
	{ 2042, 2042 },
	{ 2074, 2074 },
	{ 2084, 2084 },
	{ 2088, 2088 },
	{ 2417, 2417 },
	{ 3654, 3654 },
	{ 3782, 3782 },
	{ 4348, 4348 },
	{ 6103, 6103 },
	{ 6211, 6211 },
	{ 6823, 6823 },
	{ 7288, 7293 },
	{ 7468, 7530 },
	{ 7544, 7544 },
	{ 7579, 7615 },
	{ 8305, 8305 },
	{ 8319, 8319 },
	{ 8336, 8348 },
	{ 11388, 11389 },
	{ 11631, 11631 },
	{ 11823, 11823 },
	{ 12293, 12293 },
	{ 12337, 12341 },
	{ 12347, 12347 },
	{ 12445, 12446 },
	{ 12540, 12542 },
	{ 40981, 40981 },
	{ 42232, 42237 },
	{ 42508, 42508 },
	{ 42623, 42623 },
	{ 42652, 42653 },
	{ 42775, 42783 },
	{ 42864, 42864 },
	{ 42888, 42888 },
	{ 43000, 43001 },
	{ 43471, 43471 },
	{ 43494, 43494 },
	{ 43632, 43632 },
	{ 43741, 43741 },
	{ 43763, 43764 },
	{ 43868, 43871 },
	{ 65392, 65392 },
	{ 65438, 65439 },
};
static const URange32 Lm_range32[] = {
	{ 92992, 92995 },
	{ 94099, 94111 },
	{ 94176, 94177 },
};
static const URange16 Nd_range16[] = {
	{ 48, 57 },
	{ 1632, 1641 },
	{ 1776, 1785 },
	{ 1984, 1993 },
	{ 2406, 2415 },
	{ 2534, 2543 },
	{ 2662, 2671 },
	{ 2790, 2799 },
	{ 2918, 2927 },
	{ 3046, 3055 },
	{ 3174, 3183 },
	{ 3302, 3311 },
	{ 3430, 3439 },
	{ 3558, 3567 },
	{ 3664, 3673 },
	{ 3792, 3801 },
	{ 3872, 3881 },
	{ 4160, 4169 },
	{ 4240, 4249 },
	{ 6112, 6121 },
	{ 6160, 6169 },
	{ 6470, 6479 },
	{ 6608, 6617 },
	{ 6784, 6793 },
	{ 6800, 6809 },
	{ 6992, 7001 },
	{ 7088, 7097 },
	{ 7232, 7241 },
	{ 7248, 7257 },
	{ 42528, 42537 },
	{ 43216, 43225 },
	{ 43264, 43273 },
	{ 43472, 43481 },
	{ 43504, 43513 },
	{ 43600, 43609 },
	{ 44016, 44025 },
	{ 65296, 65305 },
};
static const URange32 Nd_range32[] = {
	{ 66720, 66729 },
	{ 68912, 68921 },
	{ 69734, 69743 },
	{ 69872, 69881 },
	{ 69942, 69951 },
	{ 70096, 70105 },
	{ 70384, 70393 },
	{ 70736, 70745 },
	{ 70864, 70873 },
	{ 71248, 71257 },
	{ 71360, 71369 },
	{ 71472, 71481 },
	{ 71904, 71913 },
	{ 72784, 72793 },
	{ 73040, 73049 },
	{ 73120, 73129 },
	{ 92768, 92777 },
	{ 93008, 93017 },
	{ 120782, 120831 },
	{ 125264, 125273 },
};
static const URange16 Pc_range16[] = {
	{ 95, 95 },
	{ 8255, 8256 },
	{ 8276, 8276 },
	{ 65075, 65076 },
	{ 65101, 65103 },
	{ 65343, 65343 },
};
static const URange16 Lt_range16[] = {
	{ 453, 453 },
	{ 456, 456 },
	{ 459, 459 },
	{ 498, 498 },
	{ 8072, 8079 },
	{ 8088, 8095 },
	{ 8104, 8111 },
	{ 8124, 8124 },
	{ 8140, 8140 },
	{ 8188, 8188 },
};
static const URange16 Lu_range16[] = {
	{ 65, 90 },
	{ 192, 214 },
	{ 216, 222 },
	{ 256, 256 },
	{ 258, 258 },
	{ 260, 260 },
	{ 262, 262 },
	{ 264, 264 },
	{ 266, 266 },
	{ 268, 268 },
	{ 270, 270 },
	{ 272, 272 },
	{ 274, 274 },
	{ 276, 276 },
	{ 278, 278 },
	{ 280, 280 },
	{ 282, 282 },
	{ 284, 284 },
	{ 286, 286 },
	{ 288, 288 },
	{ 290, 290 },
	{ 292, 292 },
	{ 294, 294 },
	{ 296, 296 },
	{ 298, 298 },
	{ 300, 300 },
	{ 302, 302 },
	{ 304, 304 },
	{ 306, 306 },
	{ 308, 308 },
	{ 310, 310 },
	{ 313, 313 },
	{ 315, 315 },
	{ 317, 317 },
	{ 319, 319 },
	{ 321, 321 },
	{ 323, 323 },
	{ 325, 325 },
	{ 327, 327 },
	{ 330, 330 },
	{ 332, 332 },
	{ 334, 334 },
	{ 336, 336 },
	{ 338, 338 },
	{ 340, 340 },
	{ 342, 342 },
	{ 344, 344 },
	{ 346, 346 },
	{ 348, 348 },
	{ 350, 350 },
	{ 352, 352 },
	{ 354, 354 },
	{ 356, 356 },
	{ 358, 358 },
	{ 360, 360 },
	{ 362, 362 },
	{ 364, 364 },
	{ 366, 366 },
	{ 368, 368 },
	{ 370, 370 },
	{ 372, 372 },
	{ 374, 374 },
	{ 376, 377 },
	{ 379, 379 },
	{ 381, 381 },
	{ 385, 386 },
	{ 388, 388 },
	{ 390, 391 },
	{ 393, 395 },
	{ 398, 401 },
	{ 403, 404 },
	{ 406, 408 },
	{ 412, 413 },
	{ 415, 416 },
	{ 418, 418 },
	{ 420, 420 },
	{ 422, 423 },
	{ 425, 425 },
	{ 428, 428 },
	{ 430, 431 },
	{ 433, 435 },
	{ 437, 437 },
	{ 439, 440 },
	{ 444, 444 },
	{ 452, 452 },
	{ 455, 455 },
	{ 458, 458 },
	{ 461, 461 },
	{ 463, 463 },
	{ 465, 465 },
	{ 467, 467 },
	{ 469, 469 },
	{ 471, 471 },
	{ 473, 473 },
	{ 475, 475 },
	{ 478, 478 },
	{ 480, 480 },
	{ 482, 482 },
	{ 484, 484 },
	{ 486, 486 },
	{ 488, 488 },
	{ 490, 490 },
	{ 492, 492 },
	{ 494, 494 },
	{ 497, 497 },
	{ 500, 500 },
	{ 502, 504 },
	{ 506, 506 },
	{ 508, 508 },
	{ 510, 510 },
	{ 512, 512 },
	{ 514, 514 },
	{ 516, 516 },
	{ 518, 518 },
	{ 520, 520 },
	{ 522, 522 },
	{ 524, 524 },
	{ 526, 526 },
	{ 528, 528 },
	{ 530, 530 },
	{ 532, 532 },
	{ 534, 534 },
	{ 536, 536 },
	{ 538, 538 },
	{ 540, 540 },
	{ 542, 542 },
	{ 544, 544 },
	{ 546, 546 },
	{ 548, 548 },
	{ 550, 550 },
	{ 552, 552 },
	{ 554, 554 },
	{ 556, 556 },
	{ 558, 558 },
	{ 560, 560 },
	{ 562, 562 },
	{ 570, 571 },
	{ 573, 574 },
	{ 577, 577 },
	{ 579, 582 },
	{ 584, 584 },
	{ 586, 586 },
	{ 588, 588 },
	{ 590, 590 },
	{ 880, 880 },
	{ 882, 882 },
	{ 886, 886 },
	{ 895, 895 },
	{ 902, 902 },
	{ 904, 906 },
	{ 908, 908 },
	{ 910, 911 },
	{ 913, 929 },
	{ 931, 939 },
	{ 975, 975 },
	{ 978, 980 },
	{ 984, 984 },
	{ 986, 986 },
	{ 988, 988 },
	{ 990, 990 },
	{ 992, 992 },
	{ 994, 994 },
	{ 996, 996 },
	{ 998, 998 },
	{ 1000, 1000 },
	{ 1002, 1002 },
	{ 1004, 1004 },
	{ 1006, 1006 },
	{ 1012, 1012 },
	{ 1015, 1015 },
	{ 1017, 1018 },
	{ 1021, 1071 },
	{ 1120, 1120 },
	{ 1122, 1122 },
	{ 1124, 1124 },
	{ 1126, 1126 },
	{ 1128, 1128 },
	{ 1130, 1130 },
	{ 1132, 1132 },
	{ 1134, 1134 },
	{ 1136, 1136 },
	{ 1138, 1138 },
	{ 1140, 1140 },
	{ 1142, 1142 },
	{ 1144, 1144 },
	{ 1146, 1146 },
	{ 1148, 1148 },
	{ 1150, 1150 },
	{ 1152, 1152 },
	{ 1162, 1162 },
	{ 1164, 1164 },
	{ 1166, 1166 },
	{ 1168, 1168 },
	{ 1170, 1170 },
	{ 1172, 1172 },
	{ 1174, 1174 },
	{ 1176, 1176 },
	{ 1178, 1178 },
	{ 1180, 1180 },
	{ 1182, 1182 },
	{ 1184, 1184 },
	{ 1186, 1186 },
	{ 1188, 1188 },
	{ 1190, 1190 },
	{ 1192, 1192 },
	{ 1194, 1194 },
	{ 1196, 1196 },
	{ 1198, 1198 },
	{ 1200, 1200 },
	{ 1202, 1202 },
	{ 1204, 1204 },
	{ 1206, 1206 },
	{ 1208, 1208 },
	{ 1210, 1210 },
	{ 1212, 1212 },
	{ 1214, 1214 },
	{ 1216, 1217 },
	{ 1219, 1219 },
	{ 1221, 1221 },
	{ 1223, 1223 },
	{ 1225, 1225 },
	{ 1227, 1227 },
	{ 1229, 1229 },
	{ 1232, 1232 },
	{ 1234, 1234 },
	{ 1236, 1236 },
	{ 1238, 1238 },
	{ 1240, 1240 },
	{ 1242, 1242 },
	{ 1244, 1244 },
	{ 1246, 1246 },
	{ 1248, 1248 },
	{ 1250, 1250 },
	{ 1252, 1252 },
	{ 1254, 1254 },
	{ 1256, 1256 },
	{ 1258, 1258 },
	{ 1260, 1260 },
	{ 1262, 1262 },
	{ 1264, 1264 },
	{ 1266, 1266 },
	{ 1268, 1268 },
	{ 1270, 1270 },
	{ 1272, 1272 },
	{ 1274, 1274 },
	{ 1276, 1276 },
	{ 1278, 1278 },
	{ 1280, 1280 },
	{ 1282, 1282 },
	{ 1284, 1284 },
	{ 1286, 1286 },
	{ 1288, 1288 },
	{ 1290, 1290 },
	{ 1292, 1292 },
	{ 1294, 1294 },
	{ 1296, 1296 },
	{ 1298, 1298 },
	{ 1300, 1300 },
	{ 1302, 1302 },
	{ 1304, 1304 },
	{ 1306, 1306 },
	{ 1308, 1308 },
	{ 1310, 1310 },
	{ 1312, 1312 },
	{ 1314, 1314 },
	{ 1316, 1316 },
	{ 1318, 1318 },
	{ 1320, 1320 },
	{ 1322, 1322 },
	{ 1324, 1324 },
	{ 1326, 1326 },
	{ 1329, 1366 },
	{ 4256, 4293 },
	{ 4295, 4295 },
	{ 4301, 4301 },
	{ 5024, 5109 },
	{ 7312, 7354 },
	{ 7357, 7359 },
	{ 7680, 7680 },
	{ 7682, 7682 },
	{ 7684, 7684 },
	{ 7686, 7686 },
	{ 7688, 7688 },
	{ 7690, 7690 },
	{ 7692, 7692 },
	{ 7694, 7694 },
	{ 7696, 7696 },
	{ 7698, 7698 },
	{ 7700, 7700 },
	{ 7702, 7702 },
	{ 7704, 7704 },
	{ 7706, 7706 },
	{ 7708, 7708 },
	{ 7710, 7710 },
	{ 7712, 7712 },
	{ 7714, 7714 },
	{ 7716, 7716 },
	{ 7718, 7718 },
	{ 7720, 7720 },
	{ 7722, 7722 },
	{ 7724, 7724 },
	{ 7726, 7726 },
	{ 7728, 7728 },
	{ 7730, 7730 },
	{ 7732, 7732 },
	{ 7734, 7734 },
	{ 7736, 7736 },
	{ 7738, 7738 },
	{ 7740, 7740 },
	{ 7742, 7742 },
	{ 7744, 7744 },
	{ 7746, 7746 },
	{ 7748, 7748 },
	{ 7750, 7750 },
	{ 7752, 7752 },
	{ 7754, 7754 },
	{ 7756, 7756 },
	{ 7758, 7758 },
	{ 7760, 7760 },
	{ 7762, 7762 },
	{ 7764, 7764 },
	{ 7766, 7766 },
	{ 7768, 7768 },
	{ 7770, 7770 },
	{ 7772, 7772 },
	{ 7774, 7774 },
	{ 7776, 7776 },
	{ 7778, 7778 },
	{ 7780, 7780 },
	{ 7782, 7782 },
	{ 7784, 7784 },
	{ 7786, 7786 },
	{ 7788, 7788 },
	{ 7790, 7790 },
	{ 7792, 7792 },
	{ 7794, 7794 },
	{ 7796, 7796 },
	{ 7798, 7798 },
	{ 7800, 7800 },
	{ 7802, 7802 },
	{ 7804, 7804 },
	{ 7806, 7806 },
	{ 7808, 7808 },
	{ 7810, 7810 },
	{ 7812, 7812 },
	{ 7814, 7814 },
	{ 7816, 7816 },
	{ 7818, 7818 },
	{ 7820, 7820 },
	{ 7822, 7822 },
	{ 7824, 7824 },
	{ 7826, 7826 },
	{ 7828, 7828 },
	{ 7838, 7838 },
	{ 7840, 7840 },
	{ 7842, 7842 },
	{ 7844, 7844 },
	{ 7846, 7846 },
	{ 7848, 7848 },
	{ 7850, 7850 },
	{ 7852, 7852 },
	{ 7854, 7854 },
	{ 7856, 7856 },
	{ 7858, 7858 },
	{ 7860, 7860 },
	{ 7862, 7862 },
	{ 7864, 7864 },
	{ 7866, 7866 },
	{ 7868, 7868 },
	{ 7870, 7870 },
	{ 7872, 7872 },
	{ 7874, 7874 },
	{ 7876, 7876 },
	{ 7878, 7878 },
	{ 7880, 7880 },
	{ 7882, 7882 },
	{ 7884, 7884 },
	{ 7886, 7886 },
	{ 7888, 7888 },
	{ 7890, 7890 },
	{ 7892, 7892 },
	{ 7894, 7894 },
	{ 7896, 7896 },
	{ 7898, 7898 },
	{ 7900, 7900 },
	{ 7902, 7902 },
	{ 7904, 7904 },
	{ 7906, 7906 },
	{ 7908, 7908 },
	{ 7910, 7910 },
	{ 7912, 7912 },
	{ 7914, 7914 },
	{ 7916, 7916 },
	{ 7918, 7918 },
	{ 7920, 7920 },
	{ 7922, 7922 },
	{ 7924, 7924 },
	{ 7926, 7926 },
	{ 7928, 7928 },
	{ 7930, 7930 },
	{ 7932, 7932 },
	{ 7934, 7934 },
	{ 7944, 7951 },
	{ 7960, 7965 },
	{ 7976, 7983 },
	{ 7992, 7999 },
	{ 8008, 8013 },
	{ 8025, 8025 },
	{ 8027, 8027 },
	{ 8029, 8029 },
	{ 8031, 8031 },
	{ 8040, 8047 },
	{ 8120, 8123 },
	{ 8136, 8139 },
	{ 8152, 8155 },
	{ 8168, 8172 },
	{ 8184, 8187 },
	{ 8450, 8450 },
	{ 8455, 8455 },
	{ 8459, 8461 },
	{ 8464, 8466 },
	{ 8469, 8469 },
	{ 8473, 8477 },
	{ 8484, 8484 },
	{ 8486, 8486 },
	{ 8488, 8488 },
	{ 8490, 8493 },
	{ 8496, 8499 },
	{ 8510, 8511 },
	{ 8517, 8517 },
	{ 8579, 8579 },
	{ 11264, 11310 },
	{ 11360, 11360 },
	{ 11362, 11364 },
	{ 11367, 11367 },
	{ 11369, 11369 },
	{ 11371, 11371 },
	{ 11373, 11376 },
	{ 11378, 11378 },
	{ 11381, 11381 },
	{ 11390, 11392 },
	{ 11394, 11394 },
	{ 11396, 11396 },
	{ 11398, 11398 },
	{ 11400, 11400 },
	{ 11402, 11402 },
	{ 11404, 11404 },
	{ 11406, 11406 },
	{ 11408, 11408 },
	{ 11410, 11410 },
	{ 11412, 11412 },
	{ 11414, 11414 },
	{ 11416, 11416 },
	{ 11418, 11418 },
	{ 11420, 11420 },
	{ 11422, 11422 },
	{ 11424, 11424 },
	{ 11426, 11426 },
	{ 11428, 11428 },
	{ 11430, 11430 },
	{ 11432, 11432 },
	{ 11434, 11434 },
	{ 11436, 11436 },
	{ 11438, 11438 },
	{ 11440, 11440 },
	{ 11442, 11442 },
	{ 11444, 11444 },
	{ 11446, 11446 },
	{ 11448, 11448 },
	{ 11450, 11450 },
	{ 11452, 11452 },
	{ 11454, 11454 },
	{ 11456, 11456 },
	{ 11458, 11458 },
	{ 11460, 11460 },
	{ 11462, 11462 },
	{ 11464, 11464 },
	{ 11466, 11466 },
	{ 11468, 11468 },
	{ 11470, 11470 },
	{ 11472, 11472 },
	{ 11474, 11474 },
	{ 11476, 11476 },
	{ 11478, 11478 },
	{ 11480, 11480 },
	{ 11482, 11482 },
	{ 11484, 11484 },
	{ 11486, 11486 },
	{ 11488, 11488 },
	{ 11490, 11490 },
	{ 11499, 11499 },
	{ 11501, 11501 },
	{ 11506, 11506 },
	{ 42560, 42560 },
	{ 42562, 42562 },
	{ 42564, 42564 },
	{ 42566, 42566 },
	{ 42568, 42568 },
	{ 42570, 42570 },
	{ 42572, 42572 },
	{ 42574, 42574 },
	{ 42576, 42576 },
	{ 42578, 42578 },
	{ 42580, 42580 },
	{ 42582, 42582 },
	{ 42584, 42584 },
	{ 42586, 42586 },
	{ 42588, 42588 },
	{ 42590, 42590 },
	{ 42592, 42592 },
	{ 42594, 42594 },
	{ 42596, 42596 },
	{ 42598, 42598 },
	{ 42600, 42600 },
	{ 42602, 42602 },
	{ 42604, 42604 },
	{ 42624, 42624 },
	{ 42626, 42626 },
	{ 42628, 42628 },
	{ 42630, 42630 },
	{ 42632, 42632 },
	{ 42634, 42634 },
	{ 42636, 42636 },
	{ 42638, 42638 },
	{ 42640, 42640 },
	{ 42642, 42642 },
	{ 42644, 42644 },
	{ 42646, 42646 },
	{ 42648, 42648 },
	{ 42650, 42650 },
	{ 42786, 42786 },
	{ 42788, 42788 },
	{ 42790, 42790 },
	{ 42792, 42792 },
	{ 42794, 42794 },
	{ 42796, 42796 },
	{ 42798, 42798 },
	{ 42802, 42802 },
	{ 42804, 42804 },
	{ 42806, 42806 },
	{ 42808, 42808 },
	{ 42810, 42810 },
	{ 42812, 42812 },
	{ 42814, 42814 },
	{ 42816, 42816 },
	{ 42818, 42818 },
	{ 42820, 42820 },
	{ 42822, 42822 },
	{ 42824, 42824 },
	{ 42826, 42826 },
	{ 42828, 42828 },
	{ 42830, 42830 },
	{ 42832, 42832 },
	{ 42834, 42834 },
	{ 42836, 42836 },
	{ 42838, 42838 },
	{ 42840, 42840 },
	{ 42842, 42842 },
	{ 42844, 42844 },
	{ 42846, 42846 },
	{ 42848, 42848 },
	{ 42850, 42850 },
	{ 42852, 42852 },
	{ 42854, 42854 },
	{ 42856, 42856 },
	{ 42858, 42858 },
	{ 42860, 42860 },
	{ 42862, 42862 },
	{ 42873, 42873 },
	{ 42875, 42875 },
	{ 42877, 42878 },
	{ 42880, 42880 },
	{ 42882, 42882 },
	{ 42884, 42884 },
	{ 42886, 42886 },
	{ 42891, 42891 },
	{ 42893, 42893 },
	{ 42896, 42896 },
	{ 42898, 42898 },
	{ 42902, 42902 },
	{ 42904, 42904 },
	{ 42906, 42906 },
	{ 42908, 42908 },
	{ 42910, 42910 },
	{ 42912, 42912 },
	{ 42914, 42914 },
	{ 42916, 42916 },
	{ 42918, 42918 },
	{ 42920, 42920 },
	{ 42922, 42926 },
	{ 42928, 42932 },
	{ 42934, 42934 },
	{ 42936, 42936 },
	{ 65313, 65338 },
};
static const URange32 Lu_range32[] = {
	{ 66560, 66599 },
	{ 66736, 66771 },
	{ 68736, 68786 },
	{ 71840, 71871 },
	{ 93760, 93791 },
	{ 119808, 119833 },
	{ 119860, 119885 },
	{ 119912, 119937 },
	{ 119964, 119964 },
	{ 119966, 119967 },
	{ 119970, 119970 },
	{ 119973, 119974 },
	{ 119977, 119980 },
	{ 119982, 119989 },
	{ 120016, 120041 },
	{ 120068, 120069 },
	{ 120071, 120074 },
	{ 120077, 120084 },
	{ 120086, 120092 },
	{ 120120, 120121 },
	{ 120123, 120126 },
	{ 120128, 120132 },
	{ 120134, 120134 },
	{ 120138, 120144 },
	{ 120172, 120197 },
	{ 120224, 120249 },
	{ 120276, 120301 },
	{ 120328, 120353 },
	{ 120380, 120405 },
	{ 120432, 120457 },
	{ 120488, 120512 },
	{ 120546, 120570 },
	{ 120604, 120628 },
	{ 120662, 120686 },
	{ 120720, 120744 },
	{ 120778, 120778 },
	{ 125184, 125217 },
};
static const URange16 Pf_range16[] = {
	{ 187, 187 },
	{ 8217, 8217 },
	{ 8221, 8221 },
	{ 8250, 8250 },
	{ 11779, 11779 },
	{ 11781, 11781 },
	{ 11786, 11786 },
	{ 11789, 11789 },
	{ 11805, 11805 },
	{ 11809, 11809 },
};
static const URange16 Pd_range16[] = {
	{ 45, 45 },
	{ 1418, 1418 },
	{ 1470, 1470 },
	{ 5120, 5120 },
	{ 6150, 6150 },
	{ 8208, 8213 },
	{ 11799, 11799 },
	{ 11802, 11802 },
	{ 11834, 11835 },
	{ 11840, 11840 },
	{ 12316, 12316 },
	{ 12336, 12336 },
	{ 12448, 12448 },
	{ 65073, 65074 },
	{ 65112, 65112 },
	{ 65123, 65123 },
	{ 65293, 65293 },
};
static const URange16 Pe_range16[] = {
	{ 41, 41 },
	{ 93, 93 },
	{ 125, 125 },
	{ 3899, 3899 },
	{ 3901, 3901 },
	{ 5788, 5788 },
	{ 8262, 8262 },
	{ 8318, 8318 },
	{ 8334, 8334 },
	{ 8969, 8969 },
	{ 8971, 8971 },
	{ 9002, 9002 },
	{ 10089, 10089 },
	{ 10091, 10091 },
	{ 10093, 10093 },
	{ 10095, 10095 },
	{ 10097, 10097 },
	{ 10099, 10099 },
	{ 10101, 10101 },
	{ 10182, 10182 },
	{ 10215, 10215 },
	{ 10217, 10217 },
	{ 10219, 10219 },
	{ 10221, 10221 },
	{ 10223, 10223 },
	{ 10628, 10628 },
	{ 10630, 10630 },
	{ 10632, 10632 },
	{ 10634, 10634 },
	{ 10636, 10636 },
	{ 10638, 10638 },
	{ 10640, 10640 },
	{ 10642, 10642 },
	{ 10644, 10644 },
	{ 10646, 10646 },
	{ 10648, 10648 },
	{ 10713, 10713 },
	{ 10715, 10715 },
	{ 10749, 10749 },
	{ 11811, 11811 },
	{ 11813, 11813 },
	{ 11815, 11815 },
	{ 11817, 11817 },
	{ 12297, 12297 },
	{ 12299, 12299 },
	{ 12301, 12301 },
	{ 12303, 12303 },
	{ 12305, 12305 },
	{ 12309, 12309 },
	{ 12311, 12311 },
	{ 12313, 12313 },
	{ 12315, 12315 },
	{ 12318, 12319 },
	{ 64830, 64830 },
	{ 65048, 65048 },
	{ 65078, 65078 },
	{ 65080, 65080 },
	{ 65082, 65082 },
	{ 65084, 65084 },
	{ 65086, 65086 },
	{ 65088, 65088 },
	{ 65090, 65090 },
	{ 65092, 65092 },
	{ 65096, 65096 },
	{ 65114, 65114 },
	{ 65116, 65116 },
	{ 65118, 65118 },
	{ 65289, 65289 },
	{ 65341, 65341 },
	{ 65373, 65373 },
	{ 65376, 65376 },
	{ 65379, 65379 },
};
static const URange16 Pi_range16[] = {
	{ 171, 171 },
	{ 8216, 8216 },
	{ 8219, 8220 },
	{ 8223, 8223 },
	{ 8249, 8249 },
	{ 11778, 11778 },
	{ 11780, 11780 },
	{ 11785, 11785 },
	{ 11788, 11788 },
	{ 11804, 11804 },
	{ 11808, 11808 },
};
static const URange16 Po_range16[] = {
	{ 33, 35 },
	{ 37, 39 },
	{ 42, 42 },
	{ 44, 44 },
	{ 46, 47 },
	{ 58, 59 },
	{ 63, 64 },
	{ 92, 92 },
	{ 161, 161 },
	{ 167, 167 },
	{ 182, 183 },
	{ 191, 191 },
	{ 894, 894 },
	{ 903, 903 },
	{ 1370, 1375 },
	{ 1417, 1417 },
	{ 1472, 1472 },
	{ 1475, 1475 },
	{ 1478, 1478 },
	{ 1523, 1524 },
	{ 1545, 1546 },
	{ 1548, 1549 },
	{ 1563, 1563 },
	{ 1566, 1567 },
	{ 1642, 1645 },
	{ 1748, 1748 },
	{ 1792, 1805 },
	{ 2039, 2041 },
	{ 2096, 2110 },
	{ 2142, 2142 },
	{ 2404, 2405 },
	{ 2416, 2416 },
	{ 2557, 2557 },
	{ 2678, 2678 },
	{ 2800, 2800 },
	{ 3204, 3204 },
	{ 3572, 3572 },
	{ 3663, 3663 },
	{ 3674, 3675 },
	{ 3844, 3858 },
	{ 3860, 3860 },
	{ 3973, 3973 },
	{ 4048, 4052 },
	{ 4057, 4058 },
	{ 4170, 4175 },
	{ 4347, 4347 },
	{ 4960, 4968 },
	{ 5741, 5742 },
	{ 5867, 5869 },
	{ 5941, 5942 },
	{ 6100, 6102 },
	{ 6104, 6106 },
	{ 6144, 6149 },
	{ 6151, 6154 },
	{ 6468, 6469 },
	{ 6686, 6687 },
	{ 6816, 6822 },
	{ 6824, 6829 },
	{ 7002, 7008 },
	{ 7164, 7167 },
	{ 7227, 7231 },
	{ 7294, 7295 },
	{ 7360, 7367 },
	{ 7379, 7379 },
	{ 8214, 8215 },
	{ 8224, 8231 },
	{ 8240, 8248 },
	{ 8251, 8254 },
	{ 8257, 8259 },
	{ 8263, 8273 },
	{ 8275, 8275 },
	{ 8277, 8286 },
	{ 11513, 11516 },
	{ 11518, 11519 },
	{ 11632, 11632 },
	{ 11776, 11777 },
	{ 11782, 11784 },
	{ 11787, 11787 },
	{ 11790, 11798 },
	{ 11800, 11801 },
	{ 11803, 11803 },
	{ 11806, 11807 },
	{ 11818, 11822 },
	{ 11824, 11833 },
	{ 11836, 11839 },
	{ 11841, 11841 },
	{ 11843, 11854 },
	{ 12289, 12291 },
	{ 12349, 12349 },
	{ 12539, 12539 },
	{ 42238, 42239 },
	{ 42509, 42511 },
	{ 42611, 42611 },
	{ 42622, 42622 },
	{ 42738, 42743 },
	{ 43124, 43127 },
	{ 43214, 43215 },
	{ 43256, 43258 },
	{ 43260, 43260 },
	{ 43310, 43311 },
	{ 43359, 43359 },
	{ 43457, 43469 },
	{ 43486, 43487 },
	{ 43612, 43615 },
	{ 43742, 43743 },
	{ 43760, 43761 },
	{ 44011, 44011 },
	{ 65040, 65046 },
	{ 65049, 65049 },
	{ 65072, 65072 },
	{ 65093, 65094 },
	{ 65097, 65100 },
	{ 65104, 65106 },
	{ 65108, 65111 },
	{ 65119, 65121 },
	{ 65128, 65128 },
	{ 65130, 65131 },
	{ 65281, 65283 },
	{ 65285, 65287 },
	{ 65290, 65290 },
	{ 65292, 65292 },
	{ 65294, 65295 },
	{ 65306, 65307 },
	{ 65311, 65312 },
	{ 65340, 65340 },
	{ 65377, 65377 },
	{ 65380, 65381 },
};
static const URange32 Po_range32[] = {
	{ 65792, 65794 },
	{ 66463, 66463 },
	{ 66512, 66512 },
	{ 66927, 66927 },
	{ 67671, 67671 },
	{ 67871, 67871 },
	{ 67903, 67903 },
	{ 68176, 68184 },
	{ 68223, 68223 },
	{ 68336, 68342 },
	{ 68409, 68415 },
	{ 68505, 68508 },
	{ 69461, 69465 },
	{ 69703, 69709 },
	{ 69819, 69820 },
	{ 69822, 69825 },
	{ 69952, 69955 },
	{ 70004, 70005 },
	{ 70085, 70088 },
	{ 70093, 70093 },
	{ 70107, 70107 },
	{ 70109, 70111 },
	{ 70200, 70205 },
	{ 70313, 70313 },
	{ 70731, 70735 },
	{ 70747, 70747 },
	{ 70749, 70749 },
	{ 70854, 70854 },
	{ 71105, 71127 },
	{ 71233, 71235 },
	{ 71264, 71276 },
	{ 71484, 71486 },
	{ 71739, 71739 },
	{ 72255, 72262 },
	{ 72346, 72348 },
	{ 72350, 72354 },
	{ 72769, 72773 },
	{ 72816, 72817 },
	{ 73463, 73464 },
	{ 74864, 74868 },
	{ 92782, 92783 },
	{ 92917, 92917 },
	{ 92983, 92987 },
	{ 92996, 92996 },
	{ 93847, 93850 },
	{ 113823, 113823 },
	{ 121479, 121483 },
	{ 125278, 125279 },
};
static const URange16 Me_range16[] = {
	{ 1160, 1161 },
	{ 6846, 6846 },
	{ 8413, 8416 },
	{ 8418, 8420 },
	{ 42608, 42610 },
};
static const URange16 C_range16[] = {
	{ 0, 31 },
	{ 127, 159 },
	{ 173, 173 },
	{ 1536, 1541 },
	{ 1564, 1564 },
	{ 1757, 1757 },
	{ 1807, 1807 },
	{ 2274, 2274 },
	{ 6158, 6158 },
	{ 8203, 8207 },
	{ 8234, 8238 },
	{ 8288, 8292 },
	{ 8294, 8303 },
	{ 55296, 63743 },
	{ 65279, 65279 },
	{ 65529, 65531 },
};
static const URange32 C_range32[] = {
	{ 69821, 69821 },
	{ 69837, 69837 },
	{ 113824, 113827 },
	{ 119155, 119162 },
	{ 917505, 917505 },
	{ 917536, 917631 },
	{ 983040, 1048573 },
	{ 1048576, 1114109 },
};
static const URange16 Mc_range16[] = {
	{ 2307, 2307 },
	{ 2363, 2363 },
	{ 2366, 2368 },
	{ 2377, 2380 },
	{ 2382, 2383 },
	{ 2434, 2435 },
	{ 2494, 2496 },
	{ 2503, 2504 },
	{ 2507, 2508 },
	{ 2519, 2519 },
	{ 2563, 2563 },
	{ 2622, 2624 },
	{ 2691, 2691 },
	{ 2750, 2752 },
	{ 2761, 2761 },
	{ 2763, 2764 },
	{ 2818, 2819 },
	{ 2878, 2878 },
	{ 2880, 2880 },
	{ 2887, 2888 },
	{ 2891, 2892 },
	{ 2903, 2903 },
	{ 3006, 3007 },
	{ 3009, 3010 },
	{ 3014, 3016 },
	{ 3018, 3020 },
	{ 3031, 3031 },
	{ 3073, 3075 },
	{ 3137, 3140 },
	{ 3202, 3203 },
	{ 3262, 3262 },
	{ 3264, 3268 },
	{ 3271, 3272 },
	{ 3274, 3275 },
	{ 3285, 3286 },
	{ 3330, 3331 },
	{ 3390, 3392 },
	{ 3398, 3400 },
	{ 3402, 3404 },
	{ 3415, 3415 },
	{ 3458, 3459 },
	{ 3535, 3537 },
	{ 3544, 3551 },
	{ 3570, 3571 },
	{ 3902, 3903 },
	{ 3967, 3967 },
	{ 4139, 4140 },
	{ 4145, 4145 },
	{ 4152, 4152 },
	{ 4155, 4156 },
	{ 4182, 4183 },
	{ 4194, 4196 },
	{ 4199, 4205 },
	{ 4227, 4228 },
	{ 4231, 4236 },
	{ 4239, 4239 },
	{ 4250, 4252 },
	{ 6070, 6070 },
	{ 6078, 6085 },
	{ 6087, 6088 },
	{ 6435, 6438 },
	{ 6441, 6443 },
	{ 6448, 6449 },
	{ 6451, 6456 },
	{ 6681, 6682 },
	{ 6741, 6741 },
	{ 6743, 6743 },
	{ 6753, 6753 },
	{ 6755, 6756 },
	{ 6765, 6770 },
	{ 6916, 6916 },
	{ 6965, 6965 },
	{ 6971, 6971 },
	{ 6973, 6977 },
	{ 6979, 6980 },
	{ 7042, 7042 },
	{ 7073, 7073 },
	{ 7078, 7079 },
	{ 7082, 7082 },
	{ 7143, 7143 },
	{ 7146, 7148 },
	{ 7150, 7150 },
	{ 7154, 7155 },
	{ 7204, 7211 },
	{ 7220, 7221 },
	{ 7393, 7393 },
	{ 7410, 7411 },
	{ 7415, 7415 },
	{ 12334, 12335 },
	{ 43043, 43044 },
	{ 43047, 43047 },
	{ 43136, 43137 },
	{ 43188, 43203 },
	{ 43346, 43347 },
	{ 43395, 43395 },
	{ 43444, 43445 },
	{ 43450, 43451 },
	{ 43453, 43456 },
	{ 43567, 43568 },
	{ 43571, 43572 },
	{ 43597, 43597 },
	{ 43643, 43643 },
	{ 43645, 43645 },
	{ 43755, 43755 },
	{ 43758, 43759 },
	{ 43765, 43765 },
	{ 44003, 44004 },
	{ 44006, 44007 },
	{ 44009, 44010 },
	{ 44012, 44012 },
};
static const URange32 Mc_range32[] = {
	{ 69632, 69632 },
	{ 69634, 69634 },
	{ 69762, 69762 },
	{ 69808, 69810 },
	{ 69815, 69816 },
	{ 69932, 69932 },
	{ 69957, 69958 },
	{ 70018, 70018 },
	{ 70067, 70069 },
	{ 70079, 70080 },
	{ 70188, 70190 },
	{ 70194, 70195 },
	{ 70197, 70197 },
	{ 70368, 70370 },
	{ 70402, 70403 },
	{ 70462, 70463 },
	{ 70465, 70468 },
	{ 70471, 70472 },
	{ 70475, 70477 },
	{ 70487, 70487 },
	{ 70498, 70499 },
	{ 70709, 70711 },
	{ 70720, 70721 },
	{ 70725, 70725 },
	{ 70832, 70834 },
	{ 70841, 70841 },
	{ 70843, 70846 },
	{ 70849, 70849 },
	{ 71087, 71089 },
	{ 71096, 71099 },
	{ 71102, 71102 },
	{ 71216, 71218 },
	{ 71227, 71228 },
	{ 71230, 71230 },
	{ 71340, 71340 },
	{ 71342, 71343 },
	{ 71350, 71350 },
	{ 71456, 71457 },
	{ 71462, 71462 },
	{ 71724, 71726 },
	{ 71736, 71736 },
	{ 72249, 72249 },
	{ 72279, 72280 },
	{ 72343, 72343 },
	{ 72751, 72751 },
	{ 72766, 72766 },
	{ 72873, 72873 },
	{ 72881, 72881 },
	{ 72884, 72884 },
	{ 73098, 73102 },
	{ 73107, 73108 },
	{ 73110, 73110 },
	{ 73461, 73462 },
	{ 94033, 94078 },
	{ 119141, 119142 },
	{ 119149, 119154 },
};
static const URange16 Mn_range16[] = {
	{ 768, 879 },
	{ 1155, 1159 },
	{ 1425, 1469 },
	{ 1471, 1471 },
	{ 1473, 1474 },
	{ 1476, 1477 },
	{ 1479, 1479 },
	{ 1552, 1562 },
	{ 1611, 1631 },
	{ 1648, 1648 },
	{ 1750, 1756 },
	{ 1759, 1764 },
	{ 1767, 1768 },
	{ 1770, 1773 },
	{ 1809, 1809 },
	{ 1840, 1866 },
	{ 1958, 1968 },
	{ 2027, 2035 },
	{ 2045, 2045 },
	{ 2070, 2073 },
	{ 2075, 2083 },
	{ 2085, 2087 },
	{ 2089, 2093 },
	{ 2137, 2139 },
	{ 2259, 2273 },
	{ 2275, 2306 },
	{ 2362, 2362 },
	{ 2364, 2364 },
	{ 2369, 2376 },
	{ 2381, 2381 },
	{ 2385, 2391 },
	{ 2402, 2403 },
	{ 2433, 2433 },
	{ 2492, 2492 },
	{ 2497, 2500 },
	{ 2509, 2509 },
	{ 2530, 2531 },
	{ 2558, 2558 },
	{ 2561, 2562 },
	{ 2620, 2620 },
	{ 2625, 2626 },
	{ 2631, 2632 },
	{ 2635, 2637 },
	{ 2641, 2641 },
	{ 2672, 2673 },
	{ 2677, 2677 },
	{ 2689, 2690 },
	{ 2748, 2748 },
	{ 2753, 2757 },
	{ 2759, 2760 },
	{ 2765, 2765 },
	{ 2786, 2787 },
	{ 2810, 2815 },
	{ 2817, 2817 },
	{ 2876, 2876 },
	{ 2879, 2879 },
	{ 2881, 2884 },
	{ 2893, 2893 },
	{ 2902, 2902 },
	{ 2914, 2915 },
	{ 2946, 2946 },
	{ 3008, 3008 },
	{ 3021, 3021 },
	{ 3072, 3072 },
	{ 3076, 3076 },
	{ 3134, 3136 },
	{ 3142, 3144 },
	{ 3146, 3149 },
	{ 3157, 3158 },
	{ 3170, 3171 },
	{ 3201, 3201 },
	{ 3260, 3260 },
	{ 3263, 3263 },
	{ 3270, 3270 },
	{ 3276, 3277 },
	{ 3298, 3299 },
	{ 3328, 3329 },
	{ 3387, 3388 },
	{ 3393, 3396 },
	{ 3405, 3405 },
	{ 3426, 3427 },
	{ 3530, 3530 },
	{ 3538, 3540 },
	{ 3542, 3542 },
	{ 3633, 3633 },
	{ 3636, 3642 },
	{ 3655, 3662 },
	{ 3761, 3761 },
	{ 3764, 3769 },
	{ 3771, 3772 },
	{ 3784, 3789 },
	{ 3864, 3865 },
	{ 3893, 3893 },
	{ 3895, 3895 },
	{ 3897, 3897 },
	{ 3953, 3966 },
	{ 3968, 3972 },
	{ 3974, 3975 },
	{ 3981, 3991 },
	{ 3993, 4028 },
	{ 4038, 4038 },
	{ 4141, 4144 },
	{ 4146, 4151 },
	{ 4153, 4154 },
	{ 4157, 4158 },
	{ 4184, 4185 },
	{ 4190, 4192 },
	{ 4209, 4212 },
	{ 4226, 4226 },
	{ 4229, 4230 },
	{ 4237, 4237 },
	{ 4253, 4253 },
	{ 4957, 4959 },
	{ 5906, 5908 },
	{ 5938, 5940 },
	{ 5970, 5971 },
	{ 6002, 6003 },
	{ 6068, 6069 },
	{ 6071, 6077 },
	{ 6086, 6086 },
	{ 6089, 6099 },
	{ 6109, 6109 },
	{ 6155, 6157 },
	{ 6277, 6278 },
	{ 6313, 6313 },
	{ 6432, 6434 },
	{ 6439, 6440 },
	{ 6450, 6450 },
	{ 6457, 6459 },
	{ 6679, 6680 },
	{ 6683, 6683 },
	{ 6742, 6742 },
	{ 6744, 6750 },
	{ 6752, 6752 },
	{ 6754, 6754 },
	{ 6757, 6764 },
	{ 6771, 6780 },
	{ 6783, 6783 },
	{ 6832, 6845 },
	{ 6912, 6915 },
	{ 6964, 6964 },
	{ 6966, 6970 },
	{ 6972, 6972 },
	{ 6978, 6978 },
	{ 7019, 7027 },
	{ 7040, 7041 },
	{ 7074, 7077 },
	{ 7080, 7081 },
	{ 7083, 7085 },
	{ 7142, 7142 },
	{ 7144, 7145 },
	{ 7149, 7149 },
	{ 7151, 7153 },
	{ 7212, 7219 },
	{ 7222, 7223 },
	{ 7376, 7378 },
	{ 7380, 7392 },
	{ 7394, 7400 },
	{ 7405, 7405 },
	{ 7412, 7412 },
	{ 7416, 7417 },
	{ 7616, 7673 },
	{ 7675, 7679 },
	{ 8400, 8412 },
	{ 8417, 8417 },
	{ 8421, 8432 },
	{ 11503, 11505 },
	{ 11647, 11647 },
	{ 11744, 11775 },
	{ 12330, 12333 },
	{ 12441, 12442 },
	{ 42607, 42607 },
	{ 42612, 42621 },
	{ 42654, 42655 },
	{ 42736, 42737 },
	{ 43010, 43010 },
	{ 43014, 43014 },
	{ 43019, 43019 },
	{ 43045, 43046 },
	{ 43204, 43205 },
	{ 43232, 43249 },
	{ 43263, 43263 },
	{ 43302, 43309 },
	{ 43335, 43345 },
	{ 43392, 43394 },
	{ 43443, 43443 },
	{ 43446, 43449 },
	{ 43452, 43452 },
	{ 43493, 43493 },
	{ 43561, 43566 },
	{ 43569, 43570 },
	{ 43573, 43574 },
	{ 43587, 43587 },
	{ 43596, 43596 },
	{ 43644, 43644 },
	{ 43696, 43696 },
	{ 43698, 43700 },
	{ 43703, 43704 },
	{ 43710, 43711 },
	{ 43713, 43713 },
	{ 43756, 43757 },
	{ 43766, 43766 },
	{ 44005, 44005 },
	{ 44008, 44008 },
	{ 44013, 44013 },
	{ 64286, 64286 },
	{ 65024, 65039 },
	{ 65056, 65071 },
};
static const URange32 Mn_range32[] = {
	{ 66045, 66045 },
	{ 66272, 66272 },
	{ 66422, 66426 },
	{ 68097, 68099 },
	{ 68101, 68102 },
	{ 68108, 68111 },
	{ 68152, 68154 },
	{ 68159, 68159 },
	{ 68325, 68326 },
	{ 68900, 68903 },
	{ 69446, 69456 },
	{ 69633, 69633 },
	{ 69688, 69702 },
	{ 69759, 69761 },
	{ 69811, 69814 },
	{ 69817, 69818 },
	{ 69888, 69890 },
	{ 69927, 69931 },
	{ 69933, 69940 },
	{ 70003, 70003 },
	{ 70016, 70017 },
	{ 70070, 70078 },
	{ 70089, 70092 },
	{ 70191, 70193 },
	{ 70196, 70196 },
	{ 70198, 70199 },
	{ 70206, 70206 },
	{ 70367, 70367 },
	{ 70371, 70378 },
	{ 70400, 70401 },
	{ 70459, 70460 },
	{ 70464, 70464 },
	{ 70502, 70508 },
	{ 70512, 70516 },
	{ 70712, 70719 },
	{ 70722, 70724 },
	{ 70726, 70726 },
	{ 70750, 70750 },
	{ 70835, 70840 },
	{ 70842, 70842 },
	{ 70847, 70848 },
	{ 70850, 70851 },
	{ 71090, 71093 },
	{ 71100, 71101 },
	{ 71103, 71104 },
	{ 71132, 71133 },
	{ 71219, 71226 },
	{ 71229, 71229 },
	{ 71231, 71232 },
	{ 71339, 71339 },
	{ 71341, 71341 },
	{ 71344, 71349 },
	{ 71351, 71351 },
	{ 71453, 71455 },
	{ 71458, 71461 },
	{ 71463, 71467 },
	{ 71727, 71735 },
	{ 71737, 71738 },
	{ 72193, 72202 },
	{ 72243, 72248 },
	{ 72251, 72254 },
	{ 72263, 72263 },
	{ 72273, 72278 },
	{ 72281, 72283 },
	{ 72330, 72342 },
	{ 72344, 72345 },
	{ 72752, 72758 },
	{ 72760, 72765 },
	{ 72767, 72767 },
	{ 72850, 72871 },
	{ 72874, 72880 },
	{ 72882, 72883 },
	{ 72885, 72886 },
	{ 73009, 73014 },
	{ 73018, 73018 },
	{ 73020, 73021 },
	{ 73023, 73029 },
	{ 73031, 73031 },
	{ 73104, 73105 },
	{ 73109, 73109 },
	{ 73111, 73111 },
	{ 73459, 73460 },
	{ 92912, 92916 },
	{ 92976, 92982 },
	{ 94095, 94098 },
	{ 113821, 113822 },
	{ 119143, 119145 },
	{ 119163, 119170 },
	{ 119173, 119179 },
	{ 119210, 119213 },
	{ 119362, 119364 },
	{ 121344, 121398 },
	{ 121403, 121452 },
	{ 121461, 121461 },
	{ 121476, 121476 },
	{ 121499, 121503 },
	{ 121505, 121519 },
	{ 122880, 122886 },
	{ 122888, 122904 },
	{ 122907, 122913 },
	{ 122915, 122916 },
	{ 122918, 122922 },
	{ 125136, 125142 },
	{ 125252, 125258 },
	{ 917760, 917999 },
};
static const URange16 M_range16[] = {
	{ 768, 879 },
	{ 1155, 1161 },
	{ 1425, 1469 },
	{ 1471, 1471 },
	{ 1473, 1474 },
	{ 1476, 1477 },
	{ 1479, 1479 },
	{ 1552, 1562 },
	{ 1611, 1631 },
	{ 1648, 1648 },
	{ 1750, 1756 },
	{ 1759, 1764 },
	{ 1767, 1768 },
	{ 1770, 1773 },
	{ 1809, 1809 },
	{ 1840, 1866 },
	{ 1958, 1968 },
	{ 2027, 2035 },
	{ 2045, 2045 },
	{ 2070, 2073 },
	{ 2075, 2083 },
	{ 2085, 2087 },
	{ 2089, 2093 },
	{ 2137, 2139 },
	{ 2259, 2273 },
	{ 2275, 2307 },
	{ 2362, 2364 },
	{ 2366, 2383 },
	{ 2385, 2391 },
	{ 2402, 2403 },
	{ 2433, 2435 },
	{ 2492, 2492 },
	{ 2494, 2500 },
	{ 2503, 2504 },
	{ 2507, 2509 },
	{ 2519, 2519 },
	{ 2530, 2531 },
	{ 2558, 2558 },
	{ 2561, 2563 },
	{ 2620, 2620 },
	{ 2622, 2626 },
	{ 2631, 2632 },
	{ 2635, 2637 },
	{ 2641, 2641 },
	{ 2672, 2673 },
	{ 2677, 2677 },
	{ 2689, 2691 },
	{ 2748, 2748 },
	{ 2750, 2757 },
	{ 2759, 2761 },
	{ 2763, 2765 },
	{ 2786, 2787 },
	{ 2810, 2815 },
	{ 2817, 2819 },
	{ 2876, 2876 },
	{ 2878, 2884 },
	{ 2887, 2888 },
	{ 2891, 2893 },
	{ 2902, 2903 },
	{ 2914, 2915 },
	{ 2946, 2946 },
	{ 3006, 3010 },
	{ 3014, 3016 },
	{ 3018, 3021 },
	{ 3031, 3031 },
	{ 3072, 3076 },
	{ 3134, 3140 },
	{ 3142, 3144 },
	{ 3146, 3149 },
	{ 3157, 3158 },
	{ 3170, 3171 },
	{ 3201, 3203 },
	{ 3260, 3260 },
	{ 3262, 3268 },
	{ 3270, 3272 },
	{ 3274, 3277 },
	{ 3285, 3286 },
	{ 3298, 3299 },
	{ 3328, 3331 },
	{ 3387, 3388 },
	{ 3390, 3396 },
	{ 3398, 3400 },
	{ 3402, 3405 },
	{ 3415, 3415 },
	{ 3426, 3427 },
	{ 3458, 3459 },
	{ 3530, 3530 },
	{ 3535, 3540 },
	{ 3542, 3542 },
	{ 3544, 3551 },
	{ 3570, 3571 },
	{ 3633, 3633 },
	{ 3636, 3642 },
	{ 3655, 3662 },
	{ 3761, 3761 },
	{ 3764, 3769 },
	{ 3771, 3772 },
	{ 3784, 3789 },
	{ 3864, 3865 },
	{ 3893, 3893 },
	{ 3895, 3895 },
	{ 3897, 3897 },
	{ 3902, 3903 },
	{ 3953, 3972 },
	{ 3974, 3975 },
	{ 3981, 3991 },
	{ 3993, 4028 },
	{ 4038, 4038 },
	{ 4139, 4158 },
	{ 4182, 4185 },
	{ 4190, 4192 },
	{ 4194, 4196 },
	{ 4199, 4205 },
	{ 4209, 4212 },
	{ 4226, 4237 },
	{ 4239, 4239 },
	{ 4250, 4253 },
	{ 4957, 4959 },
	{ 5906, 5908 },
	{ 5938, 5940 },
	{ 5970, 5971 },
	{ 6002, 6003 },
	{ 6068, 6099 },
	{ 6109, 6109 },
	{ 6155, 6157 },
	{ 6277, 6278 },
	{ 6313, 6313 },
	{ 6432, 6443 },
	{ 6448, 6459 },
	{ 6679, 6683 },
	{ 6741, 6750 },
	{ 6752, 6780 },
	{ 6783, 6783 },
	{ 6832, 6846 },
	{ 6912, 6916 },
	{ 6964, 6980 },
	{ 7019, 7027 },
	{ 7040, 7042 },
	{ 7073, 7085 },
	{ 7142, 7155 },
	{ 7204, 7223 },
	{ 7376, 7378 },
	{ 7380, 7400 },
	{ 7405, 7405 },
	{ 7410, 7412 },
	{ 7415, 7417 },
	{ 7616, 7673 },
	{ 7675, 7679 },
	{ 8400, 8432 },
	{ 11503, 11505 },
	{ 11647, 11647 },
	{ 11744, 11775 },
	{ 12330, 12335 },
	{ 12441, 12442 },
	{ 42607, 42610 },
	{ 42612, 42621 },
	{ 42654, 42655 },
	{ 42736, 42737 },
	{ 43010, 43010 },
	{ 43014, 43014 },
	{ 43019, 43019 },
	{ 43043, 43047 },
	{ 43136, 43137 },
	{ 43188, 43205 },
	{ 43232, 43249 },
	{ 43263, 43263 },
	{ 43302, 43309 },
	{ 43335, 43347 },
	{ 43392, 43395 },
	{ 43443, 43456 },
	{ 43493, 43493 },
	{ 43561, 43574 },
	{ 43587, 43587 },
	{ 43596, 43597 },
	{ 43643, 43645 },
	{ 43696, 43696 },
	{ 43698, 43700 },
	{ 43703, 43704 },
	{ 43710, 43711 },
	{ 43713, 43713 },
	{ 43755, 43759 },
	{ 43765, 43766 },
	{ 44003, 44010 },
	{ 44012, 44013 },
	{ 64286, 64286 },
	{ 65024, 65039 },
	{ 65056, 65071 },
};
static const URange32 M_range32[] = {
	{ 66045, 66045 },
	{ 66272, 66272 },
	{ 66422, 66426 },
	{ 68097, 68099 },
	{ 68101, 68102 },
	{ 68108, 68111 },
	{ 68152, 68154 },
	{ 68159, 68159 },
	{ 68325, 68326 },
	{ 68900, 68903 },
	{ 69446, 69456 },
	{ 69632, 69634 },
	{ 69688, 69702 },
	{ 69759, 69762 },
	{ 69808, 69818 },
	{ 69888, 69890 },
	{ 69927, 69940 },
	{ 69957, 69958 },
	{ 70003, 70003 },
	{ 70016, 70018 },
	{ 70067, 70080 },
	{ 70089, 70092 },
	{ 70188, 70199 },
	{ 70206, 70206 },
	{ 70367, 70378 },
	{ 70400, 70403 },
	{ 70459, 70460 },
	{ 70462, 70468 },
	{ 70471, 70472 },
	{ 70475, 70477 },
	{ 70487, 70487 },
	{ 70498, 70499 },
	{ 70502, 70508 },
	{ 70512, 70516 },
	{ 70709, 70726 },
	{ 70750, 70750 },
	{ 70832, 70851 },
	{ 71087, 71093 },
	{ 71096, 71104 },
	{ 71132, 71133 },
	{ 71216, 71232 },
	{ 71339, 71351 },
	{ 71453, 71467 },
	{ 71724, 71738 },
	{ 72193, 72202 },
	{ 72243, 72249 },
	{ 72251, 72254 },
	{ 72263, 72263 },
	{ 72273, 72283 },
	{ 72330, 72345 },
	{ 72751, 72758 },
	{ 72760, 72767 },
	{ 72850, 72871 },
	{ 72873, 72886 },
	{ 73009, 73014 },
	{ 73018, 73018 },
	{ 73020, 73021 },
	{ 73023, 73029 },
	{ 73031, 73031 },
	{ 73098, 73102 },
	{ 73104, 73105 },
	{ 73107, 73111 },
	{ 73459, 73462 },
	{ 92912, 92916 },
	{ 92976, 92982 },
	{ 94033, 94078 },
	{ 94095, 94098 },
	{ 113821, 113822 },
	{ 119141, 119145 },
	{ 119149, 119154 },
	{ 119163, 119170 },
	{ 119173, 119179 },
	{ 119210, 119213 },
	{ 119362, 119364 },
	{ 121344, 121398 },
	{ 121403, 121452 },
	{ 121461, 121461 },
	{ 121476, 121476 },
	{ 121499, 121503 },
	{ 121505, 121519 },
	{ 122880, 122886 },
	{ 122888, 122904 },
	{ 122907, 122913 },
	{ 122915, 122916 },
	{ 122918, 122922 },
	{ 125136, 125142 },
	{ 125252, 125258 },
	{ 917760, 917999 },
};
static const URange16 L_range16[] = {
	{ 65, 90 },
	{ 97, 122 },
	{ 170, 170 },
	{ 181, 181 },
	{ 186, 186 },
	{ 192, 214 },
	{ 216, 246 },
	{ 248, 705 },
	{ 710, 721 },
	{ 736, 740 },
	{ 748, 748 },
	{ 750, 750 },
	{ 880, 884 },
	{ 886, 887 },
	{ 890, 893 },
	{ 895, 895 },
	{ 902, 902 },
	{ 904, 906 },
	{ 908, 908 },
	{ 910, 929 },
	{ 931, 1013 },
	{ 1015, 1153 },
	{ 1162, 1327 },
	{ 1329, 1366 },
	{ 1369, 1369 },
	{ 1376, 1416 },
	{ 1488, 1514 },
	{ 1519, 1522 },
	{ 1568, 1610 },
	{ 1646, 1647 },
	{ 1649, 1747 },
	{ 1749, 1749 },
	{ 1765, 1766 },
	{ 1774, 1775 },
	{ 1786, 1788 },
	{ 1791, 1791 },
	{ 1808, 1808 },
	{ 1810, 1839 },
	{ 1869, 1957 },
	{ 1969, 1969 },
	{ 1994, 2026 },
	{ 2036, 2037 },
	{ 2042, 2042 },
	{ 2048, 2069 },
	{ 2074, 2074 },
	{ 2084, 2084 },
	{ 2088, 2088 },
	{ 2112, 2136 },
	{ 2144, 2154 },
	{ 2208, 2228 },
	{ 2230, 2237 },
	{ 2308, 2361 },
	{ 2365, 2365 },
	{ 2384, 2384 },
	{ 2392, 2401 },
	{ 2417, 2432 },
	{ 2437, 2444 },
	{ 2447, 2448 },
	{ 2451, 2472 },
	{ 2474, 2480 },
	{ 2482, 2482 },
	{ 2486, 2489 },
	{ 2493, 2493 },
	{ 2510, 2510 },
	{ 2524, 2525 },
	{ 2527, 2529 },
	{ 2544, 2545 },
	{ 2556, 2556 },
	{ 2565, 2570 },
	{ 2575, 2576 },
	{ 2579, 2600 },
	{ 2602, 2608 },
	{ 2610, 2611 },
	{ 2613, 2614 },
	{ 2616, 2617 },
	{ 2649, 2652 },
	{ 2654, 2654 },
	{ 2674, 2676 },
	{ 2693, 2701 },
	{ 2703, 2705 },
	{ 2707, 2728 },
	{ 2730, 2736 },
	{ 2738, 2739 },
	{ 2741, 2745 },
	{ 2749, 2749 },
	{ 2768, 2768 },
	{ 2784, 2785 },
	{ 2809, 2809 },
	{ 2821, 2828 },
	{ 2831, 2832 },
	{ 2835, 2856 },
	{ 2858, 2864 },
	{ 2866, 2867 },
	{ 2869, 2873 },
	{ 2877, 2877 },
	{ 2908, 2909 },
	{ 2911, 2913 },
	{ 2929, 2929 },
	{ 2947, 2947 },
	{ 2949, 2954 },
	{ 2958, 2960 },
	{ 2962, 2965 },
	{ 2969, 2970 },
	{ 2972, 2972 },
	{ 2974, 2975 },
	{ 2979, 2980 },
	{ 2984, 2986 },
	{ 2990, 3001 },
	{ 3024, 3024 },
	{ 3077, 3084 },
	{ 3086, 3088 },
	{ 3090, 3112 },
	{ 3114, 3129 },
	{ 3133, 3133 },
	{ 3160, 3162 },
	{ 3168, 3169 },
	{ 3200, 3200 },
	{ 3205, 3212 },
	{ 3214, 3216 },
	{ 3218, 3240 },
	{ 3242, 3251 },
	{ 3253, 3257 },
	{ 3261, 3261 },
	{ 3294, 3294 },
	{ 3296, 3297 },
	{ 3313, 3314 },
	{ 3333, 3340 },
	{ 3342, 3344 },
	{ 3346, 3386 },
	{ 3389, 3389 },
	{ 3406, 3406 },
	{ 3412, 3414 },
	{ 3423, 3425 },
	{ 3450, 3455 },
	{ 3461, 3478 },
	{ 3482, 3505 },
	{ 3507, 3515 },
	{ 3517, 3517 },
	{ 3520, 3526 },
	{ 3585, 3632 },
	{ 3634, 3635 },
	{ 3648, 3654 },
	{ 3713, 3714 },
	{ 3716, 3716 },
	{ 3719, 3720 },
	{ 3722, 3722 },
	{ 3725, 3725 },
	{ 3732, 3735 },
	{ 3737, 3743 },
	{ 3745, 3747 },
	{ 3749, 3749 },
	{ 3751, 3751 },
	{ 3754, 3755 },
	{ 3757, 3760 },
	{ 3762, 3763 },
	{ 3773, 3773 },
	{ 3776, 3780 },
	{ 3782, 3782 },
	{ 3804, 3807 },
	{ 3840, 3840 },
	{ 3904, 3911 },
	{ 3913, 3948 },
	{ 3976, 3980 },
	{ 4096, 4138 },
	{ 4159, 4159 },
	{ 4176, 4181 },
	{ 4186, 4189 },
	{ 4193, 4193 },
	{ 4197, 4198 },
	{ 4206, 4208 },
	{ 4213, 4225 },
	{ 4238, 4238 },
	{ 4256, 4293 },
	{ 4295, 4295 },
	{ 4301, 4301 },
	{ 4304, 4346 },
	{ 4348, 4680 },
	{ 4682, 4685 },
	{ 4688, 4694 },
	{ 4696, 4696 },
	{ 4698, 4701 },
	{ 4704, 4744 },
	{ 4746, 4749 },
	{ 4752, 4784 },
	{ 4786, 4789 },
	{ 4792, 4798 },
	{ 4800, 4800 },
	{ 4802, 4805 },
	{ 4808, 4822 },
	{ 4824, 4880 },
	{ 4882, 4885 },
	{ 4888, 4954 },
	{ 4992, 5007 },
	{ 5024, 5109 },
	{ 5112, 5117 },
	{ 5121, 5740 },
	{ 5743, 5759 },
	{ 5761, 5786 },
	{ 5792, 5866 },
	{ 5873, 5880 },
	{ 5888, 5900 },
	{ 5902, 5905 },
	{ 5920, 5937 },
	{ 5952, 5969 },
	{ 5984, 5996 },
	{ 5998, 6000 },
	{ 6016, 6067 },
	{ 6103, 6103 },
	{ 6108, 6108 },
	{ 6176, 6264 },
	{ 6272, 6276 },
	{ 6279, 6312 },
	{ 6314, 6314 },
	{ 6320, 6389 },
	{ 6400, 6430 },
	{ 6480, 6509 },
	{ 6512, 6516 },
	{ 6528, 6571 },
	{ 6576, 6601 },
	{ 6656, 6678 },
	{ 6688, 6740 },
	{ 6823, 6823 },
	{ 6917, 6963 },
	{ 6981, 6987 },
	{ 7043, 7072 },
	{ 7086, 7087 },
	{ 7098, 7141 },
	{ 7168, 7203 },
	{ 7245, 7247 },
	{ 7258, 7293 },
	{ 7296, 7304 },
	{ 7312, 7354 },
	{ 7357, 7359 },
	{ 7401, 7404 },
	{ 7406, 7409 },
	{ 7413, 7414 },
	{ 7424, 7615 },
	{ 7680, 7957 },
	{ 7960, 7965 },
	{ 7968, 8005 },
	{ 8008, 8013 },
	{ 8016, 8023 },
	{ 8025, 8025 },
	{ 8027, 8027 },
	{ 8029, 8029 },
	{ 8031, 8061 },
	{ 8064, 8116 },
	{ 8118, 8124 },
	{ 8126, 8126 },
	{ 8130, 8132 },
	{ 8134, 8140 },
	{ 8144, 8147 },
	{ 8150, 8155 },
	{ 8160, 8172 },
	{ 8178, 8180 },
	{ 8182, 8188 },
	{ 8305, 8305 },
	{ 8319, 8319 },
	{ 8336, 8348 },
	{ 8450, 8450 },
	{ 8455, 8455 },
	{ 8458, 8467 },
	{ 8469, 8469 },
	{ 8473, 8477 },
	{ 8484, 8484 },
	{ 8486, 8486 },
	{ 8488, 8488 },
	{ 8490, 8493 },
	{ 8495, 8505 },
	{ 8508, 8511 },
	{ 8517, 8521 },
	{ 8526, 8526 },
	{ 8579, 8580 },
	{ 11264, 11310 },
	{ 11312, 11358 },
	{ 11360, 11492 },
	{ 11499, 11502 },
	{ 11506, 11507 },
	{ 11520, 11557 },
	{ 11559, 11559 },
	{ 11565, 11565 },
	{ 11568, 11623 },
	{ 11631, 11631 },
	{ 11648, 11670 },
	{ 11680, 11686 },
	{ 11688, 11694 },
	{ 11696, 11702 },
	{ 11704, 11710 },
	{ 11712, 11718 },
	{ 11720, 11726 },
	{ 11728, 11734 },
	{ 11736, 11742 },
	{ 11823, 11823 },
	{ 12293, 12294 },
	{ 12337, 12341 },
	{ 12347, 12348 },
	{ 12353, 12438 },
	{ 12445, 12447 },
	{ 12449, 12538 },
	{ 12540, 12543 },
	{ 12549, 12591 },
	{ 12593, 12686 },
	{ 12704, 12730 },
	{ 12784, 12799 },
	{ 13312, 19893 },
	{ 19968, 40943 },
	{ 40960, 42124 },
	{ 42192, 42237 },
	{ 42240, 42508 },
	{ 42512, 42527 },
	{ 42538, 42539 },
	{ 42560, 42606 },
	{ 42623, 42653 },
	{ 42656, 42725 },
	{ 42775, 42783 },
	{ 42786, 42888 },
	{ 42891, 42937 },
	{ 42999, 43009 },
	{ 43011, 43013 },
	{ 43015, 43018 },
	{ 43020, 43042 },
	{ 43072, 43123 },
	{ 43138, 43187 },
	{ 43250, 43255 },
	{ 43259, 43259 },
	{ 43261, 43262 },
	{ 43274, 43301 },
	{ 43312, 43334 },
	{ 43360, 43388 },
	{ 43396, 43442 },
	{ 43471, 43471 },
	{ 43488, 43492 },
	{ 43494, 43503 },
	{ 43514, 43518 },
	{ 43520, 43560 },
	{ 43584, 43586 },
	{ 43588, 43595 },
	{ 43616, 43638 },
	{ 43642, 43642 },
	{ 43646, 43695 },
	{ 43697, 43697 },
	{ 43701, 43702 },
	{ 43705, 43709 },
	{ 43712, 43712 },
	{ 43714, 43714 },
	{ 43739, 43741 },
	{ 43744, 43754 },
	{ 43762, 43764 },
	{ 43777, 43782 },
	{ 43785, 43790 },
	{ 43793, 43798 },
	{ 43808, 43814 },
	{ 43816, 43822 },
	{ 43824, 43866 },
	{ 43868, 43877 },
	{ 43888, 44002 },
	{ 44032, 55203 },
	{ 55216, 55238 },
	{ 55243, 55291 },
	{ 63744, 64109 },
	{ 64112, 64217 },
	{ 64256, 64262 },
	{ 64275, 64279 },
	{ 64285, 64285 },
	{ 64287, 64296 },
	{ 64298, 64310 },
	{ 64312, 64316 },
	{ 64318, 64318 },
	{ 64320, 64321 },
	{ 64323, 64324 },
	{ 64326, 64433 },
	{ 64467, 64829 },
	{ 64848, 64911 },
	{ 64914, 64967 },
	{ 65008, 65019 },
	{ 65136, 65140 },
	{ 65142, 65276 },
	{ 65313, 65338 },
	{ 65345, 65370 },
	{ 65382, 65470 },
	{ 65474, 65479 },
	{ 65482, 65487 },
	{ 65490, 65495 },
	{ 65498, 65500 },
};
static const URange32 L_range32[] = {
	{ 65536, 65547 },
	{ 65549, 65574 },
	{ 65576, 65594 },
	{ 65596, 65597 },
	{ 65599, 65613 },
	{ 65616, 65629 },
	{ 65664, 65786 },
	{ 66176, 66204 },
	{ 66208, 66256 },
	{ 66304, 66335 },
	{ 66349, 66368 },
	{ 66370, 66377 },
	{ 66384, 66421 },
	{ 66432, 66461 },
	{ 66464, 66499 },
	{ 66504, 66511 },
	{ 66560, 66717 },
	{ 66736, 66771 },
	{ 66776, 66811 },
	{ 66816, 66855 },
	{ 66864, 66915 },
	{ 67072, 67382 },
	{ 67392, 67413 },
	{ 67424, 67431 },
	{ 67584, 67589 },
	{ 67592, 67592 },
	{ 67594, 67637 },
	{ 67639, 67640 },
	{ 67644, 67644 },
	{ 67647, 67669 },
	{ 67680, 67702 },
	{ 67712, 67742 },
	{ 67808, 67826 },
	{ 67828, 67829 },
	{ 67840, 67861 },
	{ 67872, 67897 },
	{ 67968, 68023 },
	{ 68030, 68031 },
	{ 68096, 68096 },
	{ 68112, 68115 },
	{ 68117, 68119 },
	{ 68121, 68149 },
	{ 68192, 68220 },
	{ 68224, 68252 },
	{ 68288, 68295 },
	{ 68297, 68324 },
	{ 68352, 68405 },
	{ 68416, 68437 },
	{ 68448, 68466 },
	{ 68480, 68497 },
	{ 68608, 68680 },
	{ 68736, 68786 },
	{ 68800, 68850 },
	{ 68864, 68899 },
	{ 69376, 69404 },
	{ 69415, 69415 },
	{ 69424, 69445 },
	{ 69635, 69687 },
	{ 69763, 69807 },
	{ 69840, 69864 },
	{ 69891, 69926 },
	{ 69956, 69956 },
	{ 69968, 70002 },
	{ 70006, 70006 },
	{ 70019, 70066 },
	{ 70081, 70084 },
	{ 70106, 70106 },
	{ 70108, 70108 },
	{ 70144, 70161 },
	{ 70163, 70187 },
	{ 70272, 70278 },
	{ 70280, 70280 },
	{ 70282, 70285 },
	{ 70287, 70301 },
	{ 70303, 70312 },
	{ 70320, 70366 },
	{ 70405, 70412 },
	{ 70415, 70416 },
	{ 70419, 70440 },
	{ 70442, 70448 },
	{ 70450, 70451 },
	{ 70453, 70457 },
	{ 70461, 70461 },
	{ 70480, 70480 },
	{ 70493, 70497 },
	{ 70656, 70708 },
	{ 70727, 70730 },
	{ 70784, 70831 },
	{ 70852, 70853 },
	{ 70855, 70855 },
	{ 71040, 71086 },
	{ 71128, 71131 },
	{ 71168, 71215 },
	{ 71236, 71236 },
	{ 71296, 71338 },
	{ 71424, 71450 },
	{ 71680, 71723 },
	{ 71840, 71903 },
	{ 71935, 71935 },
	{ 72192, 72192 },
	{ 72203, 72242 },
	{ 72250, 72250 },
	{ 72272, 72272 },
	{ 72284, 72323 },
	{ 72326, 72329 },
	{ 72349, 72349 },
	{ 72384, 72440 },
	{ 72704, 72712 },
	{ 72714, 72750 },
	{ 72768, 72768 },
	{ 72818, 72847 },
	{ 72960, 72966 },
	{ 72968, 72969 },
	{ 72971, 73008 },
	{ 73030, 73030 },
	{ 73056, 73061 },
	{ 73063, 73064 },
	{ 73066, 73097 },
	{ 73112, 73112 },
	{ 73440, 73458 },
	{ 73728, 74649 },
	{ 74880, 75075 },
	{ 77824, 78894 },
	{ 82944, 83526 },
	{ 92160, 92728 },
	{ 92736, 92766 },
	{ 92880, 92909 },
	{ 92928, 92975 },
	{ 92992, 92995 },
	{ 93027, 93047 },
	{ 93053, 93071 },
	{ 93760, 93823 },
	{ 93952, 94020 },
	{ 94032, 94032 },
	{ 94099, 94111 },
	{ 94176, 94177 },
	{ 94208, 100337 },
	{ 100352, 101106 },
	{ 110592, 110878 },
	{ 110960, 111355 },
	{ 113664, 113770 },
	{ 113776, 113788 },
	{ 113792, 113800 },
	{ 113808, 113817 },
	{ 119808, 119892 },
	{ 119894, 119964 },
	{ 119966, 119967 },
	{ 119970, 119970 },
	{ 119973, 119974 },
	{ 119977, 119980 },
	{ 119982, 119993 },
	{ 119995, 119995 },
	{ 119997, 120003 },
	{ 120005, 120069 },
	{ 120071, 120074 },
	{ 120077, 120084 },
	{ 120086, 120092 },
	{ 120094, 120121 },
	{ 120123, 120126 },
	{ 120128, 120132 },
	{ 120134, 120134 },
	{ 120138, 120144 },
	{ 120146, 120485 },
	{ 120488, 120512 },
	{ 120514, 120538 },
	{ 120540, 120570 },
	{ 120572, 120596 },
	{ 120598, 120628 },
	{ 120630, 120654 },
	{ 120656, 120686 },
	{ 120688, 120712 },
	{ 120714, 120744 },
	{ 120746, 120770 },
	{ 120772, 120779 },
	{ 124928, 125124 },
	{ 125184, 125251 },
	{ 126464, 126467 },
	{ 126469, 126495 },
	{ 126497, 126498 },
	{ 126500, 126500 },
	{ 126503, 126503 },
	{ 126505, 126514 },
	{ 126516, 126519 },
	{ 126521, 126521 },
	{ 126523, 126523 },
	{ 126530, 126530 },
	{ 126535, 126535 },
	{ 126537, 126537 },
	{ 126539, 126539 },
	{ 126541, 126543 },
	{ 126545, 126546 },
	{ 126548, 126548 },
	{ 126551, 126551 },
	{ 126553, 126553 },
	{ 126555, 126555 },
	{ 126557, 126557 },
	{ 126559, 126559 },
	{ 126561, 126562 },
	{ 126564, 126564 },
	{ 126567, 126570 },
	{ 126572, 126578 },
	{ 126580, 126583 },
	{ 126585, 126588 },
	{ 126590, 126590 },
	{ 126592, 126601 },
	{ 126603, 126619 },
	{ 126625, 126627 },
	{ 126629, 126633 },
	{ 126635, 126651 },
	{ 131072, 173782 },
	{ 173824, 177972 },
	{ 177984, 178205 },
	{ 178208, 183969 },
	{ 183984, 191456 },
	{ 194560, 195101 },
};
static const URange16 N_range16[] = {
	{ 48, 57 },
	{ 178, 179 },
	{ 185, 185 },
	{ 188, 190 },
	{ 1632, 1641 },
	{ 1776, 1785 },
	{ 1984, 1993 },
	{ 2406, 2415 },
	{ 2534, 2543 },
	{ 2548, 2553 },
	{ 2662, 2671 },
	{ 2790, 2799 },
	{ 2918, 2927 },
	{ 2930, 2935 },
	{ 3046, 3058 },
	{ 3174, 3183 },
	{ 3192, 3198 },
	{ 3302, 3311 },
	{ 3416, 3422 },
	{ 3430, 3448 },
	{ 3558, 3567 },
	{ 3664, 3673 },
	{ 3792, 3801 },
	{ 3872, 3891 },
	{ 4160, 4169 },
	{ 4240, 4249 },
	{ 4969, 4988 },
	{ 5870, 5872 },
	{ 6112, 6121 },
	{ 6128, 6137 },
	{ 6160, 6169 },
	{ 6470, 6479 },
	{ 6608, 6618 },
	{ 6784, 6793 },
	{ 6800, 6809 },
	{ 6992, 7001 },
	{ 7088, 7097 },
	{ 7232, 7241 },
	{ 7248, 7257 },
	{ 8304, 8304 },
	{ 8308, 8313 },
	{ 8320, 8329 },
	{ 8528, 8578 },
	{ 8581, 8585 },
	{ 9312, 9371 },
	{ 9450, 9471 },
	{ 10102, 10131 },
	{ 11517, 11517 },
	{ 12295, 12295 },
	{ 12321, 12329 },
	{ 12344, 12346 },
	{ 12690, 12693 },
	{ 12832, 12841 },
	{ 12872, 12879 },
	{ 12881, 12895 },
	{ 12928, 12937 },
	{ 12977, 12991 },
	{ 42528, 42537 },
	{ 42726, 42735 },
	{ 43056, 43061 },
	{ 43216, 43225 },
	{ 43264, 43273 },
	{ 43472, 43481 },
	{ 43504, 43513 },
	{ 43600, 43609 },
	{ 44016, 44025 },
	{ 65296, 65305 },
};
static const URange32 N_range32[] = {
	{ 65799, 65843 },
	{ 65856, 65912 },
	{ 65930, 65931 },
	{ 66273, 66299 },
	{ 66336, 66339 },
	{ 66369, 66369 },
	{ 66378, 66378 },
	{ 66513, 66517 },
	{ 66720, 66729 },
	{ 67672, 67679 },
	{ 67705, 67711 },
	{ 67751, 67759 },
	{ 67835, 67839 },
	{ 67862, 67867 },
	{ 68028, 68029 },
	{ 68032, 68047 },
	{ 68050, 68095 },
	{ 68160, 68168 },
	{ 68221, 68222 },
	{ 68253, 68255 },
	{ 68331, 68335 },
	{ 68440, 68447 },
	{ 68472, 68479 },
	{ 68521, 68527 },
	{ 68858, 68863 },
	{ 68912, 68921 },
	{ 69216, 69246 },
	{ 69405, 69414 },
	{ 69457, 69460 },
	{ 69714, 69743 },
	{ 69872, 69881 },
	{ 69942, 69951 },
	{ 70096, 70105 },
	{ 70113, 70132 },
	{ 70384, 70393 },
	{ 70736, 70745 },
	{ 70864, 70873 },
	{ 71248, 71257 },
	{ 71360, 71369 },
	{ 71472, 71483 },
	{ 71904, 71922 },
	{ 72784, 72812 },
	{ 73040, 73049 },
	{ 73120, 73129 },
	{ 74752, 74862 },
	{ 92768, 92777 },
	{ 93008, 93017 },
	{ 93019, 93025 },
	{ 93824, 93846 },
	{ 119520, 119539 },
	{ 119648, 119672 },
	{ 120782, 120831 },
	{ 125127, 125135 },
	{ 125264, 125273 },
	{ 126065, 126123 },
	{ 126125, 126127 },
	{ 126129, 126132 },
	{ 127232, 127244 },
};
static const URange16 Sk_range16[] = {
	{ 94, 94 },
	{ 96, 96 },
	{ 168, 168 },
	{ 175, 175 },
	{ 180, 180 },
	{ 184, 184 },
	{ 706, 709 },
	{ 722, 735 },
	{ 741, 747 },
	{ 749, 749 },
	{ 751, 767 },
	{ 885, 885 },
	{ 900, 901 },
	{ 8125, 8125 },
	{ 8127, 8129 },
	{ 8141, 8143 },
	{ 8157, 8159 },
	{ 8173, 8175 },
	{ 8189, 8190 },
	{ 12443, 12444 },
	{ 42752, 42774 },
	{ 42784, 42785 },
	{ 42889, 42890 },
	{ 43867, 43867 },
	{ 64434, 64449 },
	{ 65342, 65342 },
	{ 65344, 65344 },
	{ 65507, 65507 },
};
static const URange32 Sk_range32[] = {
	{ 127995, 127999 },
};
static const URange16 P_range16[] = {
	{ 33, 35 },
	{ 37, 42 },
	{ 44, 47 },
	{ 58, 59 },
	{ 63, 64 },
	{ 91, 93 },
	{ 95, 95 },
	{ 123, 123 },
	{ 125, 125 },
	{ 161, 161 },
	{ 167, 167 },
	{ 171, 171 },
	{ 182, 183 },
	{ 187, 187 },
	{ 191, 191 },
	{ 894, 894 },
	{ 903, 903 },
	{ 1370, 1375 },
	{ 1417, 1418 },
	{ 1470, 1470 },
	{ 1472, 1472 },
	{ 1475, 1475 },
	{ 1478, 1478 },
	{ 1523, 1524 },
	{ 1545, 1546 },
	{ 1548, 1549 },
	{ 1563, 1563 },
	{ 1566, 1567 },
	{ 1642, 1645 },
	{ 1748, 1748 },
	{ 1792, 1805 },
	{ 2039, 2041 },
	{ 2096, 2110 },
	{ 2142, 2142 },
	{ 2404, 2405 },
	{ 2416, 2416 },
	{ 2557, 2557 },
	{ 2678, 2678 },
	{ 2800, 2800 },
	{ 3204, 3204 },
	{ 3572, 3572 },
	{ 3663, 3663 },
	{ 3674, 3675 },
	{ 3844, 3858 },
	{ 3860, 3860 },
	{ 3898, 3901 },
	{ 3973, 3973 },
	{ 4048, 4052 },
	{ 4057, 4058 },
	{ 4170, 4175 },
	{ 4347, 4347 },
	{ 4960, 4968 },
	{ 5120, 5120 },
	{ 5741, 5742 },
	{ 5787, 5788 },
	{ 5867, 5869 },
	{ 5941, 5942 },
	{ 6100, 6102 },
	{ 6104, 6106 },
	{ 6144, 6154 },
	{ 6468, 6469 },
	{ 6686, 6687 },
	{ 6816, 6822 },
	{ 6824, 6829 },
	{ 7002, 7008 },
	{ 7164, 7167 },
	{ 7227, 7231 },
	{ 7294, 7295 },
	{ 7360, 7367 },
	{ 7379, 7379 },
	{ 8208, 8231 },
	{ 8240, 8259 },
	{ 8261, 8273 },
	{ 8275, 8286 },
	{ 8317, 8318 },
	{ 8333, 8334 },
	{ 8968, 8971 },
	{ 9001, 9002 },
	{ 10088, 10101 },
	{ 10181, 10182 },
	{ 10214, 10223 },
	{ 10627, 10648 },
	{ 10712, 10715 },
	{ 10748, 10749 },
	{ 11513, 11516 },
	{ 11518, 11519 },
	{ 11632, 11632 },
	{ 11776, 11822 },
	{ 11824, 11854 },
	{ 12289, 12291 },
	{ 12296, 12305 },
	{ 12308, 12319 },
	{ 12336, 12336 },
	{ 12349, 12349 },
	{ 12448, 12448 },
	{ 12539, 12539 },
	{ 42238, 42239 },
	{ 42509, 42511 },
	{ 42611, 42611 },
	{ 42622, 42622 },
	{ 42738, 42743 },
	{ 43124, 43127 },
	{ 43214, 43215 },
	{ 43256, 43258 },
	{ 43260, 43260 },
	{ 43310, 43311 },
	{ 43359, 43359 },
	{ 43457, 43469 },
	{ 43486, 43487 },
	{ 43612, 43615 },
	{ 43742, 43743 },
	{ 43760, 43761 },
	{ 44011, 44011 },
	{ 64830, 64831 },
	{ 65040, 65049 },
	{ 65072, 65106 },
	{ 65108, 65121 },
	{ 65123, 65123 },
	{ 65128, 65128 },
	{ 65130, 65131 },
	{ 65281, 65283 },
	{ 65285, 65290 },
	{ 65292, 65295 },
	{ 65306, 65307 },
	{ 65311, 65312 },
	{ 65339, 65341 },
	{ 65343, 65343 },
	{ 65371, 65371 },
	{ 65373, 65373 },
	{ 65375, 65381 },
};
static const URange32 P_range32[] = {
	{ 65792, 65794 },
	{ 66463, 66463 },
	{ 66512, 66512 },
	{ 66927, 66927 },
	{ 67671, 67671 },
	{ 67871, 67871 },
	{ 67903, 67903 },
	{ 68176, 68184 },
	{ 68223, 68223 },
	{ 68336, 68342 },
	{ 68409, 68415 },
	{ 68505, 68508 },
	{ 69461, 69465 },
	{ 69703, 69709 },
	{ 69819, 69820 },
	{ 69822, 69825 },
	{ 69952, 69955 },
	{ 70004, 70005 },
	{ 70085, 70088 },
	{ 70093, 70093 },
	{ 70107, 70107 },
	{ 70109, 70111 },
	{ 70200, 70205 },
	{ 70313, 70313 },
	{ 70731, 70735 },
	{ 70747, 70747 },
	{ 70749, 70749 },
	{ 70854, 70854 },
	{ 71105, 71127 },
	{ 71233, 71235 },
	{ 71264, 71276 },
	{ 71484, 71486 },
	{ 71739, 71739 },
	{ 72255, 72262 },
	{ 72346, 72348 },
	{ 72350, 72354 },
	{ 72769, 72773 },
	{ 72816, 72817 },
	{ 73463, 73464 },
	{ 74864, 74868 },
	{ 92782, 92783 },
	{ 92917, 92917 },
	{ 92983, 92987 },
	{ 92996, 92996 },
	{ 93847, 93850 },
	{ 113823, 113823 },
	{ 121479, 121483 },
	{ 125278, 125279 },
};
static const URange16 S_range16[] = {
	{ 36, 36 },
	{ 43, 43 },
	{ 60, 62 },
	{ 94, 94 },
	{ 96, 96 },
	{ 124, 124 },
	{ 126, 126 },
	{ 162, 166 },
	{ 168, 169 },
	{ 172, 172 },
	{ 174, 177 },
	{ 180, 180 },
	{ 184, 184 },
	{ 215, 215 },
	{ 247, 247 },
	{ 706, 709 },
	{ 722, 735 },
	{ 741, 747 },
	{ 749, 749 },
	{ 751, 767 },
	{ 885, 885 },
	{ 900, 901 },
	{ 1014, 1014 },
	{ 1154, 1154 },
	{ 1421, 1423 },
	{ 1542, 1544 },
	{ 1547, 1547 },
	{ 1550, 1551 },
	{ 1758, 1758 },
	{ 1769, 1769 },
	{ 1789, 1790 },
	{ 2038, 2038 },
	{ 2046, 2047 },
	{ 2546, 2547 },
	{ 2554, 2555 },
	{ 2801, 2801 },
	{ 2928, 2928 },
	{ 3059, 3066 },
	{ 3199, 3199 },
	{ 3407, 3407 },
	{ 3449, 3449 },
	{ 3647, 3647 },
	{ 3841, 3843 },
	{ 3859, 3859 },
	{ 3861, 3863 },
	{ 3866, 3871 },
	{ 3892, 3892 },
	{ 3894, 3894 },
	{ 3896, 3896 },
	{ 4030, 4037 },
	{ 4039, 4044 },
	{ 4046, 4047 },
	{ 4053, 4056 },
	{ 4254, 4255 },
	{ 5008, 5017 },
	{ 6107, 6107 },
	{ 6464, 6464 },
	{ 6622, 6655 },
	{ 7009, 7018 },
	{ 7028, 7036 },
	{ 8125, 8125 },
	{ 8127, 8129 },
	{ 8141, 8143 },
	{ 8157, 8159 },
	{ 8173, 8175 },
	{ 8189, 8190 },
	{ 8260, 8260 },
	{ 8274, 8274 },
	{ 8314, 8316 },
	{ 8330, 8332 },
	{ 8352, 8383 },
	{ 8448, 8449 },
	{ 8451, 8454 },
	{ 8456, 8457 },
	{ 8468, 8468 },
	{ 8470, 8472 },
	{ 8478, 8483 },
	{ 8485, 8485 },
	{ 8487, 8487 },
	{ 8489, 8489 },
	{ 8494, 8494 },
	{ 8506, 8507 },
	{ 8512, 8516 },
	{ 8522, 8525 },
	{ 8527, 8527 },
	{ 8586, 8587 },
	{ 8592, 8967 },
	{ 8972, 9000 },
	{ 9003, 9254 },
	{ 9280, 9290 },
	{ 9372, 9449 },
	{ 9472, 10087 },
	{ 10132, 10180 },
	{ 10183, 10213 },
	{ 10224, 10626 },
	{ 10649, 10711 },
	{ 10716, 10747 },
	{ 10750, 11123 },
	{ 11126, 11157 },
	{ 11160, 11208 },
	{ 11210, 11262 },
	{ 11493, 11498 },
	{ 11904, 11929 },
	{ 11931, 12019 },
	{ 12032, 12245 },
	{ 12272, 12283 },
	{ 12292, 12292 },
	{ 12306, 12307 },
	{ 12320, 12320 },
	{ 12342, 12343 },
	{ 12350, 12351 },
	{ 12443, 12444 },
	{ 12688, 12689 },
	{ 12694, 12703 },
	{ 12736, 12771 },
	{ 12800, 12830 },
	{ 12842, 12871 },
	{ 12880, 12880 },
	{ 12896, 12927 },
	{ 12938, 12976 },
	{ 12992, 13054 },
	{ 13056, 13311 },
	{ 19904, 19967 },
	{ 42128, 42182 },
	{ 42752, 42774 },
	{ 42784, 42785 },
	{ 42889, 42890 },
	{ 43048, 43051 },
	{ 43062, 43065 },
	{ 43639, 43641 },
	{ 43867, 43867 },
	{ 64297, 64297 },
	{ 64434, 64449 },
	{ 65020, 65021 },
	{ 65122, 65122 },
	{ 65124, 65126 },
	{ 65129, 65129 },
	{ 65284, 65284 },
	{ 65291, 65291 },
	{ 65308, 65310 },
	{ 65342, 65342 },
	{ 65344, 65344 },
	{ 65372, 65372 },
	{ 65374, 65374 },
	{ 65504, 65510 },
	{ 65512, 65518 },
	{ 65532, 65533 },
};
static const URange32 S_range32[] = {
	{ 65847, 65855 },
	{ 65913, 65929 },
	{ 65932, 65934 },
	{ 65936, 65947 },
	{ 65952, 65952 },
	{ 66000, 66044 },
	{ 67703, 67704 },
	{ 68296, 68296 },
	{ 71487, 71487 },
	{ 92988, 92991 },
	{ 92997, 92997 },
	{ 113820, 113820 },
	{ 118784, 119029 },
	{ 119040, 119078 },
	{ 119081, 119140 },
	{ 119146, 119148 },
	{ 119171, 119172 },
	{ 119180, 119209 },
	{ 119214, 119272 },
	{ 119296, 119361 },
	{ 119365, 119365 },
	{ 119552, 119638 },
	{ 120513, 120513 },
	{ 120539, 120539 },
	{ 120571, 120571 },
	{ 120597, 120597 },
	{ 120629, 120629 },
	{ 120655, 120655 },
	{ 120687, 120687 },
	{ 120713, 120713 },
	{ 120745, 120745 },
	{ 120771, 120771 },
	{ 120832, 121343 },
	{ 121399, 121402 },
	{ 121453, 121460 },
	{ 121462, 121475 },
	{ 121477, 121478 },
	{ 126124, 126124 },
	{ 126128, 126128 },
	{ 126704, 126705 },
	{ 126976, 127019 },
	{ 127024, 127123 },
	{ 127136, 127150 },
	{ 127153, 127167 },
	{ 127169, 127183 },
	{ 127185, 127221 },
	{ 127248, 127339 },
	{ 127344, 127404 },
	{ 127462, 127490 },
	{ 127504, 127547 },
	{ 127552, 127560 },
	{ 127568, 127569 },
	{ 127584, 127589 },
	{ 127744, 128724 },
	{ 128736, 128748 },
	{ 128752, 128761 },
	{ 128768, 128883 },
	{ 128896, 128984 },
	{ 129024, 129035 },
	{ 129040, 129095 },
	{ 129104, 129113 },
	{ 129120, 129159 },
	{ 129168, 129197 },
	{ 129280, 129291 },
	{ 129296, 129342 },
	{ 129344, 129392 },
	{ 129395, 129398 },
	{ 129402, 129402 },
	{ 129404, 129442 },
	{ 129456, 129465 },
	{ 129472, 129474 },
	{ 129488, 129535 },
	{ 129632, 129645 },
};
static const URange16 So_range16[] = {
	{ 166, 166 },
	{ 169, 169 },
	{ 174, 174 },
	{ 176, 176 },
	{ 1154, 1154 },
	{ 1421, 1422 },
	{ 1550, 1551 },
	{ 1758, 1758 },
	{ 1769, 1769 },
	{ 1789, 1790 },
	{ 2038, 2038 },
	{ 2554, 2554 },
	{ 2928, 2928 },
	{ 3059, 3064 },
	{ 3066, 3066 },
	{ 3199, 3199 },
	{ 3407, 3407 },
	{ 3449, 3449 },
	{ 3841, 3843 },
	{ 3859, 3859 },
	{ 3861, 3863 },
	{ 3866, 3871 },
	{ 3892, 3892 },
	{ 3894, 3894 },
	{ 3896, 3896 },
	{ 4030, 4037 },
	{ 4039, 4044 },
	{ 4046, 4047 },
	{ 4053, 4056 },
	{ 4254, 4255 },
	{ 5008, 5017 },
	{ 6464, 6464 },
	{ 6622, 6655 },
	{ 7009, 7018 },
	{ 7028, 7036 },
	{ 8448, 8449 },
	{ 8451, 8454 },
	{ 8456, 8457 },
	{ 8468, 8468 },
	{ 8470, 8471 },
	{ 8478, 8483 },
	{ 8485, 8485 },
	{ 8487, 8487 },
	{ 8489, 8489 },
	{ 8494, 8494 },
	{ 8506, 8507 },
	{ 8522, 8522 },
	{ 8524, 8525 },
	{ 8527, 8527 },
	{ 8586, 8587 },
	{ 8597, 8601 },
	{ 8604, 8607 },
	{ 8609, 8610 },
	{ 8612, 8613 },
	{ 8615, 8621 },
	{ 8623, 8653 },
	{ 8656, 8657 },
	{ 8659, 8659 },
	{ 8661, 8691 },
	{ 8960, 8967 },
	{ 8972, 8991 },
	{ 8994, 9000 },
	{ 9003, 9083 },
	{ 9085, 9114 },
	{ 9140, 9179 },
	{ 9186, 9254 },
	{ 9280, 9290 },
	{ 9372, 9449 },
	{ 9472, 9654 },
	{ 9656, 9664 },
	{ 9666, 9719 },
	{ 9728, 9838 },
	{ 9840, 10087 },
	{ 10132, 10175 },
	{ 10240, 10495 },
	{ 11008, 11055 },
	{ 11077, 11078 },
	{ 11085, 11123 },
	{ 11126, 11157 },
	{ 11160, 11208 },
	{ 11210, 11262 },
	{ 11493, 11498 },
	{ 11904, 11929 },
	{ 11931, 12019 },
	{ 12032, 12245 },
	{ 12272, 12283 },
	{ 12292, 12292 },
	{ 12306, 12307 },
	{ 12320, 12320 },
	{ 12342, 12343 },
	{ 12350, 12351 },
	{ 12688, 12689 },
	{ 12694, 12703 },
	{ 12736, 12771 },
	{ 12800, 12830 },
	{ 12842, 12871 },
	{ 12880, 12880 },
	{ 12896, 12927 },
	{ 12938, 12976 },
	{ 12992, 13054 },
	{ 13056, 13311 },
	{ 19904, 19967 },
	{ 42128, 42182 },
	{ 43048, 43051 },
	{ 43062, 43063 },
	{ 43065, 43065 },
	{ 43639, 43641 },
	{ 65021, 65021 },
	{ 65508, 65508 },
	{ 65512, 65512 },
	{ 65517, 65518 },
	{ 65532, 65533 },
};
static const URange32 So_range32[] = {
	{ 65847, 65855 },
	{ 65913, 65929 },
	{ 65932, 65934 },
	{ 65936, 65947 },
	{ 65952, 65952 },
	{ 66000, 66044 },
	{ 67703, 67704 },
	{ 68296, 68296 },
	{ 71487, 71487 },
	{ 92988, 92991 },
	{ 92997, 92997 },
	{ 113820, 113820 },
	{ 118784, 119029 },
	{ 119040, 119078 },
	{ 119081, 119140 },
	{ 119146, 119148 },
	{ 119171, 119172 },
	{ 119180, 119209 },
	{ 119214, 119272 },
	{ 119296, 119361 },
	{ 119365, 119365 },
	{ 119552, 119638 },
	{ 120832, 121343 },
	{ 121399, 121402 },
	{ 121453, 121460 },
	{ 121462, 121475 },
	{ 121477, 121478 },
	{ 126124, 126124 },
	{ 126976, 127019 },
	{ 127024, 127123 },
	{ 127136, 127150 },
	{ 127153, 127167 },
	{ 127169, 127183 },
	{ 127185, 127221 },
	{ 127248, 127339 },
	{ 127344, 127404 },
	{ 127462, 127490 },
	{ 127504, 127547 },
	{ 127552, 127560 },
	{ 127568, 127569 },
	{ 127584, 127589 },
	{ 127744, 127994 },
	{ 128000, 128724 },
	{ 128736, 128748 },
	{ 128752, 128761 },
	{ 128768, 128883 },
	{ 128896, 128984 },
	{ 129024, 129035 },
	{ 129040, 129095 },
	{ 129104, 129113 },
	{ 129120, 129159 },
	{ 129168, 129197 },
	{ 129280, 129291 },
	{ 129296, 129342 },
	{ 129344, 129392 },
	{ 129395, 129398 },
	{ 129402, 129402 },
	{ 129404, 129442 },
	{ 129456, 129465 },
	{ 129472, 129474 },
	{ 129488, 129535 },
	{ 129632, 129645 },
};
static const URange16 Sm_range16[] = {
	{ 43, 43 },
	{ 60, 62 },
	{ 124, 124 },
	{ 126, 126 },
	{ 172, 172 },
	{ 177, 177 },
	{ 215, 215 },
	{ 247, 247 },
	{ 1014, 1014 },
	{ 1542, 1544 },
	{ 8260, 8260 },
	{ 8274, 8274 },
	{ 8314, 8316 },
	{ 8330, 8332 },
	{ 8472, 8472 },
	{ 8512, 8516 },
	{ 8523, 8523 },
	{ 8592, 8596 },
	{ 8602, 8603 },
	{ 8608, 8608 },
	{ 8611, 8611 },
	{ 8614, 8614 },
	{ 8622, 8622 },
	{ 8654, 8655 },
	{ 8658, 8658 },
	{ 8660, 8660 },
	{ 8692, 8959 },
	{ 8992, 8993 },
	{ 9084, 9084 },
	{ 9115, 9139 },
	{ 9180, 9185 },
	{ 9655, 9655 },
	{ 9665, 9665 },
	{ 9720, 9727 },
	{ 9839, 9839 },
	{ 10176, 10180 },
	{ 10183, 10213 },
	{ 10224, 10239 },
	{ 10496, 10626 },
	{ 10649, 10711 },
	{ 10716, 10747 },
	{ 10750, 11007 },
	{ 11056, 11076 },
	{ 11079, 11084 },
	{ 64297, 64297 },
	{ 65122, 65122 },
	{ 65124, 65126 },
	{ 65291, 65291 },
	{ 65308, 65310 },
	{ 65372, 65372 },
	{ 65374, 65374 },
	{ 65506, 65506 },
	{ 65513, 65516 },
};
static const URange32 Sm_range32[] = {
	{ 120513, 120513 },
	{ 120539, 120539 },
	{ 120571, 120571 },
	{ 120597, 120597 },
	{ 120629, 120629 },
	{ 120655, 120655 },
	{ 120687, 120687 },
	{ 120713, 120713 },
	{ 120745, 120745 },
	{ 120771, 120771 },
	{ 126704, 126705 },
};
static const URange16 Sc_range16[] = {
	{ 36, 36 },
	{ 162, 165 },
	{ 1423, 1423 },
	{ 1547, 1547 },
	{ 2046, 2047 },
	{ 2546, 2547 },
	{ 2555, 2555 },
	{ 2801, 2801 },
	{ 3065, 3065 },
	{ 3647, 3647 },
	{ 6107, 6107 },
	{ 8352, 8383 },
	{ 43064, 43064 },
	{ 65020, 65020 },
	{ 65129, 65129 },
	{ 65284, 65284 },
	{ 65504, 65505 },
	{ 65509, 65510 },
};
static const URange32 Sc_range32[] = {
	{ 126128, 126128 },
};
static const URange16 Z_range16[] = {
	{ 32, 32 },
	{ 160, 160 },
	{ 5760, 5760 },
	{ 8192, 8202 },
	{ 8232, 8233 },
	{ 8239, 8239 },
	{ 8287, 8287 },
	{ 12288, 12288 },
};
static const URange16 Zl_range16[] = {
	{ 8232, 8232 },
};
static const URange16 Co_range16[] = {
	{ 57344, 63743 },
};
static const URange32 Co_range32[] = {
	{ 983040, 1048573 },
	{ 1048576, 1114109 },
};
static const URange16 Cc_range16[] = {
	{ 0, 31 },
	{ 127, 159 },
};
static const URange16 Cf_range16[] = {
	{ 173, 173 },
	{ 1536, 1541 },
	{ 1564, 1564 },
	{ 1757, 1757 },
	{ 1807, 1807 },
	{ 2274, 2274 },
	{ 6158, 6158 },
	{ 8203, 8207 },
	{ 8234, 8238 },
	{ 8288, 8292 },
	{ 8294, 8303 },
	{ 65279, 65279 },
	{ 65529, 65531 },
};
static const URange32 Cf_range32[] = {
	{ 69821, 69821 },
	{ 69837, 69837 },
	{ 113824, 113827 },
	{ 119155, 119162 },
	{ 917505, 917505 },
	{ 917536, 917631 },
};
static const URange16 Cs_range16[] = {
	{ 55296, 57343 },
};
static const URange16 Zp_range16[] = {
	{ 8233, 8233 },
};
static const URange16 Zs_range16[] = {
	{ 32, 32 },
	{ 160, 160 },
	{ 5760, 5760 },
	{ 8192, 8202 },
	{ 8239, 8239 },
	{ 8287, 8287 },
	{ 12288, 12288 },
};
static const URange32 Tangut_range32[] = {
	{ 94176, 94176 },
	{ 94208, 100337 },
	{ 100352, 101106 },
};
static const URange16 Thaana_range16[] = {
	{ 1920, 1969 },
};
static const URange32 Adlam_range32[] = {
	{ 125184, 125258 },
	{ 125264, 125273 },
	{ 125278, 125279 },
};
static const URange16 Telugu_range16[] = {
	{ 3072, 3084 },
	{ 3086, 3088 },
	{ 3090, 3112 },
	{ 3114, 3129 },
	{ 3133, 3140 },
	{ 3142, 3144 },
	{ 3146, 3149 },
	{ 3157, 3158 },
	{ 3160, 3162 },
	{ 3168, 3171 },
	{ 3174, 3183 },
	{ 3192, 3199 },
};
static const URange16 Cyrillic_range16[] = {
	{ 1024, 1156 },
	{ 1159, 1327 },
	{ 7296, 7304 },
	{ 7467, 7467 },
	{ 7544, 7544 },
	{ 11744, 11775 },
	{ 42560, 42655 },
	{ 65070, 65071 },
};
static const URange32 Zanabazar_Square_range32[] = {
	{ 72192, 72263 },
};
static const URange16 Hangul_range16[] = {
	{ 4352, 4607 },
	{ 12334, 12335 },
	{ 12593, 12686 },
	{ 12800, 12830 },
	{ 12896, 12926 },
	{ 43360, 43388 },
	{ 44032, 55203 },
	{ 55216, 55238 },
	{ 55243, 55291 },
	{ 65440, 65470 },
	{ 65474, 65479 },
	{ 65482, 65487 },
	{ 65490, 65495 },
	{ 65498, 65500 },
};
static const URange32 Old_South_Arabian_range32[] = {
	{ 68192, 68223 },
};
static const URange16 Ethiopic_range16[] = {
	{ 4608, 4680 },
	{ 4682, 4685 },
	{ 4688, 4694 },
	{ 4696, 4696 },
	{ 4698, 4701 },
	{ 4704, 4744 },
	{ 4746, 4749 },
	{ 4752, 4784 },
	{ 4786, 4789 },
	{ 4792, 4798 },
	{ 4800, 4800 },
	{ 4802, 4805 },
	{ 4808, 4822 },
	{ 4824, 4880 },
	{ 4882, 4885 },
	{ 4888, 4954 },
	{ 4957, 4988 },
	{ 4992, 5017 },
	{ 11648, 11670 },
	{ 11680, 11686 },
	{ 11688, 11694 },
	{ 11696, 11702 },
	{ 11704, 11710 },
	{ 11712, 11718 },
	{ 11720, 11726 },
	{ 11728, 11734 },
	{ 11736, 11742 },
	{ 43777, 43782 },
	{ 43785, 43790 },
	{ 43793, 43798 },
	{ 43808, 43814 },
	{ 43816, 43822 },
};
static const URange16 Inherited_range16[] = {
	{ 768, 879 },
	{ 1157, 1158 },
	{ 1611, 1621 },
	{ 1648, 1648 },
	{ 2385, 2386 },
	{ 6832, 6846 },
	{ 7376, 7378 },
	{ 7380, 7392 },
	{ 7394, 7400 },
	{ 7405, 7405 },
	{ 7412, 7412 },
	{ 7416, 7417 },
	{ 7616, 7673 },
	{ 7675, 7679 },
	{ 8204, 8205 },
	{ 8400, 8432 },
	{ 12330, 12333 },
	{ 12441, 12442 },
	{ 65024, 65039 },
	{ 65056, 65069 },
};
static const URange32 Inherited_range32[] = {
	{ 66045, 66045 },
	{ 66272, 66272 },
	{ 70459, 70459 },
	{ 119143, 119145 },
	{ 119163, 119170 },
	{ 119173, 119179 },
	{ 119210, 119213 },
	{ 917760, 917999 },
};
static const URange32 Meroitic_Cursive_range32[] = {
	{ 68000, 68023 },
	{ 68028, 68047 },
	{ 68050, 68095 },
};
static const URange32 Bhaiksuki_range32[] = {
	{ 72704, 72712 },
	{ 72714, 72758 },
	{ 72760, 72773 },
	{ 72784, 72812 },
};
static const URange32 Ahom_range32[] = {
	{ 71424, 71450 },
	{ 71453, 71467 },
	{ 71472, 71487 },
};
static const URange16 Han_range16[] = {
	{ 11904, 11929 },
	{ 11931, 12019 },
	{ 12032, 12245 },
	{ 12293, 12293 },
	{ 12295, 12295 },
	{ 12321, 12329 },
	{ 12344, 12347 },
	{ 13312, 19893 },
	{ 19968, 40943 },
	{ 63744, 64109 },
	{ 64112, 64217 },
};
static const URange32 Han_range32[] = {
	{ 131072, 173782 },
	{ 173824, 177972 },
	{ 177984, 178205 },
	{ 178208, 183969 },
	{ 183984, 191456 },
	{ 194560, 195101 },
};
static const URange32 Old_North_Arabian_range32[] = {
	{ 68224, 68255 },
};
static const URange16 Armenian_range16[] = {
	{ 1329, 1366 },
	{ 1369, 1416 },
	{ 1418, 1418 },
	{ 1421, 1423 },
	{ 64275, 64279 },
};
static const URange16 Tamil_range16[] = {
	{ 2946, 2947 },
	{ 2949, 2954 },
	{ 2958, 2960 },
	{ 2962, 2965 },
	{ 2969, 2970 },
	{ 2972, 2972 },
	{ 2974, 2975 },
	{ 2979, 2980 },
	{ 2984, 2986 },
	{ 2990, 3001 },
	{ 3006, 3010 },
	{ 3014, 3016 },
	{ 3018, 3021 },
	{ 3024, 3024 },
	{ 3031, 3031 },
	{ 3046, 3066 },
};
static const URange16 Bopomofo_range16[] = {
	{ 746, 747 },
	{ 12549, 12591 },
	{ 12704, 12730 },
};
static const URange32 Bassa_Vah_range32[] = {
	{ 92880, 92909 },
	{ 92912, 92917 },
};
static const URange16 Sundanese_range16[] = {
	{ 7040, 7103 },
	{ 7360, 7367 },
};
static const URange32 Osage_range32[] = {
	{ 66736, 66771 },
	{ 66776, 66811 },
};
static const URange32 Old_Sogdian_range32[] = {
	{ 69376, 69415 },
};
static const URange16 Tagalog_range16[] = {
	{ 5888, 5900 },
	{ 5902, 5908 },
};
static const URange16 Malayalam_range16[] = {
	{ 3328, 3331 },
	{ 3333, 3340 },
	{ 3342, 3344 },
	{ 3346, 3396 },
	{ 3398, 3400 },
	{ 3402, 3407 },
	{ 3412, 3427 },
	{ 3430, 3455 },
};
static const URange32 Marchen_range32[] = {
	{ 72816, 72847 },
	{ 72850, 72871 },
	{ 72873, 72886 },
};
static const URange32 Carian_range32[] = {
	{ 66208, 66256 },
};
static const URange16 Hiragana_range16[] = {
	{ 12353, 12438 },
	{ 12445, 12447 },
};
static const URange32 Hiragana_range32[] = {
	{ 110593, 110878 },
	{ 127488, 127488 },
};
static const URange16 Tagbanwa_range16[] = {
	{ 5984, 5996 },
	{ 5998, 6000 },
	{ 6002, 6003 },
};
static const URange16 Meetei_Mayek_range16[] = {
	{ 43744, 43766 },
	{ 43968, 44013 },
	{ 44016, 44025 },
};
static const URange32 Hanifi_Rohingya_range32[] = {
	{ 68864, 68903 },
	{ 68912, 68921 },
};
static const URange32 Pahawh_Hmong_range32[] = {
	{ 92928, 92997 },
	{ 93008, 93017 },
	{ 93019, 93025 },
	{ 93027, 93047 },
	{ 93053, 93071 },
};
static const URange16 Tai_Le_range16[] = {
	{ 6480, 6509 },
	{ 6512, 6516 },
};
static const URange16 Kayah_Li_range16[] = {
	{ 43264, 43309 },
	{ 43311, 43311 },
};
static const URange16 Buginese_range16[] = {
	{ 6656, 6683 },
	{ 6686, 6687 },
};
static const URange32 Kharoshthi_range32[] = {
	{ 68096, 68099 },
	{ 68101, 68102 },
	{ 68108, 68115 },
	{ 68117, 68119 },
	{ 68121, 68149 },
	{ 68152, 68154 },
	{ 68159, 68168 },
	{ 68176, 68184 },
};
static const URange16 Tai_Tham_range16[] = {
	{ 6688, 6750 },
	{ 6752, 6780 },
	{ 6783, 6793 },
	{ 6800, 6809 },
	{ 6816, 6829 },
};
static const URange32 Old_Italic_range32[] = {
	{ 66304, 66339 },
	{ 66349, 66351 },
};
static const URange32 Old_Persian_range32[] = {
	{ 66464, 66499 },
	{ 66504, 66517 },
};
static const URange32 Warang_Citi_range32[] = {
	{ 71840, 71922 },
	{ 71935, 71935 },
};
static const URange16 Latin_range16[] = {
	{ 65, 90 },
	{ 97, 122 },
	{ 170, 170 },
	{ 186, 186 },
	{ 192, 214 },
	{ 216, 246 },
	{ 248, 696 },
	{ 736, 740 },
	{ 7424, 7461 },
	{ 7468, 7516 },
	{ 7522, 7525 },
	{ 7531, 7543 },
	{ 7545, 7614 },
	{ 7680, 7935 },
	{ 8305, 8305 },
	{ 8319, 8319 },
	{ 8336, 8348 },
	{ 8490, 8491 },
	{ 8498, 8498 },
	{ 8526, 8526 },
	{ 8544, 8584 },
	{ 11360, 11391 },
	{ 42786, 42887 },
	{ 42891, 42937 },
	{ 42999, 43007 },
	{ 43824, 43866 },
	{ 43868, 43876 },
	{ 64256, 64262 },
	{ 65313, 65338 },
	{ 65345, 65370 },
};
static const URange16 Saurashtra_range16[] = {
	{ 43136, 43205 },
	{ 43214, 43225 },
};
static const URange32 Shavian_range32[] = {
	{ 66640, 66687 },
};
static const URange16 Georgian_range16[] = {
	{ 4256, 4293 },
	{ 4295, 4295 },
	{ 4301, 4301 },
	{ 4304, 4346 },
	{ 4348, 4351 },
	{ 7312, 7354 },
	{ 7357, 7359 },
	{ 11520, 11557 },
	{ 11559, 11559 },
	{ 11565, 11565 },
};
static const URange32 Grantha_range32[] = {
	{ 70400, 70403 },
	{ 70405, 70412 },
	{ 70415, 70416 },
	{ 70419, 70440 },
	{ 70442, 70448 },
	{ 70450, 70451 },
	{ 70453, 70457 },
	{ 70460, 70468 },
	{ 70471, 70472 },
	{ 70475, 70477 },
	{ 70480, 70480 },
	{ 70487, 70487 },
	{ 70493, 70499 },
	{ 70502, 70508 },
	{ 70512, 70516 },
};
static const URange32 Duployan_range32[] = {
	{ 113664, 113770 },
	{ 113776, 113788 },
	{ 113792, 113800 },
	{ 113808, 113817 },
	{ 113820, 113823 },
};
static const URange16 Batak_range16[] = {
	{ 7104, 7155 },
	{ 7164, 7167 },
};
static const URange16 Devanagari_range16[] = {
	{ 2304, 2384 },
	{ 2387, 2403 },
	{ 2406, 2431 },
	{ 43232, 43263 },
};
static const URange16 Thai_range16[] = {
	{ 3585, 3642 },
	{ 3648, 3675 },
};
static const URange16 Tibetan_range16[] = {
	{ 3840, 3911 },
	{ 3913, 3948 },
	{ 3953, 3991 },
	{ 3993, 4028 },
	{ 4030, 4044 },
	{ 4046, 4052 },
	{ 4057, 4058 },
};
static const URange16 Tifinagh_range16[] = {
	{ 11568, 11623 },
	{ 11631, 11632 },
	{ 11647, 11647 },
};
static const URange32 Ugaritic_range32[] = {
	{ 66432, 66461 },
	{ 66463, 66463 },
};
static const URange16 Braille_range16[] = {
	{ 10240, 10495 },
};
static const URange32 Anatolian_Hieroglyphs_range32[] = {
	{ 82944, 83526 },
};
static const URange16 Greek_range16[] = {
	{ 880, 883 },
	{ 885, 887 },
	{ 890, 893 },
	{ 895, 895 },
	{ 900, 900 },
	{ 902, 902 },
	{ 904, 906 },
	{ 908, 908 },
	{ 910, 929 },
	{ 931, 993 },
	{ 1008, 1023 },
	{ 7462, 7466 },
	{ 7517, 7521 },
	{ 7526, 7530 },
	{ 7615, 7615 },
	{ 7936, 7957 },
	{ 7960, 7965 },
	{ 7968, 8005 },
	{ 8008, 8013 },
	{ 8016, 8023 },
	{ 8025, 8025 },
	{ 8027, 8027 },
	{ 8029, 8029 },
	{ 8031, 8061 },
	{ 8064, 8116 },
	{ 8118, 8132 },
	{ 8134, 8147 },
	{ 8150, 8155 },
	{ 8157, 8175 },
	{ 8178, 8180 },
	{ 8182, 8190 },
	{ 8486, 8486 },
	{ 43877, 43877 },
};
static const URange32 Greek_range32[] = {
	{ 65856, 65934 },
	{ 65952, 65952 },
	{ 119296, 119365 },
};
static const URange32 Lycian_range32[] = {
	{ 66176, 66204 },
};
static const URange32 Mende_Kikakui_range32[] = {
	{ 124928, 125124 },
	{ 125127, 125142 },
};
static const URange16 Tai_Viet_range16[] = {
	{ 43648, 43714 },
	{ 43739, 43743 },
};
static const URange16 Vai_range16[] = {
	{ 42240, 42539 },
};
static const URange16 Ogham_range16[] = {
	{ 5760, 5788 },
};
static const URange32 Inscriptional_Parthian_range32[] = {
	{ 68416, 68437 },
	{ 68440, 68447 },
};
static const URange16 Cham_range16[] = {
	{ 43520, 43574 },
	{ 43584, 43597 },
	{ 43600, 43609 },
	{ 43612, 43615 },
};
static const URange16 Syriac_range16[] = {
	{ 1792, 1805 },
	{ 1807, 1866 },
	{ 1869, 1871 },
	{ 2144, 2154 },
};
static const URange16 Runic_range16[] = {
	{ 5792, 5866 },
	{ 5870, 5880 },
};
static const URange32 Gothic_range32[] = {
	{ 66352, 66378 },
};
static const URange32 Mahajani_range32[] = {
	{ 69968, 70006 },
};
static const URange16 Katakana_range16[] = {
	{ 12449, 12538 },
	{ 12541, 12543 },
	{ 12784, 12799 },
	{ 13008, 13054 },
	{ 13056, 13143 },
	{ 65382, 65391 },
	{ 65393, 65437 },
};
static const URange32 Katakana_range32[] = {
	{ 110592, 110592 },
};
static const URange32 Osmanya_range32[] = {
	{ 66688, 66717 },
	{ 66720, 66729 },
};
static const URange16 New_Tai_Lue_range16[] = {
	{ 6528, 6571 },
	{ 6576, 6601 },
	{ 6608, 6618 },
	{ 6622, 6623 },
};
static const URange16 Ol_Chiki_range16[] = {
	{ 7248, 7295 },
};
static const URange32 Newa_range32[] = {
	{ 70656, 70745 },
	{ 70747, 70747 },
	{ 70749, 70750 },
};
static const URange16 Limbu_range16[] = {
	{ 6400, 6430 },
	{ 6432, 6443 },
	{ 6448, 6459 },
	{ 6464, 6464 },
	{ 6468, 6479 },
};
static const URange32 Pau_Cin_Hau_range32[] = {
	{ 72384, 72440 },
};
static const URange16 Cherokee_range16[] = {
	{ 5024, 5109 },
	{ 5112, 5117 },
	{ 43888, 43967 },
};
static const URange32 Miao_range32[] = {
	{ 93952, 94020 },
	{ 94032, 94078 },
	{ 94095, 94111 },
};
static const URange16 Oriya_range16[] = {
	{ 2817, 2819 },
	{ 2821, 2828 },
	{ 2831, 2832 },
	{ 2835, 2856 },
	{ 2858, 2864 },
	{ 2866, 2867 },
	{ 2869, 2873 },
	{ 2876, 2884 },
	{ 2887, 2888 },
	{ 2891, 2893 },
	{ 2902, 2903 },
	{ 2908, 2909 },
	{ 2911, 2915 },
	{ 2918, 2935 },
};
static const URange32 Medefaidrin_range32[] = {
	{ 93760, 93850 },
};
static const URange32 Sharada_range32[] = {
	{ 70016, 70093 },
	{ 70096, 70111 },
};
static const URange16 Gujarati_range16[] = {
	{ 2689, 2691 },
	{ 2693, 2701 },
	{ 2703, 2705 },
	{ 2707, 2728 },
	{ 2730, 2736 },
	{ 2738, 2739 },
	{ 2741, 2745 },
	{ 2748, 2757 },
	{ 2759, 2761 },
	{ 2763, 2765 },
	{ 2768, 2768 },
	{ 2784, 2787 },
	{ 2790, 2801 },
	{ 2809, 2815 },
};
static const URange32 Nushu_range32[] = {
	{ 94177, 94177 },
	{ 110960, 111355 },
};
static const URange32 Modi_range32[] = {
	{ 71168, 71236 },
	{ 71248, 71257 },
};
static const URange32 Gunjala_Gondi_range32[] = {
	{ 73056, 73061 },
	{ 73063, 73064 },
	{ 73066, 73102 },
	{ 73104, 73105 },
	{ 73107, 73112 },
	{ 73120, 73129 },
};
static const URange32 Inscriptional_Pahlavi_range32[] = {
	{ 68448, 68466 },
	{ 68472, 68479 },
};
static const URange32 Manichaean_range32[] = {
	{ 68288, 68326 },
	{ 68331, 68342 },
};
static const URange16 Khmer_range16[] = {
	{ 6016, 6109 },
	{ 6112, 6121 },
	{ 6128, 6137 },
	{ 6624, 6655 },
};
static const URange32 Cuneiform_range32[] = {
	{ 73728, 74649 },
	{ 74752, 74862 },
	{ 74864, 74868 },
	{ 74880, 75075 },
};
static const URange32 Khudawadi_range32[] = {
	{ 70320, 70378 },
	{ 70384, 70393 },
};
static const URange16 Mandaic_range16[] = {
	{ 2112, 2139 },
	{ 2142, 2142 },
};
static const URange32 Hatran_range32[] = {
	{ 67808, 67826 },
	{ 67828, 67829 },
	{ 67835, 67839 },
};
static const URange16 Syloti_Nagri_range16[] = {
	{ 43008, 43051 },
};
static const URange16 Nko_range16[] = {
	{ 1984, 2042 },
	{ 2045, 2047 },
};
static const URange16 Canadian_Aboriginal_range16[] = {
	{ 5120, 5759 },
	{ 6320, 6389 },
};
static const URange32 Meroitic_Hieroglyphs_range32[] = {
	{ 67968, 67999 },
};
static const URange32 Phoenician_range32[] = {
	{ 67840, 67867 },
	{ 67871, 67871 },
};
static const URange32 Nabataean_range32[] = {
	{ 67712, 67742 },
	{ 67751, 67759 },
};
static const URange16 Bengali_range16[] = {
	{ 2432, 2435 },
	{ 2437, 2444 },
	{ 2447, 2448 },
	{ 2451, 2472 },
	{ 2474, 2480 },
	{ 2482, 2482 },
	{ 2486, 2489 },
	{ 2492, 2500 },
	{ 2503, 2504 },
	{ 2507, 2510 },
	{ 2519, 2519 },
	{ 2524, 2525 },
	{ 2527, 2531 },
	{ 2534, 2558 },
};
static const URange32 Kaithi_range32[] = {
	{ 69760, 69825 },
	{ 69837, 69837 },
};
static const URange16 Glagolitic_range16[] = {
	{ 11264, 11310 },
	{ 11312, 11358 },
};
static const URange32 Glagolitic_range32[] = {
	{ 122880, 122886 },
	{ 122888, 122904 },
	{ 122907, 122913 },
	{ 122915, 122916 },
	{ 122918, 122922 },
};
static const URange32 Imperial_Aramaic_range32[] = {
	{ 67648, 67669 },
	{ 67671, 67679 },
};
static const URange32 Sora_Sompeng_range32[] = {
	{ 69840, 69864 },
	{ 69872, 69881 },
};
static const URange16 Gurmukhi_range16[] = {
	{ 2561, 2563 },
	{ 2565, 2570 },
	{ 2575, 2576 },
	{ 2579, 2600 },
	{ 2602, 2608 },
	{ 2610, 2611 },
	{ 2613, 2614 },
	{ 2616, 2617 },
	{ 2620, 2620 },
	{ 2622, 2626 },
	{ 2631, 2632 },
	{ 2635, 2637 },
	{ 2641, 2641 },
	{ 2649, 2652 },
	{ 2654, 2654 },
	{ 2662, 2678 },
};
static const URange16 Javanese_range16[] = {
	{ 43392, 43469 },
	{ 43472, 43481 },
	{ 43486, 43487 },
};
static const URange32 Old_Permic_range32[] = {
	{ 66384, 66426 },
};
static const URange16 Phags_Pa_range16[] = {
	{ 43072, 43127 },
};
static const URange32 Cypriot_range32[] = {
	{ 67584, 67589 },
	{ 67592, 67592 },
	{ 67594, 67637 },
	{ 67639, 67640 },
	{ 67644, 67644 },
	{ 67647, 67647 },
};
static const URange16 Kannada_range16[] = {
	{ 3200, 3212 },
	{ 3214, 3216 },
	{ 3218, 3240 },
	{ 3242, 3251 },
	{ 3253, 3257 },
	{ 3260, 3268 },
	{ 3270, 3272 },
	{ 3274, 3277 },
	{ 3285, 3286 },
	{ 3294, 3294 },
	{ 3296, 3299 },
	{ 3302, 3311 },
	{ 3313, 3314 },
};
static const URange32 Khojki_range32[] = {
	{ 70144, 70161 },
	{ 70163, 70206 },
};
static const URange16 Mongolian_range16[] = {
	{ 6144, 6145 },
	{ 6148, 6148 },
	{ 6150, 6158 },
	{ 6160, 6169 },
	{ 6176, 6264 },
	{ 6272, 6314 },
};
static const URange32 Mongolian_range32[] = {
	{ 71264, 71276 },
};
static const URange16 Sinhala_range16[] = {
	{ 3458, 3459 },
	{ 3461, 3478 },
	{ 3482, 3505 },
	{ 3507, 3515 },
	{ 3517, 3517 },
	{ 3520, 3526 },
	{ 3530, 3530 },
	{ 3535, 3540 },
	{ 3542, 3542 },
	{ 3544, 3551 },
	{ 3558, 3567 },
	{ 3570, 3572 },
};
static const URange32 Sinhala_range32[] = {
	{ 70113, 70132 },
};
static const URange32 Brahmi_range32[] = {
	{ 69632, 69709 },
	{ 69714, 69743 },
	{ 69759, 69759 },
};
static const URange32 Elbasan_range32[] = {
	{ 66816, 66855 },
};
static const URange32 Deseret_range32[] = {
	{ 66560, 66639 },
};
static const URange16 Rejang_range16[] = {
	{ 43312, 43347 },
	{ 43359, 43359 },
};
static const URange32 SignWriting_range32[] = {
	{ 120832, 121483 },
	{ 121499, 121503 },
	{ 121505, 121519 },
};
static const URange32 Multani_range32[] = {
	{ 70272, 70278 },
	{ 70280, 70280 },
	{ 70282, 70285 },
	{ 70287, 70301 },
	{ 70303, 70313 },
};
static const URange16 Yi_range16[] = {
	{ 40960, 42124 },
	{ 42128, 42182 },
};
static const URange16 Balinese_range16[] = {
	{ 6912, 6987 },
	{ 6992, 7036 },
};
static const URange16 Lao_range16[] = {
	{ 3713, 3714 },
	{ 3716, 3716 },
	{ 3719, 3720 },
	{ 3722, 3722 },
	{ 3725, 3725 },
	{ 3732, 3735 },
	{ 3737, 3743 },
	{ 3745, 3747 },
	{ 3749, 3749 },
	{ 3751, 3751 },
	{ 3754, 3755 },
	{ 3757, 3769 },
	{ 3771, 3773 },
	{ 3776, 3780 },
	{ 3782, 3782 },
	{ 3784, 3789 },
	{ 3792, 3801 },
	{ 3804, 3807 },
};
static const URange16 Hanunoo_range16[] = {
	{ 5920, 5940 },
};
static const URange32 Masaram_Gondi_range32[] = {
	{ 72960, 72966 },
	{ 72968, 72969 },
	{ 72971, 73014 },
	{ 73018, 73018 },
	{ 73020, 73021 },
	{ 73023, 73031 },
	{ 73040, 73049 },
};
static const URange32 Linear_B_range32[] = {
	{ 65536, 65547 },
	{ 65549, 65574 },
	{ 65576, 65594 },
	{ 65596, 65597 },
	{ 65599, 65613 },
	{ 65616, 65629 },
	{ 65664, 65786 },
};
static const URange32 Linear_A_range32[] = {
	{ 67072, 67382 },
	{ 67392, 67413 },
	{ 67424, 67431 },
};
static const URange32 Dogra_range32[] = {
	{ 71680, 71739 },
};
static const URange32 Old_Turkic_range32[] = {
	{ 68608, 68680 },
};
static const URange16 Lepcha_range16[] = {
	{ 7168, 7223 },
	{ 7227, 7241 },
	{ 7245, 7247 },
};
static const URange32 Lydian_range32[] = {
	{ 67872, 67897 },
	{ 67903, 67903 },
};
static const URange32 Egyptian_Hieroglyphs_range32[] = {
	{ 77824, 78894 },
};
static const URange32 Sogdian_range32[] = {
	{ 69424, 69465 },
};
static const URange32 Caucasian_Albanian_range32[] = {
	{ 66864, 66915 },
	{ 66927, 66927 },
};
static const URange32 Makasar_range32[] = {
	{ 73440, 73464 },
};
static const URange32 Old_Hungarian_range32[] = {
	{ 68736, 68786 },
	{ 68800, 68850 },
	{ 68858, 68863 },
};
static const URange16 Samaritan_range16[] = {
	{ 2048, 2093 },
	{ 2096, 2110 },
};
static const URange16 Lisu_range16[] = {
	{ 42192, 42239 },
};
static const URange16 Buhid_range16[] = {
	{ 5952, 5971 },
};
static const URange32 Palmyrene_range32[] = {
	{ 67680, 67711 },
};
static const URange32 Tirhuta_range32[] = {
	{ 70784, 70855 },
	{ 70864, 70873 },
};
static const URange32 Mro_range32[] = {
	{ 92736, 92766 },
	{ 92768, 92777 },
	{ 92782, 92783 },
};
static const URange16 Common_range16[] = {
	{ 0, 64 },
	{ 91, 96 },
	{ 123, 169 },
	{ 171, 185 },
	{ 187, 191 },
	{ 215, 215 },
	{ 247, 247 },
	{ 697, 735 },
	{ 741, 745 },
	{ 748, 767 },
	{ 884, 884 },
	{ 894, 894 },
	{ 901, 901 },
	{ 903, 903 },
	{ 1417, 1417 },
	{ 1541, 1541 },
	{ 1548, 1548 },
	{ 1563, 1563 },
	{ 1567, 1567 },
	{ 1600, 1600 },
	{ 1757, 1757 },
	{ 2274, 2274 },
	{ 2404, 2405 },
	{ 3647, 3647 },
	{ 4053, 4056 },
	{ 4347, 4347 },
	{ 5867, 5869 },
	{ 5941, 5942 },
	{ 6146, 6147 },
	{ 6149, 6149 },
	{ 7379, 7379 },
	{ 7393, 7393 },
	{ 7401, 7404 },
	{ 7406, 7411 },
	{ 7413, 7415 },
	{ 8192, 8203 },
	{ 8206, 8292 },
	{ 8294, 8304 },
	{ 8308, 8318 },
	{ 8320, 8334 },
	{ 8352, 8383 },
	{ 8448, 8485 },
	{ 8487, 8489 },
	{ 8492, 8497 },
	{ 8499, 8525 },
	{ 8527, 8543 },
	{ 8585, 8587 },
	{ 8592, 9254 },
	{ 9280, 9290 },
	{ 9312, 10239 },
	{ 10496, 11123 },
	{ 11126, 11157 },
	{ 11160, 11208 },
	{ 11210, 11262 },
	{ 11776, 11854 },
	{ 12272, 12283 },
	{ 12288, 12292 },
	{ 12294, 12294 },
	{ 12296, 12320 },
	{ 12336, 12343 },
	{ 12348, 12351 },
	{ 12443, 12444 },
	{ 12448, 12448 },
	{ 12539, 12540 },
	{ 12688, 12703 },
	{ 12736, 12771 },
	{ 12832, 12895 },
	{ 12927, 13007 },
	{ 13144, 13311 },
	{ 19904, 19967 },
	{ 42752, 42785 },
	{ 42888, 42890 },
	{ 43056, 43065 },
	{ 43310, 43310 },
	{ 43471, 43471 },
	{ 43867, 43867 },
	{ 64830, 64831 },
	{ 65040, 65049 },
	{ 65072, 65106 },
	{ 65108, 65126 },
	{ 65128, 65131 },
	{ 65279, 65279 },
	{ 65281, 65312 },
	{ 65339, 65344 },
	{ 65371, 65381 },
	{ 65392, 65392 },
	{ 65438, 65439 },
	{ 65504, 65510 },
	{ 65512, 65518 },
	{ 65529, 65533 },
};
static const URange32 Common_range32[] = {
	{ 65792, 65794 },
	{ 65799, 65843 },
	{ 65847, 65855 },
	{ 65936, 65947 },
	{ 66000, 66044 },
	{ 66273, 66299 },
	{ 113824, 113827 },
	{ 118784, 119029 },
	{ 119040, 119078 },
	{ 119081, 119142 },
	{ 119146, 119162 },
	{ 119171, 119172 },
	{ 119180, 119209 },
	{ 119214, 119272 },
	{ 119520, 119539 },
	{ 119552, 119638 },
	{ 119648, 119672 },
	{ 119808, 119892 },
	{ 119894, 119964 },
	{ 119966, 119967 },
	{ 119970, 119970 },
	{ 119973, 119974 },
	{ 119977, 119980 },
	{ 119982, 119993 },
	{ 119995, 119995 },
	{ 119997, 120003 },
	{ 120005, 120069 },
	{ 120071, 120074 },
	{ 120077, 120084 },
	{ 120086, 120092 },
	{ 120094, 120121 },
	{ 120123, 120126 },
	{ 120128, 120132 },
	{ 120134, 120134 },
	{ 120138, 120144 },
	{ 120146, 120485 },
	{ 120488, 120779 },
	{ 120782, 120831 },
	{ 126065, 126132 },
	{ 126976, 127019 },
	{ 127024, 127123 },
	{ 127136, 127150 },
	{ 127153, 127167 },
	{ 127169, 127183 },
	{ 127185, 127221 },
	{ 127232, 127244 },
	{ 127248, 127339 },
	{ 127344, 127404 },
	{ 127462, 127487 },
	{ 127489, 127490 },
	{ 127504, 127547 },
	{ 127552, 127560 },
	{ 127568, 127569 },
	{ 127584, 127589 },
	{ 127744, 128724 },
	{ 128736, 128748 },
	{ 128752, 128761 },
	{ 128768, 128883 },
	{ 128896, 128984 },
	{ 129024, 129035 },
	{ 129040, 129095 },
	{ 129104, 129113 },
	{ 129120, 129159 },
	{ 129168, 129197 },
	{ 129280, 129291 },
	{ 129296, 129342 },
	{ 129344, 129392 },
	{ 129395, 129398 },
	{ 129402, 129402 },
	{ 129404, 129442 },
	{ 129456, 129465 },
	{ 129472, 129474 },
	{ 129488, 129535 },
	{ 129632, 129645 },
	{ 917505, 917505 },
	{ 917536, 917631 },
};
static const URange16 Coptic_range16[] = {
	{ 994, 1007 },
	{ 11392, 11507 },
	{ 11513, 11519 },
};
static const URange32 Chakma_range32[] = {
	{ 69888, 69940 },
	{ 69942, 69958 },
};
static const URange16 Arabic_range16[] = {
	{ 1536, 1540 },
	{ 1542, 1547 },
	{ 1549, 1562 },
	{ 1564, 1564 },
	{ 1566, 1566 },
	{ 1568, 1599 },
	{ 1601, 1610 },
	{ 1622, 1647 },
	{ 1649, 1756 },
	{ 1758, 1791 },
	{ 1872, 1919 },
	{ 2208, 2228 },
	{ 2230, 2237 },
	{ 2259, 2273 },
	{ 2275, 2303 },
	{ 64336, 64449 },
	{ 64467, 64829 },
	{ 64848, 64911 },
	{ 64914, 64967 },
	{ 65008, 65021 },
	{ 65136, 65140 },
	{ 65142, 65276 },
};
static const URange32 Arabic_range32[] = {
	{ 69216, 69246 },
	{ 126464, 126467 },
	{ 126469, 126495 },
	{ 126497, 126498 },
	{ 126500, 126500 },
	{ 126503, 126503 },
	{ 126505, 126514 },
	{ 126516, 126519 },
	{ 126521, 126521 },
	{ 126523, 126523 },
	{ 126530, 126530 },
	{ 126535, 126535 },
	{ 126537, 126537 },
	{ 126539, 126539 },
	{ 126541, 126543 },
	{ 126545, 126546 },
	{ 126548, 126548 },
	{ 126551, 126551 },
	{ 126553, 126553 },
	{ 126555, 126555 },
	{ 126557, 126557 },
	{ 126559, 126559 },
	{ 126561, 126562 },
	{ 126564, 126564 },
	{ 126567, 126570 },
	{ 126572, 126578 },
	{ 126580, 126583 },
	{ 126585, 126588 },
	{ 126590, 126590 },
	{ 126592, 126601 },
	{ 126603, 126619 },
	{ 126625, 126627 },
	{ 126629, 126633 },
	{ 126635, 126651 },
	{ 126704, 126705 },
};
static const URange16 Bamum_range16[] = {
	{ 42656, 42743 },
};
static const URange32 Bamum_range32[] = {
	{ 92160, 92728 },
};
static const URange16 Myanmar_range16[] = {
	{ 4096, 4255 },
	{ 43488, 43518 },
	{ 43616, 43647 },
};
static const URange32 Siddham_range32[] = {
	{ 71040, 71093 },
	{ 71096, 71133 },
};
static const URange32 Soyombo_range32[] = {
	{ 72272, 72323 },
	{ 72326, 72354 },
};
static const URange32 Avestan_range32[] = {
	{ 68352, 68405 },
	{ 68409, 68415 },
};
static const URange16 Hebrew_range16[] = {
	{ 1425, 1479 },
	{ 1488, 1514 },
	{ 1519, 1524 },
	{ 64285, 64310 },
	{ 64312, 64316 },
	{ 64318, 64318 },
	{ 64320, 64321 },
	{ 64323, 64324 },
	{ 64326, 64335 },
};
static const URange32 Psalter_Pahlavi_range32[] = {
	{ 68480, 68497 },
	{ 68505, 68508 },
	{ 68521, 68527 },
};
static const URange32 Takri_range32[] = {
	{ 71296, 71351 },
	{ 71360, 71369 },
};
// 3994 16-bit ranges, 1429 32-bit ranges
const UGroup unicode_groups[] = {
	{ "Adlam", +1, 0, 0, Adlam_range32, 3 },
	{ "Ahom", +1, 0, 0, Ahom_range32, 3 },
	{ "Anatolian_Hieroglyphs", +1, 0, 0, Anatolian_Hieroglyphs_range32, 1 },
	{ "Arabic", +1, Arabic_range16, 22, Arabic_range32, 35 },
	{ "Armenian", +1, Armenian_range16, 5, 0, 0 },
	{ "Avestan", +1, 0, 0, Avestan_range32, 2 },
	{ "Balinese", +1, Balinese_range16, 2, 0, 0 },
	{ "Bamum", +1, Bamum_range16, 1, Bamum_range32, 1 },
	{ "Bassa_Vah", +1, 0, 0, Bassa_Vah_range32, 2 },
	{ "Batak", +1, Batak_range16, 2, 0, 0 },
	{ "Bengali", +1, Bengali_range16, 14, 0, 0 },
	{ "Bhaiksuki", +1, 0, 0, Bhaiksuki_range32, 4 },
	{ "Bopomofo", +1, Bopomofo_range16, 3, 0, 0 },
	{ "Brahmi", +1, 0, 0, Brahmi_range32, 3 },
	{ "Braille", +1, Braille_range16, 1, 0, 0 },
	{ "Buginese", +1, Buginese_range16, 2, 0, 0 },
	{ "Buhid", +1, Buhid_range16, 1, 0, 0 },
	{ "C", +1, C_range16, 16, C_range32, 8 },
	{ "Canadian_Aboriginal", +1, Canadian_Aboriginal_range16, 2, 0, 0 },
	{ "Carian", +1, 0, 0, Carian_range32, 1 },
	{ "Caucasian_Albanian", +1, 0, 0, Caucasian_Albanian_range32, 2 },
	{ "Cc", +1, Cc_range16, 2, 0, 0 },
	{ "Cf", +1, Cf_range16, 13, Cf_range32, 6 },
	{ "Chakma", +1, 0, 0, Chakma_range32, 2 },
	{ "Cham", +1, Cham_range16, 4, 0, 0 },
	{ "Cherokee", +1, Cherokee_range16, 3, 0, 0 },
	{ "Co", +1, Co_range16, 1, Co_range32, 2 },
	{ "Common", +1, Common_range16, 90, Common_range32, 76 },
	{ "Coptic", +1, Coptic_range16, 3, 0, 0 },
	{ "Cs", +1, Cs_range16, 1, 0, 0 },
	{ "Cuneiform", +1, 0, 0, Cuneiform_range32, 4 },
	{ "Cypriot", +1, 0, 0, Cypriot_range32, 6 },
	{ "Cyrillic", +1, Cyrillic_range16, 8, 0, 0 },
	{ "Deseret", +1, 0, 0, Deseret_range32, 1 },
	{ "Devanagari", +1, Devanagari_range16, 4, 0, 0 },
	{ "Dogra", +1, 0, 0, Dogra_range32, 1 },
	{ "Duployan", +1, 0, 0, Duployan_range32, 5 },
	{ "Egyptian_Hieroglyphs", +1, 0, 0, Egyptian_Hieroglyphs_range32, 1 },
	{ "Elbasan", +1, 0, 0, Elbasan_range32, 1 },
	{ "Ethiopic", +1, Ethiopic_range16, 32, 0, 0 },
	{ "Georgian", +1, Georgian_range16, 10, 0, 0 },
	{ "Glagolitic", +1, Glagolitic_range16, 2, Glagolitic_range32, 5 },
	{ "Gothic", +1, 0, 0, Gothic_range32, 1 },
	{ "Grantha", +1, 0, 0, Grantha_range32, 15 },
	{ "Greek", +1, Greek_range16, 33, Greek_range32, 3 },
	{ "Gujarati", +1, Gujarati_range16, 14, 0, 0 },
	{ "Gunjala_Gondi", +1, 0, 0, Gunjala_Gondi_range32, 6 },
	{ "Gurmukhi", +1, Gurmukhi_range16, 16, 0, 0 },
	{ "Han", +1, Han_range16, 11, Han_range32, 6 },
	{ "Hangul", +1, Hangul_range16, 14, 0, 0 },
	{ "Hanifi_Rohingya", +1, 0, 0, Hanifi_Rohingya_range32, 2 },
	{ "Hanunoo", +1, Hanunoo_range16, 1, 0, 0 },
	{ "Hatran", +1, 0, 0, Hatran_range32, 3 },
	{ "Hebrew", +1, Hebrew_range16, 9, 0, 0 },
	{ "Hiragana", +1, Hiragana_range16, 2, Hiragana_range32, 2 },
	{ "Imperial_Aramaic", +1, 0, 0, Imperial_Aramaic_range32, 2 },
	{ "Inherited", +1, Inherited_range16, 20, Inherited_range32, 8 },
	{ "Inscriptional_Pahlavi", +1, 0, 0, Inscriptional_Pahlavi_range32, 2 },
	{ "Inscriptional_Parthian", +1, 0, 0, Inscriptional_Parthian_range32, 2 },
	{ "Javanese", +1, Javanese_range16, 3, 0, 0 },
	{ "Kaithi", +1, 0, 0, Kaithi_range32, 2 },
	{ "Kannada", +1, Kannada_range16, 13, 0, 0 },
	{ "Katakana", +1, Katakana_range16, 7, Katakana_range32, 1 },
	{ "Kayah_Li", +1, Kayah_Li_range16, 2, 0, 0 },
	{ "Kharoshthi", +1, 0, 0, Kharoshthi_range32, 8 },
	{ "Khmer", +1, Khmer_range16, 4, 0, 0 },
	{ "Khojki", +1, 0, 0, Khojki_range32, 2 },
	{ "Khudawadi", +1, 0, 0, Khudawadi_range32, 2 },
	{ "L", +1, L_range16, 384, L_range32, 215 },
	{ "Lao", +1, Lao_range16, 18, 0, 0 },
	{ "Latin", +1, Latin_range16, 30, 0, 0 },
	{ "Lepcha", +1, Lepcha_range16, 3, 0, 0 },
	{ "Limbu", +1, Limbu_range16, 5, 0, 0 },
	{ "Linear_A", +1, 0, 0, Linear_A_range32, 3 },
	{ "Linear_B", +1, 0, 0, Linear_B_range32, 7 },
	{ "Lisu", +1, Lisu_range16, 1, 0, 0 },
	{ "Ll", +1, Ll_range16, 604, Ll_range32, 34 },
	{ "Lm", +1, Lm_range16, 54, Lm_range32, 3 },
	{ "Lo", +1, Lo_range16, 295, Lo_range32, 175 },
	{ "Lt", +1, Lt_range16, 10, 0, 0 },
	{ "Lu", +1, Lu_range16, 594, Lu_range32, 37 },
	{ "Lycian", +1, 0, 0, Lycian_range32, 1 },
	{ "Lydian", +1, 0, 0, Lydian_range32, 2 },
	{ "M", +1, M_range16, 187, M_range32, 88 },
	{ "Mahajani", +1, 0, 0, Mahajani_range32, 1 },
	{ "Makasar", +1, 0, 0, Makasar_range32, 1 },
	{ "Malayalam", +1, Malayalam_range16, 8, 0, 0 },
	{ "Mandaic", +1, Mandaic_range16, 2, 0, 0 },
	{ "Manichaean", +1, 0, 0, Manichaean_range32, 2 },
	{ "Marchen", +1, 0, 0, Marchen_range32, 3 },
	{ "Masaram_Gondi", +1, 0, 0, Masaram_Gondi_range32, 7 },
	{ "Mc", +1, Mc_range16, 110, Mc_range32, 56 },
	{ "Me", +1, Me_range16, 5, 0, 0 },
	{ "Medefaidrin", +1, 0, 0, Medefaidrin_range32, 1 },
	{ "Meetei_Mayek", +1, Meetei_Mayek_range16, 3, 0, 0 },
	{ "Mende_Kikakui", +1, 0, 0, Mende_Kikakui_range32, 2 },
	{ "Meroitic_Cursive", +1, 0, 0, Meroitic_Cursive_range32, 3 },
	{ "Meroitic_Hieroglyphs", +1, 0, 0, Meroitic_Hieroglyphs_range32, 1 },
	{ "Miao", +1, 0, 0, Miao_range32, 3 },
	{ "Mn", +1, Mn_range16, 208, Mn_range32, 105 },
	{ "Modi", +1, 0, 0, Modi_range32, 2 },
	{ "Mongolian", +1, Mongolian_range16, 6, Mongolian_range32, 1 },
	{ "Mro", +1, 0, 0, Mro_range32, 3 },
	{ "Multani", +1, 0, 0, Multani_range32, 5 },
	{ "Myanmar", +1, Myanmar_range16, 3, 0, 0 },
	{ "N", +1, N_range16, 67, N_range32, 58 },
	{ "Nabataean", +1, 0, 0, Nabataean_range32, 2 },
	{ "Nd", +1, Nd_range16, 37, Nd_range32, 20 },
	{ "New_Tai_Lue", +1, New_Tai_Lue_range16, 4, 0, 0 },
	{ "Newa", +1, 0, 0, Newa_range32, 3 },
	{ "Nko", +1, Nko_range16, 2, 0, 0 },
	{ "Nl", +1, Nl_range16, 7, Nl_range32, 5 },
	{ "No", +1, No_range16, 29, No_range32, 38 },
	{ "Nushu", +1, 0, 0, Nushu_range32, 2 },
	{ "Ogham", +1, Ogham_range16, 1, 0, 0 },
	{ "Ol_Chiki", +1, Ol_Chiki_range16, 1, 0, 0 },
	{ "Old_Hungarian", +1, 0, 0, Old_Hungarian_range32, 3 },
	{ "Old_Italic", +1, 0, 0, Old_Italic_range32, 2 },
	{ "Old_North_Arabian", +1, 0, 0, Old_North_Arabian_range32, 1 },
	{ "Old_Permic", +1, 0, 0, Old_Permic_range32, 1 },
	{ "Old_Persian", +1, 0, 0, Old_Persian_range32, 2 },
	{ "Old_Sogdian", +1, 0, 0, Old_Sogdian_range32, 1 },
	{ "Old_South_Arabian", +1, 0, 0, Old_South_Arabian_range32, 1 },
	{ "Old_Turkic", +1, 0, 0, Old_Turkic_range32, 1 },
	{ "Oriya", +1, Oriya_range16, 14, 0, 0 },
	{ "Osage", +1, 0, 0, Osage_range32, 2 },
	{ "Osmanya", +1, 0, 0, Osmanya_range32, 2 },
	{ "P", +1, P_range16, 130, P_range32, 48 },
	{ "Pahawh_Hmong", +1, 0, 0, Pahawh_Hmong_range32, 5 },
	{ "Palmyrene", +1, 0, 0, Palmyrene_range32, 1 },
	{ "Pau_Cin_Hau", +1, 0, 0, Pau_Cin_Hau_range32, 1 },
	{ "Pc", +1, Pc_range16, 6, 0, 0 },
	{ "Pd", +1, Pd_range16, 17, 0, 0 },
	{ "Pe", +1, Pe_range16, 72, 0, 0 },
	{ "Pf", +1, Pf_range16, 10, 0, 0 },
	{ "Phags_Pa", +1, Phags_Pa_range16, 1, 0, 0 },
	{ "Phoenician", +1, 0, 0, Phoenician_range32, 2 },
	{ "Pi", +1, Pi_range16, 11, 0, 0 },
	{ "Po", +1, Po_range16, 127, Po_range32, 48 },
	{ "Ps", +1, Ps_range16, 75, 0, 0 },
	{ "Psalter_Pahlavi", +1, 0, 0, Psalter_Pahlavi_range32, 3 },
	{ "Rejang", +1, Rejang_range16, 2, 0, 0 },
	{ "Runic", +1, Runic_range16, 2, 0, 0 },
	{ "S", +1, S_range16, 147, S_range32, 73 },
	{ "Samaritan", +1, Samaritan_range16, 2, 0, 0 },
	{ "Saurashtra", +1, Saurashtra_range16, 2, 0, 0 },
	{ "Sc", +1, Sc_range16, 18, Sc_range32, 1 },
	{ "Sharada", +1, 0, 0, Sharada_range32, 2 },
	{ "Shavian", +1, 0, 0, Shavian_range32, 1 },
	{ "Siddham", +1, 0, 0, Siddham_range32, 2 },
	{ "SignWriting", +1, 0, 0, SignWriting_range32, 3 },
	{ "Sinhala", +1, Sinhala_range16, 12, Sinhala_range32, 1 },
	{ "Sk", +1, Sk_range16, 28, Sk_range32, 1 },
	{ "Sm", +1, Sm_range16, 53, Sm_range32, 11 },
	{ "So", +1, So_range16, 112, So_range32, 62 },
	{ "Sogdian", +1, 0, 0, Sogdian_range32, 1 },
	{ "Sora_Sompeng", +1, 0, 0, Sora_Sompeng_range32, 2 },
	{ "Soyombo", +1, 0, 0, Soyombo_range32, 2 },
	{ "Sundanese", +1, Sundanese_range16, 2, 0, 0 },
	{ "Syloti_Nagri", +1, Syloti_Nagri_range16, 1, 0, 0 },
	{ "Syriac", +1, Syriac_range16, 4, 0, 0 },
	{ "Tagalog", +1, Tagalog_range16, 2, 0, 0 },
	{ "Tagbanwa", +1, Tagbanwa_range16, 3, 0, 0 },
	{ "Tai_Le", +1, Tai_Le_range16, 2, 0, 0 },
	{ "Tai_Tham", +1, Tai_Tham_range16, 5, 0, 0 },
	{ "Tai_Viet", +1, Tai_Viet_range16, 2, 0, 0 },
	{ "Takri", +1, 0, 0, Takri_range32, 2 },
	{ "Tamil", +1, Tamil_range16, 16, 0, 0 },
	{ "Tangut", +1, 0, 0, Tangut_range32, 3 },
	{ "Telugu", +1, Telugu_range16, 12, 0, 0 },
	{ "Thaana", +1, Thaana_range16, 1, 0, 0 },
	{ "Thai", +1, Thai_range16, 2, 0, 0 },
	{ "Tibetan", +1, Tibetan_range16, 7, 0, 0 },
	{ "Tifinagh", +1, Tifinagh_range16, 3, 0, 0 },
	{ "Tirhuta", +1, 0, 0, Tirhuta_range32, 2 },
	{ "Ugaritic", +1, 0, 0, Ugaritic_range32, 2 },
	{ "Vai", +1, Vai_range16, 1, 0, 0 },
	{ "Warang_Citi", +1, 0, 0, Warang_Citi_range32, 2 },
	{ "Yi", +1, Yi_range16, 2, 0, 0 },
	{ "Z", +1, Z_range16, 8, 0, 0 },
	{ "Zanabazar_Square", +1, 0, 0, Zanabazar_Square_range32, 1 },
	{ "Zl", +1, Zl_range16, 1, 0, 0 },
	{ "Zp", +1, Zp_range16, 1, 0, 0 },
	{ "Zs", +1, Zs_range16, 7, 0, 0 },
};
const int num_unicode_groups = 184;


}  // namespace duckdb_re2




// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

/*
 * The authors of this software are Rob Pike and Ken Thompson.
 *              Copyright (c) 2002 by Lucent Technologies.
 * Permission to use, copy, modify, and distribute this software for any
 * purpose without fee is hereby granted, provided that this entire notice
 * is included in all copies of any software which is or includes a copy
 * or modification of this software and in all copies of the supporting
 * documentation for such software.
 * THIS SOFTWARE IS BEING PROVIDED "AS IS", WITHOUT ANY EXPRESS OR IMPLIED
 * WARRANTY.  IN PARTICULAR, NEITHER THE AUTHORS NOR LUCENT TECHNOLOGIES MAKE ANY
 * REPRESENTATION OR WARRANTY OF ANY KIND CONCERNING THE MERCHANTABILITY
 * OF THIS SOFTWARE OR ITS FITNESS FOR ANY PARTICULAR PURPOSE.
 */

#include <stdarg.h>
#include <string.h>



namespace duckdb_re2 {

enum
{
	Bit1	= 7,
	Bitx	= 6,
	Bit2	= 5,
	Bit3	= 4,
	Bit4	= 3,
	Bit5	= 2, 

	T1	= ((1<<(Bit1+1))-1) ^ 0xFF,	/* 0000 0000 */
	Tx	= ((1<<(Bitx+1))-1) ^ 0xFF,	/* 1000 0000 */
	T2	= ((1<<(Bit2+1))-1) ^ 0xFF,	/* 1100 0000 */
	T3	= ((1<<(Bit3+1))-1) ^ 0xFF,	/* 1110 0000 */
	T4	= ((1<<(Bit4+1))-1) ^ 0xFF,	/* 1111 0000 */
	T5	= ((1<<(Bit5+1))-1) ^ 0xFF,	/* 1111 1000 */

	Rune1	= (1<<(Bit1+0*Bitx))-1,		/* 0000 0000 0111 1111 */
	Rune2	= (1<<(Bit2+1*Bitx))-1,		/* 0000 0111 1111 1111 */
	Rune3	= (1<<(Bit3+2*Bitx))-1,		/* 1111 1111 1111 1111 */
	Rune4	= (1<<(Bit4+3*Bitx))-1,
                                        /* 0001 1111 1111 1111 1111 1111 */

	Maskx	= (1<<Bitx)-1,			/* 0011 1111 */
	Testx	= Maskx ^ 0xFF,			/* 1100 0000 */

	Bad	= Runeerror,
};

int
chartorune(Rune *rune, const char *str)
{
	int c, c1, c2, c3;
	long l;

	/*
	 * one character sequence
	 *	00000-0007F => T1
	 */
	c = *(unsigned char*)str;
	if(c < Tx) {
		*rune = c;
		return 1;
	}

	/*
	 * two character sequence
	 *	0080-07FF => T2 Tx
	 */
	c1 = *(unsigned char*)(str+1) ^ Tx;
	if(c1 & Testx)
		goto bad;
	if(c < T3) {
		if(c < T2)
			goto bad;
		l = ((c << Bitx) | c1) & Rune2;
		if(l <= Rune1)
			goto bad;
		*rune = l;
		return 2;
	}

	/*
	 * three character sequence
	 *	0800-FFFF => T3 Tx Tx
	 */
	c2 = *(unsigned char*)(str+2) ^ Tx;
	if(c2 & Testx)
		goto bad;
	if(c < T4) {
		l = ((((c << Bitx) | c1) << Bitx) | c2) & Rune3;
		if(l <= Rune2)
			goto bad;
		*rune = l;
		return 3;
	}

	/*
	 * four character sequence (21-bit value)
	 *	10000-1FFFFF => T4 Tx Tx Tx
	 */
	c3 = *(unsigned char*)(str+3) ^ Tx;
	if (c3 & Testx)
		goto bad;
	if (c < T5) {
		l = ((((((c << Bitx) | c1) << Bitx) | c2) << Bitx) | c3) & Rune4;
		if (l <= Rune3)
			goto bad;
		*rune = l;
		return 4;
	}

	/*
	 * Support for 5-byte or longer UTF-8 would go here, but
	 * since we don't have that, we'll just fall through to bad.
	 */

	/*
	 * bad decoding
	 */
bad:
	*rune = Bad;
	return 1;
}

int
runetochar(char *str, const Rune *rune)
{
	/* Runes are signed, so convert to unsigned for range check. */
	unsigned long c;

	/*
	 * one character sequence
	 *	00000-0007F => 00-7F
	 */
	c = *rune;
	if(c <= Rune1) {
		str[0] = static_cast<char>(c);
		return 1;
	}

	/*
	 * two character sequence
	 *	0080-07FF => T2 Tx
	 */
	if(c <= Rune2) {
		str[0] = T2 | static_cast<char>(c >> 1*Bitx);
		str[1] = Tx | (c & Maskx);
		return 2;
	}

	/*
	 * If the Rune is out of range, convert it to the error rune.
	 * Do this test here because the error rune encodes to three bytes.
	 * Doing it earlier would duplicate work, since an out of range
	 * Rune wouldn't have fit in one or two bytes.
	 */
	if (c > Runemax)
		c = Runeerror;

	/*
	 * three character sequence
	 *	0800-FFFF => T3 Tx Tx
	 */
	if (c <= Rune3) {
		str[0] = T3 | static_cast<char>(c >> 2*Bitx);
		str[1] = Tx | ((c >> 1*Bitx) & Maskx);
		str[2] = Tx | (c & Maskx);
		return 3;
	}

	/*
	 * four character sequence (21-bit value)
	 *     10000-1FFFFF => T4 Tx Tx Tx
	 */
	str[0] = T4 | static_cast<char>(c >> 3*Bitx);
	str[1] = Tx | ((c >> 2*Bitx) & Maskx);
	str[2] = Tx | ((c >> 1*Bitx) & Maskx);
	str[3] = Tx | (c & Maskx);
	return 4;
}

int
runelen(Rune rune)
{
	char str[10];

	return runetochar(str, &rune);
}

int
fullrune(const char *str, int n)
{
	if (n > 0) {
		int c = *(unsigned char*)str;
		if (c < Tx)
			return 1;
		if (n > 1) {
			if (c < T3)
				return 1;
			if (n > 2) {
				if (c < T4 || n > 3)
					return 1;
			}
		}
	}
	return 0;
}


int
utflen(const char *s)
{
	int c;
	long n;
	Rune rune;

	n = 0;
	for(;;) {
		c = *(unsigned char*)s;
		if(c < Runeself) {
			if(c == 0)
				return n;
			s++;
		} else
			s += chartorune(&rune, s);
		n++;
	}
	return 0;
}

char*
utfrune(const char *s, Rune c)
{
	long c1;
	Rune r;
	int n;

	if(c < Runesync)		/* not part of utf sequence */
		return strchr((char*)s, c);

	for(;;) {
		c1 = *(unsigned char*)s;
		if(c1 < Runeself) {	/* one byte rune */
			if(c1 == 0)
				return 0;
			if(c1 == c)
				return (char*)s;
			s++;
			continue;
		}
		n = chartorune(&r, s);
		if(r == c)
			return (char*)s;
		s += n;
	}
	return 0;
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END


// LICENSE_CHANGE_BEGIN
// The following code up to LICENSE_CHANGE_END is subject to THIRD PARTY LICENSE #6
// See the end of this file for a list

// Copyright 1999-2005 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include <stdarg.h>
#include <stdio.h>



#ifdef _WIN32
#define snprintf _snprintf
#define vsnprintf _vsnprintf
#endif

namespace duckdb_re2 {

// ----------------------------------------------------------------------
// CEscapeString()
//    Copies 'src' to 'dest', escaping dangerous characters using
//    C-style escape sequences.  'src' and 'dest' should not overlap.
//    Returns the number of bytes written to 'dest' (not including the \0)
//    or (size_t)-1 if there was insufficient space.
// ----------------------------------------------------------------------
static size_t CEscapeString(const char* src, size_t src_len,
                            char* dest, size_t dest_len) {
  const char* src_end = src + src_len;
  size_t used = 0;

  for (; src < src_end; src++) {
    if (dest_len - used < 2)   // space for two-character escape
      return (size_t)-1;

    unsigned char c = *src;
    switch (c) {
      case '\n': dest[used++] = '\\'; dest[used++] = 'n';  break;
      case '\r': dest[used++] = '\\'; dest[used++] = 'r';  break;
      case '\t': dest[used++] = '\\'; dest[used++] = 't';  break;
      case '\"': dest[used++] = '\\'; dest[used++] = '\"'; break;
      case '\'': dest[used++] = '\\'; dest[used++] = '\''; break;
      case '\\': dest[used++] = '\\'; dest[used++] = '\\'; break;
      default:
        // Note that if we emit \xNN and the src character after that is a hex
        // digit then that digit must be escaped too to prevent it being
        // interpreted as part of the character code by C.
        if (c < ' ' || c > '~') {
          if (dest_len - used < 5)   // space for four-character escape + \0
            return (size_t)-1;
          snprintf(dest + used, 5, "\\%03o", c);
          used += 4;
        } else {
          dest[used++] = c; break;
        }
    }
  }

  if (dest_len - used < 1)   // make sure that there is room for \0
    return (size_t)-1;

  dest[used] = '\0';   // doesn't count towards return value though
  return used;
}

// ----------------------------------------------------------------------
// CEscape()
//    Copies 'src' to result, escaping dangerous characters using
//    C-style escape sequences.  'src' and 'dest' should not overlap.
// ----------------------------------------------------------------------
std::string CEscape(const StringPiece& src) {
  const size_t dest_len = src.size() * 4 + 1; // Maximum possible expansion
  char* dest = new char[dest_len];
  const size_t used = CEscapeString(src.data(), src.size(),
                                    dest, dest_len);
  std::string s = std::string(dest, used);
  delete[] dest;
  return s;
}

void PrefixSuccessor(std::string* prefix) {
  // We can increment the last character in the string and be done
  // unless that character is 255, in which case we have to erase the
  // last character and increment the previous character, unless that
  // is 255, etc. If the string is empty or consists entirely of
  // 255's, we just return the empty string.
  while (!prefix->empty()) {
    char& c = prefix->back();
    if (c == '\xff') {  // char literal avoids signed/unsigned.
      prefix->pop_back();
    } else {
      ++c;
      break;
    }
  }
}

static void StringAppendV(std::string* dst, const char* format, va_list ap) {
  // First try with a small fixed size buffer
  char space[1024];

  // It's possible for methods that use a va_list to invalidate
  // the data in it upon use.  The fix is to make a copy
  // of the structure before using it and use that copy instead.
  va_list backup_ap;
  va_copy(backup_ap, ap);
  int result = vsnprintf(space, sizeof(space), format, backup_ap);
  va_end(backup_ap);

  if ((result >= 0) && (static_cast<size_t>(result) < sizeof(space))) {
    // It fit
    dst->append(space, result);
    return;
  }

  // Repeatedly increase buffer size until it fits
  int length = sizeof(space);
  while (true) {
    if (result < 0) {
      // Older behavior: just try doubling the buffer size
      length *= 2;
    } else {
      // We need exactly "result+1" characters
      length = result+1;
    }
    char* buf = new char[length];

    // Restore the va_list before we use it again
    va_copy(backup_ap, ap);
    result = vsnprintf(buf, length, format, backup_ap);
    va_end(backup_ap);

    if ((result >= 0) && (result < length)) {
      // It fit
      dst->append(buf, result);
      delete[] buf;
      return;
    }
    delete[] buf;
  }
}

std::string StringPrintf(const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  std::string result;
  StringAppendV(&result, format, ap);
  va_end(ap);
  return result;
}

void SStringPrintf(std::string* dst, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  dst->clear();
  StringAppendV(dst, format, ap);
  va_end(ap);
}

void StringAppendF(std::string* dst, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  StringAppendV(dst, format, ap);
  va_end(ap);
}

}  // namespace duckdb_re2


// LICENSE_CHANGE_END
