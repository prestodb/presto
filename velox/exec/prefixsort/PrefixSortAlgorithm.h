/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <cstdint>
#include <functional>
#include <memory>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SimdUtil.h"

namespace facebook::velox::exec::prefixsort {

namespace detail {
/// Provides LegacyRandomAccessIterator but not ValueSwappable semantic
/// iterator for PrefixSort`s sort algorithm, see:
/// https://en.cppreference.com/w/cpp/algorithm/sort
/// https://en.cppreference.com/w/cpp/named_req/RandomAccessIterator
/// https://en.cppreference.com/w/cpp/named_req/ValueSwappable
/// The difference is operator*(), the LegacyInputIterator return "reference,
/// convertible to value_type", see:
/// https://en.cppreference.com/w/cpp/named_req/InputIterator
/// In contrast, PrefixSortIterator returns char* point to the prefix data
/// (not a "value_type" but a memory buffer store normalized keys) for memcmp
/// and memcpy operators, means not ValueSwappable.
/// This is also the reason we can not use std::sort directly, and implement
/// quickSort in PrefixSortRunner.
class PrefixSortIterator {
 public:
  PrefixSortIterator(char* prefix, const uint64_t entrySize)
      : entrySize_(entrySize), prefix_(prefix) {}

  PrefixSortIterator(const PrefixSortIterator& other)
      : entrySize_(other.entrySize_), prefix_(other.prefix_) {}

  // Inline functions should be placed in header file to avoid link error.
  FOLLY_ALWAYS_INLINE char* operator*() const {
    return prefix_;
  }

  FOLLY_ALWAYS_INLINE PrefixSortIterator& operator++() {
    prefix_ += entrySize_;
    return *this;
  }

  FOLLY_ALWAYS_INLINE PrefixSortIterator& operator--() {
    prefix_ -= entrySize_;
    return *this;
  }

  FOLLY_ALWAYS_INLINE PrefixSortIterator operator++(int) {
    const auto tmp = *this;
    prefix_ += entrySize_;
    return tmp;
  }

  FOLLY_ALWAYS_INLINE PrefixSortIterator operator--(int) {
    const auto tmp = *this;
    prefix_ -= entrySize_;
    return tmp;
  }

  FOLLY_ALWAYS_INLINE PrefixSortIterator operator+(const uint64_t i) const {
    auto result = *this;
    result.prefix_ += i * entrySize_;
    return result;
  }

  FOLLY_ALWAYS_INLINE PrefixSortIterator operator-(const uint64_t i) const {
    PrefixSortIterator result = *this;
    result.prefix_ -= i * entrySize_;
    return result;
  }

  FOLLY_ALWAYS_INLINE PrefixSortIterator& operator=(
      const PrefixSortIterator& other) {
    prefix_ = other.prefix_;
    return *this;
  }

  FOLLY_ALWAYS_INLINE uint64_t
  operator-(const PrefixSortIterator& other) const {
    return (prefix_ - other.prefix_) / other.entrySize_;
  }

  FOLLY_ALWAYS_INLINE bool operator<(const PrefixSortIterator& other) const {
    return prefix_ < other.prefix_;
  }

  FOLLY_ALWAYS_INLINE bool operator>(const PrefixSortIterator& other) const {
    return prefix_ > other.prefix_;
  }

  FOLLY_ALWAYS_INLINE bool operator>=(const PrefixSortIterator& other) const {
    return prefix_ >= other.prefix_;
  }

  FOLLY_ALWAYS_INLINE bool operator<=(const PrefixSortIterator& other) const {
    return prefix_ <= other.prefix_;
  }

  FOLLY_ALWAYS_INLINE bool operator==(const PrefixSortIterator& other) const {
    return prefix_ == other.prefix_;
  }

  FOLLY_ALWAYS_INLINE bool operator!=(const PrefixSortIterator& other) const {
    return prefix_ != other.prefix_;
  }

 private:
  const uint64_t entrySize_;
  char* prefix_;
};
} // namespace detail

/// Provides methods (mostly required by the sort
/// algorithm) for PrefixSort. Maintains the buffer for swap operations and
/// some necessary context information such as entrySize.
class PrefixSortRunner {
 public:
  /// @param swapBuffer The buffer must be at least entrySize bytes long.
  PrefixSortRunner(uint64_t entrySize, char* swapBuffer)
      : entrySize_(entrySize), swapBuffer_(swapBuffer) {
    VELOX_CHECK_NOT_NULL(swapBuffer_);
  }

  // Within quickSort, when the input data length < kSmallSort, use insert-sort
  // in place of quick-sort.
  // With finding a central element, there are extra comparisons. While
  // this is cheap for large arrays, it is expensive for small arrays.
  // Therefore, chooses the middle element of smaller arrays (=kSmallSort),
  // the median of the first, middle and last elements of a mid-sized array
  // (> kSmallSort and < kMediumSort),
  // and the pseudo-median of nine evenly spaced elements of a large
  // array(> kMediumSort).
  // The two threshold values are given in the reference paper "Engineering a
  // Sort Function".
  static const int kSmallSort = 7;
  static const int kMediumSort = 40;

  template <typename TCompare>
  void quickSort(char* start, char* end, TCompare compare) const {
    quickSort(
        detail::PrefixSortIterator(start, entrySize_),
        detail::PrefixSortIterator(end, entrySize_),
        compare);
  }

  /// For testing only.
  template <typename TCompare>
  FOLLY_ALWAYS_INLINE static char* testingMedian3(
      char* a,
      char* b,
      char* c,
      const uint64_t entrySize,
      TCompare cmp) {
    return *median3(
        detail::PrefixSortIterator(a, entrySize),
        detail::PrefixSortIterator(b, entrySize),
        detail::PrefixSortIterator(c, entrySize),
        cmp);
  }

 private:
  FOLLY_ALWAYS_INLINE void swap(
      const detail::PrefixSortIterator& lhs,
      const detail::PrefixSortIterator& rhs) const {
    simd::memcpy(swapBuffer_, *lhs, entrySize_);
    simd::memcpy(*lhs, *rhs, entrySize_);
    simd::memcpy(*rhs, swapBuffer_, entrySize_);
  }

  FOLLY_ALWAYS_INLINE void rangeSwap(
      const detail::PrefixSortIterator& start1,
      const detail::PrefixSortIterator& start2,
      uint64_t length) const {
    for (uint64_t i = 0; i < length; i++) {
      swap(start1 + i, start2 + i);
    }
  }

  // Calculate which one has median of three input iterators.
  // The compare logic is (the symbol '<' and '>' means value compare result):
  //            ---------cmp(a, b)--------
  //           |                          |
  //        a<b|                      a>=b|
  //       cmp(b, c)                  cmp(b, c)
  //       |       |                  |       |
  //    b<c|   b>=c|               b>c|   b<=c|
  //   a<=b<=c  cmp(a, c)        c<=b<=a   cmp(a, c)
  //             |      |                   |      |
  //          a<c|  a>=c|                a>c|  a<=c|
  //         a<=c<=b  c<=a<=b           b<=c<=a  b<=a<=c
  // case a<=b=<c return b
  // case a<=c=<b return c
  // case c<=a<=b return a
  // case c<=b<=a return b
  // case b<=c<=a return c
  // case b<=a<=c return a
  template <typename TCompare>
  FOLLY_ALWAYS_INLINE static detail::PrefixSortIterator median3(
      const detail::PrefixSortIterator& a,
      const detail::PrefixSortIterator& b,
      const detail::PrefixSortIterator& c,
      TCompare cmp) {
    return cmp(*a, *b) < 0 ? (cmp(*b, *c) < 0       ? b
                                  : cmp(*a, *c) < 0 ? c
                                                    : a)
                           : (cmp(*b, *c) > 0       ? b
                                  : cmp(*a, *c) > 0 ? c
                                                    : a);
  }

  template <typename TCompare>
  FOLLY_ALWAYS_INLINE void insertSort(
      const detail::PrefixSortIterator& start,
      const detail::PrefixSortIterator& end,
      TCompare compare) const {
    for (auto i = start; i < end; ++i) {
      for (auto j = i; j > start && (compare(*(j - 1), *j) > 0); --j) {
        swap(j, j - 1);
      }
    }
  }

  // Sort prefix data in range [start, end) using quick-sort.
  // Algorithm implementation reference:
  // 1. J.L.Bentley and M.McIlroy’s paper:
  // “Engineering a Sort Function”
  // 2. Presto`s quickSort implementation
  //
  // Note:The compare parameter should be defined as a template type parameter
  // like std::sort, which is much faster than using a function type, because
  // of this, quickSort`s implementations also need to be placed in the header
  // file.
  // TCompare is a compare function : int compare(char*, char*).
  template <typename TCompare>
  void quickSort(
      const detail::PrefixSortIterator& start,
      const detail::PrefixSortIterator& end,
      TCompare compare) const {
    VELOX_CHECK(end >= start, "Invalid sort range.");
    const uint64_t len = end - start;

    // Insertion sort on smallest arrays
    if (len < kSmallSort) {
      insertSort(start, end, compare);
      return;
    }

    // Choose a partition element: m.

    // For small arrays, median is the middle element.
    auto m = start + len / 2;
    if (len > kSmallSort) {
      auto l = start;
      auto n = end - 1;
      // For big arrays, median is the pseudo-median of nine evenly spaced
      // elements.
      if (len > kMediumSort) {
        const uint64_t s = len / 8;
        l = median3(l, l + s, l + 2 * s, compare);
        m = median3(m - s, m, m + s, compare);
        n = median3(n - 2 * s, n - s, n, compare);
      }
      // For mid-sized array, median is the median of the first, middle and last
      // elements.
      m = median3(l, m, n, compare);
    }
    auto a = start;
    auto b = a;
    auto c = end - 1;
    auto d = c;

    // Establish invariant: v* (<v)* (>v)* v*,
    // v* means elements that value equal to partition element v,
    // (<v)* means elements that value little than partition element v,
    // (>v)* means elements that value bigger than partition element v.
    while (true) {
      int comparison;
      while (b <= c && ((comparison = compare(*b, *m)) <= 0)) {
        if (comparison == 0) {
          if (a == m) {
            m = b;
          } else if (b == m) {
            m = a;
          }
          swap(a++, b);
        }
        b++;
      }
      while (c >= b && ((comparison = compare(*c, *m)) >= 0)) {
        if (comparison == 0) {
          if (c == m) {
            m = d;
          } else if (d == m) {
            m = c;
          }
          swap(c, d--);
        }
        c--;
      }
      if (b > c) {
        break;
      }
      if (b == m) {
        m = d;
      }
      // There is a useless assignment statement in the implementation of presto
      // when c == m, remove it here,
      // see: https://github.com/prestodb/presto/blob
      // /542beb8d89fa86feb15008611f8900f0f316ef5f/
      // presto-main/src/main/java/com/facebook/presto/operator
      // /PagesIndexOrdering.java
      swap(b++, c--);
    }

    // Swap partition elements back end middle
    uint64_t s;
    auto n = end;
    s = std::min(a - start, b - a);
    rangeSwap(start, b - s, s);
    s = std::min(d - c, n - d - 1);
    rangeSwap(b, n - s, s);

    // Recursively sort non-partition-elements
    s = b - a;
    if (s > 1) {
      quickSort(start, start + s, compare);
    }
    s = d - c;
    if (s > 1) {
      quickSort(n - s, n, compare);
    }
  }

  const uint64_t entrySize_;
  char* const swapBuffer_;
};
} // namespace facebook::velox::exec::prefixsort
