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

#include <numeric>

#include <folly/container/F14Map.h>

#include "velox/common/base/Exceptions.h"
#include "velox/type/StringView.h"

namespace facebook::velox::functions {

/// Data structure to approximately compute the top frequent values from a large
/// stream.
///
/// The accuracy of the approximation can be controlled by `capacity`, which is
/// the number of elements retained in memory for the computation.  Larger
/// `capacity` uses more memory and gives more accurate results.
///
/// See
/// https://www.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop-kElementsInDataStreams.pdf
/// for more details.
///
/// Note that the memory allocated by the custom allocator must be
/// aligned on 16-byte boundary to work with F14 map.
template <typename T, typename Allocator = std::allocator<T>>
struct ApproxMostFrequentStreamSummary {
  explicit ApproxMostFrequentStreamSummary(const Allocator& = {});

  void setCapacity(int);

  /// Add one or multiple new values to the summary.
  void insert(T value, int64_t count = 1);

  /// Get the top `k` frequent elements with their estimated counts, sorted from
  /// most hits to least hits.
  std::vector<std::pair<T, int64_t>> topK(int k) const;

  /// Same as topK(int), but write the result (up to k pairs) to pre-allocated
  /// memory `out`.
  void topK(int k, std::pair<T, int64_t>* out) const;

  /// Calculate the size needed for serialization.
  size_t serializedByteSize() const;

  /// Serialize the summary into bytes.
  /// @param out Pre-allocated memory at least serializedByteSize() in size
  void serialize(char* out) const;

  /// Merge this summary with values from another serialized summary.
  void mergeSerialized(const char* bytes);

  /// Return the number of distinct values currently being tracked.
  int size() const;

  /// Return the pointer to values data.  The number of values equals to size().
  const T* values() const {
    return values_.data();
  }

  /// Return the pointer to counts data.  The number of counts equals to size().
  const int64_t* counts() const {
    return counts_.data();
  }

 private:
  template <typename U>
  using RebindAlloc =
      typename std::allocator_traits<Allocator>::template rebind_alloc<U>;

  int heapCompare(int i, int j) const;
  void percolateUp(int position);
  void percolateDown(int position);

  int capacity() const {
    return capacity_;
  }

  int capacity_ = 0;
  int64_t currentGeneration_ = 0;
  std::vector<T, Allocator> values_;
  std::vector<int64_t, RebindAlloc<int64_t>> counts_;
  std::vector<int64_t, RebindAlloc<int64_t>> generations_;
  std::vector<int32_t, RebindAlloc<int32_t>> heap_;

  folly::F14FastMap<
      T,
      int32_t,
      std::hash<T>,
      std::equal_to<T>,
      RebindAlloc<std::pair<const T, int32_t>>>
      indices_;

  std::vector<int32_t, RebindAlloc<int32_t>> heapIndices_;
};

template <typename T, typename A>
ApproxMostFrequentStreamSummary<T, A>::ApproxMostFrequentStreamSummary(
    const A& allocator)
    : values_(allocator),
      counts_(RebindAlloc<int64_t>(allocator)),
      generations_(RebindAlloc<int64_t>(allocator)),
      heap_(RebindAlloc<int32_t>(allocator)),
      indices_(RebindAlloc<std::pair<const T, int32_t>>(allocator)),
      heapIndices_(RebindAlloc<int32_t>(allocator)) {}

template <typename T, typename A>
void ApproxMostFrequentStreamSummary<T, A>::setCapacity(int capacity) {
  VELOX_CHECK_GT(capacity, 0);
  if (capacity_ == 0) {
    capacity_ = capacity;
  } else {
    VELOX_CHECK_EQ(capacity, capacity_);
  }
}

template <typename T, typename A>
int ApproxMostFrequentStreamSummary<T, A>::size() const {
  VELOX_DCHECK_EQ(values_.size(), counts_.size());
  VELOX_DCHECK_EQ(values_.size(), generations_.size());
  VELOX_DCHECK_EQ(values_.size(), heap_.size());
  VELOX_DCHECK_EQ(values_.size(), heapIndices_.size());
  return values_.size();
}

template <typename T, typename A>
int ApproxMostFrequentStreamSummary<T, A>::heapCompare(int i, int j) const {
  if (int ans = counts_[i] - counts_[j]; ans != 0) {
    return ans;
  }
  // When the counts are same, we want to consider the previously generated
  // value as minimum to prefer it over newly generated value with same count
  // when we need to remove min.
  return generations_[i] - generations_[j];
}

template <typename T, typename A>
void ApproxMostFrequentStreamSummary<T, A>::insert(T value, int64_t count) {
  if (auto it = indices_.find(value); it != indices_.end()) {
    // The value to be counted is currently being tracked, we just need to
    // increase the counter.
    int i = it->second;
    counts_[i] += count;
    generations_[i] = ++currentGeneration_;
    percolateDown(heapIndices_[i]);
    return;
  }
  if (size() < capacity()) {
    // There is still room available, just insert the value.
    int i = size();
    values_.push_back(value);
    counts_.push_back(count);
    generations_.push_back(++currentGeneration_);
    indices_.emplace(value, i);
    heapIndices_.push_back(i);
    heap_.push_back(i);
    percolateUp(i);
    return;
  }
  // Replace the element with least hits.
  VELOX_DCHECK(!heap_.empty());
  int i = heap_[0];
  indices_.erase(values_[i]);
  values_[i] = value;
  counts_[i] += count;
  generations_[i] = ++currentGeneration_;
  indices_.emplace(value, i);
  percolateDown(0);
}

template <typename T, typename A>
void ApproxMostFrequentStreamSummary<T, A>::percolateUp(int pos) {
  while (pos > 0) {
    int parent = (pos - 1) / 2;
    if (heapCompare(heap_[pos], heap_[parent]) >= 0) {
      break;
    }
    std::swap(heap_[pos], heap_[parent]);
    heapIndices_[heap_[pos]] = pos;
    pos = parent;
  }
  heapIndices_[heap_[pos]] = pos;
}

template <typename T, typename A>
void ApproxMostFrequentStreamSummary<T, A>::percolateDown(int pos) {
  for (;;) {
    int left = 2 * pos + 1;
    if (left >= size()) {
      break;
    }
    int child = left;
    if (int right = left + 1;
        right < size() && heapCompare(heap_[right], heap_[left]) < 0) {
      child = right;
    }
    if (heapCompare(heap_[pos], heap_[child]) <= 0) {
      break;
    }
    std::swap(heap_[pos], heap_[child]);
    heapIndices_[heap_[pos]] = pos;
    pos = child;
  }
  heapIndices_[heap_[pos]] = pos;
}

template <typename T, typename A>
void ApproxMostFrequentStreamSummary<T, A>::topK(
    int k,
    std::pair<T, int64_t>* out) const {
  VELOX_CHECK(k >= 0);
  k = std::min(k, size());
  if (k == 0) {
    return;
  }
  // Reuse memory provided by user, building a second heap to track `k` greatest
  // elements.
  auto posEnd = reinterpret_cast<int32_t*>(out + k);
  auto posBeg = posEnd - k;
  static_assert(sizeof(std::pair<T, int64_t>) >= sizeof(int32_t));
  auto gt = [&](auto i, auto j) { return heapCompare(i, j) > 0; };
  for (int i = 0; i < size(); ++i) {
    if (i < k) {
      posBeg[i] = i;
      std::push_heap(posBeg, posBeg + i + 1, gt);
    } else if (heapCompare(i, *posBeg) > 0) {
      std::pop_heap(posBeg, posEnd, gt);
      posBeg[k - 1] = i;
      std::push_heap(posBeg, posEnd, gt);
    }
  }
  std::sort(posBeg, posEnd, gt);
  for (auto it = posBeg; it != posEnd; ++it) {
    auto i = *it;
    out->first = values_[i];
    out->second = counts_[i];
    ++out;
  }
}

template <typename T, typename A>
std::vector<std::pair<T, int64_t>> ApproxMostFrequentStreamSummary<T, A>::topK(
    int k) const {
  VELOX_CHECK(k >= 0);
  k = std::min(k, size());
  std::vector<std::pair<T, int64_t>> ans(k);
  topK(k, ans.data());
  return ans;
}

template <typename T, typename A>
size_t ApproxMostFrequentStreamSummary<T, A>::serializedByteSize() const {
  size_t ans = sizeof(int32_t) + sizeof(T) * size() + sizeof(int64_t) * size();
  if constexpr (std::is_same_v<T, StringView>) {
    for (auto& v : values_) {
      if (!v.isInline()) {
        ans += v.size();
      }
    }
  }
  return ans;
}

// The serialized form contains:
//   1. Number of the elements
//   2. Values data
//   3. Counts data
//   4. If the value type is StringView, the actual non-inlined string data
template <typename T, typename A>
void ApproxMostFrequentStreamSummary<T, A>::serialize(char* out) const {
  auto cur = out;
  *reinterpret_cast<int32_t*>(cur) = size();
  cur += sizeof(int32_t);
  auto byteSize = sizeof(T) * size();
  memcpy(cur, values_.data(), byteSize);
  cur += byteSize;
  byteSize = sizeof(int64_t) * size();
  memcpy(cur, counts_.data(), byteSize);
  cur += byteSize;
  if constexpr (std::is_same_v<T, StringView>) {
    for (auto& v : values_) {
      if (!v.isInline()) {
        memcpy(cur, v.data(), v.size());
        cur += v.size();
      }
    }
  }
  VELOX_DCHECK_EQ(cur - out, serializedByteSize());
}

template <typename T, typename A>
void ApproxMostFrequentStreamSummary<T, A>::mergeSerialized(const char* other) {
  auto size = *reinterpret_cast<const int32_t*>(other);
  other += sizeof size;
  auto values = reinterpret_cast<const T*>(other);
  other += sizeof(T) * size;
  auto counts = reinterpret_cast<const int64_t*>(other);
  if constexpr (std::is_same_v<T, StringView>) {
    other += sizeof(int64_t) * size;
  }
  for (int i = 0; i < size; ++i) {
    auto v = values[i];
    if constexpr (std::is_same_v<T, StringView>) {
      if (!v.isInline()) {
        v = {other, v.size()};
        other += v.size();
      }
    }
    insert(v, counts[i]);
  }
}

} // namespace facebook::velox::functions
