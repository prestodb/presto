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

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/IndexedPriorityQueue.h"
#include "velox/type/StringView.h"

#include <folly/Bits.h>

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
  /// memory `values` and `counts`.
  void topK(int k, T* values, int64_t* counts) const;

  /// Calculate the size needed for serialization.
  size_t serializedByteSize() const;

  /// Serialize the summary into bytes.  The serialzation should be always
  /// backward compatible, meaning newer code should always be able to read
  /// serialization from old version.  Essentially this means the serialization
  /// format should not change.
  ///
  /// @param out Pre-allocated memory at least serializedByteSize() in size
  void serialize(char* out) const;

  /// Merge this summary with values from another serialized summary.
  void mergeSerialized(const char* bytes);

  /// Merge this summary with values from another deserialized summary.
  /// This behaves the same as if serializing `other` and calling
  /// `mergeSerialized`, except:
  /// - StringView would copied from the `other` data structure's StringViews
  ///   as opposed to `const char* bytes`
  ///
  /// Prefer `mergeSerialized`. This is more inefficient as it requires us to
  /// deserialize `other` first, and then copy the values; whereas
  /// `mergeSerialized` will directly merge the values from the serialized bytes
  void merge(const ApproxMostFrequentStreamSummary<T, Allocator>& other);

  /// Return the number of distinct values currently being tracked.
  int size() const;

  // Return the maximum number of distinct values that can be tracked.
  int capacity() const;

  /// Return the pointer to values data.  The number of values equals to size().
  const T* values() const {
    return queue_.values();
  }

  /// Return the pointer to counts data.  The number of counts equals to size().
  const int64_t* counts() const {
    return queue_.priorities();
  }

  bool contains(T value) const {
    return queue_.getValueIndex(value).has_value();
  }

 private:
  int capacity_ = 0;
  IndexedPriorityQueue<T, false, Allocator> queue_;
};

template <typename T, typename A>
ApproxMostFrequentStreamSummary<T, A>::ApproxMostFrequentStreamSummary(
    const A& allocator)
    : queue_(allocator) {}

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
  return queue_.size();
}

template <typename T, typename A>
int ApproxMostFrequentStreamSummary<T, A>::capacity() const {
  return capacity_;
}

template <typename T, typename A>
void ApproxMostFrequentStreamSummary<T, A>::insert(T value, int64_t count) {
  auto index = queue_.getValueIndex(value);
  if (index.has_value()) {
    auto oldCount = queue_.priorities()[*index];
    queue_.updatePriority(*index, oldCount + count);
  } else if (size() < capacity_) {
    queue_.addNewValue(value, count);
  } else {
    auto oldCount = queue_.topPriority();
    queue_.replaceTop(value, oldCount + count);
  }
}

template <typename T, typename A>
void ApproxMostFrequentStreamSummary<T, A>::topK(
    int k,
    T* values,
    int64_t* counts) const {
  VELOX_CHECK(k >= 0);
  k = std::min(k, size());
  if (k == 0) {
    return;
  }
  // Reuse memory provided by user, building a second heap to track `k` greatest
  // elements.
  auto posEnd = reinterpret_cast<int32_t*>(counts + k);
  auto posBeg = posEnd - k;
  auto gt = [&](auto i, auto j) { return queue_.compare(i, j) > 0; };
  for (int i = 0; i < size(); ++i) {
    if (i < k) {
      posBeg[i] = i;
      std::push_heap(posBeg, posBeg + i + 1, gt);
    } else if (queue_.compare(i, *posBeg) > 0) {
      std::pop_heap(posBeg, posEnd, gt);
      posBeg[k - 1] = i;
      std::push_heap(posBeg, posEnd, gt);
    }
  }
  std::sort(posBeg, posEnd, gt);
  for (auto it = posBeg; it != posEnd; ++it) {
    auto i = *it;
    *values++ = queue_.values()[i];
    *counts++ = queue_.priorities()[i];
  }
}

template <typename T, typename A>
std::vector<std::pair<T, int64_t>> ApproxMostFrequentStreamSummary<T, A>::topK(
    int k) const {
  VELOX_CHECK(k >= 0);
  k = std::min(k, size());
  std::vector<T> values(k);
  std::vector<int64_t> counts(k);
  topK(k, values.data(), counts.data());
  std::vector<std::pair<T, int64_t>> ans(k);
  for (int i = 0; i < k; ++i) {
    ans[i] = {values[i], counts[i]};
  }
  return ans;
}

template <typename T, typename A>
size_t ApproxMostFrequentStreamSummary<T, A>::serializedByteSize() const {
  size_t ans = sizeof(int32_t) + sizeof(T) * size() + sizeof(int64_t) * size();
  if constexpr (std::is_same_v<T, StringView>) {
    for (int i = 0; i < size(); ++i) {
      auto& v = queue_.values()[i];
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
  auto* cur = out;
  folly::storeUnaligned<int32_t>(cur, size());
  cur += sizeof(int32_t);
  auto byteSize = sizeof(T) * size();
  memcpy(cur, queue_.values(), byteSize);
  cur += byteSize;
  byteSize = sizeof(int64_t) * size();
  memcpy(cur, queue_.priorities(), byteSize);
  cur += byteSize;
  if constexpr (std::is_same_v<T, StringView>) {
    for (int i = 0; i < size(); ++i) {
      auto& v = queue_.values()[i];
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
  auto size = folly::loadUnaligned<int32_t>(other);
  other += sizeof size;
  auto* values = other;
  other += sizeof(T) * size;
  auto* counts = other;
  if constexpr (std::is_same_v<T, StringView>) {
    other += sizeof(int64_t) * size;
  }
  T v;
  for (int i = 0; i < size; ++i) {
    FOLLY_BUILTIN_MEMCPY(&v, values + i * sizeof(T), sizeof(T));
    if constexpr (std::is_same_v<T, StringView>) {
      if (!v.isInline()) {
        v = {other, static_cast<int32_t>(v.size())};
        other += v.size();
      }
    }
    insert(v, folly::loadUnaligned<int64_t>(counts + i * sizeof(int64_t)));
  }
}

template <typename T, typename A>
void ApproxMostFrequentStreamSummary<T, A>::merge(
    const ApproxMostFrequentStreamSummary<T, A>& other) {
  for (int i = 0; i < other.size(); ++i) {
    insert(other.values()[i], other.counts()[i]);
  }
}

} // namespace facebook::velox::functions
