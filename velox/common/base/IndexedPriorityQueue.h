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

#include "velox/common/base/Exceptions.h"

#include <folly/container/F14Map.h>

namespace facebook::velox {

/// A priority queue with values of type 'T'. Each value has assigned priority
/// on insertion which determines the value's position in the queue. It supports
/// to update the priority of the existing values, which adjusts the value's
/// position in the queue accordingly. Ties are broken by insertion and update
/// order. If 'kMaxQueue' is true, it is a max priority queue, otherwise a min
/// priority queue.
///
/// NOTE: this class is not thread-safe.
template <
    typename T,
    bool kMaxQueue,
    typename Allocator = std::allocator<T>,
    typename Hash = std::hash<T>,
    typename EqualTo = std::equal_to<T>>
class IndexedPriorityQueue {
 public:
  explicit IndexedPriorityQueue(const Allocator& allocator = {})
      : values_(allocator),
        priorities_(RebindAlloc<int64_t>(allocator)),
        generations_(RebindAlloc<int64_t>(allocator)),
        heap_(RebindAlloc<int32_t>(allocator)),
        valueIndices_(RebindAlloc<std::pair<const T, int32_t>>(allocator)),
        heapIndices_(RebindAlloc<int32_t>(allocator)) {}

  ~IndexedPriorityQueue() {
    validate();
  }

  int32_t size() const {
    return values_.size();
  }

  bool empty() const {
    return values_.empty();
  }

  /// Inserts 'value' into the queue with specified 'priority'. If 'value'
  /// already exists, then update its priority and the corresponding position in
  /// the queue.
  bool addOrUpdate(const T& value, int64_t priority) {
    auto it = valueIndices_.find(value);
    if (it == valueIndices_.end()) {
      addNewValue(value, priority);
      return true;
    }
    auto i = it->second;
    if (priorities_[i] == priority) {
      return false;
    }
    updatePriority(i, priority);
    return true;
  }

  const T& top() const {
    VELOX_DCHECK(!heap_.empty());
    return values_[heap_[0]];
  }

  int64_t topPriority() const {
    VELOX_DCHECK(!heap_.empty());
    return priorities_[heap_[0]];
  }

  T pop() {
    VELOX_DCHECK(!heap_.empty());
    auto i = heap_[0];
    heap_[0] = heap_.back();
    heapIndices_[heap_.back()] = 0;
    heap_.pop_back();
    if (!heap_.empty()) {
      percolateDown(0);
    }
    auto result = values_[i];
    valueIndices_.erase(values_[i]);
    if (i != size() - 1) {
      valueIndices_[values_.back()] = i;
      values_[i] = values_.back();
      priorities_[i] = priorities_.back();
      generations_[i] = generations_.back();
      heap_[heapIndices_.back()] = i;
      heapIndices_[i] = heapIndices_.back();
    }
    values_.pop_back();
    priorities_.pop_back();
    generations_.pop_back();
    heapIndices_.pop_back();
    return result;
  }

  const T* values() const {
    return values_.data();
  }

  const int64_t* priorities() const {
    return priorities_.data();
  }

  std::optional<int> getValueIndex(const T& value) const {
    auto it = valueIndices_.find(value);
    if (it != valueIndices_.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  void updatePriority(int index, int64_t priority) {
    bool up = priority < priorities_[index];
    if constexpr (kMaxQueue) {
      up = !up;
    }
    priorities_[index] = priority;
    generations_[index] = ++generation_;
    if (up) {
      percolateUp(heapIndices_[index]);
    } else {
      percolateDown(heapIndices_[index]);
    }
  }

  void addNewValue(const T& value, int64_t priority) {
    VELOX_DCHECK_LT(size(), std::numeric_limits<int32_t>::max());
    auto i = size();
    values_.push_back(value);
    priorities_.push_back(priority);
    generations_.push_back(++generation_);
    VELOX_CHECK(valueIndices_.emplace(value, i).second);
    heapIndices_.push_back(i);
    heap_.push_back(i);
    percolateUp(i);
  }

  void replaceTop(const T& value, int64_t priority) {
    VELOX_DCHECK(!heap_.empty());
    int i = heap_[0];
    valueIndices_.erase(values_[i]);
    values_[i] = value;
    priorities_[i] = priority;
    generations_[i] = ++generation_;
    VELOX_CHECK(valueIndices_.emplace(value, i).second);
    percolateDown(0);
  }

  /// Return negative number if value at i should be ordered before j; positive
  /// number if j should be ordered before i. Otherwise return 0.
  int64_t compare(int i, int j) const {
    int64_t result = priorities_[i] - priorities_[j];
    if constexpr (kMaxQueue) {
      result = -result;
    }
    return result != 0 ? result : generations_[i] - generations_[j];
  }

 private:
  template <typename U>
  using RebindAlloc =
      typename std::allocator_traits<Allocator>::template rebind_alloc<U>;

  void validate() const {
    VELOX_DCHECK_EQ(values_.size(), priorities_.size());
    VELOX_DCHECK_EQ(values_.size(), generations_.size());
    VELOX_DCHECK_EQ(values_.size(), heap_.size());
    VELOX_DCHECK_EQ(values_.size(), valueIndices_.size());
    VELOX_DCHECK_EQ(values_.size(), heapIndices_.size());
  }

  void percolateUp(int pos) {
    while (pos > 0) {
      int parent = (pos - 1) / 2;
      if (compare(heap_[pos], heap_[parent]) >= 0) {
        break;
      }
      std::swap(heap_[pos], heap_[parent]);
      heapIndices_[heap_[pos]] = pos;
      pos = parent;
    }
    heapIndices_[heap_[pos]] = pos;
  }

  void percolateDown(int pos) {
    for (;;) {
      int left = 2 * pos + 1;
      if (left >= heap_.size()) {
        break;
      }
      int right = left + 1;
      int next = right < heap_.size() && compare(heap_[right], heap_[left]) < 0
          ? right
          : left;
      if (compare(heap_[pos], heap_[next]) <= 0) {
        break;
      }
      std::swap(heap_[pos], heap_[next]);
      heapIndices_[heap_[pos]] = pos;
      pos = next;
    }
    heapIndices_[heap_[pos]] = pos;
  }

  int64_t generation_{0};
  std::vector<T, Allocator> values_;
  std::vector<int64_t, RebindAlloc<int64_t>> priorities_;
  std::vector<int64_t, RebindAlloc<int64_t>> generations_;
  std::vector<int32_t, RebindAlloc<int32_t>> heap_;
  folly::F14FastMap<
      T,
      int32_t,
      Hash,
      EqualTo,
      RebindAlloc<std::pair<const T, int32_t>>>
      valueIndices_;
  std::vector<int32_t, RebindAlloc<int32_t>> heapIndices_;
};

} // namespace facebook::velox
