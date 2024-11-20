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

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <set>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox {
/// A priority queue with values of type 'T'. Each value has assigned priority
/// on insertion which determines the value's position in the quue. It supports
/// to update the priority of the existing values, which adjusts the value's
/// position in the queue accordingly. Ties are broken by insertion order. If
/// 'MaxQueue' is true, it is a max priority queue, otherwise a min priority
/// queue.
///
/// NOTE: this class is not thread-safe.
template <typename T, bool MaxQueue>
class IndexedPriorityQueue {
 public:
  IndexedPriorityQueue() = default;

  ~IndexedPriorityQueue() {
    VELOX_CHECK_EQ(queue_.size(), index_.size());
  }

  size_t size() const {
    return queue_.size();
  }

  bool empty() const {
    return queue_.empty();
  }

  /// Inserts 'value' into the queue with specified 'priority'. If 'value'
  /// already exists, then update its priority and the corresponding position in
  /// the queue.
  bool addOrUpdate(T value, uint64_t priority) {
    auto it = index_.find(value);
    if (it != index_.end()) {
      if (it->second->priority() == priority) {
        return false;
      }
      queue_.erase(it->second.get());

      it->second->updatePriority(priority);
      queue_.insert(it->second.get());
      return false;
    }

    auto newEntry = std::make_unique<Entry>(value, priority, generation_++);
    queue_.insert(newEntry.get());
    index_.emplace(value, std::move(newEntry));
    return true;
  }

  std::optional<T> pop() {
    VELOX_CHECK_EQ(queue_.size(), index_.size());
    if (queue_.empty()) {
      return std::nullopt;
    }

    auto it = queue_.begin();
    Entry* removedEntry = *it;
    const auto value = removedEntry->value();
    queue_.erase(it);
    VELOX_CHECK(index_.contains(removedEntry->value()));
    index_.erase(removedEntry->value());
    return value;
  }

  /// Describes the state of an inserted 'value' in the queue.
  class Entry {
   public:
    Entry(T value, uint64_t priority, uint64_t generation)
        : value_(std::move(value)),
          priority_(priority),
          generation_(generation) {}

    const T& value() const {
      return value_;
    }

    uint64_t priority() const {
      return priority_;
    }

    void updatePriority(uint64_t priority) {
      priority_ = priority;
    }

    uint64_t generation() const {
      return generation_;
    }

   private:
    const T value_;
    uint64_t priority_;
    const uint64_t generation_;
  };

  /// Used to iterate through the queue in priority order.
  class Iterator {
   public:
    Iterator(
        typename std::set<Entry*>::const_iterator cur,
        typename std::set<Entry*>::const_iterator end)
        : end_{end}, cur_{cur}, val_{0} {
      if (cur_ != end_) {
        val_ = (*cur_)->value();
      }
    }

    bool operator==(const Iterator& other) const {
      return std::tie(cur_, end_) == std::tie(other.cur_, other.end_);
    }

    bool operator!=(const Iterator& other) const {
      return !operator==(other);
    }

    Iterator& operator++() {
      VELOX_DCHECK(cur_ != end_);
      ++cur_;
      if (cur_ != end_) {
        val_ = (*cur_)->value();
      }
      return *this;
    }

    const T& operator*() const {
      VELOX_DCHECK(cur_ != end_);
      return val_;
    }

   private:
    const typename std::set<Entry*>::const_iterator end_;
    typename std::set<Entry*>::const_iterator cur_;
    T val_;
  };

  Iterator begin() const {
    return Iterator{queue_.cbegin(), queue_.cend()};
  }

  Iterator end() const {
    return Iterator{queue_.cend(), queue_.cend()};
  }

 private:
  struct EntrySetComparator {
    bool operator()(Entry* lhs, Entry* rhs) const {
      if (MaxQueue) {
        if (lhs->priority() > rhs->priority()) {
          return true;
        }
        if (lhs->priority() < rhs->priority()) {
          return false;
        }
      } else {
        if (lhs->priority() > rhs->priority()) {
          return false;
        }
        if (lhs->priority() < rhs->priority()) {
          return true;
        }
      }
      return lhs->generation() < rhs->generation();
    }
  };

  uint64_t generation_{0};
  folly::F14FastMap<T, std::unique_ptr<Entry>> index_;
  std::set<Entry*, EntrySetComparator> queue_;
};

} // namespace facebook::velox
