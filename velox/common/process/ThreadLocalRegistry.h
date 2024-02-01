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

#include <list>
#include <memory>
#include <mutex>

namespace facebook::velox::process {

/// A registry for keeping static thread local objects of type T.  Similar to
/// folly::ThreadLocal but a little bit more efficient in terms of performance
/// and memory usage, because we do not support thread local with lexical scope.
///
/// NOTE: only one instance of ThreadLocalRegistry<T> can be created with each
/// T.
template <typename T>
class ThreadLocalRegistry {
 public:
  class Reference;

  /// Access values from all threads.  Takes a global lock and should be used
  /// with caution.
  template <typename F>
  void forAllValues(F f) {
    std::lock_guard<std::mutex> entriesLock(entriesMutex_);
    for (auto& entry : entries_) {
      std::lock_guard<std::mutex> lk(entry->mutex);
      f(entry->value);
    }
  }

 private:
  struct Entry {
    std::mutex mutex;
    T value;
  };

  std::list<std::unique_ptr<Entry>> entries_;
  std::mutex entriesMutex_;
};

/// Reference to one thread local value.  Should be stored in thread local
/// memory.
template <typename T>
class ThreadLocalRegistry<T>::Reference {
 public:
  explicit Reference(const std::shared_ptr<ThreadLocalRegistry>& registry)
      : registry_(registry) {
    auto entry = std::make_unique<Entry>();
    std::lock_guard<std::mutex> lk(registry_->entriesMutex_);
    iterator_ =
        registry_->entries_.insert(registry_->entries_.end(), std::move(entry));
  }

  ~Reference() {
    std::lock_guard<std::mutex> lk(registry_->entriesMutex_);
    registry_->entries_.erase(iterator_);
  }

  /// Obtain the thread local value and process it with the functor `f'.
  template <typename F>
  auto withValue(F f) {
    auto* entry = iterator_->get();
    std::lock_guard<std::mutex> lk(entry->mutex);
    return f(entry->value);
  }

 private:
  std::shared_ptr<ThreadLocalRegistry> const registry_;
  typename std::list<std::unique_ptr<Entry>>::iterator iterator_;
};

} // namespace facebook::velox::process
