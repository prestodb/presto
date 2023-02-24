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

#include <functional>
#include <mutex>
#include <vector>

namespace facebook::velox {

/// This template class is a lock wrapper which is similar to std::lock_guard
/// and additionally, it allows user to add deferred callbacks to invoke right
/// after the lock release. 'LockType' class needs to provides lock()/unlock()
/// methods to acquire and release lock. Note that the class always calls
/// unlock() in object destructor even if lock() calls throws in constructor.
///
/// The following is the code snippets to show how to
/// use it:
///    std::mutex mu;
///    {
///       ScopedLock sl(&mu);
///       sl.addCallback([&]() {
///          std::lock_guard l(mu);
///          printf("acquired lock again"); });
///    }
template <typename LockType>
class ScopedLock {
 public:
  ScopedLock(LockType* lock) : lock_(lock) {
    lock_->lock();
  }

  ~ScopedLock() {
    lock_->unlock();
    for (auto cb : callbacks_) {
      cb();
    }
  }

  using Callback = std::function<void()>;

  /// Invoked to add 'cb' to run after release the lock on object destruction.
  void addCallback(Callback&& cb) {
    callbacks_.push_back(std::move(cb));
  }

 private:
  LockType* const lock_;
  std::vector<Callback> callbacks_;
};

} // namespace facebook::velox
