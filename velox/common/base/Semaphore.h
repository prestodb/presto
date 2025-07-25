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

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>

#include "velox/common/base/Exceptions.h"
#include "velox/common/future/VeloxPromise.h"

namespace facebook::velox {

/// Similar to std::counting_semaphore in C++ 20.
class Semaphore {
 public:
  Semaphore(int32_t count) : count_(count) {}

  // If count > 0, decrements count. Otherwise blocks until count is incremented
  // by release() from another thread.
  void acquire() {
    std::unique_lock l(mutex_);
    if (count_) {
      --count_;
    } else {
      ++numWaiting_;
      cv_.wait(l, [&]() { return count_ > 0; });
      --numWaiting_;
      --count_;
    }
  }

  /// Increments count. Releases one blocking call to acquire, if any.
  void release() {
    std::lock_guard<std::mutex> l(mutex_);
    ++count_;
    if (numWaiting_ > 0) {
      cv_.notify_one();
    }
  }

  int32_t count() {
    std::lock_guard<std::mutex> l(mutex_);
    return count_;
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  int32_t count_;
  int32_t numWaiting_{0};
};

} // namespace facebook::velox
