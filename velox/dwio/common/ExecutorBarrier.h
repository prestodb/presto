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
#include <mutex>

#include "folly/ExceptionWrapper.h"
#include "folly/Executor.h"

namespace facebook::velox::dwio::common {

class ExecutorBarrier : public folly::Executor {
 public:
  explicit ExecutorBarrier(folly::Executor& executor)
      : executor_{executor}, count_{0} {}

  explicit ExecutorBarrier(std::shared_ptr<folly::Executor> executor)
      : owned_{std::move(executor)}, executor_{*owned_}, count_{0} {}

  ~ExecutorBarrier() override {
    // If this object gets destroyed while there are still tasks pending, those
    // tasks will try to access invalid memory addresses in the current object.
    std::unique_lock lock{mutex_};
    cv_.wait(lock, [&]() { return count_ == 0; });
    // We won't throw from the destructor so we don't check for exceptions
    // Also, I don't need to clear the exception because this is the destructor.
  }

  /// Enqueue a function to be executed by this executor. This and all
  /// variants must be threadsafe.
  void add(folly::Func) override;

  /// Enqueue a function with a given priority, where 0 is the medium priority
  /// This is up to the implementation to enforce
  void addWithPriority(folly::Func, int8_t priority) override;

  uint8_t getNumPriorities() const override {
    return executor_.getNumPriorities();
  }

  void waitAll() {
    std::unique_lock lock{mutex_};
    cv_.wait(lock, [&]() { return count_ == 0; });
    if (exception_.has_exception_ptr()) {
      folly::exception_wrapper ew;
      // Clear the exception for the next time
      std::swap(ew, exception_);
      ew.throw_exception();
    }
  }

 private:
  auto wrapMethod(folly::Func f);

  std::shared_ptr<folly::Executor> owned_;
  folly::Executor& executor_;
  size_t count_;
  std::mutex mutex_;
  std::condition_variable cv_;
  folly::exception_wrapper exception_;
};

} // namespace facebook::velox::dwio::common
