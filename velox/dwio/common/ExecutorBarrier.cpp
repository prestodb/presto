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

#include "velox/dwio/common/ExecutorBarrier.h"

namespace facebook::velox::dwio::common {

namespace {

class BarrierElement {
 public:
  BarrierElement(size_t& count, std::mutex& mutex, std::condition_variable& cv)
      : count_{count}, mutex_{&mutex}, cv_{cv} {
    std::lock_guard lock{*mutex_};
    ++count_;
  }

  BarrierElement(BarrierElement&& other) noexcept
      : count_{other.count_}, mutex_{other.mutex_}, cv_{other.cv_} {
    // Move away
    other.mutex_ = nullptr;
  }

  BarrierElement(const BarrierElement& other) = delete;
  BarrierElement& operator=(BarrierElement&& other) = delete;
  BarrierElement& operator=(const BarrierElement& other) = delete;

  ~BarrierElement() {
    // If this object wasn't moved away
    if (mutex_) {
      std::lock_guard lock{*mutex_};
      if (--count_ == 0) {
        cv_.notify_all();
      }
    }
  }

 private:
  size_t& count_;
  std::mutex* mutex_;
  std::condition_variable& cv_;
};

} // namespace

auto ExecutorBarrier::wrapMethod(folly::Func f) {
  return [f = std::move(f),
          this,
          barrierElement = BarrierElement(count_, mutex_, cv_)]() mutable {
    try {
      f();
    } catch (...) {
      std::lock_guard lock{mutex_};
      if (!exception_.has_exception_ptr()) {
        exception_ = folly::exception_wrapper(std::current_exception());
      }
    }
  };
}

void ExecutorBarrier::add(folly::Func f) {
  executor_.add(wrapMethod(std::move(f)));
}

void ExecutorBarrier::addWithPriority(folly::Func f, int8_t priority) {
  executor_.addWithPriority(wrapMethod(std::move(f)), priority);
}

} // namespace facebook::velox::dwio::common
