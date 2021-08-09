/*
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

#include <folly/futures/Future.h>
#include <mutex>
#include "velox/common/future/VeloxPromise.h"

namespace facebook::velox::core {

enum class StopReason {
  // Keep running.
  kNone,
  // Go off thread and do not schedule more activity.
  kPause,
  // Stop and free all. This is returned once and the thread that gets
  // this value is responsible for freeing the state associated with
  // the thread. Other threads will get kAlreadyTerminated after the
  // first thread has received kTerminate.
  kTerminate,
  kAlreadyTerminated,
  // Go off thread and then enqueue to the back of the runnable queue.
  kYield,
  // Must wait for external events.
  kBlock,
  // No more data to produce.
  kAtEnd,
  kAlreadyOnThread
};

class CancelPool {
 public:
  // Returns kNone if no pause or terminate is requested. The thread count is
  // incremented if kNone is returned. If something else is returned the
  // calling tread should unwind and return itself to its pool.
  StopReason enter(bool* isOnThread, bool* isTerminated) {
    std::lock_guard<std::mutex> l(mutex_);
    if (*isTerminated) {
      return StopReason::kAlreadyTerminated;
    }
    if (*isOnThread) {
      return StopReason::kAlreadyOnThread;
    }
    auto reason = shouldStopLocked();
    if (reason == StopReason::kTerminate) {
      *isTerminated = true;
    }
    if (reason == StopReason::kNone) {
      ++numThreads_;
      *isOnThread = true;
    }
    return reason;
  }

  StopReason enterForTerminate(bool* isOnThread, bool* isTerminated) {
    std::lock_guard<std::mutex> l(mutex_);
    if (*isOnThread || *isTerminated) {
      *isTerminated = true;
      return StopReason::kAlreadyOnThread;
    }
    *isTerminated = true;
    *isOnThread = true;
    return StopReason::kTerminate;
  }

  StopReason leave(bool* isOnThread, bool* isTerminated) {
    std::lock_guard<std::mutex> l(mutex_);
    if (--numThreads_ == 0) {
      for (auto& promise : finishPromises_) {
        promise.setValue(true);
      }
      finishPromises_.clear();
    }
    *isOnThread = false;
    if (*isTerminated) {
      return StopReason::kTerminate;
    }
    auto reason = shouldStopLocked();
    if (reason == StopReason::kTerminate) {
      *isTerminated = true;
    }
    return reason;
  }

  // Returns a stop reason without synchronization. If the stop reason
  // is yield, then atomically decrements the count of threads that
  // are to yield.
  StopReason shouldStop() {
    if (terminateRequested_) {
      return StopReason::kTerminate;
    }
    if (pauseRequested_) {
      return StopReason::kPause;
    }
    if (toYield_) {
      std::lock_guard<std::mutex> l(mutex_);
      return shouldStopLocked();
    }
    return StopReason::kNone;
  }

  void requestPause(bool pause) {
    pauseRequested_ = pause;
  }

  void requestTerminate() {
    terminateRequested_ = true;
  }

  void requestYield() {
    std::lock_guard<std::mutex> l(mutex_);
    toYield_ = numThreads_;
  }

  bool terminateRequested() const {
    return terminateRequested_;
  }

  // Returns a future that is completed when all threads have acknowledged
  // terminate or pause. If the future is realized there is no running activity
  // on behalf of threads that have entered 'this'.
  folly::SemiFuture<bool> finishFuture() {
    auto [promise, future] =
        makeVeloxPromiseContract<bool>("CancelPool::finishFuture");
    std::lock_guard<std::mutex> l(mutex_);
    if (numThreads_ == 0) {
      promise.setValue(true);
      return std::move(future);
    }
    finishPromises_.push_back(std::move(promise));
    return std::move(future);
  }

  std::mutex* mutex() {
    return &mutex_;
  }

 private:
  StopReason shouldStopLocked() {
    if (terminateRequested_) {
      return StopReason::kTerminate;
    }
    if (pauseRequested_) {
      return StopReason::kPause;
    }
    if (toYield_) {
      --toYield_;
      return StopReason::kYield;
    }
    return StopReason::kNone;
  }

  std::mutex mutex_;
  bool pauseRequested_ = false;
  bool terminateRequested_ = false;
  int32_t toYield_ = 0;
  int32_t numThreads_ = 0;
  std::vector<VeloxPromise<bool>> finishPromises_;
};

using CancelPoolPtr = std::shared_ptr<CancelPool>;

} // namespace facebook::velox::core
