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

#include "velox/core/CancelPool.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::core {

StopReason CancelPool::enter(ThreadState& state) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(state.isEnqueued);
  state.isEnqueued = false;
  if (state.isTerminated) {
    return StopReason::kAlreadyTerminated;
  }
  if (state.isOnThread()) {
    return StopReason::kAlreadyOnThread;
  }
  auto reason = shouldStopLocked();
  if (reason == StopReason::kTerminate) {
    state.isTerminated = true;
  }
  if (reason == StopReason::kNone) {
    ++numThreads_;
    state.setThread();
    state.hasBlockingFuture = false;
  }
  return reason;
}

StopReason CancelPool::enterForTerminate(ThreadState& state) {
  std::lock_guard<std::mutex> l(mutex_);
  if (state.isOnThread() || state.isTerminated) {
    state.isTerminated = true;
    return StopReason::kAlreadyOnThread;
  }
  state.isTerminated = true;
  state.setThread();
  return StopReason::kTerminate;
}

StopReason CancelPool::leave(ThreadState& state) {
  std::lock_guard<std::mutex> l(mutex_);
  if (--numThreads_ == 0) {
    finished();
  }
  state.clearThread();
  if (state.isTerminated) {
    return StopReason::kTerminate;
  }
  auto reason = shouldStopLocked();
  if (reason == StopReason::kTerminate) {
    state.isTerminated = true;
  }
  return reason;
}

StopReason CancelPool::enterSuspended(ThreadState& state) {
  VELOX_CHECK(!state.hasBlockingFuture);
  VELOX_CHECK(state.isOnThread());
  std::lock_guard<std::mutex> l(mutex_);
  if (state.isTerminated) {
    return StopReason::kAlreadyTerminated;
  }
  if (!state.isOnThread()) {
    return StopReason::kAlreadyTerminated;
  }
  auto reason = shouldStopLocked();
  if (reason == StopReason::kTerminate) {
    state.isTerminated = true;
  }
  // A pause will not stop entering the suspended section. It will
  // just ack that the thread is no longer in inside the
  // CancelPool. The pause can wait at the exit of the suspended
  // section.
  if (reason == StopReason::kNone || reason == StopReason::kPause) {
    state.isSuspended = true;
    if (--numThreads_ == 0) {
      finished();
    }
  }
  return StopReason::kNone;
}

StopReason CancelPool::leaveSuspended(ThreadState& state) {
  for (;;) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      ++numThreads_;
      state.isSuspended = false;
      if (state.isTerminated) {
        return StopReason::kAlreadyTerminated;
      }
      if (terminateRequested_) {
        state.isTerminated = true;
        return StopReason::kTerminate;
      }
      if (!pauseRequested_) {
        // For yield or anything but pause  we return here.
        return StopReason::kNone;
      }
      --numThreads_;
      state.isSuspended = true;
    }
    // If the pause flag is on when trying to reenter, sleep a while
    // outside of the mutex and recheck. This is rare and not time
    // critical. Can happen if memory interrupt sets pause while
    // already inside a suspended section for other reason, like
    // IO.
    std::this_thread::sleep_for(std::chrono::milliseconds(10)); // NOLINT
  }
}

} // namespace facebook::velox::core
