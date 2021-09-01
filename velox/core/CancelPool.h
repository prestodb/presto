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

#include <folly/futures/Future.h>
#include <folly/portability/SysSyscall.h>
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

// Represents a Driver's state. This is used for cancellation, forcing
// release of and for waiting for memory. The fields are serialized on
// the mutex of the Driver's CancelPool.
//
// The Driver goes through the following states:
// Not on thread. It is created and has not started. All flags are false.
//
// Enqueued - The Driver is added to an executor but does not yet have a thread.
// isEnqueued is true. Next states are terminated or on thread.
//
// On thread - 'thread' is set to the thread that is running the Driver. Next
// states are blocked, terminated, suspended, enqueued.
//
//  Blocked - The Driver is not on thread and is waiting for an external event.
//  Next states are terminated, enqueud.
//
// Suspended - The Driver is on thread, 'thread' and 'isSuspended' are set. The
// thread does not manipulate the Driver's state and is suspended as in waiting
// for memory or out of process IO. This is different from Blocked in that here
// we keep the stack so that when the wait is over the control stack is not
// lost. Next states are on thread or terminated.
//
//  Terminated - 'isTerminated' is set. The Driver cannot run after this and
// the state is final.
//
// CancelPool  allows terminating or pausing a set of Drivers. The Task API
// allows starting or resuming Drivers. When terminate is requested the request
// is successful when all Drivers are off thread, blocked or suspended. When
// pause is requested, we have success when all Drivers are either enqueued,
// suspended, off thread or blocked.
struct ThreadState {
  // The thread currently running this.
  std::thread::id thread;
  // The tid of 'thread'. Allows finding the thread in a debugger.
  int32_t tid;
  // True if queued on an executor but not on thread.
  bool isEnqueued{false};
  // True if being terminated or already terminated.
  bool isTerminated{false};
  // True if there is a future outstanding that will schedule this on an
  // executor thread when some promise is realized.
  bool hasBlockingFuture{false};
  // True if on thread but in a section waiting for RPC or memory
  // strategy decision. The thread is not supposed to access its
  // memory, which a third party can revoke while the thread is in
  // this state.
  bool isSuspended{false};

  bool isOnThread() const {
    return thread != std::thread::id();
  }

  void setThread() {
    thread = std::this_thread::get_id();
#if !defined(__APPLE__)
    // This is a debugging feature disabled on the Mac since syscall
    // is deprecated on that platform.
    tid = syscall(FOLLY_SYS_gettid);
#endif
  }

  void clearThread() {
    thread = std::thread::id(); // no thread.
    tid = 0;
  }
};

class CancelPool {
 public:
  // Returns kNone if no pause or terminate is requested. The thread count is
  // incremented if kNone is returned. If something else is returned the
  // calling thread should unwind and return itself to its pool.
  StopReason enter(ThreadState& state);

  // Sets the state to terminated. Returns kAlreadyOnThread if the
  // Driver is running. In this case, the Driver will free resources
  // and the caller should not do anything. Returns kTerminate if the
  // Driver was not on thread. When this happens, the Driver is on the
  // caller thread wit isTerminated set and the caller is responsible
  // for freeing resources.
  StopReason enterForTerminate(ThreadState& state);

  // Marks that the Driver is not on thread. If no more Drivers in the
  // CancelPool are on thread, this realizes any finishFutures. The
  // Driver may go off thread because of hasBlockingFuture or pause
  // requested or terminate requested. The return value indicates the
  // reason. If kTerminate is returned, the isTerminated flag is set.
  StopReason leave(ThreadState& state);

  // Enters a suspended section where the caller stays on thread but
  // is not accounted as being on the thread.  Returns kNone if no
  // terminate is requested. The thread count is decremented if kNone
  // is returned. If thread count goes to zero, waiting promises are
  // realized. If kNone is not returned the calling thread should
  // unwind and return itself to its pool.
  StopReason enterSuspended(ThreadState& state);

  StopReason leaveSuspended(ThreadState& state);
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
    std::lock_guard<std::mutex> l(mutex_);
    requestPauseLocked(pause);
  }

  void requestPauseLocked(bool pause) {
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

  // Once 'pauseRequested_' is set, it will not be cleared until
  // task::resume(). It is therefore OK to read it without a mutex
  // from a thread that this flag concerns.
  bool pauseRequested() const {
    return pauseRequested_;
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
  void finished() {
    for (auto& promise : finishPromises_) {
      promise.setValue(true);
    }
    finishPromises_.clear();
  }

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
