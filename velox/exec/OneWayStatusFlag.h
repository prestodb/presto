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

#include <folly/synchronization/SanitizeThread.h>
#include <atomic>

namespace facebook::velox::exec {

/// A simple one way status flag that uses a non atomic flag to avoid
/// unnecessary atomic operations.
class OneWayStatusFlag {
 public:
  bool check() const {
#if defined(__x86_64__)
    folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
    return fastStatus_ || atomicStatus_.load();
#else
    return atomicStatus_.load(std::memory_order_relaxed) ||
        atomicStatus_.load();
#endif
  }

  void set() {
#if defined(__x86_64__)
    folly::annotate_ignore_thread_sanitizer_guard g(__FILE__, __LINE__);
    if (!fastStatus_) {
      atomicStatus_.store(true);
      fastStatus_ = true;
    }
#else
    if (!atomicStatus_.load(std::memory_order_relaxed)) {
      atomicStatus_.store(true);
    }
#endif
  }

  /// Operator overload to convert OneWayStatusFlag to bool
  operator bool() const {
    return check();
  }

 private:
#if defined(__x86_64__)
  // This flag can only go from false to true, and is only checked at the end of
  // a loop. Given that once a flag is true it can never go back to false, we
  // are ok to use this in a non synchronized manner to avoid the overhead. As
  // such we consciously exempt ourselves here from TSAN detection.
  bool fastStatus_{false};
#endif
  std::atomic_bool atomicStatus_{false};
};

} // namespace facebook::velox::exec
