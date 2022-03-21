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

#include "velox/common/memory/MemoryUsageTracker.h"

namespace facebook::velox::memory {
std::shared_ptr<MemoryUsageTracker> MemoryUsageTracker::create(
    const std::shared_ptr<MemoryUsageTracker>& parent,
    MemoryUsageTracker::UsageType type,
    const MemoryUsageConfig& config) {
  struct SharedMemoryUsageTracker : public MemoryUsageTracker {
    SharedMemoryUsageTracker(
        const std::shared_ptr<MemoryUsageTracker>& parent,
        MemoryUsageTracker::UsageType type,
        const MemoryUsageConfig& config)
        : MemoryUsageTracker(parent, type, config) {}
  };

  return std::make_shared<SharedMemoryUsageTracker>(parent, type, config);
}

void MemoryUsageTracker::checkAndPropagateReservationIncrement(
    int64_t increment,
    bool updateMinReservation) {
  std::exception_ptr exception;
  try {
    incrementUsage(type_, increment);
  } catch (std::exception& e) {
    exception = std::current_exception();
  }
  // Process the exception outside of catch so as not to kill the process if
  // 'mutex_' throws deadlock.
  if (exception) {
    // Compensate for the increase after exceeding limit.
    std::lock_guard<std::mutex> l(mutex_);
    reservation_ -= increment;
    if (updateMinReservation) {
      minReservation_ -= increment;
    }
    std::rethrow_exception(exception);
  }
}

void MemoryUsageTracker::incrementUsage(UsageType type, int64_t size) {
  // Update parent first. If one of the ancestor's limits are exceeded, it
  // will throw VeloxMemoryCapExceeded exception.
  if (parent_) {
    parent_->incrementUsage(type, size);
  }

  auto newUsage = usage(currentUsageInBytes_, type)
                      .fetch_add(size, std::memory_order_relaxed) +
      size;

  ++usage(numAllocs_, type);
  ++usage(numAllocs_, UsageType::kTotalMem);
  usage(cumulativeBytes_, type) += size;

  // We track the peak usage of total memory independent of user and
  // system memory since freed user memory can be reallocated as system
  // memory and vice versa.
  int64_t totalBytes = usage(currentUsageInBytes_, UsageType::kUserMem) +
      usage(currentUsageInBytes_, UsageType::kSystemMem);

  // Enforce the limit.
  if (newUsage > usage(maxMemory_, type) || totalBytes > total(maxMemory_)) {
    if (!growCallback_ || !growCallback_(type, size, *this)) {
      // Exceeded the limit.  revert the change to current usage.
      decrementUsage(type, size);
      checkNonNegativeSizes("after exceeding cap");
      VELOX_MEM_CAP_EXCEEDED(
          std::min(total(maxMemory_), usage(maxMemory_, type)));
    }
  }
  maySetMax(type, newUsage);
  maySetMax(UsageType::kTotalMem, totalBytes);
  checkNonNegativeSizes("after update");
}

void MemoryUsageTracker::decrementUsage(UsageType type, int64_t size) noexcept {
  if (parent_) {
    parent_->decrementUsage(type, size);
  }
  usage(currentUsageInBytes_, type).fetch_sub(size);
}
} // namespace facebook::velox::memory
