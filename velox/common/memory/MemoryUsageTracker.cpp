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

void MemoryUsageTracker::updateInternal(UsageType type, int64_t size) {
  // Update parent first. If one of the ancestor's limits are exceeded, it
  // will throw VeloxMemoryCapExceeded exception.
  if (parent_) {
    parent_->updateInternal(type, size);
  }

  auto newPeak = usage(currentUsageInBytes_, type)
                     .fetch_add(size, std::memory_order_relaxed) +
      size;

  if (size > 0) {
    ++usage(numAllocs_, type);
    usage(cumulativeBytes_, type) += size;
    ++usage(numAllocs_, type);
    usage(cumulativeBytes_, type) += size;
  }

  // We track the peak usage of total memory independent of user and
  // system memory since freed user memory can be reallocated as system
  // memory and vice versa.
  int64_t totalBytes = getCurrentUserBytes() + getCurrentSystemBytes();

  // Enforce the limit. Throw VeloxMemoryCapException exception if the limits
  // are exceeded.
  if (size > 0 &&
      (newPeak > usage(maxMemory_, type) || totalBytes > total(maxMemory_))) {
    // Exceeded the limit. Fail allocation after reverting changes to
    // parent and currentUsageInBytes_.
    if (parent_) {
      parent_->updateInternal(type, -size);
    }
    usage(currentUsageInBytes_, type)
        .fetch_add(-size, std::memory_order_relaxed);
    checkNonNegativeSizes("after exceeding cap");
    VELOX_MEM_CAP_EXCEEDED(usage(maxMemory_, type));
  }

  maySetMax(type, newPeak);
  maySetMax(UsageType::kTotalMem, totalBytes);
  checkNonNegativeSizes("after update");
}

} // namespace facebook::velox::memory
