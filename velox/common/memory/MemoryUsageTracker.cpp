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
} // namespace facebook::velox::memory
