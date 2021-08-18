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

#include "velox/common/memory/MemoryUsage.h"

namespace facebook {
namespace velox {
namespace memory {
void MemoryUsage::incrementCurrentBytes(int64_t size) {
  setCurrentBytes(currentBytes_.load(std::memory_order_relaxed) + size);
}

void MemoryUsage::setCurrentBytes(int64_t size) {
  currentBytes_.store(size, std::memory_order_relaxed);
  auto previousMaxBytes = maxBytes_.load(std::memory_order_relaxed);
  if (size > previousMaxBytes) {
    maxBytes_.store(size, std::memory_order_relaxed);
  }
}

int64_t MemoryUsage::getCurrentBytes() const {
  return currentBytes_.load(std::memory_order_relaxed);
}

int64_t MemoryUsage::getMaxBytes() const {
  return maxBytes_.load(std::memory_order_relaxed);
}
} // namespace memory
} // namespace velox
} // namespace facebook
