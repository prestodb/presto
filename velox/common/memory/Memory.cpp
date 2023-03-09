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

#include "velox/common/memory/Memory.h"

namespace facebook::velox::memory {
namespace {
#define VELOX_MEM_MANAGER_CAP_EXCEEDED(cap)                         \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kMemCapExceeded.c_str(),       \
      /* isRetriable */ true,                                       \
      "Exceeded memory manager cap of {} MB",                       \
      (cap) / 1024 / 1024);

constexpr folly::StringPiece kRootNodeName{"__root__"};
} // namespace

MemoryManager::MemoryManager(const Options& options)
    : allocator_{options.allocator->shared_from_this()},
      memoryQuota_{options.capacity},
      alignment_(std::max(MemoryAllocator::kMinAlignment, options.alignment)),
      root_{std::make_shared<MemoryPoolImpl>(
          *this,
          kRootNodeName.str(),
          nullptr,
          MemoryPool::Options{alignment_, memoryQuota_})} {
  VELOX_CHECK_NOT_NULL(allocator_);
  VELOX_USER_CHECK_GE(memoryQuota_, 0);
  MemoryAllocator::alignmentCheck(0, alignment_);
}

MemoryManager::~MemoryManager() {
  auto currentBytes = getTotalBytes();
  if (currentBytes > 0) {
    VELOX_MEM_LOG(WARNING) << "Leaked total memory of " << currentBytes
                           << " bytes.";
  }
}

int64_t MemoryManager::getMemoryQuota() const {
  return memoryQuota_;
}

uint16_t MemoryManager::alignment() const {
  return alignment_;
}

MemoryPool& MemoryManager::getRoot() const {
  return *root_;
}

std::shared_ptr<MemoryPool> MemoryManager::getChild(int64_t cap) {
  static std::atomic<int64_t> poolId{0};
  return root_->addChild(fmt::format("default_usage_node_{}", poolId++));
}

int64_t MemoryManager::getTotalBytes() const {
  return totalBytes_.load(std::memory_order_relaxed);
}

bool MemoryManager::reserve(int64_t size) {
  return totalBytes_.fetch_add(size, std::memory_order_relaxed) + size <=
      memoryQuota_;
}

void MemoryManager::release(int64_t size) {
  totalBytes_.fetch_sub(size, std::memory_order_relaxed);
}

MemoryAllocator& MemoryManager::getAllocator() {
  return *allocator_;
}

IMemoryManager& getProcessDefaultMemoryManager() {
  return MemoryManager::getInstance();
}

std::shared_ptr<MemoryPool> getDefaultMemoryPool(int64_t cap) {
  auto& memoryManager = getProcessDefaultMemoryManager();
  return memoryManager.getChild(cap);
}

} // namespace facebook::velox::memory
