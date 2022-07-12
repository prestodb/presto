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

#include "velox/common/base/BitUtil.h"

namespace facebook {
namespace velox {
namespace memory {
/* static */
std::shared_ptr<MemoryAllocator> MemoryAllocator::createDefaultAllocator() {
  return std::make_shared<MemoryAllocator>();
}

void* FOLLY_NULLABLE MemoryAllocator::alloc(int64_t size) {
  return std::malloc(size);
}

void* FOLLY_NULLABLE
MemoryAllocator::allocZeroFilled(int64_t numMembers, int64_t sizeEach) {
  return std::calloc(numMembers, sizeEach);
}

void* FOLLY_NULLABLE
MemoryAllocator::allocAligned(uint16_t alignment, int64_t size) {
  return aligned_alloc(alignment, size);
}

void* FOLLY_NULLABLE MemoryAllocator::realloc(
    void* FOLLY_NULLABLE p,
    int64_t /* size */,
    int64_t newSize) {
  return std::realloc(p, newSize);
}

void* FOLLY_NULLABLE MemoryAllocator::reallocAligned(
    void* FOLLY_NULLABLE p,
    uint16_t alignment,
    int64_t size,
    int64_t newSize) {
  if (newSize <= 0) {
    return nullptr;
  }
  auto block = aligned_alloc(alignment, newSize);
  if (block) {
    memcpy(block, p, std::min(size, newSize));
    std::free(p);
  }
  return block;
}

void MemoryAllocator::free(void* FOLLY_NULLABLE p, int64_t /* size */) {
  std::free(p);
}

MemoryPoolBase::MemoryPoolBase(
    const std::string& name,
    std::weak_ptr<MemoryPool> parent)
    : name_{name}, parent_{parent} {}

MemoryPoolBase::~MemoryPoolBase() {
  // Destroy child pools first.
  children_.clear();
}

const std::string& MemoryPoolBase::getName() const {
  return name_;
}

std::weak_ptr<MemoryPool> MemoryPoolBase::getWeakPtr() {
  return this->weak_from_this();
}

uint64_t MemoryPoolBase::getChildCount() const {
  folly::SharedMutex::ReadHolder guard{childrenMutex_};
  return children_.size();
}

MemoryPool& MemoryPoolBase::getChildByName(const std::string& name) {
  folly::SharedMutex::ReadHolder guard{childrenMutex_};
  // Implicitly synchronized in dtor of child so it's impossible for
  // MemoryManager to access after destruction of child.
  auto iter = std::find_if(
      children_.begin(),
      children_.end(),
      [&name](const std::shared_ptr<MemoryPool>& e) {
        return e->getName() == name;
      });

  VELOX_USER_CHECK(
      iter != children_.end(),
      "Failed to find child memory pool by name: {}",
      name);

  return **iter;
}

void MemoryPoolBase::visitChildren(
    std::function<void(MemoryPool* FOLLY_NONNULL)> visitor) const {
  folly::SharedMutex::WriteHolder guard{childrenMutex_};
  for (const auto& child : children_) {
    visitor(child.get());
  }
}

MemoryPool& MemoryPoolBase::addChild(const std::string& name, int64_t cap) {
  folly::SharedMutex::WriteHolder guard{childrenMutex_};
  // Upon name collision we would throw and not modify the map.
  auto child = genChild(getWeakPtr(), name, cap);
  if (isMemoryCapped()) {
    child->capMemoryAllocation();
  }
  if (auto usageTracker = getMemoryUsageTracker()) {
    child->setMemoryUsageTracker(usageTracker->addChild());
  }
  children_.emplace_back(std::move(child));
  return *children_.back();
}

std::unique_ptr<ScopedMemoryPool> MemoryPoolBase::addScopedChild(
    const std::string& name,
    int64_t cap) {
  auto& pool = addChild(name, cap);
  return std::make_unique<ScopedMemoryPool>(pool.getWeakPtr());
}

void MemoryPoolBase::dropChild(const MemoryPool* child) {
  folly::SharedMutex::WriteHolder guard{childrenMutex_};
  // Implicitly synchronized in dtor of child so it's impossible for
  // MemoryManager to access after destruction of child.
  auto iter = std::find_if(
      children_.begin(),
      children_.end(),
      [child](const std::shared_ptr<MemoryPool>& e) {
        return e.get() == child;
      });

  if (iter != children_.end()) {
    children_.erase(iter);
  }
}

void MemoryPoolBase::removeSelf() {
  if (auto parentPtr = parent_.lock()) {
    parentPtr->dropChild(this);
  }
}

// Rounds up to a power of 2 >= size, or to a size halfway between
// two consecutive powers of two, i.e 8, 12, 16, 24, 32, .... This
// coincides with JEMalloc size classes.
size_t MemoryPoolBase::getPreferredSize(size_t size) {
  if (size < 8) {
    return 8;
  }
  int32_t bits = 63 - bits::countLeadingZeros(size);
  size_t lower = 1ULL << bits;
  // Size is a power of 2.
  if (lower == size) {
    return size;
  }
  // If size is below 1.5 * previous power of two, return 1.5 *
  // the previous power of two, else the next power of 2.
  if (lower + (lower / 2) >= size) {
    return lower + (lower / 2);
  }
  return lower * 2;
}

IMemoryManager& getProcessDefaultMemoryManager() {
  return MemoryManager<>::getProcessDefaultManager();
}

std::unique_ptr<ScopedMemoryPool> getDefaultScopedMemoryPool(int64_t cap) {
  auto& memoryManager = getProcessDefaultMemoryManager();
  return memoryManager.getScopedPool(cap);
}
} // namespace memory
} // namespace velox
} // namespace facebook
