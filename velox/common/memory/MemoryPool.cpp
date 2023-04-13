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

#include "velox/common/memory/MemoryPool.h"

#include "velox/common/base/BitUtil.h"
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

std::shared_ptr<MemoryUsageTracker> createMemoryUsageTracker(
    MemoryPool* parent,
    MemoryPool::Kind kind,
    const MemoryPool::Options& options) {
  if (parent == nullptr) {
    return options.trackUsage
        ? MemoryUsageTracker::create(options.capacity, options.checkUsageLeak)
        : nullptr;
  }
  if (parent->getMemoryUsageTracker() == nullptr) {
    return nullptr;
  }
  return parent->getMemoryUsageTracker()->addChild(
      kind == MemoryPool::Kind::kLeaf);
}
} // namespace

MemoryPool::MemoryPool(
    const std::string& name,
    Kind kind,
    std::shared_ptr<MemoryPool> parent,
    const Options& options)
    : name_(name),
      kind_(kind),
      alignment_{options.alignment},
      parent_(std::move(parent)),
      reclaimer_(options.reclaimer),
      checkUsageLeak_(options.checkUsageLeak) {
  MemoryAllocator::alignmentCheck(0, alignment_);
  VELOX_CHECK(parent_ != nullptr || kind_ == Kind::kAggregate);
}

MemoryPool::~MemoryPool() {
  VELOX_CHECK(children_.empty());
  if (parent_ != nullptr) {
    parent_->dropChild(this);
  }
}

std::string MemoryPool::kindString(Kind kind) {
  switch (kind) {
    case Kind::kLeaf:
      return "LEAF";
    case Kind::kAggregate:
      return "AGGREGATE";
    default:
      return fmt::format("UNKNOWN_{}", static_cast<int>(kind));
  }
}

std::ostream& operator<<(std::ostream& out, MemoryPool::Kind kind) {
  return out << MemoryPool::kindString(kind);
}

const std::string& MemoryPool::name() const {
  return name_;
}

MemoryPool::Kind MemoryPool::kind() const {
  return kind_;
}

MemoryPool* MemoryPool::parent() const {
  return parent_.get();
}

uint64_t MemoryPool::getChildCount() const {
  folly::SharedMutex::ReadHolder guard{childrenMutex_};
  return children_.size();
}

void MemoryPool::visitChildren(
    const std::function<bool(MemoryPool*)>& visitor) const {
  folly::SharedMutex::ReadHolder guard{childrenMutex_};
  for (const auto& entry : children_) {
    if (!visitor(entry.second)) {
      return;
    }
  }
}

std::shared_ptr<MemoryPool> MemoryPool::addChild(
    const std::string& name,
    Kind kind,
    std::shared_ptr<MemoryReclaimer> reclaimer) {
  checkPoolManagement();

  folly::SharedMutex::WriteHolder guard{childrenMutex_};
  VELOX_CHECK_EQ(
      children_.count(name),
      0,
      "Child memory pool {} already exists in {}",
      name,
      toString());
  auto child = genChild(shared_from_this(), name, kind, std::move(reclaimer));
  children_.emplace(name, child.get());
  return child;
}

void MemoryPool::dropChild(const MemoryPool* child) {
  checkPoolManagement();

  folly::SharedMutex::WriteHolder guard{childrenMutex_};
  const auto ret = children_.erase(child->name());
  VELOX_CHECK_EQ(
      ret,
      1,
      "Child memory pool {} doesn't exist in {}",
      child->name(),
      toString());
}

size_t MemoryPool::getPreferredSize(size_t size) {
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

MemoryReclaimer* MemoryPool::reclaimer() const {
  return reclaimer_.get();
}

bool MemoryPool::canReclaim() const {
  if (reclaimer_ == nullptr) {
    return false;
  }
  return reclaimer_->canReclaim(*this);
}

uint64_t MemoryPool::reclaimableBytes() const {
  if (reclaimer_ == nullptr) {
    return 0;
  }
  return reclaimer_->reclaimableBytes(*this);
}

uint64_t MemoryPool::reclaim(uint64_t targetBytes) {
  if (reclaimer_ == nullptr) {
    return 0;
  }
  return reclaimer_->reclaim(this, targetBytes);
}

void MemoryPool::enterArbitration() {
  if (reclaimer_ != nullptr) {
    reclaimer_->enterArbitration();
  }
}

void MemoryPool::leaveArbitration() {
  if (reclaimer_ != nullptr) {
    reclaimer_->leaveArbitration();
  }
}

MemoryPoolImpl::MemoryPoolImpl(
    MemoryManager* memoryManager,
    const std::string& name,
    Kind kind,
    std::shared_ptr<MemoryPool> parent,
    DestructionCallback destructionCb,
    const Options& options)
    : MemoryPool{name, kind, parent, options},
      memoryUsageTracker_(
          createMemoryUsageTracker(parent_.get(), kind, options)),
      memoryManager_{memoryManager},
      allocator_{&memoryManager_->getAllocator()},
      destructionCb_(std::move(destructionCb)),
      localMemoryUsage_{} {}

MemoryPoolImpl::~MemoryPoolImpl() {
  if (checkUsageLeak_ && (memoryUsageTracker_ != nullptr)) {
    const auto remainingBytes = memoryUsageTracker_->currentBytes();
    VELOX_CHECK_EQ(
        0,
        remainingBytes,
        "Memory pool {} should be destroyed only after all allocated memory has been freed. Remaining bytes allocated: {}, cumulative bytes allocated: {}, number of allocations: {}",
        name(),
        remainingBytes,
        memoryUsageTracker_->cumulativeBytes(),
        memoryUsageTracker_->numAllocs());
  }
  if (destructionCb_ != nullptr) {
    destructionCb_(this);
  }
}

/* static */
int64_t MemoryPoolImpl::sizeAlign(int64_t size) {
  const auto remainder = size % alignment_;
  return (remainder == 0) ? size : (size + alignment_ - remainder);
}

void* MemoryPoolImpl::allocate(int64_t size) {
  checkMemoryAllocation();

  const auto alignedSize = sizeAlign(size);
  reserve(alignedSize);
  void* buffer = allocator_->allocateBytes(alignedSize, alignment_);
  if (FOLLY_UNLIKELY(buffer == nullptr)) {
    release(alignedSize);
    VELOX_MEM_ALLOC_ERROR(fmt::format(
        "{} failed with {} bytes from {}", __FUNCTION__, size, toString()));
  }
  return buffer;
}

void* MemoryPoolImpl::allocateZeroFilled(int64_t numEntries, int64_t sizeEach) {
  checkMemoryAllocation();

  const auto alignedSize = sizeAlign(sizeEach * numEntries);
  reserve(alignedSize);
  void* buffer = allocator_->allocateZeroFilled(alignedSize);
  if (FOLLY_UNLIKELY(buffer == nullptr)) {
    release(alignedSize);
    VELOX_MEM_ALLOC_ERROR(fmt::format(
        "{} failed with {} entries and {} bytes each from {}",
        __FUNCTION__,
        numEntries,
        sizeEach,
        toString()));
  }
  return buffer;
}

void* MemoryPoolImpl::reallocate(
    void* FOLLY_NULLABLE p,
    int64_t size,
    int64_t newSize) {
  checkMemoryAllocation();

  const auto alignedSize = sizeAlign(size);
  const auto alignedNewSize = sizeAlign(newSize);
  reserve(alignedNewSize);
  void* newP = allocator_->allocateBytes(alignedNewSize, alignment_);
  if (FOLLY_UNLIKELY(newP == nullptr)) {
    free(p, alignedSize);
    release(alignedNewSize);
    VELOX_MEM_ALLOC_ERROR(fmt::format(
        "{} failed with {} new bytes and {} old bytes from {}",
        __FUNCTION__,
        newSize,
        size,
        toString()));
  }
  VELOX_CHECK_NOT_NULL(newP);
  if (p == nullptr) {
    return newP;
  }
  ::memcpy(newP, p, std::min(size, newSize));
  free(p, alignedSize);
  return newP;
}

void MemoryPoolImpl::free(void* p, int64_t size) {
  checkMemoryAllocation();

  const auto alignedSize = sizeAlign(size);
  allocator_->freeBytes(p, alignedSize);
  release(alignedSize);
}

void MemoryPoolImpl::allocateNonContiguous(
    MachinePageCount numPages,
    Allocation& out,
    MachinePageCount minSizeClass) {
  checkMemoryAllocation();
  VELOX_CHECK_GT(numPages, 0);

  if (!allocator_->allocateNonContiguous(
          numPages,
          out,
          [this](int64_t allocBytes, bool preAlloc) {
            if (preAlloc) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          },
          minSizeClass)) {
    VELOX_CHECK(out.empty());
    VELOX_MEM_ALLOC_ERROR(fmt::format(
        "{} failed with {} pages from {}", __FUNCTION__, numPages, toString()));
  }
  VELOX_CHECK(!out.empty());
  VELOX_CHECK_NULL(out.pool());
  out.setPool(this);
}

void MemoryPoolImpl::freeNonContiguous(Allocation& allocation) {
  checkMemoryAllocation();

  const int64_t freedBytes = allocator_->freeNonContiguous(allocation);
  VELOX_CHECK(allocation.empty());
  release(freedBytes);
}

MachinePageCount MemoryPoolImpl::largestSizeClass() const {
  return allocator_->largestSizeClass();
}

const std::vector<MachinePageCount>& MemoryPoolImpl::sizeClasses() const {
  return allocator_->sizeClasses();
}

void MemoryPoolImpl::allocateContiguous(
    MachinePageCount numPages,
    ContiguousAllocation& out) {
  checkMemoryAllocation();
  VELOX_CHECK_GT(numPages, 0);

  if (!allocator_->allocateContiguous(
          numPages, nullptr, out, [this](int64_t allocBytes, bool preAlloc) {
            if (preAlloc) {
              reserve(allocBytes);
            } else {
              release(allocBytes);
            }
          })) {
    VELOX_CHECK(out.empty());
    VELOX_MEM_ALLOC_ERROR(fmt::format(
        "{} failed with {} pages from {}", __FUNCTION__, numPages, toString()));
  }
  VELOX_CHECK(!out.empty());
  VELOX_CHECK_NULL(out.pool());
  out.setPool(this);
}

void MemoryPoolImpl::freeContiguous(ContiguousAllocation& allocation) {
  checkMemoryAllocation();

  const int64_t bytesToFree = allocation.size();
  allocator_->freeContiguous(allocation);
  VELOX_CHECK(allocation.empty());
  release(bytesToFree);
}

int64_t MemoryPoolImpl::getCurrentBytes() const {
  return getAggregateBytes();
}

int64_t MemoryPoolImpl::getMaxBytes() const {
  return std::max(getSubtreeMaxBytes(), localMemoryUsage_.getMaxBytes());
}

std::string MemoryPoolImpl::toString() const {
  return fmt::format(
      "Memory Pool[{} {} {}]",
      name_,
      kindString(kind_),
      MemoryAllocator::kindString(allocator_->kind()));
}

const std::shared_ptr<MemoryUsageTracker>&
MemoryPoolImpl::getMemoryUsageTracker() const {
  return memoryUsageTracker_;
}

int64_t MemoryPoolImpl::updateSubtreeMemoryUsage(int64_t size) {
  int64_t aggregateBytes;
  updateSubtreeMemoryUsage([&aggregateBytes, size](MemoryUsage& subtreeUsage) {
    aggregateBytes = subtreeUsage.getCurrentBytes() + size;
    subtreeUsage.setCurrentBytes(aggregateBytes);
  });
  return aggregateBytes;
}

uint16_t MemoryPoolImpl::getAlignment() const {
  return alignment_;
}

std::shared_ptr<MemoryPool> MemoryPoolImpl::genChild(
    std::shared_ptr<MemoryPool> parent,
    const std::string& name,
    Kind kind,
    std::shared_ptr<MemoryReclaimer> reclaimer) {
  return std::make_shared<MemoryPoolImpl>(
      memoryManager_,
      name,
      kind,
      parent,
      nullptr,
      Options{.alignment = alignment_, .reclaimer = std::move(reclaimer)});
}

const MemoryUsage& MemoryPoolImpl::getLocalMemoryUsage() const {
  return localMemoryUsage_;
}

int64_t MemoryPoolImpl::getAggregateBytes() const {
  int64_t aggregateBytes = localMemoryUsage_.getCurrentBytes();
  accessSubtreeMemoryUsage([&aggregateBytes](const MemoryUsage& subtreeUsage) {
    aggregateBytes += subtreeUsage.getCurrentBytes();
  });
  return aggregateBytes;
}

int64_t MemoryPoolImpl::getSubtreeMaxBytes() const {
  int64_t maxBytes;
  accessSubtreeMemoryUsage([&maxBytes](const MemoryUsage& subtreeUsage) {
    maxBytes = subtreeUsage.getMaxBytes();
  });
  return maxBytes;
}

void MemoryPoolImpl::accessSubtreeMemoryUsage(
    std::function<void(const MemoryUsage&)> visitor) const {
  folly::SharedMutex::ReadHolder readLock{subtreeUsageMutex_};
  visitor(subtreeMemoryUsage_);
}

void MemoryPoolImpl::updateSubtreeMemoryUsage(
    std::function<void(MemoryUsage&)> visitor) {
  folly::SharedMutex::WriteHolder writeLock{subtreeUsageMutex_};
  visitor(subtreeMemoryUsage_);
}

void MemoryPoolImpl::reserve(int64_t size) {
  checkMemoryAllocation();

  if (memoryUsageTracker_ != nullptr) {
    memoryUsageTracker_->update(size);
  }
  localMemoryUsage_.incrementCurrentBytes(size);

  bool success = memoryManager_->reserve(size);
  if (UNLIKELY(!success)) {
    // NOTE: If we can make the reserve and release a single transaction we
    // would have more accurate aggregates in intermediate states. However, this
    // is low-pri because we can only have inflated aggregates, and be on the
    // more conservative side.
    release(size);
    VELOX_MEM_MANAGER_CAP_EXCEEDED(memoryManager_->getMemoryQuota());
  }
}

void MemoryPoolImpl::release(int64_t size) {
  checkMemoryAllocation();

  memoryManager_->release(size);
  localMemoryUsage_.incrementCurrentBytes(-size);
  if (memoryUsageTracker_ != nullptr) {
    memoryUsageTracker_->update(-size);
  }
}
} // namespace facebook::velox::memory
