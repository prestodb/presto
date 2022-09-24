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

#include <atomic>
#include <chrono>
#include <limits>
#include <list>
#include <memory>
#include <string>

#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <velox/common/base/Exceptions.h>
#include "folly/CPortability.h"
#include "folly/Likely.h"
#include "folly/Random.h"
#include "folly/SharedMutex.h"
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/GTestMacros.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/memory/MappedMemory.h"
#include "velox/common/memory/MemoryUsage.h"
#include "velox/common/memory/MemoryUsageTracker.h"

DECLARE_int32(memory_usage_aggregation_interval_millis);

namespace facebook {
namespace velox {
namespace memory {
constexpr uint16_t kNoAlignment = alignof(max_align_t);
constexpr uint16_t kDefaultAlignment = 64;

#define VELOX_MEM_MANAGER_CAP_EXCEEDED(cap)                         \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kMemCapExceeded.c_str(),       \
      /* isRetriable */ true,                                       \
      "Exceeded memory manager cap of {} MB",                       \
      (cap) / 1024 / 1024);

#define VELOX_MEM_MANUAL_CAP()                                      \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kMemCapExceeded.c_str(),       \
      /* isRetriable */ true,                                       \
      "Memory allocation manually capped");

class ScopedMemoryPool;

class AbstractMemoryPool {
 public:
  virtual ~AbstractMemoryPool() {}
  virtual void* FOLLY_NULLABLE allocate(int64_t size) = 0;
  virtual void* FOLLY_NULLABLE
  allocateZeroFilled(int64_t numMembers, int64_t sizeEach) = 0;
  virtual void* FOLLY_NULLABLE
  reallocate(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) = 0;
  virtual void free(void* FOLLY_NULLABLE p, int64_t size) = 0;
  virtual size_t getPreferredSize(size_t size) = 0;
};

class MemoryPool : public AbstractMemoryPool {
 public:
  virtual ~MemoryPool() {}
  // Accounting/governing methods, lazily managed and expensive for intermediate
  // tree nodes, cheap for leaf nodes.
  virtual int64_t getCurrentBytes() const = 0;
  virtual int64_t getMaxBytes() const = 0;
  // Tracks memory usage from leaf nodes to root and updates immediately
  // with atomic operations.
  // Unlike pool's getCurrentBytes(), getMaxBytes(), trace's API's returns
  // the aggregated usage of subtree. See 'ScopedChildUsageTest' for
  // difference in their behavior.
  virtual void setMemoryUsageTracker(
      const std::shared_ptr<MemoryUsageTracker>& tracker) = 0;
  virtual const std::shared_ptr<MemoryUsageTracker>& getMemoryUsageTracker()
      const = 0;
  // Used for external aggregation.
  virtual void setSubtreeMemoryUsage(int64_t size) = 0;
  virtual int64_t updateSubtreeMemoryUsage(int64_t size) = 0;
  // Get the cap for the memory node and its subtree.
  virtual int64_t getCap() const = 0;
  virtual uint16_t getAlignment() const = 0;

  ////////////////////// resource governing methods ////////////////////
  // Called by MemoryManager and MemoryPool upon memory usage updates and
  // propagates down the subtree recursively.
  virtual void capMemoryAllocation() = 0;
  // Called by MemoryManager and propagates down recursively if applicable.
  virtual void uncapMemoryAllocation() = 0;
  // We might need to freeze memory allocating operations under severe global
  // memory pressure.
  virtual bool isMemoryCapped() const = 0;

  ////////////////////////// tree methods ////////////////////////////
  // TODO: might be able to move implementation to abstract class
  // name of the node, preferably unique among siblings, but not enforced.
  virtual const std::string& getName() const = 0;
  virtual std::weak_ptr<MemoryPool> getWeakPtr() = 0;
  virtual MemoryPool& getChildByName(const std::string& name) = 0;
  virtual uint64_t getChildCount() const = 0;
  virtual void visitChildren(
      std::function<void(MemoryPool* FOLLY_NONNULL)> visitor) const = 0;

  virtual std::shared_ptr<MemoryPool> genChild(
      std::weak_ptr<MemoryPool> parent,
      const std::string& name,
      int64_t cap) = 0;
  virtual MemoryPool& addChild(
      const std::string& name,
      int64_t cap = kMaxMemory) = 0;
  virtual std::unique_ptr<ScopedMemoryPool> addScopedChild(
      const std::string& name,
      int64_t cap = kMaxMemory) = 0;
  virtual void dropChild(const MemoryPool* FOLLY_NONNULL child) = 0;

  // Used to explicitly remove self when a user is done with the node, since
  // nodes are owned by parents and users only get object references. Any
  // reference to self would be invalid after this call. This will also drop the
  // entire subtree.
  virtual void removeSelf() = 0;

  // Used to manage existing externally allocated memories without doing a new
  // allocation.
  virtual void reserve(int64_t /* bytes */) {
    VELOX_NYI("reserve() needs to be implemented in derived memory pool.");
  }
  // Sometimes in memory governance we want to mock an update for quota
  // accounting purposes and different implementations can
  // choose to accommodate this differently.
  virtual void release(int64_t /* bytes */, bool /* mock */ = false) {
    VELOX_NYI("release() needs to be implemented in derived memory pool.");
  }
};

namespace detail {
static inline MemoryPool& getCheckedReference(std::weak_ptr<MemoryPool> ptr) {
  auto sptr = ptr.lock();
  VELOX_USER_CHECK(sptr);
  return *sptr;
};
} // namespace detail

class ScopedMemoryPool final : public MemoryPool {
 public:
  explicit ScopedMemoryPool(std::weak_ptr<MemoryPool> poolPtr)
      : poolPtr_{poolPtr}, pool_{detail::getCheckedReference(poolPtr)} {}

  ~ScopedMemoryPool() {
    if (auto sptr = poolPtr_.lock()) {
      sptr->removeSelf();
    }
  }

  int64_t getCurrentBytes() const override {
    return pool_.getCurrentBytes();
  }

  int64_t getMaxBytes() const override {
    return pool_.getMaxBytes();
  }

  void setMemoryUsageTracker(
      const std::shared_ptr<MemoryUsageTracker>& tracker) override {
    pool_.setMemoryUsageTracker(tracker);
  }

  const std::shared_ptr<MemoryUsageTracker>& getMemoryUsageTracker()
      const override {
    return pool_.getMemoryUsageTracker();
  }

  void* FOLLY_NULLABLE allocate(int64_t size) override {
    return pool_.allocate(size);
  }

  void* FOLLY_NULLABLE
  reallocate(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) override {
    return pool_.reallocate(p, size, newSize);
  }

  void free(void* FOLLY_NULLABLE p, int64_t size) override {
    return pool_.free(p, size);
  }

  // Rounds up to a power of 2 >= size, or to a size halfway between
  // two consecutive powers of two, i.e 8, 12, 16, 24, 32, .... This
  // coincides with JEMalloc size classes.
  size_t getPreferredSize(size_t size) override {
    return pool_.getPreferredSize(size);
  }

  int64_t updateSubtreeMemoryUsage(int64_t size) override {
    return pool_.updateSubtreeMemoryUsage(size);
  }

  void* FOLLY_NULLABLE
  allocateZeroFilled(int64_t numMembers, int64_t sizeEach) override {
    return pool_.allocateZeroFilled(numMembers, sizeEach);
  }

  int64_t getCap() const override {
    return pool_.getCap();
  }

  uint16_t getAlignment() const override {
    return pool_.getAlignment();
  }

  void capMemoryAllocation() override {
    pool_.capMemoryAllocation();
  }

  void uncapMemoryAllocation() override {
    pool_.uncapMemoryAllocation();
  }
  bool isMemoryCapped() const override {
    return pool_.isMemoryCapped();
  }

  const std::string& getName() const override {
    return pool_.getName();
  }

  std::weak_ptr<MemoryPool> getWeakPtr() override {
    VELOX_NYI();
  }

  MemoryPool& getChildByName(const std::string& name) override {
    return pool_.getChildByName(name);
  }

  uint64_t getChildCount() const override {
    return pool_.getChildCount();
  }

  std::shared_ptr<MemoryPool> genChild(
      std::weak_ptr<MemoryPool> parent,
      const std::string& name,
      int64_t cap) override {
    return pool_.genChild(parent, name, cap);
  }

  MemoryPool& addChild(const std::string& name, int64_t cap = kMaxMemory)
      override {
    return pool_.addChild(name, cap);
  }

  std::unique_ptr<memory::ScopedMemoryPool> addScopedChild(
      const std::string& name,
      int64_t cap = kMaxMemory) override {
    return pool_.addScopedChild(name, cap);
  }

  void removeSelf() override {
    VELOX_NYI();
  }

  void visitChildren(
      std::function<void(velox::memory::MemoryPool* FOLLY_NONNULL)> visitor)
      const override {
    pool_.visitChildren(visitor);
  }

  void dropChild(
      const velox::memory::MemoryPool* FOLLY_NONNULL child) override {
    pool_.dropChild(child);
  }

  void setSubtreeMemoryUsage(int64_t size) override {
    pool_.setSubtreeMemoryUsage(size);
  }

  MemoryPool& getPool() {
    return pool_;
  }

  void reserve(int64_t bytes) override {
    pool_.reserve(bytes);
  }

  void release(int64_t bytes, bool mock = false) override {
    pool_.release(bytes, mock);
  }

 private:
  std::weak_ptr<MemoryPool> poolPtr_;
  MemoryPool& pool_;
};

// A standard allocator interface for the actual allocator of the memory
// node tree.
class MemoryAllocator {
 public:
  static std::shared_ptr<MemoryAllocator> createDefaultAllocator();

  virtual ~MemoryAllocator() {}

  virtual void* FOLLY_NULLABLE alloc(int64_t size);
  virtual void* FOLLY_NULLABLE
  allocZeroFilled(int64_t numMembers, int64_t sizeEach);
  // TODO: might be able to collapse this with templated class
  virtual void* FOLLY_NULLABLE allocAligned(uint16_t alignment, int64_t size);
  virtual void* FOLLY_NULLABLE
  realloc(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize);
  virtual void* FOLLY_NULLABLE reallocAligned(
      void* FOLLY_NULLABLE p,
      uint16_t alignment,
      int64_t size,
      int64_t newSize);
  virtual void free(void* FOLLY_NULLABLE p, int64_t size);
};

// An allocator that uses memory::MappedMemory to allocate memory. We leverage
// MappedMemory for relatively small allocations. Allocations less than 3/4 of
// smallest size class and larger than largest size class are delegated to
// malloc still.
class MmapMemoryAllocator : public MemoryAllocator {
 public:
  static std::shared_ptr<MmapMemoryAllocator> createDefaultAllocator();

  MmapMemoryAllocator() : mappedMemory_(MappedMemory::getInstance()) {}
  void* FOLLY_NULLABLE alloc(int64_t size) override;
  void* FOLLY_NULLABLE
  allocZeroFilled(int64_t numMembers, int64_t sizeEach) override;
  void* FOLLY_NULLABLE allocAligned(uint16_t alignment, int64_t size) override;
  void* FOLLY_NULLABLE
  realloc(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) override;
  void* FOLLY_NULLABLE reallocAligned(
      void* FOLLY_NULLABLE p,
      uint16_t alignment,
      int64_t size,
      int64_t newSize) override;
  void free(void* FOLLY_NULLABLE p, int64_t size) override;

  MappedMemory* FOLLY_NONNULL mappedMemory() {
    return mappedMemory_;
  }

 private:
  MappedMemory* FOLLY_NONNULL mappedMemory_;
};

class MemoryPoolBase : public std::enable_shared_from_this<MemoryPoolBase>,
                       public MemoryPool {
 public:
  MemoryPoolBase(const std::string& name, std::weak_ptr<MemoryPool> parent);
  virtual ~MemoryPoolBase();

  // Gets the unique name corresponding to the node in MemoryManager's
  // registry.
  const std::string& getName() const override;
  std::weak_ptr<MemoryPool> getWeakPtr() override;
  MemoryPool& getChildByName(const std::string& name) override;
  uint64_t getChildCount() const override;
  void visitChildren(
      std::function<void(MemoryPool* FOLLY_NONNULL)> visitor) const override;

  MemoryPool& addChild(const std::string& name, int64_t cap = kMaxMemory)
      override;
  std::unique_ptr<ScopedMemoryPool> addScopedChild(
      const std::string& name,
      int64_t cap = kMaxMemory) override;
  // Drops child (and implicitly the entire subtree) to reclaim memory.
  // Derived classes can override this behavior. e.g. append only semantics.
  void dropChild(const MemoryPool* FOLLY_NONNULL child) override;
  // Used to explicitly remove self when a user is done with the node, since
  // nodes are owned by parents and users only get object references. Any
  // reference to self would be invalid after this call. This will also drop the
  // entire subtree.
  void removeSelf() override;

  size_t getPreferredSize(size_t size) override;

 protected:
  const std::string name_;
  std::weak_ptr<MemoryPool> parent_;

  mutable folly::SharedMutex childrenMutex_;
  std::list<std::shared_ptr<MemoryPool>> children_;
};

template <typename Allocator, uint16_t ALIGNMENT>
class MemoryManager;
template <typename Allocator, uint16_t ALIGNMENT>
class MemoryPoolImpl;

// Signaling for trimming and dropping. Component can either derive this class
// or use this class as pure signaling. Dropping of component can be implicit.
// NOTE: MemoryPoolImpl being templated by type means we don't track memory
// across different allocators and need external workarounds, e.g. static
// boundaries.
template <
    typename Allocator = MemoryAllocator,
    uint16_t ALIGNMENT = kNoAlignment>
class MemoryPoolImpl : public MemoryPoolBase {
 public:
  // Should perhaps make this method private so that we only create node through
  // parent.
  explicit MemoryPoolImpl(
      MemoryManager<Allocator, ALIGNMENT>& memoryManager,
      const std::string& name,
      std::weak_ptr<MemoryPool> parent,
      int64_t cap = kMaxMemory);

  // Actual memory allocation operations. Can be delegated.
  // Access global MemoryManager to check usage of current node and enforce
  // memory cap accordingly. Since MemoryManager walks the MemoryPoolImpl
  // tree periodically, this is slightly stale and we have to reserve our own
  // overhead.
  void* FOLLY_NULLABLE allocate(int64_t size) override;
  void* FOLLY_NULLABLE
  allocateZeroFilled(int64_t numMembers, int64_t sizeEach) override;

  // No-op for attempts to shrink buffer.
  void* FOLLY_NULLABLE
  reallocate(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) override;
  void free(void* FOLLY_NULLABLE p, int64_t size) override;

  //////////////////// Memory Management methods /////////////////////
  // Library checks for low memory mode on a push model. The respective root,
  // component level or global, would compute for memory pressure.
  // This is the signaling mechanism the customer application can use to make
  // all subcomponents start trimming memory usage.
  // virtual bool shouldTrim() const {
  //   return trimming_;
  // }
  // // Set by MemoryManager in periodic refresh threads. Stores the trim
  // target
  // // state potentially for a more granular/simplified global control.
  // virtual void startTrimming(int64_t target) {
  //   trimming_ = true;
  //   trimTarget_ = target;
  // }
  // // Resets the trim flag and trim target.
  // virtual void stopTrimming() {
  //   trimming_ = false;
  //   trimTarget_ = std::numeric_limits<int64_t>::max();
  // }

  // TODO: Consider putting these in base class also.
  int64_t getCurrentBytes() const override;
  int64_t getMaxBytes() const override;
  void setMemoryUsageTracker(
      const std::shared_ptr<MemoryUsageTracker>& tracker) override;
  const std::shared_ptr<MemoryUsageTracker>& getMemoryUsageTracker()
      const override;
  void setSubtreeMemoryUsage(int64_t size) override;
  int64_t updateSubtreeMemoryUsage(int64_t size) override;
  // Get the cap for the memory node and its subtree.
  int64_t getCap() const override;
  uint16_t getAlignment() const override;

  void capMemoryAllocation() override;
  void uncapMemoryAllocation() override;
  bool isMemoryCapped() const override;

  std::shared_ptr<MemoryPool> genChild(
      std::weak_ptr<MemoryPool> parent,
      const std::string& name,
      int64_t cap) override;

  // Gets the memory allocation stats of the MemoryPoolImpl attached to the
  // current MemoryPoolImpl. Not to be confused with total memory usage of the
  // subtree.
  const MemoryUsage& getLocalMemoryUsage() const;

  // Get the total memory consumption of the subtree, self + all recursive
  // children.
  int64_t getAggregateBytes() const;
  int64_t getSubtreeMaxBytes() const;

  // TODO: consider returning bool instead.
  void reserve(int64_t size) override;
  void release(int64_t size, bool mock = false) override;

 private:
  VELOX_FRIEND_TEST(MemoryPoolTest, Ctor);

  template <uint16_t A>
  struct ALIGNER {};

  template <uint16_t A, typename = std::enable_if_t<A != kNoAlignment>>
  int64_t sizeAlign(ALIGNER<A> /* unused */, int64_t size) {
    auto remainder = size % ALIGNMENT;
    return (remainder == 0) ? size : (size + ALIGNMENT - remainder);
  }

  template <uint16_t A>
  int64_t sizeAlign(ALIGNER<kNoAlignment> /* unused */, int64_t size) {
    return size;
  }

  template <uint16_t A, typename = std::enable_if_t<A != kNoAlignment>>
  void* FOLLY_NULLABLE allocAligned(ALIGNER<A> /* unused */, int64_t size) {
    return allocator_.allocAligned(A, size);
  }

  template <uint16_t A>
  void* FOLLY_NULLABLE
  allocAligned(ALIGNER<kNoAlignment> /* unused */, int64_t size) {
    return allocator_.alloc(size);
  }

  template <uint16_t A, typename = std::enable_if_t<A != kNoAlignment>>
  void* FOLLY_NULLABLE reallocAligned(
      ALIGNER<A> /* unused */,
      void* FOLLY_NULLABLE p,
      int64_t size,
      int64_t newSize) {
    return allocator_.reallocAligned(p, A, size, newSize);
  }

  template <uint16_t A>
  void* FOLLY_NULLABLE reallocAligned(
      ALIGNER<kNoAlignment> /* unused */,
      void* FOLLY_NULLABLE p,
      int64_t size,
      int64_t newSize) {
    return allocator_.realloc(p, size, newSize);
  }

  void accessSubtreeMemoryUsage(
      std::function<void(const MemoryUsage&)> visitor) const;
  void updateSubtreeMemoryUsage(std::function<void(MemoryUsage&)> visitor);

  MemoryManager<Allocator, ALIGNMENT>& memoryManager_;

  // Memory allocated attributed to the memory node.
  MemoryUsage localMemoryUsage_;
  std::shared_ptr<MemoryUsageTracker> memoryUsageTracker_;
  mutable folly::SharedMutex subtreeUsageMutex_;
  MemoryUsage subtreeMemoryUsage_;
  int64_t cap_;
  std::atomic_bool capped_{false};

  Allocator& allocator_;
};

constexpr folly::StringPiece kRootNodeName{"__root__"};

class IMemoryManager {
 public:
  virtual ~IMemoryManager() {}
  // Returns the total memory usage allowed under this MemoryManager.
  // MemoryManager maintains this quota as a hard cap, and any allocation
  // that would cause a quota breach results in exceptions.
  virtual int64_t getMemoryQuota() const = 0;
  // Power users that want to explicitly modify the tree should get the root of
  // the tree. Users opting to use this api needs to explicitly manage the life
  // cycle of the memory pools by calling removeSelf() after use.
  // TODO: perhaps the root pool should be a specialized pool that
  //        * doesn't do allocation
  //        * cannot be removed
  virtual MemoryPool& getRoot() const = 0;

  // Adds a child pool to root for use. The actual return type is
  // std::unique_ptr of a wrapper class that knows to remove itself.
  virtual std::unique_ptr<ScopedMemoryPool> getScopedPool(
      int64_t cap = kMaxMemory) = 0;

  // Returns the current total memory usage under the MemoryManager.
  virtual int64_t getTotalBytes() const = 0;
  // Reserves size for the allocation. Return a true if the total usage remains
  // under quota after the reservation. Caller is responsible for releasing the
  // offending reservation.
  virtual bool reserve(int64_t size) = 0;
  // Subtracts from current total and regain memory quota.
  virtual void release(int64_t size) = 0;
};

// For now, users wanting multiple different allocators would need to
// instantiate different MemoryManager classes and manage them across
// static boundaries.
template <
    typename Allocator = MemoryAllocator,
    uint16_t ALIGNMENT = kNoAlignment>
class MemoryManager final : public IMemoryManager {
 public:
  // Tries to get the process singleton manager. If not previously initialized,
  // the process singleton manager will be initialized with the given quota.
  FOLLY_EXPORT static MemoryManager<Allocator, ALIGNMENT>&
  getProcessDefaultManager(
      int64_t quota = kMaxMemory,
      bool ensureQuota = false) {
    static MemoryManager<Allocator, ALIGNMENT> manager{
        Allocator::createDefaultAllocator(), quota};
    auto actualQuota = manager.getMemoryQuota();
    VELOX_USER_CHECK(
        !ensureQuota || actualQuota == quota,
        "Process level manager manager created with input_quota: {}, current_quota: {}",
        quota,
        actualQuota);

    return manager;
  }

  explicit MemoryManager(int64_t memoryQuota = kMaxMemory);

  explicit MemoryManager(
      std::shared_ptr<Allocator> allocator,
      int64_t memoryQuota = kMaxMemory);
  ~MemoryManager();

  int64_t getMemoryQuota() const final;
  MemoryPool& getRoot() const final;

  std::unique_ptr<ScopedMemoryPool> getScopedPool(
      int64_t cap = kMaxMemory) final;

  int64_t getTotalBytes() const final;
  bool reserve(int64_t size) final;
  void release(int64_t size) final;

  Allocator& getAllocator();

 private:
  VELOX_FRIEND_TEST(MemoryPoolImplTest, CapSubtree);
  VELOX_FRIEND_TEST(MemoryPoolImplTest, CapAllocation);
  VELOX_FRIEND_TEST(MemoryPoolImplTest, UncapMemory);
  VELOX_FRIEND_TEST(MemoryPoolImplTest, MemoryManagerGlobalCap);
  VELOX_FRIEND_TEST(MultiThreadingUncappingTest, Flat);
  VELOX_FRIEND_TEST(MultiThreadingUncappingTest, SimpleTree);

  std::shared_ptr<Allocator> allocator_;
  const int64_t memoryQuota_;
  std::shared_ptr<MemoryPool> root_;
  mutable folly::SharedMutex mutex_;
  std::atomic_long totalBytes_{0};
};

template <typename Allocator, uint16_t ALIGNMENT>
MemoryPoolImpl<Allocator, ALIGNMENT>::MemoryPoolImpl(
    MemoryManager<Allocator, ALIGNMENT>& memoryManager,
    const std::string& name,
    std::weak_ptr<MemoryPool> parent,
    int64_t cap)
    : MemoryPoolBase{name, parent},
      memoryManager_{memoryManager},
      localMemoryUsage_{},
      cap_{cap},
      allocator_{memoryManager_.getAllocator()} {
  VELOX_USER_CHECK_GT(cap, 0);
}

template <typename Allocator, uint16_t ALIGNMENT>
void* FOLLY_NULLABLE
MemoryPoolImpl<Allocator, ALIGNMENT>::allocate(int64_t size) {
  if (this->isMemoryCapped()) {
    VELOX_MEM_MANUAL_CAP();
  }
  auto alignedSize = sizeAlign<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, size);
  reserve(alignedSize);
  return allocAligned<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, alignedSize);
}

template <typename Allocator, uint16_t ALIGNMENT>
void* FOLLY_NULLABLE MemoryPoolImpl<Allocator, ALIGNMENT>::allocateZeroFilled(
    int64_t numMembers,
    int64_t sizeEach) {
  VELOX_USER_CHECK_EQ(sizeEach, 1);
  auto alignedSize = sizeAlign<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, numMembers);
  if (this->isMemoryCapped()) {
    VELOX_MEM_MANUAL_CAP();
  }
  reserve(alignedSize * sizeEach);
  return allocator_.allocZeroFilled(alignedSize, sizeEach);
}

template <typename Allocator, uint16_t ALIGNMENT>
void* FOLLY_NULLABLE MemoryPoolImpl<Allocator, ALIGNMENT>::reallocate(
    void* FOLLY_NULLABLE p,
    int64_t size,
    int64_t newSize) {
  auto alignedSize = sizeAlign<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, size);
  auto alignedNewSize = sizeAlign<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, newSize);
  int64_t difference = alignedNewSize - alignedSize;
  if (UNLIKELY(difference <= 0)) {
    // Track and pretend the shrink took place for accounting purposes.
    release(-difference, true);
    return p;
  }

  reserve(difference);
  void* newP = reallocAligned<ALIGNMENT>(
      ALIGNER<ALIGNMENT>{}, p, alignedSize, alignedNewSize);
  if (UNLIKELY(!newP)) {
    free(p, alignedSize);
    auto errorMessage = fmt::format(
        MEM_CAP_EXCEEDED_ERROR_FORMAT,
        succinctBytes(cap_),
        succinctBytes(difference));
    VELOX_MEM_CAP_EXCEEDED(errorMessage);
  }

  return newP;
}

template <typename Allocator, uint16_t ALIGNMENT>
void MemoryPoolImpl<Allocator, ALIGNMENT>::free(
    void* FOLLY_NULLABLE p,
    int64_t size) {
  auto alignedSize = sizeAlign<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, size);
  allocator_.free(p, alignedSize);
  release(alignedSize);
}

template <typename Allocator, uint16_t ALIGNMENT>
int64_t MemoryPoolImpl<Allocator, ALIGNMENT>::getCurrentBytes() const {
  return getAggregateBytes();
}

template <typename Allocator, uint16_t ALIGNMENT>
int64_t MemoryPoolImpl<Allocator, ALIGNMENT>::getMaxBytes() const {
  return std::max(getSubtreeMaxBytes(), localMemoryUsage_.getMaxBytes());
}

template <typename Allocator, uint16_t ALIGNMENT>
void MemoryPoolImpl<Allocator, ALIGNMENT>::setMemoryUsageTracker(
    const std::shared_ptr<MemoryUsageTracker>& tracker) {
  memoryUsageTracker_ = tracker;
  memoryUsageTracker_->update(getCurrentBytes());
}

template <typename Allocator, uint16_t ALIGNMENT>
const std::shared_ptr<MemoryUsageTracker>&
MemoryPoolImpl<Allocator, ALIGNMENT>::getMemoryUsageTracker() const {
  return memoryUsageTracker_;
}

template <typename Allocator, uint16_t ALIGNMENT>
void MemoryPoolImpl<Allocator, ALIGNMENT>::setSubtreeMemoryUsage(int64_t size) {
  updateSubtreeMemoryUsage([size](MemoryUsage& subtreeUsage) {
    subtreeUsage.setCurrentBytes(size);
  });
}

template <typename Allocator, uint16_t ALIGNMENT>
int64_t MemoryPoolImpl<Allocator, ALIGNMENT>::updateSubtreeMemoryUsage(
    int64_t size) {
  int64_t aggregateBytes;
  updateSubtreeMemoryUsage([&aggregateBytes, size](MemoryUsage& subtreeUsage) {
    aggregateBytes = subtreeUsage.getCurrentBytes() + size;
    subtreeUsage.setCurrentBytes(aggregateBytes);
  });
  return aggregateBytes;
}

template <typename Allocator, uint16_t ALIGNMENT>
int64_t MemoryPoolImpl<Allocator, ALIGNMENT>::getCap() const {
  return cap_;
}

template <typename Allocator, uint16_t ALIGNMENT>
uint16_t MemoryPoolImpl<Allocator, ALIGNMENT>::getAlignment() const {
  return ALIGNMENT;
}

template <typename Allocator, uint16_t ALIGNMENT>
void MemoryPoolImpl<Allocator, ALIGNMENT>::capMemoryAllocation() {
  capped_.store(true);
  for (const auto& child : children_) {
    child->capMemoryAllocation();
  }
}

template <typename Allocator, uint16_t ALIGNMENT>
void MemoryPoolImpl<Allocator, ALIGNMENT>::uncapMemoryAllocation() {
  // This means if we try to post-order traverse the tree like we do
  // in MemoryManager, only parent has the right to lift the cap.
  // This suffices because parent will then recursively lift the cap on the
  // entire tree.
  if (getAggregateBytes() > getCap()) {
    return;
  }
  if (auto parentPtr = parent_.lock()) {
    if (parentPtr->isMemoryCapped()) {
      return;
    }
  }
  capped_.store(false);
  visitChildren([](MemoryPool* child) { child->uncapMemoryAllocation(); });
}

template <typename Allocator, uint16_t ALIGNMENT>
bool MemoryPoolImpl<Allocator, ALIGNMENT>::isMemoryCapped() const {
  return capped_.load();
}

template <typename Allocator, uint16_t ALIGNMENT>
std::shared_ptr<MemoryPool> MemoryPoolImpl<Allocator, ALIGNMENT>::genChild(
    std::weak_ptr<MemoryPool> parent,
    const std::string& name,
    int64_t cap) {
  return std::make_shared<MemoryPoolImpl<Allocator, ALIGNMENT>>(
      memoryManager_, name, parent, cap);
}

template <typename Allocator, uint16_t ALIGNMENT>
const MemoryUsage& MemoryPoolImpl<Allocator, ALIGNMENT>::getLocalMemoryUsage()
    const {
  return localMemoryUsage_;
}

template <typename Allocator, uint16_t ALIGNMENT>
int64_t MemoryPoolImpl<Allocator, ALIGNMENT>::getAggregateBytes() const {
  int64_t aggregateBytes = localMemoryUsage_.getCurrentBytes();
  accessSubtreeMemoryUsage([&aggregateBytes](const MemoryUsage& subtreeUsage) {
    aggregateBytes += subtreeUsage.getCurrentBytes();
  });
  return aggregateBytes;
}

template <typename Allocator, uint16_t ALIGNMENT>
int64_t MemoryPoolImpl<Allocator, ALIGNMENT>::getSubtreeMaxBytes() const {
  int64_t maxBytes;
  accessSubtreeMemoryUsage([&maxBytes](const MemoryUsage& subtreeUsage) {
    maxBytes = subtreeUsage.getMaxBytes();
  });
  return maxBytes;
}

template <typename Allocator, uint16_t ALIGNMENT>
void MemoryPoolImpl<Allocator, ALIGNMENT>::accessSubtreeMemoryUsage(
    std::function<void(const MemoryUsage&)> visitor) const {
  folly::SharedMutex::ReadHolder readLock{subtreeUsageMutex_};
  visitor(subtreeMemoryUsage_);
}

template <typename Allocator, uint16_t ALIGNMENT>
void MemoryPoolImpl<Allocator, ALIGNMENT>::updateSubtreeMemoryUsage(
    std::function<void(MemoryUsage&)> visitor) {
  folly::SharedMutex::WriteHolder writeLock{subtreeUsageMutex_};
  visitor(subtreeMemoryUsage_);
}

template <typename Allocator, uint16_t ALIGNMENT>
void MemoryPoolImpl<Allocator, ALIGNMENT>::reserve(int64_t size) {
  if (memoryUsageTracker_) {
    memoryUsageTracker_->update(size);
  }
  localMemoryUsage_.incrementCurrentBytes(size);

  bool success = memoryManager_.reserve(size);
  bool manualCap = isMemoryCapped();
  int64_t aggregateBytes = getAggregateBytes();
  if (UNLIKELY(!success || manualCap || aggregateBytes > cap_)) {
    // NOTE: If we can make the reserve and release a single transaction we
    // would have more accurate aggregates in intermediate states. However, this
    // is low-pri because we can only have inflated aggregates, and be on the
    // more conservative side.
    release(size);
    if (!success) {
      VELOX_MEM_MANAGER_CAP_EXCEEDED(memoryManager_.getMemoryQuota());
    }
    if (manualCap) {
      VELOX_MEM_MANUAL_CAP();
    }
    auto errorMessage = fmt::format(
        MEM_CAP_EXCEEDED_ERROR_FORMAT,
        succinctBytes(cap_),
        succinctBytes(size));
    VELOX_MEM_CAP_EXCEEDED(errorMessage);
  }
}

template <typename Allocator, uint16_t ALIGNMENT>
void MemoryPoolImpl<Allocator, ALIGNMENT>::release(int64_t size, bool mock) {
  memoryManager_.release(size);
  localMemoryUsage_.incrementCurrentBytes(-size);
  if (memoryUsageTracker_) {
    memoryUsageTracker_->update(-size, mock);
  }
}

namespace detail {
static inline int64_t getTimeInUsec() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
} // namespace detail

template <typename Allocator, uint16_t ALIGNMENT>
MemoryManager<Allocator, ALIGNMENT>::MemoryManager(int64_t memoryQuota)
    : MemoryManager(Allocator::createDefaultAllocator(), memoryQuota) {}

template <typename Allocator, uint16_t ALIGNMENT>
MemoryManager<Allocator, ALIGNMENT>::MemoryManager(
    std::shared_ptr<Allocator> allocator,
    int64_t memoryQuota)
    : allocator_{std::move(allocator)},
      memoryQuota_{memoryQuota},
      root_{std::make_shared<MemoryPoolImpl<Allocator, ALIGNMENT>>(
          *this,
          kRootNodeName.str(),
          std::weak_ptr<MemoryPool>(),
          memoryQuota)} {
  VELOX_USER_CHECK_GE(memoryQuota_, 0);
}

template <typename Allocator, uint16_t ALIGNMENT>
MemoryManager<Allocator, ALIGNMENT>::~MemoryManager() {
  auto currentBytes = getTotalBytes();
  if (currentBytes) {
    LOG(INFO) << "Leaked total memory of " << currentBytes << " bytes.";
  }
}

template <typename Allocator, uint16_t ALIGNMENT>
int64_t MemoryManager<Allocator, ALIGNMENT>::getMemoryQuota() const {
  return memoryQuota_;
}

template <typename Allocator, uint16_t ALIGNMENT>
MemoryPool& MemoryManager<Allocator, ALIGNMENT>::getRoot() const {
  return *root_;
}

template <typename Allocator, uint16_t ALIGNMENT>
std::unique_ptr<ScopedMemoryPool>
MemoryManager<Allocator, ALIGNMENT>::getScopedPool(int64_t cap) {
  auto& pool = root_->addChild(
      fmt::format(
          "default_usage_node_{}",
          folly::to<std::string>(folly::Random::rand64())),
      cap);
  return std::make_unique<ScopedMemoryPool>(pool.getWeakPtr());
}

template <typename Allocator, uint16_t ALIGNMENT>
int64_t MemoryManager<Allocator, ALIGNMENT>::getTotalBytes() const {
  return totalBytes_.load(std::memory_order_relaxed);
}

template <typename Allocator, uint16_t ALIGNMENT>
bool MemoryManager<Allocator, ALIGNMENT>::reserve(int64_t size) {
  return totalBytes_.fetch_add(size, std::memory_order_relaxed) + size <=
      memoryQuota_;
}

template <typename Allocator, uint16_t ALIGNMENT>
void MemoryManager<Allocator, ALIGNMENT>::release(int64_t size) {
  totalBytes_.fetch_sub(size, std::memory_order_relaxed);
}

template <typename Allocator, uint16_t ALIGNMENT>
Allocator& MemoryManager<Allocator, ALIGNMENT>::getAllocator() {
  return *allocator_;
}

IMemoryManager& getProcessDefaultMemoryManager();

// adds a new scoped child memory pool to the root
// the new scoped child pool memory cap is set to the input value provided
std::unique_ptr<ScopedMemoryPool> getDefaultScopedMemoryPool(
    int64_t cap = kMaxMemory);

// Allocator that uses passed in memory pool to allocate memory.
template <typename T>
class Allocator {
 public:
  typedef T value_type;
  MemoryPool& pool;

  /* implicit */ Allocator(MemoryPool& pool) : pool{pool} {}

  template <typename U>
  /* implicit */ Allocator(const Allocator<U>& a) : pool{a.pool} {}

  T* FOLLY_NULLABLE allocate(size_t n) {
    return static_cast<T*>(pool.allocate(checkedMultiply(n, sizeof(T))));
  }

  void deallocate(T* FOLLY_NULLABLE p, size_t n) {
    pool.free(p, checkedMultiply(n, sizeof(T)));
  }

  template <typename T1>
  bool operator==(const Allocator<T1>& rhs) const {
    if constexpr (std::is_same_v<T, T1>) {
      return &this->pool == &rhs.pool;
    }
    return false;
  }

  template <typename T1>
  bool operator!=(const Allocator<T1>& rhs) const {
    return !(*this == rhs);
  }
};
} // namespace memory
} // namespace velox
} // namespace facebook
