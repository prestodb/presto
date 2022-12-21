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

/// This class provides the memory allocation interfaces for a query execution.
/// Each query execution entity creates a dedicated memory pool object. The
/// memory pool objects from a query are organized as a tree with four levels
/// which reflects the query's physical execution plan:
///
/// The top level is a single root pool object (query pool) associated with the
/// query. The query pool is created on the first executed query task and owned
/// by QueryCtx. Note that the query pool is optional as not all the engines
/// using memory pool are creating multiple tasks for the same query in the same
/// process.
///
/// The second level is a number of intermediate pool objects (task pool) with
/// one per each query task. The query pool is the parent of all the task pools
/// from the same query. The task pool is created by the query task and owned by
/// Task.
///
/// The third level is a number of intermediate pool objects (node pool) with
/// one per each query plan node. The task pool is the parent of all the node
/// pools from the task's physical query plan fragment. The node pool is created
/// by the first instantiated operator. It is owned by Task in 'childPools_' to
/// enable the memory sharing within a query task without copy.
///
/// The bottom level is a number of leaf pool objects (operator pool) with one
/// per with each instantiated query operator. Each node pool is the parent of
/// all the operators instantiated from associated query plan node with one per
/// each driver. For instance, if the pipeline of a query plan node N has M
/// drivers in par, then N node pool has M child operator pools. The operator
/// pool is created by the operator. It is also owned by Task in 'childPools_'
/// to enable the memory sharing within a query task without copy.
///
/// The query pool is created from IMemoryManager::getChild() as a child of a
/// singleton root pool object (system pool). There is only one system pool for
/// a velox runtime system. Hence each query pool objects forms a subtree rooted
/// from the system pool.
///
/// Each child pool object holds a shared reference on its parent pool object.
/// The parent object tracks its child pool objects through the raw pool object
/// pointer with lock protected. The child pool object destruction first removes
/// its raw pointer from its parent through dropChild() and then drops the
/// shared reference on the parent.
///
/// NOTE: for the users that integrate at expression evaluation level, we don't
/// need to build the memory pool hierarchy as described above. Users can either
/// create a single memory pool from IMemoryManager::getChild() to share with
/// all the concurrent expression evaluations or create one dedicated memory
/// pool for each expression evaluation if they need per-expression memory quota
/// enforcement.
///
/// In addition to providing memory allocation functions, the memory pool object
/// also provides the memory usage counting through MemoryUsageTracker which
/// will be merged into memory pool object implementation later.
///
/// TODO: extend to provide contiguous and non-contiguous large chunk memory
/// allocation and remove ScopedMappedMemory.
class MemoryPool : public std::enable_shared_from_this<MemoryPool> {
 public:
  /// Constructs a named memory pool with specified 'parent'.
  MemoryPool(const std::string& name, std::shared_ptr<MemoryPool> parent);

  /// Removes this memory pool's tracking from its parent through dropChild()
  /// as well as drops the shared reference on its parent.
  virtual ~MemoryPool();

  /// Tree methods used to access and manage the memory hierarchy.
  /// Returns the name of this memory pool.
  virtual const std::string& name() const;

  /// Returns the raw pointer to the parent pool. The root memory pool has
  /// no parent.
  ///
  /// NOTE: users are only safe to access the returned parent pool pointer while
  /// they hold the shared reference on this child memory pool. Otherwise, the
  /// parent memory pool might have been destroyed.
  virtual MemoryPool* FOLLY_NULLABLE parent() const;

  /// Returns the number of child memory pools.
  virtual uint64_t getChildCount() const;

  /// Invoked to traverse the memory pool subtree rooted at this, and calls
  /// 'visitor' on each visited child memory pool.
  virtual void visitChildren(
      std::function<void(MemoryPool* FOLLY_NONNULL)> visitor) const;

  /// Invoked to create a named child memory pool from this with specified
  /// 'cap'.
  virtual std::shared_ptr<MemoryPool> addChild(
      const std::string& name,
      int64_t cap = kMaxMemory);

  /// Invoked to allocate a buffer with specified 'size'.
  virtual void* FOLLY_NULLABLE allocate(int64_t size) = 0;

  /// Invoked to allocate a zero-filled buffer with capacity that can store
  /// 'numMembers' entries with each size of 'sizeEach'.
  ///
  /// NOTE: 'allocateZeroFilled' memory allocation is not aligned.
  virtual void* FOLLY_NULLABLE
  allocateZeroFilled(int64_t numMembers, int64_t sizeEach) = 0;

  /// Invoked to re-allocate from an existing buffer with 'newSize' and update
  /// memory usage counting accordingly. If 'newSize' is larger than the current
  /// buffer 'size', the function will allocate a new buffer and free the old
  /// buffer.
  virtual void* FOLLY_NULLABLE
  reallocate(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) = 0;

  /// Invoked to free an allocated buffer.
  virtual void free(void* FOLLY_NULLABLE p, int64_t size) = 0;

  /// Rounds up to a power of 2 >= size, or to a size halfway between
  /// two consecutive powers of two, i.e 8, 12, 16, 24, 32, .... This
  /// coincides with JEMalloc size classes.
  virtual size_t getPreferredSize(size_t size);

  /// Returns the memory allocation alignment size applied internally by this
  /// memory pool object.
  virtual uint16_t getAlignment() const = 0;

  /// Resource governing methods used to track and limit the memory usage
  /// through this memory pool object.

  /// Accounting/governing methods, lazily managed and expensive for
  /// intermediate tree nodes, cheap for leaf nodes.
  virtual int64_t getCurrentBytes() const = 0;

  /// Returns the peak memory usage of this memory pool.
  virtual int64_t getMaxBytes() const = 0;

  /// Tracks memory usage from leaf nodes to root and updates immediately
  /// with atomic operations.
  /// Unlike pool's getCurrentBytes(), getMaxBytes(), trace's API's returns
  /// the aggregated usage of subtree. See 'ScopedChildUsageTest' for
  /// difference in their behavior.
  virtual void setMemoryUsageTracker(
      const std::shared_ptr<MemoryUsageTracker>& tracker) = 0;

  /// Returns the memory usage tracker associated with this memory pool.
  virtual const std::shared_ptr<MemoryUsageTracker>& getMemoryUsageTracker()
      const = 0;

  /// Used for external aggregation.
  virtual void setSubtreeMemoryUsage(int64_t size) = 0;

  virtual int64_t updateSubtreeMemoryUsage(int64_t size) = 0;

  /// Used to manage existing externally allocated memories without doing a new
  /// allocation.
  virtual void reserve(int64_t /* bytes */) {
    VELOX_NYI("reserve() needs to be implemented in derived memory pool.");
  }

  /// Sometimes in memory governance we want to mock an update for quota
  /// accounting purposes and different implementations can
  /// choose to accommodate this differently.
  virtual void release(int64_t /* bytes */) {
    VELOX_NYI("release() needs to be implemented in derived memory pool.");
  }

  /// Get the cap for the memory node and its subtree.
  virtual int64_t cap() const = 0;

  /// Called by MemoryManager and MemoryPool upon memory usage updates and
  /// propagates down the subtree recursively.
  virtual void capMemoryAllocation() = 0;

  /// Called by MemoryManager and propagates down recursively if applicable.
  virtual void uncapMemoryAllocation() = 0;

  /// We might need to freeze memory allocating operations under severe global
  /// memory pressure.
  virtual bool isMemoryCapped() const = 0;

 protected:
  /// Invoked by addChild() to create a child memory pool object. 'parent' is
  /// a shared pointer created from this.
  virtual std::shared_ptr<MemoryPool> genChild(
      std::shared_ptr<MemoryPool> parent,
      const std::string& name,
      int64_t cap) = 0;

  /// Invoked only on object destructor to remove this memory pool from its
  /// parent's child memory pool tracking.
  virtual void dropChild(const MemoryPool* FOLLY_NONNULL child);

  const std::string name_;
  std::shared_ptr<MemoryPool> parent_;

  /// Used protect the concurrent access to 'children_'.
  mutable folly::SharedMutex childrenMutex_;
  std::list<MemoryPool*> children_;
};

namespace detail {
static inline MemoryPool& getCheckedReference(std::weak_ptr<MemoryPool> ptr) {
  auto sptr = ptr.lock();
  VELOX_USER_CHECK(sptr);
  return *sptr;
};
} // namespace detail

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

template <typename Allocator, uint16_t ALIGNMENT>
class MemoryManager;
template <typename Allocator, uint16_t ALIGNMENT>
class MemoryPoolImpl;

/// The implementation of MemoryPool interface with customized memory
/// 'Allocator' and memory allocation size 'ALIGNMENT' through template
/// parameters.
template <
    typename Allocator = MemoryAllocator,
    uint16_t ALIGNMENT = kNoAlignment>
class MemoryPoolImpl : public MemoryPool {
 public:
  // Should perhaps make this method private so that we only create node through
  // parent.
  MemoryPoolImpl(
      MemoryManager<Allocator, ALIGNMENT>& memoryManager,
      const std::string& name,
      std::shared_ptr<MemoryPool> parent,
      int64_t cap = kMaxMemory);

  ~MemoryPoolImpl() {
    if (const auto& tracker = getMemoryUsageTracker()) {
      auto remainingBytes = tracker->getCurrentUserBytes();
      VELOX_CHECK_EQ(
          0,
          remainingBytes,
          "Memory pool should be destroyed only after all allocated memory has been freed. Remaining bytes allocated: {}, cumulative bytes allocated: {}, number of allocations: {}",
          remainingBytes,
          tracker->getCumulativeBytes(),
          tracker->getNumAllocs());
    }
  }

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
  int64_t cap() const override;
  uint16_t getAlignment() const override;

  void capMemoryAllocation() override;
  void uncapMemoryAllocation() override;
  bool isMemoryCapped() const override;

  std::shared_ptr<MemoryPool> genChild(
      std::shared_ptr<MemoryPool> parent,
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
  void release(int64_t size) override;

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
  // the tree.
  // TODO: perhaps the root pool should be a specialized pool that
  //        * doesn't do allocation
  //        * cannot be removed
  virtual MemoryPool& getRoot() const = 0;

  // Adds a child pool to root for use.
  virtual std::shared_ptr<MemoryPool> getChild(int64_t cap = kMaxMemory) = 0;

  // Returns the current total memory usage under the MemoryManager.
  virtual int64_t getTotalBytes() const = 0;
  // Reserves size for the allocation. Return a true if the total usage remains
  // under quota after the reservation. Caller is responsible for releasing the
  // offending reservation.
  virtual bool reserve(int64_t size) = 0;
  // Subtracts from current total and regain memory quota.
  virtual void release(int64_t size) = 0;

  // Returns the debug string of this memory manager.
  virtual std::string toString() = 0;
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

  std::shared_ptr<MemoryPool> getChild(int64_t cap = kMaxMemory) final;

  int64_t getTotalBytes() const final;
  bool reserve(int64_t size) final;
  void release(int64_t size) final;

  Allocator& getAllocator();

  std::string toString() final {
    return fmt::format(
        "memoryQuota: {}bytes, alignment: {}bytes", memoryQuota_, ALIGNMENT);
  }

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
    std::shared_ptr<MemoryPool> parent,
    int64_t cap)
    : MemoryPool{name, parent},
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
  auto alignedSize =
      sizeAlign<ALIGNMENT>(ALIGNER<ALIGNMENT>{}, numMembers * sizeEach);
  if (this->isMemoryCapped()) {
    VELOX_MEM_MANUAL_CAP();
  }
  reserve(alignedSize);
  return allocator_.allocZeroFilled(1, alignedSize);
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
    // Track shrink took place for accounting purposes.
    release(-difference);
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
  const auto currentBytes = getCurrentBytes();
  if (memoryUsageTracker_) {
    memoryUsageTracker_->update(-currentBytes);
  }
  memoryUsageTracker_ = tracker;
  memoryUsageTracker_->update(currentBytes);
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
int64_t MemoryPoolImpl<Allocator, ALIGNMENT>::cap() const {
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
  if (getAggregateBytes() > cap()) {
    return;
  }
  if (parent_ != nullptr && parent_->isMemoryCapped()) {
    return;
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
    std::shared_ptr<MemoryPool> parent,
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
void MemoryPoolImpl<Allocator, ALIGNMENT>::release(int64_t size) {
  memoryManager_.release(size);
  localMemoryUsage_.incrementCurrentBytes(-size);
  if (memoryUsageTracker_) {
    memoryUsageTracker_->update(-size);
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
          nullptr,
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
std::shared_ptr<MemoryPool> MemoryManager<Allocator, ALIGNMENT>::getChild(
    int64_t cap) {
  return root_->addChild(
      fmt::format(
          "default_usage_node_{}",
          folly::to<std::string>(folly::Random::rand64())),
      cap);
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

/// Adds a new child memory pool to the root. The new child pool memory cap is
/// set to the input value provided.
std::shared_ptr<MemoryPool> getDefaultMemoryPool(int64_t cap = kMaxMemory);

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
