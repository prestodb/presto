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
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MemoryUsage.h"
#include "velox/common/memory/MemoryUsageTracker.h"

DECLARE_int32(memory_usage_aggregation_interval_millis);

namespace facebook {
namespace velox {
namespace memory {

/// This class provides the memory allocation interfaces for query execution.
/// Each query execution creates a dedicated memory pool object. The memory pool
/// objects from a query are organized as a tree with four levels which mirrors
/// the query's physical execution plan:
///
/// The top level is a single root pool object (query pool) associated with the
/// query. The query pool is created on the first executed query task and owned
/// by QueryCtx. Note that the query pool is optional as not all engines
/// using memory pool are creating multiple tasks for the same query in the same
/// process.
///
/// The second level is a number of intermediate pool objects (task pool) with
/// one per each query task. The query pool is the parent of all the task pools
/// of the same query. The task pool is created by the query task and owned by
/// Task.
///
/// The third level is a number of intermediate pool objects (node pool) with
/// one per each query plan node. The task pool is the parent of all the node
/// pools from the task's physical query plan fragment. The node pool is created
/// by the first operator instantiated for the corresponding plan node. It is
/// owned by Task via 'childPools_'
///
/// The bottom level consists of per-operator pools. These are children of the
/// node pool that corresponds to the plan node from which the operator is
/// created. Operator and node pools are owned by the Task via 'childPools_'.
///
/// The query pool is created from IMemoryManager::getChild() as a child of a
/// singleton root pool object (system pool). There is only one system pool for
/// a velox process. Hence each query pool objects forms a subtree rooted from
/// the system pool.
///
/// Each child pool object holds a shared reference to its parent pool object.
/// The parent object tracks its child pool objects through the raw pool object
/// pointer protected by a mutex. The child pool object destruction first
/// removes its raw pointer from its parent through dropChild() and then drops
/// the shared reference on the parent.
///
/// NOTE: for the users that integrate at expression evaluation level, we don't
/// need to build the memory pool hierarchy as described above. Users can either
/// create a single memory pool from IMemoryManager::getChild() to share with
/// all the concurrent expression evaluations or create one dedicated memory
/// pool for each expression evaluation if they need per-expression memory quota
/// enforcement.
///
/// In addition to providing memory allocation functions, the memory pool object
/// also provides memory usage accounting through MemoryUsageTracker. This will
/// be merged into memory pool object later.
class MemoryPool : public std::enable_shared_from_this<MemoryPool> {
 public:
  struct Options {
    /// Specifies the memory allocation alignment through this memory pool.
    uint16_t alignment{MemoryAllocator::kMaxAlignment};
    /// Specifies the memory capacity of this memory pool.
    int64_t capacity{kMaxMemory};
  };

  /// Constructs a named memory pool with specified 'parent'.
  MemoryPool(
      const std::string& name,
      std::shared_ptr<MemoryPool> parent,
      uint16_t alignment);

  /// Removes this memory pool's tracking from its parent through dropChild().
  /// Drops the shared reference to its parent.
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

  /// Allocates a buffer with specified 'size'.
  virtual void* FOLLY_NULLABLE allocate(int64_t size) = 0;

  /// Allocates a zero-filled buffer with capacity that can store 'numEntries'
  /// entries with each size of 'sizeEach'.
  virtual void* FOLLY_NULLABLE
  allocateZeroFilled(int64_t numEntries, int64_t sizeEach) = 0;

  /// Re-allocatea from an existing buffer with 'newSize' and update memory
  /// usage counting accordingly. If 'newSize' is larger than the current buffer
  /// 'size', the function will allocate a new buffer and free the old buffer.
  virtual void* FOLLY_NULLABLE
  reallocate(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) = 0;

  /// Frees an allocated buffer.
  virtual void free(void* FOLLY_NULLABLE p, int64_t size) = 0;

  /// Allocates one or more runs that add up to at least 'numPages', with the
  /// smallest run being at least 'minSizeClass' pages. 'minSizeClass' must be
  /// <= the size of the largest size class. The new memory is returned in 'out'
  /// and any memory formerly referenced by 'out' is freed. The function returns
  /// true if the allocation succeeded. If returning false, 'out' references no
  /// memory and any partially allocated memory is freed.
  virtual bool allocateNonContiguous(
      MachinePageCount numPages,
      MemoryAllocator::Allocation& out,
      MachinePageCount minSizeClass = 0) = 0;

  /// Frees non-contiguous 'allocation'. 'allocation' is empty on return.
  virtual void freeNonContiguous(MemoryAllocator::Allocation& allocation) = 0;

  /// Returns the largest class size used by non-contiguous memory allocation.
  virtual MachinePageCount largestSizeClass() const = 0;

  /// Returns the list of supported size class sizes used by non-contiguous
  /// memory allocation.
  virtual const std::vector<MachinePageCount>& sizeClasses() const = 0;

  /// Makes a large contiguous mmap of 'numPages'. The new mapped pages are
  /// returned in 'out' on success. Any formly mapped pages referenced by
  /// 'out' is unmapped in all the cases even if the allocation fails.
  virtual bool allocateContiguous(
      MachinePageCount numPages,
      MemoryAllocator::ContiguousAllocation& out) = 0;

  /// Frees contiguous 'allocation'. 'allocation' is empty on return.
  virtual void freeContiguous(
      MemoryAllocator::ContiguousAllocation& allocation) = 0;

  /// Rounds up to a power of 2 >= size, or to a size halfway between
  /// two consecutive powers of two, i.e 8, 12, 16, 24, 32, .... This
  /// coincides with JEMalloc size classes.
  virtual size_t getPreferredSize(size_t size);

  /// Returns the memory allocation alignment size applied internally by this
  /// memory pool object.
  virtual uint16_t alignment() const {
    return alignment_;
  }

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

  /// Tracks the externally allocated memory usage without doing a new
  /// allocation.
  virtual void reserve(int64_t /* bytes */) {
    VELOX_NYI("reserve() needs to be implemented in derived memory pool.");
  }

  /// Sometimes in memory management we want to mock an update for quota
  /// accounting purposes and different implementations can
  /// choose to accommodate this differently.
  virtual void release(int64_t /* bytes */, bool /* mock */ = false) {
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

  /// Invoked only on destruction to remove this memory pool from its parent's
  /// child memory pool tracking.
  virtual void dropChild(const MemoryPool* FOLLY_NONNULL child);

  const std::string name_;
  const uint16_t alignment_;
  const std::shared_ptr<MemoryPool> parent_;

  /// Protects 'children_'.
  mutable folly::SharedMutex childrenMutex_;
  std::list<MemoryPool*> children_;
};

class MemoryManager;

/// The implementation of MemoryPool interface with a specified memory manager.
class MemoryPoolImpl : public MemoryPool {
 public:
  // Should perhaps make this method private so that we only create node through
  // parent.
  MemoryPoolImpl(
      MemoryManager& memoryManager,
      const std::string& name,
      std::shared_ptr<MemoryPool> parent,
      const Options& options = Options{});

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

  bool allocateNonContiguous(
      MachinePageCount numPages,
      MemoryAllocator::Allocation& out,
      MachinePageCount minSizeClass = 0) override;

  void freeNonContiguous(MemoryAllocator::Allocation& allocation) override;

  MachinePageCount largestSizeClass() const override;

  const std::vector<MachinePageCount>& sizeClasses() const override;

  bool allocateContiguous(
      MachinePageCount numPages,
      MemoryAllocator::ContiguousAllocation& allocation) override;

  void freeContiguous(
      MemoryAllocator::ContiguousAllocation& allocation) override;

  /// Memory Management methods.

  /// TODO: Consider putting these in base class also.
  int64_t getCurrentBytes() const override;

  int64_t getMaxBytes() const override;

  void setMemoryUsageTracker(
      const std::shared_ptr<MemoryUsageTracker>& tracker) override;

  const std::shared_ptr<MemoryUsageTracker>& getMemoryUsageTracker()
      const override;

  void setSubtreeMemoryUsage(int64_t size) override;

  int64_t updateSubtreeMemoryUsage(int64_t size) override;

  int64_t cap() const override;

  uint16_t alignment() const override;

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

  void release(int64_t size, bool mock = false) override;

 private:
  VELOX_FRIEND_TEST(MemoryPoolTest, Ctor);

  int64_t sizeAlign(int64_t size);

  void* FOLLY_NULLABLE
  reallocAligned(void* FOLLY_NULLABLE p, int64_t size, int64_t newSize) {
    return allocator_.reallocateBytes(p, size, newSize, alignment_);
  }

  void accessSubtreeMemoryUsage(
      std::function<void(const MemoryUsage&)> visitor) const;
  void updateSubtreeMemoryUsage(std::function<void(MemoryUsage&)> visitor);

  const int64_t cap_;
  MemoryManager& memoryManager_;

  // Memory allocated attributed to the memory node.
  MemoryUsage localMemoryUsage_;
  std::shared_ptr<MemoryUsageTracker> memoryUsageTracker_;
  mutable folly::SharedMutex subtreeUsageMutex_;
  MemoryUsage subtreeMemoryUsage_;
  std::atomic_bool capped_{false};

  MemoryAllocator& allocator_;
};

/// This class provides the interface of memory manager. The memory manager is
/// responsible for enforcing the memory usage quota as well as managing the
/// memory pools.
class IMemoryManager {
 public:
  struct Options {
    /// Specifies the default memory allocation alignment.
    uint16_t alignment{MemoryAllocator::kMaxAlignment};

    /// Specifies the max memory capacity in bytes.
    int64_t capacity{kMaxMemory};

    /// Specifies the backing memory allocator.
    MemoryAllocator* FOLLY_NONNULL allocator{MemoryAllocator::getInstance()};
  };

  virtual ~IMemoryManager() = default;

  /// Returns the total memory usage in bytes allowed under this memory manager.
  /// The memory manager maintains this capacity as a hard cap, and any
  /// allocation that would exceed the quota throws.
  virtual int64_t capacity() const = 0;

  /// Returns the memory allocation alignment of this memory manager.
  virtual uint16_t alignment() const = 0;

  /// Power users that want to explicitly modify the tree should get the root of
  /// the tree.
  ///
  /// TODO: deprecate this API to disallow user to allocate from the root memory
  /// pool directly.
  virtual MemoryPool& getRoot() const = 0;

  /// Adds a child pool to root for use.
  virtual std::shared_ptr<MemoryPool> getChild(int64_t cap = kMaxMemory) = 0;

  /// Returns the current total memory usage under this memory manager.
  virtual int64_t getTotalBytes() const = 0;

  /// Reserves size for the allocation. Return a true if the total usage remains
  /// under quota after the reservation. Caller is responsible for releasing the
  /// offending reservation.
  ///
  /// TODO: deprecate this and enforce the memory usage quota by memory pool.
  virtual bool reserve(int64_t size) = 0;

  /// Subtracts from current total and regain memory quota.
  ///
  /// TODO: deprecate this and enforce the memory usage quota by memory pool.
  virtual void release(int64_t size) = 0;
};

class MemoryManager final : public IMemoryManager {
 public:
  /// Tries to get the singleton memory manager. If not previously initialized,
  /// the process singleton manager will be initialized with the 'options'.
  FOLLY_EXPORT static MemoryManager& getInstance(
      const Options& options = Options{},
      bool ensureCapacity = false) {
    static MemoryManager manager{options};
    auto actualCapacity = manager.capacity();
    VELOX_USER_CHECK(
        !ensureCapacity || actualCapacity == options.capacity,
        "Process level manager manager created with input capacity: {}, actual capacity: {}",
        options.capacity,
        actualCapacity);

    return manager;
  }

  explicit MemoryManager(const Options& options = Options{});

  ~MemoryManager();

  int64_t capacity() const final;

  uint16_t alignment() const final;

  MemoryPool& getRoot() const final;

  std::shared_ptr<MemoryPool> getChild(int64_t cap = kMaxMemory) final;

  int64_t getTotalBytes() const final;

  bool reserve(int64_t size) final;
  void release(int64_t size) final;

  MemoryAllocator& getAllocator();

 private:
  VELOX_FRIEND_TEST(MemoryPoolImplTest, CapSubtree);
  VELOX_FRIEND_TEST(MemoryPoolImplTest, CapAllocation);
  VELOX_FRIEND_TEST(MemoryPoolImplTest, UncapMemory);
  VELOX_FRIEND_TEST(MemoryPoolImplTest, MemoryManagerGlobalCap);
  VELOX_FRIEND_TEST(MultiThreadingUncappingTest, Flat);
  VELOX_FRIEND_TEST(MultiThreadingUncappingTest, SimpleTree);

  MemoryAllocator* const FOLLY_NONNULL allocator_;
  const uint16_t alignment_;
  const int64_t capacity_;

  std::shared_ptr<MemoryPool> root_;
  mutable folly::SharedMutex mutex_;
  std::atomic_long totalBytes_{0};
};

IMemoryManager& getProcessDefaultMemoryManager();

/// Adds a new child memory pool to the root. The new child pool memory cap is
/// set to the input value provided.
std::shared_ptr<MemoryPool> getDefaultMemoryPool(int64_t cap = kMaxMemory);

/// Allocator that uses passed in memory pool to allocate memory.
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
