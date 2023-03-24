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

#include "velox/common/base/GTestMacros.h"
#include "velox/common/memory/Allocation.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MemoryUsage.h"
#include "velox/common/memory/MemoryUsageTracker.h"

namespace facebook::velox::memory {

class MemoryManager;

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
  /// Defines the kinds of a memory pool.
  enum class Kind {
    /// The leaf memory pool is used for memory allocation. User can allocate
    /// memory from this kind of pool but can't create child pool from it.
    kLeaf = 0,
    /// The aggregation memory pool is used to manage the memory pool hierarchy
    /// and aggregate the memory usage from the leaf pools. The user can't
    /// directly allocate memory from this kind of pool but can create child
    /// pools from it.
    kAggregate = 1,
  };
  static std::string kindString(Kind kind);

  struct Options {
    /// Specifies the memory allocation alignment through this memory pool.
    uint16_t alignment{MemoryAllocator::kMaxAlignment};
    /// Specifies the memory capacity of this memory pool.
    int64_t capacity{kMaxMemory};
  };

  /// Constructs a named memory pool with specified 'name', 'parent' and 'kind'.
  ///
  /// NOTE: we can't create a memory pool with no 'parent' but has 'kind' of
  /// kLeaf.
  MemoryPool(
      const std::string& name,
      Kind kind,
      std::shared_ptr<MemoryPool> parent,
      const Options& options);

  /// Removes this memory pool's tracking from its parent through dropChild().
  /// Drops the shared reference to its parent.
  virtual ~MemoryPool();

  /// Tree methods used to access and manage the memory hierarchy.
  /// Returns the name of this memory pool.
  virtual const std::string& name() const;

  /// Returns the kind of this memory pool.
  virtual Kind kind() const;

  /// Returns the raw pointer to the parent pool. The root memory pool has
  /// no parent.
  ///
  /// NOTE: users are only safe to access the returned parent pool pointer while
  /// they hold the shared reference on this child memory pool. Otherwise, the
  /// parent memory pool might have been destroyed.
  virtual MemoryPool* parent() const;

  /// Returns the number of child memory pools.
  virtual uint64_t getChildCount() const;

  /// Invoked to traverse the memory pool subtree rooted at this, and calls
  /// 'visitor' on each visited child memory pool.
  virtual void visitChildren(std::function<void(MemoryPool*)> visitor) const;

  /// Invoked to create a named child memory pool from this with specified
  /// 'kind'.
  virtual std::shared_ptr<MemoryPool> addChild(
      const std::string& name,
      Kind kind = MemoryPool::Kind::kLeaf);

  /// Allocates a buffer with specified 'size'.
  virtual void* allocate(int64_t size) = 0;

  /// Allocates a zero-filled buffer with capacity that can store 'numEntries'
  /// entries with each size of 'sizeEach'.
  virtual void* allocateZeroFilled(int64_t numEntries, int64_t sizeEach) = 0;

  /// Re-allocates from an existing buffer with 'newSize' and update memory
  /// usage counting accordingly. If 'newSize' is larger than the current buffer
  /// 'size', the function will allocate a new buffer and free the old buffer.
  virtual void* reallocate(void* p, int64_t size, int64_t newSize) = 0;

  /// Frees an allocated buffer.
  virtual void free(void* p, int64_t size) = 0;

  /// Allocates one or more runs that add up to at least 'numPages', with the
  /// smallest run being at least 'minSizeClass' pages. 'minSizeClass' must be
  /// <= the size of the largest size class. The new memory is returned in 'out'
  /// and any memory formerly referenced by 'out' is freed. The function returns
  /// true if the allocation succeeded. If returning false, 'out' references no
  /// memory and any partially allocated memory is freed.
  virtual void allocateNonContiguous(
      MachinePageCount numPages,
      Allocation& out,
      MachinePageCount minSizeClass = 0) = 0;

  /// Frees non-contiguous 'allocation'. 'allocation' is empty on return.
  virtual void freeNonContiguous(Allocation& allocation) = 0;

  /// Returns the largest class size used by non-contiguous memory allocation.
  virtual MachinePageCount largestSizeClass() const = 0;

  /// Returns the list of supported size class sizes used by non-contiguous
  /// memory allocation.
  virtual const std::vector<MachinePageCount>& sizeClasses() const = 0;

  /// Makes a large contiguous mmap of 'numPages'. The new mapped pages are
  /// returned in 'out' on success. Any formly mapped pages referenced by
  /// 'out' is unmapped in all the cases even if the allocation fails.
  virtual void allocateContiguous(
      MachinePageCount numPages,
      ContiguousAllocation& out) = 0;

  /// Frees contiguous 'allocation'. 'allocation' is empty on return.
  virtual void freeContiguous(ContiguousAllocation& allocation) = 0;

  /// Rounds up to a power of 2 >= size, or to a size halfway between
  /// two consecutive powers of two, i.e 8, 12, 16, 24, 32, .... This
  /// coincides with JEMalloc size classes.
  virtual size_t getPreferredSize(size_t size);

  /// Returns the memory allocation alignment size applied internally by this
  /// memory pool object.
  virtual uint16_t getAlignment() const {
    return alignment_;
  }

  /// Resource governing methods used to track and limit the memory usage
  /// through this memory pool object.

  /// Accounting/governing methods, lazily managed and expensive for
  /// intermediate tree nodes, cheap for leaf nodes.
  virtual int64_t getCurrentBytes() const = 0;

  /// Returns the peak memory usage of this memory pool.
  virtual int64_t getMaxBytes() const = 0;

  /// Returns the memory usage tracker associated with this memory pool.
  virtual const std::shared_ptr<MemoryUsageTracker>& getMemoryUsageTracker()
      const = 0;

  virtual int64_t updateSubtreeMemoryUsage(int64_t size) = 0;

  /// Tracks the externally allocated memory usage without doing a new
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

  virtual std::string toString() const = 0;

 protected:
  /// Invoked by addChild() to create a child memory pool object. 'parent' is
  /// a shared pointer created from this.
  virtual std::shared_ptr<MemoryPool> genChild(
      std::shared_ptr<MemoryPool> parent,
      const std::string& name,
      Kind kind) = 0;

  /// Invoked only on destruction to remove this memory pool from its parent's
  /// child memory pool tracking.
  virtual void dropChild(const MemoryPool* child);

  FOLLY_ALWAYS_INLINE virtual void checkMemoryAllocation() {
    VELOX_CHECK_EQ(
        kind_,
        Kind::kLeaf,
        "Memory allocation is only allowed on leaf memory pool: {}",
        toString());
  }

  FOLLY_ALWAYS_INLINE virtual void checkPoolManagement() {
    VELOX_CHECK_EQ(
        kind_,
        Kind::kAggregate,
        "Pool management is only allowed on aggregation memory pool: {}",
        toString());
  }

  const std::string name_;
  const Kind kind_;
  const uint16_t alignment_;
  const std::shared_ptr<MemoryPool> parent_;

  /// Protects 'children_'.
  mutable folly::SharedMutex childrenMutex_;
  std::unordered_map<std::string, std::weak_ptr<MemoryPool>> children_;
};

std::ostream& operator<<(std::ostream& out, MemoryPool::Kind kind);

class MemoryManager;

/// The implementation of MemoryPool interface with a specified memory manager.
class MemoryPoolImpl : public MemoryPool {
 public:
  using DestructionCallback = std::function<void(MemoryPool*)>;

  // Should perhaps make this method private so that we only create node through
  // parent.
  MemoryPoolImpl(
      MemoryManager* memoryManager,
      const std::string& name,
      Kind kind,
      std::shared_ptr<MemoryPool> parent,
      DestructionCallback destructionCb = nullptr,
      const Options& options = Options{});

  ~MemoryPoolImpl() override;

  // Actual memory allocation operations. Can be delegated.
  // Access global MemoryManager to check usage of current node and enforce
  // memory cap accordingly.
  void* allocate(int64_t size) override;

  void* allocateZeroFilled(int64_t numMembers, int64_t sizeEach) override;

  // No-op for attempts to shrink buffer.
  void* reallocate(void* p, int64_t size, int64_t newSize) override;

  void free(void* p, int64_t size) override;

  void allocateNonContiguous(
      MachinePageCount numPages,
      Allocation& out,
      MachinePageCount minSizeClass = 0) override;

  void freeNonContiguous(Allocation& allocation) override;

  MachinePageCount largestSizeClass() const override;

  const std::vector<MachinePageCount>& sizeClasses() const override;

  void allocateContiguous(
      MachinePageCount numPages,
      ContiguousAllocation& allocation) override;

  void freeContiguous(ContiguousAllocation& allocation) override;

  /// Memory Management methods.

  /// TODO: Consider putting these in base class also.
  int64_t getCurrentBytes() const override;

  int64_t getMaxBytes() const override;

  const std::shared_ptr<MemoryUsageTracker>& getMemoryUsageTracker()
      const override;
  int64_t updateSubtreeMemoryUsage(int64_t size) override;
  uint16_t getAlignment() const override;

  std::shared_ptr<MemoryPool> genChild(
      std::shared_ptr<MemoryPool> parent,
      const std::string& name,
      Kind kind) override;

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

  std::string toString() const override;

  MemoryAllocator* testingAllocator() const {
    return allocator_;
  }

 private:
  int64_t sizeAlign(int64_t size);

  void accessSubtreeMemoryUsage(
      std::function<void(const MemoryUsage&)> visitor) const;
  void updateSubtreeMemoryUsage(std::function<void(MemoryUsage&)> visitor);

  const std::shared_ptr<MemoryUsageTracker> memoryUsageTracker_;
  MemoryManager* const memoryManager_;
  MemoryAllocator* const allocator_;
  const DestructionCallback destructionCb_;

  // Memory allocated attributed to the memory node.
  MemoryUsage localMemoryUsage_;
  mutable folly::SharedMutex subtreeUsageMutex_;
  MemoryUsage subtreeMemoryUsage_;
};

/// An Allocator backed by a memory pool for STL containers.
template <typename T>
class StlAllocator {
 public:
  typedef T value_type;
  MemoryPool& pool;

  /* implicit */ StlAllocator(MemoryPool& pool) : pool{pool} {}

  template <typename U>
  /* implicit */ StlAllocator(const StlAllocator<U>& a) : pool{a.pool} {}

  T* allocate(size_t n) {
    return static_cast<T*>(pool.allocate(checkedMultiply(n, sizeof(T))));
  }

  void deallocate(T* p, size_t n) {
    pool.free(p, checkedMultiply(n, sizeof(T)));
  }

  template <typename T1>
  bool operator==(const StlAllocator<T1>& rhs) const {
    if constexpr (std::is_same_v<T, T1>) {
      return &this->pool == &rhs.pool;
    }
    return false;
  }

  template <typename T1>
  bool operator!=(const StlAllocator<T1>& rhs) const {
    return !(*this == rhs);
  }
};
} // namespace facebook::velox::memory
