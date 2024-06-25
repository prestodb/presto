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

#include <array>
#include <atomic>
#include <memory>
#include <optional>
#include <queue>

#include <fmt/format.h>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Portability.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/common/memory/Allocation.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MemoryArbitrator.h"

DECLARE_bool(velox_memory_leak_check_enabled);
DECLARE_bool(velox_memory_pool_debug_enabled);

namespace facebook::velox::exec {
class ParallelMemoryReclaimer;
}

namespace facebook::velox::memory {
#define VELOX_MEM_POOL_CAP_EXCEEDED(errorMessage)                   \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kMemCapExceeded.c_str(),       \
      /* isRetriable */ true,                                       \
      "{}",                                                         \
      errorMessage);

#define VELOX_MEM_POOL_ABORTED(errorMessage)                        \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kMemAborted.c_str(),           \
      /* isRetriable */ true,                                       \
      "{}",                                                         \
      errorMessage);

class MemoryManager;

constexpr int64_t kMaxMemory = std::numeric_limits<int64_t>::max();

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
/// owned by Task via 'childPools_'.
///
/// The bottom level consists of per-operator pools. These are children of the
/// node pool that corresponds to the plan node from which the operator is
/// created. Operator and node pools are owned by the Task via 'childPools_'.
///
/// The query pool is created from MemoryManager::addRootPool(), it has no
/// parent and is the root node of its corresponding subtree. Each query pool is
/// owned by QueryCtx (such as in Prestissimo), and the memory manager also
/// tracks the current alive query pools in MemoryManager::pools_ through weak
/// pointers.
///
/// Each child pool object holds a shared reference to its parent pool object.
/// The parent object tracks its child pool objects through weak pointers
/// protected by a mutex. The child pool object destruction first removes its
/// weak pointer from its parent through dropChild() and then drops the shared
/// reference on the parent.
///
/// NOTE: for the users that integrate at expression evaluation level, we don't
/// need to build the memory pool hierarchy as described above. Users can either
/// create a single memory pool from MemoryManager::addLeafPool() to share with
/// all the concurrent expression evaluations or create one dedicated memory
/// pool for each expression evaluation if they need per-expression memory quota
/// enforcement.
///
/// In addition to providing memory allocation functions, the memory pool object
/// also provides memory usage accounting.
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
    /// Specifies the max memory capacity of this memory pool.
    int64_t maxCapacity{kMaxMemory};

    /// If true, tracks the memory usage from the leaf memory pool and aggregate
    /// up to the root memory pool for capacity enforcement. Otherwise there is
    /// no memory usage tracking.
    ///
    /// NOTE: there are some use cases which doesn't need the memory usage
    /// tracking and the capacity enforcement on top of that, but are sensitive
    /// to its cpu cost so we provide an options for user to turn it off. We can
    /// only turn on/off this feature at the root memory pool and automatically
    /// applies to all its child pools , and we don't support to selectively
    /// enable it on a subset of memory pools.
    bool trackUsage{true};

    /// If true, tracks the leaf memory pool usage in a thread-safe mode
    /// otherwise not. This only applies for leaf memory pool with memory usage
    /// tracking enabled. We use non-thread safe tracking mode for single
    /// threaded use case.
    ///
    /// NOTE: user can turn on/off the thread-safe mode of each individual leaf
    /// memory pools from the same root memory pool independently.
    bool threadSafe{true};

    /// If true, tracks the allocation and free call stacks to detect the source
    /// of memory leak for testing purpose.
    bool debugEnabled{FLAGS_velox_memory_pool_debug_enabled};

    /// Terminates the process and generates a core file on an allocation
    /// failure
    bool coreOnAllocationFailureEnabled{false};
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

  /// Returns the root of this memory pool.
  virtual MemoryPool* root() const;

  /// Returns the number of child memory pools.
  virtual uint64_t getChildCount() const;

  /// Returns true if this memory pool tracks memory usage.
  virtual bool trackUsage() const {
    return trackUsage_;
  }

  /// Returns true if this memory pools is thread safe which only applies for a
  /// leaf memory pool with memory usage tracking enabled.
  virtual bool threadSafe() const {
    return threadSafe_;
  }

  /// Invoked to visit the memory pool's direct children, and calls 'visitor' on
  /// each visited child memory pool with the parent pool's 'poolMutex_' reader
  /// lock held. The 'visitor' must not access the parent memory pool to avoid
  /// the potential recursive locking issues. Note that the traversal stops if
  /// 'visitor' returns false.
  virtual void visitChildren(
      const std::function<bool(MemoryPool*)>& visitor) const;

  /// Invoked to create a named leaf child memory pool.
  ///
  /// NOTE: 'threadSafe' and 'reclaimer' only applies if the leaf memory pool
  /// has enabled memory usage tracking which inherits from its parent.
  virtual std::shared_ptr<MemoryPool> addLeafChild(
      const std::string& name,
      bool threadSafe = true,
      std::unique_ptr<MemoryReclaimer> reclaimer = nullptr);

  /// Invoked to create a named aggregate child memory pool.
  ///
  /// NOTE: 'reclaimer' only applies if the aggregate memory pool has enabled
  /// memory usage tracking which inherits from its parent.
  virtual std::shared_ptr<MemoryPool> addAggregateChild(
      const std::string& name,
      std::unique_ptr<MemoryReclaimer> reclaimer = nullptr);

  /// Allocates a buffer with specified 'size'.
  virtual void* allocate(int64_t size) = 0;

  /// Allocates a zero-filled buffer with capacity that can store 'numEntries'
  /// entries with each size of 'sizeEach'.
  virtual void* allocateZeroFilled(int64_t numEntries, int64_t sizeEach) = 0;

  /// Re-allocates from an existing buffer with 'newSize' and update memory
  /// usage counting accordingly.
  virtual void* reallocate(void* p, int64_t size, int64_t newSize) = 0;

  /// Frees an allocated buffer.
  virtual void free(void* p, int64_t size) = 0;

  /// Allocates one or more runs that add up to at least 'numPages', with the
  /// smallest run being at least 'minSizeClass' pages. 'minSizeClass' must be
  /// <= the size of the largest size class. The new memory is returned in 'out'
  /// on success and any memory formerly referenced by 'out' is freed. The
  /// function throws if allocation fails and 'out' references no memory and any
  /// partially allocated memory is freed. 'out' will always be freed regardless
  /// of success or not.
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

  /// Makes a large contiguous mmap of 'numPages'. The new mapped
  /// pages are returned in 'out' on success. Any formly mapped pages
  /// referenced by 'out' is unmapped in all the cases even if the
  /// allocation fails. If 'maxPages' is not given, this defaults to
  /// 'numPages'. 'maxPages' gives the size of the mmap in
  /// addresses. 'numPages' gives the amount to declare as
  /// used. growContiguous() is used to increase the
  /// reservation up to 'maxPages'. This allows reserving a large
  /// range of addresses for huge pages. The range can be larger than
  /// is likely to be used because usage can be declared as needed but
  /// the number of huge pages  can be set according to an assumption of large
  /// utilization.
  virtual void allocateContiguous(
      MachinePageCount numPages,
      ContiguousAllocation& out,
      MachinePageCount maxPages = 0) = 0;

  /// Frees contiguous 'allocation'. 'allocation' is empty on return.
  virtual void freeContiguous(ContiguousAllocation& allocation) = 0;

  virtual void growContiguous(
      MachinePageCount increment,
      ContiguousAllocation& allocation) = 0;

  /// Rounds up to a power of 2 >= size, or to a size halfway between
  /// two consecutive powers of two, i.e 8, 12, 16, 24, 32, .... This
  /// coincides with JEMalloc size classes.
  virtual size_t preferredSize(size_t size);

  /// Returns the memory allocation alignment size applied internally by this
  /// memory pool object.
  virtual uint16_t alignment() const {
    return alignment_;
  }

  /// Resource governing methods used to track and limit the memory usage
  /// through this memory pool object.

  /// Returns the max capacity of the root memory pool which is a hard limit of
  /// the memory pool's capacity.
  virtual int64_t maxCapacity() const {
    return parent_ != nullptr ? parent_->maxCapacity() : maxCapacity_;
  }

  /// Returns the current capacity of the root memory pool. The memory
  /// arbitrator allocates an initial memory capacity for a newly created memory
  /// pool, and continuously adjusts its capacity during the query execution and
  /// ensures it is within 'maxCapacity()' limit. Without memory arbitrator,
  /// 'capacity()' is fixed and set to 'maxCapacity()' on creation.
  virtual int64_t capacity() const = 0;

  /// Returns the currently used memory in bytes of this memory pool. For
  /// non-leaf memory pool, the function returns the aggregated used memory from
  /// all its child memory pools.
  virtual int64_t usedBytes() const = 0;

  /// Returns the peak memory usage in bytes of this memory pool.
  virtual int64_t peakBytes() const = 0;

  /// Returns the reserved but not used memory in bytes of this memory pool.
  ///
  /// NOTE: this is always zero for non-leaf memory pool as it only aggregates
  /// the memory reservations from its child memory pools but not
  /// differentiating whether the aggregated reservations have been actually
  /// used in child pools or not.
  virtual int64_t availableReservation() const = 0;

  /// Returns the reserved but not used memory in bytes that can be released by
  /// calling 'release()'. This might be different from 'availableReservation()'
  /// because leaf memory pool makes quantized memory reservation.
  ///
  /// NOTE: For non-leaf memory pool, it returns the aggregated releasable
  /// memory reservations from all its leaf memory pool.
  virtual int64_t releasableReservation() const = 0;

  /// Returns the reserved memory reservation in bytes including both used and
  /// unused reservations.
  virtual int64_t reservedBytes() const = 0;

  /// Checks if it is likely that the reservation on this memory pool can be
  /// incremented by 'size'. Returns false if this seems unlikely. Otherwise
  /// attempts the reservation increment and returns true if succeeded.
  virtual bool maybeReserve(uint64_t size) = 0;

  /// If a minimum reservation has been set with maybeReserve(), resets the
  /// minimum reservation. If the current usage is below the minimum
  /// reservation, decreases reservation and usage down to the rounded actual
  /// usage.
  virtual void release() = 0;

  /// Memory arbitration related interfaces.

  /// Returns the free memory capacity in bytes that haven't been reserved for
  /// use from the root of this memory pool. The memory arbitrator can reclaim
  /// the free bytes from the root memory pool by reducing its memory capacity
  /// without actually freeing the used memory.
  virtual uint64_t freeBytes() const = 0;

  /// Sets the memory reclaimer for this memory pool.
  ///
  /// NOTE: this shall only be called at most once if the memory pool hasn't set
  /// reclaimer on construction.
  virtual void setReclaimer(std::unique_ptr<MemoryReclaimer> reclaimer) = 0;

  /// Returns the memory reclaimer of this memory pool if not null.
  virtual MemoryReclaimer* reclaimer() const = 0;

  /// Invoked by the memory arbitrator to enter memory arbitration processing.
  /// It is a noop if 'reclaimer' is not set, otherwise invoke the reclaimer's
  /// corresponding method.
  virtual void enterArbitration() = 0;

  /// Invoked by the memory arbitrator to leave memory arbitration processing.
  /// It is a noop if 'reclaimer' is not set, otherwise invoke the reclaimer's
  /// corresponding method.
  virtual void leaveArbitration() noexcept = 0;

  /// Function estimates the number of reclaimable bytes and returns in
  /// 'reclaimableBytes'. If the 'reclaimer' is not set, the function returns
  /// std::nullopt. Otherwise, it will invoke the corresponding method of the
  /// reclaimer.
  virtual std::optional<uint64_t> reclaimableBytes() const = 0;

  /// Invoked by the memory arbitrator to reclaim memory from this memory pool
  /// with specified reclaim target bytes. If 'targetBytes' is zero, then it
  /// tries to reclaim all the reclaimable memory from the memory pool. It is
  /// noop if the reclaimer is not set, otherwise invoke the reclaimer's
  /// corresponding method. If not zero, 'maxWaitMs' specifies the max time in
  /// milliseconds to wait for reclaim. The memory reclaim might fail if exceeds
  /// the timeout. The function returns the actually freed capacity from the
  /// root of this memory pool.
  virtual uint64_t reclaim(
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      memory::MemoryReclaimer::Stats& stats) = 0;

  /// Invoked by the memory arbitrator to abort a root memory pool. The function
  /// forwards the request to the corresponding query object to abort its
  /// execution through the reclaimer. The function throws if the reclaimer is
  /// not set, otherwise returns a future to wait for the abort processing to
  /// completion. We expect the query object to release its used memory soon
  /// after the abort completes. 'error' should be the cause of the abortion. It
  /// will be propagated to task level for accurate error exposure.
  virtual void abort(const std::exception_ptr& error) = 0;

  /// Returns true if this memory pool has been aborted.
  virtual bool aborted() const;

  /// The memory pool's execution stats.
  struct Stats {
    /// The current memory usage.
    uint64_t usedBytes{0};
    /// The current reserved memory.
    uint64_t reservedBytes{0};
    /// The peak memory usage.
    uint64_t peakBytes{0};
    /// The accumulative memory usage.
    uint64_t cumulativeBytes{0};
    /// The number of memory allocations.
    uint64_t numAllocs{0};
    /// The number of memory frees.
    uint64_t numFrees{0};
    /// The number of memory reservations.
    ///
    /// NOTE: this only counts the explicit memory reservations called by
    /// maybeReserve().
    uint64_t numReserves{0};
    /// The number of memory reservation releases.
    ///
    /// NOTE: this only counts the explicit memory reservation releases called
    /// by release().
    uint64_t numReleases{0};
    /// The number of memory capacity shrinks.
    uint64_t numShrinks{0};
    /// The number of memory reclamation which triggers to actually freeing used
    /// memory.
    uint64_t numReclaims{0};
    /// The number of internal memory reservation collisions caused by
    /// concurrent memory requests.
    uint64_t numCollisions{0};
    /// The number of memory capacity growth attempts through the memory
    /// arbitration.
    ///
    /// NOTE: this only applies for the root memory pool.
    uint64_t numCapacityGrowths{0};

    bool operator==(const Stats& rhs) const;

    std::string toString() const;

    /// Returns true if the current and reserved bytes are zero.
    /// Note that peak or cumulative bytes might be non-zero and we are still
    /// empty at this moment.
    bool empty() const {
      return usedBytes == 0 && reservedBytes == 0;
    }
  };

  /// Returns the stats of this memory pool.
  virtual Stats stats() const = 0;

  virtual std::string toString() const = 0;

  /// Invoked to generate a descriptive memory usage summary of the entire tree.
  /// MemoryPoolImpl::treeMemoryUsage(). If 'skipEmptyPool' is true, then skip
  /// print out the child memory pools with empty memory usage.
  virtual std::string treeMemoryUsage(bool skipEmptyPool = true) const = 0;

  /// Indicates if this is a leaf memory pool or not.
  FOLLY_ALWAYS_INLINE bool isLeaf() const {
    return kind_ == Kind::kLeaf;
  }

  /// Indicates if this is a root memory pool or not.
  FOLLY_ALWAYS_INLINE bool isRoot() const {
    return parent_ == nullptr;
  }

  /// Returns the next higher quantized size for the internal memory reservation
  /// propagation. Small sizes are at MB granularity, larger ones at coarser
  /// granularity.
  FOLLY_ALWAYS_INLINE static uint64_t quantizedSize(uint64_t size) {
    if (size < 16 * kMB) {
      return bits::roundUp(size, kMB);
    }
    if (size < 64 * kMB) {
      return bits::roundUp(size, 4 * kMB);
    }
    return bits::roundUp(size, 8 * kMB);
  }

 protected:
  static constexpr uint64_t kMB = 1 << 20;

  /// Invoked to free up to the specified amount of free memory by reducing
  /// this memory pool's capacity without actually freeing any used memory. The
  /// function returns the actually freed memory capacity in bytes. If
  /// 'targetBytes' is zero, the function frees all the free memory capacity.
  virtual uint64_t shrink(uint64_t targetBytes = 0) = 0;

  /// Invoked to increase the memory pool's capacity by 'growBytes' and commit
  /// the reservation by 'reservationBytes'. The function makes the two updates
  /// atomic. The function returns true if the updates succeed, otherwise false
  /// and neither change will apply.
  ///
  /// NOTE: this should only be called by memory arbitrator when a root memory
  /// pool tries to grow its capacity for a new reservation request which
  /// exceeds its current capacity limit.
  virtual bool grow(uint64_t growBytes, uint64_t reservationBytes = 0) = 0;

  /// Invoked by addLeafChild() and addAggregateChild() to create a child memory
  /// pool object. 'parent' is a shared pointer created from this, ie,
  /// shared_from_this().
  virtual std::shared_ptr<MemoryPool> genChild(
      std::shared_ptr<MemoryPool> parent,
      const std::string& name,
      Kind kind,
      bool threadSafe,
      std::unique_ptr<MemoryReclaimer> reclaimer) = 0;

  virtual std::exception_ptr abortError() const;

  /// Invoked only on destruction to remove this memory pool from its parent's
  /// child memory pool tracking.
  virtual void dropChild(const MemoryPool* child);

  const std::string name_;
  const Kind kind_;
  const uint16_t alignment_;
  const std::shared_ptr<MemoryPool> parent_;
  const int64_t maxCapacity_;
  const bool trackUsage_;
  const bool threadSafe_;
  const bool debugEnabled_;
  const bool coreOnAllocationFailureEnabled_;

  /// Indicates if the memory pool has been aborted by the memory arbitrator or
  /// not.
  ///
  /// NOTE: this flag is only set for a root memory pool if it has memory
  /// reclaimer. We process a query abort request from the root memory pool.
  std::atomic<bool> aborted_{false};
  /// Saves the aborted error exception which is only set if 'aborted_' is true.
  std::exception_ptr abortError_{nullptr};

  mutable folly::SharedMutex poolMutex_;
  std::unordered_map<std::string, std::weak_ptr<MemoryPool>> children_;

  friend class MemoryReclaimer;
  friend class velox::exec::ParallelMemoryReclaimer;
  friend class MemoryManager;
  friend class MemoryArbitrator;

  VELOX_FRIEND_TEST(MemoryPoolTest, shrinkAndGrowAPIs);
  VELOX_FRIEND_TEST(MemoryPoolTest, grow);
  VELOX_FRIEND_TEST(MemoryPoolTest, growFailures);
  VELOX_FRIEND_TEST(MemoryPoolTest, grownonContiguousAllocateFailures);
};

std::ostream& operator<<(std::ostream& out, MemoryPool::Kind kind);

std::ostream& operator<<(std::ostream& os, const MemoryPool::Stats& stats);

class MemoryPoolImpl : public MemoryPool {
 public:
  /// The callback invoked on the root memory pool destruction. It is set by
  /// memory manager to removes the pool from 'MemoryManager::pools_'.
  using DestructionCallback = std::function<void(MemoryPool*)>;
  /// The callback invoked when the used memory reservation of the root memory
  /// pool exceed its capacity. It is set by memory manager to grow the memory
  /// pool capacity. The callback returns true if the capacity growth succeeds,
  /// otherwise false.
  using GrowCapacityCallback = std::function<bool(MemoryPool*, uint64_t)>;

  MemoryPoolImpl(
      MemoryManager* manager,
      const std::string& name,
      Kind kind,
      std::shared_ptr<MemoryPool> parent,
      std::unique_ptr<MemoryReclaimer> reclaimer,
      GrowCapacityCallback growCapacityCb,
      DestructionCallback destructionCb,
      const Options& options = Options{});

  ~MemoryPoolImpl() override;

  void* allocate(int64_t size) override;

  void* allocateZeroFilled(int64_t numEntries, int64_t sizeEach) override;

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
      ContiguousAllocation& out,
      MachinePageCount maxPages = 0) override;

  void freeContiguous(ContiguousAllocation& allocation) override;

  void growContiguous(
      MachinePageCount increment,
      ContiguousAllocation& allocation) override;

  int64_t capacity() const override;

  int64_t usedBytes() const override;

  int64_t peakBytes() const override {
    std::lock_guard<std::mutex> l(mutex_);
    return peakBytes_;
  }

  int64_t availableReservation() const override {
    std::lock_guard<std::mutex> l(mutex_);
    return availableReservationLocked();
  }

  int64_t releasableReservation() const override;

  int64_t reservedBytes() const override {
    return reservationBytes_;
  }

  bool maybeReserve(uint64_t size) override;

  void release() override;

  uint64_t freeBytes() const override;

  void setReclaimer(std::unique_ptr<MemoryReclaimer> reclaimer) override;

  MemoryReclaimer* reclaimer() const override;

  void enterArbitration() override;

  void leaveArbitration() noexcept override;

  std::optional<uint64_t> reclaimableBytes() const override;

  uint64_t reclaim(
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      memory::MemoryReclaimer::Stats& stats) override;

  void abort(const std::exception_ptr& error) override;

  std::string toString() const override {
    std::lock_guard<std::mutex> l(mutex_);
    return toStringLocked();
  }

  /// Detailed debug pool state printout by traversing the pool structure from
  /// the root memory pool.
  ///
  /// Exceeded memory cap of 5.00MB when requesting 2.00MB
  /// default_root_1 usage 5.00MB peak 5.00MB
  ///     task.test_cursor 1 usage 5.00MB peak 5.00MB
  ///         node.N/A usage 0B peak 0B
  ///             op.N/A.0.0.CallbackSink usage 0B peak 0B
  ///         node.2 usage 4.00MB peak 4.00MB
  ///             op.2.0.0.Aggregation usage 3.77MB peak 3.77MB
  ///         node.1 usage 1.00MB peak 1.00MB
  ///             op.1.0.0.FilterProject usage 12.00KB peak 12.00KB
  ///         node.3 usage 0B peak 0B
  ///             op.3.0.0.OrderBy usage 0B peak 0B
  ///         node.0 usage 0B peak 0B
  ///             op.0.0.0.Values usage 0B peak 0B
  ///
  /// Top 5 leaf memory pool usages:
  ///     op.2.0.0.Aggregation usage 3.77MB peak 3.77MB
  ///     op.1.0.0.FilterProject usage 12.00KB peak 12.00KB
  ///     op.N/A.0.0.CallbackSink usage 0B peak 0B
  ///     op.3.0.0.OrderBy usage 0B peak 0B
  ///     op.0.0.0.Values usage 0B peak 0B
  std::string treeMemoryUsage(bool skipEmptyPool = true) const override;

  Stats stats() const override;

  void testingSetCapacity(int64_t bytes);

  void testingSetReservation(int64_t bytes);

  MemoryManager* testingManager() const {
    return manager_;
  }

  MemoryAllocator* testingAllocator() const {
    return allocator_;
  }

  void testingCheckIfAborted() const {
    checkIfAborted();
  }

  uint64_t testingMinReservationBytes() const {
    return minReservationBytes_;
  }

  /// Structure to store allocation details in debug mode.
  struct AllocationRecord {
    uint64_t size;
    process::StackTrace callStack;
  };

  std::unordered_map<uint64_t, AllocationRecord>& testingDebugAllocRecords() {
    return debugAllocRecords_;
  }

  static void setDebugPoolNameRegex(const std::string& regex) {
    debugPoolNameRegex() = regex;
  }

 private:
  uint64_t shrink(uint64_t targetBytes = 0) override;

  bool grow(uint64_t growBytes, uint64_t reservationBytes = 0) override;

  FOLLY_ALWAYS_INLINE static MemoryPoolImpl* toImpl(MemoryPool* pool) {
    return static_cast<MemoryPoolImpl*>(pool);
  }

  FOLLY_ALWAYS_INLINE static MemoryPoolImpl* toImpl(
      const std::shared_ptr<MemoryPool>& pool) {
    return static_cast<MemoryPoolImpl*>(pool.get());
  }

  static folly::Synchronized<std::string>& debugPoolNameRegex() {
    static folly::Synchronized<std::string> debugPoolNameRegex_;
    return debugPoolNameRegex_;
  }

  std::shared_ptr<MemoryPool> genChild(
      std::shared_ptr<MemoryPool> parent,
      const std::string& name,
      Kind kind,
      bool threadSafe,
      std::unique_ptr<MemoryReclaimer> reclaimer) override;

  FOLLY_ALWAYS_INLINE int64_t capacityLocked() const {
    return parent_ != nullptr ? toImpl(parent_)->capacity_ : capacity_;
  }

  FOLLY_ALWAYS_INLINE int64_t availableReservationLocked() const {
    return !isLeaf()
        ? 0
        : std::max<int64_t>(0, reservationBytes_ - usedReservationBytes_);
  }

  FOLLY_ALWAYS_INLINE int64_t sizeAlign(int64_t size) {
    const auto remainder = size % alignment_;
    return (remainder == 0) ? size : (size + alignment_ - remainder);
  }

  // Returns a rounded up delta based on adding 'delta' to 'size'. Adding the
  // rounded delta to 'size' will result in 'size' a quantized size, rounded to
  // the MB or 8MB for larger sizes.
  FOLLY_ALWAYS_INLINE static int64_t roundedDelta(int64_t size, int64_t delta) {
    return quantizedSize(size + delta) - size;
  }

  // Reserve memory for a new allocation/reservation with specified 'size'.
  // 'reserveThreadSafe' processes the memory reservation with mutex lock
  // protection to prevent concurrent updates to the same leaf memory pool.
  // 'reserveNonThreadSafe' processes the memory reservation without mutex lock
  // at the leaf memory pool.
  void reserve(uint64_t size, bool reserveOnly = false);

  FOLLY_ALWAYS_INLINE void reserveNonThreadSafe(
      uint64_t size,
      bool reserveOnly = false) {
    VELOX_CHECK(isLeaf());

    int32_t numAttempts{0};
    for (;; ++numAttempts) {
      int64_t increment = reservationSizeLocked(size);
      if (FOLLY_LIKELY(increment == 0)) {
        if (FOLLY_UNLIKELY(reserveOnly)) {
          minReservationBytes_ = tsanAtomicValue(reservationBytes_);
        } else {
          usedReservationBytes_ += size;
          cumulativeBytes_ += size;
          maybeUpdatePeakBytesLocked(usedReservationBytes_);
        }
        sanityCheckLocked();
        break;
      }
      incrementReservationNonThreadSafe(this, increment);
    }

    // NOTE: in case of concurrent reserve requests to the same root memory pool
    // from the other leaf memory pools, we might have to retry
    // incrementReservation(). This should happen rarely in production
    // as the leaf tracker does quantized memory reservation so that we don't
    // expect high concurrency at the root memory pool.
    if (FOLLY_UNLIKELY(numAttempts > 1)) {
      numCollisions_ += numAttempts - 1;
    }
  }

  void reserveThreadSafe(uint64_t size, bool reserveOnly = false);

  // Increments the reservation and checks against limits at root tracker. Calls
  // root tracker's 'growCallback_' if it is set and limit exceeded. Should be
  // called without holding 'mutex_'. This function returns true if reservation
  // succeeds. It returns false if there is concurrent reservation increment
  // requests and need a retry from the leaf memory usage tracker. The function
  // throws if a limit is exceeded and there is no corresponding GrowCallback or
  // the GrowCallback fails.
  bool incrementReservationThreadSafe(MemoryPool* requestor, uint64_t size);

  FOLLY_ALWAYS_INLINE bool incrementReservationNonThreadSafe(
      MemoryPool* requestor,
      uint64_t size) {
    VELOX_CHECK_NOT_NULL(parent_);
    VELOX_CHECK(isLeaf());

    if (!toImpl(parent_)->incrementReservationThreadSafe(requestor, size)) {
      return false;
    }

    reservationBytes_ += size;
    return true;
  }

  // Returns the needed reservation size. If there is sufficient unused memory
  // reservation, this function returns zero.
  FOLLY_ALWAYS_INLINE int64_t reservationSizeLocked(int64_t size) {
    const int64_t neededSize =
        size - (reservationBytes_ - usedReservationBytes_);
    if (neededSize <= 0) {
      return 0;
    }
    return roundedDelta(reservationBytes_, neededSize);
  }

  FOLLY_ALWAYS_INLINE void maybeUpdatePeakBytesLocked(int64_t newPeak) {
    peakBytes_ = std::max<int64_t>(peakBytes_, newPeak);
  }

  // Tries to increment the reservation 'size' if it is within the limit and
  // returns true, otherwise the function returns false.
  bool maybeIncrementReservation(uint64_t size);

  void incrementReservationLocked(uint64_t bytes);

  // Release memory reservation for an allocation free or memory release with
  // specified 'size'. If 'releaseOnly' is true, then we only release the unused
  // reservation if 'minReservationBytes_' is set. 'releaseThreadSafe' processes
  // the memory reservation release with mutex lock protection at the leaf
  // memory pool while 'reserveThreadSafe' doesn't.
  void release(uint64_t bytes, bool releaseOnly = false);

  void releaseThreadSafe(uint64_t size, bool releaseOnly);

  FOLLY_ALWAYS_INLINE void releaseNonThreadSafe(
      uint64_t size,
      bool releaseOnly) {
    VELOX_CHECK(isLeaf());
    VELOX_DCHECK_NOT_NULL(parent_);

    int64_t newQuantized;
    if (FOLLY_UNLIKELY(releaseOnly)) {
      VELOX_DCHECK_EQ(size, 0);
      if (minReservationBytes_ == 0) {
        return;
      }
      newQuantized = quantizedSize(usedReservationBytes_);
      minReservationBytes_ = 0;
    } else {
      usedReservationBytes_ -= size;
      const int64_t newCap =
          std::max(minReservationBytes_, usedReservationBytes_);
      newQuantized = quantizedSize(newCap);
    }

    const int64_t freeable = reservationBytes_ - newQuantized;
    if (FOLLY_UNLIKELY(freeable > 0)) {
      reservationBytes_ = newQuantized;
      sanityCheckLocked();
      toImpl(parent_)->decrementReservation(freeable);
    }
  }

  // Decrements the reservation in 'this' and parents.
  void decrementReservation(uint64_t size) noexcept;

  FOLLY_ALWAYS_INLINE void sanityCheckLocked() const {
    if (FOLLY_UNLIKELY(
            (reservationBytes_ < usedReservationBytes_) ||
            (reservationBytes_ < minReservationBytes_) ||
            (usedReservationBytes_ < 0))) {
      VELOX_FAIL("Bad memory usage track state: {}", toStringLocked());
    }
  }

  void setAbortError(const std::exception_ptr& error);

  // Check if this memory pool has been aborted. If already aborted, we rethrow
  // the preserved abort error to prevent this pool from triggering additional
  // memory arbitration. The associated query should also abort soon.
  void checkIfAborted() const;

  Stats statsLocked() const;

  FOLLY_ALWAYS_INLINE std::string toStringLocked() const {
    std::stringstream out;
    out << "Memory Pool[" << name_ << " " << kindString(kind_) << " root["
        << root()->name() << "] parent["
        << (isRoot() ? "null" : parent_->name()) << "] "
        << MemoryAllocator::kindString(allocator_->kind())
        << (trackUsage_ ? " track-usage" : " no-usage-track")
        << (threadSafe_ ? " thread-safe" : " non-thread-safe") << "]<";
    if (maxCapacity() != kMaxMemory) {
      out << "max capacity " << succinctBytes(maxCapacity()) << " ";
    } else {
      out << "unlimited max capacity ";
    }
    if (capacityLocked() != kMaxMemory) {
      out << "capacity " << succinctBytes(capacityLocked()) << " ";
    } else {
      out << "unlimited capacity ";
    }
    out << "used " << succinctBytes(usedBytes()) << " available "
        << succinctBytes(availableReservationLocked());
    out << " reservation [used " << succinctBytes(usedReservationBytes_)
        << ", reserved " << succinctBytes(reservationBytes_) << ", min "
        << succinctBytes(minReservationBytes_);
    out << "] counters [allocs " << numAllocs_ << ", frees " << numFrees_
        << ", reserves " << numReserves_ << ", releases " << numReleases_
        << ", collisions " << numCollisions_ << "])";
    out << ">";
    return out.str();
  }

  // Recording on every allocation is very expensive and normally will result
  // in CPU saturation. Modify this method while debugging to limit the number
  // of times that the allocations are recorded. 'isAlloc' will be true at
  // allocation sites, false at free sites. A good example of this filter would
  // be based on the 'name_' of the MemoryPool.
  //  TODO(jtan6): Add support for dynamic condition change.
  bool needRecordDbg(bool isAlloc);

  // Invoked to record the call stack of a buffer allocation if debug mode of
  // this memory pool is enabled.
  void recordAllocDbg(const void* addr, uint64_t size);

  // Invoked to record the call stack of a non-contiguous allocation if debug
  // mode of this memory pool is enabled.
  void recordAllocDbg(const Allocation& allocation);

  // Invoked to record the call stack of a contiguous allocation if debug mode
  // of this memory pool is enabled.
  void recordAllocDbg(const ContiguousAllocation& allocation);

  // Invoked to free the call stack of a buffer allocation if debug mode of this
  // memory pool is enabled.
  void recordFreeDbg(const void* addr, uint64_t size);

  // Invoked to free the call stack of a non-contiguous allocation if debug mode
  // of this memory pool is enabled.
  void recordFreeDbg(const Allocation& allocation);

  // Invoked to free the call stack of a contiguous allocation if debug mode
  // of this memory pool is enabled.
  void recordFreeDbg(const ContiguousAllocation& allocation);

  // Accounts for ContiguousAllocation size change in growContiguous().
  void recordGrowDbg(const void* addr, uint64_t newSize);

  // Invoked by memory pool destructor to detect the sources of leaked memory
  // allocations from the call sites which are still recorded in
  // 'debugAllocRecords_'. If there is no memory leaks, 'debugAllocRecords_'
  // should be empty as all the memory allocations should have been freed on
  // memory pool destruction. We only check this if debug mode of this memory
  // pool is enabled.
  void leakCheckDbg();

  void handleAllocationFailure(const std::string& failureMessage);

  MemoryManager* const manager_;
  MemoryAllocator* const allocator_;
  const GrowCapacityCallback growCapacityCb_;
  const DestructionCallback destructionCb_;

  // Regex for filtering on 'name_' when debug mode is enabled. This allows us
  // to only track the callsites of memory allocations for memory pools whose
  // name matches the specified regular expression 'debugPoolNameRegex_'.
  const std::string debugPoolNameRegex_;

  // Serializes updates on 'reservationBytes_', 'usedReservationBytes_'
  // and 'minReservationBytes_' to make reservation decision on a consistent
  // read/write of those counters. incrementReservation()/decrementReservation()
  // work based on atomic 'reservationBytes_' without mutex as children updating
  // the same parent do not have to be serialized.
  mutable std::mutex mutex_;

  // Used by memory arbitration to reclaim memory from the associated query
  // object if not null. For example, a memory pool can reclaim the used memory
  // from a spillable operator through disk spilling. If null, we can't reclaim
  // memory from this memory pool.
  std::unique_ptr<MemoryReclaimer> reclaimer_;

  // The memory cap in bytes to enforce.
  int64_t capacity_;

  // The number of reservation bytes.
  tsan_atomic<int64_t> reservationBytes_{0};

  // The number of used reservation bytes which is maintained at the leaf
  // tracker and protected by mutex for consistent memory reservation/release
  // decisions.
  tsan_atomic<int64_t> usedReservationBytes_{0};

  // Minimum amount of reserved memory in bytes to hold until explicit
  // release().
  tsan_atomic<int64_t> minReservationBytes_{0};

  tsan_atomic<int64_t> peakBytes_{0};
  tsan_atomic<int64_t> cumulativeBytes_{0};

  // Stats counters.
  // The number of memory allocations.
  std::atomic_uint64_t numAllocs_{0};

  // The number of memory frees.
  std::atomic_uint64_t numFrees_{0};

  // The number of external memory reservations made through maybeReserve().
  std::atomic_uint64_t numReserves_{0};

  // The number of external memory releases made through release().
  std::atomic_uint64_t numReleases_{0};

  // The number of internal memory reservation collisions caused by concurrent
  // memory reservation requests.
  std::atomic_uint64_t numCollisions_{0};

  // The number of memory capacity growth attempts through the memory
  // arbitration.
  //
  // NOTE: this only applies for root memory pool.
  std::atomic_uint64_t numCapacityGrowths_{0};

  // Mutex for 'debugAllocRecords_'.
  std::mutex debugAllocMutex_;

  // Map from address to 'AllocationRecord'.
  std::unordered_map<uint64_t, AllocationRecord> debugAllocRecords_;
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

template <>
struct fmt::formatter<facebook::velox::memory::MemoryPool::Kind>
    : formatter<std::string> {
  auto format(
      facebook::velox::memory::MemoryPool::Kind s,
      format_context& ctx) {
    return formatter<std::string>::format(
        facebook::velox::memory::MemoryPool::kindString(s), ctx);
  }
};
