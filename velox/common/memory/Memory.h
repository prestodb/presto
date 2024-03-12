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
#include "folly/GLog.h"
#include "folly/Likely.h"
#include "folly/Random.h"
#include "folly/SharedMutex.h"
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/memory/Allocation.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MemoryPool.h"

DECLARE_bool(velox_memory_leak_check_enabled);
DECLARE_bool(velox_memory_pool_debug_enabled);
DECLARE_bool(velox_enable_memory_usage_track_in_default_memory_pool);

namespace facebook::velox::memory {
#define VELOX_MEM_LOG_PREFIX "[MEM] "
#define VELOX_MEM_LOG(severity) LOG(severity) << VELOX_MEM_LOG_PREFIX
#define VELOX_MEM_LOG_EVERY_MS(severity, ms) \
  FB_LOG_EVERY_MS(severity, ms) << VELOX_MEM_LOG_PREFIX

#define VELOX_MEM_ALLOC_ERROR(errorMessage)                         \
  _VELOX_THROW(                                                     \
      ::facebook::velox::VeloxRuntimeError,                         \
      ::facebook::velox::error_source::kErrorSourceRuntime.c_str(), \
      ::facebook::velox::error_code::kMemAllocError.c_str(),        \
      /* isRetriable */ true,                                       \
      "{}",                                                         \
      errorMessage);

struct MemoryManagerOptions {
  /// Specifies the default memory allocation alignment.
  uint16_t alignment{MemoryAllocator::kMaxAlignment};

  /// If true, enable memory usage tracking in the default memory pool.
  bool trackDefaultUsage{
      FLAGS_velox_enable_memory_usage_track_in_default_memory_pool};

  /// If true, check the memory pool and usage leaks on destruction.
  ///
  /// TODO: deprecate this flag after all the existing memory leak use cases
  /// have been fixed.
  bool checkUsageLeak{FLAGS_velox_memory_leak_check_enabled};

  /// If true, the memory pool will be running in debug mode to track the
  /// allocation and free call stacks to detect the source of memory leak for
  /// testing purpose.
  bool debugEnabled{FLAGS_velox_memory_pool_debug_enabled};

  /// Terminates the process and generates a core file on an allocation failure
  bool coreOnAllocationFailureEnabled{false};

  /// ================== 'MemoryAllocator' settings ==================
  /// Specifies the max memory allocation capacity in bytes enforced by
  /// MemoryAllocator, default unlimited.
  int64_t allocatorCapacity{kMaxMemory};

  /// If true, uses MmapAllocator for memory allocation which manages the
  /// physical memory allocation on its own through std::mmap techniques. If
  /// false, use MallocAllocator which delegates the memory allocation to
  /// std::malloc.
  bool useMmapAllocator{false};

  /// If true, allocations larger than largest size class size will be delegated
  /// to ManagedMmapArena. Otherwise a system mmap call will be issued for each
  /// such allocation.
  ///
  /// NOTE: this only applies for MmapAllocator.
  bool useMmapArena{false};

  /// Used to determine MmapArena capacity. The ratio represents
  /// 'allocatorCapacity' to single MmapArena capacity ratio.
  ///
  /// NOTE: this only applies for MmapAllocator.
  int32_t mmapArenaCapacityRatio{10};

  /// If not zero, reserve 'smallAllocationReservePct'% of space from
  /// 'allocatorCapacity' for ad hoc small allocations. And those allocations
  /// are delegated to std::malloc. If 'maxMallocBytes' is 0, this value will be
  /// disregarded.
  ///
  /// NOTE: this only applies for MmapAllocator.
  uint32_t smallAllocationReservePct{0};

  /// The allocation threshold less than which an allocation is delegated to
  /// std::malloc(). If it is zero, then we don't delegate any allocation
  /// std::malloc, and 'smallAllocationReservePct' will be automatically set to
  /// 0 disregarding any passed in value.
  ///
  /// NOTE: this only applies for MmapAllocator.
  int32_t maxMallocBytes{3072};

  /// The memory allocations with size smaller than this threshold check the
  /// capacity with local sharded counter to reduce the lock contention on the
  /// global allocation counter. The sharded local counters reserve/release
  /// memory capacity from the global counter in batch. With this optimization,
  /// we don't have to update the global counter for each individual small
  /// memory allocation. If it is zero, then this optimization is disabled. The
  /// default is 1MB.
  ///
  /// NOTE: this only applies for MallocAllocator.
  uint32_t allocationSizeThresholdWithReservation{1 << 20};

  /// ================== 'MemoryArbitrator' settings =================

  /// Memory capacity available for query/task memory pools. This capacity
  /// setting should be equal or smaller than 'allocatorCapacity'. The
  /// difference between 'allocatorCapacity' and 'arbitratorCapacity' is
  /// reserved for system usage such as cache and spilling.
  ///
  /// NOTE:
  /// - if 'arbitratorCapacity' is greater than 'allocatorCapacity', the
  /// behavior will be equivalent to as if they are equal, meaning no
  /// reservation capacity for system usage.
  int64_t arbitratorCapacity{kMaxMemory};

  /// The string kind of memory arbitrator used in the memory manager.
  ///
  /// NOTE: the arbitrator will only be created if its kind is set explicitly.
  /// Otherwise MemoryArbitrator::create returns a nullptr.
  std::string arbitratorKind{};

  /// The initial memory capacity to reserve for a newly created memory pool.
  uint64_t memoryPoolInitCapacity{256 << 20};

  /// The minimal memory capacity to transfer out of or into a memory pool
  /// during the memory arbitration.
  uint64_t memoryPoolTransferCapacity{32 << 20};

  /// Specifies the max time to wait for memory reclaim by arbitration. The
  /// memory reclaim might fail if the max wait time has exceeded. If it is
  /// zero, then there is no timeout. The default is 5 mins.
  uint64_t memoryReclaimWaitMs{300'000};

  /// Provided by the query system to validate the state after a memory pool
  /// enters arbitration if not null. For instance, Prestissimo provides
  /// callback to check if a memory arbitration request is issued from a driver
  /// thread, then the driver should be put in suspended state to avoid the
  /// potential deadlock when reclaim memory from the task of the request memory
  /// pool.
  MemoryArbitrationStateCheckCB arbitrationStateCheckCb{nullptr};
};

/// 'MemoryManager' is responsible for creating allocator, arbitrator and
/// managing the memory pools.
class MemoryManager {
 public:
  explicit MemoryManager(
      const MemoryManagerOptions& options = MemoryManagerOptions{});

  ~MemoryManager();

  /// Creates process-wide memory manager using specified options. Throws if
  /// memory manager has already been created by an easier call.
  static void initialize(const MemoryManagerOptions& options);

  /// Returns process-wide memory manager. Throws if 'initialize' hasn't been
  /// called yet.
  static MemoryManager* getInstance();

  /// Deprecated. Do not use. Remove once existing call sites are updated.
  /// Returns the process-wide default memory manager instance if exists,
  /// otherwise creates one based on the specified 'options'.
  FOLLY_EXPORT static MemoryManager& deprecatedGetInstance(
      const MemoryManagerOptions& options = MemoryManagerOptions{});

  /// Used by test to override the process-wide memory manager.
  static MemoryManager& testingSetInstance(const MemoryManagerOptions& options);

  /// Returns the memory capacity of this memory manager which puts a hard cap
  /// on memory usage, and any allocation that exceeds this capacity throws.
  int64_t capacity() const;

  /// Returns the memory allocation alignment of this memory manager.
  uint16_t alignment() const;

  /// Creates a root memory pool with specified 'name' and 'maxCapacity'. If
  /// 'name' is missing, the memory manager generates a default name internally
  /// to ensure uniqueness.
  std::shared_ptr<MemoryPool> addRootPool(
      const std::string& name = "",
      int64_t maxCapacity = kMaxMemory,
      std::unique_ptr<MemoryReclaimer> reclaimer = nullptr);

  /// Creates a leaf memory pool for direct memory allocation use with specified
  /// 'name'. If 'name' is missing, the memory manager generates a default name
  /// internally to ensure uniqueness. The leaf memory pool is created as the
  /// child of the memory manager's default root memory pool. If 'threadSafe' is
  /// true, then we track its memory usage in a non-thread-safe mode to reduce
  /// its cpu cost.
  std::shared_ptr<MemoryPool> addLeafPool(
      const std::string& name = "",
      bool threadSafe = true);

  /// Invoked to shrink alive pools to free 'targetBytes' capacity. The function
  /// returns the actual freed memory capacity in bytes. If 'targetBytes' is
  /// zero, then try to reclaim all the memory from the alive pools. If
  /// 'allowSpill' is true, it reclaims the used memory by spilling. If
  /// 'allowAbort' is true, it reclaims the used memory by aborting the queries
  /// with the most memory usage. If both are true, it first reclaims the used
  /// memory by spilling and then abort queries to reach the reclaim target.
  uint64_t shrinkPools(
      uint64_t targetBytes = 0,
      bool allowSpill = true,
      bool allowAbort = false);

  /// Default unmanaged leaf pool with no threadsafe stats support. Libraries
  /// using this method can get a pool that is shared with other threads. The
  /// goal is to minimize lock contention while supporting such use cases.
  ///
  /// TODO: deprecate this API after all the use cases are able to manage the
  /// lifecycle of the allocated memory pools properly.
  MemoryPool& deprecatedSharedLeafPool();

  /// Returns the current total memory usage under this memory manager.
  int64_t getTotalBytes() const;

  /// Returns the number of alive memory pools allocated from addRootPool() and
  /// addLeafPool().
  ///
  /// NOTE: this doesn't count the memory manager's internal default root and
  /// leaf memory pools.
  size_t numPools() const;

  MemoryAllocator* allocator();

  MemoryArbitrator* arbitrator();

  /// Returns debug string of this memory manager. If 'detail' is true, it
  /// returns the detailed tree memory usage from all the top level root memory
  /// pools.
  std::string toString(bool detail = false) const;

  /// Returns the memory manger's internal default root memory pool for testing
  /// purpose.
  MemoryPool& testingDefaultRoot() const {
    return *defaultRoot_;
  }

  /// Returns the process wide leaf memory pool used for disk spilling.
  MemoryPool* spillPool() {
    return spillPool_.get();
  }

  const std::vector<std::shared_ptr<MemoryPool>>& testingSharedLeafPools() {
    return sharedLeafPools_;
  }

  bool testingGrowPool(MemoryPool* pool, uint64_t incrementBytes) {
    return growPool(pool, incrementBytes);
  }

 private:
  void dropPool(MemoryPool* pool);

  // Invoked to grow a memory pool's free capacity with at least
  // 'incrementBytes'. The function returns true on success, otherwise false.
  bool growPool(MemoryPool* pool, uint64_t incrementBytes);

  //  Returns the shared references to all the alive memory pools in 'pools_'.
  std::vector<std::shared_ptr<MemoryPool>> getAlivePools() const;

  const std::shared_ptr<MemoryAllocator> allocator_;
  // Specifies the capacity to allocate from 'arbitrator_' for a newly created
  // root memory pool.
  const uint64_t poolInitCapacity_;
  // If not null, used to arbitrate the memory capacity among 'pools_'.
  const std::unique_ptr<MemoryArbitrator> arbitrator_;
  const uint16_t alignment_;
  const bool checkUsageLeak_;
  const bool debugEnabled_;
  const bool coreOnAllocationFailureEnabled_;
  // The destruction callback set for the allocated root memory pools which are
  // tracked by 'pools_'. It is invoked on the root pool destruction and removes
  // the pool from 'pools_'.
  const MemoryPoolImpl::DestructionCallback poolDestructionCb_;
  // Callback invoked by the root memory pool to request memory capacity growth.
  const MemoryPoolImpl::GrowCapacityCallback poolGrowCb_;

  const std::shared_ptr<MemoryPool> defaultRoot_;
  const std::shared_ptr<MemoryPool> spillPool_;

  std::vector<std::shared_ptr<MemoryPool>> sharedLeafPools_;

  mutable folly::SharedMutex mutex_;
  std::unordered_map<std::string, std::weak_ptr<MemoryPool>> pools_;
};

/// Initializes the process-wide memory manager based on the specified
/// 'options'.
///
/// NOTE: user should only call this once on query system startup. Otherwise,
/// the function throws.
void initializeMemoryManager(const MemoryManagerOptions& options);

/// Returns the process-wide memory manager.
///
/// NOTE: user should have already initialized memory manager by calling.
/// Otherwise, the function throws.
MemoryManager* memoryManager();

/// Deprecated. Do not use.
MemoryManager& deprecatedDefaultMemoryManager();

/// Deprecated. Do not use.
/// Creates a leaf memory pool from the default memory manager for memory
/// allocation use. If 'threadSafe' is true, then creates a leaf memory pool
/// with thread-safe memory usage tracking.
std::shared_ptr<MemoryPool> deprecatedAddDefaultLeafMemoryPool(
    const std::string& name = "",
    bool threadSafe = true);

/// Default unmanaged leaf pool with no threadsafe stats support. Libraries
/// using this method can get a pool that is shared with other threads. The goal
/// is to minimize lock contention while supporting such use cases.
///
///
/// TODO: deprecate this API after all the use cases are able to manage the
/// lifecycle of the allocated memory pools properly.
MemoryPool& deprecatedSharedLeafPool();

/// Returns the system-wide memory pool for spilling memory usage.
memory::MemoryPool* spillMemoryPool();

/// Returns true if the provided 'pool' is the spilling memory pool.
bool isSpillMemoryPool(memory::MemoryPool* pool);

FOLLY_ALWAYS_INLINE int32_t alignmentPadding(void* address, int32_t alignment) {
  auto extra = reinterpret_cast<uintptr_t>(address) % alignment;
  return extra == 0 ? 0 : alignment - extra;
}
} // namespace facebook::velox::memory
