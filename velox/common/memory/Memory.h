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
#include "velox/common/base/GTestMacros.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/memory/Allocation.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/MemoryPool.h"

DECLARE_int32(memory_usage_aggregation_interval_millis);
DECLARE_bool(velox_memory_leak_check_enabled);

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

/// This class provides the interface of memory manager. The memory manager is
/// responsible for enforcing the memory capacity is within the capacity as well
/// as managing the memory pools.
class IMemoryManager {
 public:
  struct Options {
    /// Specifies the default memory allocation alignment.
    uint16_t alignment{MemoryAllocator::kMaxAlignment};

    /// Specifies the max memory capacity in bytes.
    int64_t capacity{kMaxMemory};

    /// If true, check the memory pool and usage leaks on destruction.
    ///
    /// TODO: deprecate this flag after all the existing memory leak use cases
    /// have been fixed.
    bool checkUsageLeak{FLAGS_velox_memory_leak_check_enabled};

    /// Specifies the backing memory allocator.
    MemoryAllocator* allocator{MemoryAllocator::getInstance()};

    /// Specifies the memory arbitration config.
    MemoryArbitrator::Config arbitratorConfig{};
  };

  virtual ~IMemoryManager() = default;

  /// Returns the memory capacity of this memory manager which puts a hard cap
  /// on the memory usage, and any allocation that would exceed this capacity
  /// throws.
  virtual int64_t capacity() const = 0;

  /// Returns the memory allocation alignment of this memory manager.
  virtual uint16_t alignment() const = 0;

  /// Creates a root memory pool with specified 'name' and 'capacity'. If 'name'
  /// is missing, the memory manager generates a default name internally to
  /// ensure uniqueness.
  virtual std::shared_ptr<MemoryPool> addRootPool(
      const std::string& name = "",
      int64_t capacity = kMaxMemory,
      std::unique_ptr<MemoryReclaimer> reclaimer = nullptr) = 0;

  /// Creates a leaf memory pool for direct memory allocation use with specified
  /// 'name'. If 'name' is missing, the memory manager generates a default name
  /// internally to ensure uniqueness. The leaf memory pool is created as the
  /// child of the memory manager's default root memory pool. If 'threadSafe' is
  /// true, then we track its memory usage in a non-thread-safe mode to reduce
  /// its cpu cost.
  virtual std::shared_ptr<MemoryPool> addLeafPool(
      const std::string& name = "",
      bool threadSafe = true) = 0;

  /// Invoked to grows a memory pool's free capacity with at least
  /// 'incrementBytes'. The function returns true on success, otherwise false.
  virtual bool growPool(MemoryPool* pool, uint64_t incrementBytes) = 0;

  /// Default unmanaged leaf pool with no threadsafe stats support. Libraries
  /// using this method can get a pool that is shared with other threads. The
  /// goal is to minimize lock contention while supporting such use cases.
  ///
  /// TODO: deprecate this API after all the use cases are able to manage the
  /// lifecycle of the allocated memory pools properly.
  virtual MemoryPool& deprecatedSharedLeafPool() = 0;

  /// Returns the number of alive memory pools allocated from addRootPool() and
  /// addLeafPool().
  ///
  /// NOTE: this doesn't count the memory manager's internal default root and
  /// leaf memory pools.
  virtual size_t numPools() const = 0;

  /// Returns the current total memory usage under this memory manager.
  virtual int64_t getTotalBytes() const = 0;

  /// Reserves size for the allocation. Returns true if the total usage remains
  /// under capacity after the reservation. Caller is responsible for releasing
  /// the offending reservation.
  ///
  /// TODO: deprecate this and enforce the memory usage capacity by memory
  /// allocator.
  virtual bool reserve(int64_t size) = 0;

  /// Subtracts from current total memory usage.
  ///
  /// TODO: deprecate this and enforce the memory usage capacity by memory
  /// allocator.
  virtual void release(int64_t size) = 0;

  /// Returns debug string of this memory manager.
  virtual std::string toString() const = 0;
};

/// For now, users wanting multiple different allocators would need to
/// instantiate different MemoryManager classes and manage them across static
/// boundaries.
class MemoryManager final : public IMemoryManager {
 public:
  /// Tries to get the singleton memory manager. If not previously initialized,
  /// the process singleton manager will be initialized with the given capacity.
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

  std::shared_ptr<MemoryPool> addRootPool(
      const std::string& name = "",
      int64_t maxBytes = kMaxMemory,
      std::unique_ptr<MemoryReclaimer> reclaimer = nullptr) final;

  std::shared_ptr<MemoryPool> addLeafPool(
      const std::string& name = "",
      bool threadSafe = true) final;

  bool growPool(MemoryPool* pool, uint64_t incrementBytes) final;

  MemoryPool& deprecatedSharedLeafPool() final;

  int64_t getTotalBytes() const final;

  bool reserve(int64_t size) final;
  void release(int64_t size) final;

  size_t numPools() const final;

  MemoryAllocator& allocator();

  MemoryArbitrator* arbitrator();

  std::string toString() const final;

  /// Returns the memory manger's internal default root memory pool for testing
  /// purpose.
  MemoryPool& testingDefaultRoot() const {
    return *defaultRoot_;
  }

  const std::vector<std::shared_ptr<MemoryPool>>& testingSharedLeafPools() {
    return sharedLeafPools_;
  }

 private:
  void dropPool(MemoryPool* pool);

  //  Returns the shared references to all the alive memory pools in 'pools_'.
  std::vector<std::shared_ptr<MemoryPool>> getAlivePools() const;

  const int64_t capacity_;
  const std::shared_ptr<MemoryAllocator> allocator_;
  // If not null, used to arbitrate the memory capacity among 'pools_'.
  const std::unique_ptr<MemoryArbitrator> arbitrator_;
  const uint16_t alignment_;
  const bool checkUsageLeak_;
  // The destruction callback set for the allocated  root memory pools which are
  // tracked by 'pools_'. It is invoked on the root pool destruction and removes
  // the pool from 'pools_'.
  const MemoryPoolImpl::DestructionCallback poolDestructionCb_;

  const std::shared_ptr<MemoryPool> defaultRoot_;
  std::vector<std::shared_ptr<MemoryPool>> sharedLeafPools_;

  mutable folly::SharedMutex mutex_;
  std::atomic_long totalBytes_{0};
  std::unordered_map<std::string, std::weak_ptr<MemoryPool>> pools_;
};

IMemoryManager& defaultMemoryManager();

/// Creates a leaf memory pool from the default memory manager for memory
/// allocation use. If 'threadSafe' is true, then creates a leaf memory pool
/// with thread-safe memory usage tracking.
std::shared_ptr<MemoryPool> addDefaultLeafMemoryPool(
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

FOLLY_ALWAYS_INLINE int32_t alignmentPadding(void* address, int32_t alignment) {
  auto extra = reinterpret_cast<uintptr_t>(address) % alignment;
  return extra == 0 ? 0 : alignment - extra;
}
} // namespace facebook::velox::memory
