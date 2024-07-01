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
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_set>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Allocation.h"
#include "velox/common/time/Timer.h"

DECLARE_bool(velox_time_allocations);

namespace facebook::velox::memory {

struct SizeClassStats {
  //// Size of the tracked size class  in pages.
  int32_t size{0};

  /// Cumulative CPU clocks spent inside allocation.
  std::atomic<uint64_t> allocateClocks{0};

  /// Cumulative CPU clocks spent inside free.
  std::atomic<uint64_t> freeClocks{0};

  /// Cumulative count of distinct allocations.
  std::atomic<int64_t> numAllocations{0};

  /// Cumulative count of bytes allocated. This is not size * numAllocations for
  /// large classes where the allocation does not have the exact size of the
  /// size class.
  std::atomic<int64_t> totalBytes{0};

  SizeClassStats() = default;
  SizeClassStats(const SizeClassStats& other) {
    *this = other;
  }

  void operator=(const SizeClassStats& other) {
    size = other.size;
    allocateClocks = static_cast<int64_t>(other.allocateClocks);
    freeClocks = static_cast<int64_t>(other.freeClocks);
    numAllocations = static_cast<int64_t>(other.numAllocations);
    totalBytes = static_cast<int64_t>(other.totalBytes);
  }

  SizeClassStats operator-(const SizeClassStats& other) const {
    SizeClassStats result;
    result.size = size;
    result.allocateClocks = allocateClocks - other.allocateClocks;
    result.allocateClocks = freeClocks - other.freeClocks;
    result.numAllocations = numAllocations - other.numAllocations;
    result.totalBytes = totalBytes - other.totalBytes;
    return result;
  }

  /// Returns the total clocks for this size class.
  uint64_t clocks() const {
    return allocateClocks + freeClocks;
  }
};

struct Stats {
  /// 20 size classes in powers of 2 are tracked, from 4K to 4G. The
  /// allocation is recorded to the class corresponding to the closest
  /// power of 2 >= the allocation size.
  static constexpr int32_t kNumSizes = 20;
  Stats() {
    for (auto i = 0; i < sizes.size(); ++i) {
      sizes[i].size = 1 << i;
    }
  }

  Stats operator-(const Stats& stats) const;

  template <typename Op>
  void recordAllocate(int64_t bytes, int32_t count, Op op) {
    if (FLAGS_velox_time_allocations) {
      auto index = sizeIndex(bytes);
      velox::ClockTimer timer(sizes[index].allocateClocks);
      op();
      sizes[index].numAllocations += count;
      sizes[index].totalBytes += bytes * count;
    } else {
      op();
    }
  }

  template <typename Op>
  void recordFree(int64_t bytes, Op op) {
    if (FLAGS_velox_time_allocations) {
      auto index = sizeIndex(bytes);
      ClockTimer timer(sizes[index].freeClocks);
      op();
    } else {
      op();
    }
  }

  std::string toString() const;

  /// Returns the size class index for a given size. Here the accounting is in
  /// steps of powers of two. Allocators may have their own size classes or
  /// allocate exact sizes.
  static int32_t sizeIndex(int64_t size) {
    if (size == 0) {
      return 0;
    }
    const auto power = bits::nextPowerOfTwo(size / AllocationTraits::kPageSize);
    return std::min(kNumSizes - 1, 63 - bits::countLeadingZeros(power));
  }

  /// Counters for each size class.
  std::array<SizeClassStats, kNumSizes> sizes;

  /// Cumulative count of pages advised away, if the allocator exposes this.
  int64_t numAdvise{0};
};

class MemoryAllocator;

/// A general cache interface using 'MemoryAllocator' to allocate memory, that
/// is also able to free up memory upon request by shrinking itself.
class Cache {
 public:
  virtual ~Cache() = default;

  /// This method should be implemented so that it tries to
  /// accommodate the passed in 'allocate' by freeing up space from
  /// 'this' if needed. 'numPages' is the number of pages 'allocate
  /// needs to be free for allocate to succeed. This should return
  /// true if 'allocate' succeeds, and false otherwise. 'numPages' can
  /// be less than the planned allocation, even 0 but not
  /// negative. This is possible if 'allocate' brings its own memory
  /// that is exchanged for the new allocation.
  virtual bool makeSpace(
      memory::MachinePageCount numPages,
      std::function<bool(Allocation&)> allocate) = 0;

  /// This method is implemented to shrink the cache space with the specified
  /// 'targetBytes'. The method returns the actually freed cache space in bytes.
  virtual uint64_t shrink(uint64_t targetBytes) = 0;

  virtual MemoryAllocator* allocator() const = 0;
};

/// Sets a thread level failure message describing cache state. Used
/// for example to expose why space could not be freed from
/// cache. This is defined here with the abstract Cache base class
/// and not the cache implementation because allocator cannot depend
/// on cache.
void setCacheFailureMessage(std::string message);

/// Returns and clears a thread local message set with
/// setCacheFailureMessage().
std::string getAndClearCacheFailureMessage();

/// This class provides interface for the actual memory allocations from memory
/// pool. It allocates runs of machine pages from predefined size classes, and
/// supports both contiguous and non-contiguous memory allocations. An
/// non-contiguous allocation that does not match a size class is composed of
/// multiple runs from different size classes. To get 11 pages, one could have a
/// run of 8, one of 2 and one of 1 page. This is intended for all high volume
/// allocations, like caches, IO buffers and hash tables for join/group by.
/// Implementations may use malloc or mmap/madvise. Caches subclass this to
/// provide allocation that is fungible with cached capacity, i.e. a cache can
/// evict data to make space for non-cache memory users. The point is to have
/// all large allocation come from a single source to have dynamic balancing
/// between different users. Proxy subclasses may provide context specific
/// tracking while delegating the allocation to a root allocator.
class MemoryAllocator : public std::enable_shared_from_this<MemoryAllocator> {
 public:
  /// Defines the memory allocator kinds.
  enum class Kind {
    /// The default memory allocator kind which is implemented by
    /// MallocAllocator. It delegates the memory allocations to std::malloc.
    kMalloc,
    /// The memory allocator kind which is implemented by MmapAllocator. It
    /// manages the large chunk of memory allocations on its own by leveraging
    /// mmap and madvise, to optimize the memory fragmentation in the long
    /// running service such as Prestissimo.
    kMmap,
  };

  static std::string kindString(Kind kind);

  virtual ~MemoryAllocator() = default;

  static constexpr int32_t kMaxSizeClasses = 12;
  static constexpr uint16_t kMinAlignment = alignof(max_align_t);
  static constexpr uint16_t kMaxAlignment = 64;

  /// Returns the kind of this memory allocator. For AsyncDataCache, it returns
  /// the kind of the delegated memory allocator underneath.
  virtual Kind kind() const = 0;

  /// Registers a 'Cache' that is used for freeing up space when this allocator
  /// is under memory pressure. The allocator of registered 'Cache' needs to be
  /// the same as 'this'.
  virtual void registerCache(const std::shared_ptr<Cache>& cache) = 0;

  using ReservationCallback = std::function<void(uint64_t, bool)>;

  /// Returns the capacity of the allocator in bytes.
  virtual size_t capacity() const = 0;

  /// Allocates one or more runs that add up to at least 'numPages', with the
  /// smallest run being at least 'minSizeClass' pages. 'minSizeClass' must be
  /// <= the size of the largest size class. The new memory is returned in 'out'
  /// and any memory formerly referenced by 'out' is freed. 'reservationCB' is
  /// called with the actual allocation bytes and a flag indicating if it is
  /// called for pre-allocation or post-allocation failure. The flag is true for
  /// pre-allocation call and false for post-allocation failure call. The latter
  /// is to let user have a chance to rollback if needed. As for now, it is only
  /// used by 'MemoryPoolImpl' object to make memory counting reservation in
  /// 'reservationCB' before the actual memory allocation so it needs to release
  /// the reservation if the actual allocation fails halfway. The function
  /// returns true if the allocation succeeded. If it returns false, 'out'
  /// references no memory and any partially allocated memory is freed. The
  /// function might retry allocation failure by making space from 'cache()' if
  /// registered. But sufficient space is not guaranteed.
  ///
  /// NOTE:
  ///  - 'out' is guaranteed to be freed if it's not empty.
  ///  - Allocation is not guaranteed even if collateral 'out' is larger than
  ///    'numPages', because this method is not atomic.
  ///  - Throws if allocation exceeds capacity.
  bool allocateNonContiguous(
      MachinePageCount numPages,
      Allocation& out,
      ReservationCallback reservationCB = nullptr,
      MachinePageCount minSizeClass = 0);

  /// Frees non-contiguous 'allocation'. 'allocation' is empty on return. The
  /// function returns the actual freed bytes.
  virtual int64_t freeNonContiguous(Allocation& allocation) = 0;

  /// Makes a contiguous mmap of 'numPages' or 'maxPages'. 'maxPages' defaults
  /// to 'numPages'.  Advises away the required number of free pages so as not
  /// to have resident size exceed the capacity if capacity is bounded. Returns
  /// false if sufficient free pages do not exist. 'collateral' and 'allocation'
  /// are freed and unmapped or advised away to provide pages to back the new
  /// 'allocation'. This will always succeed if collateral and allocation
  /// together cover the new size of allocation. 'allocation' is newly mapped
  /// and hence zeroed. The contents of 'allocation' and 'collateral' are freed
  /// in all cases, also if the allocation fails. 'reservationCB' is used in the
  /// same way as allocate does. It may throw and the end state will be
  /// consistent, with no new allocation and 'allocation' and 'collateral'
  /// cleared.
  ///
  /// NOTE: - 'collateral' and passed in 'allocation' are guaranteed
  /// to be freed. If 'maxPages' is non-0, 'maxPages' worth of address space is
  /// mapped but the utilization in the allocator and pool is incremented by
  /// 'numPages'. This allows reserving a large range of addresses for use with
  /// huge pages without declaring the whole range as held by the query. The
  /// reservation will be increased as and if addresses in the range are used.
  /// See growContiguous().
  bool allocateContiguous(
      MachinePageCount numPages,
      Allocation* collateral,
      ContiguousAllocation& allocation,
      ReservationCallback reservationCB = nullptr,
      MachinePageCount maxPages = 0);

  /// Frees contiguous 'allocation'. 'allocation' is empty on return.
  virtual void freeContiguous(ContiguousAllocation& allocation) = 0;

  /// Increments the reserved part of 'allocation' by
  /// 'increment'. false if would exceed capacity, Throws if size
  /// would exceed maxSize given in allocateContiguous(). Calls reservationCB
  /// before increasing the utilization and returns false with no effect if this
  /// fails. The function might retry allocation failure by making
  /// space from 'cache()' if registered. But sufficient space is not guaranteed
  bool growContiguous(
      MachinePageCount increment,
      ContiguousAllocation& allocation,
      ReservationCallback reservationCB = nullptr);

  /// Allocates contiguous 'bytes' and return the first byte. Returns nullptr if
  /// there is no space. The function might retry allocation failure by making
  /// space from 'cache()' if registered. But sufficient space is not
  /// guaranteed.
  ///
  /// NOTE: 'alignment' must be power of two and in range of
  /// [kMinAlignment, kMaxAlignment].
  void* allocateBytes(uint64_t bytes, uint16_t alignment = kMinAlignment);

  /// Allocates a zero-filled contiguous bytes. Returns nullptr if there is no
  /// space. The function might retry allocation failure by making space from
  /// 'cache()' if registered. But sufficient space is not guaranteed.
  void* allocateZeroFilled(uint64_t bytes);

  /// Frees contiguous memory allocated by allocateBytes, allocateZeroFilled,
  /// reallocateBytes.
  virtual void freeBytes(void* p, uint64_t size) noexcept = 0;

  /// Unmaps the unused memory space to return the backing physical pages back
  /// to the operating system. This only works for MmapAllocator implementation
  /// which manages the physical memory on its own by mmap. The function returns
  /// the number of actual unmapped physical pages.
  virtual MachinePageCount unmap(MachinePageCount targetPages) = 0;

  /// Checks internal consistency of allocation data structures. Returns true if
  /// OK.
  virtual bool checkConsistency() const = 0;

  /// Returns the largest class size.
  virtual MachinePageCount largestSizeClass() const {
    return sizeClassSizes_.back();
  }

  virtual const std::vector<MachinePageCount>& sizeClasses() const {
    return sizeClassSizes_;
  }

  /// Returns the total number of used bytes by this allocator
  virtual size_t totalUsedBytes() const = 0;

  virtual MachinePageCount numAllocated() const = 0;

  virtual MachinePageCount numMapped() const = 0;

  virtual Stats stats() const {
    return stats_;
  }

  virtual std::string toString() const = 0;

  /// Invoked to check if 'alignmentBytes' is valid and 'allocateBytes' is
  /// multiple of 'alignmentBytes'. Returns true if check succeeds, false
  /// otherwise
  static bool isAlignmentValid(uint64_t allocateBytes, uint16_t alignmentBytes);

  /// Invoked to check if 'alignmentBytes' is valid and 'allocateBytes' is
  /// multiple of 'alignmentBytes'. Semantically the same as isAlignmentValid().
  /// Throws if check fails.
  static void alignmentCheck(uint64_t allocateBytes, uint16_t alignmentBytes);

  /// Causes 'failure' to occur in memory allocation calls. This is a test-only
  /// function for validating error paths which are rare to trigger in unit
  /// test. If 'persistent' is false, then we only inject failure once in the
  /// next call. Otherwise, we keep injecting failures until next
  /// 'testingClearFailureInjection' call.
  enum class InjectedFailure {
    kNone,
    /// Mimic case of not finding anything to advise away.
    ///
    /// NOTE: this only applies for MmapAllocator.
    kMadvise,
    /// Mimic running out of mmaps for process.
    ///
    /// NOTE: this only applies for MmapAllocator.
    kMmap,
    /// Mimic the actual memory allocation failure.
    kAllocate,
    /// Mimic the case that exceeds the memory allocator's internal cap limit.
    ///
    /// NOTE: this only applies for MmapAllocator.
    kCap
  };
  void testingSetFailureInjection(
      InjectedFailure failure,
      bool persistent = false) {
    injectedFailure_ = failure;
    isPersistentFailureInjection_ = persistent;
  }

  void testingClearFailureInjection() {
    injectedFailure_ = InjectedFailure::kNone;
    isPersistentFailureInjection_ = false;
  }

  /// Sets a thread level failure message describing the reason for the last
  /// allocation failure.
  void setAllocatorFailureMessage(std::string message);

  /// Returns extra information after returning false from any of the allocate
  /// functions. The error message is scoped to the most recent call on the
  /// thread. The message is cleared after return.
  std::string getAndClearFailureMessage();

  void getTracingHooks(
      std::function<void()>& init,
      std::function<std::string()>& report,
      std::function<int64_t()> ioVolume = nullptr);

 protected:
  MemoryAllocator(MachinePageCount largestSizeClassPages = 256)
      : sizeClassSizes_(makeSizeClassSizes(largestSizeClassPages)) {}

  static std::vector<MachinePageCount> makeSizeClassSizes(
      MachinePageCount largest);

  /// Represents a mix of blocks of different sizes for covering a single
  /// allocation.
  struct SizeMix {
    // Index into 'sizeClassSizes_'
    std::vector<int32_t> sizeIndices;
    // Number of items of the class of the corresponding element in
    // '"sizeIndices'.
    std::vector<int32_t> sizeCounts;
    // Number of valid elements in 'sizeCounts' and 'sizeIndices'.
    int32_t numSizes{0};
    // Total number of pages.
    int32_t totalPages{0};

    SizeMix() {
      sizeIndices.reserve(kMaxSizeClasses);
      sizeCounts.reserve(kMaxSizeClasses);
    }
  };

  /// The actual memory allocation function implementation without retry
  /// attempts by making space from cache.
  virtual bool allocateContiguousWithoutRetry(
      MachinePageCount numPages,
      Allocation* collateral,
      ContiguousAllocation& allocation,
      MachinePageCount maxPages = 0) = 0;

  virtual bool allocateNonContiguousWithoutRetry(
      const SizeMix& sizeMix,
      Allocation& out) = 0;

  virtual void* allocateBytesWithoutRetry(
      uint64_t bytes,
      uint16_t alignment) = 0;

  virtual void* allocateZeroFilledWithoutRetry(uint64_t bytes);

  virtual bool growContiguousWithoutRetry(
      MachinePageCount increment,
      ContiguousAllocation& allocation) = 0;

  // 'Cache' getter. The cache is only responsible for freeing up memory space
  // by shrinking itself when there is not enough space upon allocating. The
  // free of space is not guaranteed.
  virtual Cache* cache() const = 0;

  // Returns the size class size that corresponds to 'bytes'.
  static MachinePageCount roundUpToSizeClassSize(
      size_t bytes,
      const std::vector<MachinePageCount>& sizes);

  // Returns a mix of standard sizes and allocation counts for covering
  // 'numPages' worth of memory. 'minSizeClass' is the size of the
  // smallest usable size class.
  SizeMix allocationSize(
      MachinePageCount numPages,
      MachinePageCount minSizeClass) const;

  FOLLY_ALWAYS_INLINE bool testingHasInjectedFailure(InjectedFailure failure) {
    if (FOLLY_LIKELY(injectedFailure_ != failure)) {
      return false;
    }
    if (!isPersistentFailureInjection_) {
      injectedFailure_ = InjectedFailure::kNone;
    }
    return true;
  }

  // If 'data' is sufficiently large, enables/disables adaptive  huge pages
  // for the address range.
  void useHugePages(const ContiguousAllocation& data, bool enable);

  // The machine page counts corresponding to different sizes in order
  // of increasing size.
  const std::vector<MachinePageCount>
      sizeClassSizes_{1, 2, 4, 8, 16, 32, 64, 128, 256};

  // Tracks the number of allocated pages. Allocated pages are the memory
  // pages that are currently being used.
  std::atomic<MachinePageCount> numAllocated_{0};

  // Tracks the number of mapped pages. Mapped pages are the memory pages that
  // meet following requirements:
  // 1. They are obtained from the operating system from mmap calls directly,
  // without going through std::malloc.
  // 2. They are currently being allocated (used) or they were allocated
  // (used) and freed in the past but haven't been returned to the operating
  // system by 'this' (via madvise calls).
  std::atomic<MachinePageCount> numMapped_{0};

  // Indicates if the failure injection is persistent or transient.
  //
  // NOTE: this is only used for testing purpose.
  InjectedFailure injectedFailure_{InjectedFailure::kNone};
  bool isPersistentFailureInjection_{false};

  Stats stats_;
};

std::ostream& operator<<(std::ostream& out, const MemoryAllocator::Kind& kind);
} // namespace facebook::velox::memory
template <>
struct fmt::formatter<facebook::velox::memory::MemoryAllocator::InjectedFailure>
    : fmt::formatter<int> {
  auto format(
      facebook::velox::memory::MemoryAllocator::InjectedFailure s,
      format_context& ctx) {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
