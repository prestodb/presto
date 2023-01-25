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

#include <gflags/gflags.h>
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryUsageTracker.h"
#include "velox/common/time/Timer.h"

DECLARE_bool(velox_use_malloc);
DECLARE_int32(velox_memory_pool_mb);
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
    constexpr int32_t kPageSize = 4096;
    if (size == 0) {
      return 0;
    }
    const auto power = bits::nextPowerOfTwo(size / kPageSize);
    return std::min(kNumSizes - 1, 63 - bits::countLeadingZeros(power));
  }

  /// Counters for each size class.
  std::array<SizeClassStats, kNumSizes> sizes;

  /// Cumulative count of pages advised away, if the allocator exposes this.
  int64_t numAdvise{0};
};

class MemoryPool;

/// Denotes a number of machine pages as in mmap and related functions.
using MachinePageCount = uint64_t;

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
    /// MemoryAllocatorImpl. It delegates the memory allocations to std::malloc.
    kMalloc,
    /// The memory allocator kind which is implemented by MmapAllocator. It
    /// manages the large chunk of memory allocations on its own by leveraging
    /// mmap and madvice, to optimize the memory fragmentation in the long
    /// running service such as Prestissimo.
    kMmap,
  };

  static std::string kindString(Kind kind);

  /// Returns the process-wide default instance or an application-supplied
  /// custom instance set via setDefaultInstance().
  static MemoryAllocator* getInstance();

  /// Overrides the process-wide default instance. The caller keeps ownership
  /// and must not destroy the instance until it is empty. Calling this with
  /// nullptr restores the initial process-wide default instance.
  static void setDefaultInstance(MemoryAllocator* instance);

  /// Creates a default MemoryAllocator instance but does not set this to
  /// process default.
  static std::shared_ptr<MemoryAllocator> createDefaultInstance();

  static void testingDestroyInstance();

  virtual ~MemoryAllocator() = default;

  static constexpr uint64_t kPageSize = 4096;
  static constexpr int32_t kMaxSizeClasses = 12;
  /// Allocations smaller than 3K should go to malloc.
  static constexpr int32_t kMaxMallocBytes = 3072;
  static constexpr uint16_t kMinAlignment = alignof(max_align_t);
  static constexpr uint16_t kMaxAlignment = 64;

  /// Represents a number of consecutive pages of kPageSize bytes.
  class PageRun {
   public:
    static constexpr uint8_t kPointerSignificantBits = 48;
    static constexpr uint64_t kPointerMask = 0xffffffffffff;
    static constexpr uint32_t kMaxPagesInRun =
        (1UL << (64U - kPointerSignificantBits)) - 1;

    PageRun(void* address, MachinePageCount numPages) {
      auto word = reinterpret_cast<uint64_t>(address); // NOLINT
      if (!FLAGS_velox_use_malloc) {
        VELOX_CHECK_EQ(
            word & (kPageSize - 1),
            0,
            "Address is not page-aligned for PageRun");
      }
      VELOX_CHECK_LE(numPages, kMaxPagesInRun);
      VELOX_CHECK_EQ(
          word & ~kPointerMask, 0, "A pointer must have its 16 high bits 0");
      data_ =
          word | (static_cast<uint64_t>(numPages) << kPointerSignificantBits);
    }

    template <typename T = uint8_t>
    T* data() const {
      return reinterpret_cast<T*>(data_ & kPointerMask); // NOLINT
    }

    MachinePageCount numPages() const {
      return data_ >> kPointerSignificantBits;
    }

    uint64_t numBytes() const {
      return numPages() * kPageSize;
    }

   private:
    uint64_t data_;
  };

  /// Represents a set of PageRuns that are allocated together.
  class Allocation {
   public:
    Allocation() = default;
    ~Allocation();

    Allocation(const Allocation& other) = delete;

    Allocation(Allocation&& other) noexcept {
      pool_ = other.pool_;
      runs_ = std::move(other.runs_);
      numPages_ = other.numPages_;
      other.clear();
      sanityCheck();
    }

    void operator=(const Allocation& other) = delete;

    void operator=(Allocation&& other) {
      pool_ = other.pool_;
      runs_ = std::move(other.runs_);
      numPages_ = other.numPages_;
      other.clear();
    }

    MachinePageCount numPages() const {
      return numPages_;
    }

    uint32_t numRuns() const {
      return runs_.size();
    }

    PageRun runAt(int32_t index) const {
      return runs_[index];
    }

    uint64_t byteSize() const {
      return numPages_ * kPageSize;
    }

    void append(uint8_t* address, int32_t numPages);

    /// Invoked by memory pool to set the ownership on allocation success. All
    /// the external non-contiguous memory allocations go through memory pool.
    ///
    /// NOTE: we can't set the memory pool on object constructor as the memory
    /// allocator also uses it for temporal allocation internally.
    void setPool(MemoryPool* pool) {
      VELOX_CHECK_NOT_NULL(pool);
      VELOX_CHECK_NULL(pool_);
      pool_ = pool;
    }

    MemoryPool* pool() const {
      return pool_;
    }

    void clear() {
      runs_.clear();
      numPages_ = 0;
      pool_ = nullptr;
    }

    /// Returns the run number in 'runs_' and the position within the run
    /// corresponding to 'offset' from the start of 'this'.
    void findRun(uint64_t offset, int32_t* index, int32_t* offsetInRun) const;

    /// Returns if this allocation is empty.
    bool empty() const {
      sanityCheck();
      return numPages_ == 0;
    }

    std::string toString() const;

   private:
    FOLLY_ALWAYS_INLINE void sanityCheck() const {
      VELOX_CHECK_EQ(numPages_ == 0, runs_.empty());
      VELOX_CHECK(numPages_ != 0 || pool_ == nullptr);
    }

    MemoryPool* pool_{nullptr};
    std::vector<PageRun> runs_;
    int32_t numPages_ = 0;
  };

  /// Represents a run of contiguous pages that do not belong to any size class.
  class ContiguousAllocation {
   public:
    ContiguousAllocation() = default;
    ~ContiguousAllocation();

    ContiguousAllocation(const ContiguousAllocation& other) = delete;

    ContiguousAllocation& operator=(ContiguousAllocation&& other) {
      pool_ = other.pool_;
      data_ = other.data_;
      size_ = other.size_;
      other.clear();
      sanityCheck();
      return *this;
    }

    ContiguousAllocation(ContiguousAllocation&& other) noexcept {
      pool_ = other.pool_;
      data_ = other.data_;
      size_ = other.size_;
      other.clear();
      sanityCheck();
    }

    MachinePageCount numPages() const;

    template <typename T = uint8_t>
    T* data() const {
      return reinterpret_cast<T*>(data_);
    }

    /// size in bytes.
    uint64_t size() const {
      return size_;
    }

    /// Invoked by memory pool to set the ownership on allocation success. All
    /// the external contiguous memory allocations go through memory pool.
    ///
    /// NOTE: we can't set the memory pool on object constructor as the memory
    /// allocator also uses it for temporal allocation internally.
    void setPool(MemoryPool* pool) {
      VELOX_CHECK_NOT_NULL(pool);
      VELOX_CHECK_NULL(pool_);
      pool_ = pool;
    }

    MemoryPool* pool() const {
      return pool_;
    }

    bool empty() const {
      sanityCheck();
      return size_ == 0;
    }

    void set(void* data, uint64_t size);
    void clear();

    std::string toString() const;

   private:
    FOLLY_ALWAYS_INLINE void sanityCheck() const {
      VELOX_CHECK_EQ(size_ == 0, data_ == nullptr);
      VELOX_CHECK(size_ != 0 || pool_ == nullptr);
    }

    MemoryPool* pool_{nullptr};
    void* data_{nullptr};
    uint64_t size_{0};
  };

  /// Returns the kind of this memory allocator. For AsyncDataCache, it returns
  /// the kind of the delegated memory allocator underneath.
  virtual Kind kind() const = 0;

  using ReservationCallback = std::function<void(int64_t, bool)>;

  /// Allocates one or more runs that add up to at least 'numPages', with the
  /// smallest run being at least 'minSizeClass' pages. 'minSizeClass' must be
  /// <= the size of the largest size class. The new memory is returned in 'out'
  /// and any memory formerly referenced by 'out' is freed. 'reservationCB' is
  /// called with the actual allocation bytes and a flag indicating if it is
  /// called for pre-allocation or post-allocation failure. The flag is true for
  /// pre-allocation call and false for post-allocation failure call. The latter
  /// is to let user have a chance to rollback if needed. For instance,
  /// 'MemoryPoolImpl' object will make memory counting reservation in
  /// 'reservationCB' before the actual memory allocation so it needs to release
  /// the reservation if the actual allocation fails. The function returns true
  /// if the allocation succeeded. If returning false, 'out' references no
  /// memory and any partially allocated memory is freed.
  ///
  /// NOTE: user needs to explicitly release allocation 'out' by calling
  /// 'freeNonContiguous' on the same memory allocator object.
  virtual bool allocateNonContiguous(
      MachinePageCount numPages,
      Allocation& out,
      ReservationCallback reservationCB = nullptr,
      MachinePageCount minSizeClass = 0) = 0;

  /// Frees non-contiguous 'allocation'. 'allocation' is empty on return. The
  /// function returns the actual freed bytes.
  virtual int64_t freeNonContiguous(Allocation& allocation) = 0;

  /// Makes a contiguous mmap of 'numPages'. Advises away the required number of
  /// free pages so as not to have resident size exceed the capacity if capacity
  /// is bounded. Returns false if sufficient free pages do not exist.
  /// 'collateral' and 'allocation' are freed and unmapped or advised away to
  /// provide pages to back the new 'allocation'. This will always succeed if
  /// collateral and allocation together cover the new size of allocation.
  /// 'allocation' is newly mapped and hence zeroed. The contents of
  /// 'allocation' and 'collateral' are freed in all cases, also if the
  /// allocation fails. 'reservationCB' is used in the same way as allocate
  /// does. It may throw and the end state will be consistent, with no new
  /// allocation and 'allocation' and 'collateral' cleared.
  ///
  /// NOTE: user needs to explicitly release allocation 'out' by calling
  /// 'freeContiguous' on the same memory allocator object.
  virtual bool allocateContiguous(
      MachinePageCount numPages,
      Allocation* collateral,
      ContiguousAllocation& allocation,
      ReservationCallback reservationCB = nullptr) = 0;

  /// Frees contiguous 'allocation'. 'allocation' is empty on return.
  virtual void freeContiguous(ContiguousAllocation& allocation) = 0;

  /// Allocates 'bytes' contiguous bytes and returns the pointer to the first
  /// byte. If 'bytes' is less than 'kMaxMallocBytes', delegates the allocation
  /// to malloc. If the size is above that and below the largest size classes'
  /// size, allocates one element of the next size classes' size. If 'size' is
  /// greater than the largest size classes' size, calls allocateContiguous().
  /// Returns nullptr if there is no space. The amount to allocate is subject to
  /// the size limit of 'this'. This function is not virtual but calls the
  /// virtual functions allocateNonContiguous and allocateContiguous, which can
  /// track sizes and enforce caps etc. If 'alignment' is not kMinAlignment,
  /// then 'bytes' must be a multiple of 'alignment'.
  ///
  /// NOTE: 'alignment' must be power of two and in range of [kMinAlignment,
  /// kMaxAlignment].
  virtual void* allocateBytes(
      uint64_t bytes,
      uint16_t alignment = kMinAlignment) = 0;

  /// Allocates a zero-filled contiguous bytes.
  virtual void* allocateZeroFilled(uint64_t bytes);

  /// Allocates 'newSize' contiguous bytes. If 'p' is not null, this function
  /// copies std::min(size, newSize) bytes from 'p' to the newly allocated
  /// buffer and free 'p' after that. If 'alignment' is not kMinAlignment, then
  /// newSize must be a multiple of 'alignment'.
  ///
  /// NOTE: 'alignment' must be power of two and in range of [kMinAlignment,
  /// kMaxAlignment].
  virtual void* reallocateBytes(
      void* p,
      int64_t size,
      int64_t newSize,
      uint16_t alignment = kMinAlignment);

  /// Frees contiguous memory allocated by allocateBytes, allocateZeroFilled,
  /// reallocateBytes.
  virtual void freeBytes(void* p, uint64_t size) noexcept = 0;

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

  virtual MachinePageCount numAllocated() const = 0;

  virtual MachinePageCount numMapped() const = 0;

  virtual Stats stats() const {
    return Stats();
  }

  virtual std::string toString() const;

  /// Invoked to check if 'alignmentBytes' is valid and 'allocateBytes' is
  /// multiple of 'alignmentBytes'.
  static void alignmentCheck(uint64_t allocateBytes, uint16_t alignmentBytes);

 protected:
  MemoryAllocator() = default;

  // Returns the size class size that corresponds to 'bytes'.
  static MachinePageCount roundUpToSizeClassSize(
      size_t bytes,
      const std::vector<MachinePageCount>& sizes);

  // Represents a mix of blocks of different sizes for covering a single
  // allocation.
  struct SizeMix {
    // Index into 'sizeClassSizes_'
    std::array<int32_t, kMaxSizeClasses> sizeIndices{};
    // Number of items of the class of the corresponding element in
    // '"sizeIndices'.
    std::array<int32_t, kMaxSizeClasses> sizeCounts{};
    // Number of valid elements in 'sizeCounts' and 'sizeIndices'.
    int32_t numSizes{0};
    // Total number of pages.
    int32_t totalPages{0};
  };

  /// Returns a mix of standard sizes and allocation counts for covering
  /// 'numPages' worth of memory. 'minSizeClass' is the size of the smallest
  /// usable size class.
  SizeMix allocationSize(
      MachinePageCount numPages,
      MachinePageCount minSizeClass) const;

  // The machine page counts corresponding to different sizes in order
  // of increasing size.
  const std::vector<MachinePageCount>
      sizeClassSizes_{1, 2, 4, 8, 16, 32, 64, 128, 256};

 private:
  static std::mutex initMutex_;
  // Singleton instance.
  static std::shared_ptr<MemoryAllocator> instance_;
  // Application-supplied custom implementation of MemoryAllocator to be
  // returned by getInstance().
  static MemoryAllocator* customInstance_;
};

std::ostream& operator<<(std::ostream& out, const MemoryAllocator::Kind& kind);
} // namespace facebook::velox::memory
