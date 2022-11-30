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

  SizeClassStats() {}
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

  // Returns the total clocks for this size class.
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
    if (!size) {
      return 0;
    }
    int64_t power = bits::nextPowerOfTwo(size / kPageSize);
    return std::min(kNumSizes - 1, 63 - bits::countLeadingZeros(power));
  }

  /// Counters for each size class.
  std::array<SizeClassStats, kNumSizes> sizes;

  /// Cumulative count of pages advised away, if the allocator exposes this.
  int64_t numAdvise{0};
};

class ScopedMemoryAllocator;

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
class MemoryAllocator : std::enable_shared_from_this<MemoryAllocator> {
 public:
  /// Returns the process-wide default instance or an application-supplied
  /// custom instance set via setDefaultInstance().
  static MemoryAllocator* FOLLY_NONNULL getInstance();

  /// Overrides the process-wide default instance. The caller keeps ownership
  /// and must not destroy the instance until it is empty. Calling this with
  /// nullptr restores the initial process-wide default instance.
  static void setDefaultInstance(MemoryAllocator* FOLLY_NULLABLE instance);

  /// Creates a default MemoryAllocator instance but does not set this to
  /// process default.
  static std::shared_ptr<MemoryAllocator> createDefaultInstance();

  static void testingDestroyInstance();

  MemoryAllocator() = default;
  virtual ~MemoryAllocator() = default;

  static constexpr uint64_t kPageSize = 4096;
  static constexpr int32_t kMaxSizeClasses = 12;
  static constexpr int32_t kNoOwner = -1;
  /// Marks allocation via allocateBytes, e.g. StlMemoryAllocator.
  static constexpr int32_t kMallocOwner = -14;
  /// Allocations smaller than 3K should go to malloc.
  static constexpr int32_t kMaxMallocBytes = 3072;
  static constexpr uint16_t kMaxAlignment = 64;

  /// Represents a number of consecutive pages of kPageSize bytes.
  class PageRun {
   public:
    static constexpr uint8_t kPointerSignificantBits = 48;
    static constexpr uint64_t kPointerMask = 0xffffffffffff;
    static constexpr uint32_t kMaxPagesInRun =
        (1UL << (64U - kPointerSignificantBits)) - 1;

    PageRun(void* FOLLY_NONNULL address, MachinePageCount numPages) {
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
    T* FOLLY_NONNULL data() const {
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
    explicit Allocation(MemoryAllocator* FOLLY_NONNULL allocator)
        : allocator_(allocator) {
      VELOX_CHECK_NOT_NULL(allocator_);
    }

    ~Allocation() {
      allocator_->freeNonContiguous(*this);
    }

    Allocation(const Allocation& other) = delete;

    Allocation(Allocation&& other) noexcept {
      allocator_ = other.allocator_;
      runs_ = std::move(other.runs_);
      numPages_ = other.numPages_;
      other.numPages_ = 0;
    }

    void operator=(const Allocation& other) = delete;

    void operator=(Allocation&& other) {
      allocator_ = other.allocator_;
      runs_ = std::move(other.runs_);
      numPages_ = other.numPages_;
      other.numPages_ = 0;
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

    void append(uint8_t* FOLLY_NONNULL address, int32_t numPages);

    void clear() {
      runs_.clear();
      numPages_ = 0;
    }

    /// Returns the run number in 'runs_' and the position within the run
    /// corresponding to 'offset' from the start of 'this'.
    void findRun(
        uint64_t offset,
        int32_t* FOLLY_NONNULL index,
        int32_t* FOLLY_NONNULL offsetInRun) const;

   private:
    MemoryAllocator* FOLLY_NONNULL allocator_;
    std::vector<PageRun> runs_;
    int32_t numPages_ = 0;
  };

  /// Represents a run of contiguous pages that do not belong to any size class.
  class ContiguousAllocation {
   public:
    ContiguousAllocation() = default;
    ~ContiguousAllocation() {
      if (data_ && allocator_) {
        allocator_->freeContiguous(*this);
      }
      data_ = nullptr;
    }

    ContiguousAllocation(ContiguousAllocation&& other) noexcept {
      allocator_ = other.allocator_;
      data_ = other.data_;
      size_ = other.size_;
      other.data_ = nullptr;
      other.size_ = 0;
    }

    MachinePageCount numPages() const;

    template <typename T = uint8_t>
    T* FOLLY_NULLABLE data() const {
      return reinterpret_cast<T*>(data_);
    }

    /// size in bytes.
    uint64_t size() const {
      return size_;
    }

    void reset(
        MemoryAllocator* FOLLY_NULLABLE allocator,
        void* FOLLY_NULLABLE data,
        uint64_t size) {
      allocator_ = allocator;
      data_ = data;
      size_ = size;
    }

   private:
    MemoryAllocator* FOLLY_NULLABLE allocator_{nullptr};
    void* FOLLY_NULLABLE data_{nullptr};
    uint64_t size_{0};
  };

  /// Stats on memory allocated by allocateBytes().
  struct AllocateBytesStats {
    /// Total size of small allocations.
    uint64_t totalSmall;
    /// Total size of allocations from some size class.
    uint64_t totalInSizeClasses;
    /// Total in standalone large allocations via allocateContiguous().
    uint64_t totalLarge;

    AllocateBytesStats operator-(const AllocateBytesStats& other) const {
      auto result = *this;
      result.totalSmall -= other.totalSmall;
      result.totalInSizeClasses -= other.totalInSizeClasses;
      result.totalLarge -= other.totalLarge;
      return result;
    }
  };

  /// Allocates 'bytes' contiguous bytes and returns the pointer to the first
  /// byte. If 'bytes' is less than 'maxMallocSize', delegates the allocation to
  /// malloc. If the size is above that and below the largest size classes'
  /// size, allocates one element of the next size classes' size. If 'size' is
  /// greater than the largest size classes' size, calls allocateContiguous().
  /// Returns nullptr if there is no space. The amount to allocate is subject to
  /// the size limit of 'this'. This function is not virtual but calls the
  /// virtual functions allocateNonContiguous and allocateContiguous, which can
  /// track sizes and enforce caps etc.
  virtual void* FOLLY_NULLABLE allocateBytes(
      uint64_t bytes,
      uint16_t alignment = 0,
      uint64_t maxMallocSize = kMaxMallocBytes);

  /// Allocates a zero-filled contiguous bytes with capacity that can store
  /// 'numMembers' entries with each size of 'sizeEach'.
  virtual void* FOLLY_NULLABLE
  allocateZeroFilled(int64_t numMembers, int64_t sizeEach);

  /// Allocates 'newSize' contiguous bytes. If 'p' is not null, this function
  /// copies std::min(size, newSize) bytes from 'p' to the newly allocated
  /// buffer and free 'p' after that.
  virtual void* FOLLY_NULLABLE reallocateBytes(
      void* FOLLY_NULLABLE p,
      int64_t size,
      int64_t newSize,
      uint16_t alignment = 0);

  /// Frees contiguous memory allocated by allocateBytes, allocateZeroFilled,
  /// reallocateBytes.
  virtual void freeBytes(
      void* FOLLY_NONNULL p,
      uint64_t size,
      uint64_t maxMallocSize = kMaxMallocBytes) noexcept;

  /// Allocates one or more runs that add up to at least 'numPages', with the
  /// smallest run being at least 'minSizeClass' pages. 'minSizeClass' must be
  /// <= the size of the largest size class. The new memory is returned in 'out'
  /// and any memory formerly referenced by 'out' is freed. 'userAllocCB' is
  /// called with the actual allocation bytes and a flag indicating if it is
  /// called for pre-allocation or post-allocation failure. The flag is true for
  /// pre-allocation call and false for post-allocation failure call. The latter
  /// is to let user have a chance to rollback if needed. For instance,
  /// 'ScopedMemoryAllocator' object will make memory counting reservation in
  /// 'userAllocCB' before the actual memory allocation so it needs to release
  /// the reservation if the actual allocation fails. The function returns true
  /// if the allocation succeeded. If returning false, 'out' references no
  /// memory and any partially allocated memory is freed.
  virtual bool allocateNonContiguous(
      MachinePageCount numPages,
      int32_t owner,
      Allocation& out,
      std::function<void(int64_t, bool)> userAllocCB = nullptr,
      MachinePageCount minSizeClass = 0) = 0;

  /// Returns non-contiguous 'allocation'.
  virtual int64_t freeNonContiguous(Allocation& allocation) = 0;

  /// Makes a contiguous mmap of 'numPages'. Advises away the required number of
  /// free pages so as not to have resident size exceed the capacity if capacity
  /// is bounded. Returns false if sufficient free pages do not exist.
  /// 'collateral' and 'allocation' are freed and unmapped or advised away to
  /// provide pages to back the new 'allocation'. This will always succeed if
  /// collateral and allocation together cover the new size of allocation.
  /// 'allocation' is newly mapped and hence zeroed. The contents of
  /// 'allocation' and 'collateral' are freed in all cases, also if the
  /// allocation fails. 'userAllocCB' is used in the same way as allocate does.
  /// It may throw and the end state will be consistent, with no new allocation
  /// and 'allocation' and 'collateral' cleared.
  virtual bool allocateContiguous(
      MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      std::function<void(int64_t, bool)> userAllocCB = nullptr) = 0;

  /// Returns contiguous 'allocation'.
  virtual void freeContiguous(ContiguousAllocation& allocation) = 0;

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

  /// Returns static counters for allocateBytes usage.
  static AllocateBytesStats allocateBytesStats() {
    return {
        totalSmallAllocateBytes_,
        totalSizeClassAllocateBytes_,
        totalLargeAllocateBytes_};
  }

  /// Clears counters to revert effect of previous tests.
  static void testingClearAllocateBytesStats() {
    totalSmallAllocateBytes_ = 0;
    totalSizeClassAllocateBytes_ = 0;
    totalLargeAllocateBytes_ = 0;
  }

  virtual Stats stats() const {
    return Stats();
  }

  /// TODO: deprecate the following ScopedMemoryAllocator related APIs after
  /// provides both contiguous and non-contiguous memory allocations through
  /// memory pool interface.
  virtual std::shared_ptr<MemoryAllocator> addChild(
      std::shared_ptr<MemoryUsageTracker> tracker);

  virtual MemoryUsageTracker* FOLLY_NULLABLE tracker() const {
    return nullptr;
  }

  virtual std::string toString() const;

 protected:
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
  inline static std::mutex initMutex_;
  // Singleton instance.
  inline static std::shared_ptr<MemoryAllocator> instance_;
  // Application-supplied custom implementation of MemoryAllocator to be
  // returned by getInstance().
  inline static MemoryAllocator* FOLLY_NULLABLE customInstance_;

  // Static counters for STL and memoryPool users of
  // MemoryAllocator. Updated by allocateBytes() and freeBytes(). These
  // are intended to be exported via StatsReporter. These are
  // respectively backed by malloc, allocate from a single size class
  // and standalone mmap.
  inline static std::atomic<uint64_t> totalSmallAllocateBytes_;
  inline static std::atomic<uint64_t> totalSizeClassAllocateBytes_;
  inline static std::atomic<uint64_t> totalLargeAllocateBytes_;
};

/// Wrapper around MemoryAllocator for scoped tracking of activity. We
/// expect a single level of wrappers around the process root
/// MemoryAllocator. Each will have its own MemoryUsageTracker that will
/// be a child of the Driver/Task level tracker. in this way
/// MemoryAllocator activity can be attributed to individual operators and
/// these operators can be requested to spill or limit their memory utilization.
///
/// TODO: deprecate this class after we provide both contiguous and
/// non-contiguous memory allocations through memory pool interface.
class ScopedMemoryAllocator final : public MemoryAllocator {
 public:
  ScopedMemoryAllocator(
      MemoryAllocator* FOLLY_NONNULL parent,
      std::shared_ptr<MemoryUsageTracker> tracker)
      : parent_(parent), tracker_(std::move(tracker)) {}

  ScopedMemoryAllocator(
      std::shared_ptr<ScopedMemoryAllocator> parent,
      std::shared_ptr<MemoryUsageTracker> tracker)
      : parentPtr_(std::move(parent)),
        parent_(parentPtr_.get()),
        tracker_(std::move(tracker)) {}

  bool allocateNonContiguous(
      MachinePageCount numPages,
      int32_t owner,
      Allocation& out,
      std::function<void(int64_t, bool)> userAllocCB,
      MachinePageCount minSizeClass) override;

  int64_t freeNonContiguous(Allocation& allocation) override {
    int64_t freed = parent_->freeNonContiguous(allocation);
    if (tracker_) {
      tracker_->update(-freed);
    }
    return freed;
  }

  bool allocateContiguous(
      MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      std::function<void(int64_t, bool)> userAllocCB = nullptr) override;

  void freeContiguous(ContiguousAllocation& allocation) override {
    int64_t size = allocation.size();
    parent_->freeContiguous(allocation);
    if (tracker_) {
      tracker_->update(-size);
    }
  }

  bool checkConsistency() const override {
    return parent_->checkConsistency();
  }

  const std::vector<MachinePageCount>& sizeClasses() const override {
    return parent_->sizeClasses();
  }

  MachinePageCount numAllocated() const override {
    return parent_->numAllocated();
  }

  MachinePageCount numMapped() const override {
    return parent_->numMapped();
  }

  std::shared_ptr<MemoryAllocator> addChild(
      std::shared_ptr<MemoryUsageTracker> tracker) override {
    return std::make_shared<ScopedMemoryAllocator>(this, tracker);
  }

  MemoryUsageTracker* FOLLY_NULLABLE tracker() const override {
    return tracker_.get();
  }

  Stats stats() const override {
    return parent_->stats();
  }

 private:
  std::shared_ptr<MemoryAllocator> parentPtr_;
  MemoryAllocator* FOLLY_NONNULL parent_;
  std::shared_ptr<MemoryUsageTracker> tracker_;
};

/// An Allocator backed by MemoryAllocator for STL containers.
template <class T>
struct StlMemoryAllocator {
  using value_type = T;

  explicit StlMemoryAllocator(MemoryAllocator* FOLLY_NONNULL allocator)
      : allocator_{allocator} {
    VELOX_CHECK_NOT_NULL(allocator_);
  }

  template <class U>
  explicit StlMemoryAllocator(const StlMemoryAllocator<U>& allocator)
      : allocator_{allocator.allocator()} {
    VELOX_CHECK_NOT_NULL(allocator_);
  }

  T* FOLLY_NONNULL allocate(std::size_t n) {
    return reinterpret_cast<T*>(
        allocator_->allocateBytes(checkedMultiply(n, sizeof(T))));
  }

  void deallocate(T* FOLLY_NONNULL p, std::size_t n) {
    allocator_->freeBytes(p, checkedMultiply(n, sizeof(T)));
  }

  MemoryAllocator* FOLLY_NONNULL allocator() const {
    return allocator_;
  }

  friend bool operator==(
      const StlMemoryAllocator& lhs,
      const StlMemoryAllocator& rhs) {
    return lhs.allocator_ == rhs.allocator_;
  }
  friend bool operator!=(
      const StlMemoryAllocator& lhs,
      const StlMemoryAllocator& rhs) {
    return !(lhs == rhs);
  }

 private:
  MemoryAllocator* FOLLY_NONNULL const allocator_;
};

} // namespace facebook::velox::memory
