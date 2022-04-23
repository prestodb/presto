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
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryUsageTracker.h"

DECLARE_bool(velox_use_malloc);
DECLARE_int32(velox_memory_pool_mb);

namespace facebook::velox::memory {

class ScopedMappedMemory;

// Denotes a number of machine pages as in mmap and related functions.
using MachinePageCount = uint64_t;

// Base class for allocating runs of machine pages from predefined
// size classes. An allocation that does not match a size class is
// composed of multiple runs from different size classes. To get 11
// pages, one could have a run of 8, one of 2 and one of 1
// page. This is intended for all high volume allocations, like
// caches, IO buffers and hash tables for join/group
// by. Implementations may use malloc or mmap/madvise. Caches
// subclass this to provide allocation that is fungible with cached
// capacity, i.e. a cache can evict data to make space for non-cache
// memory users. The point is to have all large allocation come from
// a single source to have dynamic balancing between different
// users. Proxy subclasses may provide context specific tracking
// while delegating the allocation to a root allocator.
class MappedMemory {
 public:
  static constexpr uint64_t kPageSize = 4096;
  static constexpr int32_t kMaxSizeClasses = 12;
  static constexpr int32_t kNoOwner = -1;
  // Marks allocation via allocateBytes, e.g. StlMappedMemoryAllocator.
  static constexpr int32_t kMallocOwner = -14;
  // Allocations smaller than 3K should  go to malloc.
  static constexpr int32_t kMaxMallocBytes = 3072;

  // Represents a number of consecutive pages of kPageSize bytes.
  class PageRun {
   public:
    static constexpr uint8_t kPointerSignificantBits = 48;
    static constexpr uint64_t kPointerMask = 0xffffffffffff;
    static constexpr uint32_t kMaxPagesInRun =
        (1UL << (64U - kPointerSignificantBits)) - 1;

    PageRun(void* FOLLY_NONNULL address, MachinePageCount numPages) {
      auto word = reinterpret_cast<uint64_t>(address); // NOLINT
      if (!FLAGS_velox_use_malloc) {
        VELOX_CHECK(
            (word & (kPageSize - 1)) == 0,
            "Address is not page-aligned for PageRun");
      }
      VELOX_CHECK(numPages <= kMaxPagesInRun);
      VELOX_CHECK(
          (word & ~kPointerMask) == 0,
          "A pointer must have its 16 high bits 0");
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

  // Represents a set of PageRuns that are allocated together.
  class Allocation {
   public:
    explicit Allocation(MappedMemory* FOLLY_NONNULL mappedMemory)
        : mappedMemory_(mappedMemory) {
      VELOX_CHECK(mappedMemory);
    }

    ~Allocation() {
      mappedMemory_->free(*this);
    }

    Allocation(const Allocation& other) = delete;

    Allocation(Allocation&& other) noexcept {
      mappedMemory_ = other.mappedMemory_;
      runs_ = std::move(other.runs_);
      numPages_ = other.numPages_;
      other.numPages_ = 0;
    }

    void operator=(const Allocation& other) = delete;

    void operator=(Allocation&& other) {
      mappedMemory_ = other.mappedMemory_;
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

    // Returns the run number and the position within the run
    // corresponding to 'offset' from the start of 'this'.
    void findRun(
        uint64_t offset,
        int32_t* FOLLY_NONNULL index,
        int32_t* FOLLY_NONNULL offsetInRun);

   private:
    MappedMemory* FOLLY_NONNULL mappedMemory_;
    std::vector<PageRun> runs_;
    int32_t numPages_ = 0;
  };

  // Represents a mmap'd run of contiguous pages that do not belong to
  // any size class but are still accounted by the owning
  // MappedMemory.
  class ContiguousAllocation {
   public:
    ContiguousAllocation() = default;
    ~ContiguousAllocation() {
      if (data_ && mappedMemory_) {
        mappedMemory_->freeContiguous(*this);
      }
      data_ = nullptr;
    }

    MappedMemory* FOLLY_NULLABLE mappedMemory() const {
      return mappedMemory_;
    }

    MachinePageCount numPages() const;

    template <typename T = uint8_t>
    T* FOLLY_NULLABLE data() const {
      return reinterpret_cast<T*>(data_);
    }

    // size in bytes.
    uint64_t size() const {
      return size_;
    }

    void reset(
        MappedMemory* FOLLY_NULLABLE mappedMemory,
        void* FOLLY_NULLABLE data,
        uint64_t size) {
      mappedMemory_ = mappedMemory;
      data_ = data;
      size_ = size;
    }

   private:
    MappedMemory* FOLLY_NULLABLE mappedMemory_{nullptr};
    void* FOLLY_NULLABLE data_{nullptr};
    uint64_t size_{0};
  };

  // Stats on memory allocated by allocateBytes().
  struct AllocateBytesCounters {
    // Total size of small allocations.
    uint64_t totalSmall;
    // Total size of allocations from some size class.
    uint64_t totalInSizeClasses;
    // Total in standalone large allocations via allocateContiguous().
    uint64_t totalLarge;
  };

  MappedMemory() {}

  virtual ~MappedMemory() {}

  // Returns the process-wide default instance or an application-supplied custom
  // instance set via setDefaultInstance().
  static MappedMemory* FOLLY_NONNULL getInstance();

  // Creates a default MappedMemory instance but does not set this to process
  // default.
  static std::unique_ptr<MappedMemory> createDefaultInstance();

  // Overrides the process-wide default instance. The caller keeps
  // ownership and must not destroy the instance until it is
  // empty. Calling this with nullptr restores the initial
  // process-wide default instance.
  static void setDefaultInstance(MappedMemory* FOLLY_NULLABLE instance);

  /// Allocates one or more runs that add up to at least 'numPages',
  /// with the smallest run being at least 'minSizeClass'
  /// pages. 'minSizeClass' must be <= the size of the largest size
  /// class. The new memory is returned in 'out' and any memory
  /// formerly referenced by 'out' is freed. 'beforeAllocCb' is called
  /// before making the allocation. Returns true if the allocation
  /// succeeded. If returning false, 'out' references no memory and
  /// any partially allocated memory is freed.
  virtual bool allocate(
      MachinePageCount numPages,
      int32_t owner,
      Allocation& out,
      std::function<void(int64_t)> beforeAllocCB = nullptr,
      MachinePageCount minSizeClass = 0) = 0;

  // Returns the number of freed bytes.
  virtual int64_t free(Allocation& allocation) = 0;

  // Makes a contiguous mmap of 'numPages'. Advises away the required
  // number of free pages so as not to have resident size exceed the
  // capacity if capacity is bounded. Returns false if sufficient free
  // pages do not exist. 'collateral' and 'allocation' are freed and
  // unmapped or advised away to provide pages to back the new
  // 'allocation'. This will always succeed if collateral and
  // allocation together cover the new size of
  // allocation. 'allocation' is newly mapped and hence zeroed. The
  // contents of 'allocation' and 'collateral' are freed in all cases,
  // also if the allocation fails. 'beforeAllocCB can be used to
  // update trackers. It may throw and the end state will be
  // consistent, with no new allocation and 'allocation' and
  // 'collateral' cleared.
  virtual bool allocateContiguous(
      MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      std::function<void(int64_t)> beforeAllocCB = nullptr) = 0;

  virtual void freeContiguous(ContiguousAllocation& allocation) = 0;

  // Allocates 'size'contiguous bytes and returns the pointer to the
  // first byte. If 'size' is less than 'maxMallocSize', delegates the
  // allocation to malloc. If the size is above that and below the
  // largest size class, allocates one element of the next size
  // class. If 'size' is greater than the largest size class, calls
  // allocateContiguous(). Returns nullptr if there is no space. The
  // amount to allocate is subject to the size limit of 'this'. This
  // function is not virtual but calls the virtual functions allocate
  // and allocateContiguous, which can track sizes and enforce caps etc.
  void* FOLLY_NULLABLE
  allocateBytes(uint64_t bytes, uint64_t maxMallocSize = kMaxMallocBytes);

  // Frees memory allocated with allocateBytes().
  void freeBytes(
      void* FOLLY_NONNULL p,
      uint64_t size,
      uint64_t maxMallocSize = kMaxMallocBytes) noexcept;

  // Checks internal consistency of allocation data
  // structures. Returns true if OK.
  virtual bool checkConsistency() const = 0;

  static void destroyTestOnly();

  virtual const std::vector<MachinePageCount>& sizeClasses() const {
    return sizeClassSizes_;
  }

  virtual MachinePageCount numAllocated() const = 0;

  virtual MachinePageCount numMapped() const = 0;

  virtual std::shared_ptr<MappedMemory> addChild(
      std::shared_ptr<MemoryUsageTracker> tracker);

  virtual MemoryUsageTracker* FOLLY_NULLABLE tracker() const {
    return nullptr;
  }

  // Returns static counters for allocateBytes usage.

  static AllocateBytesCounters allocateBytesStats() {
    return {
        totalSmallAllocateBytes_,
        totalSizeClassAllocateBytes_,
        totalLargeAllocateBytes_};
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

  // Returns a mix of standard sizes and allocation counts for
  // covering 'numPages' worth of memory. 'minSizeClass' is the size
  // of the smallest usable size class.
  SizeMix allocationSize(
      MachinePageCount numPages,
      MachinePageCount minSizeClass) const;

  // The machine page counts corresponding to different sizes in order
  // of increasing size.
  const std::vector<MachinePageCount>
      sizeClassSizes_{1, 2, 4, 8, 16, 32, 64, 128, 256};

 private:
  // Singleton instance.
  static std::unique_ptr<MappedMemory> instance_;
  // Application-supplied custom implementation of MappedMemory to be returned
  // by getInstance().
  static MappedMemory* FOLLY_NULLABLE customInstance_;
  static std::mutex initMutex_;
  // Static counters for STL and memoryPool users of
  // MappedMemory. Updated by allocateBytes() and freeBytes(). These
  // are intended to be exported via StatsReporter. These are
  // respectively backed by malloc, allocate from a single size class
  // and standalone mmap.
  static std::atomic<uint64_t> totalSmallAllocateBytes_;
  static std::atomic<uint64_t> totalSizeClassAllocateBytes_;
  static std::atomic<uint64_t> totalLargeAllocateBytes_;
};

// Wrapper around MappedMemory for scoped tracking of activity. We
// expect a single level of wrappers around the process root
// MappedMemory. Each will have its own MemoryUsageTracker that will
// be a child of the Driver/Task level tracker. in this way
// MappedMemory activity can be attributed to individual operators and
// these operators can be requested to spill or limit their memory utilization.
class ScopedMappedMemory final : public MappedMemory {
 public:
  ScopedMappedMemory(
      MappedMemory* FOLLY_NONNULL parent,
      std::shared_ptr<MemoryUsageTracker> tracker)
      : parent_(parent), tracker_(std::move(tracker)) {}

  ScopedMappedMemory(
      std::shared_ptr<ScopedMappedMemory> parent,
      std::shared_ptr<MemoryUsageTracker> tracker)
      : parentPtr_(std::move(parent)),
        parent_(parentPtr_.get()),
        tracker_(std::move(tracker)) {}

  bool allocate(
      MachinePageCount numPages,
      int32_t owner,
      Allocation& out,
      std::function<void(int64_t)> beforeAllocCB,
      MachinePageCount minSizeClass) override;

  int64_t free(Allocation& allocation) override {
    int64_t freed = parent_->free(allocation);
    if (tracker_) {
      tracker_->update(-freed);
    }
    return freed;
  }

  bool allocateContiguous(
      MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      std::function<void(int64_t)> beforeAllocCB = nullptr) override;

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

  std::shared_ptr<MappedMemory> addChild(
      std::shared_ptr<MemoryUsageTracker> tracker) override {
    return std::make_shared<ScopedMappedMemory>(this, tracker);
  }

  MemoryUsageTracker* FOLLY_NULLABLE tracker() const override {
    return tracker_.get();
  }

 private:
  std::shared_ptr<MappedMemory> parentPtr_;
  MappedMemory* FOLLY_NONNULL parent_;
  std::shared_ptr<MemoryUsageTracker> tracker_;
};

// An Allocator backed by MappedMemory for for STL containers.
template <class T>
struct StlMappedMemoryAllocator {
  using value_type = T;

  explicit StlMappedMemoryAllocator(MappedMemory* FOLLY_NONNULL allocator)
      : allocator_{allocator} {
    VELOX_CHECK(allocator);
  }

  template <class U>
  explicit StlMappedMemoryAllocator(
      const StlMappedMemoryAllocator<U>& allocator)
      : allocator_{allocator.allocator()} {
    VELOX_CHECK(allocator_);
  }

  T* FOLLY_NONNULL allocate(std::size_t n) {
    return reinterpret_cast<T*>(allocator_->allocateBytes(n * sizeof(T)));
  }

  void deallocate(T* FOLLY_NONNULL p, std::size_t n) noexcept {
    allocator_->freeBytes(p, n * sizeof(T));
  }

  MappedMemory* FOLLY_NONNULL allocator() const {
    return allocator_;
  }

  friend bool operator==(
      const StlMappedMemoryAllocator& lhs,
      const StlMappedMemoryAllocator& rhs) {
    return lhs.allocator_ == rhs.allocator_;
  }
  friend bool operator!=(
      const StlMappedMemoryAllocator& lhs,
      const StlMappedMemoryAllocator& rhs) {
    return !(lhs == rhs);
  }

 private:
  MappedMemory* FOLLY_NONNULL allocator_;
};

} // namespace facebook::velox::memory
