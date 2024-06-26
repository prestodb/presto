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

#include <folly/Range.h>
#include <cstdint>

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/GTestMacros.h"

namespace facebook::velox::memory {

class MemoryPool;

/// Denotes a number of machine pages as in mmap and related functions.
using MachinePageCount = uint64_t;

struct AllocationTraits {
  /// Defines a machine page size in bytes.
  static constexpr uint64_t kPageSize = 4096;

  /// Size of huge page as intended with MADV_HUGEPAGE.
  static constexpr uint64_t kHugePageSize = 2 << 20; // 2MB

  static_assert(kHugePageSize >= kPageSize);
  static_assert(kHugePageSize % kPageSize == 0);

  /// Returns the bytes of the given number pages.
  FOLLY_ALWAYS_INLINE static uint64_t pageBytes(MachinePageCount numPages) {
    return numPages * kPageSize;
  }

  FOLLY_ALWAYS_INLINE static MachinePageCount numPages(uint64_t bytes) {
    return bits::roundUp(bytes, kPageSize) / kPageSize;
  }

  /// Returns the round up page bytes.
  FOLLY_ALWAYS_INLINE static uint64_t roundUpPageBytes(uint64_t bytes) {
    return bits::roundUp(bytes, kPageSize);
  }

  /// The number of pages in a huge page.
  static constexpr MachinePageCount numPagesInHugePage() {
    return kHugePageSize / kPageSize;
  }
};

/// Represents a set of PageRuns that are allocated together.
class Allocation {
 public:
  /// Represents a number of consecutive pages of AllocationTraits::kPageSize
  /// bytes.
  class PageRun {
   public:
    static constexpr uint8_t kPointerSignificantBits = 48;
    static constexpr uint64_t kPointerMask = 0xffffffffffff;
    static constexpr uint32_t kMaxPagesInRun =
        (1UL << (64U - kPointerSignificantBits)) - 1;

    PageRun(void* address, MachinePageCount numPages) {
      auto word = reinterpret_cast<uint64_t>(address); // NOLINT
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
      return numPages() * AllocationTraits::kPageSize;
    }

   private:
    uint64_t data_;
  };

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
    return numPages_ * AllocationTraits::kPageSize;
  }

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

  /// Returns the run number in 'runs_' and the position within the run
  /// corresponding to 'offset' from the start of 'this'.
  void findRun(uint64_t offset, int32_t* index, int32_t* offsetInRun) const;

  /// Returns if this allocation is empty.
  bool empty() const {
    sanityCheck();
    return numPages_ == 0;
  }

  /// Moves the runs in 'from' to 'this'. 'from' is empty on return.
  void appendMove(Allocation& from);

  std::string toString() const;

 private:
  FOLLY_ALWAYS_INLINE void sanityCheck() const {
    VELOX_CHECK_EQ(numPages_ == 0, runs_.empty());
    VELOX_CHECK(numPages_ != 0 || pool_ == nullptr);
  }

  void append(uint8_t* address, MachinePageCount numPages);

  void clear() {
    runs_.clear();
    numPages_ = 0;
    pool_ = nullptr;
  }

  MemoryPool* pool_{nullptr};
  std::vector<PageRun> runs_;
  int32_t numPages_ = 0;

  // NOTE: we only allow memory allocators to change an allocation's internal
  // state.
  friend class MemoryAllocator;
  friend class MmapAllocator;
  friend class MallocAllocator;

  VELOX_FRIEND_TEST(MemoryAllocatorTest, allocationClass1);
  VELOX_FRIEND_TEST(MemoryAllocatorTest, allocationClass2);
  VELOX_FRIEND_TEST(AllocationTest, append);
  VELOX_FRIEND_TEST(AllocationTest, appendMove);
  VELOX_FRIEND_TEST(AllocationTest, maxPageRunLimit);
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
    maxSize_ = other.maxSize_;
    other.clear();
    sanityCheck();
    return *this;
  }

  ContiguousAllocation(ContiguousAllocation&& other) noexcept {
    pool_ = other.pool_;
    data_ = other.data_;
    size_ = other.size_;
    maxSize_ = other.maxSize_;
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

  /// Returns the largest huge page range covered by 'this' or std::nullopt if
  /// no full huge page is fully contained in 'this'.
  std::optional<folly::Range<char*>> hugePageRange() const;

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
    return maxSize_ == 0;
  }

  /// Sets the pointer and sizes. If maxSize is not specified it defaults to
  /// 'size'.
  void set(void* data, uint64_t size, uint64_t maxSize_ = 0);

  // Adjusts 'size' towards 'maxSize' by 'increment' pages. Rounds
  // 'increment' to huge pages, since this is the unit of growth of
  // RSS for large contiguous runs.  Increases the reservation in
  // 'pool_' and its allocator. May fail by cap exceeded. If failing,
  // the size is not changed. 'size_' cannot exceed 'maxSize_'.
  void grow(MachinePageCount increment);

  void clear();

  /// Returns the maximum size
  uint64_t maxSize() const {
    return maxSize_;
  }

  std::string toString() const;

 private:
  FOLLY_ALWAYS_INLINE void sanityCheck() const {
    VELOX_CHECK_EQ(size_ == 0, data_ == nullptr);
    VELOX_CHECK(size_ != 0 || pool_ == nullptr);
  }

  MemoryPool* pool_{nullptr};
  void* data_{nullptr};

  // Offset of first byte in 'data_' not counted reserved in 'pool_'.
  uint64_t size_{0};

  // Offset of first byte after the mmap of 'data'.
  uint64_t maxSize_{0};
};
} // namespace facebook::velox::memory
