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

// Allocates sets of mmapped pages, so that each allocation is
// composed of the needed mix of standard size contiguous runs.  If
// --velox_use_malloc is true, allocates with malloc instead of mmap. This
// allows using asan and similar tools.
class MappedMemory {
 public:
  static constexpr uint64_t kPageSize = 4096;
  static constexpr int32_t kMaxSizeClasses = 12;

  // Represents a number of consecutive pages of kPageSize bytes.
  class PageRun {
   public:
    static constexpr uint8_t kPointerSignificantBits = 48;
    static constexpr uint64_t kPointerMask = 0xffffffffffff;
    static constexpr uint32_t kMaxPagesInRun =
        (1UL << (64U - kPointerSignificantBits)) - 1;

    PageRun(void* address, MachinePageCount numPages) {
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

  // Represents a set of PageRuns that are allocated together.
  class Allocation {
   public:
    explicit Allocation(MappedMemory* mappedMemory)
        : mappedMemory_(mappedMemory) {
      VELOX_CHECK(mappedMemory);
      // We keep reference to mappedMemory's shared pointer to prevent
      // destruction of ScopedMappedMemory instance until all allocations
      // are destroyed.
      mappedMemoryPtr_ = mappedMemory->sharedPtr();
    }

    ~Allocation() {
      mappedMemory_->free(*this);
    }

    Allocation(const Allocation& other) = delete;

    Allocation(Allocation&& other) noexcept {
      mappedMemory_ = other.mappedMemory_;
      mappedMemoryPtr_ = other.mappedMemoryPtr_;
      runs_ = std::move(other.runs_);
      numPages_ = other.numPages_;
      other.numPages_ = 0;
    }

    void operator=(const Allocation& other) = delete;

    void operator=(Allocation&& other) {
      mappedMemory_ = other.mappedMemory_;
      mappedMemoryPtr_ = other.mappedMemoryPtr_;
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

    void append(uint8_t* address, int32_t numPages);

    void clear() {
      runs_.clear();
      numPages_ = 0;
    }

    // Returns the run number and the position within the run
    // corresponding to 'offset' from the start of 'this'.
    void findRun(uint64_t offset, int32_t* index, int32_t* offsetInRun);

   private:
    MappedMemory* mappedMemory_;
    std::shared_ptr<MappedMemory> mappedMemoryPtr_;
    std::vector<PageRun> runs_;
    int32_t numPages_ = 0;
  };

  virtual ~MappedMemory() {}
  static MappedMemory* getInstance();

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

  // Checks internal consistency of allocation data
  // structures. Returns true if OK.
  virtual bool checkConsistency() = 0;

  static void destroyTestOnly();

  virtual const std::vector<MachinePageCount>& sizes() const = 0;
  virtual MachinePageCount numAllocated() const = 0;
  virtual MachinePageCount numMapped() const = 0;

  virtual std::shared_ptr<MappedMemory> addChild(
      std::shared_ptr<MemoryUsageTracker> tracker);

  virtual std::shared_ptr<MappedMemory> sharedPtr() {
    return nullptr;
  }

 private:
  // Singleton instance.
  static std::unique_ptr<MappedMemory> instance_;
  static std::mutex initMutex_;
};

// Wrapper around MappedMemory for scoped tracking of activity. We
// expect a single level of wrappers around the process root
// MappedMemory. Each will have its own MemoryUsageTracker that will
// be a child of the Driver/Task level tracker. in this way
// MappedMemory activity can be attributed to individual operators and
// these operators can be requested to spill or limit their memory utilization.
class ScopedMappedMemory final
    : public MappedMemory,
      public std::enable_shared_from_this<ScopedMappedMemory> {
 public:
  ScopedMappedMemory(
      MappedMemory* parent,
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

  bool checkConsistency() override {
    return parent_->checkConsistency();
  }

  const std::vector<MachinePageCount>& sizes() const override {
    return parent_->sizes();
  }

  MachinePageCount numAllocated() const override {
    return parent_->numAllocated();
  }

  MachinePageCount numMapped() const override {
    return parent_->numMapped();
  }

  std::shared_ptr<MappedMemory> addChild(
      std::shared_ptr<MemoryUsageTracker> tracker) override {
    return std::make_shared<ScopedMappedMemory>(shared_from_this(), tracker);
  }

  std::shared_ptr<MappedMemory> sharedPtr() override {
    return shared_from_this();
  }

 private:
  std::shared_ptr<MappedMemory> parentPtr_;
  MappedMemory* parent_;
  std::shared_ptr<MemoryUsageTracker> tracker_;
};

} // namespace facebook::velox::memory
