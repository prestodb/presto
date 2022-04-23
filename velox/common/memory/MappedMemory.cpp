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

#include "velox/common/memory/MappedMemory.h"
#include "velox/common/base/BitUtil.h"

#include <sys/mman.h>

#include <iostream>

namespace facebook::velox::memory {

void MappedMemory::Allocation::append(uint8_t* address, int32_t numPages) {
  numPages_ += numPages;
  if (runs_.empty()) {
    runs_.emplace_back(address, numPages);
    return;
  }
  PageRun last = runs_.back();
  if (address == last.data()) {
    VELOX_CHECK(false, "Appending a duplicate address into a PageRun");
  }
  // Increment page count if new data starts at end of the last run
  // and the combined page count is within limits.
  if (address == last.data() + last.numPages() * kPageSize &&
      last.numPages() + numPages <= PageRun::kMaxPagesInRun) {
    runs_.back() = PageRun(last.data(), last.numPages() + numPages);
  } else {
    runs_.emplace_back(address, numPages);
  }
}

void MappedMemory::Allocation::findRun(
    uint64_t offset,
    int32_t* index,
    int32_t* offsetInRun) {
  uint64_t skipped = 0;
  for (int32_t i = 0; i < runs_.size(); ++i) {
    uint64_t size = runs_[i].numPages() * kPageSize;
    if (offset - skipped < size) {
      *index = i;
      *offsetInRun = static_cast<int32_t>(offset - skipped);
      return;
    }
    skipped += size;
  }
  VELOX_CHECK(false, "Seeking to an out of range offset in Allocation");
}

MachinePageCount MappedMemory::ContiguousAllocation::numPages() const {
  return bits::roundUp(size_, kPageSize) / kPageSize;
}

// static
void MappedMemory::destroyTestOnly() {
  instance_ = nullptr;
}

std::string MappedMemory::toString() const {
  return fmt::format("MappedMemory: Allocated pages {}", numAllocated());
}

MappedMemory::SizeMix MappedMemory::allocationSize(
    MachinePageCount numPages,
    MachinePageCount minSizeClass) const {
  int32_t needed = numPages;
  int32_t pagesToAlloc = 0;
  SizeMix mix;
  VELOX_CHECK_LE(
      minSizeClass,
      sizeClassSizes_.back(),
      "Requesting minimum size {} larger than largest size class {}",
      minSizeClass,
      sizeClassSizes_.back());
  for (int32_t sizeIndex = sizeClassSizes_.size() - 1; sizeIndex >= 0;
       sizeIndex--) {
    int32_t size = sizeClassSizes_[sizeIndex];
    bool isSmallest =
        sizeIndex == 0 || sizeClassSizes_[sizeIndex - 1] < minSizeClass;
    // If the size is less than 1/8 of the size from the next larger,
    // use the next larger size.
    if (size > (needed + (needed / 8)) && !isSmallest) {
      continue;
    }
    int32_t numUnits = std::max(1, needed / size);
    needed -= numUnits * size;
    if (isSmallest && needed > 0) {
      // If needed / size had a remainder, add one more unit. Do this
      // if the present size class is the smallest or 'minSizeClass'
      // size.
      numUnits++;
      needed -= size;
    }
    mix.sizeCounts[mix.numSizes] = numUnits;
    pagesToAlloc += numUnits * size;
    mix.sizeIndices[mix.numSizes++] = sizeIndex;
    if (needed <= 0) {
      break;
    }
  }
  mix.totalPages = pagesToAlloc;
  return mix;
}

namespace {
// Actual Implementation of MappedMemory.
class MappedMemoryImpl : public MappedMemory {
 public:
  MappedMemoryImpl();
  bool allocate(
      MachinePageCount numPages,
      int32_t owner,
      Allocation& out,
      std::function<void(int64_t)> beforeAllocCB = nullptr,
      MachinePageCount minSizeClass = 0) override;
  int64_t free(Allocation& allocation) override;

  bool allocateContiguous(
      MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      std::function<void(int64_t)> beforeAllocCB = nullptr) override;

  void freeContiguous(ContiguousAllocation& allocation) override;

  bool checkConsistency() const override;

  MachinePageCount numAllocated() const override {
    return numAllocated_;
  }

  MachinePageCount numMapped() const override {
    return numMapped_;
  }

 private:
  std::atomic<MachinePageCount> numAllocated_;
  // When using mmap/madvise, the current of number pages backed by memory.
  std::atomic<MachinePageCount> numMapped_;

  std::mutex mallocsMutex_;
  // Tracks malloc'd pointers to detect bad frees.
  std::unordered_set<void*> mallocs_;
};

} // namespace

MappedMemoryImpl::MappedMemoryImpl() : numAllocated_(0), numMapped_(0) {}

bool MappedMemoryImpl::allocate(
    MachinePageCount numPages,
    int32_t owner,
    Allocation& out,
    std::function<void(int64_t)> beforeAllocCB,
    MachinePageCount minSizeClass) {
  free(out);

  auto mix = allocationSize(numPages, minSizeClass);

  if (FLAGS_velox_use_malloc) {
    if (beforeAllocCB) {
      uint64_t bytesAllocated = 0;
      for (int32_t i = 0; i < mix.numSizes; ++i) {
        MachinePageCount numPages =
            mix.sizeCounts[i] * sizeClassSizes_[mix.sizeIndices[i]];
        bytesAllocated += numPages * kPageSize;
      }
      beforeAllocCB(bytesAllocated);
    }

    std::vector<void*> pages;
    pages.reserve(mix.numSizes);
    for (int32_t i = 0; i < mix.numSizes; ++i) {
      MachinePageCount numPages =
          mix.sizeCounts[i] * sizeClassSizes_[mix.sizeIndices[i]];
      void* ptr = malloc(numPages * kPageSize); // NOLINT
      if (!ptr) {
        // Failed to allocate memory from memory.
        break;
      }
      pages.emplace_back(ptr);
      out.append(reinterpret_cast<uint8_t*>(ptr), numPages); // NOLINT
    }
    if (pages.size() != mix.numSizes) {
      // Failed to allocate memory using malloc. Free any malloced pages and
      // return false.
      for (auto ptr : pages) {
        ::free(ptr);
      }
      out.clear();
      return false;
    }

    {
      std::lock_guard<std::mutex> l(mallocsMutex_);
      mallocs_.insert(pages.begin(), pages.end());
    }

    // Successfully allocated all pages.
    numAllocated_.fetch_add(mix.totalPages);
    return true;
  }
  throw std::runtime_error("Not implemented");
}

bool MappedMemoryImpl::allocateContiguous(
    MachinePageCount numPages,
    Allocation* FOLLY_NULLABLE collateral,
    ContiguousAllocation& allocation,
    std::function<void(int64_t)> beforeAllocCB) {
  MachinePageCount numCollateralPages = 0;
  if (collateral) {
    numCollateralPages = free(*collateral) / kPageSize;
  }
  auto numContiguousCollateralPages = allocation.numPages();
  if (numContiguousCollateralPages) {
    if (munmap(allocation.data(), allocation.size()) < 0) {
      LOG(ERROR) << "munmap got " << errno << "for " << allocation.data()
                 << ", " << allocation.size();
    }
    allocation.reset(nullptr, nullptr, 0);
  }
  int64_t numNeededPages =
      numPages - numCollateralPages - numContiguousCollateralPages;
  if (beforeAllocCB) {
    try {
      beforeAllocCB(numNeededPages * kPageSize);
    } catch (std::exception& e) {
      beforeAllocCB(
          -(numCollateralPages + numContiguousCollateralPages) * kPageSize);
      numAllocated_ -= numContiguousCollateralPages;
      std::rethrow_exception(std::current_exception());
    }
  }
  numAllocated_.fetch_add(numNeededPages);
  numMapped_.fetch_add(numNeededPages);
  void* data = mmap(
      nullptr,
      numPages * kPageSize,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS,
      -1,
      0);
  allocation.reset(this, data, numPages * kPageSize);
  return true;
}

int64_t MappedMemoryImpl::free(Allocation& allocation) {
  if (allocation.numRuns() == 0) {
    return 0;
  }
  MachinePageCount numFreed = 0;
  if (FLAGS_velox_use_malloc) {
    for (int32_t i = 0; i < allocation.numRuns(); ++i) {
      PageRun run = allocation.runAt(i);
      numFreed += run.numPages();
      void* ptr = run.data();
      {
        std::lock_guard<std::mutex> l(mallocsMutex_);
        if (mallocs_.find(ptr) == mallocs_.end()) {
          VELOX_CHECK(false, "Bad free");
        }
        mallocs_.erase(ptr);
      }
      ::free(ptr); // NOLINT
    }
  } else {
    throw std::runtime_error("Not implemented");
  }
  numAllocated_.fetch_sub(numFreed);
  allocation.clear();
  return numFreed * kPageSize;
}
void MappedMemoryImpl::freeContiguous(ContiguousAllocation& allocation) {
  if (allocation.data() && allocation.size()) {
    if (munmap(allocation.data(), allocation.size()) < 0) {
      LOG(ERROR) << "munmap returned " << errno << "for " << allocation.data()
                 << ", " << allocation.size();
    }
    numMapped_.fetch_sub(allocation.numPages());
    numAllocated_.fetch_sub(allocation.numPages());
    allocation.reset(nullptr, nullptr, 0);
  }
}

bool MappedMemoryImpl::checkConsistency() const {
  if (FLAGS_velox_use_malloc) {
    return true;
  }
  throw std::runtime_error("Not implemented");
}

MappedMemory* MappedMemory::customInstance_;
std::unique_ptr<MappedMemory> MappedMemory::instance_;
std::mutex MappedMemory::initMutex_;
std::atomic<uint64_t> MappedMemory::totalSmallAllocateBytes_;
std::atomic<uint64_t> MappedMemory::totalSizeClassAllocateBytes_;
std::atomic<uint64_t> MappedMemory::totalLargeAllocateBytes_;

// static
MappedMemory* MappedMemory::getInstance() {
  if (customInstance_) {
    return customInstance_;
  }
  if (instance_) {
    return instance_.get();
  }
  std::lock_guard<std::mutex> l(initMutex_);
  if (instance_) {
    return instance_.get();
  }
  instance_ = createDefaultInstance();
  return instance_.get();
}

// static
std::unique_ptr<MappedMemory> MappedMemory::createDefaultInstance() {
  return std::make_unique<MappedMemoryImpl>();
}

// static
void MappedMemory::setDefaultInstance(MappedMemory* instance) {
  customInstance_ = instance;
}

std::shared_ptr<MappedMemory> MappedMemory::addChild(
    std::shared_ptr<MemoryUsageTracker> tracker) {
  return std::make_shared<ScopedMappedMemory>(this, tracker);
}

namespace {
// Returns the size class size that corresponds to 'bytes'.
MachinePageCount roundUpToSizeClassSize(
    size_t bytes,
    const std::vector<MachinePageCount>& sizes) {
  auto pages =
      bits::roundUp(bytes, MappedMemory::kPageSize) / MappedMemory::kPageSize;
  VELOX_CHECK_LE(pages, sizes.back());
  return *std::lower_bound(sizes.begin(), sizes.end(), pages);
}
} // namespace

void* FOLLY_NULLABLE
MappedMemory::allocateBytes(uint64_t bytes, uint64_t maxMallocSize) {
  if (bytes <= maxMallocSize) {
    auto result = ::malloc(bytes);
    if (result) {
      totalSmallAllocateBytes_ += bytes;
    }
    return result;
  }
  if (bytes <= sizeClassSizes_.back() * kPageSize) {
    Allocation allocation(this);
    auto numPages = roundUpToSizeClassSize(bytes, sizeClassSizes_);
    if (allocate(numPages, kMallocOwner, allocation, nullptr, numPages)) {
      auto run = allocation.runAt(0);
      VELOX_CHECK_EQ(
          1,
          allocation.numRuns(),
          "A size class allocateBytes must produce one run");
      allocation.clear();
      totalSizeClassAllocateBytes_ += numPages * kPageSize;
      return run.data<char>();
    }
    return nullptr;
  }
  ContiguousAllocation allocation;
  auto numPages = bits::roundUp(bytes, kPageSize) / kPageSize;
  if (allocateContiguous(numPages, nullptr, allocation)) {
    char* data = allocation.data<char>();
    allocation.reset(nullptr, nullptr, 0);
    totalLargeAllocateBytes_ += numPages * kPageSize;
    return data;
  }
  return nullptr;
}

void MappedMemory::freeBytes(
    void* FOLLY_NONNULL p,
    uint64_t bytes,
    uint64_t maxMallocSize) noexcept {
  if (bytes <= maxMallocSize) {
    ::free(p); // NOLINT
    totalSmallAllocateBytes_ -= bytes;
  } else if (bytes <= sizeClassSizes_.back() * kPageSize) {
    Allocation allocation(this);
    auto numPages = roundUpToSizeClassSize(bytes, sizeClassSizes_);
    allocation.append(reinterpret_cast<uint8_t*>(p), numPages);
    free(allocation);
    totalSizeClassAllocateBytes_ -= numPages * kPageSize;
  } else {
    ContiguousAllocation allocation;
    allocation.reset(this, p, bytes);
    freeContiguous(allocation);
    totalLargeAllocateBytes_ -= bits::roundUp(bytes, kPageSize);
  }
}

bool ScopedMappedMemory::allocate(
    MachinePageCount numPages,
    int32_t owner,
    Allocation& out,
    std::function<void(int64_t)> beforeAllocCB,
    MachinePageCount minSizeClass) {
  free(out);
  return parent_->allocate(
      numPages,
      owner,
      out,
      [this, beforeAllocCB](int64_t allocated) {
        if (tracker_) {
          tracker_->update(allocated);
        }
        if (beforeAllocCB) {
          beforeAllocCB(allocated);
        }
      },
      minSizeClass);
}

bool ScopedMappedMemory::allocateContiguous(
    MachinePageCount numPages,
    Allocation* FOLLY_NULLABLE collateral,
    ContiguousAllocation& allocation,
    std::function<void(int64_t)> beforeAllocCB) {
  bool success = parent_->allocateContiguous(
      numPages,
      collateral,
      allocation,
      [this, beforeAllocCB](int64_t allocated) {
        if (tracker_) {
          tracker_->update(allocated);
        }
        if (beforeAllocCB) {
          beforeAllocCB(allocated);
        }
      });
  if (success) {
    allocation.reset(this, allocation.data(), allocation.size());
  }
  return success;
}

} // namespace facebook::velox::memory
