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

#include <sys/mman.h>
#include <iostream>
#include <numeric>

#include "velox/common/base/BitUtil.h"
#include "velox/common/testutil/TestValue.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::memory {

void MappedMemory::Allocation::append(uint8_t* address, int32_t numPages) {
  numPages_ += numPages;
  if (runs_.empty()) {
    runs_.emplace_back(address, numPages);
    return;
  }
  PageRun last = runs_.back();
  VELOX_CHECK_NE(
      address, last.data(), "Appending a duplicate address into a PageRun");

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
    int32_t* offsetInRun) const {
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

  VELOX_UNREACHABLE(
      "Seeking to an out of range offset {} in Allocation with {} pages and {} runs",
      offset,
      numPages_,
      runs_.size());
}

MachinePageCount MappedMemory::ContiguousAllocation::numPages() const {
  return bits::roundUp(size_, kPageSize) / kPageSize;
}

// static
void MappedMemory::testingDestroyInstance() {
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
       --sizeIndex) {
    const int32_t size = sizeClassSizes_[sizeIndex];
    const bool isSmallest =
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
      ++numUnits;
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
// The implementation of MappedMemory using std::malloc.
class MappedMemoryImpl : public MappedMemory {
 public:
  MappedMemoryImpl();

  bool allocateNonContiguous(
      MachinePageCount numPages,
      Allocation& out,
      std::function<void(int64_t, bool)> userAllocCB = nullptr,
      MachinePageCount minSizeClass = 0) override;

  int64_t freeNonContiguous(Allocation& allocation) override;

  bool allocateContiguous(
      MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      std::function<void(int64_t, bool)> userAllocCB = nullptr) override {
    bool result;
    stats_.recordAllocate(numPages * kPageSize, 1, [&]() {
      result =
          allocateContiguousImpl(numPages, collateral, allocation, userAllocCB);
    });
    return result;
  }

  void freeContiguous(ContiguousAllocation& allocation) override {
    stats_.recordFree(
        allocation.size(), [&]() { freeContiguousImpl(allocation); });
  }

  MachinePageCount numAllocated() const override {
    return numAllocated_;
  }

  MachinePageCount numMapped() const override {
    return numMapped_;
  }

  Stats stats() const override {
    return stats_;
  }

  bool checkConsistency() const override;

 private:
  bool allocateContiguousImpl(
      MachinePageCount numPages,
      Allocation* FOLLY_NULLABLE collateral,
      ContiguousAllocation& allocation,
      std::function<void(int64_t, bool)> userAllocCB);

  void freeContiguousImpl(ContiguousAllocation& allocation);

  std::atomic<MachinePageCount> numAllocated_;
  // When using mmap/madvise, the current of number pages backed by memory.
  std::atomic<MachinePageCount> numMapped_;

  std::mutex mallocsMutex_;
  // Tracks malloc'd pointers to detect bad frees.
  std::unordered_set<void*> mallocs_;
  Stats stats_;
};

} // namespace

MappedMemoryImpl::MappedMemoryImpl() : numAllocated_(0), numMapped_(0) {}

bool MappedMemoryImpl::allocateNonContiguous(
    MachinePageCount numPages,
    Allocation& out,
    std::function<void(int64_t, bool)> userAllocCB,
    MachinePageCount minSizeClass) {
  freeNonContiguous(out);

  const auto mix = allocationSize(numPages, minSizeClass);

  uint64_t bytesToAllocate = 0;
  if (userAllocCB != nullptr) {
    for (int32_t i = 0; i < mix.numSizes; ++i) {
      MachinePageCount numPages =
          mix.sizeCounts[i] * sizeClassSizes_[mix.sizeIndices[i]];
      bytesToAllocate += numPages * kPageSize;
    }
    userAllocCB(bytesToAllocate, true);
  }

  std::vector<void*> pages;
  pages.reserve(mix.numSizes);
  for (int32_t i = 0; i < mix.numSizes; ++i) {
    if (TestValue::enabled()) {
      // NOTE: if 'injectAllocFailure' is set to true by test callback, then
      // we break out the loop to trigger a memory allocation failure scenario
      // which doesn't have all the request memory allocated.
      bool injectAllocFailure = false;
      TestValue::adjust(
          "facebook::velox::memory::MappedMemoryImpl::allocate",
          &injectAllocFailure);
      if (injectAllocFailure) {
        break;
      }
    }
    MachinePageCount numPages =
        mix.sizeCounts[i] * sizeClassSizes_[mix.sizeIndices[i]];
    void* ptr;
    stats_.recordAllocate(
        sizeClassSizes_[mix.sizeIndices[i]] * kPageSize,
        mix.sizeCounts[i],
        [&]() {
          ptr = ::malloc(numPages * kPageSize); // NOLINT
        });
    if (ptr == nullptr) {
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
    if (userAllocCB != nullptr) {
      userAllocCB(bytesToAllocate, false);
    }
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

bool MappedMemoryImpl::allocateContiguousImpl(
    MachinePageCount numPages,
    Allocation* FOLLY_NULLABLE collateral,
    ContiguousAllocation& allocation,
    std::function<void(int64_t, bool)> userAllocCB) {
  MachinePageCount numCollateralPages = 0;
  if (collateral != nullptr) {
    numCollateralPages = freeNonContiguous(*collateral) / kPageSize;
  }
  auto numContiguousCollateralPages = allocation.numPages();
  if (numContiguousCollateralPages > 0) {
    if (::munmap(allocation.data(), allocation.size()) < 0) {
      LOG(ERROR) << "munmap got " << errno << "for " << allocation.data()
                 << ", " << allocation.size();
    }
    numMapped_.fetch_sub(numContiguousCollateralPages);
    numAllocated_.fetch_sub(numContiguousCollateralPages);
    allocation.reset(nullptr, nullptr, 0);
  }
  const int64_t numNeededPages =
      numPages - numCollateralPages - numContiguousCollateralPages;
  if (userAllocCB != nullptr) {
    try {
      userAllocCB(numNeededPages * kPageSize, true);
    } catch (std::exception& e) {
      userAllocCB(
          (numCollateralPages + numContiguousCollateralPages) * kPageSize,
          false);
      std::rethrow_exception(std::current_exception());
    }
  }
  numAllocated_.fetch_add(numPages);
  numMapped_.fetch_add(numNeededPages + numContiguousCollateralPages);
  void* data = ::mmap(
      nullptr,
      numPages * kPageSize,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS,
      -1,
      0);
  allocation.reset(this, data, numPages * kPageSize);
  return true;
}

int64_t MappedMemoryImpl::freeNonContiguous(Allocation& allocation) {
  if (allocation.numRuns() == 0) {
    return 0;
  }
  MachinePageCount numFreed = 0;
  for (int32_t i = 0; i < allocation.numRuns(); ++i) {
    PageRun run = allocation.runAt(i);
    numFreed += run.numPages();
    void* ptr = run.data();
    {
      std::lock_guard<std::mutex> l(mallocsMutex_);
      const auto ret = mallocs_.erase(ptr);
      VELOX_CHECK_EQ(ret, 1, "Bad free page pointer: ", ptr);
    }
    stats_.recordFree(
        std::min<int64_t>(
            sizeClassSizes_.back() * kPageSize, run.numPages() * kPageSize),
        [&]() {
          ::free(ptr); // NOLINT
        });
  }
  numAllocated_.fetch_sub(numFreed);
  allocation.clear();
  return numFreed * kPageSize;
}

void MappedMemoryImpl::freeContiguousImpl(ContiguousAllocation& allocation) {
  if (allocation.data() == nullptr || allocation.size() == 0) {
    return;
  }
  if (::munmap(allocation.data(), allocation.size()) < 0) {
    LOG(ERROR) << "munmap returned " << errno << "for " << allocation.data()
               << ", " << allocation.size();
  }
  numMapped_.fetch_sub(allocation.numPages());
  numAllocated_.fetch_sub(allocation.numPages());
  allocation.reset(nullptr, nullptr, 0);
}

bool MappedMemoryImpl::checkConsistency() const {
  return true;
}

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
std::shared_ptr<MappedMemory> MappedMemory::createDefaultInstance() {
  return std::make_shared<MappedMemoryImpl>();
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

void* FOLLY_NULLABLE MappedMemory::allocateBytes(uint64_t bytes) {
  if (bytes <= kMaxMallocBytes) {
    auto result = ::malloc(bytes);
    if (result) {
      totalSmallAllocateBytes_ += bytes;
    }
    return result;
  }
  if (bytes <= sizeClassSizes_.back() * kPageSize) {
    Allocation allocation(this);
    auto numPages = roundUpToSizeClassSize(bytes, sizeClassSizes_);
    if (allocateNonContiguous(numPages, allocation, nullptr, numPages)) {
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

void MappedMemory::freeBytes(void* FOLLY_NONNULL p, uint64_t bytes) noexcept {
  if (bytes <= kMaxMallocBytes) {
    ::free(p); // NOLINT
    totalSmallAllocateBytes_ -= bytes;
    return;
  }

  if (bytes <= sizeClassSizes_.back() * kPageSize) {
    Allocation allocation(this);
    auto numPages = roundUpToSizeClassSize(bytes, sizeClassSizes_);
    allocation.append(reinterpret_cast<uint8_t*>(p), numPages);
    freeNonContiguous(allocation);
    totalSizeClassAllocateBytes_ -= numPages * kPageSize;
    return;
  }

  ContiguousAllocation allocation;
  allocation.reset(this, p, bytes);
  freeContiguous(allocation);
  totalLargeAllocateBytes_ -= bits::roundUp(bytes, kPageSize);
}

bool ScopedMappedMemory::allocateNonContiguous(
    MachinePageCount numPages,
    Allocation& out,
    std::function<void(int64_t, bool)> userAllocCB,
    MachinePageCount minSizeClass) {
  freeNonContiguous(out);
  return parent_->allocateNonContiguous(
      numPages,
      out,
      [this, userAllocCB](int64_t allocBytes, bool preAllocate) {
        if (tracker_) {
          tracker_->update(preAllocate ? allocBytes : -allocBytes);
        }
        if (userAllocCB) {
          userAllocCB(allocBytes, preAllocate);
        }
      },
      minSizeClass);
}

bool ScopedMappedMemory::allocateContiguous(
    MachinePageCount numPages,
    Allocation* FOLLY_NULLABLE collateral,
    ContiguousAllocation& allocation,
    std::function<void(int64_t, bool)> userAllocCB) {
  bool success = parent_->allocateContiguous(
      numPages,
      collateral,
      allocation,
      [this, userAllocCB](int64_t allocBytes, bool preAlloc) {
        if (tracker_) {
          tracker_->update(preAlloc ? allocBytes : -allocBytes);
        }
        if (userAllocCB) {
          userAllocCB(allocBytes, preAlloc);
        }
      });
  if (success) {
    allocation.reset(this, allocation.data(), allocation.size());
  }
  return success;
}
Stats Stats::operator-(const Stats& other) const {
  Stats result;
  for (auto i = 0; i < sizes.size(); ++i) {
    result.sizes[i] = sizes[i] - other.sizes[i];
  }
  result.numAdvise = numAdvise - other.numAdvise;
  return result;
}

std::string Stats::toString() const {
  std::stringstream out;
  int64_t totalClocks = 0;
  int64_t totalBytes = 0;
  for (auto i = 0; i < sizes.size(); ++i) {
    totalClocks += sizes[i].clocks();
    totalBytes += sizes[i].totalBytes;
  }
  out << fmt::format(
      "Alloc: {}MB {} Gigaclocks, {}MB advised\n",
      totalBytes >> 20,
      totalClocks >> 30,
      numAdvise >> 8);

  // Sort the size classes by decreasing clocks.
  std::vector<int32_t> indices(sizes.size());
  std::iota(indices.begin(), indices.end(), 0);
  std::sort(indices.begin(), indices.end(), [&](int32_t left, int32_t right) {
    return sizes[left].clocks() > sizes[right].clocks();
  });
  for (auto i : indices) {
    // Do not report size classes with under 1M clocks.
    if (sizes[i].clocks() < 1000000) {
      break;
    }
    out << fmt::format(
        "Size {}K: {}MB {} Megaclocks\n",
        sizes[i].size * 4,
        sizes[i].totalBytes >> 20,
        sizes[i].clocks() >> 20);
  }
  return out.str();
}

} // namespace facebook::velox::memory
