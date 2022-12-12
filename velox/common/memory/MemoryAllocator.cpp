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

#include "velox/common/memory/MemoryAllocator.h"

#include <sys/mman.h>

#include <iostream>
#include <numeric>

#include "velox/common/base/BitUtil.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::memory {
/*static*/
void MemoryAllocator::validateAlignment(uint16_t alignment) {
  if (alignment == 0) {
    return;
  }
  VELOX_CHECK_LE(MemoryAllocator::kMinAlignment, alignment);
  VELOX_CHECK_GE(MemoryAllocator::kMaxAlignment, alignment);
  VELOX_CHECK_EQ(alignment & (alignment - 1), 0);
}

MemoryAllocator::Allocation::~Allocation() {
  if (pool_ != nullptr) {
    pool_->freeNonContiguous(*this);
  }
  VELOX_CHECK_EQ(numPages_, 0);
  VELOX_CHECK(runs_.empty());
}

void MemoryAllocator::Allocation::append(uint8_t* address, int32_t numPages) {
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

void MemoryAllocator::Allocation::findRun(
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

std::string MemoryAllocator::Allocation::toString() const {
  return fmt::format(
      "Allocation[numPages:{}, numRuns:{}, pool:{}]",
      numPages_,
      runs_.size(),
      pool_ == nullptr ? "null" : "set");
}

MemoryAllocator::ContiguousAllocation::~ContiguousAllocation() {
  if (pool_ != nullptr) {
    pool_->freeContiguous(*this);
    data_ = nullptr;
    pool_ = nullptr;
    size_ = 0;
  }
  VELOX_CHECK_NULL(data_);
  VELOX_CHECK_EQ(size_, 0);
}

void MemoryAllocator::ContiguousAllocation::set(void* data, uint64_t size) {
  data_ = data;
  size_ = size;
  sanityCheck();
}

void MemoryAllocator::ContiguousAllocation::clear() {
  pool_ = nullptr;
  set(nullptr, 0);
}

MachinePageCount MemoryAllocator::ContiguousAllocation::numPages() const {
  return bits::roundUp(size_, kPageSize) / kPageSize;
}

std::string MemoryAllocator::ContiguousAllocation::toString() const {
  return fmt::format(
      "ContiguousAllocation[data:{}, size:{}, pool:{}]",
      data_,
      size_,
      pool_ == nullptr ? "null" : "set");
}

MemoryAllocator::SizeMix MemoryAllocator::allocationSize(
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

// static
void MemoryAllocator::alignmentCheck(
    uint64_t allocateBytes,
    uint16_t alignmentBytes) {
  if (FOLLY_LIKELY(alignmentBytes == 0)) {
    return;
  }
  VELOX_CHECK_GE(alignmentBytes, MemoryAllocator::kMinAlignment);
  VELOX_CHECK_LE(alignmentBytes, MemoryAllocator::kMaxAlignment);
  VELOX_CHECK_EQ(allocateBytes % alignmentBytes, 0);
  VELOX_CHECK_EQ((alignmentBytes & (alignmentBytes - 1)), 0);
}

namespace {
// The implementation of MemoryAllocator using malloc.
class MemoryAllocatorImpl : public MemoryAllocator {
 public:
  MemoryAllocatorImpl();

  void* allocateBytes(uint64_t bytes, uint16_t alignment, uint64_t /*unused*/)
      override;

  void freeBytes(void* p, uint64_t bytes, uint64_t /*unused*/) noexcept
      override;

  bool allocateNonContiguous(
      MachinePageCount numPages,
      Allocation& out,
      ReservationCallback reservationCB = nullptr,
      MachinePageCount minSizeClass = 0) override;

  int64_t freeNonContiguous(Allocation& allocation) override;

  bool allocateContiguous(
      MachinePageCount numPages,
      Allocation* collateral,
      ContiguousAllocation& allocation,
      ReservationCallback reservationCB = nullptr) override {
    // VELOX_CHECK_GT(numPages, 0);
    bool result;
    stats_.recordAllocate(numPages * kPageSize, 1, [&]() {
      result = allocateContiguousImpl(
          numPages, collateral, allocation, reservationCB);
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
      ReservationCallback reservationCB);

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

MemoryAllocatorImpl::MemoryAllocatorImpl() : numAllocated_(0), numMapped_(0) {}

void* MemoryAllocatorImpl::allocateBytes(
    uint64_t bytes,
    uint16_t alignment,
    uint64_t /*unused*/) {
  alignmentCheck(bytes, alignment);
  auto* result =
      alignment != 0 ? ::aligned_alloc(alignment, bytes) : ::malloc(bytes);
  if (result != nullptr) {
    totalSmallAllocateBytes_ += bytes;
  } else {
    LOG(ERROR) << "Invalid aligned memory allocation with " << alignment
               << " alignment and " << bytes << " bytes";
  }
  return result;
}

void MemoryAllocatorImpl::freeBytes(
    void* p,
    uint64_t bytes,
    uint64_t /*unused*/) noexcept {
  ::free(p); // NOLINT
  totalSmallAllocateBytes_ -= bytes;
  VELOX_CHECK_GE(totalSmallAllocateBytes_, 0);
}

bool MemoryAllocatorImpl::allocateNonContiguous(
    MachinePageCount numPages,
    Allocation& out,
    ReservationCallback reservationCB,
    MachinePageCount minSizeClass) {
  VELOX_CHECK_GT(numPages, 0);
  const uint64_t freedBytes = freeNonContiguous(out);

  const auto mix = allocationSize(numPages, minSizeClass);

  uint64_t bytesToAllocate = 0;
  if (reservationCB != nullptr) {
    for (int32_t i = 0; i < mix.numSizes; ++i) {
      MachinePageCount numClassPages =
          mix.sizeCounts[i] * sizeClassSizes_[mix.sizeIndices[i]];
      bytesToAllocate += numClassPages * kPageSize;
    }
    bytesToAllocate -= freedBytes;
    try {
      reservationCB(bytesToAllocate, true);
    } catch (std::exception& e) {
      // If the new memory reservation fails, we need to release the memory
      // reservation of the freed memory of previously allocation.
      reservationCB(freedBytes, false);
      std::rethrow_exception(std::current_exception());
    }
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
          "facebook::velox::memory::MemoryAllocatorImpl::allocateNonContiguous",
          &injectAllocFailure);
      if (injectAllocFailure) {
        break;
      }
    }
    MachinePageCount numClassPages =
        mix.sizeCounts[i] * sizeClassSizes_[mix.sizeIndices[i]];
    void* ptr;
    stats_.recordAllocate(
        sizeClassSizes_[mix.sizeIndices[i]] * kPageSize,
        mix.sizeCounts[i],
        [&]() {
          ptr = ::aligned_alloc(
              MemoryAllocator::kMaxAlignment,
              numClassPages * kPageSize); // NOLINT
        });
    if (ptr == nullptr) {
      // Failed to allocate memory from memory.
      break;
    }
    pages.emplace_back(ptr);
    out.append(reinterpret_cast<uint8_t*>(ptr), numClassPages); // NOLINT
  }

  if (pages.size() != mix.numSizes) {
    // Failed to allocate memory using malloc. Free any malloced pages and
    // return false.
    for (auto ptr : pages) {
      ::free(ptr);
    }
    out.clear();
    if (reservationCB != nullptr) {
      reservationCB(bytesToAllocate + freedBytes, false);
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

bool MemoryAllocatorImpl::allocateContiguousImpl(
    MachinePageCount numPages,
    Allocation* collateral,
    ContiguousAllocation& allocation,
    ReservationCallback reservationCB) {
  MachinePageCount numCollateralPages = 0;
  if (collateral != nullptr) {
    numCollateralPages = freeNonContiguous(*collateral) / kPageSize;
  }
  auto numContiguousCollateralPages = allocation.numPages();
  if (numContiguousCollateralPages > 0) {
    if (::munmap(allocation.data(), allocation.size()) < 0) {
      LOG(ERROR) << "munmap got " << folly::errnoStr(errno) << "for "
                 << allocation.data() << ", " << allocation.size();
    }
    numMapped_.fetch_sub(numContiguousCollateralPages);
    numAllocated_.fetch_sub(numContiguousCollateralPages);
    allocation.clear();
  }
  const int64_t numNeededPages =
      numPages - numCollateralPages - numContiguousCollateralPages;
  if (reservationCB != nullptr) {
    try {
      reservationCB(numNeededPages * kPageSize, true);
    } catch (std::exception& e) {
      // If the new memory reservation fails, we need to release the memory
      // reservation of the freed contiguous and non-contiguous memory.
      reservationCB(
          (numCollateralPages + numContiguousCollateralPages) * kPageSize,
          false);
      std::rethrow_exception(std::current_exception());
    }
  }
  numAllocated_.fetch_add(numPages);
  numMapped_.fetch_add(numNeededPages + numContiguousCollateralPages);

  if (TestValue::enabled()) {
    bool injectMmapError = false;
    TestValue::adjust(
        "facebook::velox::memory::MemoryAllocatorImpl::allocateContiguousImpl",
        &injectMmapError);
    if (injectMmapError) {
      return false;
    }
  }
  void* data = ::mmap(
      nullptr,
      numPages * kPageSize,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS,
      -1,
      0);
  allocation.set(data, numPages * kPageSize);
  return true;
}

int64_t MemoryAllocatorImpl::freeNonContiguous(Allocation& allocation) {
  if (allocation.empty()) {
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
      VELOX_CHECK_EQ(ret, 1, "Bad free page pointer: {}", ptr);
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

void MemoryAllocatorImpl::freeContiguousImpl(ContiguousAllocation& allocation) {
  if (allocation.empty()) {
    return;
  }

  if (::munmap(allocation.data(), allocation.size()) < 0) {
    LOG(ERROR) << "munmap returned " << folly::errnoStr(errno) << " for "
               << allocation.data() << ", " << allocation.size();
  }
  numMapped_.fetch_sub(allocation.numPages());
  numAllocated_.fetch_sub(allocation.numPages());
  allocation.clear();
}

bool MemoryAllocatorImpl::checkConsistency() const {
  return true;
}

// static
MemoryAllocator* MemoryAllocator::getInstance() {
  {
    folly::SharedMutex::ReadHolder readGuard(instanceMutex_);
    if (customInstance_) {
      return customInstance_;
    }
    if (instance_) {
      return instance_.get();
    }
  }
  folly::SharedMutex::WriteHolder writeGuard(instanceMutex_);
  if (instance_) {
    return instance_.get();
  }
  instance_ = createDefaultInstance();
  return instance_.get();
}

// static
std::shared_ptr<MemoryAllocator> MemoryAllocator::createDefaultInstance() {
  return std::make_shared<MemoryAllocatorImpl>();
}

// static
void MemoryAllocator::setDefaultInstance(MemoryAllocator* instance) {
  folly::SharedMutex::WriteHolder writeGuard(instanceMutex_);
  customInstance_ = instance;
}

// static
void MemoryAllocator::testingDestroyInstance() {
  folly::SharedMutex::WriteHolder writeGuard(instanceMutex_);
  instance_ = nullptr;
}

// static
MachinePageCount MemoryAllocator::roundUpToSizeClassSize(
    size_t bytes,
    const std::vector<MachinePageCount>& sizes) {
  auto pages = bits::roundUp(bytes, MemoryAllocator::kPageSize) /
      MemoryAllocator::kPageSize;
  VELOX_CHECK_LE(pages, sizes.back());
  return *std::lower_bound(sizes.begin(), sizes.end(), pages);
}

std::string MemoryAllocator::toString() const {
  return fmt::format("MemoryAllocator: Allocated pages {}", numAllocated());
}

void* MemoryAllocator::allocateZeroFilled(uint64_t bytes, uint64_t alignment) {
  auto* result = allocateBytes(bytes, alignment);
  if (result != nullptr) {
    ::memset(result, 0, bytes);
  }
  return result;
}

void* FOLLY_NULLABLE MemoryAllocator::reallocateBytes(
    void* FOLLY_NULLABLE p,
    int64_t size,
    int64_t newSize,
    uint16_t alignment) {
  auto* newAlloc = allocateBytes(newSize, alignment);
  if (p == nullptr || newAlloc == nullptr) {
    return newAlloc;
  }
  ::memcpy(newAlloc, p, std::min(size, newSize));
  freeBytes(p, size);
  return newAlloc;
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
