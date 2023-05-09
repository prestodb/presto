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

namespace facebook::velox::memory {

std::shared_ptr<MemoryAllocator> MemoryAllocator::instance_;
MemoryAllocator* MemoryAllocator::customInstance_;
std::mutex MemoryAllocator::initMutex_;

std::string MemoryAllocator::kindString(Kind kind) {
  switch (kind) {
    case Kind::kMalloc:
      return "MALLOC";
    case Kind::kMmap:
      return "MMAP";
    default:
      return fmt::format("UNKNOWN: {}", static_cast<int>(kind));
  }
}

std::ostream& operator<<(std::ostream& out, const MemoryAllocator::Kind& kind) {
  out << MemoryAllocator::kindString(kind);
  return out;
}

MemoryAllocator::SizeMix MemoryAllocator::allocationSize(
    MachinePageCount numPages,
    MachinePageCount minSizeClass) const {
  VELOX_CHECK_LE(
      minSizeClass,
      sizeClassSizes_.back(),
      "Requesting minimum size {} larger than largest size class {}",
      minSizeClass,
      sizeClassSizes_.back());

  MemoryAllocator::SizeMix mix;
  int32_t needed = numPages;
  int32_t pagesToAlloc = 0;
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
    if (FOLLY_UNLIKELY(numUnits * size > Allocation::PageRun::kMaxPagesInRun)) {
      VELOX_MEM_ALLOC_ERROR(fmt::format(
          "Too many pages {} to allocate, the number of units {} at size class of {} exceeds the PageRun limit {}",
          numPages,
          numUnits,
          size,
          Allocation::PageRun::kMaxPagesInRun));
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

MallocAllocator::MallocAllocator() : kind_(MemoryAllocator::Kind::kMalloc) {}

bool MallocAllocator::allocateNonContiguous(
    MachinePageCount numPages,
    Allocation& out,
    ReservationCallback reservationCB,
    MachinePageCount minSizeClass) {
  VELOX_CHECK_GT(numPages, 0);

  const SizeMix mix = allocationSize(numPages, minSizeClass);

  const uint64_t freedBytes = freeNonContiguous(out);
  uint64_t bytesToAllocate = 0;
  if (reservationCB != nullptr) {
    for (int32_t i = 0; i < mix.numSizes; ++i) {
      MachinePageCount numPages =
          mix.sizeCounts[i] * sizeClassSizes_[mix.sizeIndices[i]];
      bytesToAllocate += AllocationTraits::pageBytes(numPages);
    }
    bytesToAllocate -= freedBytes;
    try {
      reservationCB(bytesToAllocate, true);
    } catch (std::exception& e) {
      VELOX_MEM_LOG(WARNING) << "Failed to reserve " << bytesToAllocate
                             << " bytes for non-contiguous allocation of "
                             << numPages << " pages, then release "
                             << freedBytes << " bytes from the old allocation";
      // If the new memory reservation fails, we need to release the memory
      // reservation of the freed memory of previously allocation.
      reservationCB(freedBytes, false);
      std::rethrow_exception(std::current_exception());
    }
  }

  std::vector<void*> pages;
  pages.reserve(mix.numSizes);
  for (int32_t i = 0; i < mix.numSizes; ++i) {
    // Trigger allocation failure by breaking out the loop.
    if (testingHasInjectedFailure(InjectedFailure::kAllocate)) {
      break;
    }
    MachinePageCount numPages =
        mix.sizeCounts[i] * sizeClassSizes_[mix.sizeIndices[i]];
    void* ptr;
    stats_.recordAllocate(
        AllocationTraits::pageBytes(sizeClassSizes_[mix.sizeIndices[i]]),
        mix.sizeCounts[i],
        [&]() {
          ptr = ::malloc(AllocationTraits::pageBytes(numPages)); // NOLINT
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
    if (reservationCB != nullptr) {
      VELOX_MEM_LOG(WARNING)
          << "Failed to allocate memory for non-contiguous allocation of "
          << numPages << " pages, then release " << bytesToAllocate + freedBytes
          << " bytes of memory reservation including the old gallocation";
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

bool MallocAllocator::allocateContiguousImpl(
    MachinePageCount numPages,
    Allocation* collateral,
    ContiguousAllocation& allocation,
    ReservationCallback reservationCB) {
  MachinePageCount numCollateralPages = 0;
  if (collateral != nullptr) {
    numCollateralPages =
        freeNonContiguous(*collateral) / AllocationTraits::kPageSize;
  }
  auto numContiguousCollateralPages = allocation.numPages();
  if (numContiguousCollateralPages > 0) {
    if (::munmap(allocation.data(), allocation.size()) < 0) {
      VELOX_MEM_LOG(ERROR) << "munmap got " << folly::errnoStr(errno) << "for "
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
      reservationCB(AllocationTraits::pageBytes(numNeededPages), true);
    } catch (std::exception& e) {
      // If the new memory reservation fails, we need to release the memory
      // reservation of the freed contiguous and non-contiguous memory.
      VELOX_MEM_LOG(WARNING)
          << "Failed to reserve " << AllocationTraits::pageBytes(numNeededPages)
          << " bytes for contiguous allocation of " << numPages
          << " pages, then release "
          << (numCollateralPages + numContiguousCollateralPages) *
              AllocationTraits::kPageSize
          << " bytes from the old allocations";
      reservationCB(
          (numCollateralPages + numContiguousCollateralPages) *
              AllocationTraits::kPageSize,
          false);
      std::rethrow_exception(std::current_exception());
    }
  }
  numAllocated_.fetch_add(numPages);
  numMapped_.fetch_add(numPages);

  void* data = ::mmap(
      nullptr,
      AllocationTraits::pageBytes(numPages),
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS,
      -1,
      0);
  // TODO: add handling of MAP_FAILED.
  allocation.set(data, AllocationTraits::pageBytes(numPages));
  return true;
}

int64_t MallocAllocator::freeNonContiguous(Allocation& allocation) {
  if (allocation.empty()) {
    return 0;
  }
  MachinePageCount numFreed = 0;
  for (int32_t i = 0; i < allocation.numRuns(); ++i) {
    Allocation::PageRun run = allocation.runAt(i);
    numFreed += run.numPages();
    void* ptr = run.data();
    {
      std::lock_guard<std::mutex> l(mallocsMutex_);
      const auto ret = mallocs_.erase(ptr);
      VELOX_CHECK_EQ(ret, 1, "Bad free page pointer: {}", ptr);
    }
    stats_.recordFree(
        std::min<int64_t>(
            AllocationTraits::pageBytes(sizeClassSizes_.back()),
            AllocationTraits::pageBytes(run.numPages())),
        [&]() {
          ::free(ptr); // NOLINT
        });
  }
  numAllocated_.fetch_sub(numFreed);
  allocation.clear();
  return AllocationTraits::pageBytes(numFreed);
}

void MallocAllocator::freeContiguousImpl(ContiguousAllocation& allocation) {
  if (allocation.empty()) {
    return;
  }

  if (::munmap(allocation.data(), allocation.size()) < 0) {
    VELOX_MEM_LOG(ERROR) << "munmap returned " << folly::errnoStr(errno)
                         << " for " << allocation.data() << ", "
                         << allocation.size();
  }
  numMapped_.fetch_sub(allocation.numPages());
  numAllocated_.fetch_sub(allocation.numPages());
  allocation.clear();
}

void* MallocAllocator::allocateBytes(uint64_t bytes, uint16_t alignment) {
  alignmentCheck(bytes, alignment);
  void* result = (alignment > kMinAlignment) ? ::aligned_alloc(alignment, bytes)
                                             : ::malloc(bytes);
  if (FOLLY_UNLIKELY(result == nullptr)) {
    VELOX_MEM_LOG(ERROR) << "Failed to allocateBytes " << bytes
                         << " bytes with " << alignment << " alignment";
  }
  return result;
}

void* MallocAllocator::allocateZeroFilled(uint64_t bytes) {
  void* result = std::calloc(1, bytes);
  if (FOLLY_UNLIKELY(result == nullptr)) {
    VELOX_MEM_LOG(ERROR) << "Failed to allocateZeroFilled " << bytes
                         << " bytes";
  }
  return result;
}

void MallocAllocator::freeBytes(void* p, uint64_t bytes) noexcept {
  ::free(p); // NOLINT
}

bool MallocAllocator::checkConsistency() const {
  return true;
}

std::string MallocAllocator::toString() const {
  return fmt::format(
      "[allocated pages {}, mapped pages {}]", numAllocated_, numMapped_);
}

// static
MemoryAllocator* MemoryAllocator::getInstance() {
  std::lock_guard<std::mutex> l(initMutex_);
  if (customInstance_ != nullptr) {
    return customInstance_;
  }
  if (instance_ != nullptr) {
    return instance_.get();
  }
  instance_ = createDefaultInstance();
  return instance_.get();
}

// static
std::shared_ptr<MemoryAllocator> MemoryAllocator::createDefaultInstance() {
  return std::make_shared<MallocAllocator>();
}

// static
void MemoryAllocator::setDefaultInstance(MemoryAllocator* instance) {
  std::lock_guard<std::mutex> l(initMutex_);
  customInstance_ = instance;
}

// static
void MemoryAllocator::testingDestroyInstance() {
  std::lock_guard<std::mutex> l(initMutex_);
  instance_ = nullptr;
}

// static
void MemoryAllocator::alignmentCheck(
    uint64_t allocateBytes,
    uint16_t alignmentBytes) {
  VELOX_CHECK_GE(alignmentBytes, kMinAlignment);
  if (alignmentBytes == kMinAlignment) {
    return;
  }
  VELOX_CHECK_LE(alignmentBytes, kMaxAlignment);
  VELOX_CHECK_EQ(allocateBytes % alignmentBytes, 0);
  VELOX_CHECK_EQ((alignmentBytes & (alignmentBytes - 1)), 0);
}

// static.
MachinePageCount MemoryAllocator::roundUpToSizeClassSize(
    size_t bytes,
    const std::vector<MachinePageCount>& sizes) {
  auto pages = bits::roundUp(bytes, AllocationTraits::kPageSize) /
      AllocationTraits::kPageSize;
  VELOX_CHECK_LE(pages, sizes.back());
  return *std::lower_bound(sizes.begin(), sizes.end(), pages);
}

void* MemoryAllocator::allocateZeroFilled(uint64_t bytes) {
  void* result = allocateBytes(bytes);
  if (result != nullptr) {
    ::memset(result, 0, bytes);
  } else {
    VELOX_MEM_LOG(ERROR) << "Failed to allocateZeroFilled " << bytes
                         << " bytes";
  }
  return result;
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
