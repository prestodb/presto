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
#include "velox/common/memory/MallocAllocator.h"

#include <sys/mman.h>
#include <sys/resource.h>
#include <iostream>
#include <numeric>

#include "velox/common/base/BitUtil.h"
#include "velox/common/memory/Memory.h"

DECLARE_bool(velox_memory_use_hugepages);

namespace facebook::velox::memory {

// static
std::vector<MachinePageCount> MemoryAllocator::makeSizeClassSizes(
    MachinePageCount largest) {
  VELOX_CHECK_LE(256, largest);
  VELOX_CHECK_EQ(largest, bits::nextPowerOfTwo(largest));
  std::vector<MachinePageCount> sizes;
  for (auto size = 1; size <= largest; size *= 2) {
    sizes.push_back(size);
  }
  return sizes;
}

namespace {
std::string& cacheFailureMessage() {
  thread_local std::string message;
  return message;
}

std::string& allocatorFailureMessage() {
  thread_local std::string errMsg;
  return errMsg;
}
} // namespace

void setCacheFailureMessage(std::string message) {
  cacheFailureMessage() = std::move(message);
}

std::string getAndClearCacheFailureMessage() {
  auto errMsg = std::move(cacheFailureMessage());
  cacheFailureMessage().clear(); // ensure its in valid state
  return errMsg;
}

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
  int32_t neededPages = numPages;
  MachinePageCount pagesToAlloc{0};
  for (int32_t sizeIndex = sizeClassSizes_.size() - 1; sizeIndex >= 0;
       --sizeIndex) {
    const MachinePageCount classPageSize = sizeClassSizes_[sizeIndex];
    const bool isSmallest =
        sizeIndex == 0 || sizeClassSizes_[sizeIndex - 1] < minSizeClass;
    // If the size is less than 1/8 of the size from the next larger,
    // use the next larger size.
    if (classPageSize > (neededPages + (neededPages / 8)) && !isSmallest) {
      continue;
    }
    const MachinePageCount maxNumClassPages =
        Allocation::PageRun::kMaxPagesInRun / classPageSize;
    MachinePageCount numClassPages = std::min<int32_t>(
        maxNumClassPages,
        std::max<MachinePageCount>(1, neededPages / classPageSize));
    neededPages -= numClassPages * classPageSize;
    if (isSmallest && neededPages > 0 && numClassPages < maxNumClassPages) {
      // If needed / size had a remainder, add one more unit. Do this if the
      // present size class is the smallest or 'minSizeClass' size.
      ++numClassPages;
      neededPages -= classPageSize;
    }
    VELOX_CHECK_LE(
        classPageSize * numClassPages, Allocation::PageRun::kMaxPagesInRun);

    mix.sizeCounts.push_back(numClassPages);
    mix.sizeIndices.push_back(sizeIndex);
    ++mix.numSizes;
    pagesToAlloc += numClassPages * classPageSize;
    if (neededPages <= 0) {
      break;
    }
    if (FOLLY_UNLIKELY(numClassPages == maxNumClassPages)) {
      ++sizeIndex;
    }
  }
  mix.totalPages = pagesToAlloc;
  return mix;
}

// static
bool MemoryAllocator::isAlignmentValid(
    uint64_t allocateBytes,
    uint16_t alignmentBytes) {
  return (alignmentBytes == kMinAlignment) ||
      (alignmentBytes >= kMinAlignment && alignmentBytes <= kMaxAlignment &&
       allocateBytes % alignmentBytes == 0 &&
       (alignmentBytes & (alignmentBytes - 1)) == 0);
}

void MemoryAllocator::alignmentCheck(
    uint64_t allocateBytes,
    uint16_t alignmentBytes) {
  if (FOLLY_UNLIKELY(!isAlignmentValid(allocateBytes, alignmentBytes))) {
    VELOX_FAIL(
        "Alignment check failed, allocateBytes {}, alignmentBytes {}",
        allocateBytes,
        alignmentBytes);
  }
}

// static.
MachinePageCount MemoryAllocator::roundUpToSizeClassSize(
    size_t bytes,
    const std::vector<MachinePageCount>& sizes) {
  auto pages = AllocationTraits::numPages(bytes);
  VELOX_CHECK_LE(pages, sizes.back());
  return *std::lower_bound(sizes.begin(), sizes.end(), pages);
}

namespace {
MachinePageCount pagesToAcquire(
    MachinePageCount numPages,
    MachinePageCount collateralPages) {
  return numPages <= collateralPages ? 0 : numPages - collateralPages;
}
} // namespace

bool MemoryAllocator::allocateNonContiguous(
    MachinePageCount numPages,
    Allocation& out,
    ReservationCallback reservationCB,
    MachinePageCount minSizeClass) {
  const MachinePageCount numPagesToFree = out.numPages();
  const uint64_t bytesToFree = AllocationTraits::pageBytes(numPagesToFree);
  auto cleanupAllocAndReleaseReservation = [&](uint64_t reservationBytes) {
    if (!out.empty()) {
      freeNonContiguous(out);
    }
    if (reservationCB != nullptr && reservationBytes > 0) {
      reservationCB(reservationBytes, false);
    }
  };
  if (numPages == 0) {
    cleanupAllocAndReleaseReservation(bytesToFree);
    return true;
  }

  const SizeMix mix = allocationSize(numPages, minSizeClass);
  if (reservationCB != nullptr) {
    if (mix.totalPages >= numPagesToFree) {
      const uint64_t numNeededPages = mix.totalPages - numPagesToFree;
      try {
        reservationCB(AllocationTraits::pageBytes(numNeededPages), true);
      } catch (const std::exception&) {
        VELOX_MEM_LOG_EVERY_MS(WARNING, 1'000)
            << "Exceeded memory reservation limit when reserve "
            << numNeededPages << " new pages when allocate " << mix.totalPages
            << " pages";
        cleanupAllocAndReleaseReservation(bytesToFree);
        std::rethrow_exception(std::current_exception());
      }
    } else {
      const uint64_t numExtraPages = numPagesToFree - mix.totalPages;
      reservationCB(AllocationTraits::pageBytes(numExtraPages), false);
    }
  }

  const auto totalBytesReserved = AllocationTraits::pageBytes(mix.totalPages);
  bool success = false;
  if (cache() == nullptr) {
    success = allocateNonContiguousWithoutRetry(mix, out);
  } else {
    success = cache()->makeSpace(
        pagesToAcquire(numPages, out.numPages()), [&](Allocation& acquired) {
          freeNonContiguous(acquired);
          return allocateNonContiguousWithoutRetry(mix, out);
        });
  }
  if (!success) {
    // There can be a failure where allocation was never called because there
    // never was a chance based on numAllocated() and capacity(). Make sure old
    // data is still freed.
    cleanupAllocAndReleaseReservation(totalBytesReserved);
  }
  return success;
}

bool MemoryAllocator::allocateContiguous(
    MachinePageCount numPages,
    Allocation* collateral,
    ContiguousAllocation& allocation,
    ReservationCallback reservationCB,
    MachinePageCount maxPages) {
  const MachinePageCount numCollateralPages =
      allocation.numPages() + (collateral ? collateral->numPages() : 0);
  const uint64_t totalCollateralBytes =
      AllocationTraits::pageBytes(numCollateralPages);
  auto cleanupCollateralAndReleaseReservation = [&](uint64_t reservationBytes) {
    if ((collateral != nullptr) && !collateral->empty()) {
      freeNonContiguous(*collateral);
    }
    if (!allocation.empty()) {
      freeContiguous(allocation);
    }
    if ((reservationCB) != nullptr && (reservationBytes > 0)) {
      reservationCB(reservationBytes, false);
    }
  };

  if (numPages == 0) {
    cleanupCollateralAndReleaseReservation(totalCollateralBytes);
    return true;
  }

  if (reservationCB != nullptr) {
    if (numPages >= numCollateralPages) {
      const int64_t numNeededPages = numPages - numCollateralPages;
      try {
        reservationCB(AllocationTraits::pageBytes(numNeededPages), true);
      } catch (const std::exception& e) {
        VELOX_MEM_LOG_EVERY_MS(WARNING, 1'000)
            << "Exceeded memory reservation limit when reserve "
            << numNeededPages << " new pages when allocate " << numPages
            << " pages, error: " << e.what();
        cleanupCollateralAndReleaseReservation(totalCollateralBytes);
        std::rethrow_exception(std::current_exception());
      }
    } else {
      const uint64_t numExtraPages = numCollateralPages - numPages;
      reservationCB(AllocationTraits::pageBytes(numExtraPages), false);
    }
  }

  const uint64_t totalBytesReserved = AllocationTraits::pageBytes(numPages);
  bool success = false;
  if (cache() == nullptr) {
    success = allocateContiguousWithoutRetry(
        numPages, collateral, allocation, maxPages);
  } else {
    success = cache()->makeSpace(
        pagesToAcquire(numPages, numCollateralPages),
        [&](Allocation& acquired) {
          freeNonContiguous(acquired);
          return allocateContiguousWithoutRetry(
              numPages, collateral, allocation, maxPages);
        });
  }

  if (!success) {
    // There can be a failure where allocation was never called because there
    // never was a chance based on numAllocated() and capacity(). Make sure old
    // data is still freed.
    cleanupCollateralAndReleaseReservation(totalBytesReserved);
  }
  return success;
}

bool MemoryAllocator::growContiguous(
    MachinePageCount increment,
    ContiguousAllocation& allocation,
    ReservationCallback reservationCB) {
  VELOX_CHECK_LE(
      allocation.size() + increment * AllocationTraits::kPageSize,
      allocation.maxSize());
  if (increment == 0) {
    return true;
  }
  if (reservationCB != nullptr) {
    // May throw. If it does, there is nothing to revert.
    reservationCB(AllocationTraits::pageBytes(increment), true);
  }
  bool success = false;
  if (cache() == nullptr) {
    success = growContiguousWithoutRetry(increment, allocation);
  } else {
    success = cache()->makeSpace(increment, [&](Allocation& acquired) {
      freeNonContiguous(acquired);
      return growContiguousWithoutRetry(increment, allocation);
    });
  }
  if (!success && reservationCB != nullptr) {
    reservationCB(AllocationTraits::pageBytes(increment), false);
  }
  return success;
}

void* MemoryAllocator::allocateBytes(uint64_t bytes, uint16_t alignment) {
  if (cache() == nullptr) {
    return allocateBytesWithoutRetry(bytes, alignment);
  }
  void* result = nullptr;
  cache()->makeSpace(
      AllocationTraits::numPages(bytes), [&](Allocation& acquired) {
        freeNonContiguous(acquired);
        result = allocateBytesWithoutRetry(bytes, alignment);
        return result != nullptr;
      });
  return result;
}

void* MemoryAllocator::allocateZeroFilled(uint64_t bytes) {
  if (cache() == nullptr) {
    return allocateZeroFilledWithoutRetry(bytes);
  }
  void* result = nullptr;
  cache()->makeSpace(
      AllocationTraits::numPages(bytes), [&](Allocation& acquired) {
        freeNonContiguous(acquired);
        result = allocateZeroFilledWithoutRetry(bytes);
        return result != nullptr;
      });
  return result;
}

void* MemoryAllocator::allocateZeroFilledWithoutRetry(uint64_t bytes) {
  void* result = allocateBytes(bytes);
  if (result != nullptr) {
    ::memset(result, 0, bytes);
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
  int64_t totalAllocations = 0;
  for (auto i = 0; i < sizes.size(); ++i) {
    totalClocks += sizes[i].clocks();
    totalBytes += sizes[i].totalBytes;
    totalAllocations += sizes[i].numAllocations;
  }
  out << fmt::format(
      "Alloc: {}MB {} Gigaclocks Allocations={}, advised={} MB\n",
      totalBytes >> 20,
      totalClocks >> 30,
      totalAllocations,
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
        "Size {}K: {}MB {} Megaclocks {} Allocations\n",
        sizes[i].size * 4,
        sizes[i].totalBytes >> 20,
        sizes[i].clocks() >> 20,
        sizes[i].numAllocations);
  }
  return out.str();
}

void MemoryAllocator::useHugePages(
    const ContiguousAllocation& data,
    bool enable) {
#ifdef linux
  if (!FLAGS_velox_memory_use_hugepages) {
    return;
  }
  auto maybeRange = data.hugePageRange();
  if (!maybeRange.has_value()) {
    return;
  }
  auto rc = ::madvise(
      maybeRange.value().data(),
      maybeRange.value().size(),
      enable ? MADV_HUGEPAGE : MADV_NOHUGEPAGE);
  if (rc != 0) {
    VELOX_MEM_LOG(WARNING) << "madvise hugepage errno="
                           << folly ::errnoStr(errno);
  }
#endif
}

void MemoryAllocator::setAllocatorFailureMessage(std::string message) {
  allocatorFailureMessage() = std::move(message);
}

std::string MemoryAllocator::getAndClearFailureMessage() {
  auto allocatorErrMsg = std::move(allocatorFailureMessage());
  allocatorFailureMessage().clear();
  if (cache()) {
    if (allocatorErrMsg.empty()) {
      return getAndClearCacheFailureMessage();
    }
    allocatorErrMsg =
        fmt::format("{} {}", allocatorErrMsg, getAndClearCacheFailureMessage());
  }
  return allocatorErrMsg;
}

namespace {
struct TraceState {
  struct rusage rusage;
  Stats allocatorStats;
  int64_t ioTotal;
  struct timeval tv;
};

int64_t toUsec(struct timeval tv) {
  return tv.tv_sec * 1000000LL + tv.tv_usec;
}

int32_t elapsedUsec(struct timeval end, struct timeval begin) {
  return toUsec(end) - toUsec(begin);
}
} // namespace

void MemoryAllocator::getTracingHooks(
    std::function<void()>& init,
    std::function<std::string()>& report,
    std::function<int64_t()> ioVolume) {
  auto allocator = shared_from_this();
  auto state = std::make_shared<TraceState>();
  init = [state, allocator, ioVolume]() {
    getrusage(RUSAGE_SELF, &state->rusage);
    struct timezone tz;
    gettimeofday(&state->tv, &tz);
    state->allocatorStats = allocator->stats();
    state->ioTotal = ioVolume ? ioVolume() : 0;
  };
  report = [state, allocator, ioVolume]() -> std::string {
    struct rusage rusage;
    getrusage(RUSAGE_SELF, &rusage);
    auto newStats = allocator->stats();
    float u = elapsedUsec(rusage.ru_utime, state->rusage.ru_utime);
    float s = elapsedUsec(rusage.ru_stime, state->rusage.ru_stime);
    auto m = allocator->stats() - state->allocatorStats;
    float flt = rusage.ru_minflt - state->rusage.ru_minflt;
    struct timeval tv;
    struct timezone tz;
    gettimeofday(&tv, &tz);
    float elapsed = elapsedUsec(tv, state->tv);
    int64_t io = 0;
    if (ioVolume) {
      io = ioVolume() - state->ioTotal;
    }
    std::stringstream out;
    out << std::endl
        << std::endl
        << fmt::format(
               "user%={} sys%={} minflt/s={}, io={} MB/s\n",
               100 * u / elapsed,
               100 * s / elapsed,
               flt / (elapsed / 1000000),
               io / (elapsed));
    out << m.toString() << std::endl;
    out << allocator->toString() << std::endl;
    return out.str();
  };
}

} // namespace facebook::velox::memory
