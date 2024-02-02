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

#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/Memory.h"

#include <sys/mman.h>

namespace facebook::velox::memory {
MallocAllocator::MallocAllocator(size_t capacity, uint32_t reservationByteLimit)
    : kind_(MemoryAllocator::Kind::kMalloc),
      capacity_(capacity),
      reservationByteLimit_(reservationByteLimit),
      reserveFunc_(
          [this](uint32_t& counter, uint32_t increment, std::mutex& lock) {
            return incrementUsageWithReservationFunc(counter, increment, lock);
          }),
      releaseFunc_(
          [&](uint32_t& counter, uint32_t decrement, std::mutex& lock) {
            decrementUsageWithReservationFunc(counter, decrement, lock);
            return true;
          }),
      reservations_(std::thread::hardware_concurrency()) {}

MallocAllocator::~MallocAllocator() {
  // TODO: Remove the check when memory leak issue is resolved.
  if (FLAGS_velox_memory_leak_check_enabled) {
    VELOX_CHECK(
        ((allocatedBytes_ - reservations_.read()) == 0) &&
            (numAllocated_ == 0) && (numMapped_ == 0),
        "{}",
        toString());
  }
}

bool MallocAllocator::allocateNonContiguousWithoutRetry(
    MachinePageCount numPages,
    Allocation& out,
    ReservationCallback reservationCB,
    MachinePageCount minSizeClass) {
  const uint64_t freedBytes = freeNonContiguous(out);
  if (numPages == 0) {
    if (freedBytes != 0 && reservationCB != nullptr) {
      reservationCB(freedBytes, false);
    }
    return true;
  }
  const SizeMix mix = allocationSize(numPages, minSizeClass);
  const auto totalBytes = AllocationTraits::pageBytes(mix.totalPages);
  if (testingHasInjectedFailure(InjectedFailure::kCap) ||
      !incrementUsage(totalBytes)) {
    if (freedBytes != 0 && reservationCB != nullptr) {
      reservationCB(freedBytes, false);
    }
    const auto errorMsg = fmt::format(
        "Exceeded memory allocator limit when allocating {} new pages for "
        "total allocation of {} pages, the memory allocator capacity is {}",
        mix.totalPages,
        numPages,
        succinctBytes(capacity_));
    VELOX_MEM_LOG_EVERY_MS(WARNING, 1000) << errorMsg;
    setAllocatorFailureMessage(errorMsg);
    return false;
  }

  uint64_t bytesToAllocate = 0;
  if (reservationCB != nullptr) {
    bytesToAllocate = AllocationTraits::pageBytes(mix.totalPages) - freedBytes;
    try {
      reservationCB(bytesToAllocate, true);
    } catch (std::exception&) {
      VELOX_MEM_LOG(WARNING)
          << "Failed to reserve " << succinctBytes(bytesToAllocate)
          << " for non-contiguous allocation of " << numPages
          << " pages, then release " << succinctBytes(freedBytes)
          << " from the old allocation";
      // If the new memory reservation fails, we need to release the memory
      // reservation of the freed memory of previously allocation.
      reservationCB(freedBytes, false);
      decrementUsage(totalBytes);
      std::rethrow_exception(std::current_exception());
    }
  }

  std::vector<void*> buffers;
  buffers.reserve(mix.numSizes);
  for (int32_t i = 0; i < mix.numSizes; ++i) {
    MachinePageCount numSizeClassPages =
        mix.sizeCounts[i] * sizeClassSizes_[mix.sizeIndices[i]];
    void* ptr = nullptr;
    // Trigger allocation failure by skipping malloc
    if (!testingHasInjectedFailure(InjectedFailure::kAllocate)) {
      stats_.recordAllocate(
          AllocationTraits::pageBytes(sizeClassSizes_[mix.sizeIndices[i]]),
          mix.sizeCounts[i],
          [&]() {
            ptr = ::malloc(
                AllocationTraits::pageBytes(numSizeClassPages)); // NOLINT
          });
    }
    if (ptr == nullptr) {
      // Failed to allocate memory from memory.
      const auto errorMsg = fmt::format(
          "Malloc failed to allocate {} of memory while allocating for "
          "non-contiguous allocation of {} pages",
          succinctBytes(AllocationTraits::pageBytes(numSizeClassPages)),
          numPages);
      VELOX_MEM_LOG(WARNING) << errorMsg;
      setAllocatorFailureMessage(errorMsg);
      break;
    }
    buffers.push_back(ptr);
    out.append(reinterpret_cast<uint8_t*>(ptr), numSizeClassPages); // NOLINT
  }

  if (buffers.size() != mix.numSizes) {
    // Failed to allocate memory using malloc. Free any malloced pages and
    // return false.
    for (auto* buffer : buffers) {
      ::free(buffer);
    }
    out.clear();
    if (reservationCB != nullptr) {
      VELOX_MEM_LOG(WARNING)
          << "Failed to allocate memory for non-contiguous allocation of "
          << numPages << " pages, then release "
          << succinctBytes(bytesToAllocate + freedBytes)
          << " of memory reservation including the old allocation";
      reservationCB(bytesToAllocate + freedBytes, false);
    }
    decrementUsage(totalBytes);
    return false;
  }

  // Successfully allocated all pages.
  numAllocated_.fetch_add(mix.totalPages);
  return true;
}

bool MallocAllocator::allocateContiguousWithoutRetry(
    MachinePageCount numPages,
    Allocation* collateral,
    ContiguousAllocation& allocation,
    ReservationCallback reservationCB,
    MachinePageCount maxPages) {
  bool result;
  stats_.recordAllocate(AllocationTraits::pageBytes(numPages), 1, [&]() {
    result = allocateContiguousImpl(
        numPages, collateral, allocation, reservationCB, maxPages);
  });
  return result;
}

bool MallocAllocator::allocateContiguousImpl(
    MachinePageCount numPages,
    Allocation* collateral,
    ContiguousAllocation& allocation,
    ReservationCallback reservationCB,
    MachinePageCount maxPages) {
  if (maxPages == 0) {
    maxPages = numPages;
  } else {
    VELOX_CHECK_LE(numPages, maxPages);
  }
  MachinePageCount numCollateralPages = 0;
  if (collateral != nullptr) {
    numCollateralPages =
        freeNonContiguous(*collateral) / AllocationTraits::kPageSize;
  }
  auto numContiguousCollateralPages = allocation.numPages();
  if (numContiguousCollateralPages > 0) {
    useHugePages(allocation, false);
    if (::munmap(allocation.data(), allocation.maxSize()) < 0) {
      VELOX_MEM_LOG(ERROR) << "munmap got " << folly::errnoStr(errno) << "for "
                           << allocation.data() << ", " << allocation.size();
    }
    numMapped_.fetch_sub(numContiguousCollateralPages);
    numAllocated_.fetch_sub(numContiguousCollateralPages);
    decrementUsage(AllocationTraits::pageBytes(numContiguousCollateralPages));
    allocation.clear();
  }
  const auto totalCollateralPages =
      numCollateralPages + numContiguousCollateralPages;
  const auto totalCollateralBytes =
      AllocationTraits::pageBytes(totalCollateralPages);
  if (numPages == 0) {
    if (totalCollateralBytes != 0 && reservationCB != nullptr) {
      reservationCB(totalCollateralBytes, false);
    }
    return true;
  }

  const auto totalBytes = AllocationTraits::pageBytes(numPages);
  if (testingHasInjectedFailure(InjectedFailure::kCap) ||
      !incrementUsage(totalBytes)) {
    if (totalCollateralBytes != 0 && reservationCB != nullptr) {
      reservationCB(totalCollateralBytes, false);
    }
    const auto errorMsg = fmt::format(
        "Exceeded memory allocator limit when allocating {} new pages, the "
        "memory allocator capacity is {}",
        numPages,
        succinctBytes(capacity_));
    setAllocatorFailureMessage(errorMsg);
    VELOX_MEM_LOG_EVERY_MS(WARNING, 1000) << errorMsg;
    return false;
  }
  const int64_t numNeededPages = numPages - totalCollateralPages;
  if (reservationCB != nullptr) {
    try {
      reservationCB(AllocationTraits::pageBytes(numNeededPages), true);
    } catch (std::exception&) {
      // If the new memory reservation fails, we need to release the memory
      // reservation of the freed contiguous and non-contiguous memory.
      VELOX_MEM_LOG(WARNING)
          << "Failed to reserve " << AllocationTraits::pageBytes(numNeededPages)
          << " bytes for contiguous allocation of " << numPages
          << " pages, then release " << succinctBytes(totalCollateralBytes)
          << " from the old allocations";
      reservationCB(totalCollateralBytes, false);
      decrementUsage(totalBytes);
      std::rethrow_exception(std::current_exception());
    }
  }
  numAllocated_.fetch_add(numPages);
  numMapped_.fetch_add(numPages);
  void* data = ::mmap(
      nullptr,
      AllocationTraits::pageBytes(maxPages),
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS,
      -1,
      0);
  // TODO: add handling of MAP_FAILED.
  allocation.set(
      data,
      AllocationTraits::pageBytes(numPages),
      AllocationTraits::pageBytes(maxPages));
  useHugePages(allocation, true);
  return true;
}

int64_t MallocAllocator::freeNonContiguous(Allocation& allocation) {
  if (allocation.empty()) {
    return 0;
  }
  MachinePageCount freedPages{0};
  for (int32_t i = 0; i < allocation.numRuns(); ++i) {
    Allocation::PageRun run = allocation.runAt(i);
    void* ptr = run.data();
    const int64_t numPages = run.numPages();
    freedPages += numPages;
    stats_.recordFree(AllocationTraits::pageBytes(numPages), [&]() {
      ::free(ptr); // NOLINT
    });
  }

  const auto freedBytes = AllocationTraits::pageBytes(freedPages);
  decrementUsage(freedBytes);
  numAllocated_.fetch_sub(freedPages);
  allocation.clear();
  return freedBytes;
}

void MallocAllocator::freeContiguous(ContiguousAllocation& allocation) {
  stats_.recordFree(
      allocation.size(), [&]() { freeContiguousImpl(allocation); });
}

void MallocAllocator::freeContiguousImpl(ContiguousAllocation& allocation) {
  if (allocation.empty()) {
    return;
  }
  useHugePages(allocation, false);
  const auto bytes = allocation.size();
  const auto numPages = allocation.numPages();
  if (::munmap(allocation.data(), allocation.maxSize()) < 0) {
    VELOX_MEM_LOG(ERROR) << "Error for munmap(" << allocation.data() << ", "
                         << succinctBytes(bytes) << "): '"
                         << folly::errnoStr(errno) << "'";
  }
  numMapped_.fetch_sub(numPages);
  numAllocated_.fetch_sub(numPages);
  decrementUsage(bytes);
  allocation.clear();
}

bool MallocAllocator::growContiguousWithoutRetry(
    MachinePageCount increment,
    ContiguousAllocation& allocation,
    ReservationCallback reservationCB) {
  VELOX_CHECK_LE(
      allocation.size() + increment * AllocationTraits::kPageSize,
      allocation.maxSize());
  if (reservationCB != nullptr) {
    // May throw. If does, there is nothing to revert.
    reservationCB(AllocationTraits::pageBytes(increment), true);
  }
  if (!incrementUsage(AllocationTraits::pageBytes(increment))) {
    const auto errorMsg = fmt::format(
        "Exceeded memory allocator limit when allocating {} new pages for "
        "total allocation of {} pages, the memory allocator capacity is {}",
        increment,
        allocation.numPages(),
        succinctBytes(capacity_));
    setAllocatorFailureMessage(errorMsg);
    VELOX_MEM_LOG_EVERY_MS(WARNING, 1000) << errorMsg;
    if (reservationCB != nullptr) {
      reservationCB(AllocationTraits::pageBytes(increment), false);
    }
    return false;
  }
  numAllocated_ += increment;
  numMapped_ += increment;
  allocation.set(
      allocation.data(),
      allocation.size() + AllocationTraits::kPageSize * increment,
      allocation.maxSize());
  return true;
}

void* MallocAllocator::allocateBytesWithoutRetry(
    uint64_t bytes,
    uint16_t alignment) {
  if (!incrementUsage(bytes)) {
    auto errorMsg = fmt::format(
        "Failed to allocateBytes {}: Exceeded memory allocator "
        "limit of {}",
        succinctBytes(bytes),
        succinctBytes(capacity_));
    VELOX_MEM_LOG_EVERY_MS(WARNING, 1000) << errorMsg;
    setAllocatorFailureMessage(errorMsg);
    return nullptr;
  }
  if (!isAlignmentValid(bytes, alignment)) {
    decrementUsage(bytes);
    VELOX_FAIL(
        "Alignment check failed, allocateBytes {}, alignmentBytes {}",
        bytes,
        alignment);
  }
  void* result = (alignment > kMinAlignment) ? ::aligned_alloc(alignment, bytes)
                                             : ::malloc(bytes);
  if (FOLLY_UNLIKELY(result == nullptr)) {
    VELOX_MEM_LOG(ERROR) << "Failed to allocateBytes " << succinctBytes(bytes)
                         << " with " << alignment << " alignment";
  }
  return result;
}

void* MallocAllocator::allocateZeroFilledWithoutRetry(uint64_t bytes) {
  if (!incrementUsage(bytes)) {
    auto errorMsg = fmt::format(
        "Failed to allocateZeroFilled {}: Exceeded memory allocator "
        "limit of {}",
        succinctBytes(bytes),
        succinctBytes(capacity_));
    VELOX_MEM_LOG_EVERY_MS(WARNING, 1000) << errorMsg;
    setAllocatorFailureMessage(errorMsg);
    return nullptr;
  }
  void* result = std::calloc(1, bytes);
  if (FOLLY_UNLIKELY(result == nullptr)) {
    VELOX_MEM_LOG(ERROR) << "Failed to allocateZeroFilled "
                         << succinctBytes(bytes);
  }
  return result;
}

void MallocAllocator::freeBytes(void* p, uint64_t bytes) noexcept {
  ::free(p); // NOLINT
  decrementUsage(bytes);
}

bool MallocAllocator::checkConsistency() const {
  const auto allocatedBytes = allocatedBytes_.load();
  return allocatedBytes >= 0 && allocatedBytes <= capacity_;
}

std::string MallocAllocator::toString() const {
  std::stringstream out;
  out << "Memory Allocator[" << kindString(kind_) << " capacity "
      << ((capacity_ == kMaxMemory) ? "UNLIMITED" : succinctBytes(capacity_))
      << " allocated bytes " << allocatedBytes_ << " allocated pages "
      << numAllocated_ << " mapped pages " << numMapped_ << "]";
  return out.str();
}
} // namespace facebook::velox::memory
