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
    const SizeMix& sizeMix,
    Allocation& out) {
  freeNonContiguous(out);
  if (sizeMix.totalPages == 0) {
    return true;
  }
  const auto totalBytes = AllocationTraits::pageBytes(sizeMix.totalPages);
  if (testingHasInjectedFailure(InjectedFailure::kCap) ||
      !incrementUsage(totalBytes)) {
    const auto errorMsg = fmt::format(
        "Exceeded memory allocator limit when allocating {} new pages"
        ", the memory allocator capacity is {}",
        sizeMix.totalPages,
        succinctBytes(capacity_));
    VELOX_MEM_LOG_EVERY_MS(WARNING, 1000) << errorMsg;
    setAllocatorFailureMessage(errorMsg);
    return false;
  }

  std::vector<void*> buffers;
  buffers.reserve(sizeMix.numSizes);
  for (int32_t i = 0; i < sizeMix.numSizes; ++i) {
    MachinePageCount numSizeClassPages =
        sizeMix.sizeCounts[i] * sizeClassSizes_[sizeMix.sizeIndices[i]];
    void* ptr = nullptr;
    // Trigger allocation failure by skipping malloc
    if (!testingHasInjectedFailure(InjectedFailure::kAllocate)) {
      stats_.recordAllocate(
          AllocationTraits::pageBytes(sizeClassSizes_[sizeMix.sizeIndices[i]]),
          sizeMix.sizeCounts[i],
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
          sizeMix.totalPages);
      VELOX_MEM_LOG(WARNING) << errorMsg;
      setAllocatorFailureMessage(errorMsg);
      break;
    }
    buffers.push_back(ptr);
    out.append(reinterpret_cast<uint8_t*>(ptr), numSizeClassPages); // NOLINT
  }

  if (buffers.size() != sizeMix.numSizes) {
    // Failed to allocate memory using malloc. Free any malloced pages and
    // return false.
    for (auto* buffer : buffers) {
      ::free(buffer);
    }
    out.clear();
    VELOX_MEM_LOG(WARNING)
        << "Failed to allocate memory for non-contiguous allocation of "
        << sizeMix.totalPages << " pages";
    decrementUsage(totalBytes);
    return false;
  }

  // Successfully allocated all pages.
  numAllocated_.fetch_add(sizeMix.totalPages);
  return true;
}

bool MallocAllocator::allocateContiguousWithoutRetry(
    MachinePageCount numPages,
    Allocation* collateral,
    ContiguousAllocation& allocation,
    MachinePageCount maxPages) {
  bool result;
  stats_.recordAllocate(AllocationTraits::pageBytes(numPages), 1, [&]() {
    result = allocateContiguousImpl(numPages, collateral, allocation, maxPages);
  });
  return result;
}

bool MallocAllocator::allocateContiguousImpl(
    MachinePageCount numPages,
    Allocation* collateral,
    ContiguousAllocation& allocation,
    MachinePageCount maxPages) {
  if (maxPages == 0) {
    maxPages = numPages;
  } else {
    VELOX_CHECK_LE(numPages, maxPages);
  }
  if (collateral != nullptr) {
    freeNonContiguous(*collateral);
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
  if (numPages == 0) {
    return true;
  }

  const auto totalBytes = AllocationTraits::pageBytes(numPages);
  if (testingHasInjectedFailure(InjectedFailure::kCap) ||
      !incrementUsage(totalBytes)) {
    const auto errorMsg = fmt::format(
        "Exceeded memory allocator limit when allocating {} new pages, the "
        "memory allocator capacity is {}",
        numPages,
        succinctBytes(capacity_));
    setAllocatorFailureMessage(errorMsg);
    VELOX_MEM_LOG_EVERY_MS(WARNING, 1000) << errorMsg;
    return false;
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
    ContiguousAllocation& allocation) {
  if (!incrementUsage(AllocationTraits::pageBytes(increment))) {
    const auto errorMsg = fmt::format(
        "Exceeded memory allocator limit when allocating {} new pages for "
        "total allocation of {} pages, the memory allocator capacity is {}",
        increment,
        allocation.numPages(),
        succinctBytes(capacity_));
    setAllocatorFailureMessage(errorMsg);
    VELOX_MEM_LOG_EVERY_MS(WARNING, 1000) << errorMsg;
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
