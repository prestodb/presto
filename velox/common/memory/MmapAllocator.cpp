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

#include "velox/common/memory/MmapAllocator.h"
#include "velox/common/base/BitUtil.h"

#include <sys/mman.h>

namespace facebook::velox::memory {

MmapAllocator::MmapAllocator(const MmapAllocatorOptions& options)
    : MappedMemory(),
      numAllocated_(0),
      numMapped_(0),

      capacity_(bits::roundUp(
          options.capacity / kPageSize,
          64 * sizeClassSizes_.back())) {
  for (int size : sizeClassSizes_) {
    sizeClasses_.push_back(std::make_unique<SizeClass>(capacity_ / size, size));
  }
}

bool MmapAllocator::allocate(
    MachinePageCount numPages,
    int32_t owner,
    Allocation& out,
    std::function<void(int64_t)> beforeAllocCB,
    MachinePageCount minSizeClass) {
  auto numFreed = freeInternal(out);
  if (numFreed != 0) {
    numAllocated_.fetch_sub(numFreed);
  }
  auto mix = allocationSize(numPages, minSizeClass);
  if (numAllocated_ + mix.totalPages > capacity_) {
    return false;
  }
  if (numAllocated_.fetch_add(mix.totalPages) + mix.totalPages > capacity_) {
    numAllocated_.fetch_sub(mix.totalPages);
    return false;
  }
  ++numAllocations_;
  numAllocatedPages_ += mix.totalPages;
  if (beforeAllocCB) {
    try {
      beforeAllocCB(mix.totalPages * kPageSize);
    } catch (const std::exception& e) {
      numAllocated_.fetch_sub(mix.totalPages);
      std::rethrow_exception(std::current_exception());
    }
  }
  MachinePageCount newMapsNeeded = 0;
  for (int i = 0; i < mix.numSizes; ++i) {
    bool success;
    stats_.recordAllocate(
        sizeClassSizes_[mix.sizeIndices[i]] * kPageSize,
        mix.sizeCounts[i],
        [&]() {
          success = sizeClasses_[mix.sizeIndices[i]]->allocate(
              mix.sizeCounts[i], owner, newMapsNeeded, out);
        });
    if (!success) {
      // This does not normally happen since any size class can accommodate
      // all the capacity. 'allocatedPages_' must be out of sync.
      LOG(WARNING) << "Failed allocation in size class " << i << " for "
                   << mix.sizeCounts[i] << " pages";
      auto failedPages = mix.totalPages - out.numPages();
      free(out);
      numAllocated_.fetch_sub(failedPages);
      return false;
    }
  }
  if (newMapsNeeded == 0) {
    return true;
  }
  if (ensureEnoughMappedPages(newMapsNeeded)) {
    markAllMapped(out);
    return true;
  }
  free(out);
  return false;
}

bool MmapAllocator::ensureEnoughMappedPages(int32_t newMappedNeeded) {
  std::lock_guard<std::mutex> l(sizeClassBalanceMutex_);
  if (injectedFailure_ == Failure::kMadvise) {
    // Mimic case of not finding anything to advise away.
    injectedFailure_ = Failure::kNone;
    return false;
  }
  int totalMaps = numMapped_.fetch_add(newMappedNeeded) + newMappedNeeded;
  if (totalMaps <= capacity_) {
    // We are not at capacity. No need to advise away.
    return true;
  }
  // We need to advise away a number of pages or we fail the alloc.
  int target = totalMaps - capacity_;
  int numAdvised = adviseAway(target);
  numAdvisedPages_ += numAdvised;
  if (numAdvised >= target) {
    numMapped_.fetch_sub(numAdvised);
    return true;
  }
  numMapped_.fetch_sub(numAdvised + newMappedNeeded);
  return false;
}

int64_t MmapAllocator::free(Allocation& allocation) {
  auto numFreed = freeInternal(allocation);
  numAllocated_.fetch_sub(numFreed);
  return numFreed * kPageSize;
}
MachinePageCount MmapAllocator::freeInternal(Allocation& allocation) {
  if (allocation.numRuns() == 0) {
    return 0;
  }
  MachinePageCount numFreed = 0;

  for (auto i = 0; i < sizeClasses_.size(); ++i) {
    auto& sizeClass = sizeClasses_[i];
    int32_t pages = 0;
    uint64_t clocks = 0;
    {
      ClockTimer timer(clocks);
      pages = sizeClass->free(allocation);
    }
    if (pages && FLAGS_velox_time_allocations) {
      // Increment the free time only if the allocation contained
      // pages in the class. Note that size class indices in the
      // allocator are not necessarily the same as in the stats.
      auto sizeIndex = Stats::sizeIndex(sizeClassSizes_[i] * kPageSize);
      stats_.sizes[sizeIndex].freeClocks += clocks;
    }
    numFreed += pages;
  }
  allocation.clear();
  return numFreed;
}

bool MmapAllocator::allocateContiguousImpl(
    MachinePageCount numPages,
    MmapAllocator::Allocation* FOLLY_NULLABLE collateral,
    MmapAllocator::ContiguousAllocation& allocation,
    std::function<void(int64_t)> beforeAllocCB) {
  MachinePageCount numCollateralPages = 0;
  // 'collateral' and 'allocation' get freed anyway. But the counters
  // are not updated to reflect this. Rather, we add the delta that is
  // needed on top of collaterals to the allocation and mapped
  // counters. In this way another thread will not see the temporary
  // dip in allocation and we are sure to succeed if 'collateral' and
  // 'allocation' together cover 'numPages'. If we need more space and
  // fail to get this, then we subtract 'collateral' and 'allocation'
  // from the counters.
  //
  // Specifically, we do not subtract anything from counters with a
  // resource reservation semantic, i.e. 'numAllocated_' and
  // 'numMapped_' except at the end where the outcome of the
  // operation is clear. Otherwise we could not have the guarantee
  // that the operation succeeds if 'collateral' and 'allocation'
  // cover the new size, as other threads might grab the transiently
  // free pages.
  if (collateral) {
    numCollateralPages = freeInternal(*collateral);
  }
  int64_t numLargeCollateralPages = allocation.numPages();
  if (numLargeCollateralPages) {
    if (munmap(allocation.data(), allocation.size()) < 0) {
      LOG(ERROR) << "munmap got " << errno << "for " << allocation.data()
                 << ", " << allocation.size();
    }
    allocation.reset(nullptr, nullptr, 0);
  }
  auto totalCollateralPages = numCollateralPages + numLargeCollateralPages;
  auto numCollateralUnmap = numLargeCollateralPages;
  int64_t newPages = numPages - totalCollateralPages;
  if (beforeAllocCB) {
    try {
      beforeAllocCB(newPages * kPageSize);
    } catch (const std::exception& e) {
      numAllocated_ -= totalCollateralPages;
      // We failed to grow by 'newPages. So we record the freeing off
      // the whole collaterall and the unmap of former 'allocation'.
      try {
        beforeAllocCB(-static_cast<int64_t>(totalCollateralPages) * kPageSize);
      } catch (const std::exception& inner) {
      };
      numMapped_ -= numCollateralUnmap;
      numExternalMapped_ -= numCollateralUnmap;
      throw;
    }
  }

  // Rolls back the counters on failure. 'mappedDecrement is subtracted from
  // 'numMapped_' on top of other adjustment.
  auto rollbackAllocation = [&](int64_t mappedDecrement) {
    // The previous allocation and collateral were both freed but not counted as
    // freed.
    numAllocated_ -= numPages;
    try {
      beforeAllocCB(-numPages * kPageSize);
    } catch (const std::exception& e) {
      // Ignore exception, this is run on failure return path.
    }
    // was incremented by numPages - numLargeCollateralPages. On failure,
    // numLargeCollateralPages are freed and numPages - numLargeCollateralPages
    // were never allocated.
    numExternalMapped_ -= numPages;
    numMapped_ -= numCollateralUnmap + mappedDecrement;
  };
  numExternalMapped_ += numPages - numCollateralUnmap;
  auto numAllocated = numAllocated_.fetch_add(newPages) + newPages;
  // Check if went over the limit. But a net decrease always succeeds even if
  // ending up over the limit because some other thread might be transiently
  // over the limit.
  if (newPages > 0 && numAllocated > capacity_) {
    rollbackAllocation(0);
    return false;
  }
  // Make sure there are free backing pages for the size minus what we just
  // unmapped.
  int64_t numToMap = numPages - numCollateralUnmap;
  if (numToMap > 0) {
    if (!ensureEnoughMappedPages(numToMap)) {
      LOG(WARNING) << "Could not advise away  enough for " << numToMap
                   << " pages for allocateContiguous";
      rollbackAllocation(0);
      return false;
    }
  } else {
    // We exchange a large mmap for a smaller one. Add negative delta.
    numMapped_ += numToMap;
  }
  void* data;
  if (injectedFailure_ == Failure::kMmap) {
    // Mimic running out of mmaps for process.
    injectedFailure_ = Failure::kNone;
    data = nullptr;
  } else {
    data = mmap(
        nullptr,
        numPages * kPageSize,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS,
        -1,
        0);
  }
  if (!data) {
    // If the mmap failed, we have unmapped former 'allocation' and
    // the extra to be mapped.
    rollbackAllocation(numToMap);
    return false;
  }

  allocation.reset(this, data, numPages * kPageSize);
  return true;
}

void MmapAllocator::freeContiguousImpl(ContiguousAllocation& allocation) {
  if (allocation.data() && allocation.size()) {
    if (munmap(allocation.data(), allocation.size()) < 0) {
      LOG(ERROR) << "munmap returned " << errno << "for " << allocation.data()
                 << ", " << allocation.size();
    }
    numMapped_ -= allocation.numPages();
    numExternalMapped_ -= allocation.numPages();
    numAllocated_ -= allocation.numPages();
    allocation.reset(nullptr, nullptr, 0);
  }
}

void MmapAllocator::markAllMapped(const Allocation& allocation) {
  for (auto& sizeClass : sizeClasses_) {
    sizeClass->setAllMapped(allocation, true);
  }
}

MachinePageCount MmapAllocator::adviseAway(MachinePageCount target) {
  int numAway = 0;
  for (int i = sizeClasses_.size() - 1; i >= 0; --i) {
    numAway += sizeClasses_[i]->adviseAway(target - numAway, this);
    if (numAway >= target) {
      break;
    }
  }
  return numAway;
}

MmapAllocator::SizeClass::SizeClass(size_t capacity, MachinePageCount unitSize)
    : capacity_(capacity),
      unitSize_(unitSize),
      byteSize_(capacity_ * unitSize_ * kPageSize),
      pageAllocated_(capacity_ / 64),
      pageMapped_(capacity_ / 64) {
  VELOX_CHECK(
      capacity_ % 64 == 0, "Sizeclass must have a multiple of 64 capacity.");
  void* ptr = mmap(
      nullptr,
      capacity_ * unitSize_ * kPageSize,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS,
      -1,
      0);
  if (ptr == MAP_FAILED || !ptr) {
    LOG(ERROR) << "mmap failed with " << errno;
    VELOX_FAIL(
        "Could not allocate working memory"
        "mmap failed with {}",
        errno);
  }
  address_ = reinterpret_cast<uint8_t*>(ptr);
}

MmapAllocator::SizeClass::~SizeClass() {
  munmap(address_, byteSize_);
}

ClassPageCount MmapAllocator::SizeClass::checkConsistency(
    ClassPageCount& numMapped) const {
  int count = 0;
  int mappedCount = 0;
  int mappedFreeCount = 0;
  for (int i = 0; i < pageAllocated_.size(); ++i) {
    count += __builtin_popcountll(pageAllocated_[i]);
    mappedCount += __builtin_popcountll(pageMapped_[i]);
    mappedFreeCount +=
        __builtin_popcountll(~pageAllocated_[i] & pageMapped_[i]);
  }
  if (mappedFreeCount != numMappedFreePages_) {
    LOG(WARNING) << "Mismatched count of mapped free pages in size class "
                 << unitSize_ << ". Actual= " << mappedFreeCount
                 << " vs recorded= " << numMappedFreePages_
                 << ". Total mapped=" << mappedCount;
  }
  numMapped = mappedCount;
  return count;
}

std::string MmapAllocator::SizeClass::toString() const {
  std::stringstream out;
  int count = 0;
  int mappedCount = 0;
  int mappedFreeCount = 0;
  for (int i = 0; i < pageAllocated_.size(); ++i) {
    count += __builtin_popcountll(pageAllocated_[i]);
    mappedCount += __builtin_popcountll(pageMapped_[i]);
    mappedFreeCount +=
        __builtin_popcountll(~pageAllocated_[i] & pageMapped_[i]);
  }
  auto mb = (count * MappedMemory::kPageSize * unitSize_) >> 20;
  out << "[size " << unitSize_ << ": " << count << "(" << mb << "MB) allocated "
      << mappedCount << " mapped";
  if (mappedFreeCount != numMappedFreePages_) {
    out << "Mismatched count of mapped free pages "
        << ". Actual= " << mappedFreeCount
        << " vs recorded= " << numMappedFreePages_
        << ". Total mapped=" << mappedCount;
  }
  out << "]";
  return out.str();
}

bool MmapAllocator::SizeClass::allocate(
    ClassPageCount numPages,
    int32_t owner,
    MachinePageCount& numUnmapped,
    MmapAllocator::Allocation& out) {
  std::lock_guard<std::mutex> l(mutex_);
  return allocateLocked(numPages, owner, &numUnmapped, out);
}

bool MmapAllocator::SizeClass::allocateLocked(
    const ClassPageCount numPages,
    int32_t /* unused */,
    MachinePageCount* FOLLY_NULLABLE numUnmapped,
    MmapAllocator::Allocation& out) {
  size_t numWords = pageAllocated_.size();
  uint32_t cursor = clockHand_ += 64;
  if (clockHand_ > numWords) {
    clockHand_ = clockHand_ % numWords;
  }
  cursor = cursor % numWords;
  int numWordsTried = 0;
  int considerMappedOnly = std::min(numMappedFreePages_, numPages);
  auto numPagesToGo = numPages;
  for (;;) {
    if (++cursor >= numWords) {
      cursor = 0;
    }
    if (++numWordsTried > numWords) {
      return false;
    }
    uint64_t bits = pageAllocated_[cursor];
    if (bits != kAllSet) {
      if (considerMappedOnly > 0) {
        uint64_t mapped = pageMapped_[cursor];
        uint64_t mappedFree = ~bits & mapped;
        if (mappedFree == 0) {
          continue;
        }
        int previousToGo = numPagesToGo;
        allocateMapped(cursor, mappedFree, numPagesToGo, out);
        numAllocatedMapped_ += previousToGo - numPagesToGo;
        considerMappedOnly -= previousToGo - numPagesToGo;
        if (!considerMappedOnly && numPagesToGo) {
          // We move from allocating mapped to allocating
          // any. Previously skipped words are again eligible.
          VELOX_CHECK_NOT_NULL(numUnmapped, "numUnmapped is not set");

          numWordsTried = 0;
        }
      } else {
        int previousToGo = numPagesToGo;
        assert(numUnmapped != nullptr);
        allocateAny(cursor, numPagesToGo, *numUnmapped, out);
        numAllocatedUnmapped_ += previousToGo - numPagesToGo;
      }
      if (numPagesToGo == 0) {
        return true;
      }
    }
  }
}

MachinePageCount MmapAllocator::SizeClass::adviseAway(
    MachinePageCount numPages,
    MmapAllocator* allocator) {
  // Allocate as many mapped free pages as needed and advise them away.
  ClassPageCount target = bits::roundUp(numPages, unitSize_) / unitSize_;
  Allocation allocation(allocator);
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!numMappedFreePages_) {
      return 0;
    }
    target = std::min(target, numMappedFreePages_);
    allocateLocked(target, kNoOwner, nullptr, allocation);
    VELOX_CHECK(allocation.numPages() == target * unitSize_);
    numAllocatedMapped_ -= target;
    numAdvisedAway_ += target;
  }
  // Outside of 'mutex_'.
  adviseAway(allocation);
  free(allocation);
  allocation.clear();
  return unitSize_ * target;
}

bool MmapAllocator::SizeClass::isInRange(uint8_t* ptr) const {
  if (ptr >= address_ && ptr < address_ + byteSize_) {
    // See that ptr falls on a page boundary.
    if ((ptr - address_) % unitSize_ != 0) {
      VELOX_FAIL("Pointer is in a SizeClass but not at page boundary");
    }
    return true;
  }
  return false;
}

void MmapAllocator::SizeClass::setAllMapped(
    const Allocation& allocation,
    bool value) {
  for (int i = 0; i < allocation.numRuns(); ++i) {
    MmapAllocator::PageRun run = allocation.runAt(i);
    if (!isInRange(run.data())) {
      continue;
    }
    std::lock_guard<std::mutex> l(mutex_);
    setMappedBits(run, value);
  }
}

void MmapAllocator::SizeClass::adviseAway(const Allocation& allocation) {
  for (int i = 0; i < allocation.numRuns(); ++i) {
    PageRun run = allocation.runAt(i);
    if (!isInRange(run.data())) {
      continue;
    }
    if (madvise(run.data(), run.numPages() * kPageSize, MADV_DONTNEED) < 0) {
      LOG(WARNING) << "madvise got errno " << errno;
    } else {
      std::lock_guard<std::mutex> l(mutex_);
      setMappedBits(run, false);
    }
  }
}

void MmapAllocator::SizeClass::setMappedBits(
    const MappedMemory::PageRun run,
    bool value) {
  const uint8_t* runAddress = run.data();
  const int firstBit = (runAddress - address_) / (unitSize_ * kPageSize);
  VELOX_CHECK(
      (runAddress - address_) % (kPageSize * unitSize_) == 0,
      "Unaligned allocation in setting mapped bits");
  const int numPages = run.numPages() / unitSize_;
  for (int page = firstBit; page < firstBit + numPages; ++page) {
    bits::setBit(pageMapped_.data(), page, value);
  }
}

MachinePageCount MmapAllocator::SizeClass::free(
    MappedMemory::Allocation& allocation) {
  MachinePageCount numFreed = 0;
  int firstRunInClass = -1;
  // Check if there are any runs in 'this' outside of 'mutex_'.
  for (int i = 0; i < allocation.numRuns(); ++i) {
    PageRun run = allocation.runAt(i);
    uint8_t* runAddress = run.data();
    if (isInRange(runAddress)) {
      firstRunInClass = i;
      break;
    }
  }
  if (firstRunInClass == -1) {
    return 0;
  }
  std::lock_guard<std::mutex> l(mutex_);
  for (int i = firstRunInClass; i < allocation.numRuns(); ++i) {
    PageRun run = allocation.runAt(i);
    uint8_t* runAddress = run.data();
    if (!isInRange(runAddress)) {
      continue;
    }
    const ClassPageCount numPages = run.numPages() / unitSize_;
    const int firstBit = (runAddress - address_) / (kPageSize * unitSize_);
    for (auto page = firstBit; page < firstBit + numPages; ++page) {
      if (!bits::isBitSet(pageAllocated_.data(), page)) {
        LOG(ERROR) << "Double free: page = " << page
                   << " sizeclass = " << unitSize_;
        continue;
      }
      if (bits::isBitSet(pageMapped_.data(), page)) {
        ++numMappedFreePages_;
      }
      bits::clearBit(pageAllocated_.data(), page);
      numFreed += unitSize_;
    }
  }
  return numFreed;
}

void MmapAllocator::SizeClass::allocateMapped(
    int32_t wordIndex,
    uint64_t candidates,
    ClassPageCount& numPages,
    MmapAllocator::Allocation& allocation) {
  int numSet = __builtin_popcountll(candidates);
  int toAlloc = std::min(numPages, numSet);
  int allocated = 0;
  for (int i = 0; i < toAlloc; ++i) {
    int bit = __builtin_ctzll(candidates);
    bits::setBit(&pageAllocated_[wordIndex], bit);
    // Remove the least significant bit that is going to be allocated.
    candidates &= candidates - 1;
    allocation.append(
        address_ + kPageSize * unitSize_ * (bit + wordIndex * 64), unitSize_);
    ++allocated;
  }
  numMappedFreePages_ -= allocated;
  numPages -= allocated;
}

void MmapAllocator::SizeClass::allocateAny(
    int32_t wordIndex,
    ClassPageCount& numPages,
    MachinePageCount& numUnmapped,
    MmapAllocator::Allocation& allocation) {
  uint64_t freeBits = ~pageAllocated_[wordIndex];
  int toAlloc = std::min(numPages, __builtin_popcountll(freeBits));
  for (int i = 0; i < toAlloc; ++i) {
    int bit = __builtin_ctzll(freeBits);
    bits::setBit(&pageAllocated_[wordIndex], bit);
    if (!(pageMapped_[wordIndex] & (1UL << bit))) {
      numUnmapped += unitSize_;
    } else {
      --numMappedFreePages_;
    }
    allocation.append(
        address_ + kPageSize * unitSize_ * (bit + wordIndex * 64), unitSize_);
    freeBits &= freeBits - 1;
  }
  numPages -= toAlloc;
}

bool MmapAllocator::checkConsistency() const {
  int count = 0;
  int mappedCount = 0;
  for (auto& sizeClass : sizeClasses_) {
    int mapped = 0;
    count += sizeClass->checkConsistency(mapped) * sizeClass->unitSize();
    mappedCount += mapped * sizeClass->unitSize();
  }
  bool ok = true;
  if (count != numAllocated_ - numExternalMapped_) {
    ok = false;
    LOG(WARNING) << "Allocated count out of sync. Actual= " << count
                 << " recorded= " << numAllocated_ - numExternalMapped_;
  }
  if (mappedCount != numMapped_ - numExternalMapped_) {
    ok = false;
    LOG(WARNING) << "Mapped count out of sync. Actual= "
                 << mappedCount + numExternalMapped_
                 << " recorded= " << numMapped_;
  }
  return ok;
}

std::string MmapAllocator::toString() const {
  std::stringstream out;
  out << "[Memory capacity " << capacity_ << " free "
      << static_cast<int64_t>(capacity_ - numAllocated_) << " mapped "
      << numMapped_ << std::endl;
  for (auto& sizeClass : sizeClasses_) {
    out << sizeClass->toString() << std::endl;
  }
  out << "]" << std::endl;
  return out.str();
}

} // namespace facebook::velox::memory
