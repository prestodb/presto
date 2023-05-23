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
