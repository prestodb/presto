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

// Rounds 'value' to the next multiple of 'factor'.
template <typename T, typename U>
static inline T roundUp(T value, U factor) {
  return (value + (factor - 1)) / factor * factor;
}

// static
void MappedMemory::destroyTestOnly() {
  instance_ = nullptr;
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
  bool checkConsistency() override;

  const std::vector<MachinePageCount>& sizes() const override {
    return sizes_;
  }

  MachinePageCount numAllocated() const override {
    return numAllocated_;
  }

  MachinePageCount numMapped() const override {
    return numMapped_;
  }

  MachinePageCount allocationSize(
      MachinePageCount numPages,
      MachinePageCount minSizeClass,
      std::array<int32_t, kMaxSizeClasses>* sizeIndices,
      std::array<int32_t, kMaxSizeClasses>* sizeCounts,
      int32_t* numSizes) const;

 private:
  std::atomic<MachinePageCount> numAllocated_;
  // When using mmap/madvise, the current of number pages backed by memory.
  std::atomic<MachinePageCount> numMapped_;
  // The machine page counts corresponding to different sizes in order
  // of increasing size.
  std::vector<MachinePageCount> sizes_;

  std::mutex mallocsMutex_;
  // Tracks malloc'd pointers to detect bad frees.
  std::unordered_set<void*> mallocs_;
};

} // namespace

MappedMemoryImpl::MappedMemoryImpl() : numAllocated_(0), numMapped_(0) {
  sizes_ = {4, 8, 16, 32, 64, 128, 256};
}

bool MappedMemoryImpl::allocate(
    MachinePageCount numPages,
    int32_t owner,
    Allocation& out,
    std::function<void(int64_t)> beforeAllocCB,
    MachinePageCount minSizeClass) {
  free(out);

  std::array<int32_t, kMaxSizeClasses> sizeIndices = {};
  std::array<int32_t, kMaxSizeClasses> sizeCounts = {};
  int32_t numSizes = 0;
  int32_t pagesToAlloc = allocationSize(
      numPages, minSizeClass, &sizeIndices, &sizeCounts, &numSizes);

  if (FLAGS_velox_use_malloc) {
    if (beforeAllocCB) {
      uint64_t bytesAllocated = 0;
      for (int32_t i = 0; i < numSizes; ++i) {
        MachinePageCount numPages = sizeCounts[i] * sizes_[sizeIndices[i]];
        bytesAllocated += numPages * kPageSize;
      }
      beforeAllocCB(bytesAllocated);
    }

    std::vector<void*> pages;
    pages.reserve(numSizes);
    for (int32_t i = 0; i < numSizes; ++i) {
      MachinePageCount numPages = sizeCounts[i] * sizes_[sizeIndices[i]];
      void* ptr = malloc(numPages * kPageSize); // NOLINT
      if (!ptr) {
        // Failed to allocate memory from memory.
        break;
      }
      pages.emplace_back(ptr);
      out.append(reinterpret_cast<uint8_t*>(ptr), numPages); // NOLINT
    }
    if (pages.size() != numSizes) {
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
    numAllocated_.fetch_add(pagesToAlloc);
    return true;
  }
  throw std::runtime_error("Not implemented");
}

MachinePageCount MappedMemoryImpl::allocationSize(
    MachinePageCount numPages,
    MachinePageCount minSizeClass,
    std::array<int32_t, kMaxSizeClasses>* sizeIndices,
    std::array<int32_t, kMaxSizeClasses>* sizeCounts,
    int32_t* numSizes) const {
  int32_t needed = numPages;
  int32_t pagesToAlloc = 0;
  *numSizes = 0;
  VELOX_CHECK_LE(
      minSizeClass,
      sizes_.back(),
      "Requesting minimum size larger than largest size class");
  for (int32_t sizeIndex = sizes_.size() - 1; sizeIndex >= 0; sizeIndex--) {
    int32_t size = sizes_[sizeIndex];
    bool isSmallest = sizeIndex == 0 || sizes_[sizeIndex - 1] < minSizeClass;
    // If the size is less than 1/8 of the size from the next larger,
    // use the next larger size.
    if (size > (needed + (needed / 8)) && !isSmallest) {
      continue;
    }
    int32_t numUnits = std::max(1, needed / size);
    needed -= numUnits * size;
    if (isSmallest && needed > 0) {
      // If needed / size had a remainder, add one unit of smallest class.
      numUnits++;
      needed -= size;
    }
    (*sizeCounts)[*numSizes] = numUnits;
    pagesToAlloc += numUnits * size;
    (*sizeIndices)[(*numSizes)++] = sizeIndex;
    if (needed <= 0) {
      break;
    }
  }
  return pagesToAlloc;
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

bool MappedMemoryImpl::checkConsistency() {
  if (FLAGS_velox_use_malloc) {
    return true;
  }
  throw std::runtime_error("Not implemented");
}

MappedMemory* MappedMemory::customInstance_;
std::unique_ptr<MappedMemory> MappedMemory::instance_;
std::mutex MappedMemory::initMutex_;

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

} // namespace facebook::velox::memory
