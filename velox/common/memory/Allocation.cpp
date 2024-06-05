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

#include "velox/common/memory/Allocation.h"

#include "velox/common/memory/Memory.h"

namespace facebook::velox::memory {

Allocation::~Allocation() {
  if (pool_ != nullptr) {
    pool_->freeNonContiguous(*this);
  }
  // NOTE: exception throw on object destruction will cause process crash.
  if ((numPages_ != 0) || !runs_.empty()) {
    VELOX_FAIL("Bad Allocation state on destruction: {}", toString());
  }
}

void Allocation::append(uint8_t* address, MachinePageCount numPages) {
  VELOX_CHECK(
      runs_.empty() || address != runs_.back().data(),
      "Appending a duplicate address into a PageRun");
  if (FOLLY_UNLIKELY(numPages > Allocation::PageRun::kMaxPagesInRun)) {
    VELOX_MEM_ALLOC_ERROR(fmt::format(
        "The number of pages to append {} exceeds the PageRun limit {}",
        numPages,
        Allocation::PageRun::kMaxPagesInRun));
  }
  numPages_ += numPages;
  runs_.emplace_back(address, numPages);
}

void Allocation::appendMove(Allocation& other) {
  for (auto& run : other.runs_) {
    numPages_ += run.numPages();
    runs_.push_back(std::move(run));
  }
  other.runs_.clear();
  other.numPages_ = 0;
}

void Allocation::findRun(uint64_t offset, int32_t* index, int32_t* offsetInRun)
    const {
  uint64_t skipped = 0;
  for (int32_t i = 0; i < runs_.size(); ++i) {
    uint64_t size = AllocationTraits::pageBytes(runs_[i].numPages());
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

std::string Allocation::toString() const {
  return fmt::format(
      "Allocation[numPages:{}, numRuns:{}, pool:{}]",
      numPages_,
      runs_.size(),
      pool_ == nullptr ? "null" : "set");
}

ContiguousAllocation::~ContiguousAllocation() {
  if (pool_ != nullptr) {
    pool_->freeContiguous(*this);
    pool_ = nullptr;
  }
  // NOTE: exception throw on object destruction will cause process crash.
  if ((data_ != nullptr) || (size_ != 0)) {
    VELOX_FAIL("Bad ContiguousAllocation state on destruction: {}", toString());
  }
}

void ContiguousAllocation::set(void* data, uint64_t size, uint64_t maxSize) {
  data_ = data;
  size_ = size;
  maxSize_ = maxSize != 0 ? maxSize : size;
  sanityCheck();
}

void ContiguousAllocation::grow(MachinePageCount increment) {
  pool_->growContiguous(increment, *this);
}

void ContiguousAllocation::clear() {
  pool_ = nullptr;
  set(nullptr, 0);
}

MachinePageCount ContiguousAllocation::numPages() const {
  return AllocationTraits::numPages(size_);
}

std::optional<folly::Range<char*>> ContiguousAllocation::hugePageRange() const {
  auto begin = reinterpret_cast<uintptr_t>(data_);
  auto roundedBegin = bits::roundUp(begin, AllocationTraits::kHugePageSize);
  auto roundedEnd = (begin + maxSize_) / AllocationTraits::kHugePageSize *
      AllocationTraits::kHugePageSize;
  if (roundedEnd <= roundedBegin) {
    return std::nullopt;
  }
  return folly::Range<char*>(
      reinterpret_cast<char*>(roundedBegin), roundedEnd - roundedBegin);
}

std::string ContiguousAllocation::toString() const {
  return fmt::format(
      "ContiguousAllocation[data:{}, size:{}, maxSize: {}, pool:{}]",
      data_,
      size_,
      maxSize_,
      pool_ == nullptr ? "null" : "set");
}
} // namespace facebook::velox::memory
