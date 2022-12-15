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
#include "velox/common/memory/StreamArena.h"
#include "velox/common/memory/ByteStream.h"

namespace facebook::velox {

StreamArena::StreamArena(memory::MappedMemory* mappedMemory)
    : mappedMemory_(mappedMemory->shared_from_this()),
      allocation_(mappedMemory) {}

void StreamArena::newRange(int32_t bytes, ByteRange* range) {
  VELOX_CHECK(bytes > 0);
  memory::MachinePageCount numPages =
      bits::roundUp(bytes, memory::MappedMemory::kPageSize) /
      memory::MappedMemory::kPageSize;
  int32_t numRuns = allocation_.numRuns();
  if (currentRun_ >= numRuns) {
    if (numRuns) {
      allocations_.push_back(std::make_unique<memory::MappedMemory::Allocation>(
          std::move(allocation_)));
    }
    if (!mappedMemory_->allocate(
            std::max(allocationQuantum_, numPages),
            kVectorStreamOwner,
            allocation_)) {
      throw std::bad_alloc();
    }
    currentRun_ = 0;
    currentPage_ = 0;
    size_ += allocation_.byteSize();
  }
  auto run = allocation_.runAt(currentRun_);
  int32_t available = run.numPages() - currentPage_;
  range->buffer = run.data() + memory::MappedMemory::kPageSize * currentPage_;
  range->size =
      std::min<int32_t>(numPages, available) * memory::MappedMemory::kPageSize;
  range->position = 0;
  currentPage_ += std::min<int32_t>(available, numPages);
  if (currentPage_ == run.numPages()) {
    ++currentRun_;
    currentPage_ = 0;
  }
}

void StreamArena::newTinyRange(int32_t bytes, ByteRange* range) {
  tinyRanges_.emplace_back();
  tinyRanges_.back().resize(bytes);
  range->position = 0;
  range->buffer = reinterpret_cast<uint8_t*>(tinyRanges_.back().data());
  range->size = bytes;
}

} // namespace facebook::velox
