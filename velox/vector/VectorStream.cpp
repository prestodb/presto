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
#include "velox/vector/VectorStream.h"
#include <memory>

namespace facebook::velox {

namespace {
std::unique_ptr<VectorSerde>& getVectorSerde() {
  static std::unique_ptr<VectorSerde> serde;
  return serde;
}
} // namespace

bool registerVectorSerde(std::unique_ptr<VectorSerde> serde) {
  VELOX_CHECK(!getVectorSerde().get(), "Vector serde is already registered");
  getVectorSerde() = std::move(serde);
  return true;
}

bool isRegisteredVectorSerde() {
  return (getVectorSerde().get() != nullptr);
}

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

void ByteStream::flush(std::ostream* out) {
  for (int32_t i = 0; i < ranges_.size(); ++i) {
    int32_t count = ranges_[i].position;
    int32_t bytes = isBits_ ? bits::nbytes(count) : count;
    if (isBits_ && isReverseBitOrder_ && !isReversed_) {
      bits::reverseBits(ranges_[i].buffer, bytes);
    }
    out->write(reinterpret_cast<char*>(ranges_[i].buffer), bytes);
  }
  if (isBits_ && isReverseBitOrder_) {
    isReversed_ = true;
  }
}

void VectorStreamGroup::createStreamTree(
    std::shared_ptr<const RowType> type,
    int32_t numRows) {
  VELOX_CHECK(getVectorSerde().get(), "Vector serde is not registered");
  serializer_ = getVectorSerde()->createSerializer(type, numRows, this);
}

void VectorStreamGroup::append(
    std::shared_ptr<RowVector> vector,
    const folly::Range<const IndexRange*>& ranges) {
  serializer_->append(vector, ranges);
}

void VectorStreamGroup::flush(std::ostream* out) {
  serializer_->flush(out);
}

// static
void VectorStreamGroup::estimateSerializedSize(
    std::shared_ptr<BaseVector> vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  VELOX_CHECK(getVectorSerde().get(), "Vector serde is not registered");
  getVectorSerde()->estimateSerializedSize(vector, ranges, sizes);
}

// static
void VectorStreamGroup::read(
    ByteStream* source,
    velox::memory::MemoryPool* pool,
    std::shared_ptr<const RowType> type,
    std::shared_ptr<RowVector>* result) {
  VELOX_CHECK(getVectorSerde().get(), "Vector serde is not registered");
  getVectorSerde()->deserialize(source, pool, type, result);
}

} // namespace facebook::velox
