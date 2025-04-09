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

#include "velox/vector/StringVectorBuffer.h"

namespace facebook::velox {
StringVectorBuffer::StringVectorBuffer(
    FlatVector<StringView>* vector,
    size_t initialCapacity,
    size_t maxCapacity)
    : maxCapacity_(maxCapacity), vector_(vector) {
  VELOX_CHECK_GE(
      maxCapacity_, initialCapacity, "initialCapacity must be <= maxCapacity");
  ensureCapacity(initialCapacity);
}

void StringVectorBuffer::appendByte(int8_t value) {
  ensureCapacity(1);
  VELOX_CHECK_GT(writableCapacity(), 0, "No writable capacity");
  rawBuffer_[currentPosition_++] = value;
}

void StringVectorBuffer::flushRow(vector_size_t rowId) {
  // Flush the current row to the vector.
  vector_->setNoCopy(
      rowId, StringView(data() + startPosition_, unflushedRowSize()));
  startPosition_ = currentPosition_;
}

void StringVectorBuffer::ensureCapacity(size_t growSize) {
  if (writableCapacity() >= growSize) {
    return;
  }

  // minimal space needed for resize to write the current unflushed row and
  // 'growSize' number of additional bytes.
  const auto currUnflushedRowSize = unflushedRowSize();
  auto minRequiredCapacity = totalCapacity_ + currUnflushedRowSize + growSize;

  auto newTotalCapacity = std::min(
      std::max(totalCapacity_ * kGrowFactor, minRequiredCapacity),
      maxCapacity_);

  VELOX_CHECK_GE(
      newTotalCapacity,
      minRequiredCapacity,
      "Cannot grow buffer with totalCapacity:{} to meet minRequiredCapacity:{}",
      succinctBytes(totalCapacity_),
      succinctBytes(minRequiredCapacity));

  // newBufferCapacity is the additional free space we need to allocate.
  const auto newBufferCapacity = newTotalCapacity - totalCapacity_;

  auto newBuffer = vector_->getRawStringBufferWithSpace(newBufferCapacity);

  // A row needs to fit in continuous memory.
  // Copy the data from the old buffer to the new buffer,
  // if the current row has not been flushed.
  if (currUnflushedRowSize > 0) {
    VELOX_CHECK_GE(
        newBufferCapacity,
        currUnflushedRowSize,
        "not enough buffer space to write unflushed row");
    std::memcpy(newBuffer, data() + startPosition_, currUnflushedRowSize);
  }

  rawBuffer_ = newBuffer;
  startPosition_ = 0;
  currentPosition_ = currUnflushedRowSize;

  // Update the capacity.
  currentCapacity_ = newBufferCapacity;
  totalCapacity_ = newTotalCapacity;
}

} // namespace facebook::velox
