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

#pragma once

#include "velox/vector/FlatVector.h"

namespace facebook::velox {

/// StringVectorBuffer is a utility for managing a buffer that can grow
/// dynamically as needed. It is designed to work with a FlatVector of
/// StringView objects, allowing writing to the flatVector with zero copying.
///
/// The buffer starts with an initial capacity and can grow
/// up to a specified maximum capacity. Once existing capacity is full,
/// The buffer increases its capacity by kGrowFactor each time,
/// up to the maximum capacity.
///
/// Note: when the resizes happens, old buffer will stay in
/// `FlatVector::stringBuffers_`, the partial last row is duplicated in
/// both old buffer and new buffer.
class StringVectorBuffer {
 public:
  StringVectorBuffer(
      FlatVector<StringView>* vector,
      size_t initialCapacity,
      size_t maxCapacity);

  /// Appends a byte to the buffer.
  /// TODO: supports appending multiple bytes at a time.
  void appendByte(int8_t value);

  /// Sets the row at 'rowId' with current buffered data without data copy.
  void flushRow(vector_size_t rowId);

 private:
  FOLLY_ALWAYS_INLINE size_t writableCapacity() const {
    VELOX_CHECK_GE(currentCapacity_, currentPosition_);
    return currentCapacity_ - currentPosition_;
  }

  FOLLY_ALWAYS_INLINE size_t unflushedRowSize() const {
    VELOX_CHECK_GE(currentPosition_, startPosition_);
    return currentPosition_ - startPosition_;
  }

  /// Try to increase the capacity of the buffer by at least growSize.
  /// If the max capacity is reached,
  /// the buffer can't be resized and VELOX_CHECK will fail.
  void ensureCapacity(size_t growSize);

  char* data() const {
    return rawBuffer_;
  }

  // The factor to multiple the current capacity by when resizing.
  static constexpr size_t kGrowFactor = 2;

  // The maximum capacity of the buffer we can grow to.
  const size_t maxCapacity_;

  FlatVector<StringView>* const vector_;
  char* rawBuffer_ = nullptr;

  // Keeps track of where the current row starts in the rawBuffer.
  size_t startPosition_ = 0;
  // Keeps track of the current write position in the rawBuffer.
  size_t currentPosition_ = 0;
  // The size of the buffer that accepts the new append.
  size_t currentCapacity_ = 0;
  // The total size of the buffers have been allocated.
  size_t totalCapacity_ = 0;
};

} // namespace facebook::velox
