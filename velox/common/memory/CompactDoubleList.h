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

#include <glog/logging.h>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

/// Circular double linked list with 6 byte pointers. Used for free list in
/// HashStringAllocator so that we get a minimum allocation payload size of 16
/// bytes. (12 bits for the links, 4 for trailer).
///
/// NOTE: this class is not thread-safe.
class CompactDoubleList {
 public:
  CompactDoubleList() {
    setNext(this);
    setPrevious(this);
  }

  CompactDoubleList(const CompactDoubleList& other) = delete;
  CompactDoubleList(CompactDoubleList&& other) = delete;
  void operator=(const CompactDoubleList& other) = delete;
  void operator=(CompactDoubleList&& other) = delete;

  /// Returns true if 'this' is the only element.
  bool empty() const {
    return next() == this;
  }

  /// Inserts 'entry' after 'this'
  void insert(CompactDoubleList* entry) {
    entry->setNext(next());
    entry->setPrevious(this);
    next()->setPrevious(entry);
    setNext(entry);
  }

  /// Unlinks 'this' from its list. Throws if 'this' is the only element.
  void remove() {
    VELOX_CHECK(!empty());
    previous()->setNext(next());
    next()->setPrevious(previous());
  }

  CompactDoubleList* next() const {
    return loadPointer(nextLow_, nextHigh_);
  }

  CompactDoubleList* previous() const {
    return loadPointer(previousLow_, previousHigh_);
  }

  /// Updates links after the next() of 'this' has been moved to 'newNext'. Sets
  /// the next link of this, the previous link of 'newNext' and the previous
  /// link of the next() of the moved 'newNext'. The use case is taking the
  /// head of a free list block without a full remove of block plus reinsert of
  /// the remainder of the block.
  void nextMoved(CompactDoubleList* newNext) {
    setNext(newNext);
    VELOX_CHECK(newNext->previous() == this);
    newNext->next()->setPrevious(newNext);
  }

 private:
  static constexpr uint8_t kPointerSignificantBits = 48;

  void setNext(CompactDoubleList* next) {
    storePointer(next, nextLow_, nextHigh_);
  }

  void setPrevious(CompactDoubleList* previous) {
    storePointer(previous, previousLow_, previousHigh_);
  }

  CompactDoubleList* loadPointer(uint32_t low, uint16_t high) const {
    return reinterpret_cast<CompactDoubleList*>(
        low | (static_cast<uint64_t>(high) << 32));
  }

  void storePointer(CompactDoubleList* pointer, uint32_t& low, uint16_t& high) {
    VELOX_DCHECK_EQ(
        reinterpret_cast<uint64_t>(pointer) &
            ~bits::lowMask(kPointerSignificantBits),
        0);
    const uint64_t data = reinterpret_cast<uint64_t>(pointer);
    low = static_cast<uint32_t>(data);
    high = static_cast<uint16_t>(data >> 32);
  }

  // 12 bytes. Stores 2 48 bit pointers.
  uint32_t nextLow_;
  uint32_t previousLow_;
  uint16_t nextHigh_;
  uint16_t previousHigh_;
};

} // namespace facebook::velox
