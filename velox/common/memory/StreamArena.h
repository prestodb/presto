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
#include "velox/common/memory/MappedMemory.h"

namespace facebook::velox {

struct ByteRange;

// An abstract class that holds memory for serialized vector
// content. A single repartitioning target is one use case: The bytes
// held are released as a unit when the destination acknowledges
// receipt. Another use case is a hash table partition that holds
// complex types as serialized rows.
class StreamArena {
 public:
  static constexpr int32_t kVectorStreamOwner = 1;

  explicit StreamArena(memory::MappedMemory* mappedMemory);

  virtual ~StreamArena() = default;

  // Sets range to refer  to at least one page of writable memory owned by
  // 'this'. Up to 'numPages' may be  allocated.
  virtual void newRange(int32_t bytes, ByteRange* range);

  // sets 'range' to point to a small piece of memory owned by this. These alwys
  // come from the heap. The use case is for headers that may change length
  // based on data properties, not for bulk data.
  virtual void newTinyRange(int32_t bytes, ByteRange* range);

  // Returns the Total size in bytes held by all Allocations.
  virtual size_t size() const {
    return size_;
  }

  memory::MappedMemory* mappedMemory() {
    return mappedMemory_.get();
  }

 private:
  std::shared_ptr<memory::MappedMemory> mappedMemory_;
  // All allocations.
  std::vector<std::unique_ptr<memory::MappedMemory::Allocation>> allocations_;
  // The allocation from which pages are given out. Moved to 'allocations_' when
  // used up.
  memory::MappedMemory::Allocation allocation_;
  int32_t currentRun_ = 0;
  int32_t currentPage_ = 0;
  memory::MachinePageCount allocationQuantum_ = 2;
  size_t size_ = 0;
  std::vector<std::string> tinyRanges_;
};

} // namespace facebook::velox
