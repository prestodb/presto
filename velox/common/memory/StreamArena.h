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
#include "velox/common/memory/Memory.h"

namespace facebook::velox {

struct ByteRange;

/// An abstract class that holds memory for serialized vector content. A single
/// repartitioning target is one use case: The bytes held are released as a unit
/// when the destination acknowledges receipt. Another use case is a hash table
/// partition that holds complex types as serialized rows.
class StreamArena {
 public:
  explicit StreamArena(memory::MemoryPool* pool);

  virtual ~StreamArena() = default;

  /// Sets range to the request 'bytes' of writable memory owned by
  /// 'this'.  We allocate non-contiguous memory to store range bytes
  /// if requested 'bytes' is equal or less than the largest class
  /// page size. Otherwise, we allocate from contiguous
  /// memory. 'range' is set to point to the allocated memory. If
  /// 'lastRange' is non-nullptr, it is the last range of the stream
  /// to which we are adding the new range. 'lastRange' is nullptr if
  /// adding the first range to a stream. The memory is stays owned by
  /// 'this' in all cases. Used by HashStringAllocator when extending
  /// a multipart entry. The previously last part has its last 8 bytes
  /// moved to the next part and gets a pointer to the next part as
  /// its last 8 bytes. When extending, we need to update the entry so
  /// that the next pointer is not seen when reading the content and
  /// is also not counted in the payload size of the multipart entry.
  virtual void newRange(int32_t bytes, ByteRange* lastRange, ByteRange* range);

  /// sets 'range' to point to a small piece of memory owned by this. These
  /// always come from the heap. The use case is for headers that may change
  /// length based on data properties, not for bulk data. See 'newRange' for the
  /// meaning of 'lastRange'.
  virtual void
  newTinyRange(int32_t bytes, ByteRange* lastRange, ByteRange* range);

  /// Returns the Total size in bytes held by all Allocations.
  virtual size_t size() const {
    return size_;
  }

  memory::MemoryPool* pool() const {
    return pool_;
  }

  /// Restores 'this' to post-construction state. Used in recycling streams for
  /// serilizers.
  virtual void clear();

 private:
  memory::MemoryPool* const pool_;
  const memory::MachinePageCount allocationQuantum_{2};

  // All non-contiguous allocations.
  std::vector<std::unique_ptr<memory::Allocation>> allocations_;

  // The allocation from which pages are given out. Moved to 'allocations_' when
  // used up.
  memory::Allocation allocation_;

  // The index of page run in 'allocation_' for next allocation.
  int32_t currentRun_ = 0;

  // The byte offset in page run indexed by 'currentRun_' for next allocation.
  int32_t currentOffset_ = 0;

  // All contiguous allocations.
  std::vector<memory::ContiguousAllocation> largeAllocations_;

  // Tracks all the contiguous and non-contiguous allocations in bytes.
  size_t size_ = 0;

  std::vector<std::string> tinyRanges_;
};

} // namespace facebook::velox
