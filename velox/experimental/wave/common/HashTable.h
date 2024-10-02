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

#include <string.h>
#include <cstdint>

/// Structs for tagged GPU hash table. Can be inclued in both Velox .cpp and
/// .cu.
namespace facebook::velox::wave {

/// A 32 byte tagged bucket with 4 tags, 4 flag bytes and 4 6-byte
/// pointers. Fits in one 32 byte GPU cache sector.
struct GpuBucketMembers {
  static constexpr int32_t kNumSlots = 4;

  uint32_t tags;
  uint32_t flags;
  uint16_t data[12];

  template <typename T>
  T* testingLoad(int32_t idx) {
    auto uptr = static_cast<uint64_t>(data[8 + idx]) << 32;
    uptr |= reinterpret_cast<uint32_t*>(data)[idx];
    return reinterpret_cast<T*>(uptr);
  }
};

template <typename T, int32_t kSize>
class FreeSetBase {
  int32_t full_{0};
  int32_t empty_{1};
  unsigned long long bits_[kSize / 64] = {};
  T items_[kSize] = {};
};

static inline int32_t roundUp64(int32_t value) {
  return (value + 64 - 1) / 64 * 64;
}

/// Range of addresses. fixed length from bottom and variable length from top.
/// if 'rowOffset' goes above 'rowLimit' then rows are full. If 'stringOffset'
/// goes below 'rowLimit' then strings are full.
struct AllocationRange {
  AllocationRange() = default;
  AllocationRange(
      uintptr_t base,
      uint32_t capacity,
      uint32_t rowLimit,
      int32_t rowSize)
      : fixedFull(false),
        variableFull(false),
        base(base),
        capacity(capacity),
        rowLimit(rowLimit),
        // We leave n words of 64 bits, one bit for each possible row within
        // 'capacity' below first row.
        firstRowOffset(roundUp64(capacity / rowSize) / 8),
        rowOffset(firstRowOffset),
        stringOffset(capacity) {
    ::memset(reinterpret_cast<char*>(base), 0, firstRowOffset);
  }

  AllocationRange(AllocationRange&& other) {
    *this = std::move(other);
  }

  AllocationRange& operator=(const AllocationRange& other) = default;

  void operator=(AllocationRange&& other) {
    *this = other;
    new (&other) AllocationRange();
  }

  int64_t availableFixed() {
    return rowOffset > rowLimit ? 0 : rowLimit - rowOffset;
  }

  /// Raises rowLimit by up to 'size'. Returns the amount raised.
  int32_t raiseRowLimit(int32_t size) {
    auto space = stringOffset - rowLimit;
    auto delta = std::min<int32_t>(space, size);
    rowLimit += delta;
    if (delta > 0) {
      fixedFull = false;
    }
    return delta;
  }

  void clearOverflows(int32_t rowSize) {
    if (rowOffset > rowLimit) {
      // Set 'rowOffset' to the greatest multipl of rowSize from 'base' that is
      // below the limit.
      int32_t numRows = (rowLimit - firstRowOffset) / rowSize;
      rowOffset = firstRowOffset + numRows * rowSize;
    }
    if (stringOffset < rowLimit) {
      stringOffset = rowLimit;
    }
  }

  /// Sets row limit so that there are at most 'target' allocatable
  /// bytes. If available space is less than the target, the available
  /// space is not changed. Returns 'target' minus the available space
  // in 'this'.
  int32_t trimFixed(int32_t target) {
    auto available = rowLimit - rowOffset;
    if (available > target) {
      rowLimit = rowOffset + target;
    }
    return target - (rowLimit - rowOffset);
  }

  /// True if in post-default constructed state.
  bool empty() {
    return capacity == 0;
  }

  bool fixedFull{true};
  bool variableFull{true};
  /// Number of the partition. Used when filing away ranges on the control
  /// plane.
  uint8_t partition{0};
  uint64_t base{0};
  uint32_t capacity{0};
  uint32_t rowLimit{0};
  int32_t firstRowOffset{0};
  uint32_t rowOffset{0};
  uint32_t stringOffset{0};
};

/// A device arena for device side allocation.
struct HashPartitionAllocator {
  static constexpr uint32_t kEmpty = ~0;

  HashPartitionAllocator(
      char* data,
      uint32_t capacity,
      uint32_t rowLimit,
      uint32_t rowSize)
      : rowSize(rowSize) {
    ranges[0] = AllocationRange(
        reinterpret_cast<uintptr_t>(data), capacity, rowLimit, rowSize);
  }
  /// Returns the available bytes  in fixed size pools.
  int64_t availableFixed() {
    return ranges[0].availableFixed() + ranges[1].availableFixed();
  }

  /// Sets allocated offsets to limit if these are over the
  /// limit. They are over limit and available is negative after many
  /// concurrent failed allocations.
  void clearOverflows() {
    ranges[0].clearOverflows(rowSize);
    ranges[1].clearOverflows(rowSize);
  }

  /// Raises the row limit by up to size bytes. Returns th amount raised.
  int32_t raiseRowLimits(int32_t size) {
    auto raised = ranges[0].raiseRowLimit(size);
    return raised + ranges[1].raiseRowLimit(size - raised);
  }

  /// sets rowLimit so that there will be at most 'maxSize' bytes of fixed
  /// length.
  void trimRows(int32_t target) {
    target = ranges[0].trimFixed(target);
    ranges[1].trimFixed(target);
  }

  const int32_t rowSize{0};
  AllocationRange ranges[2];
};

/// Implementation of HashPartitionAllocator, defined in .cuh.
struct RowAllocator;

enum class ProbeState : uint8_t { kDone, kMoreValues, kNeedSpace, kRetry };

/// Operands for one TB of hash probe.
struct HashProbe {
  /// The number of input rows processed by each thread of a TB. The base index
  /// for a block in the arrays in 'this' is 'numRowsPerThread * blockDim.x *
  /// blockIdx.x'
  int32_t numRowsPerThread{1};

  /// Count of probe keys for each TB. Subscript is blockIdx.x.
  int32_t* numRows;

  /// Data for probe keys. To be interpreted by Ops of the probe, no
  /// fixed format.
  void* keys;

  /// Hash numbers for probe keys.
  uint64_t* hashes;

  /// List of input rows to retry in kernel. Sized to one per row of
  /// input. Used inside kernel, not meaningful after return. Sample
  /// use case is another warp updating the same row.
  int32_t* kernelRetries1;
  int32_t* kernelRetries2;

  /// List of input rows to retry after host updated state. Sized to
  /// one per row of input. The reason for a host side retry is
  /// needing more space. The host will decide to allocate/spill/error
  /// out.
  int32_t* hostRetries;

  /// Count of valid items in 'hostRetries'. The subscript is blockIdx.x.
  int32_t* numHostRetries;

  /// Space in 'hits' and 'hitRows'. Should be a multiple of probe block width.
  int32_t maxHits{0};

  /// Row numbers for hits. Indices into 'hashes'.
  int32_t* hitRows{nullptr};

  // Optional payload rows hitting from a probe.
  void** hits{nullptr};
};

struct GpuBucket;

struct GpuHashTableBase {
  GpuHashTableBase(
      GpuBucket* buckets,
      int32_t sizeMask,
      int32_t partitionMask,
      RowAllocator* allocators)
      : buckets(buckets),
        sizeMask(sizeMask),
        partitionMask(partitionMask),
        allocators(allocators),
        maxEntries(((sizeMask + 1) * GpuBucketMembers::kNumSlots) / 6 * 5) {}

  /// Bucket array. Size is 'sizeMask + 1'.
  GpuBucket* buckets{nullptr};

  // Mask to extract index into 'buckets' from a hash number. a
  // sizemask of 63 means 64 buckets, which is up to 256 entries.
  uint32_t sizeMask;

  // Translates a hash number to a partition number '(hash >> 41) &
  // partitionMask gives a physical partition of the table. Used as
  // index into 'allocators'.
  uint32_t partitionMask{0};

  /// A RowAllocator for each partition.
  RowAllocator* allocators;

  /// Count of entries in buckets.
  int64_t numDistinct{0};

  /// Maximum number of entries. Incremented by atomic add at warp
  /// level. Must be at least 32 belo count of slots. If numDistinct
  /// after add exceeds max, the inserts in the warp fail and will be
  /// retried after rehash.
  int64_t maxEntries{0};
};

} // namespace facebook::velox::wave
