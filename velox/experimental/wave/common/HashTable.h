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

#include <cstdint>

/// Structs for tagged GPU hash table. Can be inclued in both Velox .cpp and
/// .cu.
namespace facebook::velox::wave {

/// A 32 byte tagged bucket with 4 tags, 4 flag bytes and 4 6-byte
/// pointers. Fits in one 32 byte GPU cache sector.
struct GpuBucketMembers {
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

/// A device arena for device side allocation.
struct HashPartitionAllocator {
  static constexpr uint32_t kEmpty = ~0;

  HashPartitionAllocator(
      char* data,
      uint32_t size,
      uint32_t rowSize,
      void* freeSet)
      : rowSize(rowSize),
        base(reinterpret_cast<uint64_t>(data)),
        capacity(size),
        stringOffset(capacity),
        freeSet(freeSet) {}

  const int32_t rowSize{0};
  const uint64_t base{0};
  uint32_t rowOffset{0};
  const uint32_t capacity{0};
  uint32_t stringOffset{0};
  void* freeSet{nullptr};
  int32_t numFromFree{0};
  int32_t numFull{0};
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
  /// Bucket array. Size is 'sizeMask + 1'.
  GpuBucket* buckets{nullptr};

  // Mask to extract index into 'buckets' from a hash number. a
  // sizemask of 63 means 64 buckets, which is up to 256 entries.
  uint32_t sizeMask;

  // Translates a hash number to a partition number '(hash &
  // partitionMask) >> partitionShift' is a partition number used as
  // a physical partition of the table. Used as index into 'allocators'.
  uint32_t partitionMask{0};
  uint8_t partitionShift{0};

  /// A RowAllocator for each partition.
  RowAllocator* allocators;
};

} // namespace facebook::velox::wave
