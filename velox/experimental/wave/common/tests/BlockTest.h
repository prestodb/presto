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

#include "velox/experimental/wave/common/Cuda.h"
#include "velox/experimental/wave/common/HashTable.h"
#include "velox/experimental/wave/common/tests/HashTestUtil.h"

/// Sample header for testing Wave Utilities.

namespace facebook::velox::wave {

constexpr uint32_t kPrime32 = 1815531889;

/// A mock aggregate that concatenates numbers, like array_agg of bigint.
struct ArrayAgg64 {
  struct Run {
    Run* next;
    int64_t data[16];
  };

  Run* first{nullptr};
  Run* last{nullptr};
  // Fill of 'last->data', all other runs are full.
  int8_t numInLast{0};
};

/// A mock hash table content row to test HashTable.
struct TestingRow {
  // Single ke part.
  int64_t key;

  // Count of updates. Sample aggregate
  int64_t count{0};

  // A mock concatenating aggregate. Use for testing control flow in
  // running out of space in updating a group.
  ArrayAgg64 concatenation;

  // Next pointer in the case simulating a non-unique join table.
  TestingRow* next{nullptr};

  // flags for updating the row. E.g. probed flag, marker for exclusive write.
  int32_t flags{0};
};

/// Result of allocator test kernel.
struct AllocatorTestResult {
  RowAllocator* allocator;
  int32_t numRows;
  int32_t numStrings;
  int64_t* rows[200000];
  int64_t* strings[200000];
};

class BlockTestStream : public Stream {
 public:
  /// In each block of 256 bools in bools[i], counts the number of
  /// true and writes the indices of true lanes into the corresponding
  /// indices[i]. Stors the number of true values to sizes[i].
  void testBoolToIndices(
      int32_t numBlocks,
      uint8_t** flags,
      int32_t** indices,
      int32_t* sizes);
  void testBoolToIndicesNoShared(
      int32_t numBlocks,
      uint8_t** flags,
      int32_t** indices,
      int32_t* sizes,
      void*);

  // Returns the smem size for block size 256 of boolToIndices().
  static int32_t boolToIndicesSize();

  void testBool256ToIndices(
      int32_t numBlocks,
      uint8_t** flags,
      int32_t** indices,
      int32_t* sizes);

  void testBool256ToIndicesNoShared(
      int32_t numBlocks,
      uint8_t** flags,
      int32_t** indices,
      int32_t* sizes,
      void*);

  // Returns the smem size for bool256ToIndices().
  static int32_t bool256ToIndicesSize();

  // calculates the sum over blocks of 256 int64s and returns the result for
  // numbers[i * 256] ... numbers[(i + 1) * 256 - 1] inclusive  in results[i].
  void testSum64(int32_t numBlocks, int64_t* numbers, int64_t* results);

  static int32_t sort16SharedSize();

  void testSort16(int32_t numBlocks, uint16_t** keys, uint16_t** values);
  void testSort16NoShared(
      int32_t numBlocks,
      uint16_t** keys,
      uint16_t** values,
      char* temp);

  void partitionShorts(
      int32_t numBlocks,
      uint16_t** keys,
      int32_t* numKeys,
      int32_t numPartitions,
      int32_t** ranks,
      int32_t** partitionStarts,
      int32_t** partitionedRows);

  // Operation for hash table tests.
  enum class HashCase { kGroup, kBuild, kProbe };

  /// Does probe/groupby/build on 'table'. 'probe' contains the parameters and
  /// temp storage. 'table' and 'probe' are expected to be resident on device.
  /// 'numBlocks' gives how many TBs are run, the rows per TB are in 'probe'.
  void hashTest(GpuHashTableBase* table, HashRun& probe, HashCase mode);

  static int32_t freeSetSize();

  void initAllocator(HashPartitionAllocator* allocator);

  /// tests RowAllocator.
  void rowAllocatorTest(
      int32_t numBlocks,
      int32_t numAlloc,
      int32_t numFree,
      int32_t numStr,
      AllocatorTestResult* results);

  void updateSum1Atomic(TestingRow* rows, HashRun& run);
  void updateSum1Exch(TestingRow* rows, HashRun& run);
  void updateSum1NoSync(TestingRow* rows, HashRun& run);
  void updateSum1AtomicCoalesceShfl(TestingRow* rows, HashRun& run);
  void updateSum1AtomicCoalesceShmem(TestingRow* rows, HashRun& run);
  void updateSum1Part(TestingRow* rows, HashRun& run);
  void updateSum1Mtx(TestingRow* rows, HashRun& run);
  void updateSum1MtxCoalesce(TestingRow* rows, HashRun& run);
  void updateSum1Order(TestingRow* rows, HashRun& run);

  static int32_t scatterBitsSize(int32_t blockSize);

  void scatterBits(
      int32_t numSource,
      int32_t numTarget,
      const char* source,
      const uint64_t* targetMask,
      char* target,
      int32_t* temp);

  /// Tests nonNullIndex256 if 'rows' are all consecutive integers and
  /// nonNullIndex256Sparse otherwise. If the row hits a null in 'nulls' the
  /// result in indices is -1, therwise it is the index in non null values, i.e.
  /// the original minus the count of null positions below it.
  void nonNullIndex(
      char* nulls,
      int32_t* rows,
      int32_t numRows,
      int32_t* indices,
      int32_t* temp);
};

} // namespace facebook::velox::wave
