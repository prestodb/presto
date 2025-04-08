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

#include <stdint.h>
#include "velox/experimental/wave/common/HashTable.h"
#include "velox/experimental/wave/exec/ErrorCode.h"
#include "velox/experimental/wave/vector/Operand.h"

/// Wave common instruction set. Instructions run a thread block wide
/// and offer common operations like arithmetic, conditionals,
/// filters, hash lookups, etc. Several vectorized operators can fuse
/// into one instruction stream. Instruction streams may use shared
/// memory depending on the instruction mix. The shared memory is to
/// be allocated dynamically at kernel invocation.
namespace facebook::velox::wave {

/// Device-side state for group by
struct DeviceAggregation {
  /// hash table, nullptr if no grouping keys.
  GpuHashTableBase* table{nullptr};

  /// Device side atomic counting thread blocks working on the state. Assert
  /// this is 0 at rehash or resupply of allocators.
  uint32_t debugActiveBlockCounter{0};

  // Byte size of a rowm rounded to next 8.
  int32_t rowSize = 0;

  /// Allocator for variable llength accumulators, if not provided by 'table'.
  RowAllocator* allocator{nullptr};

  char* singleRow{nullptr};

  /// Number of int64_t* in groupResultRows. One for each potential
  /// streamIdx of reading kernel.
  int32_t numReadStreams{0};

  /// Pointers to group by result row arrays. Subscripts is
  /// '[streamIdx][row + 1]'. Element 0 is the row count.
  uintptr_t** resultRowPointers{nullptr};
};

/// Parameters for creating/updating a group by.
struct AggregationControl {
  /// Pointer to page-aligned DeviceAggregation.
  DeviceAggregation* head;

  /// Size of block starting at 'head'. Must be set on first setup.
  int64_t headSize{0};

  /// For a rehashing command, old bucket array.
  void* oldBuckets{nullptr};

  /// Count of buckets starting at oldBuckets for rehash.
  int64_t numOldBuckets{0};

  /// Size of single row allocation.
  int32_t rowSize{0};
};

struct AggregateReturn {
  /// Count of rows in the table. Triggers rehash when high enough.
  int64_t numDistinct;
};

// Return status for hash join build. Only indicates if more space is needed.
struct BuildReturn {
  // Flag 8 wide for alignment.
  int64_t needMore{0};
};

/// Thread block wide status in Wave kernels
struct WaveShared {
  /// per lane status and row count.
  BlockStatus* status;
  Operand** operands;
  void** states;

  /// Every wrap in the kernel will also wrap these otherwise not accessed
  /// Operands.
  OperandIndex extraWraps;
  int16_t numExtraWraps;
  // The continue label where execution is to resume if continuing.
  int16_t startLabel;
  /// True if continuing the first instruction. The instruction will
  /// pick up its lane status from blockStatus or an
  /// instruction-specific source. The instruction must clear this
  /// before executing the next instruction.
  bool isContinue;

  /// True if some lane needs a continue. Used inside a kernel to
  /// indicate that the grid level status should be set to indicate
  /// continue. Reset before end of instruction.
  bool hasContinue;

  /// True if doing an extra iteration to increase cardinality in
  /// non-continue case, e.g. first kernel of 1:n join or unnest
  /// producing non-first result for an active lane of input.
  bool localContinue;

  /// If true, all threads in block return before starting next instruction.
  bool stop;
  int32_t blockBase;
  int32_t numRows;
  /// Number of blocks for the program. Return statuses are at
  /// '&blockStatus[numBlocks']
  int32_t numBlocks;

  // The branch of a multibranch kernel this block is doing.
  int16_t programIdx;

  /// Number of items in blockStatus covered by each TB.
  int16_t numRowsPerThread;

  /// Iteration counter, =0; < numRowsPerThread.
  int16_t nthBlock;
  int16_t streamIdx;

  // Scratch data area. Size depends on shared memory size for instructions.
  // Align 8.
  int64_t data;
};

/// Parameters for a Wave kernel. All pointers are device side readable.
struct KernelParams {
  /// The first thread block with the program. Subscript is blockIdx.x.
  int32_t* blockBase{nullptr};
  // The ordinal of the program. All blocks with the same program have the same
  // number here. Subscript is blockIdx.x. For compiled kernels, this gives the
  // branch to follow for the TB at blockIdx.x.
  int32_t* programIdx{nullptr};

  /// The label where to start the execution. If nullptr,
  /// 0. Otherwise subscript is programIdx. The active lanes are given
  /// in 'blockStatus'. Used when restarting program at a specific
  /// instruction, e.g. after allocating new memory on host. nullptr means first
  /// launch, starting at 0.
  int32_t* startPC{nullptr};

  // For each exe, the start of the array of Operand*. Instructions reference
  // operands via offset in this array. The subscript is
  // programIdx[blockIdx.x].
  Operand*** operands{nullptr};

  // the status return block for each TB. The subscript is blockIdx.x -
  // (blockBase[blockIdx.x] / kBlockSize). Shared between all programs.
  BlockStatus* status{nullptr};

  // Address of global states like hash tables. Subscript is 'programIdx' and
  // next subscript is state id in the instruction.
  void*** operatorStates;

  /// first operand index for extra wraps. 'numExtraWraps' next
  /// operands get wrapped by all wraps in the kernel.
  OperandIndex extraWraps{0};
  int16_t numExtraWraps{0};

  /// Number of blocks in each program. gridDim.x can be a multiple if many
  /// programs in launch.
  int32_t numBlocks{0};

  /// Number of elements of blockStatus covered by each TB.
  int16_t numRowsPerThread{1};

  /// Id of stream <stream ordinal within WaveDriver> + (<driverId of
  /// WaveDriver> * <number of Drivers>.
  int16_t streamIdx{0};
};

/// grid status for a hash join expand.
struct HashJoinExpandGridStatus {
  /// True if any lane in the grid has unfetched data. If true,
  /// hashJoinExpandBlockstatus has the lane statuses. int32_t to support
  /// atomics.
  int32_t anyContinuable{0};
  int32_t pad{0};
};

/// Tracks the state of a hash join probe between batches of output. Needed when
/// the join increases cardinality.
struct HashJoinExpandBlockStatus {
  /// The next row in the hash table to look at. nullptr if all hits produced.
  void* next[kBlockSize];
};

// Shared memory resident state for hash join expand.
struct JoinShared {
  HashJoinExpandGridStatus* gridStatus;
  HashJoinExpandBlockStatus* blockStatus;
  int32_t anyNext;
  int32_t temp[kBlockSize / 32];
};

} // namespace facebook::velox::wave
