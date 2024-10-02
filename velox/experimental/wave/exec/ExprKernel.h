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
#include "velox/experimental/wave/common/Cuda.h"
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

/// Opcodes for common instruction set. First all instructions that
/// do not have operand type variants, then all the ones that
/// do. For type templated instructions, the case label is opcode *
/// numTypes + type, so these must be last in oredr not to conflict.
enum class OpCode {
  // First all OpCodes that have no operand type specialization.
  kFilter = 0,
  kWrap,
  kLiteral,
  kNegate,
  kAggregate,
  kReadAggregate,
  kReturn,

  // From here, only OpCodes that have variants for scalar types.
  kPlus_BIGINT,
  kLT_BIGINT
};

constexpr int32_t kLastScalarKind = static_cast<int32_t>(WaveTypeKind::HUGEINT);

struct IBinary {
  OperandIndex left;
  OperandIndex right;
  OperandIndex result;
  // If set, apply operation to lanes where there is a non-zero byte in this.
  OperandIndex predicate{kEmpty};
};

struct IFilter {
  OperandIndex flags;
  OperandIndex indices;
};

struct IWrap {
  // The indices to wrap on top of 'columns'.
  OperandIndex indices;
  int32_t numColumns;

  // The columns to wrap.
  OperandIndex* columns;
};

struct ILiteral {
  OperandIndex literal;
  OperandIndex result;
  OperandIndex predicate;
};

struct INegate {
  OperandIndex value;
  OperandIndex result;
  OperandIndex predicate;
};
struct IReturn {};

enum class AggregateOp : uint8_t { kSum };

/// Device-side state for group by
struct DeviceAggregation {
  /// hash table, nullptr if no grouping keys.
  GpuHashTableBase* table{nullptr};

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

struct IUpdateAgg {
  AggregateOp op;
  // Offset of null indicator byte on accumulator row.
  int32_t nullOffset;
  // Offset of accumulator on accumulator row. Aligned at 8.
  int32_t accumulatorOffset;
  OperandIndex arg1{kEmpty};
  OperandIndex arg2{kEmpty};
  OperandIndex result{kEmpty};
};

struct IAggregate {
  uint16_t numKeys;
  uint16_t numAggregates;
  // Serial is used in BlockStatus to identify 'this' for continue.
  uint8_t serial;
  /// Index of the state in operator states.
  uint8_t stateIndex;
  /// Position of status return block in operator status returned to host.
  InstructionStatus status;
  //  'numAggregates' Updates followed by key 'numKeys' key operand indices.
  IUpdateAgg* aggregates;
};

struct AggregateReturn {
  /// Count of rows in the table. Triggers rehash when high enough.
  int64_t numDistinct;
};

struct Instruction {
  OpCode opCode;
  union {
    IBinary binary;
    IFilter filter;
    IWrap wrap;
    ILiteral literal;
    INegate negate;
    IAggregate aggregate;
  } _;
};

struct ThreadBlockProgram {
  // Shared memory needed for block. The kernel is launched with max of this
  // across the ThreadBlockPrograms.
  int32_t sharedMemorySize{0};
  int32_t numInstructions;
  // Array of instructions. Ends in a kReturn.
  Instruction* instructions;
};

/// Thread block wide status in Wave kernels
struct WaveShared {
  /// per lane status and row count.
  BlockStatus* status;
  Operand** operands;
  void** states;

  /// True if continuing the first instruction. The instructoin will
  /// pick up its lane status from blockStatus or an
  /// instruction-specific source. The instruction must clear this
  /// before executing the next instruction.
  bool isContinue;

  /// True if some lane needs a continue. Used inside a kernel to
  /// indicate that the grid level status should be set to indicate
  /// continue. Reset before end of instruction.
  bool hasContinue;

  /// If true, all threads in block return before starting next instruction.
  bool stop;
  int32_t blockBase;
  int32_t numRows;
  /// Number of blocks for the program. Return statuses are at
  /// '&blockStatus[numBlocks']
  int32_t numBlocks;

  /// Number of items in blockStatus covered by each TB.
  int16_t numRowsPerThread;

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
  // number here. Subscript is blockIdx.x.
  int32_t* programIdx{nullptr};

  // The TB program for each exe. The subscript is programIdx[blockIdx.x].
  ThreadBlockProgram** programs{nullptr};

  /// The instruction where to start the execution. If nullptr,
  /// 0. Otherwise subscript is programIdx. The active lanes are given
  /// in 'blockStatus'. Used when restarting program at a specific
  /// instruction, e.g. after allocating new memory on host. nullptr means first
  /// launch, starting at 0.
  int32_t* startPC{nullptr};

  // For each exe, the start of the array of Operand*. Instructions reference
  // operands via offset in this array. The subscript is
  // programIndx[blockIdx.x].
  Operand*** operands{nullptr};

  // the status return block for each TB. The subscript is blockIdx.x -
  // (blockBase[blockIdx.x] / kBlockSize). Shared between all programs.
  BlockStatus* status{nullptr};

  // Address of global states like hash tables. Subscript is 'programIdx' and
  // next subscript is state id in the instruction.
  void*** operatorStates;

  /// Number of blocks in each program. gridDim.x can be a multiple if many
  /// programs in launch.
  int32_t numBlocks{0};

  /// Number of elements of blockStatus covered by each TB.
  int16_t numRowsPerThread{1};

  /// Id of stream <stream ordinal within WaveDriver> + (<driverId of
  /// WaveDriver> * <number of Drivers>.
  int16_t streamIdx{0};
};

/// Returns the shared memory size for instruction for kBlockSize.
int32_t instructionSharedMemory(const Instruction& instruction);

/// A stream for invoking ExprKernel.
class WaveKernelStream : public Stream {
 public:
  /// Enqueus an invocation of ExprKernel for 'numBlocks' b
  /// tBs. 'blockBase' is the ordinal of the TB within the TBs with
  /// the same program.  'programIdx[blockIndx.x]' is the index into
  /// 'programs' for the program of the TB. 'operands[i]' is the start
  /// of the Operand array for 'programs[i]'. status[blockIdx.x] is
  /// the return status for each TB. 'sharedSize' is the per TB bytes
  /// shared memory to be reserved at launch.
  void call(
      Stream* alias,
      int32_t numBlocks,
      int32_t sharedSize,
      KernelParams& params);

  /// Sets up or updates an aggregation.
  void setupAggregation(AggregationControl& op);

 private:
  // Debug implementation of call() where each instruction is a separate kernel
  // launch.
  void callOne(
      Stream* alias,
      int32_t numBlocks,
      int32_t sharedSize,
      KernelParams& params);
};

} // namespace facebook::velox::wave
