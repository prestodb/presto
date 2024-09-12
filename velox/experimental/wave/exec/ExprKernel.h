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
};

/// Parameters for creating/updating a group by.
struct AggregationControl {
  /// Pointer to uninitialized first buffer in the case of initializing and to
  /// the previous head in the case of rehashing. Must always be set.
  void* head;
  /// Size of block starting at 'head'. Must be set on first setup.
  int64_t headSize{0};
  /// For a rehashing request, space for a new head and a new HashTable.
  void* newHead{nullptr};
  int64_t newHeadSize{0};
  /// Size of single row allocation. Required on first init.
  int32_t rowSize{0};
  //// Number of slots in HashTable, must be a powr of two. 0 means no hash
  /// table (aggregation without grouping keys).
  int32_t maxTableEntries{0};
  /// Number of allocators for the hash table, if any. Must be a powr of two.
  int32_t numPartitions{1};
  /// Uninitialized space to be added to the allocators of an existing
  /// HashTable.
  char* extraSpace{nullptr};
  /// Usable bytes starting at extraSpace.
  int64_t extraSpaceSize{0};
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
  //  'numAggre gates' Updates followed by key 'numKeys' key operand indices.
  IUpdateAgg* aggregates;
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
  /// If true, all threads in block return before starting next instruction.
  bool stop;
  int32_t blockBase;
  int32_t numRows;
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
