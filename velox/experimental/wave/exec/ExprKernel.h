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
  kReturn,

  // From here, only OpCodes that have variants for scalar types.
  kPlus,
  kMinus,
  kTimes,
  kDivide,
  kEquals,
  kLT,
  kLTE,
  kGT,
  kGTE,
  kNE,

};
constexpr int32_t kLastScalarKind = static_cast<int32_t>(WaveTypeKind::HUGEINT);

#define OP_MIX(op, t)           \
  static_cast<OpCode>(          \
      static_cast<int32_t>(t) + \
      (kLastScalarKind + 1) * static_cast<int32_t>(op))

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

struct Instruction {
  OpCode opCode;
  union {
    IBinary binary;
    IFilter filter;
    IWrap wrap;
    ILiteral literal;
    INegate negate;
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
      int32_t* blockBase,
      int32_t* programIndices,
      ThreadBlockProgram** programs,
      Operand*** operands,
      BlockStatus* status,
      int32_t sharedSize);
};

} // namespace facebook::velox::wave
