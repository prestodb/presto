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
#include "velox/experimental/wave/vector/Operand.h"

/// Wave common instruction set. Instructions run a thread block wide
/// and offer common operations like arithmetic, conditionals,
/// filters, hash lookups, etc. Several vectorized operators can fuse
/// into one instruction stream. Instruction streams may use shared
/// memory depending on the instruction mix. The shared memory is to
/// be allocated dynamically at kernel invocation.
namespace facebook::velox::wave {

/// Mixed with opcode to switch between instantiations of instructions for
/// different types.
enum class ScalarType {
  kInt32,
  kInt64,
  kReal,
  kDouble,
  kString,
};

/// Opcodes for common instruction set. First all instructions that
/// do not have operand type variants, then all the ones that
/// do. For type templated instructions, the case label is opcode *
/// numTypes + type, so these must be last in oredr not to conflict.
enum class OpCode {
  // First all OpCodes that have no operand type specialization.
  kFilter = 0,
  kWrap,

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

struct IBinary {
  Operand* left;
  Operand* right;
  Operand* result;
  // If set, apply operation to lanes where there is a non-zero byte in this.
  Operand* predicate;
  // If true, inverts the meaning of 'predicate', so that the operation is
  // perfformed on lanes with a zero byte bit. Xored with predicate[idx].
  uint8_t invert;
};

struct IFilter {
  Operand* flags;
  int32_t* indices;
};

struct IWrap {
  // The indices to wrap on top of 'columns'.
  Operand* indices;

  // Number of items in 'columns', 'targetColumns', 'nuwIndices',
  // 'mayShareIndices'.
  int32_t numColumns;

  // The columns to wrap.
  Operand** columns;
  // The post wrap columns. If the original is not wrapped, these
  // have the base of original and indices to wrap and posssibly new
  // nulls from 'newNulls'. If the original is wrapped and
  // newIndices[i] is non-nullptr, the combined indices from the
  // existing wrap and 'indices are stored in
  // 'newIndices'. 'newIndices[i]' is the indices of
  // targetColumn[i]. If 'newIndices[i]' is nullptr, the new indices
  // overwrite the indices in 'column[i]' and the indices are
  // referenced from targetColunns[i]'.
  Operand** targetColumns;

  Operand** newIndices;

  // If mayShareIndices[i]' is an index of a previous entry in 'columns' and
  // columns[mayshareIndices[i]] shares indices of columns[i], then
  // targetColumns[i] has indices of targetColumn[mayShareIndices[i]]. If the
  // wrappings were not the same, indices are obtained from newIndices[i].
  int32_t mayShareIndices;
};

struct Instruction {
  OpCode opCode;
  union {
    IBinary binary;
    IFilter filter;
    IWrap wrap;
  } _;
};

///
enum class ErrorCode : int32_t {
  // All operations completed.
  kOk,

  // Catchall for runtime errors.
  kError
};

/// Contains a count of active lanes and a per lane error code.
struct BlockStatus {
  int32_t numRows{0};
  ErrorCode errors[kBlockSize];
};

struct ThreadBlockProgram {
  // Shared memory needed for block. The kernel is launched with max of this
  // across the ThreadBlockPrograms.
  int32_t sharedMemorySize{0};
  int32_t numInstructions;

  Instruction** instructions;
};
/// A stream for invoking ExprKernel.
class WaveKernelStream : public Stream {
 public:
  /// Enqueus an invocation of ExprKernel for 'numBlocks' blocks. 'programs[i]'
  /// is the program for blockIdx.x == i. 'blockIdx.x - baseIndices[i] is the
  /// starting index for the TB in its set of peer TBs all running the same
  /// program. 'status' is set to the return status of the corresponding TB.
  /// 'sharedSize' is the per TB bytes shared memory to be reserved at launch.
  void call(
      Stream* alias,
      int32_t numBlocks,
      ThreadBlockProgram** programs,
      int32_t* bases,
      BlockStatus* status,
      int32_t sharedSize);
};

} // namespace facebook::velox::wave
