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

/// Types for device side access to device vectors. Separate header
/// independent of Velox headers, included for both host and device
/// side files.
namespace facebook::velox::wave {

/// Copy of TypeKind in velox/type/Type.h. Type.h is incompatible with Cuda
/// headers, therefore duplicated here.
enum class WaveTypeKind : int8_t {

  BOOLEAN = 0,
  TINYINT = 1,
  SMALLINT = 2,
  INTEGER = 3,
  BIGINT = 4,
  REAL = 5,
  DOUBLE = 6,
  VARCHAR = 7,
  VARBINARY = 8,
  TIMESTAMP = 9,
  HUGEINT = 10,
  // Enum values for ComplexTypes start after 30 to leave
  // some values space to accommodate adding new scalar/native
  // types above.
  ARRAY = 30,
  MAP = 31,
  ROW = 32,
  UNKNOWN = 33,
  FUNCTION = 34,
  OPAQUE = 35,
  INVALID = 36
};

template <typename T>
struct WaveTypeTrait {};

template <>
struct WaveTypeTrait<int32_t> {
  static constexpr WaveTypeKind typeKind = WaveTypeKind::INTEGER;
};

template <>
struct WaveTypeTrait<uint32_t> {
  static constexpr WaveTypeKind typeKind = WaveTypeKind::INTEGER;
};

template <>
struct WaveTypeTrait<int64_t> {
  static constexpr WaveTypeKind typeKind = WaveTypeKind::BIGINT;
};
template <>
struct WaveTypeTrait<uint64_t> {
  static constexpr WaveTypeKind typeKind = WaveTypeKind::BIGINT;
};

// Normal thread block size for Wave kernels
constexpr int32_t kBlockSize = 256;
using OperandId = int32_t;

constexpr OperandId kNoOperand = ~0;

using OperandIndex = uint16_t;
constexpr OperandIndex kEmpty = ~0;

// operand indices above this are offsets into TB shared memory arrays.
constexpr OperandIndex kMinSharedMemIndex = 0x8000;

// Number of nullable locals in shared memory. Each has kBlockSize null bytes at
// the start of the TB shared memory. 0 means no nulls. 1 means first kBlockSize
// bytes are nulls, 2 means second kBlockSize  bytes are null flags etc.
constexpr uint16_t kSharedNullMask = 3;

/// Start of the parameter array in the TB shared memory. 13 bits. Shift 1 left
/// to get offset.
constexpr uint16_t kSharedOperandMask = 0x7ffc;

/// Describes an operand for a Wave kernel instruction. The same
/// insttruction is interpreted by multiple thread blocks in the
/// kernel invocation. When accessing an operand, we have the base
/// index of the thread block. This is blockIdx.x * blockDim.x if all
/// thread blocks run the same instructions. When the blocks run
/// different instruction streams, the base is (blockIdx.x - <index of
/// first block with this instruction stream>) * blockDim.x. We also have a
/// shared memory pointer to thread block shared memory. Some operands may come
/// from thread block shared memory.

constexpr uint8_t kNull = 0;
constexpr uint8_t kNotNull = 255;

struct Operand {
  static constexpr int32_t kPointersInOperand = 4;

  int32_t indexMask;

  int32_t size;

  // Array of flat base values. Cast to pod type or StringView.
  void* base;

  // Array of null indicators. No nulls if nullptr.  A 1 means not-null, for
  // consistency with Velox.
  uint8_t* nulls;

  // If non-nullptr, provides index into 'base. Subscripted with the
  // blockIdx - idx of first bllock wit this instruction
  // stream. Different thread blocks may or may not have indices for
  // a given operand.
  int32_t** indices;
};

/// Per-lane error code.
enum class ErrorCode : uint8_t {
  // All operations completed.
  kOk = 0,

  // Set on entry when continuing, e.g. produce more data from hash probe.
  kContinue,

  // all codes from here onwards mean the lane is off
  // Catchall for runtime errors.
  kError,

  kInsufficientMemory,

  kInactive,

};

/// Thread block status with count of active lanes and a per lane
/// error code and continue points for operators that can produce more
/// data.
struct BlockStatus {
  int32_t numRows{0};
  ErrorCode errors[kBlockSize];
};

/// Returns the number of active rows in 'status' for 'numBlocks'.
int32_t statusNumRows(const BlockStatus* status, int32_t numBlocks);

} // namespace facebook::velox::wave
