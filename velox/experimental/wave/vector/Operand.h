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

// Normal thread block size for Wave kernels
constexpr int32_t kBlockSize = 256;
using OperandId = int32_t;

constexpr OperandId kNoOperand = ~0;

/// Describes an operand for a Wave kernel instruction. The same
/// insttruction is interpreted by multiple thread blocks in the
/// kernel invocation. When accessing an operand, we have the base
/// index of the thread block. This is blockIdx.x * blockDim.x if all
/// thread blocks run the same instructions. When the blocks run
/// different instruction streams, the base is (blockIdx.x - <index of
/// first block with this instruction stream>) * blockDim.x. We also have a
/// shared memory pointer to thread block shared memory. Some operands may come
/// from thread block shared memory.

struct Operand {
  static constexpr uint16_t kGlobal = ~0;

  int32_t indexMask{~0};

  // If != !0, this indicates that instead of base, we use the thread block's
  // shared memry base + 'sharedOffset'.
  uint16_t sharedOffset{kGlobal};

  // Array of flat base values. Cast to pod type or StringView.
  void* base;

  // If non-nullptr, provides index into 'base. Subscripted with the
  // blockIdx - idx of first bllock wit this instruction
  // stream. Different thread blocks may or may not have indices for
  // a given operand.
  int32_t** indices;

  // Array of null indicators. No nulls if nullptr.  A 1 means not-null, for
  // consistency with Velox.
  uint8_t* nulls{nullptr};
};

} // namespace facebook::velox::wave
