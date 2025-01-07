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
#include "velox/experimental/wave/exec/ExprKernel.h"

namespace facebook::velox::wave {

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
  void setupAggregation(
      AggregationControl& op,
      int32_t entryPoint = 0,
      CompiledKernel* kernel = nullptr);

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
