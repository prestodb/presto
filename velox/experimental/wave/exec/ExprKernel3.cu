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

#include "velox/experimental/wave/exec/ExprKernel.h"

#include <gflags/gflags.h>
#include "velox/experimental/wave/common/Block.cuh"
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/exec/Aggregate.cuh"
#include "velox/experimental/wave/exec/WaveCore.cuh"

DECLARE_bool(kernel_gdb);

namespace facebook::velox::wave {

__global__ void oneFilter(KernelParams params, int32_t pc, int32_t base) {
  PROGRAM_PREAMBLE(base);
  filterKernel(
      instruction[pc]._.filter, operands, blockBase, shared, laneStatus);
  wrapKernel(
      instruction[pc + 1]._.wrap,
      operands,
      blockBase,
      shared->numRows,
      &shared->data);
  PROGRAM_EPILOGUE();
}

} // namespace facebook::velox::wave
