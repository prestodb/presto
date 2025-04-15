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

#include <assert.h>
#include <gflags/gflags.h>
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/exec/ExprKernelStream.h"
#include "velox/experimental/wave/exec/Join.cuh"
#include "velox/experimental/wave/exec/WaveCore.cuh"

DEFINE_bool(kernel_gdb, false, "Run kernels sequentially for debugging");

namespace facebook::velox::wave {

void __global__ setupAggregationKernel(AggregationControl op) {
  assert(!op.oldBuckets);
  auto* data = reinterpret_cast<DeviceAggregation*>(op.head);
  *data = DeviceAggregation();
  data->rowSize = op.rowSize;
  data->singleRow = reinterpret_cast<char*>(data + 1);
  memset(data->singleRow, 0, op.rowSize);
}

void WaveKernelStream::setupAggregation(
    AggregationControl& op,
    int32_t entryPoint,
    CompiledKernel* kernel) {
  int32_t numBlocks = 1;
  int32_t numThreads = 1;
  if (op.oldBuckets) {
    // One thread per bucket. Enough TBs for full device.
    numThreads = kBlockSize;
    numBlocks = std::min<int64_t>(
        roundUp(op.numOldBuckets, kBlockSize) / kBlockSize, 640);
  }
  if (kernel) {
    void* args = &op;
    kernel->launch(entryPoint, numBlocks, numThreads, 0, this, &args);
  } else {
    setupAggregationKernel<<<numBlocks, numThreads, 0, stream_->stream>>>(op);
  }
  wait();
}

} // namespace facebook::velox::wave
