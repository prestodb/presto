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

#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/common/tests/CudaTest.h"

namespace facebook::velox::wave {

__global__ void addOneKernel(int32_t* numbers, int32_t size, int32_t stride) {
  auto index = blockDim.x * blockIdx.x + threadIdx.x;
  for (; index < size; index += stride) {
    ++numbers[index];
  }
}

void TestStream::addOne(int32_t* numbers, int32_t size) {
  constexpr int32_t kWidth = 10240;
  constexpr int32_t kBlockSize = 256;
  auto numBlocks = roundUp(size, kBlockSize) / kBlockSize;
  int32_t stride = size;
  if (numBlocks > kWidth / kBlockSize) {
    stride = kWidth;
    numBlocks = kWidth / kBlockSize;
  }
  addOneKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(
      numbers, size, stride);
  CUDA_CHECK(cudaGetLastError());
}

} // namespace facebook::velox::wave
