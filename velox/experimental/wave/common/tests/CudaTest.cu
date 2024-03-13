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

__global__ void
addOneKernel(int32_t* numbers, int32_t size, int32_t stride, int32_t repeats) {
  auto index = blockDim.x * blockIdx.x + threadIdx.x;
  for (auto counter = 0; counter < repeats; ++counter) {
    for (; index < size; index += stride) {
      ++numbers[index];
    }
    __syncthreads();
  }
}

void TestStream::addOne(int32_t* numbers, int32_t size, int32_t repeats) {
  constexpr int32_t kWidth = 10240;
  constexpr int32_t kBlockSize = 256;
  auto numBlocks = roundUp(size, kBlockSize) / kBlockSize;
  int32_t stride = size;
  if (numBlocks > kWidth / kBlockSize) {
    stride = kWidth;
    numBlocks = kWidth / kBlockSize;
  }
  addOneKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(
      numbers, size, stride, repeats);
  CUDA_CHECK(cudaGetLastError());
}

__global__ void addOneWideKernel(WideParams params) {
  auto index = blockDim.x * blockIdx.x + threadIdx.x;
  auto numbers = params.numbers;
  auto size = params.size;
  auto repeat = params.repeat;
  auto stride = params.stride;
  for (auto counter = 0; counter < repeat; ++counter) {
    for (; index < size; index += stride) {
      ++numbers[index];
    }
  }
}

void TestStream::addOneWide(int32_t* numbers, int32_t size, int32_t repeat) {
  constexpr int32_t kWidth = 10240;
  constexpr int32_t kBlockSize = 256;
  auto numBlocks = roundUp(size, kBlockSize) / kBlockSize;
  int32_t stride = size;
  if (numBlocks > kWidth / kBlockSize) {
    stride = kWidth;
    numBlocks = kWidth / kBlockSize;
  }
  WideParams params;
  params.numbers = numbers;
  params.size = size;
  params.stride = stride;
  params.repeat = repeat;
  addOneWideKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(params);
  CUDA_CHECK(cudaGetLastError());
}

__global__ void addOneRandomKernel(
    int32_t* numbers,
    const int32_t* lookup,
    uint32_t size,
    int32_t stride,
    int32_t repeats) {
  auto index = blockDim.x * blockIdx.x + threadIdx.x;
  for (uint32_t counter = 0; counter < repeats; ++counter) {
    for (; index < size; index += stride) {
      auto rnd = (static_cast<uint64_t>(static_cast<uint32_t>(
                      index * (counter + 1) * 1367836089)) *
                  size) >>
          32;
      numbers[index] += lookup[rnd];
    }
    __syncthreads();
  }
}

void TestStream::addOneRandom(
    int32_t* numbers,
    const int32_t* lookup,
    int32_t size,
    int32_t repeats) {
  constexpr int32_t kWidth = 10240;
  constexpr int32_t kBlockSize = 256;
  auto numBlocks = roundUp(size, kBlockSize) / kBlockSize;
  int32_t stride = size;
  if (numBlocks > kWidth / kBlockSize) {
    stride = kWidth;
    numBlocks = kWidth / kBlockSize;
  }
  addOneRandomKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(
      numbers, lookup, size, stride, repeats);
  CUDA_CHECK(cudaGetLastError());
}

} // namespace facebook::velox::wave
