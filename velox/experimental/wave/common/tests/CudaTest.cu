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

#include "velox/experimental/wave/common/Block.cuh"
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/common/tests/CudaTest.h"

namespace facebook::velox::wave {
constexpr uint32_t kPrime32 = 1815531889;

__global__ void
addOneKernel(int32_t* numbers, int32_t size, int32_t stride, int32_t repeats) {
  for (auto counter = 0; counter < repeats; ++counter) {
    for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
         index += stride) {
      ++numbers[index];
    }
    __syncthreads();
  }
}

__global__ void addOneSharedKernel(
    int32_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  extern __shared__ __align__(16) char smem[];
  int32_t* temp = reinterpret_cast<int32_t*>(smem);
  for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
       index += stride) {
    temp[threadIdx.x] = numbers[blockDim.x * blockIdx.x + threadIdx.x];
    for (auto counter = 0; counter < repeats; ++counter) {
      ++temp[index];
    }
    __syncthreads();
    numbers[blockDim.x * blockIdx.x + threadIdx.x] = temp[threadIdx.x];
  }
}

void TestStream::addOne(
    int32_t* numbers,
    int32_t size,
    int32_t repeats,
    int32_t width) {
  constexpr int32_t kBlockSize = 256;
  auto numBlocks = roundUp(size, kBlockSize) / kBlockSize;
  int32_t stride = size;
  if (numBlocks > width / kBlockSize) {
    stride = width;
    numBlocks = width / kBlockSize;
  }
  addOneKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(
      numbers, size, stride, repeats);
  CUDA_CHECK(cudaGetLastError());
}

__global__ void addOneWideKernel(WideParams params) {
  auto numbers = params.numbers;
  auto size = params.size;
  auto repeat = params.repeat;
  auto stride = params.stride;
  for (auto counter = 0; counter < repeat; ++counter) {
    for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
         index += stride) {
      ++numbers[index];
    }
  }
}

void TestStream::addOneWide(
    int32_t* numbers,
    int32_t size,
    int32_t repeat,
    int32_t width) {
  constexpr int32_t kBlockSize = 256;
  auto numBlocks = roundUp(size, kBlockSize) / kBlockSize;
  int32_t stride = size;
  if (numBlocks > width / kBlockSize) {
    stride = width;
    numBlocks = width / kBlockSize;
  }
  WideParams params;
  params.numbers = numbers;
  params.size = size;
  params.stride = stride;
  params.repeat = repeat;
  addOneWideKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(params);
  CUDA_CHECK(cudaGetLastError());
}

void TestStream::addOneShared(
    int32_t* numbers,
    int32_t size,
    int32_t repeats,
    int32_t width) {
  constexpr int32_t kBlockSize = 256;
  auto numBlocks = roundUp(size, kBlockSize) / kBlockSize;
  int32_t stride = size;
  if (numBlocks > width / kBlockSize) {
    stride = width;
    numBlocks = width / kBlockSize;
  }
  addOneSharedKernel<<<
      numBlocks,
      kBlockSize,
      sizeof(int32_t) * kBlockSize,
      stream_->stream>>>(numbers, size, stride, repeats);
  CUDA_CHECK(cudaGetLastError());
}

__global__ void __launch_bounds__(1024) addOneRandomKernel(
    int32_t* numbers,
    const int32_t* lookup,
    uint32_t size,
    int32_t stride,
    int32_t repeats,
    bool emptyWarps,
    bool emptyThreads) {
  for (uint32_t counter = 0; counter < repeats; ++counter) {
    if (emptyWarps) {
      if (((threadIdx.x / 32) & 1) == 0) {
        for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
             index += stride) {
          auto rnd = deviceScale32(index * (counter + 1) * kPrime32, size);
          numbers[index] += lookup[rnd];
          rnd = deviceScale32((index + 32) * (counter + 1) * kPrime32, size);
          numbers[index + 32] += lookup[rnd];
        }
      }
    } else if (emptyThreads) {
      if ((threadIdx.x & 1) == 0) {
        for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
             index += stride) {
          auto rnd = deviceScale32(index * (counter + 1) * kPrime32, size);
          numbers[index] += lookup[rnd];
          rnd = deviceScale32((index + 1) * (counter + 1) * kPrime32, size);
          numbers[index + 1] += lookup[rnd];
        }
      }
    } else {
#pragma unroll
      for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
           index += stride) {
        auto rnd = deviceScale32(index * (counter + 1) * kPrime32, size);
        numbers[index] += lookup[rnd];
      }
    }
    __syncthreads();
  }
  __syncthreads();
}

void TestStream::addOneRandom(
    int32_t* numbers,
    const int32_t* lookup,
    int32_t size,
    int32_t repeats,
    int32_t width,
    bool emptyWarps,
    bool emptyThreads) {
  constexpr int32_t kBlockSize = 256;
  auto numBlocks = roundUp(size, kBlockSize) / kBlockSize;
  int32_t stride = size;
  if (numBlocks > width / kBlockSize) {
    stride = width;
    numBlocks = width / kBlockSize;
  }
  addOneRandomKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(
      numbers, lookup, size, stride, repeats, emptyWarps, emptyThreads);
  CUDA_CHECK(cudaGetLastError());
}

REGISTER_KERNEL("addOne", addOneKernel);
REGISTER_KERNEL("addOneWide", addOneWideKernel);
REGISTER_KERNEL("addOneRandom", addOneRandomKernel);

} // namespace facebook::velox::wave
