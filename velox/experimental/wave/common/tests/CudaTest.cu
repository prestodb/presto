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

#include <assert.h>
#include "velox/experimental/wave/common/Block.cuh"
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/common/tests/CudaTest.h"

namespace facebook::velox::wave {
constexpr uint32_t kPrime32 = 1815531889;

struct ResultPair {
  int64_t n;
  bool f;
};

typedef ResultPair (
    *TestFunc)(int64_t data, int64_t data2, bool& flag, int32_t* ptr);

__device__ TestFunc testFuncs[2];

__device__ ResultPair
testFunc(int64_t data, int64_t data2, bool& flag, int32_t* ptr) {
  return {data + (data2 & 31), false};
}

void __global__ setupFuncs() {
  testFuncs[0] = testFunc;
  testFuncs[1] = testFunc;
}

__global__ void
incOneKernel(int32_t* numbers, int32_t size, int32_t stride, int32_t repeats) {
  for (auto counter = 0; counter < repeats; ++counter) {
    for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
         index += stride) {
      ++numbers[index];
    }
    __syncthreads();
  }
}

__global__ void
addOneKernel(int32_t* numbers, int32_t size, int32_t stride, int32_t repeats) {
  for (auto counter = 0; counter < repeats; ++counter) {
    for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
         index += stride) {
      numbers[index] += index & 31;
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
    temp[threadIdx.x] = numbers[index];
    for (auto counter = 0; counter < repeats; ++counter) {
      temp[threadIdx.x] += (index + counter) & 31;
    }
    __syncthreads();
    numbers[index] = temp[threadIdx.x];
  }
}

__global__ void addOneRegKernel(
    int32_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
       index += stride) {
    auto temp = numbers[index];
    for (auto counter = 0; counter < repeats; ++counter) {
      temp += (index + counter) & 31;
    }
    __syncthreads();
    numbers[index] = temp;
  }
}

__global__ void addOneFuncKernel(
    int32_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
       index += stride) {
    int32_t* ptr = nullptr;
    bool flag;
    auto temp = numbers[index];
    for (auto counter = 0; counter < repeats; ++counter) {
      auto result = testFuncs[counter & 1](temp, counter, flag, ptr);
      temp = result.n;
    }
    __syncthreads();
    numbers[index] = temp;
  }
}

#define TCASE(nn, m)                                 \
  case nn:                                           \
    temp = m + testFunc(temp, counter, flag, ptr).n; \
    break;

__global__ void addOneSwitchKernel(
    int32_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
       index += stride) {
    int32_t* ptr = nullptr;
    bool flag;
    auto temp = numbers[index];
    for (auto counter = 0; counter < repeats; ++counter) {
      switch (counter & 31) {
        TCASE(0, 1);
        TCASE(1, 82);
        TCASE(2, 91);
        TCASE(3, 181);
        TCASE(4, 28);
        TCASE(5, 36);
        TCASE(6, 18);
        TCASE(7, 13);
        TCASE(8, 21);
        TCASE(9, 32);
        TCASE(10, 31);
        TCASE(11, 191);
        TCASE(12, 181);
        TCASE(13, 151);
        TCASE(14, 121);
        TCASE(15, 111);
        TCASE(16, 1);
        TCASE(17, 82);
        TCASE(18, 91);
        TCASE(19, 181);
        TCASE(20, 28);
        TCASE(21, 36);
        TCASE(22, 18);
        TCASE(23, 13);
        TCASE(24, 21);
        TCASE(25, 32);
        TCASE(26, 31);
        TCASE(27, 191);
        TCASE(28, 181);
        TCASE(29, 151);
        TCASE(30, 121);
        TCASE(31, 111);
      }
    }
    __syncthreads();
    numbers[index] = temp;
  }
}

#define BTCASE(nn, m)                              \
  asm volatile("BLK" nn ":");                      \
  temp = m + testFunc(temp, counter, flag, ptr).n; \
  if (repeats < 1000000000)                        \
    goto end;

__global__ void addOneBranchKernel(
    int32_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
       index += stride) {
    int32_t* ptr = nullptr;
    bool flag;
    auto temp = numbers[index];

#pragma unroll 1
    for (auto counter = 0; counter < repeats; ++counter) {
#if 0
      if (repeats > 100000000) {
      switch (repeats) {
      case 100000001: asm volatile("bra BLK1;");
      case 100000002: asm volatile("bra BLK2;");
	break;
      }
    }
#endif
      uint32_t bits = counter & 31;
      asm volatile(
          "ts: .branchtargets BLK0, BLK1, BLK2, BLK3, BLK4, BLK5, BLK6, BLK7, BLK8, BLK9, BLK10, BLK11, BLK12, BLK13, BLK14, BLK15, BLK16, BLK17, BLK18, BLK19, BLK20, BLK21, BLK22, BLK23, BLK24, BLK25, BLK26, BLK27, BLK28, BLK29, BLK30, BLK31;");
      asm volatile("brx.idx %0, ts;" ::"r"(bits));

      BTCASE("0", 1);
      BTCASE("1", 82);
      BTCASE("2", 91);
      BTCASE("3", 181);
      BTCASE("4", 28);
      BTCASE("5", 36);
      BTCASE("6", 18);
      BTCASE("7", 13);
      BTCASE("8", 21);
      BTCASE("9", 32);
      BTCASE("10", 31);
      BTCASE("11", 191);
      BTCASE("12", 181);
      BTCASE("13", 151);
      BTCASE("14", 121);
      BTCASE("15", 111);
      BTCASE("16", 1);
      BTCASE("17", 82);
      BTCASE("18", 91);
      BTCASE("19", 181);
      BTCASE("20", 28);
      BTCASE("21", 36);
      BTCASE("22", 18);
      BTCASE("23", 13);
      BTCASE("24", 21);
      BTCASE("25", 32);
      BTCASE("26", 31);
      BTCASE("27", 191);
      BTCASE("28", 181);
      BTCASE("29", 151);
      BTCASE("30", 121);
      BTCASE("31", 111);
    end:;
    }

    __syncthreads();
    numbers[index] = temp;
  }
}

__global__ void addOneFuncStoreKernel(
    int32_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  for (auto counter = 0; counter < repeats; ++counter) {
    for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
         index += stride) {
      int32_t* ptr = nullptr;
      bool flag;
      auto temp = numbers[index];
      numbers[index] = testFuncs[counter & 1](temp, counter, flag, ptr).n;
    }
    __syncthreads();
  }
}

void TestStream::incOne(
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
  incOneKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(
      numbers, size, stride, repeats);
  CUDA_CHECK(cudaGetLastError());
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

void TestStream::addOneReg(
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
  addOneRegKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(
      numbers, size, stride, repeats);
  CUDA_CHECK(cudaGetLastError());
}

void TestStream::addOneFunc(
    int32_t* numbers,
    int32_t size,
    int32_t repeats,
    int32_t width) {
  constexpr int32_t kBlockSize = 256;
  setupFuncs<<<1, 1, 0, stream_->stream>>>();
  CUDA_CHECK(cudaGetLastError());
  auto numBlocks = roundUp(size, kBlockSize) / kBlockSize;
  int32_t stride = size;
  if (numBlocks > width / kBlockSize) {
    stride = width;
    numBlocks = width / kBlockSize;
  }
  addOneFuncKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(
      numbers, size, stride, repeats);
  CUDA_CHECK(cudaGetLastError());
}

void TestStream::addOneFuncStore(
    int32_t* numbers,
    int32_t size,
    int32_t repeats,
    int32_t width) {
  constexpr int32_t kBlockSize = 256;
  setupFuncs<<<1, 1, 0, stream_->stream>>>();
  CUDA_CHECK(cudaGetLastError());
  auto numBlocks = roundUp(size, kBlockSize) / kBlockSize;
  int32_t stride = size;
  if (numBlocks > width / kBlockSize) {
    stride = width;
    numBlocks = width / kBlockSize;
  }
  addOneFuncStoreKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(
      numbers, size, stride, repeats);
  CUDA_CHECK(cudaGetLastError());
}

void TestStream::addOneBranch(
    int32_t* numbers,
    int32_t size,
    int32_t repeats,
    int32_t width) {
  constexpr int32_t kBlockSize = 256;
  setupFuncs<<<1, 1, 0, stream_->stream>>>();
  CUDA_CHECK(cudaGetLastError());
  auto numBlocks = roundUp(size, kBlockSize) / kBlockSize;
  int32_t stride = size;
  if (numBlocks > width / kBlockSize) {
    stride = width;
    numBlocks = width / kBlockSize;
  }
  addOneBranchKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(
      numbers, size, stride, repeats);
  CUDA_CHECK(cudaGetLastError());
}

void TestStream::addOneSwitch(
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
  addOneSwitchKernel<<<numBlocks, kBlockSize, 0, stream_->stream>>>(
      numbers, size, stride, repeats);
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
      kBlockSize * sizeof(int32_t),
      stream_->stream>>>(numbers, size, stride, repeats);
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

__global__ void __launch_bounds__(1024) addOneRandomKernel(
    int32_t* numbers,
    const int32_t* lookup,
    uint32_t size,
    int32_t stride,
    int32_t repeats,
    int32_t numLocal,
    int32_t localStride,
    bool emptyWarps,
    bool emptyThreads) {
  for (uint32_t counter = 0; counter < repeats; ++counter) {
    if (emptyWarps) {
      if (((threadIdx.x / 32) & 1) == 0) {
        for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
             index += stride) {
          auto rnd = deviceScale32(index * (counter + 1) * kPrime32, size);
          auto sum = lookup[rnd];
          auto limit = min(rnd + localStride * (1 + numLocal), size);
          for (auto j = rnd + localStride; j < limit; j += localStride) {
            sum += lookup[j];
          }
          numbers[index] += sum;

          rnd = deviceScale32((index + 32) * (counter + 1) * kPrime32, size);
          sum = lookup[rnd];
          limit = min(rnd + localStride * (1 + numLocal), size);
          for (auto j = rnd + localStride; j < limit; j += localStride) {
            sum += lookup[j];
          }
          numbers[index + 32] += sum;
        }
      }
    } else if (emptyThreads) {
      if ((threadIdx.x & 1) == 0) {
        for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
             index += stride) {
          auto rnd = deviceScale32(index * (counter + 1) * kPrime32, size);
          auto sum = lookup[rnd];
          auto limit = min(rnd + localStride * (1 + numLocal), size);
          for (auto j = rnd + localStride; j < limit; j += localStride) {
            sum += lookup[j];
          }
          numbers[index] += sum;

          rnd = deviceScale32((index + 1) * (counter + 1) * kPrime32, size);
          sum = lookup[rnd];
          limit = min(rnd + localStride * (1 + numLocal), size);
          for (auto j = rnd + localStride; j < limit; j += localStride) {
            sum += lookup[j];
          }
          numbers[index + 1] += sum;
        }
      }
    } else {
      for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
           index += stride) {
        auto rnd = deviceScale32(index * (counter + 1) * kPrime32, size);
        auto sum = lookup[rnd];
        auto limit = min(rnd + localStride * (1 + numLocal), size);
        for (auto j = rnd + localStride; j < limit; j += localStride) {
          sum += lookup[j];
        }
        numbers[index] += sum;
      }
    }
    __syncthreads();
  }
  __syncthreads();
}

__global__ void __launch_bounds__(1024) addOneRandomPrefetchKernel(
    int32_t* numbers,
    const int32_t* lookup,
    uint32_t size,
    int32_t stride,
    int32_t repeats,
    int32_t numLocal,
    int32_t localStride,
    bool emptyWarps,
    bool emptyThreads) {
  for (uint32_t counter = 0; counter < repeats; ++counter) {
    if (emptyWarps) {
      if (((threadIdx.x / 32) & 1) == 0) {
        for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
             index += stride) {
          auto rnd = deviceScale32(index * (counter + 1) * kPrime32, size);
          auto sum = lookup[rnd];
          auto limit = min(rnd + localStride * (1 + numLocal), size);
          for (auto j = rnd + localStride; j < limit; j += localStride) {
            sum += lookup[j];
          }
          numbers[index] += sum;

          rnd = deviceScale32((index + 32) * (counter + 1) * kPrime32, size);
          sum = lookup[rnd];
          limit = min(rnd + localStride * (1 + numLocal), size);
          for (auto j = rnd + localStride; j < limit; j += localStride) {
            sum += lookup[j];
          }
          numbers[index + 32] += sum;
        }
      }
    } else if (emptyThreads) {
      if ((threadIdx.x & 1) == 0) {
        for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
             index += stride) {
          auto rnd = deviceScale32(index * (counter + 1) * kPrime32, size);
          auto sum = lookup[rnd];
          auto limit = min(rnd + localStride * (1 + numLocal), size);
          for (auto j = rnd + localStride; j < limit; j += localStride) {
            sum += lookup[j];
          }
          numbers[index] += sum;

          rnd = deviceScale32((index + 1) * (counter + 1) * kPrime32, size);
          sum = lookup[rnd];
          limit = min(rnd + localStride * (1 + numLocal), size);
          for (auto j = rnd + localStride; j < limit; j += localStride) {
            sum += lookup[j];
          }
          numbers[index + 1] += sum;
        }
      }
    } else {
      for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
           index += stride) {
        auto rnd = deviceScale32(index * (counter + 1) * kPrime32, size);
        auto sum = lookup[rnd];
        auto limit = min(rnd + localStride * (1 + numLocal), size);
        for (auto j = rnd + localStride; j < limit; j += localStride) {
          sum += lookup[j];
        }
        numbers[index] += sum;
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
    int32_t numLocal,
    int32_t localStride,
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
      numbers,
      lookup,
      size,
      stride,
      repeats,
      numLocal,
      localStride,
      emptyWarps,
      emptyThreads);
  CUDA_CHECK(cudaGetLastError());
}

/// Memory width and stride
__global__ void addOne64Kernel(
    int64_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  for (auto counter = 0; counter < repeats; ++counter) {
    for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
         index += stride) {
      numbers[index] += index & 31;
    }
    __syncthreads();
  }
}

__global__ void addOne4x64ConsecKernel(
    int64_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  for (auto counter = 0; counter < repeats; ++counter) {
    for (auto index = 4 * (blockDim.x * blockIdx.x + threadIdx.x); index < size;
         index += stride) {
      long2 l1 = *addCast<long2>(numbers, sizeof(int64_t) * index);
      long2 l2 = *addCast<long2>(numbers, sizeof(int64_t) * (index + 2));
      l1.x += index & 31;
      l1.y += (index + 1) & 31;
      l2.x += (index + 2) & 31;
      l2.y += (index + 3) & 31;
      *addCast<long2>(numbers, sizeof(int64_t) * index) = l1;
      *addCast<long2>(numbers, sizeof(int64_t) * (index + 2)) = l2;
    }
    __syncthreads();
  }
}

__global__ void addOne1x64Kernel(
    int64_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  int32_t stride1 = stride / 4;
  for (auto counter = 0; counter < repeats; ++counter) {
    for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
         index += stride1) {
      numbers[index] += 31 & index;
    }
    __syncthreads();
  }
}

__global__ void addOne4Ox64Kernel(
    int64_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  int32_t stride1 = stride / 4;
  for (auto counter = 0; counter < repeats; ++counter) {
    for (auto index = blockDim.x * blockIdx.x + threadIdx.x; index < size;
         index += stride) {
      numbers[index] += 31 & index;
      numbers[index + stride1] += 31 & index;
      numbers[index + stride1 * 2] += 31 & index;
      numbers[index + stride1 * 3] += 31 & index;
    }
    __syncthreads();
  }
}

__global__ void addOne4x64CoaKernel(
    int64_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  int32_t halfStride = stride / 2;
  for (auto counter = 0; counter < repeats; ++counter) {
    for (auto index = 4 * (blockDim.x * blockIdx.x + threadIdx.x); index < size;
         index += stride) {
      long2 l1 = *addCast<long2>(numbers, sizeof(int64_t) * index);
      long2 l2 =
          *addCast<long2>(numbers, sizeof(int64_t) * (index + halfStride));
      l1.x += index & 31;
      l1.y += (index + 1) & 31;
      l2.x += (index + halfStride) & 31;
      l2.y += (index + halfStride + 1) & 31;
      *addCast<long2>(numbers, sizeof(int64_t) * index) = l1;
      *addCast<long2>(numbers, sizeof(int64_t) * (index + halfStride)) = l2;
    }
    __syncthreads();
  }
}

struct Params1 {
  int64_t n;
  bool flag;
};

struct Params4 {
  long2 l1;
  long2 l2;
  int32_t flags;
};

typedef Params4 (*TestFunc4)(Params4 params, int32_t index, long* args);

__device__ Params4 testFunc4(Params4 params, int32_t index, long* args) {
  params.l1.x += 32 & index;
  params.l1.y = 31 & (index + 1);
  params.l2.x += 31 & (index + 2);
  params.l2.y += 31 & (index + 3);
  return params;
}

__device__ TestFunc4 testFuncs4[2];

typedef void (*TestFunc4SMem)(int64_t* params, int32_t index, int64_t* args);
__device__ void testFunc4SMem(int64_t* smem, int32_t index, long* args) {
  long2 l1 = *addCast<long2>(smem, 0);
  long2 l2 = *addCast<long2>(smem, 16);
  l1.x += 32 & index;
  l1.y = 31 & (index + 1);
  l2.x += 31 & (index + 2);
  l2.y += 31 & (index + 3);
  *addCast<long2>(smem, 0) = l1;
  *addCast<long2>(smem, 16) = l2;
}

__device__ TestFunc4SMem testFuncs4SMem[2];

__global__ void setupFuncs4() {
  testFuncs[0] = testFunc;
  testFuncs[1] = testFunc;
  testFuncs4[0] = testFunc4;
  testFuncs4[1] = testFunc4;
  testFuncs4SMem[0] = testFunc4SMem;
  testFuncs4SMem[1] = testFunc4SMem;
}

__global__ void addOne4x64RegKernel(
    int64_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  int32_t halfStride = stride / 2;
  for (auto index = 4 * (blockDim.x * blockIdx.x + threadIdx.x); index < size;
       index += stride) {
    long2 l1 = *addCast<long2>(numbers, sizeof(int64_t) * index);
    long2 l2 = *addCast<long2>(numbers, sizeof(int64_t) * (index + halfStride));

    for (auto counter = 0; counter < repeats; ++counter) {
      l1.x += index & 31;
      l1.y += (index + 1) & 31;
      l2.x += (index + halfStride) & 31;
      l2.y += (index + halfStride + 1) & 31;
    }
    *addCast<long2>(numbers, sizeof(int64_t) * (index)) = l1;
    *addCast<long2>(numbers, sizeof(int64_t) * (index + halfStride)) = l2;

    __syncthreads();
  }
}

#define BTCASE4(nn, m)                            \
  asm volatile("BLK" nn ":");                     \
  params = testFunc4(params, index + m, nullptr); \
  if (repeats < 1000000000)                       \
    goto end;

void __global__ __launch_bounds__(1024) addOne4x64BranchKernel(
    int64_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  int32_t halfStride = stride / 2;
  for (auto index = 4 * (blockDim.x * blockIdx.x + threadIdx.x); index < size;
       index += stride) {
    Params4 params;
    params.l1 = *addCast<long2>(numbers, sizeof(int64_t) * index);
    params.l2 =
        *addCast<long2>(numbers, sizeof(int64_t) * (index + halfStride));
    for (auto counter = 0; counter < repeats; ++counter) {
      asm volatile(
          "ts: .branchtargets BLK0, BLK1, BLK2, BLK3, BLK4, BLK5, BLK6, BLK7, BLK8, BLK9, BLK10, BLK11, BLK12, BLK13, BLK14, BLK15, BLK16, BLK17, BLK18, BLK19, BLK20, BLK21, BLK22, BLK23, BLK24, BLK25, BLK26, BLK27, BLK28, BLK29, BLK30, BLK31;");
      asm volatile("brx.idx %0, ts;" ::"r"(counter & 31));

      BTCASE4("0", 1);
      BTCASE4("1", 82);
      BTCASE4("2", 91);
      BTCASE4("3", 181);
      BTCASE4("4", 28);
      BTCASE4("5", 36);
      BTCASE4("6", 18);
      BTCASE4("7", 13);
      BTCASE4("8", 21);
      BTCASE4("9", 32);
      BTCASE4("10", 31);
      BTCASE4("11", 191);
      BTCASE4("12", 181);
      BTCASE4("13", 151);
      BTCASE4("14", 121);
      BTCASE4("15", 111);
      BTCASE4("16", 1);
      BTCASE4("17", 82);
      BTCASE4("18", 91);
      BTCASE4("19", 181);
      BTCASE4("20", 28);
      BTCASE4("21", 36);
      BTCASE4("22", 18);
      BTCASE4("23", 13);
      BTCASE4("24", 21);
      BTCASE4("25", 32);
      BTCASE4("26", 31);
      BTCASE4("27", 191);
      BTCASE4("28", 181);
      BTCASE4("29", 151);
      BTCASE4("30", 121);
      BTCASE4("31", 111);
    end:;
    }
    *addCast<long2>(numbers, sizeof(int64_t) * (index)) = params.l1;
    *addCast<long2>(numbers, sizeof(int64_t) * (index + halfStride)) =
        params.l2;

    __syncthreads();
  }
}

__global__ void addOne4x64FuncKernel(
    int64_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  int32_t halfStride = stride / 2;
  for (auto index = 4 * (blockDim.x * blockIdx.x + threadIdx.x); index < size;
       index += stride) {
    Params4 params;
    params.l1 = *addCast<long2>(numbers, sizeof(int64_t) * index);
    params.l2 =
        *addCast<long2>(numbers, sizeof(int64_t) * (index + halfStride));
    for (auto counter = 0; counter < repeats; ++counter) {
      params = testFuncs4[counter & 1](params, index, nullptr);
    }
    *addCast<long2>(numbers, sizeof(int64_t) * (index)) = params.l1;
    *addCast<long2>(numbers, sizeof(int64_t) * (index + halfStride)) =
        params.l2;

    __syncthreads();
  }
}

__global__ void addOne1x64FuncKernel(
    int64_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  int32_t stride1 = stride / 4;
  for (auto index = (blockDim.x * blockIdx.x + threadIdx.x); index < size;
       index += stride1) {
    ResultPair r;
    r.n = numbers[index];
    bool flag;
    for (auto counter = 0; counter < repeats; ++counter) {
      r = testFuncs[counter & 1](r.n, 0, flag, nullptr);
    }
    numbers[index] = r.n;
    __syncthreads();
  }
}

__global__ void addOne4x64SMemFuncKernel(
    int64_t* numbers,
    int32_t size,
    int32_t stride,
    int32_t repeats) {
  int32_t halfStride = stride / 2;
  extern __shared__ __align__(16) char smemBase[];

  for (auto index = 4 * (blockDim.x * blockIdx.x + threadIdx.x); index < size;
       index += stride) {
    long2* smem = addCast<long2>(smemBase, threadIdx.x * 4 * sizeof(int64_t));
    *smem = *addCast<long2>(numbers, sizeof(int64_t) * index);
    smem[1] = *addCast<long2>(numbers, sizeof(int64_t) * (index + halfStride));
    for (auto counter = 0; counter < repeats; ++counter) {
      testFuncs4SMem[counter & 1](
          reinterpret_cast<int64_t*>(smem), index, nullptr);
    }
    *addCast<long2>(numbers, sizeof(int64_t) * (index)) = smem[0];
    *addCast<long2>(numbers, sizeof(int64_t) * (index + halfStride)) = smem[1];
    __syncthreads();
  }
}

void TestStream::addOne4x64(
    int64_t* numbers,
    int32_t size,
    int32_t repeats,
    int32_t width,
    Add64Mode mode) {
  constexpr int32_t kBlockSize = 256;
  constexpr int32_t kNumPerThread = 4;
  setupFuncs4<<<1, 1, 0, stream_->stream>>>();
  CUDA_CHECK(cudaGetLastError());
  int32_t numThreads = size / kNumPerThread;
  auto numBlocks = roundUp(size / kNumPerThread, kBlockSize) / kBlockSize;
  int32_t stride = numThreads;
  if (numBlocks > (width / (kBlockSize * kNumPerThread))) {
    stride = width * kNumPerThread;
    numBlocks = width / kBlockSize;
  }
  int32_t smem = 0;
  if (mode == Add64Mode::k4SMem || mode == Add64Mode::k4SMemFunc) {
    smem = 5 * kBlockSize * sizeof(int64_t);
  }
  switch (mode) {
    case Add64Mode::k4Seq:
      addOne4x64ConsecKernel<<<numBlocks, kBlockSize, smem, stream_->stream>>>(
          numbers, size, stride, repeats);
      break;
    case Add64Mode::k4Add:
      addOne4Ox64Kernel<<<numBlocks, kBlockSize, smem, stream_->stream>>>(
          numbers, size, stride, repeats);
      break;

    case Add64Mode::k4Coa:
      addOne4x64CoaKernel<<<numBlocks, kBlockSize, smem, stream_->stream>>>(
          numbers, size, stride, repeats);
      break;
    case Add64Mode::k4Reg:
      addOne4x64RegKernel<<<numBlocks, kBlockSize, smem, stream_->stream>>>(
          numbers, size, stride, repeats);
      break;
    case Add64Mode::k4Branch:
      addOne4x64BranchKernel<<<numBlocks, kBlockSize, smem, stream_->stream>>>(
          numbers, size, stride, repeats);
      break;

    case Add64Mode::k4Func:
      addOne4x64FuncKernel<<<numBlocks, kBlockSize, smem, stream_->stream>>>(
          numbers, size, stride, repeats);
      break;
    case Add64Mode::k1Func:
      addOne1x64FuncKernel<<<numBlocks, kBlockSize, smem, stream_->stream>>>(
          numbers, size, stride, repeats);
      break;

    case Add64Mode::k4SMemFunc:
      addOne4x64SMemFuncKernel<<<
          numBlocks,
          kBlockSize,
          smem,
          stream_->stream>>>(numbers, size, stride, repeats);
      break;
    case Add64Mode::k1Add:
      addOne1x64Kernel<<<numBlocks, kBlockSize, smem, stream_->stream>>>(
          numbers, size, stride, repeats);
      break;

    default:
      assert(false);
  }
  CUDA_CHECK(cudaGetLastError());
}

REGISTER_KERNEL("addOne", addOneKernel);
REGISTER_KERNEL("addOneFunc", addOneFuncKernel);
REGISTER_KERNEL("addOneWide", addOneWideKernel);
REGISTER_KERNEL("addOneRandom", addOneRandomKernel);
REGISTER_KERNEL("add4x64branch", addOne4x64BranchKernel);
REGISTER_KERNEL("add4x64func", addOne4x64FuncKernel);
REGISTER_KERNEL("add4x64smemfunc", addOne4x64SMemFuncKernel);

} // namespace facebook::velox::wave
