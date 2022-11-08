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

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <cub/cub.cuh> // @manual
#include <numeric>
#include "velox/experimental/gpu/Common.h"

DEFINE_int64(buffer_size, 24 << 20, "");
DEFINE_int32(repeat, 100, "");
DEFINE_string(devices, "0", "Comma-separate list of device ids");
DEFINE_bool(validate, false, "");
DEFINE_int32(num_strides, 32, "");

constexpr int kBlockSize = 256;
constexpr int kItemsPerThread = 32;
constexpr int kItemsPerBlock = kBlockSize * kItemsPerThread;

namespace facebook::velox::gpu {
namespace {

struct BlockStorage {
  using Load = cub::BlockLoad<
      int64_t,
      kBlockSize,
      kItemsPerThread,
      cub::BLOCK_LOAD_TRANSPOSE>;

  using Store = cub::BlockStore<
      int64_t,
      kBlockSize,
      kItemsPerThread,
      cub::BLOCK_STORE_TRANSPOSE>;

  using Sort = cub::BlockRadixSort<int64_t, kBlockSize, kItemsPerThread>;

  union {
    Load::TempStorage load;
    Store::TempStorage store;
    Sort::TempStorage sort;
  };
};

__global__ void sortBlock(int64_t* data) {
  extern __shared__ BlockStorage sharedStorage[];
  int64_t threadKeys[kItemsPerThread];
  int offset = blockIdx.x * kItemsPerBlock;
  BlockStorage::Load(sharedStorage[0].load).Load(data + offset, threadKeys);
  __syncthreads();
  BlockStorage::Sort(sharedStorage[0].sort).Sort(threadKeys);
  __syncthreads();
  BlockStorage::Store(sharedStorage[0].store).Store(data + offset, threadKeys);
}

__device__ uint64_t hashMix(uint64_t upper, uint64_t lower) {
  constexpr uint64_t kMul = 0x9ddfea08eb382d69ULL;
  uint64_t a = (lower ^ upper) * kMul;
  a ^= (a >> 47);
  uint64_t b = (upper ^ a) * kMul;
  b ^= (b >> 47);
  b *= kMul;
  return b;
}

__global__ void
strideMemory(const int64_t* data, int64_t count, int strides, int64_t* out) {
  using Reduce = cub::BlockReduce<int64_t, kBlockSize>;
  __shared__ Reduce::TempStorage tmp;
  auto i = threadIdx.x + blockIdx.x * blockDim.x;
  int64_t ans = 0;
  for (int j = 0; j < strides; ++j) {
    ans = hashMix(ans, data[i]);
    i = (i + data[i]) % count;
  }
  out[blockIdx.x] = Reduce(tmp).Reduce(ans, hashMix);
}

void testCudaEvent(int deviceId) {
  CUDA_CHECK_FATAL(cudaSetDevice(deviceId));
  CUDA_CHECK_FATAL(cudaFuncSetAttribute(
      sortBlock,
      cudaFuncAttributeMaxDynamicSharedMemorySize,
      sizeof(BlockStorage)));
  auto elementCount = FLAGS_buffer_size / sizeof(int64_t);
  if (!(elementCount >= kItemsPerBlock && elementCount % kItemsPerBlock == 0)) {
    abort();
  }
  int64_t* hostBuffer;
  int64_t* deviceBuffer[2];
  int64_t* outputBuffer;
  CUDA_CHECK_FATAL(cudaMallocHost(&hostBuffer, FLAGS_buffer_size));
  CUDA_CHECK_FATAL(cudaMalloc(&deviceBuffer[0], FLAGS_buffer_size));
  CUDA_CHECK_FATAL(cudaMalloc(&deviceBuffer[1], FLAGS_buffer_size));
  CUDA_CHECK_FATAL(
      cudaMalloc(&outputBuffer, sizeof(int64_t) * elementCount / kBlockSize));
  std::iota(hostBuffer, hostBuffer + elementCount, 0);
  std::reverse(hostBuffer, hostBuffer + elementCount);
  CudaStream streams[] = {
      createCudaStream(),
      createCudaStream(),
  };
  CudaEvent bufferReady[] = {
      createCudaEvent(),
      createCudaEvent(),
  };
  CudaEvent processDone[] = {
      createCudaEvent(),
      createCudaEvent(),
  };
  auto startEvent = createCudaEvent();
  CUDA_CHECK_FATAL(cudaEventRecord(startEvent.get()));
  int loading = 0;
  int processing = 1;
  for (int i = 0; i < FLAGS_repeat; ++i) {
    std::swap(loading, processing);
    if (i > 0) {
      CUDA_CHECK_FATAL(cudaEventSynchronize(processDone[loading].get()));
    }
    CUDA_CHECK_FATAL(cudaMemcpyAsync(
        deviceBuffer[loading],
        hostBuffer,
        FLAGS_buffer_size,
        cudaMemcpyHostToDevice,
        streams[loading].get()));
    CUDA_CHECK_FATAL(
        cudaEventRecord(bufferReady[loading].get(), streams[loading].get()));
    if (i > 0) {
      CUDA_CHECK_FATAL(cudaEventSynchronize(bufferReady[processing].get()));
      sortBlock<<<
          elementCount / kItemsPerBlock,
          kBlockSize,
          sizeof(BlockStorage),
          streams[processing].get()>>>(deviceBuffer[processing]);
      CUDA_CHECK_FATAL(cudaGetLastError());
      strideMemory<<<elementCount / kBlockSize, kBlockSize>>>(
          deviceBuffer[processing],
          elementCount,
          FLAGS_num_strides,
          outputBuffer);
      CUDA_CHECK_FATAL(cudaGetLastError());
    }
    CUDA_CHECK_FATAL(cudaEventRecord(
        processDone[processing].get(), streams[processing].get()));
  }
  auto stopEvent = createCudaEvent();
  CUDA_CHECK_FATAL(cudaEventRecord(stopEvent.get()));
  CUDA_CHECK_FATAL(cudaEventSynchronize(stopEvent.get()));
  float time;
  CUDA_CHECK_FATAL(
      cudaEventElapsedTime(&time, startEvent.get(), stopEvent.get()));
  printf(
      "Device %d throughput: %.2f GB/s\n",
      deviceId,
      FLAGS_buffer_size * FLAGS_repeat * 1e-6 / time);
  if (FLAGS_validate) {
    CUDA_CHECK_FATAL(cudaMemcpy(
        hostBuffer,
        deviceBuffer[processing],
        FLAGS_buffer_size,
        cudaMemcpyDeviceToHost));
    for (int64_t i = 0; i < elementCount; i += kItemsPerBlock) {
      for (int64_t j = elementCount - i - kItemsPerBlock, di = 0;
           j < elementCount - i;
           ++j, ++di) {
        if (hostBuffer[i + di] != j) {
          fprintf(
              stderr,
              "hostBuffer[%ld]: %ld != %ld\n",
              i + di,
              hostBuffer[i + di],
              j);
          abort();
        }
      }
    }
  }
  CUDA_CHECK_LOG(cudaFree(outputBuffer));
  CUDA_CHECK_LOG(cudaFree(deviceBuffer[0]));
  CUDA_CHECK_LOG(cudaFree(deviceBuffer[1]));
  CUDA_CHECK_LOG(cudaFreeHost(hostBuffer));
}

} // namespace
} // namespace facebook::velox::gpu

int main(int argc, char** argv) {
  using namespace facebook::velox::gpu;
  folly::init(&argc, &argv);
  int deviceCount;
  CUDA_CHECK_FATAL(cudaGetDeviceCount(&deviceCount));
  printf("Device count: %d\n", deviceCount);
  std::vector<int> devices;
  for (int i = 0, deviceId = 0; i < FLAGS_devices.size(); ++i) {
    char c = FLAGS_devices[i];
    if (c != ',') {
      deviceId = 10 * deviceId + (c - '0');
    }
    if (c == ',' || i + 1 == FLAGS_devices.size()) {
      devices.push_back(deviceId);
      deviceId = 0;
    }
  }
  std::vector<std::thread> threads;
  for (int deviceId : devices) {
    threads.emplace_back(testCudaEvent, deviceId);
  }
  for (auto& t : threads) {
    t.join();
  }
  return 0;
}
