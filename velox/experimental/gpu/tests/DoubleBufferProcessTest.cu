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
#include <thread>
#include "velox/experimental/gpu/Common.h"

DEFINE_int64(buffer_size, 32 << 20, "");
DEFINE_int32(repeat, 10, "");
DEFINE_string(devices, "0", "Comma-separate list of device ids");
DEFINE_bool(validate, false, "");
DEFINE_int32(num_strides, 32, "");
DEFINE_int32(num_cpu_threads, 128, "");

constexpr int kBlockSize = 256;

namespace facebook::velox::gpu {
namespace {

__host__ __device__ uint64_t hashMix(uint64_t upper, uint64_t lower) {
  return upper ^ lower;
  constexpr uint64_t kMul = 0x9ddfea08eb382d69ULL;
  uint64_t a = (lower ^ upper) * kMul;
  a ^= (a >> 47);
  uint64_t b = (upper ^ a) * kMul;
  b ^= (b >> 47);
  b *= kMul;
  return b;
}

__global__ void strideMemory(const int64_t* data, int strides, int64_t* out) {
  using Reduce = cub::BlockReduce<int64_t, kBlockSize>;
  __shared__ Reduce::TempStorage tmp;
  int64_t i = threadIdx.x + 1ll * blockIdx.x * blockDim.x;
  auto ans = i;
  for (int j = 0; j < strides; ++j, i = data[i]) {
    ans = hashMix(ans, data[i]);
  }
  out[blockIdx.x] = Reduce(tmp).Reduce(ans, hashMix);
}

void cpuStrideMemory(
    const int64_t* data,
    int64_t size,
    int strides,
    int ti,
    int threadCount,
    int64_t* out) {
  auto ans = 0;
  for (int64_t i0 = ti; i0 < size; i0 += threadCount) {
    auto i = i0;
    ans = hashMix(ans, i);
    for (int j = 0; j < strides; ++j, i = data[i]) {
      ans = hashMix(ans, data[i]);
    }
  }
  out[ti] = ans;
}

void testCudaEvent(int deviceId) {
  CUDA_CHECK_FATAL(cudaSetDevice(deviceId));
  auto elementCount = FLAGS_buffer_size / sizeof(int64_t);
  if (__builtin_popcount(elementCount) != 1) {
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
  for (int64_t i = 0, j = 1; j <= elementCount; ++j) {
    hostBuffer[i] = (i + j) % elementCount;
    i = hostBuffer[i];
  }
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
      strideMemory<<<elementCount / kBlockSize, kBlockSize>>>(
          deviceBuffer[processing], FLAGS_num_strides, outputBuffer);
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
      "Device %d memcpy throughput: %.2f GB/s\n",
      deviceId,
      FLAGS_buffer_size * FLAGS_repeat * 1e-6 / time);

  CUDA_CHECK_FATAL(cudaEventRecord(startEvent.get()));
  for (int i = 0; i < FLAGS_repeat; ++i) {
    strideMemory<<<elementCount / kBlockSize, kBlockSize>>>(
        deviceBuffer[processing], FLAGS_num_strides, outputBuffer);
  }
  CUDA_CHECK_FATAL(cudaEventRecord(stopEvent.get()));
  CUDA_CHECK_FATAL(cudaEventSynchronize(stopEvent.get()));
  CUDA_CHECK_FATAL(
      cudaEventElapsedTime(&time, startEvent.get(), stopEvent.get()));
  printf(
      "Device %d device memory random read throughput: %.2f GB/s\n",
      deviceId,
      FLAGS_buffer_size * FLAGS_num_strides * FLAGS_repeat * 1e-6 / time);

  CUDA_CHECK_FATAL(cudaEventRecord(startEvent.get()));
  if (elementCount % FLAGS_num_cpu_threads != 0) {
    fprintf(stderr, "%ld %% %d != 0\n", elementCount, FLAGS_num_cpu_threads);
    abort();
  }
  std::vector<int64_t> hostOutputBuffer(FLAGS_num_cpu_threads);
  for (int i = 0; i < FLAGS_repeat; ++i) {
    std::vector<std::thread> threads;
    for (int j = 0; j < FLAGS_num_cpu_threads; ++j) {
      threads.emplace_back(
          cpuStrideMemory,
          hostBuffer,
          elementCount,
          FLAGS_num_strides,
          j,
          FLAGS_num_cpu_threads,
          hostOutputBuffer.data());
    }
    for (auto& t : threads) {
      t.join();
    }
  }
  CUDA_CHECK_FATAL(cudaEventRecord(stopEvent.get()));
  CUDA_CHECK_FATAL(cudaEventSynchronize(stopEvent.get()));
  CUDA_CHECK_FATAL(
      cudaEventElapsedTime(&time, startEvent.get(), stopEvent.get()));
  printf(
      "Device %d host memory random read throughput: %.2f GB/s\n",
      deviceId,
      FLAGS_buffer_size * FLAGS_num_strides * FLAGS_repeat * 1e-6 / time);

  CUDA_CHECK_LOG(cudaFree(outputBuffer));
  CUDA_CHECK_LOG(cudaFree(deviceBuffer[0]));
  CUDA_CHECK_LOG(cudaFree(deviceBuffer[1]));
  CUDA_CHECK_LOG(cudaFreeHost(hostBuffer));
}

} // namespace
} // namespace facebook::velox::gpu

int main(int argc, char** argv) {
  using namespace facebook::velox::gpu;
  folly::Init init{&argc, &argv};
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
