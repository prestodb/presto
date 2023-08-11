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

#include <cuda_runtime.h>
#include <fmt/format.h>
#include "velox/experimental/wave/common/Cuda.h"
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/common/Exception.h"

namespace facebook::velox::wave {

void cudaCheck(cudaError_t err, const char* file, int line) {
  if (err == cudaSuccess) {
    return;
  }
  waveError(
      fmt::format("Cuda error: {}:{} {}", file, line, cudaGetErrorString(err)));
}

namespace {
class CudaManagedAllocator : public GpuAllocator {
 public:
  void* allocate(size_t size) override {
    void* ret;
    CUDA_CHECK(cudaMallocManaged(&ret, size));
    return ret;
  }

  void free(void* ptr, size_t /*size*/) override {
    cudaFree(ptr);
  }
};
} // namespace

GpuAllocator* getAllocator(Device* /*device*/) {
  static auto* allocator = new CudaManagedAllocator();
  return allocator;
}

// Always returns device 0.
Device* getDevice(int32_t /*preferredDevice*/) {
  static Device device(0);
  return &device;
}

void setDevice(Device* device) {
  CUDA_CHECK(cudaSetDevice(device->deviceId));
}

Stream::Stream() {
  stream = std::make_unique<StreamImpl>();
  CUDA_CHECK(cudaStreamCreate(&stream->stream));
}

Stream::~Stream() {
  cudaStreamDestroy(stream->stream);
}

void Stream::wait() {
  CUDA_CHECK(cudaStreamSynchronize(stream->stream));
}

void Stream::prefetch(Device* device, void* ptr, size_t size) {
  CUDA_CHECK(cudaMemPrefetchAsync(
      ptr, size, device ? device->deviceId : cudaCpuDeviceId, stream->stream));
}

} // namespace facebook::velox::wave
