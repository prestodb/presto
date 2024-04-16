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

class CudaDeviceAllocator : public GpuAllocator {
 public:
  void* allocate(size_t size) override {
    void* ret;
    CUDA_CHECK(cudaMalloc(&ret, size));
    return ret;
  }

  void free(void* ptr, size_t /*size*/) override {
    cudaFree(ptr);
  }
};

class CudaHostAllocator : public GpuAllocator {
 public:
  void* allocate(size_t size) override {
    void* ret;
    CUDA_CHECK(cudaMallocHost(&ret, size));
    return ret;
  }

  void free(void* ptr, size_t /*size*/) override {
    cudaFreeHost(ptr);
  };
};

} // namespace

GpuAllocator* getAllocator(Device* /*device*/) {
  static auto* allocator = new CudaManagedAllocator();
  return allocator;
}

GpuAllocator* getDeviceAllocator(Device* /*device*/) {
  static auto* allocator = new CudaDeviceAllocator();
  return allocator;
}
GpuAllocator* getHostAllocator(Device* /*device*/) {
  static auto* allocator = new CudaHostAllocator();
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
  stream_ = std::make_unique<StreamImpl>();
  CUDA_CHECK(cudaStreamCreate(&stream_->stream));
}

Stream::~Stream() {
  cudaStreamDestroy(stream_->stream);
}

void Stream::wait() {
  CUDA_CHECK(cudaStreamSynchronize(stream_->stream));
}

void Stream::prefetch(Device* device, void* ptr, size_t size) {
  CUDA_CHECK(cudaMemPrefetchAsync(
      ptr, size, device ? device->deviceId : cudaCpuDeviceId, stream_->stream));
}

void Stream::hostToDeviceAsync(
    void* deviceAddress,
    const void* hostAddress,
    size_t size) {
  CUDA_CHECK(cudaMemcpyAsync(
      deviceAddress,
      hostAddress,
      size,
      cudaMemcpyHostToDevice,
      stream_->stream));
}

void Stream::deviceToHostAsync(
    void* hostAddress,
    const void* deviceAddress,
    size_t size) {
  CUDA_CHECK(cudaMemcpyAsync(
      hostAddress,
      deviceAddress,
      size,
      cudaMemcpyDeviceToHost,
      stream_->stream));
}

namespace {
struct CallbackData {
  CallbackData(std::function<void()> callback)
      : callback(std::move(callback)){};
  std::function<void()> callback;
};

void readyCallback(void* voidData) {
  std::unique_ptr<CallbackData> data(reinterpret_cast<CallbackData*>(voidData));
  data->callback();
}
} // namespace

void Stream::addCallback(std::function<void()> callback) {
  auto cdata = new CallbackData(std::move(callback));
  CUDA_CHECK(cudaLaunchHostFunc(stream_->stream, readyCallback, cdata));
}

struct EventImpl {
  ~EventImpl() {
    auto err = cudaEventDestroy(event);
    if (err != cudaSuccess) {
      // Do not throw because it can shadow other more important exceptions.  As
      // a rule of thumb, we should not throw in any destructors.
      LOG(ERROR) << "cudaEventDestroy: " << cudaGetErrorString(err);
    }
  }
  cudaEvent_t event;
};

Event::Event(bool withTime) : hasTiming_(withTime) {
  event_ = std::make_unique<EventImpl>();
  CUDA_CHECK(cudaEventCreateWithFlags(
      &event_->event, withTime ? 0 : cudaEventDisableTiming));
}

Event::~Event() {}

void Event::record(Stream& stream) {
  CUDA_CHECK(cudaEventRecord(event_->event, stream.stream_->stream));
  recorded_ = true;
}

void Event::wait() {
  CUDA_CHECK(cudaEventSynchronize(event_->event));
}

bool Event::query() const {
  auto rc = cudaEventQuery(event_->event);
  if (rc == ::cudaErrorNotReady) {
    return false;
  }
  CUDA_CHECK(rc);
  return true;
}

void Event::wait(Stream& stream) {
  CUDA_CHECK(cudaStreamWaitEvent(stream.stream_->stream, event_->event));
}

/// Returns time in ms betweene 'this' and an earlier 'start'. Both events must
/// enable timing.
float Event::elapsedTime(const Event& start) const {
  float ms;
  if (!hasTiming_ || !start.hasTiming_) {
    waveError("Event timing not enabled");
  }
  CUDA_CHECK(cudaEventElapsedTime(&ms, start.event_->event, event_->event));
  return ms;
}

} // namespace facebook::velox::wave
