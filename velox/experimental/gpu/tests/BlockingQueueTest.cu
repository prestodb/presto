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
#include <algorithm>
#include <mutex>
#include "velox/experimental/gpu/BlockingQueue.h"
#include "velox/experimental/gpu/Common.h"

DEFINE_int32(gpu_grid_size, 1024, "");
DEFINE_int32(host_threads, 10, "");
DEFINE_int32(queue_size, 2048, "");
DEFINE_int32(running_time_seconds, 5, "");

DEFINE_int32(
    sync_every,
    50,
    "Host threads will wait for this number of messages"
    " from GPU before send this number of messages again");

namespace facebook::velox::gpu {
namespace {

void checkDeviceProperties() {
  int device;
  CUDA_CHECK_FATAL(cudaGetDevice(&device));
  cudaDeviceProp prop;
  CUDA_CHECK_FATAL(cudaGetDeviceProperties(&prop, device));
  assert(prop.concurrentManagedAccess);
}

template <typename T, cuda::thread_scope kScope>
T* xCudaMalloc(int count) {
  T* ptr;
  if constexpr (kScope == cuda::thread_scope_system) {
    CUDA_CHECK_FATAL(cudaMallocManaged(&ptr, count * sizeof(T)));
  } else {
    CUDA_CHECK_FATAL(cudaMalloc(&ptr, count * sizeof(T)));
  }
  return ptr;
}

template <typename T, cuda::thread_scope kScope>
struct Queue {
  CudaPtr<T[]> data;
  CudaPtr<BlockingQueue<T, kScope>> queue;
};

template <typename T, cuda::thread_scope kScope>
__global__ void
initBlockingQueue(BlockingQueue<T, kScope>* queue, T* data, size_t capacity) {
  new (queue) BlockingQueue<T, kScope>(data, capacity);
}

template <typename T, cuda::thread_scope kScope>
Queue<T, kScope> createQueue(int capacity) {
  Queue<T, kScope> ans;
  ans.data.reset(xCudaMalloc<T, kScope>(capacity));
  ans.queue.reset(xCudaMalloc<BlockingQueue<T, kScope>, kScope>(1));
  if constexpr (kScope == cuda::thread_scope_system) {
    new (ans.queue.get()) BlockingQueue<T, kScope>(ans.data.get(), capacity);
  } else {
    initBlockingQueue<<<1, 1>>>(ans.queue.get(), ans.data.get(), capacity);
    CUDA_CHECK_FATAL(cudaDeviceSynchronize());
  }
  return ans;
}

std::atomic_int64_t nextId;
std::chrono::steady_clock::time_point startTime;
std::vector<int64_t> received;
std::mutex receivedMutex;

void runCpu(BlockingQueue<int64_t>* rx, BlockingQueue<int64_t>* tx) {
  int messageCount = 0;
  std::chrono::seconds duration(FLAGS_running_time_seconds);
#ifndef NDEBUG
  std::vector<int64_t> localReceived;
#endif
  for (;;) {
    auto message = rx->dequeue();
#ifndef NDEBUG
    localReceived.push_back(message);
#else
    (void)message;
#endif
    if (++messageCount < FLAGS_sync_every) {
      continue;
    }
    if (std::chrono::steady_clock::now() >= startTime + duration) {
#ifndef NDEBUG
      {
        std::lock_guard<std::mutex> lock(receivedMutex);
        (void)lock;
        std::copy(
            localReceived.begin(),
            localReceived.end(),
            std::back_inserter(received));
      }
#else
      (void)receivedMutex;
#endif
      return;
    }
    messageCount = 0;
    for (int i = 0; i < FLAGS_sync_every; ++i) {
      tx->enqueue(nextId++);
    }
  }
}

__global__ void echo(BlockingQueue<int64_t>& rx, BlockingQueue<int64_t>& tx) {
  __shared__ int64_t message;
  for (;;) {
    if (threadIdx.x == 0) {
      message = rx.dequeue();
    }
    __syncthreads();
    if (message == -1) {
      return;
    }
    if (threadIdx.x == 0) {
      tx.enqueue(message);
    }
  }
}

void runCpuGpuPingPong() {
  auto cpu2gpu =
      createQueue<int64_t, cuda::thread_scope_system>(FLAGS_queue_size);
  auto gpu2cpu =
      createQueue<int64_t, cuda::thread_scope_system>(FLAGS_queue_size);
  for (int i = 0; i < FLAGS_queue_size; ++i) {
    cpu2gpu.queue->enqueue(nextId++);
  }
  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_host_threads; ++i) {
    threads.emplace_back(runCpu, gpu2cpu.queue.get(), cpu2gpu.queue.get());
  }
  startTime = std::chrono::steady_clock::now();
  echo<<<FLAGS_gpu_grid_size, 1>>>(*cpu2gpu.queue, *gpu2cpu.queue);
  for (auto& t : threads) {
    t.join();
  }
  for (int i = 0; i < FLAGS_gpu_grid_size; ++i) {
    cpu2gpu.queue->enqueue(-1);
  }
  CUDA_CHECK_FATAL(cudaDeviceSynchronize());
  auto endTime = std::chrono::steady_clock::now();
  assert(cpu2gpu.queue->size() == 0);
  size_t remaining = 0;
  int64_t message;
  while (gpu2cpu.queue->tryDequeue(message)) {
#ifndef NDEBUG
    received.push_back(message);
#endif
    ++remaining;
  }
  printf("CPU-GPU Ping Pong\n");
  printf("=================\n");
  printf("Total send: %lu\n", nextId.load());
  printf("Total received: %lu\n", nextId - remaining);
  printf(
      "%.2f ns per element\n",
      1.0 * (endTime - startTime).count() / (nextId - remaining));
  assert(gpu2cpu.queue->size() == 0);
  assert(received.size() == nextId);
#ifndef NDEBUG
  std::sort(received.begin(), received.end());
  for (int64_t i = 0; i < nextId; ++i) {
    if (received[i] != i) {
      printf("received[%ld] = %ld\n", i, received[i]);
      abort();
    }
  }
#else
  (void)received;
#endif
}

__global__ void runGpu2Gpu(
    BlockingQueue<int64_t, cuda::thread_scope_device>& q1,
    BlockingQueue<int64_t, cuda::thread_scope_device>& q2,
    cuda::atomic<int64_t, cuda::thread_scope_device>& nextId,
    int capacity,
    int limit) {
  __shared__ struct {
    int64_t message;
  } shared;
  if (blockIdx.x == 0) {
    nextId = 0;
    for (int i = 0; i < capacity; ++i) {
      q2.enqueue(nextId++);
    }
  }
  if (blockIdx.x % 2 == 0) {
    // Read from q1.
    for (;;) {
      if (threadIdx.x == 0) {
        shared.message = q1.dequeue();
      }
      __syncthreads();
      if (nextId > limit) {
        q2.enqueue(-1);
        return;
      }
      q2.enqueue(nextId++);
    }
  } else {
    // Read from q2.
    for (;;) {
      if (threadIdx.x == 0) {
        shared.message = q2.dequeue();
      }
      __syncthreads();
      if (shared.message == -1) {
        return;
      }
      if (threadIdx.x == 0) {
        q1.enqueue(shared.message);
      }
    }
  }
}

void runGpuGpuPingPong() {
  constexpr int kLimit = 40'000;
  auto gpu1 = createQueue<int64_t, cuda::thread_scope_device>(FLAGS_queue_size);
  auto gpu2 = createQueue<int64_t, cuda::thread_scope_device>(FLAGS_queue_size);
  CudaPtr<cuda::atomic<int64_t, cuda::thread_scope_device>> nextId(
      xCudaMalloc<
          cuda::atomic<int64_t, cuda::thread_scope_device>,
          cuda::thread_scope_device>(1));
  startTime = std::chrono::steady_clock::now();
  runGpu2Gpu<<<FLAGS_gpu_grid_size, 1>>>(
      *gpu1.queue, *gpu2.queue, *nextId, FLAGS_queue_size, kLimit);
  CUDA_CHECK_FATAL(cudaDeviceSynchronize());
  auto endTime = std::chrono::steady_clock::now();
  printf("GPU-GPU Ping Pong\n");
  printf("=================\n");
  printf("%.2f ns per element\n", 1.0 * (endTime - startTime).count() / kLimit);
}

__global__ void runSimpleKernel(int64_t m, int64_t& out) {
  __shared__ int64_t message;
  if (threadIdx.x == 0) {
    message = m;
  }
  __syncthreads();
  out ^= message ^ threadIdx.x;
}

void runKernelLaunches() {
  startTime = std::chrono::steady_clock::now();
  CudaPtr<int64_t> out(xCudaMalloc<int64_t, cuda::thread_scope_system>(1));
  for (int i = 0; i < nextId; ++i) {
    runSimpleKernel<<<1, 1>>>(i, *out);
  }
  CUDA_CHECK_FATAL(cudaDeviceSynchronize());
  printf("Simple Kernel Launch\n");
  printf("====================\n");
  printf(
      "%.2f ns per kernel launch\n",
      1.0 * (std::chrono::steady_clock::now() - startTime).count() / nextId);
}

} // namespace
} // namespace facebook::velox::gpu

int main(int argc, char** argv) {
  using namespace facebook::velox::gpu;
  folly::Init init{&argc, &argv};
  checkDeviceProperties();
  runCpuGpuPingPong();
  printf("\n");
#ifdef NDEBUG
  // For unknown reason BlockingQueue constructor is not called in dev mode.
  runGpuGpuPingPong();
  printf("\n");
#endif
  runKernelLaunches();
  return 0;
}
