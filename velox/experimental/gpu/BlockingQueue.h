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

#pragma once

#include <cuda/semaphore>

namespace facebook::velox::gpu {

/// A blocking queue enabling synchronous communication between CPU and GPU, as
/// well as between different blocks/threads in GPU.
template <typename T, cuda::thread_scope kScope = cuda::thread_scope_system>
struct BlockingQueue {
  /// `capacity` must be a power of 2.
  __host__ __device__ BlockingQueue(T* data, size_t capacity)
      : data_(data), capacityMask_(capacity - 1), canEnqueue_(capacity) {
    assert(__builtin_popcount(capacity) == 1);
  }

  /// Enqueue a new element.  If the queue is full, block the calling thread.
  __host__ __device__ void enqueue(const T& x) {
    canEnqueue_.acquire();
    mutex_.acquire();
    auto i = (first_ + size_) & capacityMask_;
    data_[i] = x;
    ++size_;
    mutex_.release();
    canDequeue_.release();
  }

  /// Dequeue an element and return it.  If the queue is empty, block the
  /// calling thread.
  __host__ __device__ T dequeue() {
    canDequeue_.acquire();
    return doDequeue();
  }

  /// Return true if the dequeue succeeds.  Cannot return `std::optional` here
  /// because of the limitation of nvcc.
  __host__ __device__ bool tryDequeue(T& out) {
    if (!canDequeue_.try_acquire()) {
      return false;
    }
    out = doDequeue();
    return true;
  }

  size_t size() const {
    mutex_.acquire();
    auto ans = size_;
    mutex_.release();
    return ans;
  }

 private:
  __host__ __device__ T doDequeue() {
    mutex_.acquire();
    T ans = data_[first_];
    first_ = (first_ + 1) & capacityMask_;
    --size_;
    mutex_.release();
    canEnqueue_.release();
    return ans;
  }

  T* data_;
  size_t capacityMask_;
  size_t first_ = 0;
  size_t size_ = 0;
  mutable cuda::binary_semaphore<kScope> mutex_{1};
  cuda::counting_semaphore<kScope> canEnqueue_;
  cuda::counting_semaphore<kScope> canDequeue_{0};
};

} // namespace facebook::velox::gpu
