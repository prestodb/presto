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

/*
 * Copyright (c) 2024 by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include <hip/hip_runtime.h>

struct HipSpecialization {
  template <typename T>
  static __device__ __forceinline__ int count_leading_zeros(T value);
  template <typename T>
  static __device__ __forceinline__ int population_count(T value);
  template <typename T>
  static __device__ __forceinline__ T extract_bits(T value, int start_bit,
                                                   int num_bits);
};

template <int HIP_BLOCK_THREADS, int HIP_WARP_THREADS>
struct HipPlatform {
  enum {
    BLOCK_THREADS = HIP_BLOCK_THREADS,
    WARP_THREADS = HIP_WARP_THREADS,
  };
  __device__ __forceinline__ int thread_idx() { return threadIdx.x; }
  __device__ __forceinline__ int block_idx() { return blockIdx.x; }
  __device__ __forceinline__ void syncthreads() { __syncthreads(); }
  __device__ __forceinline__ void syncwarp() { __syncwarp(); }
  __device__ __forceinline__ int lane_idx() {
    return threadIdx.x % WARP_THREADS;
  }
  __device__ __forceinline__ int warp_idx() {
    return threadIdx.x / WARP_THREADS;
  }
  __device__ __forceinline__ unsigned lower_rank_lanemask() {
    return (1 << lane_idx()) - 1;
  }
  __device__ __forceinline__ unsigned higher_rank_lanemask() {
    static_assert(WARP_THREADS <= sizeof(unsigned) * 8,
                  "WARP_THREADS must be less or equal to unsigned bits");
    unsigned lane_mask = 1 << lane_idx();
    return ~((lane_mask - 1) | lane_mask);
  }
  __device__ __forceinline__ void reconvergence_hint() { __syncwarp(); }
  template <typename T>
  __device__ __forceinline__ T min(T lhs, T rhs) {
    return ::min(lhs, rhs);
  }
  template <typename T>
  __device__ __forceinline__ T max(T lhs, T rhs) {
    return ::max(lhs, rhs);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ T atomic_load(SliceT address) {
    return *reinterpret_cast<volatile T *>(address.data());
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ void atomic_store(SliceT address, T value) {
    *reinterpret_cast<volatile T *>(address.data()) = value;
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ T atomic_cas(SliceT address, T compare, T value) {
    return atomicCAS(address.data(), compare, value);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ T atomic_add(SliceT address, T value) {
    return atomicAdd(address.data(), value);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ void atomic_min(SliceT address, T value) {
    atomicMin(address.data(), value);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ void atomic_max(SliceT address, T value) {
    atomicMax(address.data(), value);
  }
  template <typename T>
  __device__ __forceinline__ T reduce_add(T value) {
#pragma unroll
    for (int offset = WARP_THREADS / 2; offset > 0; offset /= 2) {
      value += __shfl_down(value, offset);
    }
    return value;
  }
  template <typename T>
  __device__ __forceinline__ T reduce_min(T value) {
#pragma unroll
    for (int offset = WARP_THREADS / 2; offset > 0; offset /= 2) {
      value = ::min(value, __shfl_down(value, offset));
    }
    return value;
  }
  template <typename T>
  __device__ __forceinline__ T reduce_max(T value) {
#pragma unroll
    for (int offset = WARP_THREADS / 2; offset > 0; offset /= 2) {
      value = ::max(value, __shfl_down(value, offset));
    }
    return value;
  }
  template <typename T>
  __device__ __forceinline__ T scan_add(T value) {
#pragma unroll
    for (int offset = 1; offset < WARP_THREADS; offset <<= 1) {
      T result = __shfl_up_sync(0xffffffff, value, offset);
      if ((threadIdx.x % WARP_THREADS) >= offset) {
        value += result;
      }
    }
    return value;
  }
  template <typename T>
  __device__ __forceinline__ T ballot(bool value) {
    return __ballot_sync(0xffffffff, value);
  }
  template <int MIN_BITS, typename T>
  __device__ __forceinline__ unsigned match_any(T value) {
    unsigned result;
#pragma unroll
    for (unsigned i = 0; i < MIN_BITS; ++i) {
      unsigned current_bit = 1 << i;
      bool pred = (value & current_bit) == current_bit;
      unsigned mask = __ballot_sync(0xffffffff, pred);
      if (!pred) {
        mask = ~mask;
      }
      result = (i == 0) ? mask : result & mask;
    }
    return result;
  }
  template <typename T>
  __device__ __forceinline__ int count_leading_zeros(T value) {
    return HipSpecialization::count_leading_zeros(value);
  }
  template <typename T>
  __device__ __forceinline__ int population_count(T value) {
    return HipSpecialization::population_count(value);
  }
  template <typename T>
  __device__ __forceinline__ T extract_bits(T value, int start_bit,
                                            int num_bits) {
    return HipSpecialization::extract_bits(value, start_bit, num_bits);
  }
  template <bool HIGH_PRIORITY>
  __device__ __forceinline__ void scheduling_hint() {}
  template <typename SliceT>
  __device__ __forceinline__ void prefetch(SliceT) {}
};

// specialization for T=unsigned
template <>
__device__ __forceinline__ int HipSpecialization::count_leading_zeros<unsigned>(
    unsigned value) {
  return __clz(value);
}

// specialization for T=unsigned long long
template <>
__device__ __forceinline__ int
HipSpecialization::count_leading_zeros<unsigned long long>(
    unsigned long long value) {
  return __clzll(value);
}

// specialization for T=unsigned
template <>
__device__ __forceinline__ int HipSpecialization::population_count<unsigned>(
    unsigned value) {
  return __popc(value);
}

// specialization for T=unsigned long long
template <>
__device__ __forceinline__ int
HipSpecialization::population_count<unsigned long long>(
    unsigned long long value) {
  return __popcll(value);
}

// specialization for T=unsigned
template <>
__device__ __forceinline__ unsigned HipSpecialization::extract_bits(
    unsigned value, int start_bit, int num_bits) {
  unsigned mask = (1u << num_bits) - 1;
  return (value >> start_bit) & mask;
}

// specialization for T=unsigned long long
template <>
__device__ __forceinline__ unsigned long long HipSpecialization::extract_bits(
    unsigned long long value, int start_bit, int num_bits) {
  unsigned long long mask = (1llu << num_bits) - 1;
  return (value >> start_bit) & mask;
}
