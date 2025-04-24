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

#include <cuda_runtime_api.h>

#include "breeze/utils/types.h"

#if defined(__CUDA_ARCH__)
#if __CUDA_ARCH__ < 500
#error "Unsupported CUDA architecture"
#endif
#endif

struct CudaSpecialization {
  template <int WARP_THREADS>
  static __device__ __forceinline__ int lane_idx();
  template <int WARP_THREADS>
  static __device__ __forceinline__ int warp_idx();
  template <int WARP_THREADS>
  static __device__ __forceinline__ unsigned lower_rank_lanemask();
  template <int WARP_THREADS>
  static __device__ __forceinline__ unsigned higher_rank_lanemask();
  static __device__ __forceinline__ void reconvergence_hint() {
    // Use full warp syncs as hint to encourage reconvergence by default.
    __syncwarp();
  }
  template <breeze::utils::MemoryOrder MEMORY_ORDER = breeze::utils::RELAXED,
            typename SliceT, typename T = typename SliceT::data_type>
  static __device__ __forceinline__ T atomic_load(SliceT address) {
    static_assert(MEMORY_ORDER != breeze::utils::RELEASE,
                  "RELEASE is not a valid memory order for atomic_load");
    T value;
    if constexpr (SliceT::ADDRESS_SPACE == breeze::utils::GLOBAL) {
      value = __ldcg(address.data());  // cache only globally
    } else {
      value = *address.data();
    }
    if constexpr (MEMORY_ORDER == breeze::utils::ACQUIRE) {
      __threadfence();
    }
    return value;
  }
  template <breeze::utils::MemoryOrder MEMORY_ORDER = breeze::utils::RELAXED,
            typename SliceT, typename T = typename SliceT::data_type>
  static __device__ __forceinline__ void atomic_store(SliceT address, T value) {
    static_assert(MEMORY_ORDER != breeze::utils::ACQUIRE,
                  "ACQUIRE is not a valid memory order for atomic_store");
    if constexpr (MEMORY_ORDER == breeze::utils::RELEASE) {
      __threadfence();
    }
    if constexpr (SliceT::ADDRESS_SPACE == breeze::utils::GLOBAL) {
      __stcg(address.data(), value);  // cache only globally
    } else {
      *address.data() = value;
    }
  }
  template <breeze::utils::MemoryOrder MEMORY_ORDER = breeze::utils::RELAXED,
            typename SliceT, typename T = typename SliceT::data_type>
  static __device__ __forceinline__ T atomic_cas(SliceT address, T compare,
                                                 T value) {
    if constexpr (MEMORY_ORDER == breeze::utils::RELEASE) {
      __threadfence();
    }
    T old = atomicCAS(address.data(), compare, value);
    if constexpr (MEMORY_ORDER == breeze::utils::ACQUIRE) {
      __threadfence();
    }
    return old;
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  static __device__ __forceinline__ T atomic_add(SliceT address, T value) {
    return atomicAdd(address.data(), value);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  static __device__ __forceinline__ void atomic_min(SliceT address, T value) {
    atomicMin(address.data(), value);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  static __device__ __forceinline__ void atomic_max(SliceT address, T value) {
    atomicMax(address.data(), value);
  }
  template <int WARP_THREADS, typename T>
  static __device__ __forceinline__ T reduce_add(T value) {
#pragma unroll
    for (int offset = WARP_THREADS / 2; offset > 0; offset /= 2) {
      value += __shfl_xor_sync(0xffffffff, value, offset);
    }
    return value;
  }
  template <int WARP_THREADS, typename T>
  static __device__ __forceinline__ T reduce_min(T value) {
#pragma unroll
    for (int offset = WARP_THREADS / 2; offset > 0; offset /= 2) {
      value = ::min(value, __shfl_xor_sync(0xffffffff, value, offset));
    }
    return value;
  }
  template <int WARP_THREADS, typename T>
  static __device__ __forceinline__ T reduce_max(T value) {
#pragma unroll
    for (int offset = WARP_THREADS / 2; offset > 0; offset /= 2) {
      value = ::max(value, __shfl_xor_sync(0xffffffff, value, offset));
    }
    return value;
  }
  template <int WARP_THREADS, typename T>
  static __device__ __forceinline__ T scan_add(T value) {
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
  static __device__ __forceinline__ int count_leading_zeros(T value);
  template <typename T>
  static __device__ __forceinline__ int population_count(T value);
  template <typename T>
  static __device__ __forceinline__ T extract_bits(T value, int start_bit,
                                                   int num_bits);
  template <bool HIGH_PRIORITY>
  static __device__ __forceinline__ void scheduling_hint() {}
};

template <int CUDA_BLOCK_THREADS, int CUDA_WARP_THREADS>
struct CudaPlatform {
  enum {
    BLOCK_THREADS = CUDA_BLOCK_THREADS,
    WARP_THREADS = CUDA_WARP_THREADS,
  };
  __device__ __forceinline__ int thread_idx() { return threadIdx.x; }
  __device__ __forceinline__ int block_idx() { return blockIdx.x; }
  __device__ __forceinline__ void syncthreads() { __syncthreads(); }
  __device__ __forceinline__ void syncwarp() { __syncwarp(); }
  __device__ __forceinline__ int lane_idx() {
    return CudaSpecialization::template lane_idx<WARP_THREADS>();
  }
  __device__ __forceinline__ int warp_idx() {
    return CudaSpecialization::template warp_idx<WARP_THREADS>();
  }
  __device__ __forceinline__ unsigned lower_rank_lanemask() {
    return CudaSpecialization::template lower_rank_lanemask<WARP_THREADS>();
  }
  __device__ __forceinline__ unsigned higher_rank_lanemask() {
    return CudaSpecialization::template higher_rank_lanemask<WARP_THREADS>();
  }
  __device__ __forceinline__ void reconvergence_hint() {
    CudaSpecialization::reconvergence_hint();
  }
  template <typename T>
  __device__ __forceinline__ T min(T lhs, T rhs) {
    return ::min(lhs, rhs);
  }
  template <typename T>
  __device__ __forceinline__ T max(T lhs, T rhs) {
    return ::max(lhs, rhs);
  }
  template <breeze::utils::MemoryOrder MEMORY_ORDER = breeze::utils::RELAXED,
            typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ T atomic_load(SliceT address) {
    return CudaSpecialization::atomic_load<MEMORY_ORDER>(address);
  }
  template <breeze::utils::MemoryOrder MEMORY_ORDER = breeze::utils::RELAXED,
            typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ void atomic_store(SliceT address, T value) {
    CudaSpecialization::atomic_store<MEMORY_ORDER>(address, value);
  }
  template <breeze::utils::MemoryOrder MEMORY_ORDER = breeze::utils::RELAXED,
            typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ T atomic_cas(SliceT address, T compare, T value) {
    return CudaSpecialization::atomic_cas<MEMORY_ORDER>(address, compare,
                                                        value);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ T atomic_add(SliceT address, T value) {
    return CudaSpecialization::atomic_add(address, value);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ void atomic_min(SliceT address, T value) {
    CudaSpecialization::atomic_min(address, value);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  __device__ __forceinline__ void atomic_max(SliceT address, T value) {
    CudaSpecialization::atomic_max(address, value);
  }
  template <typename T>
  __device__ __forceinline__ T reduce_add(T value) {
    return CudaSpecialization::template reduce_add<WARP_THREADS, T>(value);
  }
  template <typename T>
  __device__ __forceinline__ T reduce_min(T value) {
    return CudaSpecialization::template reduce_min<WARP_THREADS, T>(value);
  }
  template <typename T>
  __device__ __forceinline__ T reduce_max(T value) {
    return CudaSpecialization::template reduce_max<WARP_THREADS, T>(value);
  }
  template <typename T>
  __device__ __forceinline__ T scan_add(T value) {
    return CudaSpecialization::template scan_add<WARP_THREADS, T>(value);
  }
  template <typename T>
  __device__ __forceinline__ T ballot(bool value) {
    return __ballot_sync(0xffffffff, value);
  }
  template <int MIN_BITS, typename T>
  __device__ __forceinline__ unsigned match_any(T value) {
#if __CUDA_ARCH__ >= 800
    return __match_any_sync(0xffffffff, value);
#else
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
#endif
  }
  template <typename T>
  __device__ __forceinline__ int count_leading_zeros(T value) {
    return CudaSpecialization::count_leading_zeros(value);
  }
  template <typename T>
  __device__ __forceinline__ int population_count(T value) {
    return CudaSpecialization::population_count(value);
  }
  template <typename T>
  __device__ __forceinline__ T extract_bits(T value, int start_bit,
                                            int num_bits) {
    return CudaSpecialization::extract_bits(value, start_bit, num_bits);
  }
  template <bool HIGH_PRIORITY>
  __device__ __forceinline__ void scheduling_hint() {
    CudaSpecialization::scheduling_hint<HIGH_PRIORITY>();
  }
};

#if CUDART_VERSION >= 12080 && __CUDA_ARCH__ >= 700
// specialization for MEMORY_ORDER=ACQUIRE, SliceT=Slice<GLOBAL, BLOCKED, int>
template <>
__device__ __forceinline__ int CudaSpecialization::atomic_load<
    breeze::utils::ACQUIRE,
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, int>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, int>
        address) {
  return __nv_atomic_load_n(address.data(), __NV_ATOMIC_ACQUIRE,
                            __NV_THREAD_SCOPE_DEVICE);
}

// specialization for MEMORY_ORDER=ACQUIRE, SliceT=Slice<GLOBAL, BLOCKED,
// unsigned>
template <>
__device__ __forceinline__ unsigned CudaSpecialization::atomic_load<
    breeze::utils::ACQUIRE,
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED,
                         unsigned>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED,
                         unsigned>
        address) {
  return __nv_atomic_load_n(address.data(), __NV_ATOMIC_ACQUIRE,
                            __NV_THREAD_SCOPE_DEVICE);
}

// specialization for MEMORY_ORDER=ACQUIRE, SliceT=Slice<GLOBAL, BLOCKED, int>
template <>
__device__ __forceinline__ void CudaSpecialization::atomic_store<
    breeze::utils::ACQUIRE,
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, int>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, int>
        address,
    int value) {
  __nv_atomic_store_n(address.data(), value, __NV_ATOMIC_RELEASE,
                      __NV_THREAD_SCOPE_DEVICE);
}

// specialization for MEMORY_ORDER=ACQUIRE, SliceT=Slice<GLOBAL, BLOCKED,
// unsigned>
template <>
__device__ __forceinline__ void CudaSpecialization::atomic_store<
    breeze::utils::ACQUIRE,
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED,
                         unsigned>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED,
                         unsigned>
        address,
    unsigned value) {
  __nv_atomic_store_n(address.data(), value, __NV_ATOMIC_RELEASE,
                      __NV_THREAD_SCOPE_DEVICE);
}

// specialization for MEMORY_ORDER=ACQUIRE, SliceT=Slice<GLOBAL, BLOCKED,
// unsigned>
template <>
__device__ __forceinline__ int CudaSpecialization::atomic_cas<
    breeze::utils::ACQUIRE,
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, int>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, int>
        address,
    int compare, int value) {
  int expected = compare;
  __nv_atomic_compare_exchange_n(address.data(), &expected, value, false,
                                 __NV_ATOMIC_ACQUIRE, __NV_ATOMIC_ACQUIRE,
                                 __NV_THREAD_SCOPE_DEVICE);
  return expected;
}

// specialization for MEMORY_ORDER=ACQUIRE, SliceT=Slice<GLOBAL, BLOCKED,
// unsigned>
template <>
__device__ __forceinline__ unsigned CudaSpecialization::atomic_cas<
    breeze::utils::ACQUIRE,
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED,
                         unsigned>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED,
                         unsigned>
        address,
    unsigned compare, unsigned value) {
  unsigned expected = compare;
  __nv_atomic_compare_exchange_n(address.data(), &expected, value, false,
                                 __NV_ATOMIC_ACQUIRE, __NV_ATOMIC_ACQUIRE,
                                 __NV_THREAD_SCOPE_DEVICE);
  return expected;
}
#endif

#if __CUDA_ARCH__ < 600
// specialization for T=Slice<GLOBAL, BLOCKED, double>
template <>
__device__ __forceinline__ double
CudaSpecialization::atomic_add<breeze::utils::Slice<
    breeze::utils::GLOBAL, breeze::utils::BLOCKED, double>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, double>
        address,
    double value) {
  static_assert(sizeof(double) == sizeof(unsigned long long),
                "unexpected type sizes");
  unsigned long long old =
      *reinterpret_cast<unsigned long long *>(address.data());
  unsigned long long assumed;
  do {
    assumed = old;
    old = atomicCAS(
        reinterpret_cast<unsigned long long *>(address.data()), assumed,
        __double_as_longlong(value + __longlong_as_double(assumed)));
  } while (assumed != old);

  return __longlong_as_double(old);
}
#endif

// specialization for T=Slice<GLOBAL, BLOCKED, long long>
template <>
__device__ __forceinline__ long long
CudaSpecialization::atomic_add<breeze::utils::Slice<
    breeze::utils::GLOBAL, breeze::utils::BLOCKED, long long>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED,
                         long long>
        address,
    long long value) {
  unsigned long long result =
      atomicAdd(reinterpret_cast<unsigned long long *>(address.data()),
                *reinterpret_cast<unsigned long long *>(&value));
  return *reinterpret_cast<long long *>(&result);
}

// specialization for T=Slice<GLOBAL, BLOCKED, float>
template <>
__device__ __forceinline__ void CudaSpecialization::atomic_min<
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, float>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, float>
        address,
    float value) {
  static_assert(sizeof(float) == sizeof(unsigned), "unexpected type sizes");
  float current = atomic_load(address);
  while (current > value) {
    unsigned old = atomicCAS(reinterpret_cast<unsigned *>(address.data()),
                             *reinterpret_cast<unsigned *>(&current),
                             *reinterpret_cast<unsigned *>(&value));
    current = *reinterpret_cast<float *>(&old);
    if (current == value) {
      break;
    }
  }
}

// specialization for T=Slice<GLOBAL, BLOCKED, float>
template <>
__device__ __forceinline__ void CudaSpecialization::atomic_max<
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, float>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, float>
        address,
    float value) {
  static_assert(sizeof(float) == sizeof(unsigned), "unexpected type sizes");
  float current = atomic_load(address);
  while (current < value) {
    unsigned old = atomicCAS(reinterpret_cast<unsigned *>(address.data()),
                             *reinterpret_cast<unsigned *>(&current),
                             *reinterpret_cast<unsigned *>(&value));
    current = *reinterpret_cast<float *>(&old);
    if (current == value) {
      break;
    }
  }
}

// specialization for T=Slice<GLOBAL, BLOCKED, double>
template <>
__device__ __forceinline__ void
CudaSpecialization::atomic_min<breeze::utils::Slice<
    breeze::utils::GLOBAL, breeze::utils::BLOCKED, double>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, double>
        address,
    double value) {
  static_assert(sizeof(double) == sizeof(unsigned long long),
                "unexpected type sizes");
  double current = atomic_load(address);
  while (current > value) {
    unsigned long long old =
        atomicCAS(reinterpret_cast<unsigned long long *>(address.data()),
                  *reinterpret_cast<unsigned long long *>(&current),
                  *reinterpret_cast<unsigned long long *>(&value));
    current = *reinterpret_cast<double *>(&old);
    if (current == value) {
      break;
    }
  }
}

// specialization for T=Slice<GLOBAL, BLOCKED, double>
template <>
__device__ __forceinline__ void
CudaSpecialization::atomic_max<breeze::utils::Slice<
    breeze::utils::GLOBAL, breeze::utils::BLOCKED, double>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, double>
        address,
    double value) {
  static_assert(sizeof(double) == sizeof(unsigned long long),
                "unexpected type sizes");
  double current = atomic_load(address);
  while (current < value) {
    unsigned long long old =
        atomicCAS(reinterpret_cast<unsigned long long *>(address.data()),
                  *reinterpret_cast<unsigned long long *>(&current),
                  *reinterpret_cast<unsigned long long *>(&value));
    current = *reinterpret_cast<double *>(&old);
    if (current == value) {
      break;
    }
  }
}

// specialization for T=Slice<GLOBAL, BLOCKED, long long>
template <>
__device__ __forceinline__ long long CudaSpecialization::atomic_cas<
    breeze::utils::RELAXED,
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED,
                         long long>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED,
                         long long>
        address,
    long long compare, long long value) {
  unsigned long long old =
      atomicCAS(reinterpret_cast<unsigned long long *>(address.data()),
                *reinterpret_cast<unsigned long long *>(&compare),
                *reinterpret_cast<unsigned long long *>(&value));
  return *reinterpret_cast<long long *>(&old);
}

// specialization for T=Slice<SHARED, BLOCKED, long long>
template <>
__device__ __forceinline__ long long CudaSpecialization::atomic_cas<
    breeze::utils::RELAXED,
    breeze::utils::Slice<breeze::utils::SHARED, breeze::utils::BLOCKED,
                         long long>>(
    breeze::utils::Slice<breeze::utils::SHARED, breeze::utils::BLOCKED,
                         long long>
        address,
    long long compare, long long value) {
  using pointer_type =
      typename breeze::utils::Slice<breeze::utils::SHARED,
                                    breeze::utils::BLOCKED,
                                    unsigned long long>::pointer_type;
  unsigned long long old =
      atomicCAS(reinterpret_cast<pointer_type>(address.data()),
                *reinterpret_cast<unsigned long long *>(&compare),
                *reinterpret_cast<unsigned long long *>(&value));
  return *reinterpret_cast<long long *>(&old);
}

// specialization for T=Slice<GLOBAL, BLOCKED, float>
template <>
__device__ __forceinline__ float CudaSpecialization::atomic_cas<
    breeze::utils::RELAXED,
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, float>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, float>
        address,
    float compare, float value) {
  static_assert(sizeof(float) == sizeof(unsigned), "unexpected type sizes");
  unsigned old = atomicCAS(reinterpret_cast<unsigned *>(address.data()),
                           *reinterpret_cast<unsigned *>(&compare),
                           *reinterpret_cast<unsigned *>(&value));
  return *reinterpret_cast<float *>(&old);
}

#if __CUDA_ARCH__ >= 800
// specialization for T=int
template <>
__device__ __forceinline__ int CudaSpecialization::reduce_add<32, int>(
    int value) {
  return __reduce_add_sync(0xffffffff, value);
}

// specialization for T=unsigned
template <>
__device__ __forceinline__ unsigned
CudaSpecialization::reduce_add<32, unsigned>(unsigned value) {
  return __reduce_add_sync(0xffffffff, value);
}

// specialization for T=unsigned long long
template <>
__device__ __forceinline__ unsigned long long
CudaSpecialization::reduce_add<32, unsigned long long>(
    unsigned long long value) {
  unsigned low =
      __reduce_add_sync(0xffffffff, static_cast<unsigned>(value & 0xffffff));
  unsigned mid = __reduce_add_sync(
      0xffffffff, static_cast<unsigned>((value >> 24) & 0xffffff));
  unsigned high =
      __reduce_add_sync(0xffffffff, static_cast<unsigned>(value >> 48));
  return low + (static_cast<unsigned long long>(mid) << 24) +
         (static_cast<unsigned long long>(high) << 48);
}

// specialization for T=long long
template <>
__device__ __forceinline__ long long
CudaSpecialization::reduce_add<32, long long>(long long value) {
  return reduce_add<32, unsigned long long>(value);
}

// specialization for T=int
template <>
__device__ __forceinline__ int CudaSpecialization::reduce_min<32, int>(
    int value) {
  return __reduce_min_sync(0xffffffff, value);
}

// specialization for T=unsigned
template <>
__device__ __forceinline__ unsigned
CudaSpecialization::reduce_min<32, unsigned>(unsigned value) {
  return __reduce_min_sync(0xffffffff, value);
}

// specialization for T=long long
template <>
__device__ __forceinline__ long long
CudaSpecialization::reduce_min<32, long long>(long long value) {
  int high = __reduce_min_sync(0xffffffff, static_cast<int>(value >> 32));
  bool match_high = high == static_cast<int>(value >> 32);
  // force threads that lost the first reduction to lose the second reduction
  unsigned sel_low =
      match_high ? static_cast<unsigned>(value & 0xffffffff) : ~0u;
  unsigned low = __reduce_min_sync(0xffffffff, sel_low);
  return low + (static_cast<long long>(high) << 32);
}

// specialization for T=unsigned long long
template <>
__device__ __forceinline__ unsigned long long
CudaSpecialization::reduce_min<32, unsigned long long>(
    unsigned long long value) {
  unsigned high =
      __reduce_min_sync(0xffffffff, static_cast<unsigned>(value >> 32));
  bool match_high = high == static_cast<unsigned>(value >> 32);
  // force threads that lost the first reduction to lose the second reduction
  unsigned sel_low =
      match_high ? static_cast<unsigned>(value & 0xffffffff) : ~0u;
  unsigned low = __reduce_min_sync(0xffffffff, sel_low);
  return low + (static_cast<unsigned long long>(high) << 32);
}

// specialization for T=int
template <>
__device__ __forceinline__ int CudaSpecialization::reduce_max<32, int>(
    int value) {
  return __reduce_max_sync(0xffffffff, value);
}

// specialization for T=unsigned
template <>
__device__ __forceinline__ unsigned
CudaSpecialization::reduce_max<32, unsigned>(unsigned value) {
  return __reduce_max_sync(0xffffffff, value);
}

// specialization for T=long long
template <>
__device__ __forceinline__ long long
CudaSpecialization::reduce_max<32, long long>(long long value) {
  int high = __reduce_max_sync(0xffffffff, static_cast<int>(value >> 32));
  bool match_high = high == static_cast<int>(value >> 32);
  // force threads that lost the first reduction to lose the second reduction
  unsigned sel_low =
      match_high ? static_cast<unsigned>(value & 0xffffffff) : 0u;
  unsigned low = __reduce_max_sync(0xffffffff, sel_low);
  return low + (static_cast<long long>(high) << 32);
}

// specialization for T=unsigned long long
template <>
__device__ __forceinline__ unsigned long long
CudaSpecialization::reduce_max<32, unsigned long long>(
    unsigned long long value) {
  unsigned high =
      __reduce_max_sync(0xffffffff, static_cast<unsigned>(value >> 32));
  bool match_high = high == static_cast<unsigned>(value >> 32);
  // force threads that lost the first reduction to lose the second reduction
  unsigned sel_low =
      match_high ? static_cast<unsigned>(value & 0xffffffff) : 0u;
  unsigned low = __reduce_max_sync(0xffffffff, sel_low);
  return low + (static_cast<unsigned long long>(high) << 32);
}
#endif  // __CUDA_ARCH__ >= 800

// specialization for T=unsigned
template <>
__device__ __forceinline__ int
CudaSpecialization::count_leading_zeros<unsigned>(unsigned value) {
  return __clz(value);
}

// specialization for T=unsigned long long
template <>
__device__ __forceinline__ int
CudaSpecialization::count_leading_zeros<unsigned long long>(
    unsigned long long value) {
  return __clzll(value);
}

// specialization for T=unsigned
template <>
__device__ __forceinline__ int CudaSpecialization::population_count<unsigned>(
    unsigned value) {
  return __popc(value);
}

// specialization for T=unsigned long long
template <>
__device__ __forceinline__ int
CudaSpecialization::population_count<unsigned long long>(
    unsigned long long value) {
  return __popcll(value);
}

// specialization for T=unsigned long long
template <>
__device__ __forceinline__ unsigned long long CudaSpecialization::extract_bits(
    unsigned long long value, int start_bit, int num_bits) {
  unsigned long long mask = (1llu << num_bits) - 1;
  return (value >> start_bit) & mask;
}

#define Q(x) #x
#define QUOTE(x) Q(x)
#include QUOTE(CUDA_PLATFORM_SPECIALIZATION_HEADER)
#undef QUOTE
#undef Q
