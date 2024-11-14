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

#include <metal_stdlib>

#include "breeze/utils/types.h"

struct MetalSpecialization {
  template <typename SliceT, typename T = typename SliceT::data_type>
  static inline T atomic_load(SliceT address);
  template <typename SliceT, typename T>
  static inline void atomic_store(SliceT address, T value);
};

template <int METAL_BLOCK_THREADS, int METAL_WARP_THREADS>
struct MetalPlatform {
  enum {
    BLOCK_THREADS = METAL_BLOCK_THREADS,
    WARP_THREADS = METAL_WARP_THREADS,
  };
  inline int thread_idx() { return thread_idx_; }
  inline int block_idx() { return block_idx_; }
  inline void syncthreads() {
    metal::threadgroup_barrier(metal::mem_flags::mem_threadgroup);
  }
  inline void syncwarp() {
    metal::simdgroup_barrier(metal::mem_flags::mem_threadgroup);
  }
  inline int lane_idx() { return thread_idx_ % WARP_THREADS; }
  inline int warp_idx() { return thread_idx_ / WARP_THREADS; }
  inline unsigned lower_rank_lanemask() {
    static_assert(WARP_THREADS <= sizeof(unsigned) * 8,
                  "WARP_THREADS must be less or equal to unsigned bits");
    return (1 << lane_idx()) - 1;
  }
  inline unsigned higher_rank_lanemask() {
    static_assert(WARP_THREADS <= sizeof(unsigned) * 8,
                  "WARP_THREADS must be less or equal to unsigned bits");
    unsigned lane_mask = 1 << lane_idx();
    return ~((lane_mask - 1) | lane_mask);
  }
  inline void reconvergence_hint() {}
  template <typename T>
  inline T min(T lhs, T rhs) {
    return metal::min(lhs, rhs);
  }
  template <typename T>
  inline T max(T lhs, T rhs) {
    return metal::max(lhs, rhs);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  inline T atomic_load(SliceT address) {
    return MetalSpecialization::atomic_load<SliceT, T>(address);
  }
  template <typename SliceT, typename T>
  inline void atomic_store(SliceT address, T value) {
    MetalSpecialization::atomic_store<SliceT, T>(address, value);
  }
  template <template <breeze::utils::AddressSpace,
                      breeze::utils::DataArrangement, typename>
            class SliceT>
  inline uint atomic_cas(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, uint> address,
      uint expected, uint desired) {
    volatile device uint *ptr = address.data();
    uint current = expected;
    do {
      if (metal::atomic_compare_exchange_weak_explicit(
              (device metal::atomic_uint *)ptr, &current, desired,
              metal::memory_order_relaxed, metal::memory_order_relaxed)) {
        return expected;
      }
    } while (current == expected);
    return current;
  }
  template <template <breeze::utils::AddressSpace,
                      breeze::utils::DataArrangement, typename>
            class SliceT>
  inline bool atomic_cas(
      SliceT<breeze::utils::SHARED, breeze::utils::BLOCKED, uint> address,
      uint expected, uint desired) {
    volatile threadgroup uint *ptr = address.data();
    uint current = expected;
    do {
      if (metal::atomic_compare_exchange_weak_explicit(
              (threadgroup metal::atomic_uint *)ptr, &current, desired,
              metal::memory_order_relaxed, metal::memory_order_relaxed)) {
        return expected;
      }
    } while (current == expected);
    return current;
  }
  template <template <breeze::utils::AddressSpace,
                      breeze::utils::DataArrangement, typename>
            class SliceT>
  inline int atomic_add(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, int> address,
      int value) {
    volatile device int *ptr = address.data();
    return metal::atomic_fetch_add_explicit((device metal::atomic_int *)ptr,
                                            value, metal::memory_order_relaxed);
  }
  template <template <breeze::utils::AddressSpace,
                      breeze::utils::DataArrangement, typename>
            class SliceT>
  inline int atomic_add(
      SliceT<breeze::utils::SHARED, breeze::utils::BLOCKED, int> address,
      int value) {
    volatile threadgroup int *ptr = address.data();
    return metal::atomic_fetch_add_explicit(
        (threadgroup metal::atomic_int *)ptr, value,
        metal::memory_order_relaxed);
  }
  template <template <breeze::utils::AddressSpace,
                      breeze::utils::DataArrangement, typename>
            class SliceT>
  inline uint atomic_add(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, uint> address,
      uint value) {
    volatile device uint *ptr = address.data();
    return metal::atomic_fetch_add_explicit((device metal::atomic_uint *)ptr,
                                            value, metal::memory_order_relaxed);
  }
  template <template <breeze::utils::AddressSpace,
                      breeze::utils::DataArrangement, typename>
            class SliceT>
  inline uint atomic_add(
      SliceT<breeze::utils::SHARED, breeze::utils::BLOCKED, uint> address,
      uint value) {
    volatile threadgroup uint *ptr = address.data();
    return metal::atomic_fetch_add_explicit(
        (threadgroup metal::atomic_uint *)ptr, value,
        metal::memory_order_relaxed);
  }
  template <template <breeze::utils::AddressSpace,
                      breeze::utils::DataArrangement, typename>
            class SliceT>
  inline void atomic_min(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, int> address,
      int value) {
    volatile device int *ptr = address.data();
    int current = metal::atomic_load_explicit((device metal::atomic_int *)ptr,
                                              metal::memory_order_relaxed);
    while (current > value) {
      if (metal::atomic_compare_exchange_weak_explicit(
              (device metal::atomic_int *)ptr, &current, value,
              metal::memory_order_relaxed, metal::memory_order_relaxed)) {
        break;
      }
    }
  }
  template <template <breeze::utils::AddressSpace,
                      breeze::utils::DataArrangement, typename>
            class SliceT>
  inline void atomic_min(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, uint> address,
      uint value) {
    volatile device uint *ptr = address.data();
    uint current = metal::atomic_load_explicit((device metal::atomic_uint *)ptr,
                                               metal::memory_order_relaxed);
    while (current > value) {
      if (metal::atomic_compare_exchange_weak_explicit(
              (device metal::atomic_uint *)ptr, &current, value,
              metal::memory_order_relaxed, metal::memory_order_relaxed)) {
        break;
      }
    }
  }
  template <template <breeze::utils::AddressSpace,
                      breeze::utils::DataArrangement, typename>
            class SliceT>
  inline void atomic_max(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, int> address,
      int value) {
    volatile device int *ptr = address.data();
    int current = metal::atomic_load_explicit((device metal::atomic_int *)ptr,
                                              metal::memory_order_relaxed);
    while (current < value) {
      if (metal::atomic_compare_exchange_weak_explicit(
              (device metal::atomic_int *)ptr, &current, value,
              metal::memory_order_relaxed, metal::memory_order_relaxed)) {
        break;
      }
    }
  }
  template <template <breeze::utils::AddressSpace,
                      breeze::utils::DataArrangement, typename>
            class SliceT>
  inline void atomic_max(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, uint> address,
      uint value) {
    volatile device uint *ptr = address.data();
    uint current = metal::atomic_load_explicit((device metal::atomic_uint *)ptr,
                                               metal::memory_order_relaxed);
    while (current < value) {
      if (metal::atomic_compare_exchange_weak_explicit(
              (device metal::atomic_uint *)ptr, &current, value,
              metal::memory_order_relaxed, metal::memory_order_relaxed)) {
        break;
      }
    }
  }
  template <typename T>
  inline T reduce_add(T value) {
#pragma unroll
    for (int offset = WARP_THREADS / 2; offset > 0; offset /= 2) {
      value += metal::simd_shuffle_down(value, offset);
    }
    return value;
  }
  template <typename T>
  inline T reduce_min(T value) {
#pragma unroll
    for (int offset = WARP_THREADS / 2; offset > 0; offset /= 2) {
      value = metal::min(value, metal::simd_shuffle_down(value, offset));
    }
    return value;
  }
  template <typename T>
  inline T reduce_max(T value) {
#pragma unroll
    for (int offset = WARP_THREADS / 2; offset > 0; offset /= 2) {
      value = metal::max(value, metal::simd_shuffle_down(value, offset));
    }
    return value;
  }
  template <typename T>
  inline T scan_add(T value) {
#pragma unroll
    for (int offset = 1; offset < WARP_THREADS; offset <<= 1) {
      T result = metal::simd_shuffle_up(value, offset);
      if ((thread_idx() % WARP_THREADS) >= offset) {
        value += result;
      }
    }
    return value;
  }
  template <typename T>
  inline T ballot(bool value) {
    return (metal::simd_vote::vote_t)metal::simd_ballot(value);
  }
  template <int MIN_BITS, typename T>
  inline uint match_any(T value) {
    unsigned result;
#pragma unroll
    for (uint i = 0; i < MIN_BITS; ++i) {
      uint current_bit = 1 << i;
      bool pred = (value & current_bit) == current_bit;
      uint mask = ballot<uint>(pred);
      if (!pred) {
        mask = ~mask;
      }
      result = (i == 0) ? mask : result & mask;
    }
    return result;
  }
  inline int count_leading_zeros(uint value) { return metal::clz(value); }
  inline int population_count(uint value) { return metal::popcount(value); }
  inline uint extract_bits(uint value, int start_bit, int num_bits) {
    return metal::extract_bits(value, start_bit, num_bits);
  }
  template <bool HIGH_PRIORITY>
  inline void scheduling_hint() {}

  uint thread_idx_;
  uint block_idx_;
};

// specialization for T=Slice<GLOBAL, BLOCKED, uint>
template <>
inline uint MetalSpecialization::atomic_load<
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, uint>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, uint>
        address) {
  volatile device uint *ptr = address.data();
  return metal::atomic_load_explicit((device metal::atomic_uint *)ptr,
                                     metal::memory_order_relaxed);
}

// specialization for T=Slice<GLOBAL, BLOCKED, int>
template <>
inline void MetalSpecialization::atomic_store<
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, int>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, int>
        address,
    int value) {
  volatile device int *ptr = address.data();
  metal::atomic_store_explicit((device metal::atomic_int *)ptr, value,
                               metal::memory_order_relaxed);
}

// specialization for T=Slice<GLOBAL, BLOCKED, uint>
template <>
inline void MetalSpecialization::atomic_store<
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, uint>>(
    breeze::utils::Slice<breeze::utils::GLOBAL, breeze::utils::BLOCKED, uint>
        address,
    uint value) {
  volatile device uint *ptr = address.data();
  metal::atomic_store_explicit((device metal::atomic_uint *)ptr, value,
                               metal::memory_order_relaxed);
}
