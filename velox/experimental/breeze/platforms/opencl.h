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

template <int OPENCL_BLOCK_THREADS, int OPENCL_WARP_THREADS>
struct OpenCLPlatform {
  enum {
    BLOCK_THREADS = OPENCL_BLOCK_THREADS,
    WARP_THREADS = OPENCL_WARP_THREADS,
  };
  inline int thread_idx() { return get_local_id(0); }
  inline int block_idx() { return get_group_id(0); }
  inline void syncthreads() { ::barrier(CLK_LOCAL_MEM_FENCE); }
  inline void syncwarp() { sub_group_barrier(CLK_LOCAL_MEM_FENCE); }
  inline int lane_idx() { return get_local_id(0) % WARP_THREADS; }
  inline int warp_idx() { return get_local_id(0) / WARP_THREADS; }
  inline uint lower_rank_lanemask() {
    static_assert(WARP_THREADS <= sizeof(uint) * 8,
                  "WARP_THREADS must be less or equal to unsigned bits");
    return (1 << lane_idx()) - 1;
  }
  inline uint higher_rank_lanemask() {
    static_assert(WARP_THREADS <= sizeof(uint) * 8,
                  "WARP_THREADS must be less or equal to unsigned bits");
    unsigned lane_mask = 1 << lane_idx();
    return ~((lane_mask - 1) | lane_mask);
  }
  inline void reconvergence_hint() {}
  template <typename T>
  inline T min(T lhs, T rhs) {
    return ::min(lhs, rhs);
  }
  template <typename T>
  inline T max(T lhs, T rhs) {
    return ::max(lhs, rhs);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  inline T atomic_load(SliceT address) {
    return *reinterpret_cast<const volatile T *>(address.data());
  }
  template <typename SliceT, typename T>
  inline void atomic_store(SliceT address, T value) {
    *reinterpret_cast<volatile T *>(address.data()) = value;
  }
  template <typename SliceT, typename T>
  inline T atomic_cas(SliceT address, T compare, T value) {
    return atomic_cmpxchg(address.data(), compare, value);
  }
  template <typename SliceT, typename T>
  inline T atomic_add(SliceT address, T value) {
    return ::atomic_add(address.data(), value);
  }
  template <typename SliceT, typename T>
  inline void atomic_min(SliceT address, T value) {
    ::atomic_min(address.data(), value);
  }
  template <typename SliceT, typename T>
  inline void atomic_max(SliceT address, T value) {
    ::atomic_max(address.data(), value);
  }
  template <typename T>
  inline T reduce_add(T value) {
    return sub_group_reduce_add(value);
  }
  template <typename T>
  inline T reduce_min(T value) {
    return sub_group_reduce_min(value);
  }
  template <typename T>
  inline T reduce_max(T value) {
    return sub_group_reduce_max(value);
  }
  template <typename T>
  inline T scan_add(T value) {
    return sub_group_scan_inclusive_add(value);
  }
  template <typename T>
  inline T ballot(bool value) {
    int id = get_local_id(0) % WARP_THREADS;
    T thread_mask = static_cast<T>(value) << id;
    return sub_group_reduce_add(thread_mask);
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
  template <typename T>
  inline int count_leading_zeros(T value) {
    return clz(value);
  }
  template <typename T>
  inline int population_count(T value) {
    return popcount(value);
  }
  template <typename T>
  inline T extract_bits(T value, int start_bit, int num_bits) {
    T mask = (static_cast<T>(1) << num_bits) - 1;
    return (value >> start_bit) & mask;
  }
  template <bool HIGH_PRIORITY>
  inline void scheduling_hint() {}
};
