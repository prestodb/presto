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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-local-typedef"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wmismatched-tags"
#pragma GCC diagnostic ignored "-Wdeprecated-copy"
#include <CL/sycl.hpp>
#pragma GCC diagnostic pop

#include "breeze/utils/types.h"

template <int SYCL_BLOCK_THREADS, int SYCL_WARP_THREADS>
struct SyCLPlatform {
  enum {
    BLOCK_THREADS = SYCL_BLOCK_THREADS,
    WARP_THREADS = SYCL_WARP_THREADS,
  };
  inline int thread_idx() { return work_item.get_local_id(0); }
  inline int block_idx() { return work_item.get_group(0); }
  inline void syncthreads() { work_item.barrier(); }
  inline void syncwarp() { cl::sycl::group_barrier(work_item.get_sub_group()); }
  inline int lane_idx() { return work_item.get_local_id(0) % WARP_THREADS; }
  inline int warp_idx() { return work_item.get_local_id(0) / WARP_THREADS; }
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
    return cl::sycl::min(lhs, rhs);
  }
  template <typename T>
  inline T max(T lhs, T rhs) {
    return cl::sycl::max(lhs, rhs);
  }
  template <typename T, template <breeze::utils::AddressSpace,
                                  breeze::utils::DataArrangement, typename>
                        typename SliceT>
  inline T atomic_load(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, T> address) {
    return cl::sycl::atomic<T>{cl::sycl::global_ptr<T>{address.data()}}.load();
  }
  template <typename T, template <breeze::utils::AddressSpace,
                                  breeze::utils::DataArrangement, typename>
                        typename SliceT>
  inline void atomic_store(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, T> address,
      T value) {
    cl::sycl::atomic<T>{cl::sycl::global_ptr<T>{address.data()}}.store(value);
  }
  template <typename T, template <breeze::utils::AddressSpace,
                                  breeze::utils::DataArrangement, typename>
                        typename SliceT>
  inline T atomic_cas(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, T> address,
      T compare, T value) {
    cl::sycl::atomic<T>{cl::sycl::global_ptr<T>{address.data()}}
        .compare_exchange_strong(compare, value);
    return compare;
  }
  template <typename T, template <breeze::utils::AddressSpace,
                                  breeze::utils::DataArrangement, typename>
                        typename SliceT>
  inline T atomic_add(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, T> address,
      T value) {
    return cl::sycl::atomic<T>{cl::sycl::global_ptr<T>{address.data()}}
        .fetch_add(value);
  }
  template <typename T, template <breeze::utils::AddressSpace,
                                  breeze::utils::DataArrangement, typename>
                        typename SliceT>
  inline T atomic_add(
      SliceT<breeze::utils::SHARED, breeze::utils::BLOCKED, T> address,
      T value) {
    return cl::sycl::atomic<T, cl::sycl::access::address_space::local_space>{
        cl::sycl::local_ptr<T>{address.data()}}
        .fetch_add(value);
  }
  template <typename T, template <breeze::utils::AddressSpace,
                                  breeze::utils::DataArrangement, typename>
                        typename SliceT>
  inline void atomic_min(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, T> address,
      T value) {
    cl::sycl::atomic<T>{cl::sycl::global_ptr<T>{address.data()}}.fetch_min(
        value);
  }
  template <typename T, template <breeze::utils::AddressSpace,
                                  breeze::utils::DataArrangement, typename>
                        typename SliceT>
  inline void atomic_max(
      SliceT<breeze::utils::GLOBAL, breeze::utils::BLOCKED, T> address,
      T value) {
    cl::sycl::atomic<T>{cl::sycl::global_ptr<T>{address.data()}}.fetch_max(
        value);
  }
  template <typename T>
  inline T reduce_add(T value) {
    return cl::sycl::reduce_over_group(work_item.get_sub_group(), value,
                                       cl::sycl::plus<T>());
  }
  template <typename T>
  inline T reduce_min(T value) {
    return cl::sycl::reduce_over_group(work_item.get_sub_group(), value,
                                       cl::sycl::minimum<T>());
  }
  template <typename T>
  inline T reduce_max(T value) {
    return cl::sycl::reduce_over_group(work_item.get_sub_group(), value,
                                       cl::sycl::maximum<T>());
  }
  template <typename T>
  inline T scan_add(T value) {
    return cl::sycl::inclusive_scan_over_group(work_item.get_sub_group(), value,
                                               cl::sycl::plus<T>());
  }
  template <typename T>
  inline T ballot(bool value) {
    int id = work_item.get_sub_group().get_local_linear_id();
    T thread_mask = static_cast<T>(value) << id;
    return cl::sycl::reduce_over_group(work_item.get_sub_group(), thread_mask,
                                       cl::sycl::plus<T>());
  }
  template <int MIN_BITS, typename T>
  inline unsigned match_any(T value) {
    unsigned result;
#pragma unroll
    for (unsigned i = 0; i < MIN_BITS; ++i) {
      unsigned current_bit = 1 << i;
      bool pred = (value & current_bit) == current_bit;
      unsigned mask = ballot<unsigned>(pred);
      if (!pred) {
        mask = ~mask;
      }
      result = (i == 0) ? mask : result & mask;
    }
    return result;
  }
  template <typename T>
  inline int count_leading_zeros(T value) {
    // FIXME: use cl::sycl::clz when available
    int count = 0;
    constexpr T kTopBitMask = static_cast<T>(1) << (sizeof(T) * 8 - 1);
    while (!(value & kTopBitMask)) {
      count += 1;
      value <<= 1;
    }
    return count;
  }
  template <typename T>
  inline int population_count(T value) {
    // FIXME: use cl::sycl::popcount when available
    int count = 0;
#pragma unroll
    for (unsigned i = 0; i < sizeof(T) * 8; ++i) {
      count += value & 1;
      value >>= 1;
    }
    return count;
  }
  template <typename T>
  inline T extract_bits(T value, int start_bit, int num_bits) {
    T mask = (static_cast<T>(1) << num_bits) - 1;
    return (value >> start_bit) & mask;
  }
  template <bool HIGH_PRIORITY>
  inline void scheduling_hint() {}
  template <typename SliceT>
  inline void prefetch(SliceT) {}
  cl::sycl::nd_item<1> work_item;
};
