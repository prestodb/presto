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

#include <omp.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"

template <typename T>
struct supported_type {
  static T min();
  static T max();
  static T atomic_load(T *address);
  static void atomic_store(T *address, T value);
  static bool atomic_cas(T *address, T *compare, T value, bool weak);
  static int count_leading_zeros(T value);
  static int population_count(T value);
  static T extract_bits(T value, int start_bit, int num_bits);
};

// <unsigned> specialization.
template <>
struct supported_type<unsigned> {
  static unsigned min() { return 0; }
  static unsigned max() { return __INT_MAX__ * 2u + 1; }
  static unsigned atomic_load(unsigned *address) {
    return __atomic_load_n(address, __ATOMIC_RELAXED);
  }
  static void atomic_store(unsigned *address, unsigned value) {
    __atomic_store_n(address, value, __ATOMIC_RELAXED);
  }
  static bool atomic_cas(unsigned *address, unsigned *compare, unsigned value,
                         bool weak) {
    return __atomic_compare_exchange_n(address, compare, value, weak,
                                       __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  }
  static int count_leading_zeros(unsigned value) {
    return __builtin_clz(value);
  }
  static int population_count(unsigned value) {
    return __builtin_popcount(value);
  }
  static unsigned extract_bits(unsigned value, int start_bit, int num_bits) {
    unsigned mask = (1u << num_bits) - 1;
    return (value >> start_bit) & mask;
  }
};

// <int> specialization.
template <>
struct supported_type<int> {
  static int min() { return -__INT_MAX__ - 1; }
  static int max() { return __INT_MAX__; }
  static int atomic_load(int *address) {
    return __atomic_load_n(address, __ATOMIC_RELAXED);
  }
  static void atomic_store(int *address, int value) {
    __atomic_store_n(address, value, __ATOMIC_RELAXED);
  }
  static bool atomic_cas(int *address, int *compare, int value, bool weak) {
    return __atomic_compare_exchange_n(address, compare, value, weak,
                                       __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  }
};

// <unsigned long long> specialization.
template <>
struct supported_type<unsigned long long> {
  static unsigned long long min() { return 0; }
  static unsigned long long max() { return __LONG_LONG_MAX__ * 2ull + 1; }
  static unsigned long long atomic_load(unsigned long long *address) {
    return __atomic_load_n(address, __ATOMIC_RELAXED);
  }
  static void atomic_store(unsigned long long *address,
                           unsigned long long value) {
    __atomic_store_n(address, value, __ATOMIC_RELAXED);
  }
  static bool atomic_cas(unsigned long long *address,
                         unsigned long long *compare, unsigned long long value,
                         bool weak) {
    return __atomic_compare_exchange_n(address, compare, value, weak,
                                       __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  }
  static int count_leading_zeros(unsigned long long value) {
    return __builtin_clzll(value);
  }
  static int population_count(unsigned long long value) {
    return __builtin_popcount(value);
  }
  static unsigned long long extract_bits(unsigned long long value,
                                         int start_bit, int num_bits) {
    unsigned long long mask = (1llu << num_bits) - 1;
    return (value >> start_bit) & mask;
  }
};

// <long long> specialization.
template <>
struct supported_type<long long> {
  static long long min() { return -__LONG_LONG_MAX__ - 1; }
  static long long max() { return __LONG_LONG_MAX__; }
  static long long atomic_load(long long *address) {
    return __atomic_load_n(address, __ATOMIC_RELAXED);
  }
  static void atomic_store(long long *address, long long value) {
    __atomic_store_n(address, value, __ATOMIC_RELAXED);
  }
  static bool atomic_cas(long long *address, long long *compare,
                         long long value, bool weak) {
    return __atomic_compare_exchange_n(address, compare, value, weak,
                                       __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  }
};

// <float> specialization.
template <>
struct supported_type<float> {
  static_assert(sizeof(float) == sizeof(unsigned), "unexpected type sizes");
  static float min() { return __FLT_MIN__; }
  static float max() { return __FLT_MAX__; }
  static float atomic_load(float *address) {
    union {
      float value;
      unsigned raw;
    } content;
    content.raw = __atomic_load_n(reinterpret_cast<unsigned *>(address),
                                  __ATOMIC_RELAXED);
    return content.value;
  }
  static void atomic_store(float *address, float value) {
    __atomic_store_n(reinterpret_cast<unsigned *>(address),
                     *reinterpret_cast<unsigned *>(&value), __ATOMIC_RELAXED);
  }
  static bool atomic_cas(float *address, float *compare, float value,
                         bool weak) {
    return __atomic_compare_exchange_n(reinterpret_cast<unsigned *>(address),
                                       reinterpret_cast<unsigned *>(compare),
                                       *reinterpret_cast<unsigned *>(&value),
                                       weak, __ATOMIC_RELAXED,
                                       __ATOMIC_RELAXED);
  }
};

// <double> specialization.
template <>
struct supported_type<double> {
  static_assert(sizeof(double) == sizeof(unsigned long long),
                "unexpected type sizes");
  static double min() { return __DBL_MIN__; }
  static double max() { return __DBL_MAX__; }
  static double atomic_load(double *address) {
    union {
      double value;
      unsigned long long raw;
    } content;
    content.raw = __atomic_load_n(
        reinterpret_cast<unsigned long long *>(address), __ATOMIC_RELAXED);
    return content.value;
  }
  static void atomic_store(double *address, double value) {
    __atomic_store_n(reinterpret_cast<unsigned long long *>(address),
                     *reinterpret_cast<unsigned long long *>(&value),
                     __ATOMIC_RELAXED);
  }
  static bool atomic_cas(double *address, double *compare, double value,
                         bool weak) {
    return __atomic_compare_exchange_n(
        reinterpret_cast<unsigned long long *>(address),
        reinterpret_cast<unsigned long long *>(compare),
        *reinterpret_cast<unsigned long long *>(&value), weak, __ATOMIC_RELAXED,
        __ATOMIC_RELAXED);
  }
};

// <unsigned char> specialization.
template <>
struct supported_type<unsigned char> {
  static unsigned char min() { return 0; }
  static unsigned char max() { return 255; }
  static unsigned atomic_load(unsigned char *address) {
    return __atomic_load_n(address, __ATOMIC_RELAXED);
  }
  static void atomic_store(unsigned char *address, unsigned char value) {
    __atomic_store_n(address, value, __ATOMIC_RELAXED);
  }
  static bool atomic_cas(unsigned char *address, unsigned char *compare,
                         unsigned char value, bool weak) {
    return __atomic_compare_exchange_n(address, compare, value, weak,
                                       __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  }
  static int count_leading_zeros(unsigned char value) {
    return __builtin_clz(value);
  }
  static int population_count(unsigned char value) {
    return __builtin_popcount(value);
  }
  static unsigned char extract_bits(unsigned char value, int start_bit,
                                    int num_bits) {
    unsigned mask = (1u << num_bits) - 1;
    return (value >> start_bit) & mask;
  }
};

template <int OPENMP_BLOCK_THREADS, int OPENMP_WARP_THREADS>
struct OpenMPPlatform {
  enum {
    BLOCK_THREADS = OPENMP_BLOCK_THREADS,
    WARP_THREADS = OPENMP_WARP_THREADS,
  };
  inline int thread_idx() { return omp_get_thread_num(); }
  inline int block_idx() { return block_idx_; }
  inline void syncthreads() {
#pragma omp barrier
  }
  inline void syncwarp() {
    static_assert(WARP_THREADS == BLOCK_THREADS,
                  "WARP_THREADS must be the same as BLOCK_THREADS");
#pragma omp barrier
  }
  inline int lane_idx() { return omp_get_thread_num() % WARP_THREADS; }
  inline int warp_idx() { return omp_get_thread_num() / WARP_THREADS; }
  inline unsigned lower_rank_lanemask() {
    static_assert(WARP_THREADS <= sizeof(unsigned) * 8,
                  "WARP_THREADS must be less or equal to unsigned bits");
    return (1u << lane_idx()) - 1;
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
    return lhs < rhs ? lhs : rhs;
  }
  template <typename T>
  inline T max(T lhs, T rhs) {
    return lhs > rhs ? lhs : rhs;
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  inline T atomic_load(SliceT address) {
    return supported_type<T>::atomic_load(address.data());
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  inline void atomic_store(SliceT address, T value) {
    supported_type<T>::atomic_store(address.data(), value);
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  inline T atomic_cas(SliceT address, T compare, T value) {
    supported_type<T>::atomic_cas(address.data(), &compare, value, false);
    return compare;
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  inline T atomic_add(SliceT address, T value) {
    T current = atomic_load(address);
    T new_value;
    do {
      new_value = current + value;
    } while (!supported_type<T>::atomic_cas(address.data(), &current, new_value,
                                            true));
    return current;
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  inline void atomic_min(SliceT address, T value) {
    T current = atomic_load(address);
    while (current > value) {
      if (supported_type<T>::atomic_cas(address.data(), &current, value,
                                        true)) {
        break;
      }
    }
  }
  template <typename SliceT, typename T = typename SliceT::data_type>
  inline void atomic_max(SliceT address, T value) {
    T current = atomic_load(address);
    while (current < value) {
      if (supported_type<T>::atomic_cas(address.data(), &current, value,
                                        true)) {
        break;
      }
    }
  }
  template <typename T>
  inline T reduce_add(T value) {
    static_assert(WARP_THREADS == BLOCK_THREADS,
                  "WARP_THREADS must be the same as BLOCK_THREADS");

    T *scratch = reinterpret_cast<T *>(shared_scratch_);
#pragma omp single
    scratch[0] = static_cast<T>(0);

#pragma omp for reduction(+ : scratch[0]) schedule(static)
    for (int i = 0; i < BLOCK_THREADS; ++i) {
      scratch[0] += value;
    }
    value = scratch[0];
#pragma omp barrier
    return value;
  }
  template <typename T>
  inline T reduce_min(T value) {
    static_assert(WARP_THREADS == BLOCK_THREADS,
                  "WARP_THREADS must be the same as BLOCK_THREADS");

    T *scratch = reinterpret_cast<T *>(shared_scratch_);

#pragma omp single
    scratch[0] = supported_type<T>::max();

#pragma omp for reduction(min : scratch[0]) schedule(static)
    for (int i = 0; i < BLOCK_THREADS; ++i) {
      scratch[0] = min(value, scratch[0]);
    }
    value = scratch[0];
#pragma omp barrier
    return value;
  }
  template <typename T>
  inline T reduce_max(T value) {
    static_assert(WARP_THREADS == BLOCK_THREADS,
                  "WARP_THREADS must be the same as BLOCK_THREADS");

    T *scratch = reinterpret_cast<T *>(shared_scratch_);
#pragma omp single
    scratch[0] = supported_type<T>::min();

#pragma omp for reduction(max : scratch[0]) schedule(static)
    for (int i = 0; i < BLOCK_THREADS; ++i) {
      scratch[0] = max(value, scratch[0]);
    }
    value = scratch[0];
#pragma omp barrier
    return value;
  }
  template <typename T>
  inline T scan_add(T value) {
    static_assert(WARP_THREADS == BLOCK_THREADS,
                  "WARP_THREADS must be the same as BLOCK_THREADS");

    T *scratch = reinterpret_cast<T *>(shared_scratch_);
#pragma omp single
    scratch[0] = static_cast<T>(0);

    // FIXME: this is really slow
    int idx = omp_get_thread_num();
    for (int i = 0; i < BLOCK_THREADS; ++i) {
      if (idx == i) {
        scratch[0] += value;
        value = scratch[0];
      }
#pragma omp barrier
    }

    return value;
  }
  template <typename T>
  inline T ballot(bool value) {
    static_assert(WARP_THREADS == BLOCK_THREADS,
                  "WARP_THREADS must be the same as BLOCK_THREADS");

    int id = omp_get_thread_num();
    T thread_mask = static_cast<T>(value) << id;
    return reduce_add(thread_mask);
  }
  template <int MIN_BITS, typename T>
  inline unsigned match_any(T value) {
    static_assert(WARP_THREADS == BLOCK_THREADS,
                  "WARP_THREADS must be the same as BLOCK_THREADS");

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
    return supported_type<T>::count_leading_zeros(value);
  }
  template <typename T>
  inline int population_count(T value) {
    return supported_type<T>::population_count(value);
  }
  template <typename T>
  inline T extract_bits(T value, int start_bit, int num_bits) {
    return supported_type<T>::extract_bits(value, start_bit, num_bits);
  }
  template <bool HIGH_PRIORITY>
  inline void scheduling_hint() {}
  int block_idx_;
  void *shared_scratch_;
};

#pragma GCC diagnostic pop
