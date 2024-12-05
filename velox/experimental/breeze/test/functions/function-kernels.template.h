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

// Note: This file is simply a pseudo-c++ template for the purposes of being
// specialized across the 6 supported backends. Only a limited subset of what
// you might expect to do in a C++ file will be supported here. In particular:
//    1. Includes will be copied verbatim
//    2. Function signatures will be copied but modified for individual
//    backends
//    3. Function bodies will copied verbatim.
//
// There are a few special attributes for communicating extra information:
//    1. PLATFORM(name) => the function makes use of a backend-specific
//    Platform object called `name`.
//    2. SHARED_MEM(type, name) => the function uses a shared variable of the
//    given type and name.

#define PLATFORM(X) [[clang::annotate("PlatformName=" X)]]
#define SHARED_MEM(T, id) [[clang::annotate("SharedMem=" T ";" id)]]

#include "breeze/functions/load.h"
#include "breeze/functions/reduce.h"
#include "breeze/functions/scan.h"
#include "breeze/functions/sort.h"
#include "breeze/functions/store.h"
#include "breeze/platforms/platform.h"

namespace kernels {

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T>
PLATFORM("p")
void BlockLoad(const T* in, T* out, int num_items) {
  T items[ITEMS_PER_THREAD];
  breeze::functions::BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice<breeze::utils::GLOBAL>(in),
      breeze::utils::make_slice(items), num_items);
  breeze::functions::BlockStore<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice(items),
      breeze::utils::make_slice<breeze::utils::GLOBAL>(out), num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T>
PLATFORM("p")
void BlockLoadIf(const T* in, const int* in_selection_flags, T* out,
                 int num_items) {
  int selection_flags[ITEMS_PER_THREAD];
  breeze::functions::BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice<breeze::utils::GLOBAL>(in_selection_flags),
      breeze::utils::make_slice(selection_flags), num_items);
  T items[ITEMS_PER_THREAD];
  breeze::functions::BlockLoadIf<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice<breeze::utils::GLOBAL>(in),
      breeze::utils::make_slice(selection_flags),
      breeze::utils::make_slice(items), num_items);
  breeze::functions::BlockStoreIf<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice(items),
      breeze::utils::make_slice(selection_flags),
      breeze::utils::make_slice<breeze::utils::GLOBAL>(out), num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T>
PLATFORM("p")
void BlockLoadFrom(const T* in, const int* in_offsets, T* out, int num_items) {
  int offsets[ITEMS_PER_THREAD];
  breeze::functions::BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice<breeze::utils::GLOBAL>(in_offsets),
      breeze::utils::make_slice(offsets), num_items);
  T items[ITEMS_PER_THREAD];
  breeze::functions::BlockLoadFrom<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice<breeze::utils::GLOBAL>(in),
      breeze::utils::make_slice(offsets), breeze::utils::make_slice(items),
      num_items);
  breeze::functions::BlockStore<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice(items),
      breeze::utils::make_slice<breeze::utils::GLOBAL>(out), num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T>
PLATFORM("p")
void BlockStore(const T* in, T* out, int num_items) {
  breeze::functions::BlockStore<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p,
      breeze::utils::make_slice<breeze::utils::GLOBAL, breeze::utils::STRIPED>(
          in),
      breeze::utils::make_slice<breeze::utils::GLOBAL>(out), num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T>
PLATFORM("p")
void BlockStoreIf(const T* in, const int* selection_flags, T* out,
                  int num_items) {
  breeze::functions::BlockStoreIf<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p,
      breeze::utils::make_slice<breeze::utils::GLOBAL, breeze::utils::STRIPED>(
          in),
      breeze::utils::make_slice<breeze::utils::GLOBAL, breeze::utils::STRIPED>(
          selection_flags),
      breeze::utils::make_slice<breeze::utils::GLOBAL>(out), num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T>
PLATFORM("p")
void BlockStoreAt(const T* in, const int* offsets, T* out, int num_items) {
  breeze::functions::BlockStoreAt<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p,
      breeze::utils::make_slice<breeze::utils::GLOBAL, breeze::utils::STRIPED>(
          in),
      breeze::utils::make_slice<breeze::utils::GLOBAL, breeze::utils::STRIPED>(
          offsets),
      breeze::utils::make_slice<breeze::utils::GLOBAL>(out), num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T>
PLATFORM("p")
void BlockStoreAtIf(const T* in, const int* offsets, const int* selection_flags,
                    T* out, int num_items) {
  breeze::functions::BlockStoreAtIf<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p,
      breeze::utils::make_slice<breeze::utils::GLOBAL, breeze::utils::STRIPED>(
          in),
      breeze::utils::make_slice<breeze::utils::GLOBAL, breeze::utils::STRIPED>(
          offsets),
      breeze::utils::make_slice<breeze::utils::GLOBAL, breeze::utils::STRIPED>(
          selection_flags),
      breeze::utils::make_slice<breeze::utils::GLOBAL>(out), num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T>
PLATFORM("p")
void BlockFill(const T* value, T* out, int num_items) {
  breeze::functions::BlockFill<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, *value, breeze::utils::make_slice<breeze::utils::GLOBAL>(out),
      num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T>
PLATFORM("p")
void BlockFillAtIf(const T* value, const int* offsets,
                   const int* selection_flags, T* out, int num_items) {
  breeze::functions::BlockFillAtIf<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, *value,
      breeze::utils::make_slice<breeze::utils::GLOBAL, breeze::utils::STRIPED>(
          offsets),
      breeze::utils::make_slice<breeze::utils::GLOBAL, breeze::utils::STRIPED>(
          selection_flags),
      breeze::utils::make_slice<breeze::utils::GLOBAL>(out), num_items);
}

template <typename Op, int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T,
          typename U>
PLATFORM("p")
SHARED_MEM("typename breeze::functions::BlockReduce<PlatformT, U>::Scratch",
           "scratch")
void BlockReduce(const T* in, U* out, int num_items) {
  T items[ITEMS_PER_THREAD];
  breeze::functions::BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice<breeze::utils::GLOBAL>(in),
      breeze::utils::make_slice(items), num_items);
  U aggregate = breeze::functions::BlockReduce<PlatformT, U>::template Reduce<
      Op, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice(items),
      breeze::utils::make_slice<breeze::utils::SHARED>(scratch), num_items);
  p.syncthreads();
  if (p.thread_idx() == 0) {
    *out = aggregate;
  }
}

template <typename Op, int BLOCK_THREADS, int ITEMS_PER_THREAD, typename T,
          typename U>
PLATFORM("p")
SHARED_MEM(
    "typename breeze::functions::BlockScan<PlatformT, U, ITEMS_PER_THREAD>::Scratch",
    "scratch")
void BlockScan(const T* in, U* out, int num_items) {
  T items[ITEMS_PER_THREAD];
  breeze::functions::BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice<breeze::utils::GLOBAL>(in),
      breeze::utils::make_slice(items), num_items);
  U sums[ITEMS_PER_THREAD];
  breeze::functions::BlockScan<PlatformT, U, ITEMS_PER_THREAD>::template Scan<
      Op>(p, breeze::utils::make_slice(items), breeze::utils::make_slice(sums),
          breeze::utils::make_slice<breeze::utils::SHARED>(scratch), num_items);
  breeze::functions::BlockStore<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice(sums),
      breeze::utils::make_slice<breeze::utils::GLOBAL>(out), num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS, typename T>
PLATFORM("p")
SHARED_MEM(
    "typename breeze::functions::BlockRadixRank<PlatformT, ITEMS_PER_THREAD, RADIX_BITS>::Scratch",
    "scratch")
void BlockRadixRank(const T* in, int* out, int num_items) {
  T items[ITEMS_PER_THREAD];
  // initialize invalid items to max value
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    items[i] = static_cast<T>((1 << RADIX_BITS) - 1);
  }
  breeze::functions::BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice<breeze::utils::GLOBAL>(in),
      breeze::utils::make_slice<breeze::utils::THREAD,
                                breeze::utils::WARP_STRIPED>(items),
      num_items);
  int ranks[ITEMS_PER_THREAD];
  breeze::functions::BlockRadixRank<PlatformT, ITEMS_PER_THREAD, RADIX_BITS>::
      Rank(p,
           breeze::utils::make_slice<breeze::utils::THREAD,
                                     breeze::utils::WARP_STRIPED>(items),
           breeze::utils::make_slice<breeze::utils::THREAD,
                                     breeze::utils::WARP_STRIPED>(ranks),
           breeze::utils::make_slice<breeze::utils::SHARED>(scratch));
  breeze::functions::BlockStore<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p,
      breeze::utils::make_slice<breeze::utils::THREAD,
                                breeze::utils::WARP_STRIPED>(ranks),
      breeze::utils::make_slice<breeze::utils::GLOBAL>(out), num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int RADIX_BITS,
          typename KeyT, typename ValueT>
PLATFORM("p")
SHARED_MEM(
    "typename breeze::functions::BlockRadixSort<PlatformT, ITEMS_PER_THREAD, RADIX_BITS, KeyT, ValueT>::Scratch",
    "scratch")
void BlockRadixSort(const KeyT* keys_in, const ValueT* values_in,
                    KeyT* keys_out, ValueT* values_out, int num_items) {
  KeyT keys[ITEMS_PER_THREAD];
  breeze::functions::BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p, breeze::utils::make_slice<breeze::utils::GLOBAL>(keys_in),
      breeze::utils::make_slice<breeze::utils::THREAD,
                                breeze::utils::WARP_STRIPED>(keys),
      num_items);
  if constexpr (breeze::utils::IsDifferent<ValueT,
                                           breeze::utils::NullType>::VALUE) {
    ValueT values[ITEMS_PER_THREAD];
    breeze::functions::BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
        p, breeze::utils::make_slice<breeze::utils::GLOBAL>(values_in),
        breeze::utils::make_slice<breeze::utils::THREAD,
                                  breeze::utils::WARP_STRIPED>(values),
        num_items);
    breeze::functions::BlockRadixSort<PlatformT, ITEMS_PER_THREAD, RADIX_BITS,
                                      KeyT, ValueT>::
        Sort(p,
             breeze::utils::make_slice<breeze::utils::THREAD,
                                       breeze::utils::WARP_STRIPED>(keys),
             breeze::utils::make_slice<breeze::utils::THREAD,
                                       breeze::utils::WARP_STRIPED>(values),
             breeze::utils::make_slice<breeze::utils::SHARED>(scratch),
             num_items);
    breeze::functions::BlockStore<BLOCK_THREADS, ITEMS_PER_THREAD>(
        p,
        breeze::utils::make_slice<breeze::utils::THREAD,
                                  breeze::utils::WARP_STRIPED>(values),
        breeze::utils::make_slice<breeze::utils::GLOBAL>(values_out),
        num_items);
  } else {
    breeze::functions::BlockRadixSort<
        PlatformT, ITEMS_PER_THREAD, RADIX_BITS, KeyT,
        ValueT>::Sort(p,
                      breeze::utils::make_slice<breeze::utils::THREAD,
                                                breeze::utils::WARP_STRIPED>(
                          keys),
                      breeze::utils::make_empty_slice(),
                      breeze::utils::make_slice<breeze::utils::SHARED>(scratch),
                      num_items);
  }
  breeze::functions::BlockStore<BLOCK_THREADS, ITEMS_PER_THREAD>(
      p,
      breeze::utils::make_slice<breeze::utils::THREAD,
                                breeze::utils::WARP_STRIPED>(keys),
      breeze::utils::make_slice<breeze::utils::GLOBAL>(keys_out), num_items);
}

}  // namespace kernels
