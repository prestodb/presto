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

#include "breeze/functions/sort.h"
#include "breeze/platforms/platform.h"
#include "breeze/utils/block_details.h"
#include "breeze/utils/types.h"

namespace breeze {
namespace algorithms {

template <int RADIX_BITS, typename T>
struct DeviceRadixSortHistogram {
  enum {
    NUM_BINS = 1 << RADIX_BITS,
    END_BIT = sizeof(T) * /*BITS_PER_BYTE=*/8,
    NUM_PASSES = utils::DivideAndRoundUp<END_BIT, RADIX_BITS>::VALUE,
    HISTOGRAM_SIZE = NUM_BINS * NUM_PASSES,
  };

  struct Scratch {
    unsigned data[HISTOGRAM_SIZE];
  };

  template <int ITEMS_PER_THREAD, int TILE_SIZE, typename PlatformT,
            typename InputSlice, typename HistogramSlice, typename ScratchSlice>
  static ATTR void Build(PlatformT p, const InputSlice in,
                         HistogramSlice histogram, ScratchSlice scratch,
                         int num_items) {
    using namespace functions;
    using namespace utils;

    enum {
      BLOCK_THREADS = PlatformT::BLOCK_THREADS,
    };

    static_assert(IsSame<typename ScratchSlice::data_type, Scratch>::VALUE,
                  "incorrect scratch type");

    // initialize scratch histogram
    for (int i = p.thread_idx(); i < HISTOGRAM_SIZE; i += BLOCK_THREADS) {
      scratch->data[i] = 0u;
    }

    // wait for scratch histogram to be initialized
    p.syncthreads();

#pragma unroll
    for (int tile_offset = 0; tile_offset < TILE_SIZE; ++tile_offset) {
      auto block =
          BlockDetails<BLOCK_THREADS, ITEMS_PER_THREAD,
                       TILE_SIZE>::from_tile_offset_and_num_items(p,
                                                                  tile_offset,
                                                                  num_items);

      // load items from input
      T items[ITEMS_PER_THREAD];
      const InputSlice it = in.subslice(block.offset);
      BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(p, it, make_slice(items),
                                                 block.num_items);

      // convert items to bit ordered representation
#pragma unroll
      for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
        if (p.thread_idx() + (i * BLOCK_THREADS) < block.num_items) {
          items[i] = RadixSortTraits<T>::to_bit_ordered(items[i]);
        }
      }

      // start from lsb and loop until no bits are left
#pragma unroll
      for (int j = 0; j < NUM_PASSES; ++j) {
        int start_bit = j * RADIX_BITS;
        int num_pass_bits = p.min(RADIX_BITS, END_BIT - start_bit);
        auto bfe = make_bitfield_extractor(make_slice(items), start_bit,
                                           num_pass_bits);
        // build scratch histogram
#pragma unroll
        for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
          if (p.thread_idx() + (i * BLOCK_THREADS) < block.num_items) {
            p.atomic_add(make_slice<SHARED>(&scratch->data[j * NUM_BINS])
                             .subslice(bfe[i]),
                         1u);
          }
        }
      }
    }

    // wait for scratch histogram to be ready
    p.syncthreads();

    // add scratch histogram counts to global histogram
    for (int i = p.thread_idx(); i < HISTOGRAM_SIZE; i += BLOCK_THREADS) {
      p.atomic_add(histogram.subslice(i), scratch->data[i]);
    }

    // hinting reconvergence here to make sure the atomics for merging into the
    // global histogram do not cause non-reconverging branches in this loop;
    // safe to do here because all threads of kernel can reach this point
    // together
    p.reconvergence_hint();
  }
};

template <typename T>
struct SortBlockType {
  static T from(T, functions::SortBlockStatus);
  static T value(T);
  static functions::SortBlockStatus status(T);
};

// specialization for T=unsigned
template <>
struct SortBlockType<unsigned> {
  ATTR static unsigned from(unsigned value, functions::SortBlockStatus status) {
    return static_cast<unsigned>(status) << functions::SORT_BLOCK_STATUS_SHIFT |
           value;
  }
  ATTR static unsigned value(unsigned type) {
    return type & functions::SORT_BLOCK_STATUS_VALUE_MASK;
  }
  ATTR static functions::SortBlockStatus status(unsigned type) {
    return static_cast<functions::SortBlockStatus>(
        type >> functions::SORT_BLOCK_STATUS_SHIFT);
  }
};

template <typename KeyT, typename ValueT, int BLOCK_ITEMS>
struct KeyValueScatterType {
  KeyT keys[BLOCK_ITEMS];
  ValueT values[BLOCK_ITEMS];
};

// partial specialization where ValueT is NullType
template <typename KeyT, int BLOCK_ITEMS>
struct KeyValueScatterType<KeyT, utils::NullType, BLOCK_ITEMS> {
  KeyT keys[BLOCK_ITEMS];
};

template <typename PlatformT, int ITEMS_PER_THREAD, int RADIX_BITS,
          typename KeyT, typename ValueT>
struct DeviceRadixSort {
  enum {
    BLOCK_THREADS = PlatformT::BLOCK_THREADS,
    BLOCK_ITEMS = BLOCK_THREADS * ITEMS_PER_THREAD,
    NUM_BINS = 1 << RADIX_BITS,
  };
  using BlockRadixRankT =
      typename functions::BlockRadixRank<PlatformT, ITEMS_PER_THREAD,
                                         RADIX_BITS>;

  struct Scratch {
    union {
      struct {
        typename BlockRadixRankT::Scratch rank;
        unsigned global_offsets[NUM_BINS];
        int block_idx;
      };
      KeyValueScatterType<KeyT, ValueT, BLOCK_ITEMS> scatter;
    };
  };

  template <typename BlockT, typename KeyInputSlice, typename ValueInputSlice,
            typename OffsetSlice, typename KeyOutputSlice,
            typename ValueOutputSlice, typename BlockIdxSlice,
            typename BlockSlice, typename ScratchSlice>
  static ATTR void Sort(PlatformT p, const KeyInputSlice in_keys,
                        const ValueInputSlice in_values,
                        const OffsetSlice in_offsets, int start_bit,
                        int num_pass_bits, KeyOutputSlice out_keys,
                        ValueOutputSlice out_values,
                        BlockIdxSlice next_block_idx, BlockSlice blocks,
                        ScratchSlice scratch, int num_items) {
    using namespace functions;
    using namespace utils;

    enum {
      END_BIT = sizeof(KeyT) * /*BITS_PER_BYTE=*/8,
      WARP_THREADS = PlatformT::WARP_THREADS,
      NUM_WARPS = BLOCK_THREADS / WARP_THREADS,
      WARP_ITEMS = WARP_THREADS * ITEMS_PER_THREAD,
      BINS_PER_THREAD = NUM_BINS / BLOCK_THREADS,
    };

    static_assert((NUM_BINS % BLOCK_THREADS) == 0,
                  "NUM_BINS must be a multiple of BLOCK_THREADS");
    static_assert(IsSame<typename ScratchSlice::data_type, Scratch>::VALUE,
                  "incorrect scratch type");

    p.template scheduling_hint</*HIGH_PRIORITY=*/true>();

    // retrieve an ordered block index
    if (p.thread_idx() == 0) {
      scratch->block_idx = p.atomic_add(next_block_idx, 1);
    }
    p.syncthreads();
    int block_idx = scratch->block_idx;

    auto block =
        BlockDetails<BLOCK_THREADS,
                     ITEMS_PER_THREAD>::from_block_idx_and_num_items(block_idx,
                                                                     num_items);

    // load items into warp-striped arrangement after initializing all values
    // to all bits set as that allows us to always use the fast-path version
    // radix rank function
    KeyT keys[ITEMS_PER_THREAD];
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      keys[i] = NumericLimits<KeyT>::max();
    }
    const KeyInputSlice it = in_keys.subslice(block.offset);
    BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
        p, it, make_slice<THREAD, WARP_STRIPED>(keys), block.num_items);

    // convert items to bit ordered representation
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      keys[i] = RadixSortTraits<KeyT>::to_bit_ordered(keys[i]);
    }

    // determine stable rank for each item
    int ranks[ITEMS_PER_THREAD];
    int histogram[BINS_PER_THREAD];
    int exclusive_scan[BINS_PER_THREAD];
    BlockRadixRankT::Rank(
        p,
        make_bitfield_extractor(make_slice<THREAD, WARP_STRIPED>(keys),
                                start_bit, num_pass_bits),
        make_slice<THREAD, WARP_STRIPED>(ranks), make_slice(histogram),
        blocks.subslice(block_idx * NUM_BINS), make_slice(exclusive_scan),
        make_slice<SHARED>(&scratch->rank));
    p.syncthreads();

    // scatter keys by storing them in scratch using ranks
    BlockStoreAt<BLOCK_THREADS, ITEMS_PER_THREAD>(
        p, make_slice<THREAD, WARP_STRIPED>(keys),
        make_slice<THREAD, WARP_STRIPED>(ranks),
        make_slice<SHARED>(scratch->scatter.keys));

    // load and scatter optional values
    ValueT values[ITEMS_PER_THREAD];
    if constexpr (IsDifferent<ValueT, NullType>::VALUE) {
      const ValueInputSlice it = in_values.subslice(block.offset);
      BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
          p, it, make_slice<THREAD, WARP_STRIPED>(values), block.num_items);
      // scatter values by storing them in scratch using ranks
      BlockStoreAt<BLOCK_THREADS, ITEMS_PER_THREAD>(
          p, make_slice<THREAD, WARP_STRIPED>(values),
          make_slice<THREAD, WARP_STRIPED>(ranks),
          make_slice<SHARED>(scratch->scatter.values));
    }
    p.syncthreads();

    // first block loads initial global offsets from input and other blocks
    // use decoupled lookback to determine global offsets for each bin.
    //
    // each thread is responsible for lookback of a specific bin and there's
    // limit to the lookback distance but progress can only be made by all
    // threads in a warp together.
    unsigned global_offsets[BINS_PER_THREAD];
    if (block_idx == 0) {
      BlockLoad<BLOCK_THREADS, BINS_PER_THREAD>(p, in_offsets,
                                                make_slice(global_offsets));

      // make all global prefixes for block available to subsequent blocks
#pragma unroll
      for (int i = 0; i < BINS_PER_THREAD; ++i) {
        int idx = p.thread_idx() + (i * BLOCK_THREADS);
        p.atomic_store(
            blocks.subslice(block_idx * NUM_BINS + idx),
            SortBlockType<BlockT>::from(global_offsets[i] + histogram[i],
                                        SORT_BLOCK_STATUS_GLOBAL));
      }
    } else {
      p.template scheduling_hint</*HIGH_PRIORITY=*/false>();

#pragma unroll
      for (int i = 0; i < BINS_PER_THREAD; ++i) {
        int idx = p.thread_idx() + (i * BLOCK_THREADS);
        int prefix_idx = block_idx - 1;

        // global offset is zero initially
        global_offsets[i] = 0u;

        typedef unsigned WarpMask;
        static_assert(sizeof(WarpMask) * 8 == WARP_THREADS, "bad warp size");

        // wait for global status for all threads in warp
        WarpMask non_global_status_mask = 0xffffffff;
        do {
          // loop until all threads in warp have a local or better status
          BlockT prefix_block;
          WarpMask not_ready_status_mask = 0xffffffff;
          do {
            // atomic load of prefix block if thread has a non-global status
            prefix_block =
                (non_global_status_mask & (1 << p.lane_idx()))
                    ? p.atomic_load(
                          blocks.subslice(prefix_idx * NUM_BINS + idx))
                    : SortBlockType<BlockT>::from(0, SORT_BLOCK_STATUS_GLOBAL);

            // warp ballot to communicate what threads have not-ready status
            bool not_ready_status =
                SortBlockType<BlockT>::status(prefix_block) ==
                SORT_BLOCK_STATUS_NOT_READY;
            not_ready_status_mask =
                p.template ballot<WarpMask>(not_ready_status);
          } while (not_ready_status_mask != 0);

          // check for global status
          bool global_status = SortBlockType<BlockT>::status(prefix_block) ==
                               SORT_BLOCK_STATUS_GLOBAL;

          // add prefix value to global offset if we have global status or
          // prefix index is greater than zero and local status is therefor
          // allowed
          if (global_status || prefix_idx > 0) {
            global_offsets[i] += SortBlockType<BlockT>::value(prefix_block);
          }

          // advance to previous prefix block index if possible
          prefix_idx = p.max(0, prefix_idx - 1);

          // determine if any thread in warp still has a non-global status
          non_global_status_mask = p.template ballot<WarpMask>(!global_status);
        } while (non_global_status_mask != 0);

        // make global prefix for block available to subsequent blocks
        p.atomic_store(
            blocks.subslice(block_idx * NUM_BINS + idx),
            SortBlockType<BlockT>::from(global_offsets[i] + histogram[i],
                                        SORT_BLOCK_STATUS_GLOBAL));
      }
    }

    // remove exclusive scan results from global offsets
#pragma unroll
    for (int i = 0; i < BINS_PER_THREAD; ++i) {
      global_offsets[i] -= exclusive_scan[i];
    }

    // gather scattered keys from scratch
    BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
        p, make_slice<SHARED>(scratch->scatter.keys), make_slice(keys));

    // gather optional scattered values from scratch
    if constexpr (IsDifferent<ValueT, NullType>::VALUE) {
      // gather scattered values from scratch
      BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
          p, make_slice<SHARED>(scratch->scatter.values), make_slice(values));
    }
    p.syncthreads();

    // store global offsets in scratch
    BlockStore<BLOCK_THREADS, BINS_PER_THREAD>(
        p, make_slice(global_offsets),
        make_slice<SHARED>(scratch->global_offsets));
    p.syncthreads();

    // gather global output offsets for each item
    unsigned out_offsets[ITEMS_PER_THREAD];
    BlockLoadFrom<BLOCK_THREADS, ITEMS_PER_THREAD>(
        p, make_slice<SHARED>(scratch->global_offsets),
        make_bitfield_extractor(make_slice(keys), start_bit, num_pass_bits),
        make_slice(out_offsets));

    // add item index (same as rank after scatter/gather) to output offsets
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      out_offsets[i] += p.thread_idx() + i * BLOCK_THREADS;
    }

    // convert keys back to original representation
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      keys[i] = RadixSortTraits<KeyT>::from_bit_ordered(keys[i]);
    }

    // store gathered keys in global memory using output offsets
    BlockStoreAt<BLOCK_THREADS, ITEMS_PER_THREAD>(p, make_slice(keys),
                                                  make_slice(out_offsets),
                                                  out_keys, block.num_items);

    // store gathered values in global memory using output offsets
    if constexpr (IsDifferent<ValueT, NullType>::VALUE) {
      BlockStoreAt<BLOCK_THREADS, ITEMS_PER_THREAD>(
          p, make_slice(values), make_slice(out_offsets), out_values,
          block.num_items);
    }
  }
};

}  // namespace algorithms
}  // namespace breeze
