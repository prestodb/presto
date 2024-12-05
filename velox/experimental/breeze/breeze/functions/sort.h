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

#include "breeze/functions/load.h"
#include "breeze/functions/scan.h"
#include "breeze/functions/store.h"
#include "breeze/platforms/platform.h"
#include "breeze/utils/types.h"

namespace breeze {
namespace functions {

template <typename T>
struct RadixSortTraits {
  static ATTR T to_bit_ordered(T value);
  static ATTR T from_bit_ordered(T value);
};

// specialization for T=int
template <>
struct RadixSortTraits<int> {
  static ATTR int to_bit_ordered(int value) {
    return value ^ (1 << utils::Msb<int>::VALUE);
  }
  static ATTR int from_bit_ordered(int value) {
    return value ^ (1 << utils::Msb<int>::VALUE);
  }
};

// specialization for T=unsigned
template <>
struct RadixSortTraits<unsigned> {
  static ATTR unsigned to_bit_ordered(unsigned value) { return value; }
  static ATTR unsigned from_bit_ordered(unsigned value) { return value; }
};

#if !defined(PLATFORM_METAL)

// specialization for T=long long
template <>
struct RadixSortTraits<long long> {
  static ATTR long long to_bit_ordered(long long value) {
    return value ^ (1ll << utils::Msb<long long>::VALUE);
  }
  static ATTR long long from_bit_ordered(long long value) {
    return value ^ (1ll << utils::Msb<long long>::VALUE);
  }
};

// specialization for T=unsigned long long
template <>
struct RadixSortTraits<unsigned long long> {
  static ATTR unsigned long long to_bit_ordered(unsigned long long value) {
    return value;
  }
  static ATTR unsigned long long from_bit_ordered(unsigned long long value) {
    return value;
  }
};

#endif  // !defined(PLATFORM_METAL)

template <typename SliceT>
class BitfieldExtractor {
 public:
  // data arrangement is inherited from the slice
#ifdef PLATFORM_METAL
  static constant utils::DataArrangement ARRANGEMENT = SliceT::ARRANGEMENT;
#else
  static constexpr utils::DataArrangement ARRANGEMENT = SliceT::ARRANGEMENT;
#endif

  ATTR BitfieldExtractor(SliceT data, int start_bit, int num_bits)
      : data_(data), start_bit_(start_bit), mask_((1u << num_bits) - 1) {}

  ATTR int operator[](int index) const {
    return (data_[index] >> start_bit_) & mask_;
  }

 private:
  SliceT data_;
  int start_bit_;
  unsigned mask_;
};

template <typename SliceT>
ATTR BitfieldExtractor<SliceT> constexpr make_bitfield_extractor(SliceT data,
                                                                 int start_bit,
                                                                 int num_bits) {
  return BitfieldExtractor<SliceT>(data, start_bit, num_bits);
}

enum SortBlockStatus {
  SORT_BLOCK_STATUS_NOT_READY = 0,
  SORT_BLOCK_STATUS_LOCAL = 1,
  SORT_BLOCK_STATUS_GLOBAL = 2,
  SORT_BLOCK_STATUS_SHIFT = 30,
  SORT_BLOCK_STATUS_VALUE_MASK = 0x3fffffff,
};

template <typename PlatformT, int ITEMS_PER_THREAD, int RADIX_BITS>
struct BlockRadixRank {
  enum {
    BLOCK_THREADS = PlatformT::BLOCK_THREADS,
    WARP_THREADS = PlatformT::WARP_THREADS,
    NUM_BINS = 1 << RADIX_BITS,
    NUM_WARPS = BLOCK_THREADS / WARP_THREADS,
    BINS_PER_THREAD = NUM_BINS / BLOCK_THREADS,
  };
  static_assert((PlatformT::BLOCK_THREADS % PlatformT::WARP_THREADS) == 0,
                "BLOCK_THREADS must be a multiple of WARP_THREADS");
  static_assert((NUM_BINS % PlatformT::BLOCK_THREADS) == 0,
                "NUM_BINS must be a multiple of BLOCK_THREADS");

  struct Scratch {
    int counters[NUM_WARPS][NUM_BINS];
    typename BlockScan<PlatformT, int, BINS_PER_THREAD>::Scratch scan;
  };

  template <typename BinSlice, typename RankSlice, typename HistogramSlice,
            typename LocalPrefixSlice, typename ExclusiveScanSlice,
            typename ScratchSlice>
  static ATTR void Rank(PlatformT p, BinSlice bins, RankSlice ranks,
                        HistogramSlice histogram, LocalPrefixSlice local_prefix,
                        ExclusiveScanSlice exclusive_scan,
                        ScratchSlice scratch) {
    using namespace utils;

    static_assert(BinSlice::ARRANGEMENT == WARP_STRIPED,
                  "input must have warp-striped arrangement");
    static_assert(RankSlice::ARRANGEMENT == WARP_STRIPED,
                  "output must have warp-striped arrangement");
    static_assert(HistogramSlice::ARRANGEMENT == STRIPED,
                  "histogram must have striped arrangement");
    static_assert(ScratchSlice::ARRANGEMENT == BLOCKED,
                  "scratch must have blocked arrangement");
    static_assert(IsSame<typename ScratchSlice::data_type, Scratch>::VALUE,
                  "incorrect scratch type");
    static_assert((BLOCK_THREADS % WARP_THREADS) == 0,
                  "BLOCK_THREADS must be a multiple of WARP_THREADS");
    static_assert((NUM_BINS % BLOCK_THREADS) == 0,
                  "NUM_BINS must be a multiple of BLOCK_THREADS");

    // start of shared warp counters for this warp
    auto scratch_warp_counters = &scratch->counters[p.warp_idx()][0];

    // initialize scratch counters for this warp
#pragma unroll
    for (int bin = p.lane_idx(); bin < NUM_BINS; bin += WARP_THREADS) {
      scratch_warp_counters[bin] = 0;
    }
    p.syncwarp();

    // compute warp histograms
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      p.atomic_add(make_slice<SHARED>(&scratch_warp_counters[bins[i]]), 1);
    }
    p.syncthreads();

    // up-sweep that computes histogram while adding lower warp counts to higher
    // warp counts
#pragma unroll
    for (int i = 0; i < BINS_PER_THREAD; ++i) {
      int idx = p.thread_idx() + i * BLOCK_THREADS;
      histogram[i] = 0;
#pragma unroll
      for (int j = 0; j < NUM_WARPS; ++j) {
        int count = scratch->counters[j][idx];
        scratch->counters[j][idx] = histogram[i];
        histogram[i] += count;
      }

      // atomic store of local prefix if requested by caller
      if constexpr (IsDifferent<LocalPrefixSlice, EmptySlice>::VALUE) {
        static_assert(LocalPrefixSlice::ARRANGEMENT == BLOCKED,
                      "local prefix must have blocked arrangement");

        p.atomic_store(local_prefix.subslice(idx),
                       (1u << SORT_BLOCK_STATUS_SHIFT) | histogram[i]);
      }
    }

    // inclusive scan over histogram
    int inclusive_scan[BINS_PER_THREAD];
    BlockScan<PlatformT, int, BINS_PER_THREAD>::template Scan<ScanOpAdd>(
        p, histogram, make_slice(inclusive_scan),
        make_slice<SHARED>(&scratch->scan));

    // down-sweep that adds the bin offsets from lower ranked warp counts to
    // higher ranked warp counts
#pragma unroll
    for (int i = 0; i < BINS_PER_THREAD; ++i) {
      int idx = p.thread_idx() + i * BLOCK_THREADS;
      int offset = inclusive_scan[i] - histogram[i];
#pragma unroll
      for (int j = 0; j < NUM_WARPS; ++j) {
        scratch->counters[j][idx] += offset;
      }

      // save exclusive scan if requested by caller
      if constexpr (IsDifferent<ExclusiveScanSlice, EmptySlice>::VALUE) {
        static_assert(ExclusiveScanSlice::ARRANGEMENT == STRIPED,
                      "exclusive scan must have striped arrangement");

        exclusive_scan[i] = offset;
      }
    }
    p.syncthreads();

#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      int bin = bins[i];

      // determine what lanes have the same bin
      auto same_bin_mask = p.template match_any<RADIX_BITS>(bin);

      // number of lanes with the same bin
      int warp_count = p.population_count(same_bin_mask);

      // number of lower-ranked lanes with the same bin
      int lower_rank_prefix =
          p.population_count(same_bin_mask & p.lower_rank_lanemask());

      // load warp count prefix
      int warp_count_prefix = scratch_warp_counters[bin];
      p.syncwarp();

      // first thread for each bin updates the counter
      if (lower_rank_prefix == 0) {
        scratch_warp_counters[bin] += warp_count;
      }
      p.syncwarp();

      // store final item rank
      ranks[i] = warp_count_prefix + lower_rank_prefix;
    }
  }

  template <typename BinSlice, typename RankSlice, typename ScratchSlice>
  static ATTR void Rank(PlatformT p, BinSlice bins, RankSlice ranks,
                        ScratchSlice scratch) {
    using namespace utils;

    int histogram[BINS_PER_THREAD];
    Rank(p, bins, ranks, make_slice(histogram), make_empty_slice(),
         make_empty_slice(), scratch);
  }
};

template <typename PlatformT, int ITEMS_PER_THREAD, int RADIX_BITS,
          typename KeyT, typename ValueT>
struct BlockRadixSort {
  enum {
    BLOCK_THREADS = PlatformT::BLOCK_THREADS,
    WARP_THREADS = PlatformT::WARP_THREADS,
    END_BIT = sizeof(KeyT) * /*BITS_PER_BYTE=*/8,
    NUM_PASSES = utils::DivideAndRoundUp<END_BIT, RADIX_BITS>::VALUE,
    NUM_BINS = 1 << RADIX_BITS,
    BINS_PER_THREAD = utils::DivideAndRoundUp<NUM_BINS, BLOCK_THREADS>::VALUE,
  };

  struct Scratch {
    union {
      typename BlockRadixRank<PlatformT, ITEMS_PER_THREAD, RADIX_BITS>::Scratch
          rank;
      struct {
        union {
          KeyT keys[BLOCK_THREADS * ITEMS_PER_THREAD];
          ValueT values[BLOCK_THREADS * ITEMS_PER_THREAD];
        };
      } scatter;
    };
  };

  template <typename KeySlice, typename ValueSlice, typename ScratchSlice>
  static ATTR void Sort(PlatformT p, KeySlice keys, ValueSlice values,
                        ScratchSlice scratch) {
    using namespace utils;

    static_assert(IsSame<typename ScratchSlice::data_type, Scratch>::VALUE,
                  "incorrect scratch type");

    // convert keys to bit ordered representation if needed
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      keys[i] = RadixSortTraits<KeyT>::to_bit_ordered(keys[i]);
    }

    // start from LSB and loop until no bits are left
#pragma unroll
    for (int i = 0; i < NUM_PASSES; ++i) {
      int start_bit = i * RADIX_BITS;
      int num_pass_bits = p.min(RADIX_BITS, END_BIT - start_bit);

      // determine stable rank for each key
      int ranks[ITEMS_PER_THREAD];
      BlockRadixRank<PlatformT, ITEMS_PER_THREAD, RADIX_BITS>::Rank(
          p, make_bitfield_extractor(keys, start_bit, num_pass_bits),
          make_slice<THREAD, KeySlice::ARRANGEMENT>(ranks),
          make_slice<SHARED>(&scratch->rank));
      p.syncthreads();

      // scatter keys by storing them in scratch using ranks
      BlockStoreAt<BLOCK_THREADS, ITEMS_PER_THREAD>(
          p, keys, make_slice<THREAD, KeySlice::ARRANGEMENT>(ranks),
          make_slice<SHARED>(scratch->scatter.keys));
      p.syncthreads();

      // load scattered keys
      BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
          p, make_slice<SHARED>(scratch->scatter.keys), keys);
      p.syncthreads();

      if constexpr (IsDifferent<ValueT, NullType>::VALUE) {
        // scatter values by storing them in scratch using ranks
        BlockStoreAt<BLOCK_THREADS, ITEMS_PER_THREAD>(
            p, values, make_slice<THREAD, KeySlice::ARRANGEMENT>(ranks),
            make_slice<SHARED>(scratch->scatter.values));
        p.syncthreads();

        // load scattered values
        BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(
            p, make_slice<SHARED>(scratch->scatter.values), values);
        p.syncthreads();
      }
    }

    // convert keys back to original representation
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      keys[i] = RadixSortTraits<KeyT>::from_bit_ordered(keys[i]);
    }
  }

  template <typename KeySlice, typename ValueSlice, typename ScratchSlice>
  static ATTR void Sort(PlatformT p, KeySlice keys, ValueSlice values,
                        ScratchSlice scratch, int num_items) {
    using namespace utils;

    enum {
      WARP_ITEMS = WARP_THREADS * ITEMS_PER_THREAD,
    };

    static_assert((BLOCK_THREADS % WARP_THREADS) == 0,
                  "BLOCK_THREADS must be a multiple of WARP_THREADS");
    static_assert(KeySlice::ARRANGEMENT == WARP_STRIPED,
                  "input must have warp-striped arrangement");

    int thread_offset = p.warp_idx() * WARP_ITEMS + p.lane_idx();

    // pad keys with values that have all bits set
    KeyT padded_keys[ITEMS_PER_THREAD];
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      padded_keys[i] = NumericLimits<KeyT>::max();
    }
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      if (thread_offset + (i * WARP_THREADS) < num_items) {
        padded_keys[i] = keys[i];
      }
    }

    if constexpr (IsDifferent<ValueT, NullType>::VALUE) {
      static_assert(ValueSlice::ARRANGEMENT == WARP_STRIPED,
                    "input must have warp-striped arrangement");

      ValueT padded_values[ITEMS_PER_THREAD];
#pragma unroll
      for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
        padded_values[i] = static_cast<ValueT>(0);
      }
#pragma unroll
      for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
        if (thread_offset + (i * WARP_THREADS) < num_items) {
          padded_values[i] = values[i];
        }
      }

      Sort(p, make_slice<THREAD, WARP_STRIPED>(padded_keys),
           make_slice<THREAD, WARP_STRIPED>(padded_values), scratch);

      // copy valid values back
#pragma unroll
      for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
        if (thread_offset + (i * WARP_THREADS) < num_items) {
          values[i] = padded_values[i];
        }
      }
    } else {
      Sort(p, make_slice<THREAD, WARP_STRIPED>(padded_keys), values, scratch);
    }

    // copy valid keys back
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      if (thread_offset + (i * WARP_THREADS) < num_items) {
        keys[i] = padded_keys[i];
      }
    }
  }
};

}  // namespace functions
}  // namespace breeze
