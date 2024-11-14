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
#include "breeze/utils/block_details.h"
#include "breeze/utils/types.h"

namespace breeze {
namespace algorithms {

using ScanOpAdd = functions::ScanOpAdd;

namespace internal {

enum ScanBlockStatus {
  SCAN_BLOCK_STATUS_NOT_READY = 0,
  SCAN_BLOCK_STATUS_INCLUSIVE_LAST = 1,
  SCAN_BLOCK_STATUS_PREFIX = 2,
};

template <typename T>
struct ScanBlockType {
  static T from(T, ScanBlockStatus);
  static T value(T);
  static ScanBlockStatus status(T);
};

// specialization for T=unsigned
template <>
struct ScanBlockType<unsigned> {
  ATTR static unsigned from(unsigned value, ScanBlockStatus status) {
    return static_cast<unsigned>(status) << 30 | (value & 0x3fffffff);
  }
  ATTR static unsigned value(unsigned type) { return type & 0x3fffffff; }
  ATTR static ScanBlockStatus status(unsigned type) {
    return static_cast<ScanBlockStatus>(type >> 30);
  }
};

#if !defined(PLATFORM_METAL)

// specialization for T=unsigned long long
template <>
struct ScanBlockType<unsigned long long> {
  ATTR static unsigned long long from(unsigned long long value,
                                      ScanBlockStatus status) {
    return static_cast<unsigned long long>(status) << 62 |
           (value & 0x3fffffffffffffff);
  }
  ATTR static unsigned long long value(unsigned long long type) {
    return type & 0x3fffffffffffffff;
  }
  ATTR static ScanBlockStatus status(unsigned long long type) {
    return static_cast<ScanBlockStatus>(type >> 62);
  }
};

#endif  // !PLATFORM_METAL

}  // namespace internal

template <typename PlatformT, typename T, int ITEMS_PER_THREAD,
          int LOOKBACK_DISTANCE>
struct DeviceScan {
  enum {
    BLOCK_THREADS = PlatformT::BLOCK_THREADS,
    WARP_THREADS = PlatformT::WARP_THREADS,
  };
  using BlockScanT =
      typename functions::BlockScan<PlatformT, T, ITEMS_PER_THREAD>;

  struct Scratch {
    typename BlockScanT::Scratch block;
    int block_idx;
    T inclusive_last;
    T lookback_values[LOOKBACK_DISTANCE / WARP_THREADS];
    int lookback_status_masks[LOOKBACK_DISTANCE / WARP_THREADS];
    T prefix;
  };

  // scan implementation that uses decoupled look-back with configurable
  // lookback distance
  template <typename Op, typename InputSlice, typename OutputSlice,
            typename BlockIdxSlice, typename BlockSlice, typename ScratchSlice>
  static ATTR void Scan(PlatformT p, const InputSlice in, OutputSlice out,
                        BlockIdxSlice next_block_idx, BlockSlice blocks,
                        ScratchSlice scratch, int num_items) {
    using namespace functions;
    using namespace utils;
    using namespace internal;

    static_assert(IsSame<typename ScratchSlice::data_type, Scratch>::VALUE,
                  "incorrect scratch type");

    using InputT = RemoveConst<typename InputSlice::data_type>;
    using BlockT = typename BlockSlice::data_type;
    using BlockTypeT = ScanBlockType<BlockT>;

    p.template scheduling_hint</*HIGH_PRIORITY=*/true>();

    InputT items[ITEMS_PER_THREAD];
    T results[ITEMS_PER_THREAD];

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

    const InputSlice item_it = in.subslice(block.offset);
    BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(p, item_it, make_slice(items),
                                               block.num_items);

    T inclusive_last = BlockScanT::template Scan<Op>(
        p, make_slice(items), make_slice(results),
        make_slice<SHARED>(&scratch->block), block.num_items);

    // make inclusive last available to subsequent blocks
    if (p.thread_idx() == (BLOCK_THREADS - 1)) {
      p.atomic_store(
          make_slice<BlockSlice::ADDRESS_SPACE>(&blocks[block_idx]),
          BlockTypeT::from(inclusive_last, SCAN_BLOCK_STATUS_INCLUSIVE_LAST));
      scratch->inclusive_last = inclusive_last;
    }
    p.syncthreads();

    p.template scheduling_hint</*HIGH_PRIORITY=*/false>();

    static_assert((BLOCK_THREADS % WARP_THREADS) == 0,
                  "BLOCK_THREADS must be a multiple of WARP_THREADS");
    static_assert((LOOKBACK_DISTANCE % WARP_THREADS) == 0,
                  "LOOKBACK_DISTANCE must be a multiple of WARP_THREADS");

    enum {
      NUM_WARPS = BLOCK_THREADS / WARP_THREADS,
      LOOKBACK_WARPS = LOOKBACK_DISTANCE / WARP_THREADS,
    };

    static_assert(LOOKBACK_WARPS <= NUM_WARPS,
                  "LOOKBACK_WARPS must be less or equal to NUM_WARPS");

    // warp-sized decoupled look-back of block prefix
    //
    // 1. threads of lower ranked warps wait for block prefixes to become
    //    available, status of first thread must be a prefix and not an
    //    inclusive last
    // 2. extract block prefix status and use warp ballot to determine the
    //    prefix blocks with complete prefix results
    // 3. extract prefix values that should be used to determine the prefix
    //    for this block, all values from prefix blocks after the last block
    //    with a complete prefix result should be included
    // 4. use warp scans to determine the prefixes for each warp
    // 5. last lookback thread accumulates prefixes to determine the prefix
    //    for this block and uses an atomic store to share it with subsequent
    //    blocks
    BlockT prefix_block;
    int idx = p.thread_idx();
    if (idx < LOOKBACK_DISTANCE) {
      // prefix indices ordered by thread index
      int prefix_idx = block_idx - LOOKBACK_DISTANCE + idx;
      // first thread must wait for the complete prefix to be ready
      int min_status = idx == 0 ? SCAN_BLOCK_STATUS_PREFIX
                                : SCAN_BLOCK_STATUS_INCLUSIVE_LAST;

      typedef unsigned WarpMask;
      static_assert(sizeof(WarpMask) * 8 == WARP_THREADS, "bad warp size");

      // wait for sufficient status for all threads in warp
      WarpMask insufficient_status_mask;
      do {
        prefix_block =
            prefix_idx >= 0
                ? p.atomic_load(make_slice<BlockSlice::ADDRESS_SPACE>(
                      &blocks[prefix_idx]))
                : BlockTypeT::from(0, SCAN_BLOCK_STATUS_PREFIX);
        bool insufficient_status =
            BlockTypeT::status(prefix_block) < min_status;

        // warp ballot to communicate what threads have an insufficient status
        insufficient_status_mask =
            p.template ballot<WarpMask>(insufficient_status);
      } while (insufficient_status_mask != 0);

      bool prefix_status =
          BlockTypeT::status(prefix_block) == SCAN_BLOCK_STATUS_PREFIX;

      // warp ballot to communicate what threads have a prefix status
      WarpMask prefix_status_mask = p.template ballot<WarpMask>(prefix_status);

      // mask used to determine if prefix value should be ignored
      auto higher_rank_mask = p.higher_rank_lanemask();

      // use prefix value if no higher ranked threads have a prefix status
      T prefix_value = (prefix_status_mask & higher_rank_mask) == 0
                           ? BlockTypeT::value(prefix_block)
                           : Op::template identity<T>();

      // use a warp scan to reduce this to a prefix for this block
      T prefix = Op::warp_scan(p, prefix_value);

      // last lane stores warp lookback in scratch memory
      int lane = p.lane_idx();
      if (lane == (WARP_THREADS - 1)) {
        int warp_idx = p.warp_idx();
        scratch->lookback_values[warp_idx] = prefix;
        scratch->lookback_status_masks[warp_idx] = prefix_status_mask;
      }
    }

    // wait for stores of lookback results to complete
    p.syncthreads();

    // first thread accumulates prefixes and makes block prefix available to
    // subsequent blocks
    if (idx == 0) {
      T prefix = scratch->lookback_values[LOOKBACK_WARPS - 1];
      int prefix_status_mask =
          scratch->lookback_status_masks[LOOKBACK_WARPS - 1];

      // loop until status mask is set
#pragma unroll
      for (int i = LOOKBACK_WARPS - 2; prefix_status_mask == 0 && i >= 0; --i) {
        prefix = Op::op(p, scratch->lookback_values[i], prefix);
        prefix_status_mask = scratch->lookback_status_masks[i];
      }

      // make prefix for block available to subsequent blocks
      p.atomic_store(
          make_slice<BlockSlice::ADDRESS_SPACE>(&blocks[block_idx]),
          BlockTypeT::from(Op::op(p, prefix, scratch->inclusive_last),
                           SCAN_BLOCK_STATUS_PREFIX));

      // store block prefix in scratch memory
      scratch->prefix = prefix;
    }

    // wait for block prefix store to complete
    p.syncthreads();

    // update results using prefix from previous block
    T prefix = scratch->prefix;
    if (block.num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
#pragma unroll
      for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
        results[i] = Op::op(p, prefix, results[i]);
      }
    } else {
#pragma unroll
      for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
        if (p.thread_idx() + (i * BLOCK_THREADS) < block.num_items) {
          results[i] = Op::op(p, prefix, results[i]);
        }
      }
    }

    OutputSlice out_it = out.subslice(block.offset);
    BlockStore<BLOCK_THREADS, ITEMS_PER_THREAD>(p, make_slice(results), out_it,
                                                block.num_items);
  }
};

}  // namespace algorithms
}  // namespace breeze
