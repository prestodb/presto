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

#include "breeze/platforms/platform.h"
#include "breeze/utils/types.h"

namespace breeze {
namespace functions {

template <typename PlatformT, typename T, int ITEMS_PER_THREAD>
struct BlockScan {
  enum {
    BLOCK_THREADS = PlatformT::BLOCK_THREADS,
    WARP_THREADS = PlatformT::WARP_THREADS,
    NUM_WARPS = BLOCK_THREADS / WARP_THREADS,
    BLOCK_SIZE = BLOCK_THREADS * ITEMS_PER_THREAD,
    NUM_AGGREGATES = BLOCK_SIZE / WARP_THREADS,
    NUM_WARP_SCANS_FOR_AGGREGATES =
        utils::DivideAndRoundUp<NUM_AGGREGATES, WARP_THREADS>::VALUE,
  };
  static_assert((BLOCK_THREADS % WARP_THREADS) == 0,
                "BLOCK_THREADS must be a multiple of WARP_THREADS");
  static_assert(WARP_THREADS > 1, "WARP_THREADS must be greater than 1");
  static_assert((BLOCK_THREADS % WARP_THREADS) == 0,
                "BLOCK_THREADS must be a multiple of WARP_THREADS");

  struct Scratch {
    T aggregates[BLOCK_SIZE / WARP_THREADS];
    T inclusive_last;
  };

  // fast-path for inclusive scan using scratch memory and warp-level primitives
  template <typename Op, typename ItemSlice, typename ValueSlice,
            typename ScratchSlice>
  static ATTR T Scan(PlatformT p, ItemSlice items, ValueSlice values,
                     ScratchSlice scratch) {
    using namespace utils;

    static_assert(ItemSlice::ARRANGEMENT == STRIPED,
                  "input must have striped arrangement");
    static_assert(ValueSlice::ARRANGEMENT == STRIPED,
                  "output must have striped arrangement");
    static_assert(ScratchSlice::ARRANGEMENT == BLOCKED,
                  "scratch must have blocked arrangement");
    static_assert(IsSame<typename ScratchSlice::data_type, Scratch>::VALUE,
                  "incorrect scratch type");

    int lane = p.lane_idx();
    int warp_idx = p.warp_idx();

    // use warp scans to compute independent warp results
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      // perform scan across warp
      values[i] = Op::warp_scan(p, items[i]);

      // store warp aggregate in shared memory
      if (lane == (WARP_THREADS - 1)) {
        scratch->aggregates[NUM_WARPS * i + warp_idx] = values[i];
      }
    }

    // initialize inclusive last
    if (p.thread_idx() == 0) {
      scratch->inclusive_last = Op::template identity<T>();
    }

    // wait for stores of warp aggregates to complete
    p.syncthreads();

    // use the first warp to perform scans over aggregates
    if (warp_idx == 0) {
#pragma unroll
      for (int i = 0; i < NUM_WARP_SCANS_FOR_AGGREGATES; ++i) {
        int idx = WARP_THREADS * i + p.thread_idx();

        // load warp aggregates from shared memory
        T aggregate = idx < NUM_AGGREGATES ? scratch->aggregates[idx]
                                           : Op::template identity<T>();

        // perform scan across warp
        T inclusive_last = Op::warp_scan(p, aggregate);

        // store exclusive last in shared memory
        if (idx < NUM_AGGREGATES) {
          inclusive_last = Op::op(p, scratch->inclusive_last, inclusive_last);
          scratch->aggregates[idx] = inclusive_last - aggregate;

          // update inclusive last for next iteration
          if (lane == (WARP_THREADS - 1)) {
            scratch->inclusive_last = inclusive_last;
          }
        }

        // wait for store of inclusive last to complete
        p.syncwarp();
      }
    }

    // wait for stores of exclusive lasts to complete
    p.syncthreads();

    // use aggregates to turn independent warp results into final values
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      values[i] =
          Op::op(p, scratch->aggregates[NUM_WARPS * i + warp_idx], values[i]);
    }

    return values[ITEMS_PER_THREAD - 1];
  }

  // inclusive scan using scratch memory and warp-level primitives
  template <typename Op, typename ItemSlice, typename ValueSlice,
            typename ScratchSlice>
  static ATTR T Scan(PlatformT p, ItemSlice items, ValueSlice values,
                     ScratchSlice scratch, int num_items) {
    using namespace utils;

    // fast-path check
    if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
      return Scan<Op>(p, items, values, scratch);
    }

    static_assert(ItemSlice::ARRANGEMENT == STRIPED,
                  "input must have striped arrangement");
    static_assert(ValueSlice::ARRANGEMENT == STRIPED,
                  "input must have striped arrangement");
    static_assert(ScratchSlice::ARRANGEMENT == BLOCKED,
                  "scratch must have blocked arrangement");
    static_assert(IsSame<typename ScratchSlice::data_type, Scratch>::VALUE,
                  "incorrect scratch type");

    int lane = p.lane_idx();
    int warp_idx = p.warp_idx();

#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      T item = (p.thread_idx() + (i * BLOCK_THREADS) < num_items)
                   ? items[i]
                   : Op::template identity<T>();

      values[i] = Op::warp_scan(p, item);

      // store warp aggregate in shared memory
      if (lane == (WARP_THREADS - 1)) {
        scratch->aggregates[NUM_WARPS * i + warp_idx] = values[i];
      }
    }

    // initialize inclusive last
    if (p.thread_idx() == 0) {
      scratch->inclusive_last = Op::template identity<T>();
    }

    // wait for stores of warp aggregates to complete
    p.syncthreads();

    // use the first warp to perform scans over aggregates
    if (warp_idx == 0) {
#pragma unroll
      for (int i = 0; i < NUM_WARP_SCANS_FOR_AGGREGATES; ++i) {
        int idx = WARP_THREADS * i + p.thread_idx();

        // load warp aggregates from shared memory
        T aggregate = idx < NUM_AGGREGATES ? scratch->aggregates[idx]
                                           : Op::template identity<T>();

        // perform scan across warp
        T inclusive_last = Op::warp_scan(p, aggregate);

        // store exclusive last in shared memory
        if (idx < NUM_AGGREGATES) {
          inclusive_last = Op::op(p, scratch->inclusive_last, inclusive_last);
          scratch->aggregates[idx] = inclusive_last - aggregate;

          // update inclusive last for next iteration
          if (lane == (WARP_THREADS - 1)) {
            scratch->inclusive_last = inclusive_last;
          }
        }

        // wait for store of inclusive last to complete
        p.syncwarp();
      }
    }

    // wait for stores of exclusive lasts to complete
    p.syncthreads();

    // use aggregates to turn independent warp results into final values
    T value = Op::template identity<T>();
#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      value =
          Op::op(p, scratch->aggregates[NUM_WARPS * i + warp_idx], values[i]);
      if (p.thread_idx() + (i * BLOCK_THREADS) < num_items) {
        values[i] = value;
      }
    }

    return value;
  }
};

//
// Scan operators
//

struct ScanOpAdd {
  template <typename T>
  static ATTR T identity() {
    return 0;
  }
  template <typename PlatformT, typename T, typename U>
  static ATTR T op(PlatformT, T lhs, U rhs) {
    return lhs + rhs;
  }
  template <typename PlatformT, typename T>
  static ATTR T warp_scan(PlatformT p, T value) {
    return p.scan_add(value);
  }
};

}  // namespace functions
}  // namespace breeze
