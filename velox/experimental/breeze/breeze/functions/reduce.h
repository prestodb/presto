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

template <typename PlatformT, typename T>
struct BlockReduce {
  enum {
    BLOCK_THREADS = PlatformT::BLOCK_THREADS,
    WARP_THREADS = PlatformT::WARP_THREADS,
    NUM_WARPS = BLOCK_THREADS / WARP_THREADS,
  };
  static_assert((BLOCK_THREADS % WARP_THREADS) == 0,
                "BLOCK_THREADS must be a multiple of WARP_THREADS");

  struct Scratch {
    T data[NUM_WARPS];
  };

  // reduce a single item per thread
  template <typename Op, typename ScratchSlice>
  static ATTR T Reduce(PlatformT p, T value, ScratchSlice scratch) {
    using namespace utils;

    static_assert(IsSame<typename ScratchSlice::data_type, Scratch>::VALUE,
                  "incorrect scratch type");
    static_assert(WARP_THREADS > 1, "WARP_THREADS must be greater than 1");
    static_assert(WARP_THREADS >= NUM_WARPS,
                  "WARP_THREADS must be greater or equal to NUM_WARPS");

    // calculate aggregates across warps
    value = Op::warp_reduce(p, value);

    // store warp aggregates in scratch
    int warp_idx = p.warp_idx();
    int lane = p.lane_idx();
    if (lane == 0) {
      scratch->data[warp_idx] = value;
    }

    // wait for stores of warp aggregates to complete
    p.syncthreads();

    // load the aggregates into the first warp
    int idx = p.thread_idx();
    value = (idx < NUM_WARPS) ? scratch->data[idx] : Op::template identity<T>();

    // use the first warp to reduce aggregates to a single value
    return warp_idx == 0 ? Op::warp_reduce(p, value)
                         : Op::template identity<T>();
  }

  // reduce a full block of items using `Op`
  template <typename Op, int ITEMS_PER_THREAD, typename ItemSlice,
            typename ScratchSlice>
  static ATTR T Reduce(PlatformT p, ItemSlice items, ScratchSlice scratch) {
    T thread_aggregate = items[0];

#pragma unroll
    for (int i = 1; i < ITEMS_PER_THREAD; ++i) {
      thread_aggregate = Op::op(p, thread_aggregate, items[i]);
    }

    return Reduce<Op>(p, thread_aggregate, scratch);
  }

  // reduce `num_items` of items using `Op`
  template <typename Op, int ITEMS_PER_THREAD, typename ItemSlice,
            typename ScratchSlice>
  static ATTR T Reduce(PlatformT p, ItemSlice items, ScratchSlice scratch,
                       int num_items) {
    using namespace utils;

    // fast-path check
    if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
      return Reduce<Op, ITEMS_PER_THREAD>(p, items, scratch);
    }

    static_assert(ItemSlice::ARRANGEMENT == STRIPED,
                  "input must have striped arrangement");

    T thread_aggregate = Op::template identity<T>();

#pragma unroll
    for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
      if (p.thread_idx() + (i * BLOCK_THREADS) < num_items) {
        thread_aggregate = Op::op(p, thread_aggregate, items[i]);
      }
    }

    return Reduce<Op>(p, thread_aggregate, scratch);
  }
};

//
// reduce operators
//

struct ReduceOpAdd {
  template <typename T>
  static ATTR T identity() {
    return 0;
  }
  template <typename PlatformT, typename T, typename U>
  static ATTR T op(PlatformT, T lhs, U rhs) {
    return lhs + rhs;
  }
  template <typename PlatformT, typename T>
  static ATTR T warp_reduce(PlatformT p, T value) {
    return p.reduce_add(value);
  }
  template <typename PlatformT, typename LhsSlice, typename U>
  static ATTR void atomic_op(PlatformT p, LhsSlice lhs, U rhs) {
    p.atomic_add(lhs, rhs);
  }
};

struct ReduceOpMin {
  template <typename T>
  static ATTR T identity() {
    return utils::NumericLimits<T>::max();
  }
  template <typename PlatformT, typename T, typename U>
  static ATTR T op(PlatformT p, T lhs, U rhs) {
    return p.min(lhs, rhs);
  }
  template <typename PlatformT, typename T>
  static ATTR T warp_reduce(PlatformT p, T value) {
    return p.reduce_min(value);
  }
  template <typename PlatformT, typename LhsSlice, typename U>
  static ATTR void atomic_op(PlatformT p, LhsSlice lhs, U rhs) {
    p.atomic_min(lhs, rhs);
  }
};

struct ReduceOpMax {
  template <typename T>
  static ATTR T identity() {
    return utils::NumericLimits<T>::min();
  }
  template <typename PlatformT, typename T, typename U>
  static ATTR T op(PlatformT p, T lhs, U rhs) {
    return p.max(lhs, rhs);
  }
  template <typename PlatformT, typename T>
  static ATTR T warp_reduce(PlatformT p, T value) {
    return p.reduce_max(value);
  }
  template <typename PlatformT, typename LhsSlice, typename U>
  static ATTR void atomic_op(PlatformT p, LhsSlice lhs, U rhs) {
    p.atomic_max(lhs, rhs);
  }
};

}  // namespace functions
}  // namespace breeze
