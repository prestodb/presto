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
#include "breeze/functions/reduce.h"
#include "breeze/platforms/platform.h"
#include "breeze/utils/block_details.h"
#include "breeze/utils/types.h"

namespace breeze {
namespace algorithms {

using ReduceOpAdd = functions::ReduceOpAdd;
using ReduceOpMin = functions::ReduceOpMin;
using ReduceOpMax = functions::ReduceOpMax;

template <typename PlatformT, typename T>
struct DeviceReduce {
  enum {
    BLOCK_THREADS = PlatformT::BLOCK_THREADS,
    WARP_THREADS = PlatformT::WARP_THREADS,
  };
  using BlockReduceT = typename functions::BlockReduce<PlatformT, T>;

  struct Scratch {
    typename BlockReduceT::Scratch block;
  };

  // Reduce items to a single value. The `Op` type determines how items are
  // reduced.
  //
  // Blocks of items are aggregated using scratch memory and warp-level
  // primitives for improved performance.
  template <typename Op, int ITEMS_PER_THREAD, typename InputSlice,
            typename OutputSlice, typename ScratchSlice>
  static ATTR void Reduce(PlatformT p, const InputSlice in, OutputSlice out,
                          ScratchSlice scratch, unsigned num_items) {
    using namespace functions;
    using namespace utils;

    static_assert(IsSame<typename ScratchSlice::data_type, Scratch>::VALUE,
                  "incorrect scratch type");

    using InputT = RemoveConst<typename InputSlice::data_type>;

    InputT items[ITEMS_PER_THREAD];

    auto block = BlockDetails<BLOCK_THREADS, ITEMS_PER_THREAD>::from_num_items(
        p, num_items);

    // load items
    const InputSlice it = in.subslice(block.offset);
    BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(p, it, make_slice(items),
                                               block.num_items);

    // compute aggregate for thread block
    T aggregate = BlockReduceT::template Reduce<Op, ITEMS_PER_THREAD>(
        p, make_slice(items), make_slice<SHARED>(&scratch->block),
        block.num_items);

    // first thread uses atomics to calculate final result
    if (p.thread_idx() == 0) {
      Op::atomic_op(p, out, aggregate);
    }
  }
};

}  // namespace algorithms
}  // namespace breeze
