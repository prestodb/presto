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

template <int STRIDE, int ITEMS_PER_THREAD, typename InputSlice,
          typename ItemSlice>
ATTR void BlockLoad(const InputSlice in, int thread_offset, ItemSlice items) {
  using namespace utils;

  static_assert(InputSlice::ARRANGEMENT == BLOCKED,
                "input must have blocked arrangement");

  const InputSlice thread_it = in.subslice(thread_offset);

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    items[i] = thread_it[i * STRIDE];
  }
}

template <int STRIDE, int ITEMS_PER_THREAD, typename InputSlice,
          typename ItemSlice>
ATTR void BlockLoad(const InputSlice in, int thread_offset, ItemSlice items,
                    int num_items) {
  using namespace utils;

  static_assert(InputSlice::ARRANGEMENT == BLOCKED,
                "input must have blocked arrangement");

  const InputSlice thread_it = in.subslice(thread_offset);

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (thread_offset + (i * STRIDE) < num_items) {
      items[i] = thread_it[i * STRIDE];
    }
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename InputSlice, typename ItemSlice>
ATTR void BlockLoad(PlatformT p, const InputSlice in, ItemSlice items) {
  using namespace utils;

  static_assert(ItemSlice::ARRANGEMENT == STRIPED ||
                    ItemSlice::ARRANGEMENT == WARP_STRIPED,
                "output must have striped or warp-striped arrangement");

  if constexpr (ItemSlice::ARRANGEMENT == WARP_STRIPED) {
    enum {
      WARP_THREADS = PlatformT::WARP_THREADS,
      WARP_ITEMS = WARP_THREADS * ITEMS_PER_THREAD,
    };
    static_assert((BLOCK_THREADS % WARP_THREADS) == 0,
                  "BLOCK_THREADS must be a multiple of WARP_THREADS");
    // load items into warp-striped arrangement
    BlockLoad</*STRIDE=*/WARP_THREADS, ITEMS_PER_THREAD>(
        in, p.warp_idx() * WARP_ITEMS + p.lane_idx(), items);
    return;
  }

  // load items into striped arrangement
  BlockLoad</*STRIDE=*/BLOCK_THREADS, ITEMS_PER_THREAD>(in, p.thread_idx(),
                                                        items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename InputSlice, typename ItemSlice>
ATTR void BlockLoad(PlatformT p, const InputSlice in, ItemSlice items,
                    int num_items) {
  using namespace utils;

  // fast-path check
  if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
    BlockLoad<BLOCK_THREADS, ITEMS_PER_THREAD>(p, in, items);
    return;
  }

  if constexpr (ItemSlice::ARRANGEMENT == WARP_STRIPED) {
    enum {
      WARP_THREADS = PlatformT::WARP_THREADS,
      WARP_ITEMS = WARP_THREADS * ITEMS_PER_THREAD,
    };
    // load items into warp-striped arrangement
    BlockLoad</*STRIDE=*/WARP_THREADS, ITEMS_PER_THREAD>(
        in, p.warp_idx() * WARP_ITEMS + p.lane_idx(), items, num_items);
    return;
  }

  // load items into striped arrangement
  BlockLoad</*STRIDE=*/BLOCK_THREADS, ITEMS_PER_THREAD>(in, p.thread_idx(),
                                                        items, num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename InputSlice, typename SelectionFlagSlice, typename ItemSlice>
ATTR void BlockLoadIf(PlatformT p, const InputSlice block_it,
                      const SelectionFlagSlice selection_flags,
                      ItemSlice items) {
  using namespace utils;

  static_assert(InputSlice::ARRANGEMENT == BLOCKED,
                "input must have blocked arrangement");
  static_assert(SelectionFlagSlice::ARRANGEMENT == STRIPED,
                "selection flags must have striped arrangement");
  static_assert(ItemSlice::ARRANGEMENT == STRIPED,
                "output must have striped arrangement");

  const InputSlice thread_it = block_it.subslice(p.thread_idx());

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (selection_flags[i]) {
      items[i] = thread_it[i * BLOCK_THREADS];
    }
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename InputSlice, typename SelectionFlagSlice, typename ItemSlice>
ATTR void BlockLoadIf(PlatformT p, const InputSlice block_it,
                      const SelectionFlagSlice selection_flags, ItemSlice items,
                      int num_items) {
  // fast-path check
  if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
    BlockLoadIf<BLOCK_THREADS, ITEMS_PER_THREAD>(p, block_it, selection_flags,
                                                 items);
    return;
  }

  const InputSlice thread_it = block_it.subslice(p.thread_idx());

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (p.thread_idx() + (i * BLOCK_THREADS) < num_items) {
      if (selection_flags[i]) {
        items[i] = thread_it[i * BLOCK_THREADS];
      }
    }
  }
}

template <int ITEMS_PER_THREAD, typename InputSlice, typename OffsetSlice,
          typename ItemSlice>
ATTR void BlockLoadFrom(const InputSlice in, const OffsetSlice offsets,
                        ItemSlice items) {
  using namespace utils;

  static_assert(InputSlice::ARRANGEMENT == BLOCKED,
                "input must have blocked arrangement");
  static_assert(OffsetSlice::ARRANGEMENT == ItemSlice::ARRANGEMENT,
                "offsets must the same arrangement as items");

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    items[i] = in[offsets[i]];
  }
}

template <int STRIDE, int ITEMS_PER_THREAD, typename InputSlice,
          typename OffsetSlice, typename ItemSlice>
ATTR void BlockLoadFrom(const InputSlice in, const OffsetSlice offsets,
                        int thread_offset, ItemSlice items, int num_items) {
  using namespace utils;

  static_assert(InputSlice::ARRANGEMENT == BLOCKED,
                "input must have blocked arrangement");
  static_assert(OffsetSlice::ARRANGEMENT == ItemSlice::ARRANGEMENT,
                "offsets must the same arrangement as items");

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (thread_offset + (i * STRIDE) < num_items) {
      items[i] = in[offsets[i]];
    }
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename InputSlice, typename OffsetSlice, typename ItemSlice>
ATTR void BlockLoadFrom(PlatformT, const InputSlice in,
                        const OffsetSlice offsets, ItemSlice items) {
  using namespace utils;

  static_assert(ItemSlice::ARRANGEMENT == STRIPED ||
                    ItemSlice::ARRANGEMENT == WARP_STRIPED,
                "output must have striped or warp-striped arrangement");

  // arrangement agnostic load of items
  BlockLoadFrom<ITEMS_PER_THREAD>(in, offsets, items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename InputSlice, typename OffsetSlice, typename ItemSlice>
ATTR void BlockLoadFrom(PlatformT p, const InputSlice in,
                        const OffsetSlice offsets, ItemSlice items,
                        int num_items) {
  using namespace utils;

  // fast-path check
  if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
    BlockLoadFrom<BLOCK_THREADS, ITEMS_PER_THREAD>(p, in, offsets, items);
    return;
  }

  if constexpr (ItemSlice::ARRANGEMENT == WARP_STRIPED) {
    enum {
      WARP_THREADS = PlatformT::WARP_THREADS,
      WARP_ITEMS = WARP_THREADS * ITEMS_PER_THREAD,
    };
    // load items into warp-striped arrangement
    BlockLoadFrom</*STRIDE=*/WARP_THREADS, ITEMS_PER_THREAD>(
        in, offsets, p.warp_idx() * WARP_ITEMS + p.lane_idx(), items,
        num_items);
    return;
  }

  // load items into striped arrangement
  BlockLoadFrom</*STRIDE=*/BLOCK_THREADS, ITEMS_PER_THREAD>(
      in, offsets, p.thread_idx(), items, num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename InputSlice, typename OffsetSlice,
          typename SelectionFlagSlice, typename ItemSlice>
ATTR void BlockLoadFromIf(PlatformT, const InputSlice in,
                          const OffsetSlice offsets,
                          const SelectionFlagSlice selection_flags,
                          ItemSlice items) {
  using namespace utils;

  static_assert(InputSlice::ARRANGEMENT == BLOCKED,
                "input must have blocked arrangement");
  static_assert(OffsetSlice::ARRANGEMENT == STRIPED,
                "offsets must have striped arrangement");
  static_assert(SelectionFlagSlice::ARRANGEMENT == STRIPED,
                "selection flags must have striped arrangement");
  static_assert(ItemSlice::ARRANGEMENT == STRIPED,
                "output must have striped arrangement");

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (selection_flags[i]) {
      items[i] = in[offsets[i]];
    }
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename InputSlice, typename OffsetSlice,
          typename SelectionFlagSlice, typename ItemSlice>
ATTR void BlockLoadFromIf(PlatformT p, const InputSlice in,
                          const OffsetSlice offsets,
                          const SelectionFlagSlice selection_flags,
                          ItemSlice items, int num_items) {
  // fast-path check
  if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
    BlockLoadFromIf<BLOCK_THREADS, ITEMS_PER_THREAD>(p, in, offsets,
                                                     selection_flags, items);
    return;
  }

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (p.thread_idx() + (i * BLOCK_THREADS) < num_items) {
      if (selection_flags[i]) {
        items[i] = in[offsets[i]];
      }
    }
  }
}

}  // namespace functions
}  // namespace breeze
