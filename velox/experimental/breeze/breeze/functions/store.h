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

template <int STRIDE, int ITEMS_PER_THREAD, typename ItemSlice,
          typename OutputSlice>
ATTR void BlockStore(const ItemSlice items, int thread_offset,
                     OutputSlice out) {
  using namespace utils;

  static_assert(OutputSlice::ARRANGEMENT == BLOCKED,
                "output must have blocked arrangement");

  OutputSlice thread_it = out.subslice(thread_offset);

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    thread_it[i * STRIDE] = items[i];
  }
}

template <int STRIDE, int ITEMS_PER_THREAD, typename ItemSlice,
          typename OutputSlice>
ATTR void BlockStore(const ItemSlice items, int thread_offset, OutputSlice out,
                     int num_items) {
  using namespace utils;

  static_assert(OutputSlice::ARRANGEMENT == BLOCKED,
                "output must have blocked arrangement");

  OutputSlice thread_it = out.subslice(thread_offset);

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (thread_offset + (i * STRIDE) < num_items) {
      thread_it[i * STRIDE] = items[i];
    }
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename ItemSlice, typename OutputSlice>
ATTR void BlockStore(PlatformT p, const ItemSlice items, OutputSlice out) {
  using namespace utils;

  static_assert(ItemSlice::ARRANGEMENT == STRIPED ||
                    ItemSlice::ARRANGEMENT == WARP_STRIPED,
                "input must have striped or warp-striped arrangement");

  if constexpr (ItemSlice::ARRANGEMENT == WARP_STRIPED) {
    enum {
      WARP_THREADS = PlatformT::WARP_THREADS,
      WARP_ITEMS = WARP_THREADS * ITEMS_PER_THREAD,
    };
    static_assert((BLOCK_THREADS % WARP_THREADS) == 0,
                  "BLOCK_THREADS must be a multiple of WARP_THREADS");
    // store items from warp-striped arrangement
    BlockStore</*STRIDE=*/WARP_THREADS, ITEMS_PER_THREAD>(
        items, p.warp_idx() * WARP_ITEMS + p.lane_idx(), out);
    return;
  }

  // store items from striped arrangement
  BlockStore</*STRIDE=*/BLOCK_THREADS, ITEMS_PER_THREAD>(items, p.thread_idx(),
                                                         out);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename ItemSlice, typename OutputSlice>
ATTR void BlockStore(PlatformT p, const ItemSlice items, OutputSlice out,
                     int num_items) {
  using namespace utils;

  // fast-path check
  if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
    BlockStore<BLOCK_THREADS, ITEMS_PER_THREAD>(p, items, out);
    return;
  }

  if constexpr (ItemSlice::ARRANGEMENT == WARP_STRIPED) {
    enum {
      WARP_THREADS = PlatformT::WARP_THREADS,
      WARP_ITEMS = WARP_THREADS * ITEMS_PER_THREAD,
    };
    // store items from warp-striped arrangement
    BlockStore</*STRIDE=*/WARP_THREADS, ITEMS_PER_THREAD>(
        items, p.warp_idx() * WARP_ITEMS + p.lane_idx(), out, num_items);
    return;
  }

  // store items from striped arrangement
  BlockStore</*STRIDE=*/BLOCK_THREADS, ITEMS_PER_THREAD>(items, p.thread_idx(),
                                                         out, num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename ItemSlice, typename SelectionFlagSlice, typename OutputSlice>
ATTR void BlockStoreIf(PlatformT p, const ItemSlice items,
                       const SelectionFlagSlice selection_flags,
                       OutputSlice out) {
  using namespace utils;

  static_assert(ItemSlice::ARRANGEMENT == STRIPED,
                "input must have striped arrangement");
  static_assert(SelectionFlagSlice::ARRANGEMENT == STRIPED,
                "selection flags must have striped arrangement");
  static_assert(OutputSlice::ARRANGEMENT == BLOCKED,
                "output must have blocked arrangement");

  OutputSlice thread_it = out.subslice(p.thread_idx());

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (selection_flags[i]) {
      thread_it[i * BLOCK_THREADS] = items[i];
    }
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename ItemSlice, typename SelectionFlagSlice, typename OutputSlice>
ATTR void BlockStoreIf(PlatformT p, const ItemSlice items,
                       const SelectionFlagSlice selection_flags,
                       OutputSlice out, int num_items) {
  // fast-path check
  if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
    BlockStoreIf<BLOCK_THREADS, ITEMS_PER_THREAD>(p, items, selection_flags,
                                                  out);
    return;
  }

  OutputSlice thread_it = out.subslice(p.thread_idx());

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (p.thread_idx() + (i * BLOCK_THREADS) < num_items) {
      if (selection_flags[i]) {
        thread_it[i * BLOCK_THREADS] = items[i];
      }
    }
  }
}

template <int ITEMS_PER_THREAD, typename ItemSlice, typename OffsetSlice,
          typename OutputSlice>
ATTR void BlockStoreAt(const ItemSlice items, const OffsetSlice offsets,
                       OutputSlice out) {
  using namespace utils;

  static_assert(OutputSlice::ARRANGEMENT == BLOCKED,
                "output must have blocked arrangement");
  static_assert(OffsetSlice::ARRANGEMENT == ItemSlice::ARRANGEMENT,
                "offsets must the same arrangement as items");

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    out[offsets[i]] = items[i];
  }
}

template <int STRIDE, int ITEMS_PER_THREAD, typename ItemSlice,
          typename OffsetSlice, typename OutputSlice>
ATTR void BlockStoreAt(const ItemSlice items, const OffsetSlice offsets,
                       int thread_offset, OutputSlice out, int num_items) {
  using namespace utils;

  static_assert(OutputSlice::ARRANGEMENT == BLOCKED,
                "input must have blocked arrangement");
  static_assert(OffsetSlice::ARRANGEMENT == ItemSlice::ARRANGEMENT,
                "offsets must the same arrangement as items");

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (thread_offset + (i * STRIDE) < num_items) {
      out[offsets[i]] = items[i];
    }
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename ItemSlice, typename OffsetSlice, typename OutputSlice>
ATTR void BlockStoreAt(PlatformT, const ItemSlice items,
                       const OffsetSlice offsets, OutputSlice out) {
  using namespace utils;

  static_assert(ItemSlice::ARRANGEMENT == STRIPED ||
                    ItemSlice::ARRANGEMENT == WARP_STRIPED,
                "input must have striped or warp-striped arrangement");

  // arrangement agnostic store of items
  BlockStoreAt<ITEMS_PER_THREAD>(items, offsets, out);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename ItemSlice, typename OffsetSlice, typename OutputSlice>
ATTR void BlockStoreAt(PlatformT p, const ItemSlice items,
                       const OffsetSlice offsets, OutputSlice out,
                       int num_items) {
  using namespace utils;

  // fast-path check
  if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
    BlockStoreAt<BLOCK_THREADS, ITEMS_PER_THREAD>(p, items, offsets, out);
    return;
  }

  if constexpr (ItemSlice::ARRANGEMENT == WARP_STRIPED) {
    enum {
      WARP_THREADS = PlatformT::WARP_THREADS,
      WARP_ITEMS = WARP_THREADS * ITEMS_PER_THREAD,
    };
    static_assert((BLOCK_THREADS % WARP_THREADS) == 0,
                  "BLOCK_THREADS must be a multiple of WARP_THREADS");
    // store items from warp-striped arrangement
    BlockStoreAt</*STRIDE=*/WARP_THREADS, ITEMS_PER_THREAD>(
        items, offsets, p.warp_idx() * WARP_ITEMS + p.lane_idx(), out,
        num_items);
    return;
  }

  // store items from striped arrangement
  BlockStoreAt</*STRIDE=*/BLOCK_THREADS, ITEMS_PER_THREAD>(
      items, offsets, p.thread_idx(), out, num_items);
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename ItemSlice, typename OffsetSlice, typename SelectionFlagSlice,
          typename OutputSlice>
ATTR void BlockStoreAtIf(PlatformT, const ItemSlice items,
                         const OffsetSlice offsets,
                         const SelectionFlagSlice selection_flags,
                         OutputSlice out) {
  using namespace utils;

  static_assert(ItemSlice::ARRANGEMENT == STRIPED,
                "input must have striped arrangement");
  static_assert(OffsetSlice::ARRANGEMENT == STRIPED,
                "offsets must have striped arrangement");
  static_assert(SelectionFlagSlice::ARRANGEMENT == STRIPED,
                "selection flags must have striped arrangement");
  static_assert(OutputSlice::ARRANGEMENT == BLOCKED,
                "output must have blocked arrangement");

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (selection_flags[i]) {
      out[offsets[i]] = items[i];
    }
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename ItemSlice, typename OffsetSlice, typename SelectionFlagSlice,
          typename OutputSlice>
ATTR void BlockStoreAtIf(PlatformT p, const ItemSlice items,
                         const OffsetSlice offsets,
                         const SelectionFlagSlice selection_flags,
                         OutputSlice out, int num_items) {
  // fast-path check
  if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
    BlockStoreAtIf<BLOCK_THREADS, ITEMS_PER_THREAD>(p, items, offsets,
                                                    selection_flags, out);
    return;
  }

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (p.thread_idx() + (i * BLOCK_THREADS) < num_items) {
      if (selection_flags[i]) {
        out[offsets[i]] = items[i];
      }
    }
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename T, typename OutputSlice>
ATTR void BlockFill(PlatformT p, T value, OutputSlice block_it) {
  using namespace utils;

  static_assert(OutputSlice::ARRANGEMENT == BLOCKED,
                "output must have blocked arrangement");

  OutputSlice thread_it = block_it.subslice(p.thread_idx());

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    thread_it[i * BLOCK_THREADS] = value;
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename T, typename OutputSlice>
ATTR void BlockFill(PlatformT p, T value, OutputSlice block_it, int num_items) {
  // fast-path check
  if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
    BlockFill<BLOCK_THREADS, ITEMS_PER_THREAD>(p, value, block_it);
    return;
  }

  OutputSlice thread_it = block_it.subslice(p.thread_idx());

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (p.thread_idx() + (i * BLOCK_THREADS) < num_items) {
      thread_it[i * BLOCK_THREADS] = value;
    }
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename T, typename OffsetSlice, typename SelectionFlagSlice,
          typename OutputSlice>
ATTR void BlockFillAtIf(PlatformT, T value, const OffsetSlice offsets,
                        const SelectionFlagSlice selection_flags,
                        OutputSlice out) {
  using namespace utils;

  static_assert(OffsetSlice::ARRANGEMENT == STRIPED,
                "offsets must have striped arrangement");
  static_assert(SelectionFlagSlice::ARRANGEMENT == STRIPED,
                "selection flags must have striped arrangement");
  static_assert(OutputSlice::ARRANGEMENT == BLOCKED,
                "output must have blocked arrangement");

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (selection_flags[i]) {
      out[offsets[i]] = value;
    }
  }
}

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, typename PlatformT,
          typename T, typename OffsetSlice, typename SelectionFlagSlice,
          typename OutputSlice>
ATTR void BlockFillAtIf(PlatformT p, T value, const OffsetSlice offsets,
                        const SelectionFlagSlice selection_flags,
                        OutputSlice out, int num_items) {
  // fast-path check
  if (num_items == (BLOCK_THREADS * ITEMS_PER_THREAD)) {
    BlockFillAtIf<BLOCK_THREADS, ITEMS_PER_THREAD>(p, value, offsets,
                                                   selection_flags, out);
    return;
  }

#pragma unroll
  for (int i = 0; i < ITEMS_PER_THREAD; ++i) {
    if (p.thread_idx() + (i * BLOCK_THREADS) < num_items) {
      if (selection_flags[i]) {
        out[offsets[i]] = value;
      }
    }
  }
}

}  // namespace functions
}  // namespace breeze
