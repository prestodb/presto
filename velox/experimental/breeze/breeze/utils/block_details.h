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

namespace breeze {
namespace utils {

template <int BLOCK_THREADS, int ITEMS_PER_THREAD, int TILE_SIZE = 1>
struct BlockDetails {
  enum {
    BLOCK_ITEMS = BLOCK_THREADS * ITEMS_PER_THREAD,
    TILE_ITEMS = BLOCK_ITEMS * TILE_SIZE,
  };
  template <typename PlatformT>
  static ATTR BlockDetails from_num_items(PlatformT p, int num_items) {
    return from_block_idx_and_num_items(p.block_idx(), num_items);
  }
  static ATTR BlockDetails from_block_idx_and_num_items(int block_idx,
                                                        int num_items) {
    static_assert(TILE_SIZE == 1, "TILE_SIZE must be 1");

    int block_offset = block_idx * BLOCK_ITEMS;
    int num_blocks = (num_items + BLOCK_ITEMS - 1) / BLOCK_ITEMS;
    int num_block_items = BLOCK_ITEMS;

    // adjust number of block items if this is the last block
    if (block_idx == num_blocks - 1) {
      num_block_items = num_items - block_offset;
    }

    return BlockDetails{block_offset, num_block_items};
  }
  template <typename PlatformT>
  static ATTR BlockDetails from_tile_offset_and_num_items(PlatformT p,
                                                          int tile_offset,
                                                          int num_items) {
    return from_block_idx_tile_offset_and_num_items(p.block_idx(), tile_offset,
                                                    num_items);
  }
  static ATTR BlockDetails from_block_idx_tile_offset_and_num_items(
      int block_idx, int tile_offset, int num_items) {
    int block_offset = block_idx * TILE_ITEMS + tile_offset * BLOCK_ITEMS;
    int num_tiles = (num_items + TILE_ITEMS - 1) / TILE_ITEMS;
    int num_block_items = BLOCK_ITEMS;

    // adjust number of block items if this is the last tile
    if (block_idx == num_tiles - 1) {
      num_block_items = num_items > block_offset ? num_items - block_offset : 0;
    }

    return BlockDetails{block_offset, num_block_items};
  }

  int offset;
  int num_items;
};

}  // namespace utils
}  // namespace breeze
