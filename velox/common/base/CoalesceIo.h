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

#pragma once

#include <cstdint>
#include <vector>
namespace facebook::velox {
// Utility for combining IOs to nearby location into fewer coalesced
// IOs. This may increase data transfer but generally reduces
// latency and may reduce throttling.

// Describes the outcome of coalescedIo().
struct CoalesceIoStats {
  // Number of distinct IOs.
  int32_t numIos{0};

  // Number of bytes read into pins.
  int64_t payloadBytes{0};
  // Number of bytes read and discarded due to coalescing.
  int64_t extraBytes{0};
};

static constexpr int32_t kNoCoalesce = -1;

// Generic template for grouping IOs into batches of <
// rangesPerIo ranges separated by gaps of size >= maxGap. Element
// represents the object of the IO, Range is the type representing the
// IO, e.g. pointer + size, offsetFunc and SizeFunc return the offset
// and size of an Element.  numRanges returns the number of ranges to
// process for an element. It may return the special value kNoCoalesce
// if the entry should not be coalesced.  AddRange adds the ranges
// that correspond to an Element, skipRange adds a gap between
// neighboring items, ioFunc takes the items, the first item to
// process, the first item not to process, the offset of the first
// item and a vector of Ranges.
template <
    typename Item,
    typename Range,
    typename ItemOffset,
    typename ItemSize,
    typename ItemNumRanges,
    typename AddRanges,
    typename SkipRange,
    typename IoFunc>
CoalesceIoStats coalesceIo(
    const std::vector<Item>& items,
    int32_t maxGap,
    int32_t rangesPerIo,
    ItemOffset offsetFunc,
    ItemSize sizeFunc,
    ItemNumRanges numRanges,
    AddRanges addRanges,
    SkipRange skipRange,
    IoFunc ioFunc) {
  std::vector<Range> buffers;
  auto start = offsetFunc(0);
  auto lastOffset = start;
  std::vector<Range> ranges;
  CoalesceIoStats result;
  int32_t firstItem = 0;
  for (int32_t i = 0; i < items.size(); ++i) {
    auto& item = items[i];
    auto startOffset = offsetFunc(i);
    auto size = sizeFunc(i);
    result.payloadBytes += size;
    int32_t rangesForItem = numRanges(i);
    bool enoughRanges = (rangesForItem == kNoCoalesce ||
                         ranges.size() + rangesForItem >= rangesPerIo) &&
        !ranges.empty();
    if (lastOffset != startOffset || enoughRanges) {
      int64_t gap = startOffset - lastOffset;
      if (gap > 0 && gap < maxGap && !enoughRanges) {
        // The next one is after the previous and no farther than maxGap bytes,
        // we read the gap but drop the bytes.
        result.extraBytes += gap;
        skipRange(gap, ranges);
      } else {
        ioFunc(items, firstItem, i, start, ranges);
        ranges.clear();
        firstItem = i;
        ++result.numIos;
        start = startOffset;
      }
    }
    addRanges(item, ranges);
    lastOffset = startOffset + size;
  }
  ioFunc(items, firstItem, items.size(), start, ranges);
  ++result.numIos;
  return result;
}

} // namespace facebook::velox
