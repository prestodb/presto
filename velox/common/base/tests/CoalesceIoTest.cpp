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

#include "velox/common/base/CoalesceIo.h"

#include <gtest/gtest.h>

using namespace facebook::velox;

// Mockup of a cache entry: Offset, size, number of elements in a
// chained buffer.
struct IoUnit {
  explicit IoUnit(
      int64_t _offset,
      int32_t _size,
      int32_t _numBuffers,
      bool _mayCoalesce = true)
      : offset(_offset),
        size(_size),
        numBuffers(_numBuffers),
        mayCoalesce(_mayCoalesce) {}
  const int64_t offset;
  const int32_t size;
  const int32_t numBuffers;
  const bool mayCoalesce;
};

// Represents a data transfer. size is the number of bytes. numBuffers
// is the number of chained buffers to process. If this is 0, Range
// represents a skip of size bytes. This is for illustration only, so
// does not mention the actual buffers.
struct Range {
  Range(int64_t _size, int32_t _numBuffers)
      : size(_size), numBuffers(_numBuffers) {}
  const int64_t size;
  const int32_t numBuffers;
};

TEST(CoalesceIoTest, basic) {
  std::vector<IoUnit> data;

  //
  // The first 3 are in close proximity. But the 4th is apart because
  // bundling this with the 2 first would exceed the max batch size of 4.
  data.emplace_back(1000, 100000, 3);
  data.emplace_back(105000, 100000, 3);
  data.emplace_back(220000, 100000, 3);
  data.emplace_back(330000, 100000, 3);
  data.emplace_back(700000, 100000, 3);
  data.emplace_back(810000, 1000000, 1, false);
  std::vector<int64_t> offsets;
  // Vector where each element has the indices of the IoUnits to
  // process together.
  std::vector<std::vector<int32_t>> ioGroups;

  auto stats = coalesceIo<IoUnit, Range>(
      data,
      20000,
      8,
      [&](int32_t index) { return data[index].offset; },
      [&](int32_t index) { return data[index].size; },
      [&](int32_t index) {
        return data[index].mayCoalesce ? data[index].numBuffers : kNoCoalesce;
      },
      [&](const IoUnit& item, std::vector<Range>& ranges) {
        // Add as many ranges as there are buffers. Make all but the last be 0
        // bytes.
        for (auto i = 0; i < item.numBuffers - 1; ++i) {
          ranges.emplace_back(0, 0);
        }
        ranges.emplace_back(item.size, item.numBuffers);
      },
      [&](int32_t skip, std::vector<Range>& ranges) {
        ranges.emplace_back(skip, 0);
      },
      [&](const std::vector<IoUnit>& items,
          int32_t begin,
          int32_t end,
          uint64_t offset,
          const std::vector<Range>& rages) {
        offsets.push_back(offset);
        ioGroups.emplace_back();
        for (auto i = begin; i < end; ++i) {
          ioGroups.back().push_back(i);
        }
      });

  // We expect 4 IOs. The two first are coalesced because of a gap of
  // under 30000. The third is not coalesced even though it is near
  // because of the ranges per IO limit of 10. The fourth is too far
  // from the third to coalesce.
  EXPECT_EQ(4, stats.numIos);
  EXPECT_EQ(1500000, stats.payloadBytes);
  EXPECT_EQ(14000, stats.extraBytes);

  std::vector<int64_t> expectedOffsets{1000, 220000, 700000, 810000};
  EXPECT_EQ(expectedOffsets, offsets);
  EXPECT_EQ(2, ioGroups[0].size());
  EXPECT_EQ(2, ioGroups[1].size());
  EXPECT_EQ(1, ioGroups[2].size());
  EXPECT_EQ(1, ioGroups[3].size());
}
