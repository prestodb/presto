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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "velox/dwio/common/Range.h"

using namespace ::testing;
using namespace facebook::velox::dwio::common;

namespace facebook::velox::common {

TEST(RangeTests, Add) {
  Ranges ranges;
  ASSERT_THROW(ranges.add(2, 1), exception::LoggedException);
  ASSERT_THROW(ranges.add(2, 2), exception::LoggedException);
  ranges.add(1, 3);
  ASSERT_THAT(ranges.ranges_, ElementsAre(std::tuple<size_t, size_t>{1, 3}));
  ASSERT_EQ(ranges.size(), 2);
  ranges.add(3, 5);
  ASSERT_THAT(ranges.ranges_, ElementsAre(std::tuple<size_t, size_t>{1, 5}));
  ASSERT_EQ(ranges.size(), 4);
  ranges.add(6, 9);
  ASSERT_THAT(
      ranges.ranges_,
      ElementsAre(
          std::tuple<size_t, size_t>{1, 5}, std::tuple<size_t, size_t>{6, 9}));
  ASSERT_EQ(ranges.size(), 7);
  ranges.add(8, 10);
  ASSERT_THAT(
      ranges.ranges_,
      ElementsAre(
          std::tuple<size_t, size_t>{1, 5},
          std::tuple<size_t, size_t>{6, 9},
          std::tuple<size_t, size_t>{8, 10}));
  ranges.clear();
  ASSERT_EQ(ranges.size(), 0);
}

TEST(RangeTests, ForEach) {
  Ranges ranges;
  ranges.add(1, 3);
  ranges.add(6, 9);
  auto total = 0;
  ASSERT_EQ(ranges.size(), 5);
  for (auto& i : ranges) {
    total += i;
  }
  ASSERT_EQ(total, 24);
}

TEST(RangeTests, Filter) {
  auto r = Ranges::of(1, 10);
  auto r2 = r.filter([](auto /* unused */) { return true; });
  ASSERT_EQ(r2.size(), 9);
  ASSERT_THAT(r2.ranges_, ElementsAre(std::tuple<size_t, size_t>{1, 10}));

  auto r3 = r.filter([](auto i) { return i % 3 != 0; });
  ASSERT_EQ(r3.size(), 6);
  ASSERT_THAT(
      r3.ranges_,
      ElementsAre(
          std::tuple<size_t, size_t>{1, 3},
          std::tuple<size_t, size_t>{4, 6},
          std::tuple<size_t, size_t>{7, 9}));

  Ranges r4;
  r4.add(1, 10);
  r4.add(20, 30);
  auto r5 = r4.filter([](auto i) { return i != 3 && i != 8 && i != 25; });
  ASSERT_EQ(r5.size(), 16);
  ASSERT_THAT(
      r5.ranges_,
      ElementsAre(
          std::tuple<size_t, size_t>{1, 3},
          std::tuple<size_t, size_t>{4, 8},
          std::tuple<size_t, size_t>{9, 10},
          std::tuple<size_t, size_t>{20, 25},
          std::tuple<size_t, size_t>{26, 30}));
}

} // namespace facebook::velox::common
