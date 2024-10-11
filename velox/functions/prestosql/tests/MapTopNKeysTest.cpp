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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions {
namespace {

class MapTopNKeysTest : public test::FunctionBaseTest {};

TEST_F(MapTopNKeysTest, emptyMap) {
  RowVectorPtr input = makeRowVector({
      makeMapVectorFromJson<int32_t, int64_t>({
          "{}",
      }),
  });

  assertEqualVectors(
      evaluate("map_top_n_keys(c0, 3)", input),
      makeArrayVectorFromJson<int32_t>({
          "[]",
      }));
}

TEST_F(MapTopNKeysTest, multipleMaps) {
  RowVectorPtr input = makeRowVector({
      makeMapVectorFromJson<int32_t, int64_t>({
          "{3:1, 2:1, 5:1, 4:1, 1:1}",
          "{3:1, 2:1, 1:1}",
          "{2:1, 1:1}",
      }),
  });

  assertEqualVectors(
      evaluate("map_top_n_keys(c0, 3)", input),
      makeArrayVectorFromJson<int32_t>({
          "[5, 4, 3]",
          "[3, 2, 1]",
          "[2, 1]",
      }));
}

TEST_F(MapTopNKeysTest, nIsZero) {
  RowVectorPtr input = makeRowVector({
      makeMapVectorFromJson<int32_t, int64_t>({
          "{2:1, 1:1}",
      }),
  });

  assertEqualVectors(
      evaluate("map_top_n_keys(c0, 0)", input),
      makeArrayVectorFromJson<int32_t>({"[]"}));
}

TEST_F(MapTopNKeysTest, nIsNegative) {
  RowVectorPtr input = makeRowVector({
      makeMapVectorFromJson<int32_t, int64_t>({
          "{2:1, 1:1}",
      }),
  });

  VELOX_ASSERT_THROW(
      evaluate("map_top_n_keys(c0, -1)", input),
      "n must be greater than or equal to 0");
}

TEST_F(MapTopNKeysTest, timestampWithTimeZone) {
  auto testMapTopNKeys = [&](const std::vector<int64_t>& keys,
                             const std::vector<int32_t>& values,
                             const std::vector<int64_t>& expectedKeys) {
    const auto map = makeMapVector(
        {0},
        makeFlatVector(keys, TIMESTAMP_WITH_TIME_ZONE()),
        makeFlatVector(values));
    const auto expected = makeArrayVector(
        {0}, makeFlatVector(expectedKeys, TIMESTAMP_WITH_TIME_ZONE()));

    const auto result = evaluate("map_top_n_keys(c0, 3)", makeRowVector({map}));

    assertEqualVectors(expected, result);
  };

  testMapTopNKeys(
      {pack(1, 1), pack(2, 2), pack(3, 3), pack(4, 4), pack(5, 5)},
      {3, 5, 1, 4, 2},
      {pack(5, 5), pack(4, 4), pack(3, 3)});
  testMapTopNKeys(
      {pack(5, 1), pack(4, 2), pack(3, 3), pack(2, 4), pack(1, 5)},
      {3, 5, 1, 4, 2},
      {pack(5, 1), pack(4, 2), pack(3, 3)});
  testMapTopNKeys(
      {pack(3, 1), pack(5, 2), pack(1, 3), pack(4, 4), pack(2, 5)},
      {1, 2, 3, 4, 5},
      {pack(5, 2), pack(4, 4), pack(3, 1)});
  testMapTopNKeys(
      {pack(3, 5), pack(5, 4), pack(4, 2), pack(2, 1)},
      {1, 2, 4, 5},
      {pack(5, 4), pack(4, 2), pack(3, 5)});
  testMapTopNKeys(
      {pack(10, 3), pack(7, 2), pack(0, 1)},
      {1, 2, 3},
      {pack(10, 3), pack(7, 2), pack(0, 1)});
  testMapTopNKeys(
      {pack(1, 10), pack(10, 1)}, {1, 2}, {pack(10, 1), pack(1, 10)});
  testMapTopNKeys({}, {}, {});
}

} // namespace
} // namespace facebook::velox::functions
