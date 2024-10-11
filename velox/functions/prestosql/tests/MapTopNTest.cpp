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

class MapTopNTest : public test::FunctionBaseTest {};

TEST_F(MapTopNTest, basic) {
  auto data = makeRowVector({
      makeMapVectorFromJson<int32_t, int64_t>({
          "{1:3, 2:5, 3:1, 4:4, 5:2}",
          "{1:3, 2:5, 3:null, 4:4, 5:2}",
          "{1:null, 2:null, 3:1, 4:4, 5:null}",
          "{1:10, 2:7, 3:11, 5:4}",
          "{1:10, 2:7, 3:0}",
          "{1:null, 2:10}",
          "{}",
          "{1:null, 2:null, 3:null}",
      }),
  });

  auto result = evaluate("map_top_n(c0, 3)", data);

  auto expected = makeMapVectorFromJson<int32_t, int64_t>({
      "{2:5, 4:4, 1:3}",
      "{2:5, 4:4, 1:3}",
      "{4:4, 3:1, 5:null}",
      "{3:11, 1:10, 2:7}",
      "{1:10, 2:7, 3:0}",
      "{2:10, 1:null}",
      "{}",
      "{1:null, 2:null, 3:null}",
  });

  assertEqualVectors(expected, result);

  // n = 0. Expect empty maps.
  result = evaluate("map_top_n(c0, 0)", data);

  expected = makeMapVectorFromJson<int32_t, int64_t>(
      {"{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}"});

  assertEqualVectors(expected, result);

  // n is negative. Expect an error.
  VELOX_ASSERT_THROW(
      evaluate("map_top_n(c0, -1)", data),
      "n must be greater than or equal to 0");
}

TEST_F(MapTopNTest, nestedNullFailure) {
  auto data = makeMapVector(
      /*offsets=*/{0},
      /*keyVector=*/makeFlatVector<int32_t>({1, 2, 3}),
      /*valueVector=*/
      makeNullableArrayVector<int32_t>({{std::nullopt}, {2}, {5}}));

  // Nested nulls present inhibit the orderbility of values. Expect an error.
  VELOX_ASSERT_THROW(
      evaluate("map_top_n(c0, 1)", makeRowVector({data})), // n < map size
      "Ordering nulls is not supported");
  VELOX_ASSERT_THROW(
      evaluate("map_top_n(c0, 10)", makeRowVector({data})), // n > map size
      "Ordering nulls is not supported");
}

// Test to ensure we use keys to break ties if values are
// equal.
TEST_F(MapTopNTest, equalValues) {
  auto data = makeRowVector({
      makeMapVectorFromJson<int32_t, int64_t>(
          {"{6:3, 2:5, 3:1, 4:4, 5:2, 1:3}",
           "{1:3, 2:5, 3:null, 4:4, 5:2, 6:5 }",
           "{5:null, 2:null, 3:1, 4:4, 1:null}",
           "{1:null, 5:null, 3:null, 4:null, 2:null}"}),
  });

  auto result = evaluate("map_top_n(c0, 3)", data);

  auto expected = makeMapVectorFromJson<int32_t, int64_t>(
      {"{2:5, 4:4, 6:3}",
       "{6:5, 2:5, 4:4}",
       "{4:4, 3:1, 5:null}",
       "{4:null, 3:null, 5:null}"});

  assertEqualVectors(expected, result);

  // Map vector with equal array's as values.
  auto valuesVector = makeArrayVectorFromJson<int64_t>({
      "[1, 2, 3]",
      "[4, 5, 6]",
      "[-1, -2, -3]",
      "[1, 2, 3]",
  });

  auto keysVector = makeFlatVector<int32_t>({1, 2, 3, 4});

  auto mapvector = makeMapVector({0, 4}, keysVector, valuesVector);

  result = evaluate("map_top_n(c0, 3)", makeRowVector({mapvector}));

  auto expectedValues = makeArrayVectorFromJson<int64_t>({
      "[1, 2, 3]",
      "[4, 5, 6]",
      "[1, 2, 3]",
  });

  auto expectedKeys = makeFlatVector<int32_t>({1, 2, 4});

  auto expectedResults = makeMapVector({0, 3}, expectedKeys, expectedValues);

  assertEqualVectors(expectedResults, result);
}

TEST_F(MapTopNTest, timestampWithTimeZone) {
  auto testMapTopN =
      [&](const std::vector<int32_t>& keys,
          const std::vector<std::optional<int64_t>>& values,
          const std::vector<int32_t>& expectedKeys,
          const std::vector<std::optional<int64_t>>& expectedValues) {
        const auto map = makeMapVector(
            {0},
            makeFlatVector(keys),
            makeNullableFlatVector(values, TIMESTAMP_WITH_TIME_ZONE()));
        const auto expectedMap = makeMapVector(
            {0},
            makeFlatVector(expectedKeys),
            makeNullableFlatVector(expectedValues, TIMESTAMP_WITH_TIME_ZONE()));

        const auto result = evaluate("map_top_n(c0, 3)", makeRowVector({map}));

        assertEqualVectors(expectedMap, result);
      };

  testMapTopN(
      {1, 2, 3, 4, 5},
      {pack(3, 1), pack(5, 2), pack(1, 3), pack(4, 4), pack(2, 5)},
      {2, 4, 1},
      {pack(5, 2), pack(4, 4), pack(3, 1)});
  testMapTopN(
      {1, 2, 3, 4, 5},
      {pack(3, 5), pack(5, 4), std::nullopt, pack(4, 2), pack(2, 1)},
      {2, 4, 1},
      {pack(5, 4), pack(4, 2), pack(3, 5)});
  testMapTopN(
      {1, 2, 3, 4, 5},
      {std::nullopt, std::nullopt, pack(1, 1), pack(4, 4), std::nullopt},
      {4, 3, 5},
      {pack(4, 4), pack(1, 1), std::nullopt});
  testMapTopN(
      {1, 2, 3, 5},
      {pack(10, 1), pack(7, 2), pack(11, 3), pack(4, 4)},
      {3, 1, 2},
      {pack(11, 3), pack(10, 1), pack(7, 2)});
  testMapTopN(
      {1, 2, 3},
      {pack(10, 3), pack(7, 2), pack(0, 1)},
      {1, 2, 3},
      {pack(10, 3), pack(7, 2), pack(0, 1)});
  testMapTopN(
      {1, 2}, {std::nullopt, pack(10, 1)}, {2, 1}, {pack(10, 1), std::nullopt});
  testMapTopN({}, {}, {}, {});
  testMapTopN(
      {1, 2, 3},
      {std::nullopt, std::nullopt, std::nullopt},
      {1, 2, 3},
      {std::nullopt, std::nullopt, std::nullopt});
  testMapTopN(
      {6, 2, 3, 4, 5, 1},
      {pack(3, 1), pack(5, 2), pack(1, 3), pack(4, 4), pack(2, 5), pack(3, 1)},
      {2, 4, 6},
      {pack(5, 2), pack(4, 4), pack(3, 1)});
  testMapTopN(
      {1, 2, 3, 4, 5, 6},
      {pack(3, 6),
       pack(5, 5),
       std::nullopt,
       pack(4, 3),
       pack(2, 2),
       pack(5, 5)},
      {6, 2, 4},
      {pack(5, 5), pack(5, 5), pack(4, 3)});
  testMapTopN(
      {5, 2, 3, 4, 1},
      {std::nullopt, std::nullopt, pack(1, 1), pack(4, 2), std::nullopt},
      {4, 3, 5},
      {pack(4, 2), pack(1, 1), std::nullopt});
  testMapTopN(
      {1, 5, 3, 4, 2},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {4, 3, 5},
      {std::nullopt, std::nullopt, std::nullopt});
}

} // namespace
} // namespace facebook::velox::functions
