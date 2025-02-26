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

class MapKeysByTopNValuesTest : public test::FunctionBaseTest {};

TEST_F(MapKeysByTopNValuesTest, emptyMap) {
  RowVectorPtr input = makeRowVector({
      makeMapVectorFromJson<int32_t, int64_t>({
          "{}",
      }),
  });

  assertEqualVectors(
      evaluate("map_keys_by_top_n_values(c0, 3)", input),
      makeArrayVectorFromJson<int32_t>({
          "[]",
      }));
}

TEST_F(MapKeysByTopNValuesTest, basic) {
  auto data = makeRowVector({
      makeMapVectorFromJson<int32_t, int64_t>({
          "{1:3, 2:5, 3:1, 4:4, 5:2}",
          "{1:3, 2:5, 3:null, 4:4, 5:2}",
          "{1:null, 2:null, 3:1, 4:4, 5:null}",
          "{1:10, 2:7, 3:11, 5:4}",
          "{1:10, 2:10, 3:10, 4:10, 5:10}",
          "{1:10, 2:7, 3:0}",
          "{1:null, 2:10}",
          "{}",
          "{1:null, 2:null, 3:null}",
      }),
  });

  auto result = evaluate("map_keys_by_top_n_values(c0, 3)", data);

  auto expected = makeArrayVectorFromJson<int32_t>({
      "[2, 4, 1]",
      "[2, 4, 1]",
      "[4, 3, 5]",
      "[3, 1, 2]",
      "[5, 4, 3]",
      "[1, 2, 3]",
      "[2, 1]",
      "[]",
      "[3, 2, 1]",
  });

  assertEqualVectors(expected, result);

  // n = 0. Expect empty maps.
  result = evaluate("map_keys_by_top_n_values(c0, 0)", data);

  expected = makeArrayVectorFromJson<int32_t>({
      "[]",
      "[]",
      "[]",
      "[]",
      "[]",
      "[]",
      "[]",
      "[]",
      "[]",
  });

  assertEqualVectors(expected, result);

  // n is negative. Expect an error.
  VELOX_ASSERT_THROW(
      evaluate("map_keys_by_top_n_values(c0, -1)", data),
      "n must be greater than or equal to 0");
}

TEST_F(MapKeysByTopNValuesTest, complexKeys) {
  RowVectorPtr input =
      makeRowVector({makeMapVectorFromJson<std::string, int64_t>(
          {R"( {"x":1, "y":2} )",
           R"( {"x":1, "x2":-2} )",
           R"( {"ac":1, "cc":3, "dd": 4} )"})});

  assertEqualVectors(
      evaluate("map_keys_by_top_n_values(c0, 1)", input),
      makeArrayVectorFromJson<std::string>({
          "[\"y\"]",
          "[\"x\"]",
          "[\"dd\"]",
      }));
}

TEST_F(MapKeysByTopNValuesTest, tryFuncWithInputHasNullInArray) {
  auto arrayVector = makeNullableFlatVector<std::int32_t>({1, 2, 3});
  auto flatVector = makeNullableArrayVector<bool>(
      {{true, std::nullopt}, {true, std::nullopt}, {false, std::nullopt}});
  auto mapvector = makeMapVector(
      /*offsets=*/{0},
      /*keyVector=*/arrayVector,
      /*valueVector=*/flatVector);
  auto rst = evaluate(
      "try(map_keys_by_top_n_values(c0, 6455219767830808341))",
      makeRowVector({mapvector}));
  assert(rst->size() == 1);
  assert(rst->isNullAt(0));

  arrayVector = makeNullableFlatVector<std::int32_t>({1, 2, 3, -1});
  flatVector = makeNullableArrayVector<std::int32_t>(
      {{1}, {1, 2}, {1, 3, 4}, {4, std::nullopt}});
  mapvector = makeMapVector(
      /*offsets=*/{0},
      /*keyVector=*/arrayVector,
      /*valueVector=*/flatVector);
  auto result = evaluate(
      "try(map_keys_by_top_n_values(c0, 6455219767830808341))",
      makeRowVector({mapvector}));
  auto expected = makeArrayVectorFromJson<int32_t>({"[-1, 3, 2, 1]"});
  assertEqualVectors(expected, result);
}

TEST_F(MapKeysByTopNValuesTest, complexKeysWithLargeK) {
  RowVectorPtr input =
      makeRowVector({makeMapVectorFromJson<std::string, int64_t>(
          {R"( {"x":1, "y":2} )",
           R"( {"x":1, "x2":-2} )",
           R"( {"ac":1, "cc":3, "dd": 4} )"})});

  assertEqualVectors(
      evaluate("map_keys_by_top_n_values(c0, 706100916841560005)", input),
      makeArrayVectorFromJson<std::string>({
          "[\"y\", \"x\"]",
          "[\"x\", \"x2\"]",
          "[\"dd\", \"cc\", \"ac\"]",
      }));
}

TEST_F(MapKeysByTopNValuesTest, timestampWithTimeZone) {
  auto testMapTopNKeys = [&](const std::vector<int64_t>& keys,
                             const std::vector<int32_t>& values,
                             const std::vector<int64_t>& expectedKeys) {
    const auto map = makeMapVector(
        {0},
        makeFlatVector(keys, TIMESTAMP_WITH_TIME_ZONE()),
        makeFlatVector(values));
    const auto expected = makeArrayVector(
        {0}, makeFlatVector(expectedKeys, TIMESTAMP_WITH_TIME_ZONE()));

    const auto result =
        evaluate("map_keys_by_top_n_values(c0, 3)", makeRowVector({map}));

    assertEqualVectors(expected, result);
  };

  testMapTopNKeys(
      {pack(1, 1), pack(2, 2), pack(3, 3), pack(4, 4), pack(5, 5)},
      {3, 5, 1, 4, 2},
      {pack(2, 2), pack(4, 4), pack(1, 1)});
  testMapTopNKeys(
      {pack(5, 1), pack(4, 2), pack(3, 3), pack(2, 4), pack(1, 5)},
      {3, 5, 1, 4, 2},
      {pack(4, 2), pack(2, 4), pack(5, 1)});
  testMapTopNKeys(
      {pack(3, 1), pack(5, 2), pack(1, 3), pack(4, 4), pack(2, 5)},
      {1, 2, 3, 4, 5},
      {pack(2, 5), pack(4, 4), pack(1, 3)});
  testMapTopNKeys(
      {pack(3, 5), pack(5, 4), pack(4, 2), pack(2, 1)},
      {3, 3, 3, 3},
      {pack(5, 4), pack(4, 2), pack(3, 5)});
  testMapTopNKeys({}, {}, {});
}
} // namespace
} // namespace facebook::velox::functions
