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

using namespace facebook::velox::test;

namespace facebook::velox::functions {
namespace {

class MapSubsetTest : public test::FunctionBaseTest {
 public:
  template <typename T>
  void testFloatNaNs() {
    static const auto kNaN = std::numeric_limits<T>::quiet_NaN();
    static const auto kSNaN = std::numeric_limits<T>::signaling_NaN();

    // Case 1: Non-constant search keys.
    auto data = makeRowVector(
        {makeMapVectorFromJson<T, int32_t>({
             "{1:10, NaN:20, 3:null, 4:40, 5:50, 6:60}",
             "{NaN:20}",
         }),
         makeArrayVector<T>({{1, kNaN, 5}, {kSNaN, 3}})});

    auto expected = makeMapVectorFromJson<T, int32_t>({
        "{1:10, NaN:20, 5:50}",
        "{NaN:20}",
    });
    auto result = evaluate("map_subset(c0, c1)", data);
    assertEqualVectors(expected, result);

    // Case 2: Constant search keys.
    data = makeRowVector(
        {makeMapVectorFromJson<T, int32_t>({
             "{1:10, NaN:20, 3:null, 4:40, 5:50, 6:60}",
             "{NaN:20}",
         }),
         BaseVector::wrapInConstant(2, 0, makeArrayVector<T>({{1, kNaN, 5}}))});
    expected = makeMapVectorFromJson<T, int32_t>({
        "{1:10, NaN:20, 5:50}",
        "{NaN:20}",
    });
    result = evaluate("map_subset(c0, c1)", data);
    assertEqualVectors(expected, result);

    // Case 3: Map with Complex type as key.
    // Map: { [{1, NaN,3}: 1, {4, 5}: 2], [{NaN, 3}: 3, {1, 2}: 4] }
    data = makeRowVector({
        makeMapVector(
            {0, 2},
            makeArrayVector<T>({{1, kNaN, 3}, {4, 5}, {kSNaN, 3}, {1, 2}}),
            makeFlatVector<int32_t>({1, 2, 3, 4})),
        makeNestedArrayVectorFromJson<T>({
            "[[1, NaN, 3], [4, 5]]",
            "[[1, 2, 3], [NaN, 3]]",
        }),
    });
    expected = makeMapVector(
        {0, 2},
        makeArrayVectorFromJson<T>({
            "[1, NaN, 3]",
            "[4, 5]",
            "[NaN, 3]",
        }),
        makeFlatVector<int32_t>({1, 2, 3}));

    result = evaluate("map_subset(c0, c1)", data);
    assertEqualVectors(expected, result);
  }
};

TEST_F(MapSubsetTest, bigintKey) {
  auto data = makeRowVector({
      makeMapVectorFromJson<int64_t, int32_t>({
          "{1:10, 2:20, 3:null, 4:40, 5:50, 6:60}",
          "{1:10, 2:20, 4:40, 5:50}",
          "{}",
          "{2:20, 4:40, 6:60}",
      }),
      makeArrayVectorFromJson<int64_t>({
          "[1, 3, 5]",
          "[1, 3, 5, 7]",
          "[3, 5]",
          "[1, 3]",
      }),
  });

  // Constant keys.
  auto result = evaluate("map_subset(c0, array_constructor(1, 3, 5))", data);

  auto expected = makeMapVectorFromJson<int64_t, int32_t>({
      "{1:10, 3:null, 5:50}",
      "{1:10, 5:50}",
      "{}",
      "{}",
  });

  assertEqualVectors(expected, result);

  // Non-constant keys.
  result = evaluate("map_subset(c0, c1)", data);
  assertEqualVectors(expected, result);

  // Empty list of keys. Expect empty maps.
  result = evaluate("map_subset(c0, array_constructor()::bigint[])", data);

  expected = makeMapVectorFromJson<int64_t, int32_t>({"{}", "{}", "{}", "{}"});

  assertEqualVectors(expected, result);
}

TEST_F(MapSubsetTest, varcharKey) {
  auto data = makeRowVector({
      makeMapVectorFromJson<std::string, int32_t>({
          "{\"apple\": 1, \"banana\": 2, \"Cucurbitaceae\": null, \"date\": 4, \"eggplant\": 5, \"fig\": 6}",
          "{\"banana\": 2, \"orange\": 4}",
          "{\"banana\": 2, \"fig\": 4, \"date\": 5}",
      }),
      makeArrayVectorFromJson<std::string>({
          "[\"apple\", \"Cucurbitaceae\", \"fig\"]",
          "[\"apple\", \"Cucurbitaceae\", \"date\", \"eggplant\"]",
          "[\"fig\"]",
      }),
  });

  // Constant keys.
  auto result = evaluate(
      "map_subset(c0, array_constructor('apple', 'some very looooong name', 'fig', 'Cucurbitaceae'))",
      data);

  auto expected = makeMapVectorFromJson<std::string, int32_t>({
      "{\"apple\": 1, \"Cucurbitaceae\": null, \"fig\": 6}",
      "{}",
      "{\"fig\": 4}",
  });

  assertEqualVectors(expected, result);

  // Non-constant keys.
  result = evaluate("map_subset(c0, c1)", data);
  assertEqualVectors(expected, result);

  // Empty list of keys. Expect empty maps.
  result = evaluate("map_subset(c0, array_constructor()::varchar[])", data);

  expected = makeMapVectorFromJson<std::string, int32_t>({"{}", "{}", "{}"});

  assertEqualVectors(expected, result);
}

TEST_F(MapSubsetTest, arrayKey) {
  auto data = makeRowVector({
      makeMapVector(
          {0, 2},
          makeArrayVectorFromJson<int32_t>({
              "[1, 2, 3]",
              "[4, 5]",
              "[]",
              "[1, 2]",
          }),
          makeFlatVector<std::string>(
              {"apple", "orange", "Cucurbitaceae", "date"})),
      makeNestedArrayVectorFromJson<int32_t>({
          "[[1, 2, 3], [4, 5, 6]]",
          "[[1, 2, 3], []]",
      }),
  });

  auto result = evaluate("map_subset(c0, c1)", data);

  auto expected = makeMapVector(
      {0, 1},
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3]",
          "[]",
      }),
      makeFlatVector<std::string>({"apple", "Cucurbitaceae"}));

  assertEqualVectors(expected, result);
}

TEST_F(MapSubsetTest, floatNaNs) {
  testFloatNaNs<float>();
  testFloatNaNs<double>();
}

} // namespace
} // namespace facebook::velox::functions
