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

#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

#include <stdint.h>

namespace facebook::velox::functions::sparksql::test {
namespace {

class MapTest : public SparkFunctionBaseTest {
 protected:
  template <typename K = int64_t, typename V = std::string>
  void mapSimple(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters,
      bool expectException = false,
      const VectorPtr& expected = nullptr) {
    if (expectException) {
      ASSERT_THROW(
          evaluate<MapVector>(expression, makeRowVector(parameters)),
          std::exception);
    } else {
      auto result = evaluate<MapVector>(expression, makeRowVector(parameters));
      ::facebook::velox::test::assertEqualVectors(result, expected);
    }
  }
};

TEST_F(MapTest, Basics) {
  auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
  auto inputVector2 = makeNullableFlatVector<int64_t>({4, 5, 6});
  auto mapVector =
      makeMapVector<int64_t, int64_t>({{{1, 4}}, {{2, 5}}, {{3, 6}}});
  mapSimple("map(c0, c1)", {inputVector1, inputVector2}, false, mapVector);
}

TEST_F(MapTest, Nulls) {
  auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
  auto inputVector2 =
      makeNullableFlatVector<int64_t>({std::nullopt, 5, std::nullopt});
  auto mapVector = makeMapVector<int64_t, int64_t>(
      {{{1, std::nullopt}}, {{2, 5}}, {{3, std::nullopt}}});
  mapSimple("map(c0, c1)", {inputVector1, inputVector2}, false, mapVector);
}

TEST_F(MapTest, DifferentTypes) {
  auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
  auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
  auto mapVector =
      makeMapVector<int64_t, double>({{{1, 4.0}}, {{2, 5.0}}, {{3, 6.0}}});
  mapSimple("map(c0, c1)", {inputVector1, inputVector2}, false, mapVector);
}

TEST_F(MapTest, Wide) {
  auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
  auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
  auto inputVector11 = makeNullableFlatVector<int64_t>({10, 20, 30});
  auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
  auto mapVector = makeMapVector<int64_t, double>(
      {{{1, 4.0}, {10, 4.1}}, {{2, 5.0}, {20, 5.1}}, {{3, 6.0}, {30, 6.1}}});
  mapSimple(
      "map(c0, c1, c2, c3)",
      {inputVector1, inputVector2, inputVector11, inputVector22},
      false,
      mapVector);
}

TEST_F(MapTest, ErrorCases) {
  // Number of args
  auto inputVector1 = makeNullableFlatVector<int64_t>({1, 2, 3});
  auto inputVector2 = makeNullableFlatVector<double>({4.0, 5.0, 6.0});
  auto mapVector =
      makeMapVector<int64_t, double>({{{1, 4.0}}, {{2, 5.0}}, {{3, 6.0}}});
  mapSimple("map(c0)", {inputVector1}, true);
  mapSimple(
      "map(c0, c1, c2)", {inputVector1, inputVector2, inputVector1}, true);

  // Types of args
  auto inputVector11 = makeNullableFlatVector<double>({10.0, 20.0, 30.0});
  auto inputVector22 = makeNullableFlatVector<double>({4.1, 5.1, 6.1});
  mapSimple(
      "map(c0, c1, c2, c3)",
      {inputVector1, inputVector2, inputVector11, inputVector22},
      true);
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
