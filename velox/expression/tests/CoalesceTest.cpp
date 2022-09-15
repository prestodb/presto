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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class CoalesceTest : public functions::test::FunctionBaseTest {};

TEST_F(CoalesceTest, basic) {
  vector_size_t size = 20;

  // 0, null, null, 3, null, null, 6, null, null...
  auto first = makeFlatVector<int32_t>(
      size,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row % 3 != 0; });

  // 0, 10, null, 30, 40, null, 60, 70...
  auto second = makeFlatVector<int32_t>(
      size, [](vector_size_t row) { return row * 10; }, nullEvery(3, 2));

  // 0, 100, 200, 300, 400, 500, 600...
  auto third = makeFlatVector<int32_t>(
      size, [](vector_size_t row) { return row * 100; });

  auto row = makeRowVector({first, second, third});
  auto result = evaluate<FlatVector<int32_t>>("coalesce(c0, c1, c2)", row);
  auto expectedResult = makeFlatVector<int32_t>(
      size, [](auto row) { return row * pow(10, row % 3); });
  assertEqualVectors(expectedResult, result);

  // Verify that input expressions are evaluated only on rows that are still
  // null after evaluating all the preceding inputs and not evaluated at all if
  // there are no nulls remaining.

  // The last expression 'c1 / 0' should not be evaluated.
  result = evaluate<FlatVector<int32_t>>(
      "coalesce(c0, c1, c2, cast(c1 / 0 as integer))", row);
  assertEqualVectors(expectedResult, result);

  // The second expression 'c1 / (c1 % 3)' should not be evaluated on rows where
  // c1 % 3 is zero.
  result = evaluate<FlatVector<int32_t>>(
      "coalesce(c0, cast(c1 / (c1 % 3) as integer), c2)", row);
  assertEqualVectors(expectedResult, result);

  result = evaluate<FlatVector<int32_t>>("coalesce(c2, c1, c0)", row);
  expectedResult =
      makeFlatVector<int32_t>(size, [](auto row) { return row * 100; });
  assertEqualVectors(expectedResult, result);

  result = evaluate<FlatVector<int32_t>>("coalesce(c0, c1)", row);
  expectedResult = makeFlatVector<int32_t>(
      size,
      [](auto row) { return row * pow(10, row % 3); },
      [](auto row) { return row % 3 == 2; });
  assertEqualVectors(expectedResult, result);
}

TEST_F(CoalesceTest, strings) {
  auto input = makeRowVector({
      makeNullableFlatVector<StringView>(
          {"a", std::nullopt, std::nullopt, "d", std::nullopt}),
      makeNullableFlatVector<StringView>(
          {"aa", std::nullopt, std::nullopt, "dd", std::nullopt}),
      makeNullableFlatVector<StringView>(
          {"aaa", "bbb", std::nullopt, "ddd", "eee"}),
  });

  auto expectedResult = makeNullableFlatVector<StringView>(
      {"a", "bbb", std::nullopt, "d", "eee"});

  auto result = evaluate<FlatVector<StringView>>("coalesce(c0, c1, c2)", input);
  assertEqualVectors(expectedResult, result);
}
