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
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class ZipWithTest : public FunctionBaseTest {};

TEST_F(ZipWithTest, basic) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {6, 7, 8, 9},
          {},
          {},
      }),
      makeArrayVector<int64_t>({
          {10, 20, 30},
          {40, 50, 60, 70},
          {60, 70},
          {100, 110, 120, 130, 140},
          {},
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });

  // No capture.
  auto result = evaluate("zip_with(c0, c1, (x, y) -> x + y)", data);
  auto expected = makeNullableArrayVector<int64_t>(
      {{11, 22, 33},
       {44, 55, std::nullopt, std::nullopt},
       {66, 77, std::nullopt, std::nullopt},
       {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
       {}});

  assertEqualVectors(expected, result);

  // With capture.
  result = evaluate("zip_with(c0, c1, (x, y) -> (x + y) * c2)", data);
  expected = makeNullableArrayVector<int64_t>(
      {{11, 22, 33},
       {44 * 2, 55 * 2, std::nullopt, std::nullopt},
       {66 * 3, 77 * 3, std::nullopt, std::nullopt},
       {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
       {}});

  assertEqualVectors(expected, result);

  // Lambda expression with non-default null behavior.
  result = evaluate("zip_with(c0, c1, (x, y) -> coalesce(x, y))", data);
  expected = makeArrayVector<int64_t>({
      {1, 2, 3},
      {4, 5, 60, 70},
      {6, 7, 8, 9},
      {100, 110, 120, 130, 140},
      {},
  });

  assertEqualVectors(expected, result);

  result = evaluate("zip_with(c0, c1, (x, y) -> coalesce(y, x))", data);
  expected = makeArrayVector<int64_t>({
      {10, 20, 30},
      {40, 50, 60, 70},
      {60, 70, 8, 9},
      {100, 110, 120, 130, 140},
      {},
  });

  assertEqualVectors(expected, result);
}

TEST_F(ZipWithTest, nulls) {
  auto data = makeRowVector({
      makeNullableArrayVector<int64_t>({
          {{1, 2, 3}},
          std::nullopt,
          {{4, 5}},
          {{6, 7, 8, 9}},
          {{-1, -2}},
          std::nullopt,
      }),
      makeNullableArrayVector<int64_t>({
          {{10, 20, 30}},
          {{-1, -2, -3}},
          {{40, 50, 60, 70}},
          {{60, 70}},
          std::nullopt,
          std::nullopt,
      }),
  });

  auto result = evaluate("zip_with(c0, c1, (x, y) -> x + y)", data);
  auto expected = makeNullableArrayVector<int64_t>({
      {{11, 22, 33}},
      std::nullopt,
      {{44, 55, std::nullopt, std::nullopt}},
      {{66, 77, std::nullopt, std::nullopt}},
      std::nullopt,
      std::nullopt,
  });

  assertEqualVectors(expected, result);
}

TEST_F(ZipWithTest, sameSize) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5, 6, 7},
          {8, 9},
          {10, 11, 12, 13, 14},
          {},
      }),
      makeArrayVector<int64_t>({
          {10, 20, 30},
          {40, 50, 60, 70},
          {80, 90},
          {100, 110, 120, 130, 140},
          {},
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });

  // No capture.
  auto result = evaluate("zip_with(c0, c1, (x, y) -> x + y)", data);
  auto expected = makeNullableArrayVector<int64_t>(
      {{11, 22, 33},
       {44, 55, 66, 77},
       {88, 99},
       {110, 121, 132, 143, 154},
       {}});

  assertEqualVectors(expected, result);

  // With capture.
  result = evaluate("zip_with(c0, c1, (x, y) -> (x + y) * c2)", data);
  expected = makeNullableArrayVector<int64_t>(
      {{11, 22, 33},
       {44 * 2, 55 * 2, 66 * 2, 77 * 2},
       {88 * 3, 99 * 3},
       {110 * 4, 121 * 4, 132 * 4, 143 * 4, 154 * 4},
       {}});

  assertEqualVectors(expected, result);
}

TEST_F(ZipWithTest, sameSizeWithNulls) {
  auto data = makeRowVector({
      makeNullableArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5, std::nullopt, 7},
          {8, 9},
          {10, std::nullopt, 12, 13, 14},
          {},
      }),
      makeNullableArrayVector<int64_t>({
          {std::nullopt, 20, 30},
          {40, 50, 60, 70},
          {80, 90},
          {100, 110, 120, std::nullopt, 140},
          {},
      }),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
  });

  // No capture.
  auto result = evaluate("zip_with(c0, c1, (x, y) -> x + y)", data);
  auto expected = makeNullableArrayVector<int64_t>(
      {{std::nullopt, 22, 33},
       {44, 55, std::nullopt, 77},
       {88, 99},
       {110, std::nullopt, 132, std::nullopt, 154},
       {}});

  assertEqualVectors(expected, result);

  // With capture.
  result = evaluate("zip_with(c0, c1, (x, y) -> (x + y) * c2)", data);
  expected = makeNullableArrayVector<int64_t>(
      {{std::nullopt, 22, 33},
       {44 * 2, 55 * 2, std::nullopt, 77 * 2},
       {88 * 3, 99 * 3},
       {110 * 4, std::nullopt, 132 * 4, std::nullopt, 154 * 4},
       {}});

  assertEqualVectors(expected, result);
}

TEST_F(ZipWithTest, encodings) {
  auto baseLeft = makeArrayVector<int64_t>({
      {1, 2, 3},
      {4, 5},
      {6, 7, 8, 9},
      {},
      {},
  });
  auto baseRight = makeArrayVector<int64_t>({
      {10, 20, 30},
      {40, 50, 60, 70},
      {60, 70},
      {100, 110, 120, 130, 140},
      {},
  });
  // Dict + Flat.
  auto indices = makeIndicesInReverse(baseLeft->size());
  auto data = makeRowVector({
      wrapInDictionary(indices, baseLeft),
      baseRight,
  });

  auto result = evaluate("zip_with(c0, c1, (x, y) -> x + y)", data);
  auto expected = makeNullableArrayVector<int64_t>({
      {std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {66, 77, std::nullopt, std::nullopt},
      {104, 115, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt},
  });

  assertEqualVectors(expected, result);

  // Flat + Dict.
  data = makeRowVector({
      baseLeft,
      wrapInDictionary(indices, baseRight),
  });

  result = evaluate("zip_with(c0, c1, (x, y) -> x + y)", data);
  expected = makeNullableArrayVector<int64_t>({
      {std::nullopt, std::nullopt, std::nullopt},
      {104, 115, std::nullopt, std::nullopt, std::nullopt},
      {66, 77, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt},
  });

  assertEqualVectors(expected, result);

  // Dict + Dict.
  data = makeRowVector({
      wrapInDictionary(makeIndices({0, 2, 4}), baseLeft),
      wrapInDictionary(makeIndices({1, 2, 3}), baseRight),
  });

  result = evaluate("zip_with(c0, c1, (x, y) -> x + y)", data);
  expected = makeNullableArrayVector<int64_t>({
      {41, 52, 63, std::nullopt},
      {66, 77, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
  });

  assertEqualVectors(expected, result);
}

TEST_F(ZipWithTest, conditional) {
  auto data = makeRowVector({
      makeArrayVector<int64_t>({
          {1, 2, 3},
          {4, 5},
          {6, 7, 8, 9},
          {},
          {},
      }),
      makeArrayVector<int64_t>({
          {10, 20, 30, 31, 32},
          {40, 50, 60, 70},
          {60, 70},
          {100, 110, 120, 130, 140},
          {},
      }),
      makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
  });

  auto result = evaluate(
      "zip_with(c0, c1, if(c2 % 2 = 0, (x, y) -> x + y, (x, y) -> x - y))",
      data);
  auto expectedResult = makeNullableArrayVector<int64_t>({
      {11, 22, 33, std::nullopt, std::nullopt},
      {4 - 40, 5 - 50, std::nullopt, std::nullopt},
      {66, 77, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {},
  });
  assertEqualVectors(expectedResult, result);

  result = evaluate(
      "zip_with(c0, c1, if(c2 % 2 = 0, (x, y) -> row_constructor(x, y).c1, (x, y) -> row_constructor(y, x).c1))",
      data);
  expectedResult = makeNullableArrayVector<int64_t>({
      {1, 2, 3, std::nullopt, std::nullopt},
      {40, 50, 60, 70},
      {6, 7, 8, 9},
      {100, 110, 120, 130, 140},
      {},
  });
  assertEqualVectors(expectedResult, result);
}

TEST_F(ZipWithTest, fuzzSameSizeNoNulls) {
  VectorFuzzer::Options options;
  options.vectorSize = 1024;

  auto rowType =
      ROW({"c0", "c1", "c2"}, {ARRAY(BIGINT()), ARRAY(INTEGER()), SMALLINT()});

  VectorFuzzer fuzzer(options, pool());
  for (auto i = 0; i < 10; ++i) {
    auto data = fuzzer.fuzzInputRow(rowType);
    auto flatData = flatten<RowVector>(data);

    auto result = evaluate("zip_with(c0, c1, (x, y) -> x + y)", data);
    auto expectedResult =
        evaluate("zip_with(c0, c1, (x, y) -> x + y)", flatData);
    assertEqualVectors(expectedResult, result);

    result = evaluate("zip_with(c0, c1, (x, y) -> (x + y) * c2)", data);
    expectedResult =
        evaluate("zip_with(c0, c1, (x, y) -> (x + y) * c2)", flatData);
    assertEqualVectors(expectedResult, result);
  }
}

TEST_F(ZipWithTest, fuzzVariableLengthWithNulls) {
  VectorFuzzer::Options options;
  options.vectorSize = 1024;
  options.containerVariableLength = true;
  options.nullRatio = 0.1;

  auto rowType =
      ROW({"c0", "c1", "c2"}, {ARRAY(BIGINT()), ARRAY(INTEGER()), SMALLINT()});

  VectorFuzzer fuzzer(options, pool());
  for (auto i = 0; i < 10; ++i) {
    auto data = fuzzer.fuzzInputRow(rowType);
    auto flatData = flatten<RowVector>(data);

    auto result = evaluate("zip_with(c0, c1, (x, y) -> x + y)", data);
    auto expectedResult =
        evaluate("zip_with(c0, c1, (x, y) -> x + y)", flatData);
    assertEqualVectors(expectedResult, result);

    result = evaluate("zip_with(c0, c1, (x, y) -> (x + y) * c2)", data);
    expectedResult =
        evaluate("zip_with(c0, c1, (x, y) -> (x + y) * c2)", flatData);
    assertEqualVectors(expectedResult, result);
  }
}
} // namespace
