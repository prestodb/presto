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
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

#include <fmt/format.h>
#include <cstdint>

using namespace facebook::velox;
using namespace facebook::velox::test;
using facebook::velox::functions::test::FunctionBaseTest;

namespace {

class ArrayTopNTest : public FunctionBaseTest {};

TEST_F(ArrayTopNTest, happyPath) {
  auto input = makeArrayVectorFromJson<int32_t>({
      "[1, 2, 3]",
      "[4, 5, 6]",
      "[7, 8, 9]",
  });

  auto expected_result =
      makeArrayVectorFromJson<int32_t>({"[3]", "[6]", "[9]"});
  auto result = evaluate("array_top_n(c0,INTEGER '1')", makeRowVector({input}));
  assertEqualVectors(expected_result, result);

  expected_result =
      makeArrayVectorFromJson<int32_t>({"[3, 2]", "[6, 5]", "[9, 8]"});
  result = evaluate("array_top_n(c0, INTEGER '2')", makeRowVector({input}));
  assertEqualVectors(expected_result, result);

  expected_result =
      makeArrayVectorFromJson<int32_t>({"[3, 2, 1]", "[6, 5, 4]", "[9, 8, 7]"});
  result = evaluate("array_top_n(c0, INTEGER '3')", makeRowVector({input}));
  assertEqualVectors(expected_result, result);

  result = evaluate("array_top_n(c0, INTEGER '5')", makeRowVector({input}));
  assertEqualVectors(expected_result, result);
}

TEST_F(ArrayTopNTest, nullHandler) {
  // Test fully null array vector.
  auto input = makeNullableArrayVector<int32_t>({
      {std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt},
  });
  auto expected =
      makeArrayVectorFromJson<int32_t>({"[null, null]", "[null, null]"});
  auto result =
      evaluate("array_top_n(c0, INTEGER '2')", makeRowVector({input}));
  assertEqualVectors(expected, result);

  expected =
      makeArrayVectorFromJson<int32_t>({"[null, null]", "[null, null, null]"});
  result = evaluate("array_top_n(c0, INTEGER '3')", makeRowVector({input}));
  assertEqualVectors(expected, result);

  // Test null array vector with various different top n values.
  input = makeArrayVectorFromJson<int32_t>({
      "[1, null, 2, null, 3]",
      "[4, 5, null, 6, null]",
      "[null, 7, null, 8, 9]",
  });

  expected = makeArrayVectorFromJson<int32_t>({"[3]", "[6]", "[9]"});
  result = evaluate("array_top_n(c0, INTEGER '1')", makeRowVector({input}));
  assertEqualVectors(expected, result);

  expected = makeArrayVectorFromJson<int32_t>({"[3, 2]", "[6, 5]", "[9, 8]"});
  result = evaluate("array_top_n(c0, INTEGER '2')", makeRowVector({input}));
  assertEqualVectors(expected, result);

  expected =
      makeArrayVectorFromJson<int32_t>({"[3, 2, 1]", "[6, 5, 4]", "[9, 8, 7]"});
  result = evaluate("array_top_n(c0, INTEGER '3')", makeRowVector({input}));
  assertEqualVectors(expected, result);

  expected = makeArrayVectorFromJson<int32_t>(
      {"[3, 2, 1, null]", "[6, 5, 4, null]", "[9, 8, 7, null]"});
  result = evaluate("array_top_n(c0, INTEGER '4')", makeRowVector({input}));
  assertEqualVectors(expected, result);
  assertEqualVectors(expected, result);

  expected = makeArrayVectorFromJson<int32_t>(
      {"[3, 2, 1, null, null]",
       "[6, 5, 4, null, null]",
       "[9, 8, 7, null, null]"});
  result = evaluate("array_top_n(c0, INTEGER '7')", makeRowVector({input}));
  assertEqualVectors(expected, result);

  // Test nullable aray vector of bigints.
  input = makeNullableArrayVector<int64_t>(
      {{1, 2, std::nullopt},
       {4, 5, std::nullopt, std::nullopt},
       {7, std::nullopt, std::nullopt, std::nullopt}});

  expected = makeArrayVectorFromJson<int64_t>(
      {"[2, 1, null]", "[5, 4, null]", "[7, null, null]"});
  result = evaluate("array_top_n(c0,INTEGER '3')", makeRowVector({input}));
  assertEqualVectors(expected, result);

  // Test nullable aray vector of strings.
  input = makeNullableArrayVector<std::string>({
      {"abc123", "abc", std::nullopt, "abcd"},
      {std::nullopt, "x", "xyz123", "xyzzzz"},
  });

  result = evaluate("array_top_n(c0,INTEGER '3')", makeRowVector({input}));
  assertEqualVectors(
      makeArrayVectorFromJson<std::string>(
          {"[\"abcd\", \"abc123\", \"abc\"]",
           "[\"xyzzzz\", \"xyz123\", \"x\"]"}),
      result);
  result = evaluate("array_top_n(c0,INTEGER '4')", makeRowVector({input}));
  assertEqualVectors(
      makeArrayVectorFromJson<std::string>(
          {"[\"abcd\", \"abc123\", \"abc\", null]",
           "[\"xyzzzz\", \"xyz123\", \"x\", null]"}),
      result);
}

TEST_F(ArrayTopNTest, constant) {
  // Test constant array vector and verify per row.
  vector_size_t size = 1'000;
  auto data = makeArrayVector<int64_t>({{1, 2, 3}, {4, 5, 4, 5}, {7, 7, 7, 7}});

  auto evaluateConstant = [&](vector_size_t row, const VectorPtr& vector) {
    return evaluate(
        "array_top_n(c0, INTEGER '2')",
        makeRowVector({BaseVector::wrapInConstant(size, row, vector)}));
  };

  auto result = evaluateConstant(0, data);
  auto expected = makeConstantArray<int64_t>(size, {3, 2});
  assertEqualVectors(expected, result);

  result = evaluateConstant(1, data);
  expected = makeConstantArray<int64_t>(size, {5, 5});
  assertEqualVectors(expected, result);

  result = evaluateConstant(2, data);
  expected = makeConstantArray<int64_t>(size, {7, 7});
  assertEqualVectors(expected, result);

  data = makeArrayVector<int64_t>(
      {{1, 2, 3, 0, 1, 2, 2}, {4, 5, 4, 5, 5, 4}, {6, 6, 6, 6, 7, 8, 9, 10}});

  auto evaluateMore = [&](vector_size_t row, const VectorPtr& vector) {
    return evaluate(
        "array_top_n(c0, INTEGER '3')",
        makeRowVector({BaseVector::wrapInConstant(size, row, vector)}));
  };

  result = evaluateMore(0, data);
  expected = makeConstantArray<int64_t>(size, {3, 2, 2});
  assertEqualVectors(expected, result);

  result = evaluateMore(1, data);
  expected = makeConstantArray<int64_t>(size, {5, 5, 5});
  assertEqualVectors(expected, result);

  result = evaluateMore(2, data);
  expected = makeConstantArray<int64_t>(size, {10, 9, 8});
  assertEqualVectors(expected, result);
}

TEST_F(ArrayTopNTest, inlineStringArrays) {
  // Test inline (short) strings.
  using S = StringView;

  auto input = makeNullableArrayVector<StringView>({
      {},
      {S("")},
      {std::nullopt},
      {S("a"), S("b")},
      {S("a"), std::nullopt, S("b")},
      {S("a"), S("a")},
      {S("b"), S("a"), S("b"), S("a"), S("a")},
      {std::nullopt, std::nullopt},
      {S("b"), std::nullopt, S("a"), S("a"), std::nullopt, S("b")},
  });

  auto expected = makeNullableArrayVector<StringView>({
      {},
      {S("")},
      {std::nullopt},
      {S("b"), S("a")},
      {S("b"), S("a")},
      {S("a"), S("a")},
      {S("b"), S("b")},
      {std::nullopt, std::nullopt},
      {S("b"), S("b")},
  });

  auto result = evaluate<ArrayVector>(
      "array_top_n(C0, INTEGER '2')", makeRowVector({input}));
  assertEqualVectors(expected, result);
}

TEST_F(ArrayTopNTest, stringArrays) {
  // Test non-inline (> 12 character length) strings.
  using S = StringView;

  auto input = makeNullableArrayVector<StringView>({
      {S("red shiny car ahead"), S("blue clear sky above")},
      {S("blue clear sky above"),
       S("yellow rose flowers"),
       std::nullopt,
       S("blue clear sky above"),
       S("orange beautiful sunset")},
      {
          S("red shiny car ahead"),
          std::nullopt,
          S("purple is an elegant color"),
          S("red shiny car ahead"),
          S("green plants make us happy"),
          S("purple is an elegant color"),
          std::nullopt,
          S("purple is an elegant color"),
      },
  });

  auto expected = makeNullableArrayVector<StringView>({
      {S("red shiny car ahead"), S("blue clear sky above")},
      {S("yellow rose flowers"),
       S("orange beautiful sunset"),
       S("blue clear sky above")},
      {S("red shiny car ahead"),
       S("red shiny car ahead"),
       S("purple is an elegant color")},
  });

  auto result = evaluate<ArrayVector>(
      "array_top_n(C0, INTEGER '3')", makeRowVector({input}));
  assertEqualVectors(expected, result);
}

TEST_F(ArrayTopNTest, nonContiguousRows) {
  auto c0 = makeFlatVector<int64_t>(4, [](auto row) { return row; });
  auto c1 = makeArrayVector<int64_t>({
      {1, 1, 2, 3, 3},
      {1, 1, 2, 3, 4, 4},
      {1, 1, 2, 3, 4, 5, 5},
      {1, 1, 2, 3, 3, 4, 5, 6, 6},
  });

  auto c2 = makeArrayVector<int64_t>({
      {0, 0, 1, 1, 2, 3, 3},
      {0, 0, 1, 1, 2, 3, 4, 4},
      {0, 0, 1, 1, 2, 3, 4, 5, 5},
      {0, 0, 1, 1, 2, 3, 4, 5, 6, 6},
  });

  auto expected = makeArrayVector<int64_t>({
      {3, 3},
      {4, 4},
      {5, 5},
      {6, 6},
  });

  auto result = evaluate<ArrayVector>(
      "if(c0 % 2 = 0, array_top_n(c1, INTEGER '2'), array_top_n(c2, INTEGER '2'))",
      makeRowVector({c0, c1, c2}));
  assertEqualVectors(expected, result);
}

//// Remove simple-type elements from array.
TEST_F(ArrayTopNTest, columnN) {
  const auto arrayVector = makeNullableArrayVector<int64_t>(
      {{1, std::nullopt, 2, 3, std::nullopt, 4},
       {3, 4, 5, std::nullopt, 3, 4, 5},
       {7, 8, 9},
       {10, 20, 30}});
  const auto elementVector = makeFlatVector<int32_t>({5, 4, 3, 2});
  const auto expected = makeNullableArrayVector<int64_t>({
      {4, 3, 2, 1, std::nullopt},
      {5, 5, 4, 4},
      {9, 8, 7},
      {30, 20},
  });

  auto result = evaluate<ArrayVector>(
      "array_top_n(c0, c1)", makeRowVector({arrayVector, elementVector}));
  assertEqualVectors(expected, result);
}

TEST_F(ArrayTopNTest, complexInput) {
  // Tests array of arrays as complex input.
  auto test = [this](
                  const VectorPtr& inputArrayVector,
                  int32_t size,
                  const VectorPtr& expectedArrayVector) {
    auto result = evaluate(
        fmt::format("array_top_n(c0, INTEGER '{}')", size),
        makeRowVector({inputArrayVector}));
    assertEqualVectors(expectedArrayVector, result);
  };

  auto seedVector =
      makeArrayVector<int64_t>({{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}});

  // Create arrays of array vector using above seed vector.
  // [[1, 1], [2, 2], [3, 3]]
  // [[4, 4], [5, 5]]
  const auto arrayOfArrayInput = makeArrayVector({0, 3}, seedVector);

  // [[1, 1], [2, 2]]
  // [[5, 5], [4, 4]]
  const auto expected = makeArrayVector(
      {0, 2}, makeArrayVector<int64_t>({{3, 3}, {2, 2}, {5, 5}, {4, 4}}));

  test({arrayOfArrayInput}, 2, {expected});
}

TEST_F(ArrayTopNTest, floatingPoints) {
  static const float kNaN = std::numeric_limits<float>::quiet_NaN();
  static const float kInfinity = std::numeric_limits<float>::infinity();
  static const float kNegativeInfinity =
      -1 * std::numeric_limits<float>::infinity();

  auto input = makeNullableArrayVector<float>(
      {{-1, kNaN, std::nullopt},
       {-1, -2, -3, kNaN, kNegativeInfinity},
       {kInfinity, kNaN},
       {kInfinity, kNaN, kNegativeInfinity}});
  auto expected = makeNullableArrayVector<float>(
      {{kNaN, -1, std::nullopt},
       {kNaN, -1, -2},
       {kNaN, kInfinity},
       {kNaN, kInfinity, kNegativeInfinity}});
  auto result =
      evaluate("array_top_n(c0, INTEGER '3')", makeRowVector({input}));

  assertEqualVectors(expected, result);
}

TEST_F(ArrayTopNTest, prestoLegacyTests) {
  // Legacy test cases from Presto
  assertEqualVectors(
      makeArrayVectorFromJson<int32_t>({"[1, 1, 1]"}),
      evaluate(
          "array_top_n(c0, INTEGER '3')",
          makeRowVector({makeArrayVectorFromJson<int32_t>({"[1, 1, 1, 1]"})})));
  assertEqualVectors(
      makeArrayVectorFromJson<int32_t>({"[100, 5, 3]"}),
      evaluate(
          "array_top_n(c0, INTEGER '3')",
          makeRowVector(
              {makeArrayVectorFromJson<int32_t>({"[1, 100, 2, 5, 3]"})})));

  assertEqualVectors(
      makeArrayVectorFromJson<float>({"[100.0, 5.0, 3.0]"}),
      evaluate(
          "array_top_n(c0, INTEGER '3')",
          makeRowVector({makeArrayVectorFromJson<float>(
              {"[1.0, 100.0, 2.0, 5.0, 3.0]"})})));
  assertEqualVectors(
      makeArrayVectorFromJson<float>({"[100.0, 5.0, 3.0]"}),
      evaluate(
          "array_top_n(c0, INTEGER '3')",
          makeRowVector(
              {makeArrayVectorFromJson<float>({"[1.0, 100, 2, 5.0, 3.0]"})})));

  assertEqualVectors(
      makeArrayVectorFromJson<int32_t>({"[4, 1, null]"}),
      evaluate(
          "array_top_n(c0, INTEGER '3')",
          makeRowVector({makeArrayVectorFromJson<int32_t>({"[4, 1, null]"})})));

  assertEqualVectors(
      makeArrayVectorFromJson<std::string>({"[\"z\", \"g\", \"f\", \"d\"]"}),
      evaluate(
          "array_top_n(c0, INTEGER '4')",
          makeRowVector({makeArrayVectorFromJson<std::string>(
              {"[\"a\", \"z\", \"d\", \"f\", \"g\", \"b\"]"})})));
  assertEqualVectors(
      makeArrayVectorFromJson<std::string>(
          {"[\"lorem2\", \"lorem\", \"ipsum\"]"}),
      evaluate(
          "array_top_n(c0,INTEGER '3')",
          makeRowVector({makeArrayVectorFromJson<std::string>(
              {"[\"foo\", \"bar\", \"lorem\", \"ipsum\", \"lorem2\"]"})})));
  assertEqualVectors(
      makeArrayVectorFromJson<std::string>({"[\"zzz\", \"zz\", \"g\"]"}),
      evaluate(
          "array_top_n(c0,INTEGER '3')",
          makeRowVector({makeArrayVectorFromJson<std::string>(
              {"[\"a\", \"zzz\", \"zz\", \"b\", \"g\", \"f\"]"})})));
  assertEqualVectors(
      makeArrayVectorFromJson<std::string>({"[\"d\", \"a\", \"a\"]"}),
      evaluate(
          "array_top_n(c0, INTEGER '3')",
          makeRowVector({makeArrayVectorFromJson<std::string>(
              {"[\"a\", \"a\", \"d\", \"a\", \"a\", \"a\"]"})})));

  assertEqualVectors(
      makeArrayVectorFromJson<bool>({"[true, true, true, false]"}),
      evaluate(
          "array_top_n(c0, INTEGER '4')",
          makeRowVector({makeArrayVectorFromJson<bool>(
              {"[true, true, false, true, false]"})})));
  assertEqualVectors(
      makeArrayVectorFromJson<int32_t>({"[]"}),
      evaluate(
          "array_top_n(c0, INTEGER '0')",
          makeRowVector({makeArrayVectorFromJson<int32_t>({"[1, 2, 3]"})})));
  assertEqualVectors(
      makeArrayVectorFromJson<UnknownValue>({"[null, null]"}),
      evaluate(
          "array_top_n(c0, INTEGER '2')",
          makeRowVector({makeArrayVectorFromJson<UnknownValue>(
              {"[null, null, null]"})})));
  VELOX_ASSERT_THROW(
      evaluate(
          "array_top_n(c0,INTEGER '-1')",
          makeRowVector({makeArrayVectorFromJson<int32_t>({"[1, 2, 3]"})})),
      "Parameter n: -1 to ARRAY_TOP_N is negative");
}

TEST_F(ArrayTopNTest, verifyValidInput) {
  VELOX_ASSERT_THROW(
      evaluate(
          "array_top_n(array_top_n(c0, BIGINT '2212345425207733713'), BIGINT '5485680446448804369')",
          makeRowVector({makeArrayVectorFromJson<int32_t>({"[1, 2, 3]"})})),
      "Scalar function signature is not supported: array_top_n(ARRAY<INTEGER>, BIGINT).");
}

} // namespace
