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

using namespace facebook::velox;
using namespace facebook::velox::test;

class ArrayAllMatchTest : public functions::test::FunctionBaseTest {};

TEST_F(ArrayAllMatchTest, basic) {
  auto arrayVector = makeNullableArrayVector<int64_t>(
      {{std::nullopt, 2, 3},
       {-1, 3},
       {2, 3},
       {},
       {std::nullopt, std::nullopt}});
  auto input = makeRowVector({arrayVector});
  auto result = evaluate("all_match(c0, x -> (x > 1))", input);
  auto expectedResult = makeNullableFlatVector<bool>(
      {std::nullopt, false, true, true, std::nullopt});
  assertEqualVectors(expectedResult, result);

  result = evaluate("all_match(c0, x -> (x is null))", input);
  expectedResult = makeFlatVector<bool>({false, false, false, true, true});
  assertEqualVectors(expectedResult, result);

  arrayVector = makeNullableArrayVector<bool>(
      {{false, true},
       {true, true},
       {std::nullopt, true},
       {std::nullopt, false}});
  input = makeRowVector({arrayVector});
  result = evaluate("all_match(c0, x -> x)", input);
  expectedResult =
      makeNullableFlatVector<bool>({false, true, std::nullopt, false});
  assertEqualVectors(expectedResult, result);

  auto emptyInput = makeRowVector({makeArrayVector<int32_t>({{}})});
  result = evaluate("all_match(c0, x -> (x > 1))", emptyInput);
  expectedResult = makeFlatVector<bool>(std::vector<bool>{true});
  assertEqualVectors(expectedResult, result);

  result = evaluate("all_match(c0, x -> (x < 1))", emptyInput);
  expectedResult = makeFlatVector<bool>(std::vector<bool>{true});
  assertEqualVectors(expectedResult, result);
}

TEST_F(ArrayAllMatchTest, complexTypes) {
  auto baseVector =
      makeArrayVector<int64_t>({{1, 2, 3}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {}});
  // Create an array of array vector using above base vector using offsets.
  // [
  //  [[1, 2, 3]],
  //  [[2, 2], [3, 3], [4, 4], [5, 5]],
  //  [[]]
  // ]
  auto arrayOfArrays = makeArrayVector({0, 1, 5}, baseVector);
  auto input = makeRowVector({arrayOfArrays});
  auto result = evaluate("all_match(c0, x -> (cardinality(x) > 0))", input);
  auto expectedResult = makeNullableFlatVector<bool>({true, true, false});
  assertEqualVectors(expectedResult, result);

  // Create an array of array vector using above base vector using offsets.
  // [
  //  [[1, 2, 3]],  cardinalities is 3
  //  [[2, 2], [3, 3], [4, 4], [5, 5]], all cardinalities is 2
  //  [[]],
  //  null
  // ]
  arrayOfArrays = makeArrayVector({0, 1, 5, 6}, baseVector, {3});
  input = makeRowVector({arrayOfArrays});
  result = evaluate("all_match(c0, x -> (cardinality(x) > 2))", input);
  expectedResult =
      makeNullableFlatVector<bool>({true, false, false, std::nullopt});
  assertEqualVectors(expectedResult, result);
}

TEST_F(ArrayAllMatchTest, bigints) {
  auto arrayVector = makeNullableArrayVector<int64_t>(
      {{},
       {2},
       {std::numeric_limits<int64_t>::max()},
       {std::numeric_limits<int64_t>::min()},
       {std::nullopt, std::nullopt}, // return null if all is null
       {2,
        std::nullopt}, // return null if one or more is null and others matched
       {1, std::nullopt, 2}}); // return false if one is not matched
  auto input = makeRowVector({arrayVector});
  auto result = evaluate("all_match(c0, x -> (x % 2 = 0))", input);

  auto expectedResult = makeNullableFlatVector<bool>(
      {true, true, false, true, std::nullopt, std::nullopt, false});
  assertEqualVectors(expectedResult, result);
}

TEST_F(ArrayAllMatchTest, strings) {
  auto input = makeRowVector({makeNullableArrayVector<StringView>(
      {{}, {"abc"}, {"ab", "abc"}, {std::nullopt}})});
  auto result = evaluate("all_match(c0, x -> (x = 'abc'))", input);

  auto expectedResult =
      makeNullableFlatVector<bool>({true, true, false, std::nullopt});
  assertEqualVectors(expectedResult, result);
}

TEST_F(ArrayAllMatchTest, doubles) {
  auto input = makeRowVector(
      {makeNullableArrayVector<double>({{}, {1.2}, {3.0, 0}, {std::nullopt}})});
  auto result = evaluate("all_match(c0, x -> (x > 1.1))", input);

  auto expectedResult =
      makeNullableFlatVector<bool>({true, true, false, std::nullopt});
  assertEqualVectors(expectedResult, result);
}

TEST_F(ArrayAllMatchTest, errors) {
  // No throw and return false if there are unmatched elements except nulls
  auto input = makeRowVector({makeNullableArrayVector<int8_t>(
      {{0, 2, 0, 5, 0}, {5, std::nullopt, 0}})});
  auto result = evaluate("all_match(c0, x -> ((10 / x) > 2))", input);
  auto expectedResult = makeFlatVector<bool>({false, false});
  assertEqualVectors(expectedResult, result);

  // Throw error if others are matched or null
  static constexpr std::string_view kErrorMessage{"division by zero"};
  auto errorInput = makeRowVector({makeNullableArrayVector<int8_t>(
      {{1, 0}, {2}, {6}, {1, 0, std::nullopt}, {10, std::nullopt}})});
  VELOX_ASSERT_THROW(
      evaluate("all_match(c0, x -> ((10 / x) > 2))", errorInput),
      kErrorMessage);
  VELOX_ASSERT_THROW(
      evaluate("all_match(c0, x -> ((10 / x) > 2))", errorInput),
      kErrorMessage);
  // Rerun using TRY to get right results
  expectedResult = makeNullableFlatVector<bool>(
      {std::nullopt, true, false, std::nullopt, false});
  result = evaluate("TRY(all_match(c0, x -> ((10 / x) > 2)))", errorInput);
  assertEqualVectors(expectedResult, result);

  result = evaluate("all_match(c0, x -> (TRY((10 / x) > 2)))", errorInput);
  assertEqualVectors(expectedResult, result);
}

TEST_F(ArrayAllMatchTest, conditional) {
  // No throw and return false if there are unmatched elements except nulls
  auto c0 = makeFlatVector<uint32_t>({1, 2, 3, 4, 5});
  auto c1 = makeNullableArrayVector<int32_t>(
      {{100, std::nullopt},
       {500, 120},
       {std::nullopt},
       {5, std::nullopt, 0},
       {3, 1}});
  auto result = evaluate(
      "all_match(c1, if (c0 <= 2, x -> (x > 100), x -> (10 / x > 2)))",
      makeRowVector({c0, c1}));
  auto expectedResult =
      makeNullableFlatVector<bool>({false, true, std::nullopt, false, true});
  assertEqualVectors(expectedResult, result);
}
