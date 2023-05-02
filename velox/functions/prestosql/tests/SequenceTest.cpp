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
using namespace facebook::velox::functions::test;

namespace {

class SequenceTest : public FunctionBaseTest {
 protected:
  void testExpression(
      const std::string& expression,
      const std::vector<VectorPtr>& input,
      const VectorPtr& expected) {
    auto result = evaluate(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  void testExpressionWithError(
      const std::string& expression,
      const std::vector<VectorPtr>& input,
      const std::string& expectedError) {
    VELOX_ASSERT_THROW(
        evaluate(expression, makeRowVector(input)), expectedError);
  }
};
} // namespace

TEST_F(SequenceTest, sequence) {
  const auto startVector = makeFlatVector<int64_t>({1, 2, 10});
  const auto stopVector = makeFlatVector<int64_t>({2, 5, 9});
  VectorPtr expected =
      makeArrayVector<int64_t>({{1, 2}, {2, 3, 4, 5}, {10, 9}});
  testExpression("sequence(C0, C1)", {startVector, stopVector}, expected);
}

TEST_F(SequenceTest, negative) {
  const auto startVector = makeFlatVector<int64_t>({-1, -2, -10});
  const auto stopVector = makeFlatVector<int64_t>({-2, -5, -9});
  VectorPtr expected =
      makeArrayVector<int64_t>({{-1, -2}, {-2, -3, -4, -5}, {-10, -9}});
  testExpression("sequence(C0, C1)", {startVector, stopVector}, expected);
}

TEST_F(SequenceTest, step) {
  const auto startVector = makeFlatVector<int64_t>({1, 2, 10});
  const auto stopVector = makeFlatVector<int64_t>({2, 5, 9});
  const auto stepVector = makeFlatVector<int64_t>({2, 2, -1});
  VectorPtr expected = makeArrayVector<int64_t>({{1}, {2, 4}, {10, 9}});
  testExpression(
      "sequence(C0, C1, C2)", {startVector, stopVector, stepVector}, expected);
}

TEST_F(SequenceTest, constant) {
  const auto endVector = makeFlatVector<int64_t>({2, 5, 1});
  VectorPtr expected = makeArrayVector<int64_t>({{1, 2}, {1, 2, 3, 4, 5}, {1}});
  testExpression("sequence(1, C0)", {endVector}, expected);
}

TEST_F(SequenceTest, null) {
  const auto startVector = makeNullableFlatVector<int64_t>({std::nullopt, 2});
  const auto stopVector = makeFlatVector<int64_t>({2, 5});
  VectorPtr expected = makeNullableArrayVector<int64_t>({
      std::nullopt,
      {{2, 3, 4, 5}},
  });
  testExpression("sequence(C0, C1)", {startVector, stopVector}, expected);
}

TEST_F(SequenceTest, exceedMaxEntries) {
  const auto startVector = makeFlatVector<int64_t>({1, 100});
  const auto stopVector = makeFlatVector<int64_t>({100000, 100});
  testExpressionWithError(
      "sequence(C0, C1)",
      {startVector, stopVector},
      "result of sequence function must not have more than 10000 entries");

  VectorPtr expected = makeNullableArrayVector<int64_t>({
      std::nullopt,
      {{100}},
  });
  testExpression("try(sequence(C0, C1))", {startVector, stopVector}, expected);
}

TEST_F(SequenceTest, invalidStep) {
  const auto startVector = makeFlatVector<int64_t>({1, 2});
  const auto stopVector = makeFlatVector<int64_t>({2, 5});
  const auto stepVector = makeFlatVector<int64_t>({0, 1});
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "(0 vs. 0) step must not be zero");

  VectorPtr expected = makeNullableArrayVector<int64_t>({
      std::nullopt,
      {{2, 3, 4, 5}},
  });
  testExpression(
      "try(sequence(C0, C1, C2))",
      {startVector, stopVector, stepVector},
      expected);
}

TEST_F(SequenceTest, dateArguments) {
  const auto startVector =
      makeFlatVector<Date>({Date(1991), Date(1992), Date(1992)});
  const auto stopVector =
      makeFlatVector<Date>({Date(1996), Date(1988), Date(1992)});
  const auto expected = makeArrayVector<Date>(
      {{Date(1991), Date(1992), Date(1993), Date(1994), Date(1995), Date(1996)},
       {Date(1992), Date(1991), Date(1990), Date(1989), Date(1988)},
       {Date(1992)}});
  testExpression("sequence(C0, C1)", {startVector, stopVector}, expected);
}

TEST_F(SequenceTest, dateArgumentsExceedMaxEntries) {
  const auto startVector =
      makeFlatVector<Date>({Date(1991), Date(1992), Date(1992)});
  const auto stopVector =
      makeFlatVector<Date>({Date(1996), Date(198800), Date(1992)});
  testExpressionWithError(
      "sequence(C0, C1)",
      {startVector, stopVector},
      "result of sequence function must not have more than 10000 entries");

  auto expected = makeNullableArrayVector<Date>(
      {{{Date(1991),
         Date(1992),
         Date(1993),
         Date(1994),
         Date(1995),
         Date(1996)}},
       std::nullopt,
       {{Date(1992)}}});
  testExpression("try(sequence(C0, C1))", {startVector, stopVector}, expected);
}

TEST_F(SequenceTest, intervalStep) {
  int64_t day = 86400000; // 24 * 60 * 60 * 1000
  const auto startVector = makeFlatVector<Date>({Date(1991), Date(1992)});
  const auto stopVector = makeFlatVector<Date>({Date(1996), Date(2000)});

  const auto stepVector =
      makeFlatVector<int64_t>({day, 2 * day}, INTERVAL_DAY_TIME());
  const auto expected = makeArrayVector<Date>(
      {{Date(1991), Date(1992), Date(1993), Date(1994), Date(1995), Date(1996)},
       {Date(1992), Date(1994), Date(1996), Date(1998), Date(2000)}});
  testExpression(
      "sequence(C0, C1, C2)", {startVector, stopVector, stepVector}, expected);
}

TEST_F(SequenceTest, invalidIntervalStep) {
  int64_t day = 86400000; // 24 * 60 * 60 * 1000
  const auto startVector =
      makeFlatVector<Date>({Date(1991), Date(1992), Date(1992)});
  const auto stopVector =
      makeFlatVector<Date>({Date(1996), Date(2000), Date(2000)});
  auto stepVector =
      makeFlatVector<int64_t>({-1 * day, 0, 1}, INTERVAL_DAY_TIME());
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "sequence stop value should be greater than or equal to start value if "
      "step is greater than zero otherwise stop should be less than or equal to "
      "start");

  stepVector = makeFlatVector<int64_t>({0, 1, -1 * day}, INTERVAL_DAY_TIME());
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "(0 vs. 0) step must not be zero");

  stepVector = makeFlatVector<int64_t>({1, -1 * day, 0}, INTERVAL_DAY_TIME());
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "sequence step must be a day interval if start and end values are dates");

  stepVector = makeFlatVector<int64_t>({1, -1 * day, 0}, INTERVAL_DAY_TIME());
  auto expected =
      makeNullableArrayVector<Date>({std::nullopt, std::nullopt, std::nullopt});
  testExpression(
      "try(sequence(C0, C1, C2))",
      {startVector, stopVector, stepVector},
      expected);
}
