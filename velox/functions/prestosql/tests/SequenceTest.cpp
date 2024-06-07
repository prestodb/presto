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
#include "velox/type/TimestampConversion.h"

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

TEST_F(SequenceTest, integerOverflow) {
  // stop - start = 9223372036854775807 - (-2147483648) would overflow
  auto startVector = makeFlatVector<int64_t>({int64_t(-2147483648), 1});
  auto stopVector = makeFlatVector<int64_t>({int64_t(9223372036854775807), 2});
  testExpressionWithError(
      "sequence(C0, C1)",
      {startVector, stopVector},
      "result of sequence function must not have more than 10000 entries");

  startVector = makeFlatVector<int64_t>({int64_t(-9000000000000000000), 1});
  stopVector = makeFlatVector<int64_t>({int64_t(9000000000000000000), 2});
  const auto stepVector = makeFlatVector<int64_t>({5000000000000000000, 1});
  // For the 3rd element's calculation
  // start + step * 3 = -9000000000000000000 + 5000000000000000000 * 3
  // 5000000000000000000 * 3 would overflow
  VectorPtr expected = makeArrayVector<int64_t>(
      {{-9000000000000000000,
        -4000000000000000000,
        1000000000000000000,
        6000000000000000000},
       {1, 2}});
  testExpression(
      "sequence(C0, C1, C2)", {startVector, stopVector, stepVector}, expected);
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
  const auto startVector = makeFlatVector<int32_t>({1991, 1992, 1992}, DATE());
  const auto stopVector = makeFlatVector<int32_t>({1996, 1988, 1992}, DATE());
  const auto expected = makeArrayVector<int32_t>(
      {{1991, 1992, 1993, 1994, 1995, 1996},
       {1992, 1991, 1990, 1989, 1988},
       {1992}},
      DATE());
  testExpression("sequence(C0, C1)", {startVector, stopVector}, expected);
}

TEST_F(SequenceTest, dateRange) {
  const auto startVector = makeConstant<int32_t>(0, 1, DATE());
  const auto stopVector =
      makeConstant<int32_t>(std::numeric_limits<int32_t>::max(), 1, DATE());
  const auto stepVector =
      makeConstant<int32_t>(12 * 1'000'000, 1, INTERVAL_YEAR_MONTH());
  const auto expected = makeArrayVector<int32_t>(
      {{0, 365242500, 730485000, 1095727500, 1460970000, 1826212500}}, DATE());
  testExpression(
      "sequence(C0, C1, C2)", {startVector, stopVector, stepVector}, expected);
}

TEST_F(SequenceTest, dateArgumentsExceedMaxEntries) {
  const auto startVector = makeFlatVector<int32_t>({1991, 1992, 1992}, DATE());
  const auto stopVector = makeFlatVector<int32_t>({1996, 198800, 1992}, DATE());
  testExpressionWithError(
      "sequence(C0, C1)",
      {startVector, stopVector},
      "result of sequence function must not have more than 10000 entries");

  auto expected = makeNullableArrayVector<int32_t>(
      {{{1991, 1992, 1993, 1994, 1995, 1996}}, std::nullopt, {{1992}}},
      ARRAY(DATE()));
  testExpression("try(sequence(C0, C1))", {startVector, stopVector}, expected);
}

TEST_F(SequenceTest, dateIntervalDayStep) {
  int64_t day = 86400000; // 24 * 60 * 60 * 1000
  const auto startVector = makeFlatVector<int32_t>({1991, 1992}, DATE());
  const auto stopVector = makeFlatVector<int32_t>({1996, 2000}, DATE());

  const auto stepVector =
      makeFlatVector<int64_t>({day, 2 * day}, INTERVAL_DAY_TIME());
  const auto expected = makeArrayVector<int32_t>(
      {{1991, 1992, 1993, 1994, 1995, 1996}, {1992, 1994, 1996, 1998, 2000}},
      DATE());
  testExpression(
      "sequence(C0, C1, C2)", {startVector, stopVector, stepVector}, expected);
}

TEST_F(SequenceTest, dateInvalidIntervalDayStep) {
  int64_t day = 86400000; // 24 * 60 * 60 * 1000
  const auto startVector = makeFlatVector<int32_t>({1991, 1992, 1992}, DATE());
  const auto stopVector = makeFlatVector<int32_t>({1996, 2000, 2000}, DATE());
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
  auto expected = makeNullableArrayVector<int32_t>(
      {std::nullopt, std::nullopt, std::nullopt}, ARRAY(DATE()));
  testExpression(
      "try(sequence(C0, C1, C2))",
      {startVector, stopVector, stepVector},
      expected);
}

TEST_F(SequenceTest, dateYearMonthStep) {
  const auto startVector = makeFlatVector<int32_t>(
      {parseDate("1975-01-31"),
       parseDate("1975-03-15"),
       parseDate("2023-12-31"),
       parseDate("3892314-06-02")},
      DATE());
  const auto stopVector = makeFlatVector<int32_t>(
      {parseDate("1975-06-20"),
       parseDate("1974-12-15"),
       parseDate("2024-05-31"),
       parseDate("4045127-11-23")},
      DATE());

  const auto stepVector =
      makeFlatVector<int32_t>({1, -1, 2, 162700}, INTERVAL_YEAR_MONTH());
  const auto expected = makeArrayVector<int32_t>(
      {// last day of Feb
       // result won't include 1975-06-20
       {parseDate("1975-01-31"),
        parseDate("1975-02-28"),
        parseDate("1975-03-31"),
        parseDate("1975-04-30"),
        parseDate("1975-05-31")},
       // negative step
       {parseDate("1975-03-15"),
        parseDate("1975-02-15"),
        parseDate("1975-01-15"),
        parseDate("1974-12-15")},
       // leap year
       {parseDate("2023-12-31"),
        parseDate("2024-02-29"),
        parseDate("2024-04-30")},
       // range of date
       {parseDate("3892314-06-02"),
        parseDate("3905872-10-02"),
        parseDate("3919431-02-02"),
        parseDate("3932989-06-02"),
        parseDate("3946547-10-02"),
        parseDate("3960106-02-02"),
        parseDate("3973664-06-02"),
        parseDate("3987222-10-02"),
        parseDate("4000781-02-02"),
        parseDate("4014339-06-02"),
        parseDate("4027897-10-02"),
        parseDate("4041456-02-02")}},
      DATE());
  testExpression(
      "sequence(C0, C1, C2)", {startVector, stopVector, stepVector}, expected);
}

TEST_F(SequenceTest, dateInvalidYearMonthStep) {
  const auto startVector = makeFlatVector<int32_t>(
      {parseDate("1975-01-31"), parseDate("1975-03-15")}, DATE());
  const auto stopVector = makeFlatVector<int32_t>(
      {parseDate("1975-06-01"), parseDate("1974-12-15")}, DATE());

  auto stepVector = makeFlatVector<int32_t>({0, 0}, INTERVAL_DAY_TIME());
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "(0 vs. 0) step must not be zero");

  stepVector = makeFlatVector<int32_t>({1, 1}, INTERVAL_YEAR_MONTH());
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "sequence stop value should be greater than or equal to start value if "
      "step is greater than zero otherwise stop should be less than or equal to "
      "start");

  auto expected = makeNullableArrayVector<int32_t>(
      {{{parseDate("1975-01-31"),
         parseDate("1975-02-28"),
         parseDate("1975-03-31"),
         parseDate("1975-04-30"),
         parseDate("1975-05-31")}},
       std::nullopt},
      ARRAY(DATE()));
  testExpression(
      "try(sequence(C0, C1, C2))",
      {startVector, stopVector, stepVector},
      expected);
}

TEST_F(SequenceTest, dateIntervalExceedMaxEntries) {
  const auto startVector = makeFlatVector<int32_t>(
      {parseDate("1975-01-31"), parseDate("1975-03-15")}, DATE());
  const auto stopVector = makeFlatVector<int32_t>(
      {parseDate("3975-06-01"), parseDate("3974-12-15")}, DATE());

  auto stepVector = makeFlatVector<int32_t>({1, 1}, INTERVAL_YEAR_MONTH());
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "result of sequence function must not have more than 10000 entries");
}

TEST_F(SequenceTest, timestamp) {
  const auto startVector =
      makeFlatVector<Timestamp>({Timestamp(1991, 0), Timestamp(1992, 0)});
  const auto stopVector =
      makeFlatVector<Timestamp>({Timestamp(1996, 0), Timestamp(1992, 0)});
  const auto stepVector =
      makeFlatVector<int64_t>({1000, 1000}, INTERVAL_DAY_TIME());
  const auto expected = makeArrayVector<Timestamp>(
      {{Timestamp(1991, 0),
        Timestamp(1992, 0),
        Timestamp(1993, 0),
        Timestamp(1994, 0),
        Timestamp(1995, 0),
        Timestamp(1996, 0)},
       {Timestamp(1992, 0)}});
  testExpression(
      "sequence(C0, C1, C2)", {startVector, stopVector, stepVector}, expected);
}

TEST_F(SequenceTest, timestampExceedMaxEntries) {
  const auto startVector =
      makeFlatVector<Timestamp>({Timestamp(1991, 0), Timestamp(1992, 0)});
  const auto stopVector =
      makeFlatVector<Timestamp>({Timestamp(1996, 0), Timestamp(19920, 0)});
  const auto stepVector =
      makeFlatVector<int64_t>({1000, 1}, INTERVAL_DAY_TIME());
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "result of sequence function must not have more than 10000 entries");

  const auto expected = makeNullableArrayVector<Timestamp>(
      {{{Timestamp(1991, 0),
         Timestamp(1992, 0),
         Timestamp(1993, 0),
         Timestamp(1994, 0),
         Timestamp(1995, 0),
         Timestamp(1996, 0)}},
       std::nullopt});
  testExpression(
      "try(sequence(C0, C1, C2))",
      {startVector, stopVector, stepVector},
      expected);
}

TEST_F(SequenceTest, timestampInvalidIntervalStep) {
  const auto startVector =
      makeFlatVector<Timestamp>({Timestamp(1991, 0), Timestamp(1992, 0)});
  const auto stopVector =
      makeFlatVector<Timestamp>({Timestamp(1996, 0), Timestamp(1992, 0)});
  const auto stepVector =
      makeFlatVector<int64_t>({0, 1000}, INTERVAL_DAY_TIME());
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "(0 vs. 0) step must not be zero");
  const auto expected = makeNullableArrayVector<Timestamp>(
      {std::nullopt, {{Timestamp(1992, 0)}}});
  testExpression(
      "try(sequence(C0, C1, C2))",
      {startVector, stopVector, stepVector},
      expected);
}

TEST_F(SequenceTest, timestampYearMonthStep) {
  const auto startVector = makeFlatVector<Timestamp>(
      {parseTimestamp("1975-01-31 10:00:00.500"),
       parseTimestamp("1975-03-15 10:10:10.200"),
       parseTimestamp("2023-12-31 23:00:00.500")});
  const auto stopVector = makeFlatVector<Timestamp>(
      {parseTimestamp("1975-06-01 01:00:00.500"),
       parseTimestamp("1974-12-15 10:20:00.500"),
       parseTimestamp("2024-05-31 10:00:00.500")});

  const auto stepVector =
      makeFlatVector<int32_t>({1, -1, 2}, INTERVAL_YEAR_MONTH());
  const auto expected = makeArrayVector<Timestamp>(
      {// last day of Feb
       {parseTimestamp("1975-01-31 10:00:00.500"),
        parseTimestamp("1975-02-28 10:00:00.500"),
        parseTimestamp("1975-03-31 10:00:00.500"),
        parseTimestamp("1975-04-30 10:00:00.500"),
        parseTimestamp("1975-05-31 10:00:00.500")},
       // date is the same but timestamp is different so couldn't include
       // 1974-12-15 10:10:10.200
       // negative step
       {parseTimestamp("1975-03-15 10:10:10.200"),
        parseTimestamp("1975-02-15 10:10:10.200"),
        parseTimestamp("1975-01-15 10:10:10.200")},
       // leap year
       // result won't include 2024-05-31 10:00:00.500
       {parseTimestamp("2023-12-31 23:00:00.500"),
        parseTimestamp("2024-02-29 23:00:00.500"),
        parseTimestamp("2024-04-30 23:00:00.500")}});
  testExpression(
      "sequence(C0, C1, C2)", {startVector, stopVector, stepVector}, expected);
}

TEST_F(SequenceTest, timestampInvalidYearMonthStep) {
  const auto startVector = makeFlatVector<Timestamp>(
      {parseTimestamp("1975-01-31 10:00:00.500"),
       parseTimestamp("1975-03-15 10:10:10.200"),
       parseTimestamp("2023-12-31 23:00:00.500")});
  const auto stopVector = makeFlatVector<Timestamp>(
      {parseTimestamp("1975-06-01 01:00:00.500"),
       parseTimestamp("1974-12-15 10:20:00.500"),
       parseTimestamp("2024-05-31 10:00:00.500")});

  auto stepVector = makeFlatVector<int32_t>({0, 0, 0}, INTERVAL_DAY_TIME());
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "(0 vs. 0) step must not be zero");

  stepVector = makeFlatVector<int32_t>({1, 1, 2}, INTERVAL_YEAR_MONTH());
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "sequence stop value should be greater than or equal to start value if "
      "step is greater than zero otherwise stop should be less than or equal to "
      "start");

  auto expected = makeNullableArrayVector<Timestamp>(
      {// last day of Feb
       {{parseTimestamp("1975-01-31 10:00:00.500"),
         parseTimestamp("1975-02-28 10:00:00.500"),
         parseTimestamp("1975-03-31 10:00:00.500"),
         parseTimestamp("1975-04-30 10:00:00.500"),
         parseTimestamp("1975-05-31 10:00:00.500")}},
       std::nullopt,
       // leap year
       // result won't include 2024-05-31 10:00:00.500
       {{parseTimestamp("2023-12-31 23:00:00.500"),
         parseTimestamp("2024-02-29 23:00:00.500"),
         parseTimestamp("2024-04-30 23:00:00.500")}}});
  testExpression(
      "try(sequence(C0, C1, C2))",
      {startVector, stopVector, stepVector},
      expected);
}

TEST_F(SequenceTest, timestampIntervalExceedMaxEntries) {
  const auto startVector = makeFlatVector<Timestamp>(
      {parseTimestamp("1975-01-31 10:00:00.500"),
       parseTimestamp("1975-03-15 10:10:10.200"),
       parseTimestamp("2023-12-31 23:00:00.500")});
  const auto stopVector = makeFlatVector<Timestamp>(
      {parseTimestamp("3975-06-01 01:00:00.500"),
       parseTimestamp("3974-12-15 10:20:00.500"),
       parseTimestamp("4024-05-31 10:00:00.500")});
  auto stepVector = makeFlatVector<int32_t>({1, 1, 1}, INTERVAL_YEAR_MONTH());
  testExpressionWithError(
      "sequence(C0, C1, C2)",
      {startVector, stopVector, stepVector},
      "result of sequence function must not have more than 10000 entries");
}
