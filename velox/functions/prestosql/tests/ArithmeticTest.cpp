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
#include <cmath>
#include <limits>
#include <optional>

#include <gmock/gmock.h>

#include <velox/common/base/VeloxException.h>
#include <velox/vector/SimpleVector.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox {
namespace {

constexpr double kInf = std::numeric_limits<double>::infinity();
constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
constexpr float kInfF = std::numeric_limits<float>::infinity();
constexpr float kNanF = std::numeric_limits<float>::quiet_NaN();
constexpr int64_t kLongMax = std::numeric_limits<int64_t>::max();
constexpr int64_t kLongMin = std::numeric_limits<int64_t>::min();

MATCHER(IsNan, "is NaN") {
  return arg && std::isnan(*arg);
}

MATCHER(IsInf, "is Infinity") {
  return arg && std::isinf(*arg);
}

class ArithmeticTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename T, typename TExpected = T>
  void assertExpression(
      const std::string& expression,
      const std::vector<T>& arg0,
      const std::vector<T>& arg1,
      const std::vector<TExpected>& expected) {
    auto vector0 = makeFlatVector(arg0);
    auto vector1 = makeFlatVector(arg1);

    auto result = evaluate<SimpleVector<TExpected>>(
        expression, makeRowVector({vector0, vector1}));
    for (int32_t i = 0; i < arg0.size(); ++i) {
      if (std::isnan(expected[i])) {
        ASSERT_TRUE(std::isnan(result->valueAt(i))) << "at " << i;
      } else {
        ASSERT_EQ(result->valueAt(i), expected[i]) << "at " << i;
      }
    }
  }

  void assertExpression(
      const std::string& expression,
      const VectorPtr& arg0,
      const VectorPtr& arg1,
      const VectorPtr& expected) {
    auto result = evaluate(expression, makeRowVector({arg0, arg1}));
    test::assertEqualVectors(expected, result);
  }

  template <typename T, typename U = T, typename V = T>
  void assertError(
      const std::string& expression,
      const std::vector<T>& arg0,
      const std::vector<U>& arg1,
      const std::string& errorMessage) {
    auto vector0 = makeFlatVector(arg0);
    auto vector1 = makeFlatVector(arg1);

    try {
      evaluate<SimpleVector<V>>(expression, makeRowVector({vector0, vector1}));
      ASSERT_TRUE(false) << "Expected an error";
    } catch (const std::exception& e) {
      ASSERT_TRUE(
          std::string(e.what()).find(errorMessage) != std::string::npos);
    }
  }
};

TEST_F(ArithmeticTest, plus) {
  // Test plus for intervals.
  auto op1 = makeNullableFlatVector<int64_t>(
      {-1, 2, -3, 4, kLongMax, -1, std::nullopt, 0}, INTERVAL_DAY_TIME());
  auto op2 = makeNullableFlatVector<int64_t>(
      {2, -3, -1, 1, 1, kLongMin, 0, std::nullopt}, INTERVAL_DAY_TIME());
  auto expected = makeNullableFlatVector<int64_t>(
      {1, -1, -4, 5, kLongMin, kLongMax, std::nullopt, std::nullopt},
      INTERVAL_DAY_TIME());
  assertExpression("c0 + c1", op1, op2, expected);
}

TEST_F(ArithmeticTest, minus) {
  // Test plus for intervals.
  auto op1 = makeNullableFlatVector<int64_t>(
      {-1, 2, -3, 4, kLongMin, -1, std::nullopt, 0}, INTERVAL_DAY_TIME());
  auto op2 = makeNullableFlatVector<int64_t>(
      {2, 3, -4, 1, 1, kLongMax, 0, std::nullopt}, INTERVAL_DAY_TIME());
  auto expected = makeNullableFlatVector<int64_t>(
      {-3, -1, 1, 3, kLongMax, kLongMin, std::nullopt, std::nullopt},
      INTERVAL_DAY_TIME());
  assertExpression("c0 - c1", op1, op2, expected);
}

TEST_F(ArithmeticTest, divide)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
__attribute__((__no_sanitize__("float-divide-by-zero")))
#endif
#endif
{
  assertExpression<int32_t>(
      "c0 / c1", {10, 11, -34, 0}, {2, 2, 10, -1}, {5, 5, -3, 0});
  assertExpression<int64_t>(
      "c0 / c1", {10, 11, -34, 0}, {2, 2, 10, -1}, {5, 5, -3, 0});

  assertError<int32_t>("c0 / c1", {10}, {0}, "division by zero");
  assertError<int32_t>("c0 / c1", {0}, {0}, "division by zero");
  assertError<int32_t>(
      "c0 / c1",
      {std::numeric_limits<int32_t>::min()},
      {-1},
      "integer overflow: -2147483648 / -1");

  assertExpression<float>(
      "c0 / c1",
      {10.5, 9.2, 0.0, 0.0},
      {2, 0, 0, -1},
      {5.25, kInfF, kNanF, 0.0});
  assertExpression<double>(
      "c0 / c1", {10.5, 9.2, 0.0, 0.0}, {2, 0, 0, -1}, {5.25, kInf, kNan, 0.0});

  // Test interval divided by double.
  auto intervalVector = makeNullableFlatVector<int64_t>(
      {3, 6, 9, std::nullopt, 12, 15, 18, 21, 0, 0, 1}, INTERVAL_DAY_TIME());
  auto doubleVector = makeNullableFlatVector<double>(
      {0.5,
       -2.0,
       5.0,
       1.0,
       std::nullopt,
       kNan,
       kInf,
       -kInf,
       0.0,
       -0.0,
       0.00000001});
  auto expected = makeNullableFlatVector<int64_t>(
      {6, -3, 1, std::nullopt, std::nullopt, 0, 0, 0, 0, 0, 100000000},
      INTERVAL_DAY_TIME());
  assertExpression("c0 / c1", intervalVector, doubleVector, expected);

  intervalVector = makeFlatVector<int64_t>(
      {1, 1, 1, 1, kLongMax, kLongMin, kLongMax, kLongMin},
      INTERVAL_DAY_TIME());
  doubleVector = makeFlatVector<double>(
      {0.0, -0.0, 4.9e-324, -4.9e-324, 0.1, 0.1, -0.1, -0.1});
  expected = makeFlatVector<int64_t>(
      {kLongMax,
       kLongMin,
       kLongMax,
       kLongMin,
       kLongMax,
       kLongMin,
       kLongMin,
       kLongMax},
      INTERVAL_DAY_TIME());
  assertExpression("c0 / c1", intervalVector, doubleVector, expected);
}

TEST_F(ArithmeticTest, multiply) {
  assertError<int32_t>(
      "c0 * c1",
      {std::numeric_limits<int32_t>::min()},
      {-1},
      "integer overflow: -2147483648 * -1");

  // Test multiplication of interval type with bigint.
  auto intervalVector = makeNullableFlatVector<int64_t>(
      {1, 2, 3, std::nullopt, 10, 20}, INTERVAL_DAY_TIME());
  auto bigintVector = makeNullableFlatVector<int64_t>(
      {1, std::nullopt, 3, 4, kLongMax, kLongMin});
  auto expected = makeNullableFlatVector<int64_t>(
      {1, std::nullopt, 9, std::nullopt, -10, 0}, INTERVAL_DAY_TIME());
  assertExpression("c0 * c1", intervalVector, bigintVector, expected);
  assertExpression("c1 * c0", intervalVector, bigintVector, expected);

  // Test multiplication of interval type with double.
  intervalVector = makeNullableFlatVector<int64_t>(
      {1, 2, 3, std::nullopt, 10, 20, 30, 40, 1000, 1000, 1000, 1000},
      INTERVAL_DAY_TIME());
  auto doubleVector = makeNullableFlatVector<double>(
      {-1.8,
       2.1,
       kNan,
       4.2,
       0.0,
       -0.0,
       kInf,
       -kInf,
       9223372036854775807.01,
       -9223372036854775808.01,
       1.7e308,
       -1.7e308});
  expected = makeNullableFlatVector<int64_t>(
      {-1,
       4,
       0,
       std::nullopt,
       0,
       0,
       kLongMax,
       kLongMin,
       kLongMax,
       kLongMin,
       kLongMax,
       kLongMin},
      INTERVAL_DAY_TIME());
  assertExpression("c0 * c1", intervalVector, doubleVector, expected);
  assertExpression("c1 * c0", intervalVector, doubleVector, expected);
}

TEST_F(ArithmeticTest, mod) {
  std::vector<double> numerDouble = {0, 6, 0, -7, -1, -9, 9, 10.1};
  std::vector<double> denomDouble = {1, 2, -1, 3, -1, -3, -3, -99.9};
  std::vector<double> expectedDouble = {0, 0, 0, -1, 0, 0, 0, 10.1};

  // Check using function name and alias.
  assertExpression<double>(
      "mod(c0, c1)", numerDouble, denomDouble, expectedDouble);
  assertExpression<double>(
      "mod(c0, c1)",
      {5.1, kNan, 5.1, kInf, 5.1},
      {0.0, 5.1, kNan, 5.1, kInf},
      {kNan, kNan, kNan, kNan, 5.1});
}

TEST_F(ArithmeticTest, modInt) {
  std::vector<int64_t> numerInt = {
      9, 10, 0, -9, -10, -11, std::numeric_limits<int64_t>::min()};
  std::vector<int64_t> denomInt = {3, -3, 11, -1, 199999, 77, -1};
  std::vector<int64_t> expectedInt = {0, 1, 0, 0, -10, -11, 0};

  assertExpression<int64_t, int64_t>(
      "mod(c0, c1)", numerInt, denomInt, expectedInt);
  assertError<int64_t>("mod(c0, c1)", {10}, {0}, "Cannot divide by 0");
}

TEST_F(ArithmeticTest, power) {
  std::vector<double> baseDouble = {
      0, 0, 0, -1, -1, -1, -9, 9.1, 10.1, 11.1, -11.1};
  std::vector<double> exponentDouble = {
      0, 1, -1, 0, 1, -1, -3.3, 123456.432, -99.9, 0, 100000};
  std::vector<double> expectedDouble;
  expectedDouble.reserve(baseDouble.size());

  for (size_t i = 0; i < baseDouble.size(); i++) {
    expectedDouble.emplace_back(pow(baseDouble[i], exponentDouble[i]));
  }

  // Check using function name and alias.
  assertExpression<double>(
      "power(c0, c1)", baseDouble, exponentDouble, expectedDouble);
  assertExpression<double>(
      "pow(c0, c1)", baseDouble, exponentDouble, expectedDouble);
}

TEST_F(ArithmeticTest, powerNan) {
  std::vector<double> baseDouble = {1, kNan, kNan, kNan};
  std::vector<double> exponentDouble = {kNan, 1, kInf, 0};
  std::vector<double> expectedDouble = {kNan, kNan, kNan, 1};

  // Check using function name and alias.
  assertExpression<double>(
      "power(c0, c1)", baseDouble, exponentDouble, expectedDouble);
  assertExpression<double>(
      "pow(c0, c1)", baseDouble, exponentDouble, expectedDouble);
}

TEST_F(ArithmeticTest, powerInf) {
  std::vector<double> baseDouble = {1, kInf, kInf, kInf};
  std::vector<double> exponentDouble = {kInf, 1, kNan, 0};
  std::vector<double> expectedDouble = {kNan, kInf, kNan, 1};

  // Check using function name and alias.
  assertExpression<double>(
      "power(c0, c1)", baseDouble, exponentDouble, expectedDouble);
  assertExpression<double>(
      "pow(c0, c1)", baseDouble, exponentDouble, expectedDouble);
}

TEST_F(ArithmeticTest, powerInt) {
  std::vector<int64_t> baseInt = {9, 10, 11, -9, -10, -11, 0};
  std::vector<int64_t> exponentInt = {3, -3, 0, -1, 199999, 77, 0};
  std::vector<double> expectedInt;
  expectedInt.reserve(baseInt.size());

  for (size_t i = 0; i < baseInt.size(); i++) {
    expectedInt.emplace_back(pow(baseInt[i], exponentInt[i]));
  }

  assertExpression<int64_t, double>(
      "power(c0, c1)", baseInt, exponentInt, expectedInt);
  assertExpression<int64_t, double>(
      "pow(c0, c1)", baseInt, exponentInt, expectedInt);
}

TEST_F(ArithmeticTest, exp) {
  const double kE = std::exp(1);

  const auto exp = [&](std::optional<double> a) {
    return evaluateOnce<double>("exp(c0)", a);
  };

  EXPECT_EQ(1, exp(0));
  EXPECT_EQ(kE, exp(1));
  EXPECT_EQ(1 / kE, exp(-1));
  EXPECT_EQ(kInf, exp(kInf));
  EXPECT_EQ(0, exp(-kInf));
  EXPECT_EQ(std::nullopt, exp(std::nullopt));
  EXPECT_THAT(exp(kNan), IsNan());
}

TEST_F(ArithmeticTest, ln) {
  const double kE = std::exp(1);

  const auto ln = [&](std::optional<double> a) {
    return evaluateOnce<double>("ln(c0)", a);
  };

  EXPECT_EQ(0, ln(1));
  EXPECT_EQ(1, ln(kE));
  EXPECT_EQ(-kInf, ln(0));
  EXPECT_THAT(ln(-1), IsNan());
  EXPECT_THAT(ln(kNan), IsNan());
  EXPECT_EQ(kInf, ln(kInf));
  EXPECT_EQ(std::nullopt, ln(std::nullopt));
}

TEST_F(ArithmeticTest, log2) {
  const auto log2 = [&](std::optional<double> a) {
    return evaluateOnce<double>("log2(c0)", a);
  };

  EXPECT_EQ(log2(1), 0);
  EXPECT_THAT(log2(-1), IsNan());
  EXPECT_EQ(log2(std::nullopt), std::nullopt);
  EXPECT_EQ(log2(kInf), kInf);
  EXPECT_THAT(log2(kNan), IsNan());
}

TEST_F(ArithmeticTest, log10) {
  const auto log10 = [&](std::optional<double> a) {
    return evaluateOnce<double>("log10(c0)", a);
  };

  EXPECT_EQ(log10(10), 1);
  EXPECT_EQ(log10(1), 0);
  EXPECT_EQ(log10(0.1), -1);
  EXPECT_THAT(log10(-1), IsNan());
  EXPECT_EQ(log10(std::nullopt), std::nullopt);
  EXPECT_EQ(log10(kInf), kInf);
  EXPECT_THAT(log10(kNan), IsNan());
}

TEST_F(ArithmeticTest, cos) {
  const auto cos = [&](std::optional<double> a) {
    return evaluateOnce<double>("cos(c0)", a);
  };

  EXPECT_EQ(cos(0), 1);
  EXPECT_EQ(cos(std::nullopt), std::nullopt);
  EXPECT_THAT(cos(kNan), IsNan());
}

TEST_F(ArithmeticTest, cosh) {
  const auto cosh = [&](std::optional<double> a) {
    return evaluateOnce<double>("cosh(c0)", a);
  };

  EXPECT_EQ(cosh(0), 1);
  EXPECT_EQ(cosh(std::nullopt), std::nullopt);
  EXPECT_THAT(cosh(kNan), IsNan());
}

TEST_F(ArithmeticTest, acos) {
  const auto acos = [&](std::optional<double> a) {
    return evaluateOnce<double>("acos(c0)", a);
  };

  EXPECT_EQ(acos(1), 0);
  EXPECT_EQ(acos(std::nullopt), std::nullopt);
  EXPECT_THAT(acos(kNan), IsNan());
  EXPECT_THAT(acos(1.1), IsNan());
}

TEST_F(ArithmeticTest, sin) {
  const auto sin = [&](std::optional<double> a) {
    return evaluateOnce<double>("sin(c0)", a);
  };

  EXPECT_EQ(sin(0), 0);
  EXPECT_EQ(sin(std::nullopt), std::nullopt);
  EXPECT_THAT(sin(kNan), IsNan());
}

TEST_F(ArithmeticTest, asin) {
  const auto asin = [&](std::optional<double> a) {
    return evaluateOnce<double>("asin(c0)", a);
  };

  EXPECT_EQ(asin(0), 0);
  EXPECT_EQ(asin(std::nullopt), std::nullopt);
  EXPECT_THAT(asin(kNan), IsNan());
}

TEST_F(ArithmeticTest, tan) {
  const auto tan = [&](std::optional<double> a) {
    return evaluateOnce<double>("tan(c0)", a);
  };

  EXPECT_EQ(tan(0), 0);
  EXPECT_EQ(tan(std::nullopt), std::nullopt);
  EXPECT_THAT(tan(kNan), IsNan());
}

TEST_F(ArithmeticTest, tanh) {
  const auto tanh = [&](std::optional<double> a) {
    return evaluateOnce<double>("tanh(c0)", a);
  };

  EXPECT_EQ(tanh(0), 0);
  EXPECT_EQ(tanh(std::nullopt), std::nullopt);
  EXPECT_THAT(tanh(kNan), IsNan());
}

TEST_F(ArithmeticTest, atan) {
  const auto atan = [&](std::optional<double> a) {
    return evaluateOnce<double>("atan(c0)", a);
  };

  EXPECT_EQ(atan(0), 0);
  EXPECT_EQ(atan(std::nullopt), std::nullopt);
  EXPECT_THAT(atan(kNan), IsNan());
}

TEST_F(ArithmeticTest, atan2) {
  const auto atan2 = [&](std::optional<double> y, std::optional<double> x) {
    return evaluateOnce<double>("atan2(c0, c1)", y, x);
  };

  EXPECT_EQ(atan2(0, 0), 0);
  EXPECT_EQ(atan2(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(atan2(1.0E0, std::nullopt), std::nullopt);
  EXPECT_EQ(atan2(std::nullopt, 1.0E0), std::nullopt);
}

TEST_F(ArithmeticTest, sqrt) {
  constexpr double kDoubleMax = std::numeric_limits<double>::max();

  const auto sqrt = [&](std::optional<double> a) {
    return evaluateOnce<double>("sqrt(c0)", a);
  };

  EXPECT_EQ(1.0, sqrt(1));
  EXPECT_THAT(sqrt(-1.0), IsNan());
  EXPECT_EQ(0, sqrt(0));

  EXPECT_EQ(2, sqrt(4));
  EXPECT_EQ(3, sqrt(9));
  EXPECT_FLOAT_EQ(1.34078e+154, sqrt(kDoubleMax).value_or(-1));
  EXPECT_EQ(std::nullopt, sqrt(std::nullopt));
  EXPECT_THAT(sqrt(kNan), IsNan());
}

TEST_F(ArithmeticTest, cbrt) {
  constexpr double kDoubleMax = std::numeric_limits<double>::max();

  const auto cbrt = [&](std::optional<double> a) {
    return evaluateOnce<double>("cbrt(c0)", a);
  };

  EXPECT_EQ(1.0, cbrt(1.0));
  EXPECT_EQ(-1.0, cbrt(-1.0));
  EXPECT_EQ(0, cbrt(0));

  EXPECT_DOUBLE_EQ(3, cbrt(27).value_or(-1));
  EXPECT_EQ(-4, cbrt(-64));
  EXPECT_FLOAT_EQ(1.34078e+154, cbrt(kDoubleMax).value_or(-1));
  EXPECT_EQ(std::nullopt, cbrt(std::nullopt));
  EXPECT_THAT(cbrt(kNan), IsNan());
}

TEST_F(ArithmeticTest, widthBucket) {
  constexpr int64_t kMaxInt64 = std::numeric_limits<int64_t>::max();

  const auto widthBucket = [&](std::optional<double> operand,
                               std::optional<double> bound1,
                               std::optional<double> bound2,
                               std::optional<int64_t> bucketCount) {
    return evaluateOnce<int64_t>(
        "width_bucket(c0, c1, c2, c3)", operand, bound1, bound2, bucketCount);
  };

  // bound1 < bound2
  EXPECT_EQ(3, widthBucket(3.14, 0, 4, 3));
  EXPECT_EQ(2, widthBucket(2, 0, 4, 3));
  EXPECT_EQ(4, widthBucket(kInf, 0, 4, 3));
  EXPECT_EQ(0, widthBucket(-1, 0, 3.2, 4));

  // bound1 > bound2
  EXPECT_EQ(1, widthBucket(3.14, 4, 0, 3));
  EXPECT_EQ(2, widthBucket(2, 4, 0, 3));
  EXPECT_EQ(0, widthBucket(kInf, 4, 0, 3));
  EXPECT_EQ(5, widthBucket(-1, 3.2, 0, 4));

  // failure
  VELOX_ASSERT_THROW(
      widthBucket(3.14, 0, 4, 0), "bucketCount must be greater than 0");
  VELOX_ASSERT_THROW(widthBucket(kNan, 0, 4, 10), "operand must not be NaN");
  VELOX_ASSERT_THROW(
      widthBucket(3.14, kNan, 0, 10), "first bound must be finite");
  VELOX_ASSERT_THROW(
      widthBucket(3.14, kInf, 0, 10), "first bound must be finite");
  VELOX_ASSERT_THROW(
      widthBucket(3.14, 0, kNan, 10), "second bound must be finite");
  VELOX_ASSERT_THROW(
      widthBucket(3.14, 0, kInf, 10), "second bound must be finite");
  VELOX_ASSERT_THROW(
      widthBucket(3.14, 0, 0, 10), "bounds cannot equal each other");
  VELOX_ASSERT_THROW(
      widthBucket(kInf, 0, 4, kMaxInt64),
      "Bucket for value inf is out of range");
  VELOX_ASSERT_THROW(
      widthBucket(kInf, 4, 0, kMaxInt64),
      "Bucket for value inf is out of range");
}

TEST_F(ArithmeticTest, radians) {
  const auto radians = [&](std::optional<double> a) {
    return evaluateOnce<double>("radians(c0)", a);
  };

  EXPECT_EQ(std::nullopt, radians(std::nullopt));
  EXPECT_DOUBLE_EQ(3.1415926535897931, radians(180).value());
  EXPECT_DOUBLE_EQ(1.0000736613927508, radians(57.3).value());
  EXPECT_DOUBLE_EQ(0, radians(0).value());
  EXPECT_DOUBLE_EQ(-3.1415926535897931, radians(-180).value());
  EXPECT_DOUBLE_EQ(-1.0000736613927508, radians(-57.3).value());
}

TEST_F(ArithmeticTest, degrees) {
  const auto degrees = [&](std::optional<double> a) {
    return evaluateOnce<double>("degrees(c0)", a);
  };

  EXPECT_EQ(std::nullopt, degrees(std::nullopt));
  EXPECT_DOUBLE_EQ(57.295779513082323, degrees(1).value());
  EXPECT_DOUBLE_EQ(179.90874767107849, degrees(3.14).value());
  EXPECT_DOUBLE_EQ(kInf, degrees(kInf).value());
  EXPECT_DOUBLE_EQ(0, degrees(0).value());
  EXPECT_DOUBLE_EQ(-57.295779513082323, degrees(-1).value());
  EXPECT_DOUBLE_EQ(-179.90874767107849, degrees(-3.14).value());
  EXPECT_DOUBLE_EQ(-kInf, degrees(-kInf).value());
  EXPECT_DOUBLE_EQ(
      1.2748734119735194e-306,
      degrees(std::numeric_limits<double>::min()).value());
  EXPECT_DOUBLE_EQ(kInf, degrees(std::numeric_limits<double>::max()).value());
}

TEST_F(ArithmeticTest, signFloatingPoint) {
  const auto sign = [&](std::optional<double> a) {
    return evaluateOnce<double>("sign(c0)", a);
  };

  EXPECT_FLOAT_EQ(0.0, sign(0.0).value_or(-1));
  EXPECT_FLOAT_EQ(1.0, sign(10.1).value_or(-1));
  EXPECT_FLOAT_EQ(-1.0, sign(-10.1).value_or(1));
  EXPECT_FLOAT_EQ(1.0, sign(kInf).value_or(-1));
  EXPECT_FLOAT_EQ(-1.0, sign(-kInf).value_or(1));
  EXPECT_THAT(sign(kNan), IsNan());
}

TEST_F(ArithmeticTest, signIntegral) {
  const auto sign = [&](std::optional<int64_t> a) {
    return evaluateOnce<int64_t>("sign(c0)", a);
  };

  EXPECT_EQ(0, sign(0));
  EXPECT_EQ(1, sign(10));
  EXPECT_EQ(-1, sign(-10));
}

TEST_F(ArithmeticTest, infinity) {
  const auto infinity = [&]() {
    return evaluateOnce<double>("infinity()", makeRowVector(ROW({}), 1));
  };

  EXPECT_EQ(kInf, infinity());
}

TEST_F(ArithmeticTest, isFinite) {
  const auto isFinite = [&](std::optional<double> a) {
    return evaluateOnce<bool>("is_finite(c0)", a);
  };

  EXPECT_EQ(true, isFinite(0.0));
  EXPECT_EQ(false, isFinite(kInf));
  EXPECT_EQ(false, isFinite(-kInf));
  EXPECT_EQ(false, isFinite(1.0 / 0.0));
  EXPECT_EQ(false, isFinite(-1.0 / 0.0));
  EXPECT_EQ(false, isFinite(kNan));
}

TEST_F(ArithmeticTest, isInfinite) {
  const auto isInfinite = [&](std::optional<double> a) {
    return evaluateOnce<bool>("is_infinite(c0)", a);
  };

  EXPECT_EQ(false, isInfinite(0.0));
  EXPECT_EQ(false, isInfinite(kNan));
  EXPECT_EQ(true, isInfinite(kInf));
  EXPECT_EQ(true, isInfinite(-kInf));
  EXPECT_EQ(true, isInfinite(1.0 / 0.0));
  EXPECT_EQ(true, isInfinite(-1.0 / 0.0));
}

TEST_F(ArithmeticTest, isNan) {
  const auto isNan = [&](std::optional<double> a) {
    return evaluateOnce<bool>("is_nan(c0)", a);
  };

  EXPECT_EQ(false, isNan(0.0));
  EXPECT_EQ(true, isNan(kNan));
  EXPECT_EQ(true, isNan(0.0 / 0.0));
}

TEST_F(ArithmeticTest, nan) {
  const auto nan = [&]() {
    return evaluateOnce<double>("nan()", makeRowVector(ROW({}), 1));
  };

  EXPECT_EQ(true, std::isnan(nan().value()));
}

TEST_F(ArithmeticTest, fromBase) {
  const auto fromBase = [&](const std::optional<StringView>& a,
                            std::optional<int64_t> b) {
    return evaluateOnce<int64_t>("from_base(c0, c1)", a, b);
  };

  EXPECT_EQ(12, fromBase("12"_sv, 10));
  EXPECT_EQ(12, fromBase("+12"_sv, 10));
  EXPECT_EQ(-12, fromBase("-12"_sv, 10));
  EXPECT_EQ(26, fromBase("1a"_sv, 16));
  EXPECT_EQ(3, fromBase("11"_sv, 2));
  EXPECT_EQ(71, fromBase("1z"_sv, 36));
  EXPECT_EQ(
      9223372036854775807,
      fromBase(
          "111111111111111111111111111111111111111111111111111111111111111"_sv,
          2));

  assertError<StringView, int64_t, int64_t>(
      "from_base(c0, c1)", {"0"_sv}, {1}, "Radix must be between 2 and 36.");
  assertError<StringView, int64_t, int64_t>(
      "from_base(c0, c1)", {"0"_sv}, {37}, "Radix must be between 2 and 36.");
  assertError<StringView, int64_t, int64_t>(
      "from_base(c0, c1)",
      {"0x12"_sv},
      {16},
      "Not a valid base-16 number: 0x12.");
  assertError<StringView, int64_t, int64_t>(
      "from_base(c0, c1)", {""_sv}, {10}, "Not a valid base-10 number: .");
  assertError<StringView, int64_t, int64_t>(
      "from_base(c0, c1)",
      {" 12"_sv},
      {16},
      "Not a valid base-16 number:  12.");
  assertError<StringView, int64_t, int64_t>(
      "from_base(c0, c1)",
      {"123xy"_sv},
      {10},
      "Not a valid base-10 number: 123xy.");
  assertError<StringView, int64_t, int64_t>(
      "from_base(c0, c1)",
      {"123456789012xy"_sv},
      {10},
      "Not a valid base-10 number: 123456789012xy.");
  assertError<StringView, int64_t, int64_t>(
      "from_base(c0, c1)",
      {"abc12"_sv},
      {10},
      "Not a valid base-10 number: abc12.");
  assertError<StringView, int64_t, int64_t>(
      "from_base(c0, c1)",
      {"9223372036854775808"_sv},
      {10},
      "9223372036854775808 is out of range.");
  assertError<StringView, int64_t, int64_t>(
      "from_base(c0, c1)",
      {"1111111111111111111111111111111111111111111111111111111111111111111111"_sv},
      {2},
      "1111111111111111111111111111111111111111111111111111111111111111111111 is out of range.");
}

TEST_F(ArithmeticTest, toBase) {
  const auto to_base = [&](std::optional<int64_t> value,
                           std::optional<int64_t> radix) {
    auto valueVector = makeNullableFlatVector<int64_t>(
        std::vector<std::optional<int64_t>>{value});
    auto radixVector = makeNullableFlatVector<int64_t>(
        std::vector<std::optional<int64_t>>{radix});
    auto result = evaluate<SimpleVector<StringView>>(
        "to_base(c0, c1)", makeRowVector({valueVector, radixVector}));
    return result->valueAt(0).getString();
  };

  for (auto i = 0; i < 10; i++) {
    std::string expected(1, (char)((int)'0' + i));
    EXPECT_EQ(expected, to_base(i, 36));
  }

  for (auto i = 10; i < 36; i++) {
    std::string expected(1, (char)((int)'a' + i - 10));
    EXPECT_EQ(expected, to_base(i, 36));
  }

  EXPECT_EQ("110111111010", to_base(3578, 2));
  EXPECT_EQ("313322", to_base(3578, 4));
  EXPECT_EQ("6772", to_base(3578, 8));
  EXPECT_EQ("20a2", to_base(3578, 12));
  EXPECT_EQ("-20a2", to_base(-3578, 12));

  EXPECT_EQ(
      "-1104332401304422434310311213",
      to_base(std::numeric_limits<int64_t>::min(), 5));
  EXPECT_EQ(
      "1104332401304422434310311212",
      to_base(std::numeric_limits<int64_t>::max(), 5));
  ASSERT_THROW(to_base(1, 37), velox::VeloxUserError);
}

TEST_F(ArithmeticTest, pi) {
  const auto piValue = [&]() {
    return evaluateOnce<double>("pi()", makeRowVector(ROW({}), 1));
  };

  EXPECT_EQ(piValue(), M_PI);
}

TEST_F(ArithmeticTest, e) {
  const auto eulerConstantValue = [&]() {
    return evaluateOnce<double>("e()", makeRowVector(ROW({}), 1));
  };

  EXPECT_EQ(eulerConstantValue(), M_E);
}

TEST_F(ArithmeticTest, clamp) {
  const auto clamp = [&](std::optional<int64_t> v,
                         std::optional<int64_t> lo,
                         std::optional<int64_t> hi) {
    return evaluateOnce<int64_t>("clamp(c0, c1, c2)", v, lo, hi);
  };

  // lo < v < hi => v.
  EXPECT_EQ(0, clamp(0, -1, 1));
  // v < lo => lo.
  EXPECT_EQ(-1, clamp(-2, -1, 1));
  // v > hi => hi.
  EXPECT_EQ(1, clamp(2, -1, 1));
  // v == lo => v.
  EXPECT_EQ(-1, clamp(-1, -1, 1));
  // v == hi => v.
  EXPECT_EQ(1, clamp(1, -1, 1));
  // lo == hi != v => lo.
  EXPECT_EQ(-1, clamp(2, -1, -1));
  // lo == hi == v => v.
  EXPECT_EQ(-1, clamp(-1, -1, -1));

  // lo > hi -> hi.
  EXPECT_EQ(clamp(-123, 1, -1), -1);
  EXPECT_EQ(clamp(-1, 1, -1), -1);
  EXPECT_EQ(clamp(0, 1, -1), -1);
  EXPECT_EQ(clamp(1, 1, -1), -1);
  EXPECT_EQ(clamp(2, 1, -1), -1);
  EXPECT_EQ(clamp(123456, 1, -1), -1);
}

TEST_F(ArithmeticTest, truncateDouble) {
  const auto truncate = [&](std::optional<double> d) {
    const auto r = evaluateOnce<double>("truncate(c0)", d);

    // truncate(d) == truncate(d, 0)
    if (d.has_value() && std::isfinite(d.value())) {
      const auto otherResult =
          evaluateOnce<double>("truncate(c0, 0::integer)", d);

      VELOX_CHECK_EQ(r.value(), otherResult.value());
    }

    return r;
  };

  const auto truncateN = [&](std::optional<double> d,
                             std::optional<int32_t> n) {
    return evaluateOnce<double>("truncate(c0, c1)", d, n);
  };

  EXPECT_EQ(truncate(0), 0);
  EXPECT_EQ(truncate(1.5), 1);
  EXPECT_EQ(truncate(-1.5), -1);
  EXPECT_EQ(truncate(std::nullopt), std::nullopt);
  EXPECT_THAT(truncate(kNan), IsNan());
  EXPECT_THAT(truncate(kInf), IsInf());

  EXPECT_EQ(truncate(0), 0);
  EXPECT_EQ(truncate(1.5), 1);
  EXPECT_EQ(truncate(-1.5), -1);
  EXPECT_EQ(truncate(std::nullopt), std::nullopt);
  EXPECT_THAT(truncate(kNan), IsNan());
  EXPECT_THAT(truncate(kInf), IsInf());

  EXPECT_EQ(truncateN(1.5, std::nullopt), std::nullopt);
  EXPECT_THAT(truncateN(kNan, 1), IsNan());
  EXPECT_THAT(truncateN(kInf, 1), IsInf());

  EXPECT_DOUBLE_EQ(truncateN(1.5678, 2).value(), 1.56);
  EXPECT_DOUBLE_EQ(truncateN(-1.5678, 2).value(), -1.56);
  EXPECT_DOUBLE_EQ(truncateN(1.333, -1).value(), 0);
  EXPECT_DOUBLE_EQ(truncateN(3.54555, 2).value(), 3.54);
  EXPECT_DOUBLE_EQ(truncateN(1234, 1).value(), 1234);
  EXPECT_DOUBLE_EQ(truncateN(1234, -1).value(), 1230);
  EXPECT_DOUBLE_EQ(truncateN(1234.56, 1).value(), 1234.5);
  EXPECT_DOUBLE_EQ(truncateN(1234.56, -1).value(), 1230.0);
  EXPECT_DOUBLE_EQ(truncateN(1239.999, 2).value(), 1239.99);
  EXPECT_DOUBLE_EQ(truncateN(1239.999, -2).value(), 1200.0);
  EXPECT_DOUBLE_EQ(
      truncateN(123456789012345678901.23, 3).value(), 123456789012345678901.23);
  EXPECT_DOUBLE_EQ(
      truncateN(-123456789012345678901.23, 3).value(),
      -123456789012345678901.23);
  EXPECT_DOUBLE_EQ(
      truncateN(123456789123456.999, 2).value(), 123456789123456.99);
  EXPECT_DOUBLE_EQ(truncateN(123456789012345678901.0, -21).value(), 0.0);
  EXPECT_DOUBLE_EQ(truncateN(123456789012345678901.23, -21).value(), 0.0);
  EXPECT_DOUBLE_EQ(truncateN(123456789012345678901.0, -21).value(), 0.0);
  EXPECT_DOUBLE_EQ(truncateN(123456789012345678901.23, -21).value(), 0.0);
}

TEST_F(ArithmeticTest, truncateReal) {
  const auto truncate = [&](std::optional<float> d) {
    const auto r = evaluateOnce<float>("truncate(c0)", d);

    // truncate(d) == truncate(d, 0)
    if (d.has_value() && std::isfinite(d.value())) {
      const auto otherResult =
          evaluateOnce<float>("truncate(c0, 0::integer)", d);

      VELOX_CHECK_EQ(r.value(), otherResult.value());
    }

    return r;
  };

  const auto truncateN = [&](std::optional<float> d, std::optional<int32_t> n) {
    return evaluateOnce<float>("truncate(c0, c1)", d, n);
  };

  EXPECT_EQ(truncate(0), 0);
  EXPECT_EQ(truncate(1.5), 1);
  EXPECT_EQ(truncate(-1.5), -1);

  EXPECT_EQ(truncate(std::nullopt), std::nullopt);
  EXPECT_THAT(truncate(kNan), IsNan());
  EXPECT_THAT(truncate(kInf), IsInf());

  EXPECT_FLOAT_EQ(truncateN(123.456, 0).value(), 123);
  EXPECT_FLOAT_EQ(truncateN(123.456, 1).value(), 123.4);
  EXPECT_FLOAT_EQ(truncateN(123.456, 2).value(), 123.45);
  EXPECT_FLOAT_EQ(truncateN(123.456, 3).value(), 123.456);
  EXPECT_FLOAT_EQ(truncateN(123.456, 4).value(), 123.456);

  EXPECT_FLOAT_EQ(truncateN(123.456, -1).value(), 120);
  EXPECT_FLOAT_EQ(truncateN(123.456, -2).value(), 100);
  EXPECT_FLOAT_EQ(truncateN(123.456, -3).value(), 0);

  EXPECT_FLOAT_EQ(truncateN(-123.456, 0).value(), -123);
  EXPECT_FLOAT_EQ(truncateN(-123.456, 1).value(), -123.4);
  EXPECT_FLOAT_EQ(truncateN(-123.456, 2).value(), -123.45);
  EXPECT_FLOAT_EQ(truncateN(-123.456, 3).value(), -123.456);
  EXPECT_FLOAT_EQ(truncateN(-123.456, 4).value(), -123.456);

  EXPECT_FLOAT_EQ(truncateN(-123.456, -1).value(), -120);
  EXPECT_FLOAT_EQ(truncateN(-123.456, -2).value(), -100);
  EXPECT_FLOAT_EQ(truncateN(-123.456, -3).value(), 0);
}

TEST_F(ArithmeticTest, wilsonIntervalLower) {
  const auto wilsonIntervalLower = [&](std::optional<int64_t> s,
                                       std::optional<int64_t> n,
                                       std::optional<double> z) {
    return evaluateOnce<double>("wilson_interval_lower(c0,c1,c2)", s, n, z);
  };

  // Verify that bounds checking is working.
  VELOX_ASSERT_THROW(
      wilsonIntervalLower(-1, -1, -1),
      "number of successes must not be negative");
  VELOX_ASSERT_THROW(
      wilsonIntervalLower(0, -1, -1), "number of trials must be positive");
  VELOX_ASSERT_THROW(
      wilsonIntervalLower(0, 0, -1), "number of trials must be positive");
  VELOX_ASSERT_THROW(
      wilsonIntervalLower(0, 1, -1), "z-score must not be negative");
  EXPECT_DOUBLE_EQ(wilsonIntervalLower(0, 1, 0).value(), 0.0);
  VELOX_ASSERT_THROW(
      wilsonIntervalLower(2, 1, 0),
      "number of successes must not be larger than number of trials")

  // Verify correctness on simple inputs.
  EXPECT_DOUBLE_EQ(wilsonIntervalLower(3, 5, 0.5).value(), 0.48822759497978935);
  EXPECT_DOUBLE_EQ(wilsonIntervalLower(2, 10, 1).value(), 0.10362299537513234);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalLower(7, 14, 1.6).value(), 0.30341072512680384);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalLower(1250, 1310, 1.96).value(), 0.9414883725395894);

  // Verify correctness on extreme inputs.
  constexpr int64_t max64 = std::numeric_limits<int64_t>::max();
  EXPECT_DOUBLE_EQ(wilsonIntervalLower(max64, max64, 1.6).value(), 1.0);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalLower(max64 / 10, max64, 0.4).value(), 0.09999999996048733);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalLower(max64 / 10, max64, 1e10).value(),
      9.065125579912648e-4);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalLower(max64 / 10, max64, 1e150).value(),
      9.223372036854775e-284);
  EXPECT_DOUBLE_EQ(wilsonIntervalLower(max64 / 10, max64, 1e300).value(), 0.0);
  EXPECT_DOUBLE_EQ(wilsonIntervalLower(max64 / 10, max64, 1e-10).value(), 0.1);
  EXPECT_DOUBLE_EQ(wilsonIntervalLower(max64 / 10, max64, 0).value(), 0.1);
  EXPECT_DOUBLE_EQ(wilsonIntervalLower(0, max64, 1.2).value(), 0.0);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalLower(3, max64, 0.02).value(), 3.2152648669633817e-19);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalLower(3, max64, 10.2).value(), 8.874121192596711e-21);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalLower(3, 3, 10.2).value(), 0.028026905829596414);

  // Verify correctness on nan, inf.
  VELOX_ASSERT_THROW(
      wilsonIntervalLower(1, 3, kNan), "z-score must not be negative");
  VELOX_ASSERT_THROW(
      wilsonIntervalLower(1, 3, -kInf), "z-score must not be negative");
  EXPECT_DOUBLE_EQ(wilsonIntervalLower(1, 3, kInf).value(), 0.0);
}

TEST_F(ArithmeticTest, wilsonIntervalUpper) {
  const auto wilsonIntervalUpper = [&](std::optional<int64_t> s,
                                       std::optional<int64_t> n,
                                       std::optional<double> z) {
    return evaluateOnce<double>("wilson_interval_upper(c0,c1,c2)", s, n, z);
  };

  // Verify that bounds checking is working.
  VELOX_ASSERT_THROW(
      wilsonIntervalUpper(-1, -1, -1),
      "number of successes must not be negative");
  VELOX_ASSERT_THROW(
      wilsonIntervalUpper(0, -1, -1), "number of trials must be positive");
  VELOX_ASSERT_THROW(
      wilsonIntervalUpper(0, 0, -1), "number of trials must be positive");
  VELOX_ASSERT_THROW(
      wilsonIntervalUpper(0, 1, -1), "z-score must not be negative");
  EXPECT_DOUBLE_EQ(wilsonIntervalUpper(0, 1, 0).value(), 0.0);
  VELOX_ASSERT_THROW(
      wilsonIntervalUpper(2, 1, 0),
      "number of successes must not be larger than number of trials")

  // Verify correctness on simple inputs.
  EXPECT_DOUBLE_EQ(wilsonIntervalUpper(3, 5, 0.5).value(), 0.7022485954964011);
  EXPECT_DOUBLE_EQ(wilsonIntervalUpper(2, 10, 1).value(), 0.3509224591703222);
  EXPECT_DOUBLE_EQ(wilsonIntervalUpper(7, 14, 1.6).value(), 0.6965892748731962);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalUpper(1250, 1310, 1.96).value(), 0.9642524717143908);

  // Verify correctness on extreme inputs.
  constexpr int64_t max64 = std::numeric_limits<int64_t>::max();
  EXPECT_DOUBLE_EQ(wilsonIntervalUpper(max64, max64, 1.6).value(), 1.0);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalUpper(max64 / 10, max64, 0.4).value(), 0.10000000003951268);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalUpper(max64 / 10, max64, 1e10).value(), 0.931537455322857);
  EXPECT_DOUBLE_EQ(wilsonIntervalUpper(max64 / 10, max64, 1e150).value(), 1.0);
  EXPECT_DOUBLE_EQ(wilsonIntervalUpper(max64 / 10, max64, 1e300).value(), 1.0);
  EXPECT_DOUBLE_EQ(wilsonIntervalUpper(max64 / 10, max64, 1e-10).value(), 0.1);
  EXPECT_DOUBLE_EQ(wilsonIntervalUpper(max64 / 10, max64, 0).value(), 0.1);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalUpper(0, max64, 1.2).value(), 1.5612511283791263e-19);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalUpper(3, max64, 0.02).value(), 3.290381848818639e-19);
  EXPECT_DOUBLE_EQ(
      wilsonIntervalUpper(3, max64, 10.2).value(), 1.1921686584837893e-17);
  EXPECT_DOUBLE_EQ(wilsonIntervalUpper(3, 3, 10.2).value(), 1.0);

  // Verify correctness on nan, inf.
  VELOX_ASSERT_THROW(
      wilsonIntervalUpper(1, 3, kNan), "z-score must not be negative");
  VELOX_ASSERT_THROW(
      wilsonIntervalUpper(1, 3, -kInf), "z-score must not be negative");
  EXPECT_DOUBLE_EQ(wilsonIntervalUpper(1, 3, kInf).value(), 1.0);
}

TEST_F(ArithmeticTest, cosineSimilarity) {
  const auto cosineSimilarity =
      [&](const std::vector<std::pair<std::string, std::optional<double>>>&
              left,
          const std::vector<std::pair<std::string, std::optional<double>>>&
              right) {
        auto leftMap = makeMapVector<std::string, double>({left});
        auto rightMap = makeMapVector<std::string, double>({right});
        return evaluateOnce<double>(
                   "cosine_similarity(c0,c1)",
                   makeRowVector({leftMap, rightMap}))
            .value();
      };

  EXPECT_DOUBLE_EQ(
      (2.0 * 3.0) / (std::sqrt(5.0) * std::sqrt(10.0)),
      cosineSimilarity({{"a", 1}, {"b", 2}}, {{"c", 1}, {"b", 3}}));

  EXPECT_DOUBLE_EQ(
      (2.0 * 3.0 + (-1) * 1) / (std::sqrt(1 + 4 + 1) * std::sqrt(1 + 9)),
      cosineSimilarity({{"a", 1}, {"b", 2}, {"c", -1}}, {{"c", 1}, {"b", 3}}));

  EXPECT_DOUBLE_EQ(
      (2.0 * 3.0 + (-1) * 1) / (std::sqrt(1 + 4 + 1) * std::sqrt(1 + 9)),
      cosineSimilarity({{"a", 1}, {"b", 2}, {"c", -1}}, {{"c", 1}, {"b", 3}}));

  EXPECT_DOUBLE_EQ(
      0.0,
      cosineSimilarity({{"a", 1}, {"b", 2}, {"c", -1}}, {{"d", 1}, {"e", 3}}));

  EXPECT_TRUE(std::isnan(cosineSimilarity({}, {})));
  EXPECT_TRUE(std::isnan(cosineSimilarity({{"d", 1}, {"e", 3}}, {})));
  EXPECT_TRUE(
      std::isnan(cosineSimilarity({{"a", 1}, {"b", 3}}, {{"a", 0}, {"b", 0}})));

  auto nullableLeftMap = makeNullableMapVector<StringView, double>(
      {{{{"a"_sv, 1}, {"b"_sv, std::nullopt}}}});
  auto rightMap =
      makeMapVector<StringView, double>({{{{"c"_sv, 1}, {"b"_sv, 3}}}});

  EXPECT_FALSE(evaluateOnce<double>(
                   "cosine_similarity(c0,c1)",
                   makeRowVector({nullableLeftMap, rightMap}))
                   .has_value());
}

} // namespace
} // namespace facebook::velox
