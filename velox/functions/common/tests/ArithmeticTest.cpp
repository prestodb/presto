/*
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
#include "velox/functions/common/tests/FunctionBaseTest.h"

using namespace facebook::velox;

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

  template <typename T>
  void assertError(
      const std::string& expression,
      const std::vector<T>& arg0,
      const std::vector<T>& arg1,
      const std::string& errorMessage) {
    auto vector0 = makeFlatVector(arg0);
    auto vector1 = makeFlatVector(arg1);

    try {
      evaluate<SimpleVector<T>>(expression, makeRowVector({vector0, vector1}));
      ASSERT_TRUE(false) << "Expected an error";
    } catch (const std::exception& e) {
      ASSERT_TRUE(
          std::string(e.what()).find(errorMessage) != std::string::npos);
    }
  }
};

TEST_F(ArithmeticTest, divide)
#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
__attribute__((__no_sanitize__("float-divide-by-zero")))
#endif
#endif
{
  assertExpression<int32_t>("c0 / c1", {10, 11, -34}, {2, 2, 10}, {5, 5, -3});
  assertExpression<int64_t>("c0 / c1", {10, 11, -34}, {2, 2, 10}, {5, 5, -3});

  assertError<int32_t>("c0 / c1", {10}, {0}, "division by zero");
  assertError<int32_t>("c0 / c1", {0}, {0}, "division by zero");

  float nan = std::nanf("");
  float inf = std::numeric_limits<float>::infinity();
  assertExpression<float>(
      "c0 / c1", {10.5, 9.2, 0.0}, {2, 0, 0}, {5.25, inf, nan});
  assertExpression<double>(
      "c0 / c1", {10.5, 9.2, 0.0}, {2, 0, 0}, {5.25, inf, nan});
}

TEST_F(ArithmeticTest, power) {
  float inf = std::numeric_limits<float>::infinity();

  // Doubles as input.
  std::vector<double> baseDouble = {
      0, 0, 0, -1, -1, -1, -9, 9.1, 10.1, 11.1, -11.1, 0, inf, inf};
  std::vector<double> exponentDouble = {
      0, 1, -1, 0, 1, -1, -3.3, 123456.432, -99.9, 0, 100000, inf, 0, inf};
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

  // Integers as input.
  std::vector<int64_t> baseInt = {9, 10, 11, -9, -10, -11, 0};
  std::vector<int64_t> exponentInt = {3, -3, 0, -1, 199999, 77, 0};
  std::vector<double> expectedInt;
  expectedInt.reserve(baseDouble.size());

  for (size_t i = 0; i < baseInt.size(); i++) {
    expectedInt.emplace_back(pow(baseInt[i], exponentInt[i]));
  }

  assertExpression<int64_t, double>(
      "power(c0, c1)", baseInt, exponentInt, expectedInt);
  assertExpression<int64_t, double>(
      "pow(c0, c1)", baseInt, exponentInt, expectedInt);
}

TEST_F(ArithmeticTest, exp) {
  constexpr double kInf = std::numeric_limits<double>::infinity();
  constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
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
  EXPECT_TRUE(std::isnan(exp(kNan).value_or(-1)));
}

TEST_F(ArithmeticTest, ln) {
  constexpr double kInf = std::numeric_limits<double>::infinity();
  constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
  const double kE = std::exp(1);

  const auto ln = [&](std::optional<double> a) {
    return evaluateOnce<double>("ln(c0)", a);
  };

  EXPECT_EQ(0, ln(1));
  EXPECT_EQ(1, ln(kE));
  EXPECT_EQ(-kInf, ln(0));
  EXPECT_TRUE(std::isnan(ln(-1).value_or(-1)));
  EXPECT_TRUE(std::isnan(ln(kNan).value_or(-1)));
  EXPECT_EQ(kInf, ln(kInf));
  EXPECT_EQ(std::nullopt, ln(std::nullopt));
}

TEST_F(ArithmeticTest, sqrt) {
  constexpr double kDoubleMax = std::numeric_limits<double>::max();
  const double kNan = std::numeric_limits<double>::quiet_NaN();

  const auto sqrt = [&](std::optional<double> a) {
    return evaluateOnce<double>("sqrt(c0)", a);
  };

  EXPECT_EQ(1.0, sqrt(1));
  EXPECT_TRUE(std::isnan(sqrt(-1.0).value_or(-1)));
  EXPECT_EQ(0, sqrt(0));

  EXPECT_EQ(2, sqrt(4));
  EXPECT_EQ(3, sqrt(9));
  EXPECT_FLOAT_EQ(1.34078e+154, sqrt(kDoubleMax).value_or(-1));
  EXPECT_EQ(std::nullopt, sqrt(std::nullopt));
  EXPECT_TRUE(std::isnan(sqrt(kNan).value_or(-1)));
}

TEST_F(ArithmeticTest, cbrt) {
  constexpr double kDoubleMax = std::numeric_limits<double>::max();
  const double kNan = std::numeric_limits<double>::quiet_NaN();

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
  EXPECT_TRUE(std::isnan(cbrt(kNan).value_or(-1)));
}

TEST_F(ArithmeticTest, widthBucket) {
  constexpr double kInf = std::numeric_limits<double>::infinity();
  constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
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
  assertUserInvalidArgument(
      [&]() { widthBucket(3.14, 0, 4, 0); },
      "bucketCount must be greater than 0");
  assertUserInvalidArgument(
      [&]() { widthBucket(kNan, 0, 4, 10); }, "operand must not be NaN");
  assertUserInvalidArgument(
      [&]() { widthBucket(3.14, kNan, 0, 10); }, "first bound must be finite");
  assertUserInvalidArgument(
      [&]() { widthBucket(3.14, kInf, 0, 10); }, "first bound must be finite");
  assertUserInvalidArgument(
      [&]() { widthBucket(3.14, 0, kNan, 10); }, "second bound must be finite");
  assertUserInvalidArgument(
      [&]() { widthBucket(3.14, 0, kInf, 10); }, "second bound must be finite");
  assertUserInvalidArgument(
      [&]() { widthBucket(3.14, 0, 0, 10); }, "bounds cannot equal each other");
  assertUserInvalidArgument(
      [&]() { widthBucket(kInf, 0, 4, kMaxInt64); },
      "Bucket for value inf is out of range");
  assertUserInvalidArgument(
      [&]() { widthBucket(kInf, 4, 0, kMaxInt64); },
      "Bucket for value inf is out of range");
}

TEST_F(ArithmeticTest, bitwiseAnd) {
  const auto bitwiseAnd = [&](std::optional<int32_t> a,
                              std::optional<int32_t> b) {
    return evaluateOnce<int32_t>("bitwise_and(c0, c1)", a, b);
  };

  EXPECT_EQ(bitwiseAnd(0, -1), 0);
  EXPECT_EQ(bitwiseAnd(3, 8), 0);
  EXPECT_EQ(bitwiseAnd(-4, 12), 12);
  EXPECT_EQ(bitwiseAnd(60, 21), 20);
}

TEST_F(ArithmeticTest, bitwiseNot) {
  const auto bitwiseNot = [&](std::optional<int32_t> a) {
    return evaluateOnce<int32_t>("bitwise_not(c0)", a);
  };

  EXPECT_EQ(bitwiseNot(-1), 0);
  EXPECT_EQ(bitwiseNot(0), -1);
  EXPECT_EQ(bitwiseNot(2), -3);
}

TEST_F(ArithmeticTest, bitwiseOr) {
  const auto bitwiseOr = [&](std::optional<int32_t> a,
                             std::optional<int32_t> b) {
    return evaluateOnce<int32_t>("bitwise_or(c0, c1)", a, b);
  };

  EXPECT_EQ(bitwiseOr(0, -1), -1);
  EXPECT_EQ(bitwiseOr(3, 8), 11);
  EXPECT_EQ(bitwiseOr(-4, 12), -4);
  EXPECT_EQ(bitwiseOr(60, 21), 61);
}

TEST_F(ArithmeticTest, bitwiseXor) {
  const auto bitwiseXor = [&](std::optional<int32_t> a,
                              std::optional<int32_t> b) {
    return evaluateOnce<int32_t>("bitwise_xor(c0, c1)", a, b);
  };

  EXPECT_EQ(bitwiseXor(0, -1), -1);
  EXPECT_EQ(bitwiseXor(3, 8), 11);
  EXPECT_EQ(bitwiseXor(-4, 12), -16);
  EXPECT_EQ(bitwiseXor(60, 21), 41);
}
