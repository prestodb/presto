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
#include <optional>

#include <gmock/gmock.h>

#include <velox/common/base/VeloxException.h>
#include <velox/vector/SimpleVector.h>
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox {
namespace {

constexpr double kInf = std::numeric_limits<double>::infinity();
constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
constexpr float kInfF = std::numeric_limits<float>::infinity();
constexpr float kNanF = std::numeric_limits<float>::quiet_NaN();

MATCHER(IsNan, "is NaN") {
  return arg && std::isnan(*arg);
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

  assertExpression<float>(
      "c0 / c1", {10.5, 9.2, 0.0}, {2, 0, 0}, {5.25, kInfF, kNanF});
  assertExpression<double>(
      "c0 / c1", {10.5, 9.2, 0.0}, {2, 0, 0}, {5.25, kInf, kNan});
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
  std::vector<int64_t> numerInt = {9, 10, 0, -9, -10, -11};
  std::vector<int64_t> denomInt = {3, -3, 11, -1, 199999, 77};
  std::vector<int64_t> expectedInt = {0, 1, 0, 0, -10, -11};

  assertExpression<int64_t, int64_t>(
      "mod(c0, c1)", numerInt, denomInt, expectedInt);
  assertError<int64_t>("mod(c0, c1)", {10}, {0}, "Cannot divide by 0");
}

TEST_F(ArithmeticTest, power) {
  std::vector<double> baseDouble = {
      0, 0, 0, -1, -1, -1, -9, 9.1, 10.1, 11.1, -11.1, 0, kInf, kInf};
  std::vector<double> exponentDouble = {
      0, 1, -1, 0, 1, -1, -3.3, 123456.432, -99.9, 0, 100000, kInf, 0, kInf};
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
}

TEST_F(ArithmeticTest, isInfinite) {
  const auto isInfinite = [&](std::optional<double> a) {
    return evaluateOnce<bool>("is_infinite(c0)", a);
  };

  EXPECT_EQ(false, isInfinite(0.0));
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

} // namespace
} // namespace facebook::velox
