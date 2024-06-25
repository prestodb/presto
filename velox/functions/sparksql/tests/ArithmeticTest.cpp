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
#include <cstdint>
#include <limits>

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class PmodTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> pmod(std::optional<T> a, std::optional<T> n) {
    return evaluateOnce<T>("pmod(c0, c1)", a, n);
  };
};

TEST_F(PmodTest, int8) {
  EXPECT_EQ(1, pmod<int8_t>(1, 3));
  EXPECT_EQ(2, pmod<int8_t>(-1, 3));
  EXPECT_EQ(1, pmod<int8_t>(3, -2));
  EXPECT_EQ(-1, pmod<int8_t>(-1, -3));
  EXPECT_EQ(std::nullopt, pmod<int8_t>(1, 0));
  EXPECT_EQ(std::nullopt, pmod<int8_t>(std::nullopt, 3));
  EXPECT_EQ(INT8_MAX, pmod<int8_t>(INT8_MAX, INT8_MIN));
  EXPECT_EQ(INT8_MAX - 1, pmod<int8_t>(INT8_MIN, INT8_MAX));
  EXPECT_EQ(0, pmod<int64_t>(INT8_MIN, -1));
}

TEST_F(PmodTest, int16) {
  EXPECT_EQ(0, pmod<int16_t>(23286, 3881));
  EXPECT_EQ(1026, pmod<int16_t>(-15892, 8459));
  EXPECT_EQ(127, pmod<int16_t>(7849, -297));
  EXPECT_EQ(-8, pmod<int16_t>(-1052, -12));
  EXPECT_EQ(INT16_MAX, pmod<int16_t>(INT16_MAX, INT16_MIN));
  EXPECT_EQ(INT16_MAX - 1, pmod<int16_t>(INT16_MIN, INT16_MAX));
  EXPECT_EQ(0, pmod<int64_t>(INT16_MIN, -1));
}

TEST_F(PmodTest, int32) {
  EXPECT_EQ(2095, pmod<int32_t>(391819, 8292));
  EXPECT_EQ(8102, pmod<int32_t>(-16848948, 48163));
  EXPECT_EQ(726, pmod<int32_t>(2145613151, -925));
  EXPECT_EQ(-11, pmod<int32_t>(-15181535, -12));
  EXPECT_EQ(INT32_MAX, pmod<int32_t>(INT32_MAX, INT32_MIN));
  EXPECT_EQ(INT32_MAX - 1, pmod<int32_t>(INT32_MIN, INT32_MAX));
  EXPECT_EQ(0, pmod<int64_t>(INT32_MIN, -1));
}

TEST_F(PmodTest, int64) {
  EXPECT_EQ(0, pmod<int64_t>(4611791058295013614, 2147532562));
  EXPECT_EQ(10807, pmod<int64_t>(-3828032596, 48163));
  EXPECT_EQ(673, pmod<int64_t>(4293096798, -925));
  EXPECT_EQ(-5, pmod<int64_t>(-15181561541535, -23));
  EXPECT_EQ(INT64_MAX, pmod<int64_t>(INT64_MAX, INT64_MIN));
  EXPECT_EQ(INT64_MAX - 1, pmod<int64_t>(INT64_MIN, INT64_MAX));
  EXPECT_EQ(0, pmod<int64_t>(INT64_MIN, -1));
}

TEST_F(PmodTest, float) {
  EXPECT_FLOAT_EQ(0.2, pmod<float>(0.5, 0.3).value());
  EXPECT_FLOAT_EQ(0.9, pmod<float>(-1.1, 2).value());
  EXPECT_EQ(std::nullopt, pmod<float>(2.14159, 0.0));
  EXPECT_DOUBLE_EQ(0.1, pmod<double>(0.7, -0.3).value());
}

TEST_F(PmodTest, double) {
  EXPECT_DOUBLE_EQ(0.2, pmod<double>(0.5, 0.3).value());
  EXPECT_DOUBLE_EQ(0.9, pmod<double>(-1.1, 2).value());
  EXPECT_EQ(std::nullopt, pmod<double>(2.14159, 0.0));
  EXPECT_DOUBLE_EQ(0.1, pmod<double>(0.7, -0.3).value());
}

class RemainderTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> remainder(std::optional<T> a, std::optional<T> n) {
    return evaluateOnce<T>("remainder(c0, c1)", a, n);
  };
};

TEST_F(RemainderTest, int8) {
  EXPECT_EQ(1, remainder<int8_t>(1, 3));
  EXPECT_EQ(-1, remainder<int8_t>(-1, 3));
  EXPECT_EQ(1, remainder<int8_t>(3, -2));
  EXPECT_EQ(-1, remainder<int8_t>(-1, -3));
  EXPECT_EQ(std::nullopt, remainder<int8_t>(1, 0));
  EXPECT_EQ(std::nullopt, remainder<int8_t>(std::nullopt, 3));
  EXPECT_EQ(INT8_MAX, remainder<int8_t>(INT8_MAX, INT8_MIN));
  EXPECT_EQ(-1, remainder<int8_t>(INT8_MIN, INT8_MAX));
}

TEST_F(RemainderTest, int16) {
  EXPECT_EQ(0, remainder<int16_t>(23286, 3881));
  EXPECT_EQ(-7433, remainder<int16_t>(-15892, 8459));
  EXPECT_EQ(127, remainder<int16_t>(7849, -297));
  EXPECT_EQ(-8, remainder<int16_t>(-1052, -12));
  EXPECT_EQ(INT16_MAX, remainder<int16_t>(INT16_MAX, INT16_MIN));
  EXPECT_EQ(-1, remainder<int16_t>(INT16_MIN, INT16_MAX));
}

TEST_F(RemainderTest, int32) {
  EXPECT_EQ(2095, remainder<int32_t>(391819, 8292));
  EXPECT_EQ(-40061, remainder<int32_t>(-16848948, 48163));
  EXPECT_EQ(726, remainder<int32_t>(2145613151, -925));
  EXPECT_EQ(-11, remainder<int32_t>(-15181535, -12));
  EXPECT_EQ(INT32_MAX, remainder<int32_t>(INT32_MAX, INT32_MIN));
  EXPECT_EQ(-1, remainder<int32_t>(INT32_MIN, INT32_MAX));
  EXPECT_EQ(0, remainder<int32_t>(-15181535, -1));
  EXPECT_EQ(0, remainder<int32_t>(INT32_MIN, -1));
}

TEST_F(RemainderTest, int64) {
  EXPECT_EQ(0, remainder<int64_t>(4611791058295013614, 2147532562));
  EXPECT_EQ(-37356, remainder<int64_t>(-3828032596, 48163));
  EXPECT_EQ(673, remainder<int64_t>(4293096798, -925));
  EXPECT_EQ(-5, remainder<int64_t>(-15181561541535, -23));
  EXPECT_EQ(INT64_MAX, remainder<int64_t>(INT64_MAX, INT64_MIN));
  EXPECT_EQ(-1, remainder<int64_t>(INT64_MIN, INT64_MAX));
}

class ArithmeticTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> unaryminus(std::optional<T> arg) {
    return evaluateOnce<T>("unaryminus(c0)", arg);
  }

  std::optional<double> divide(
      std::optional<double> numerator,
      std::optional<double> denominator) {
    return evaluateOnce<double>("divide(c0, c1)", numerator, denominator);
  }

  template <typename T>
  std::optional<T> checkedAdd(
      const std::optional<T> a,
      const std::optional<T> b) {
    return evaluateOnce<T>("checked_add(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> checkedDivide(
      const std::optional<T> a,
      const std::optional<T> b) {
    return evaluateOnce<T>("checked_divide(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> checkedMultiply(
      const std::optional<T> a,
      const std::optional<T> b) {
    return evaluateOnce<T>("checked_multiply(c0, c1)", a, b);
  }

  template <typename T>
  std::optional<T> checkedSubtract(
      const std::optional<T> a,
      const std::optional<T> b) {
    return evaluateOnce<T>("checked_subtract(c0, c1)", a, b);
  }

  template <typename T>
  void assertErrorForCheckedArithmetic(
      const std::string& func,
      const std::optional<T> a,
      const std::optional<T> b,
      const std::string& errorMessage) {
    auto res = evaluateOnce<T>(fmt::format("try({}(c0, c1))", func), a, b);
    ASSERT_TRUE(!res.has_value());
    try {
      evaluateOnce<T>(fmt::format("{}(c0, c1)", func), a, b);
      FAIL() << "Expected an error";
    } catch (const std::exception& e) {
      ASSERT_TRUE(
          std::string(e.what()).find(errorMessage) != std::string::npos);
    }
  }

  template <typename T>
  void assertErrorForCheckedAdd(
      const std::optional<T> a,
      const std::optional<T> b,
      const std::string& errorMessage) {
    assertErrorForCheckedArithmetic("checked_add", a, b, errorMessage);
  }

  template <typename T>
  void assertErrorForCheckedDivide(
      const std::optional<T> a,
      const std::optional<T> b,
      const std::string& errorMessage) {
    assertErrorForCheckedArithmetic("checked_divide", a, b, errorMessage);
  }

  template <typename T>
  void assertErrorForCheckedMultiply(
      const std::optional<T> a,
      const std::optional<T> b,
      const std::string& errorMessage) {
    assertErrorForCheckedArithmetic("checked_multiply", a, b, errorMessage);
  }

  template <typename T>
  void assertErrorForcheckedSubtract(
      const std::optional<T> a,
      const std::optional<T> b,
      const std::string& errorMessage) {
    assertErrorForCheckedArithmetic("checked_subtract", a, b, errorMessage);
  }

  static constexpr float kNan = std::numeric_limits<float>::quiet_NaN();
  static constexpr double kNanDouble = std::numeric_limits<double>::quiet_NaN();
  static constexpr float kInf = std::numeric_limits<float>::infinity();
  static constexpr double kInfDouble = std::numeric_limits<double>::infinity();
};

TEST_F(ArithmeticTest, UnaryMinus) {
  EXPECT_EQ(unaryminus<int8_t>(1), -1);
  EXPECT_EQ(unaryminus<int16_t>(2), -2);
  EXPECT_EQ(unaryminus<int32_t>(3), -3);
  EXPECT_EQ(unaryminus<int64_t>(4), -4);
  EXPECT_EQ(unaryminus<float>(5), -5);
  EXPECT_EQ(unaryminus<double>(6), -6);
}

TEST_F(ArithmeticTest, UnaryMinusOverflow) {
  EXPECT_EQ(unaryminus<int8_t>(INT8_MIN), INT8_MIN);
  EXPECT_EQ(unaryminus<int16_t>(INT16_MIN), INT16_MIN);
  EXPECT_EQ(unaryminus<int32_t>(INT32_MIN), INT32_MIN);
  EXPECT_EQ(unaryminus<int64_t>(INT64_MIN), INT64_MIN);
  EXPECT_EQ(unaryminus<float>(-kInf), kInf);
  EXPECT_TRUE(std::isnan(unaryminus<float>(kNan).value_or(0)));
  EXPECT_EQ(unaryminus<double>(-kInf), kInf);
  EXPECT_TRUE(std::isnan(unaryminus<double>(kNan).value_or(0)));
}

TEST_F(ArithmeticTest, Divide) {
  // Null cases.
  EXPECT_EQ(divide(std::nullopt, std::nullopt), std::nullopt);
  EXPECT_EQ(divide(std::nullopt, 1), std::nullopt);
  EXPECT_EQ(divide(1, std::nullopt), std::nullopt);
  // Division by zero is always null.
  EXPECT_EQ(divide(1, 0), std::nullopt);
  EXPECT_EQ(divide(0, 0), std::nullopt);
  EXPECT_EQ(divide(kNan, 0), std::nullopt);
  EXPECT_EQ(divide(kInf, 0), std::nullopt);
  EXPECT_EQ(divide(-kInf, 0), std::nullopt);
  // Division by NaN is always NaN.
  EXPECT_TRUE(std::isnan(divide(1, kNan).value_or(0)));
  EXPECT_TRUE(std::isnan(divide(0, kNan).value_or(0)));
  EXPECT_TRUE(std::isnan(divide(kNan, kNan).value_or(0)));
  EXPECT_TRUE(std::isnan(divide(kInf, kNan).value_or(0)));
  EXPECT_TRUE(std::isnan(divide(-kInf, kNan).value_or(0)));
  // Division by infinity is zero except when dividing infinity, when it is
  // NaN.
  EXPECT_EQ(divide(std::numeric_limits<double>::max(), kInf), 0);
  EXPECT_EQ(divide(0, kInf), 0);
  EXPECT_EQ(divide(0, -kInf), 0); // Actually -0, but it should compare equal.
  EXPECT_TRUE(std::isnan(divide(kInf, kInf).value_or(0)));
  EXPECT_TRUE(std::isnan(divide(-kInf, kInf).value_or(0)));
  EXPECT_TRUE(std::isnan(divide(kInf, -kInf).value_or(0)));
}

TEST_F(ArithmeticTest, acosh) {
  const auto acosh = [&](std::optional<double> a) {
    return evaluateOnce<double>("acosh(c0)", a);
  };

  EXPECT_EQ(acosh(1), 0);
  EXPECT_TRUE(std::isnan(acosh(0).value_or(0)));
  EXPECT_EQ(acosh(kInf), kInf);
  EXPECT_EQ(acosh(std::nullopt), std::nullopt);
  EXPECT_TRUE(std::isnan(acosh(kNan).value_or(0)));
}

TEST_F(ArithmeticTest, asinh) {
  const auto asinh = [&](std::optional<double> a) {
    return evaluateOnce<double>("asinh(c0)", a);
  };

  EXPECT_EQ(asinh(0), 0);
  EXPECT_EQ(asinh(kInf), kInf);
  EXPECT_EQ(asinh(-kInf), -kInf);
  EXPECT_EQ(asinh(std::nullopt), std::nullopt);
  EXPECT_TRUE(std::isnan(asinh(kNan).value_or(0)));
}

TEST_F(ArithmeticTest, atanh) {
  const auto atanh = [&](std::optional<double> a) {
    return evaluateOnce<double>("atanh(c0)", a);
  };

  EXPECT_EQ(atanh(0), 0);
  EXPECT_EQ(atanh(1), kInf);
  EXPECT_EQ(atanh(-1), -kInf);
  EXPECT_TRUE(std::isnan(atanh(1.1).value_or(0)));
  EXPECT_TRUE(std::isnan(atanh(-1.1).value_or(0)));
  EXPECT_EQ(atanh(std::nullopt), std::nullopt);
  EXPECT_TRUE(std::isnan(atanh(kNan).value_or(0)));
}

TEST_F(ArithmeticTest, sec) {
  const auto sec = [&](std::optional<double> a) {
    return evaluateOnce<double>("sec(c0)", a);
  };

  EXPECT_EQ(sec(0), 1);
  EXPECT_EQ(sec(std::nullopt), std::nullopt);
  EXPECT_TRUE(std::isnan(sec(kNan).value_or(0)));
}

TEST_F(ArithmeticTest, csc) {
  const auto csc = [&](std::optional<double> a) {
    return evaluateOnce<double>("csc(c0)", a);
  };

  EXPECT_EQ(csc(0), kInf);
  EXPECT_EQ(csc(std::nullopt), std::nullopt);
  EXPECT_TRUE(std::isnan(csc(kNan).value_or(0)));
}

TEST_F(ArithmeticTest, cosh) {
  const auto cosh = [&](std::optional<double> a) {
    return evaluateOnce<double>("cosh(c0)", a);
  };

  EXPECT_EQ(cosh(0), 1);
  EXPECT_EQ(cosh(kInf), kInf);
  EXPECT_EQ(cosh(-kInf), kInf);
  EXPECT_EQ(cosh(std::nullopt), std::nullopt);
  EXPECT_TRUE(std::isnan(cosh(kNan).value_or(0)));
}

TEST_F(ArithmeticTest, rint) {
  const auto rint = [&](double a) {
    return evaluateOnce<double>("rint(c0)", std::optional(a)).value();
  };

  EXPECT_EQ(rint(2.3), 2.0);
  EXPECT_EQ(rint(3.8), 4.0);
  EXPECT_EQ(rint(-2.3), -2.0);
  EXPECT_EQ(rint(-3.8), -4.0);

  EXPECT_EQ(rint(2.5), 2.0);

  EXPECT_TRUE(std::isnan(rint(kNanDouble)));
  EXPECT_EQ(rint(kInfDouble), kInfDouble);
  EXPECT_EQ(rint(-kInfDouble), -kInfDouble);
  EXPECT_EQ(rint(0.0), 0.0);
  EXPECT_EQ(rint(-0.0), -0.0);

  EXPECT_EQ(rint(std::nextafter(1.0, 0.0)), 1.0);
  EXPECT_EQ(rint(std::nextafter(1.0, 2.0)), 1.0);
  EXPECT_EQ(rint(std::nextafter(1e+16, kInfDouble)), 1e+16 + 2);
}

TEST_F(ArithmeticTest, unhex) {
  const auto unhex = [&](std::optional<std::string> a) {
    return evaluateOnce<std::string>("unhex(c0)", a);
  };

  EXPECT_EQ(unhex("737472696E67"), "string");
  EXPECT_EQ(unhex(""), "");
  EXPECT_EQ(unhex("23"), "#");
  EXPECT_EQ(unhex("123"), "\x01#");
  EXPECT_EQ(unhex("b23"), "\x0B#");
  EXPECT_EQ(unhex("b2323"), "\x0B##");
  EXPECT_EQ(unhex("F"), "\x0F");
  EXPECT_EQ(unhex("ff"), "\xFF");
  EXPECT_EQ(unhex("G"), std::nullopt);
  EXPECT_EQ(unhex("GG"), std::nullopt);
  EXPECT_EQ(unhex("G23"), std::nullopt);
  EXPECT_EQ(unhex("E4B889E9878DE79A84"), "\u4E09\u91CD\u7684");
}

class CeilFloorTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<int64_t> ceil(std::optional<T> a) {
    return evaluateOnce<int64_t>("ceil(c0)", a);
  }
  template <typename T>
  std::optional<int64_t> floor(std::optional<T> a) {
    return evaluateOnce<int64_t>("floor(c0)", a);
  }
};

TEST_F(CeilFloorTest, Limits) {
  EXPECT_EQ(1, ceil<int64_t>(1));
  EXPECT_EQ(-1, ceil<int64_t>(-1));
  EXPECT_EQ(3, ceil<double>(2.878));
  EXPECT_EQ(2, floor<double>(2.878));
  EXPECT_EQ(1, floor<double>(1.5678));
  EXPECT_EQ(
      std::numeric_limits<int64_t>::max(),
      floor<int64_t>(std::numeric_limits<int64_t>::max()));
  EXPECT_EQ(
      std::numeric_limits<int64_t>::min(),
      floor<int64_t>(std::numeric_limits<int64_t>::min()));

  // Very large double values are truncated to int64_t::max/min.
  EXPECT_EQ(
      std::numeric_limits<int64_t>::max(),
      ceil<double>(std::numeric_limits<double>::infinity()));
  EXPECT_EQ(
      std::numeric_limits<int64_t>::max(),
      floor<double>(std::numeric_limits<double>::infinity()));

  EXPECT_EQ(
      std::numeric_limits<int64_t>::min(),
      ceil<double>(-std::numeric_limits<double>::infinity()));
  EXPECT_EQ(
      std::numeric_limits<int64_t>::min(),
      floor<double>(-std::numeric_limits<double>::infinity()));
}

TEST_F(ArithmeticTest, sinh) {
  const auto sinh = [&](std::optional<double> a) {
    return evaluateOnce<double>("sinh(c0)", a);
  };

  EXPECT_EQ(sinh(0), 0);
  EXPECT_EQ(sinh(kInf), kInf);
  EXPECT_EQ(sinh(-kInf), -kInf);
  EXPECT_EQ(sinh(std::nullopt), std::nullopt);
  EXPECT_TRUE(std::isnan(sinh(kNan).value_or(0)));
}

TEST_F(ArithmeticTest, log1p) {
  const double kE = std::exp(1);

  static const auto log1p = [&](std::optional<double> a) {
    return evaluateOnce<double>("log1p(c0)", a);
  };

  EXPECT_EQ(log1p(0), 0);
  EXPECT_EQ(log1p(kE - 1), 1);
  EXPECT_EQ(log1p(kInf), kInf);
  EXPECT_TRUE(std::isnan(log1p(kNan).value_or(0)));
}

TEST_F(ArithmeticTest, expm1) {
  static const auto expm1 = [&](std::optional<double> a) {
    return evaluateOnce<double>("expm1(c0)", a);
  };

  const double kE = std::exp(1);

  // If the argument is NaN, the result is NaN.
  // If the argument is positive infinity, then the result is positive infinity.
  // If the argument is negative infinity, then the result is -1.0.
  // If the argument is zero, then the result is a zero with the same sign as
  // the argument.
  EXPECT_TRUE(std::isnan(expm1(kNan).value_or(0)));
  EXPECT_EQ(expm1(kInf), kInf);
  EXPECT_EQ(expm1(-kInf), -1);
  EXPECT_EQ(expm1(0), 0);
  EXPECT_EQ(expm1(1), kE - 1);
  // As this is only for high accuracy of little number, we use a little number
  // 1e-12 which can give the difference. If you use std::exp(x) - 1, the value
  // may be 1.000009e-12, while the true value should be
  // below 1.000000000000005e-12.
  EXPECT_LT(expm1(1e-12), 1.00009e-12);
}

class BinTest : public SparkFunctionBaseTest {
 protected:
  std::optional<std::string> bin(std::optional<std::int64_t> arg) {
    return evaluateOnce<std::string>("bin(c0)", arg);
  }
};

TEST_F(BinTest, bin) {
  EXPECT_EQ(bin(std::nullopt), std::nullopt);
  EXPECT_EQ(bin(13), "1101");
  EXPECT_EQ(
      bin(-13),
      "1111111111111111111111111111111111111111111111111111111111110011");
  EXPECT_EQ(
      bin(std::numeric_limits<int64_t>::max()),
      "111111111111111111111111111111111111111111111111111111111111111");
  EXPECT_EQ(bin(0), "0");
  auto result = evaluateOnce<std::string, int64_t>(
      "bin(row_constructor(c0).c1)", std::make_optional(13L));
  EXPECT_EQ(result, "1101");
}

TEST_F(ArithmeticTest, hypot) {
  const auto hypot = [&](std::optional<double> a, std::optional<double> b) {
    return evaluateOnce<double>("hypot(c0, c1)", a, b);
  };

  EXPECT_EQ(hypot(3, 4), 5);
  EXPECT_EQ(hypot(-3, -4), 5);
  EXPECT_EQ(hypot(3.0, -4.0), 5.0);
  EXPECT_DOUBLE_EQ(5.70087712549569, hypot(3.5, 4.5).value());
  EXPECT_DOUBLE_EQ(5.70087712549569, hypot(3.5, -4.5).value());
}

TEST_F(ArithmeticTest, cot) {
  const auto cot = [&](std::optional<double> a) {
    return evaluateOnce<double>("cot(c0)", a);
  };

  EXPECT_EQ(cot(0), kInf);
  EXPECT_TRUE(std::isnan(cot(kNan).value_or(0)));
  EXPECT_EQ(cot(1), 1 / std::tan(1));
  EXPECT_EQ(cot(-1), 1 / std::tan(-1));
  EXPECT_EQ(cot(0), 1 / std::tan(0));
}

TEST_F(ArithmeticTest, atan2) {
  const auto atan2 = [&](std::optional<double> y, std::optional<double> x) {
    return evaluateOnce<double>("atan2(c0, c1)", y, x);
  };

  EXPECT_EQ(atan2(0.0, 0.0), 0.0);
  EXPECT_EQ(atan2(-0.0, -0.0), 0.0);
  EXPECT_EQ(atan2(0.0, -0.0), 0.0);
  EXPECT_EQ(atan2(-0.0, 0.0), 0.0);
  EXPECT_EQ(atan2(-1.0, 1.0), std::atan2(-1.0, 1.0));
  EXPECT_EQ(atan2(1.0, 1.0), std::atan2(1.0, 1.0));
  EXPECT_EQ(atan2(1.0, -1.0), std::atan2(1.0, -1.0));
  EXPECT_EQ(atan2(-1.0, -1.0), std::atan2(-1.0, -1.0));
}

TEST_F(ArithmeticTest, isNanFloat) {
  const auto isNan = [&](std::optional<float> a) {
    return evaluateOnce<bool>("isnan(c0)", a);
  };

  EXPECT_EQ(false, isNan(0.0f));
  EXPECT_EQ(true, isNan(kNan));
  EXPECT_EQ(true, isNan(0.0f / 0.0f));
  EXPECT_EQ(false, isNan(std::nullopt));
}

TEST_F(ArithmeticTest, isNanDouble) {
  const auto isNan = [&](std::optional<double> a) {
    return evaluateOnce<bool>("isnan(c0)", a);
  };

  EXPECT_EQ(false, isNan(0.0));
  EXPECT_EQ(true, isNan(kNanDouble));
  EXPECT_EQ(true, isNan(0.0 / 0.0));
  EXPECT_EQ(false, isNan(std::nullopt));
}

TEST_F(ArithmeticTest, hexWithBigint) {
  const auto toHex = [&](std::optional<int64_t> value) {
    return evaluateOnce<std::string>("hex(c0)", value);
  };
  EXPECT_EQ("11", toHex(17));
  EXPECT_EQ("FFFFFFFFFFFFFFEF", toHex(-17));
  EXPECT_EQ("0", toHex(0));
  EXPECT_EQ("FFFFFFFFFFFFFFFF", toHex(-1));
  EXPECT_EQ("7FFFFFFFFFFFFFFF", toHex(INT64_MAX));
  EXPECT_EQ("8000000000000000", toHex(INT64_MIN));
}

TEST_F(ArithmeticTest, hexWithVarbinaryAndVarchar) {
  const auto toHex = [&](std::optional<std::string> value) {
    auto varbinaryResult =
        evaluateOnce<std::string>("hex(cast(c0 as varbinary))", value);
    auto varcharResult = evaluateOnce<std::string>("hex(c0)", value);

    EXPECT_TRUE(varbinaryResult.has_value());
    EXPECT_TRUE(varcharResult.has_value());
    EXPECT_EQ(varbinaryResult.value(), varcharResult.value());

    return varcharResult.value();
  };
  ASSERT_EQ(toHex(""), "");
  ASSERT_EQ(toHex("Spark SQL"), "537061726B2053514C");
  ASSERT_EQ(toHex("Spark\x65\x21SQL"), "537061726B652153514C");
  ASSERT_EQ(toHex("Spark\u6570\u636ESQL"), "537061726BE695B0E68DAE53514C");
}

TEST_F(ArithmeticTest, widthBucket) {
  constexpr int64_t kMaxInt64 = std::numeric_limits<int64_t>::max();

  const auto widthBucket = [&](std::optional<double> value,
                               std::optional<double> min,
                               std::optional<double> max,
                               std::optional<int64_t> numBucket) {
    return evaluateOnce<int64_t>(
        "width_bucket(c0, c1, c2, c3)", value, min, max, numBucket);
  };

  // min < max
  EXPECT_EQ(3, widthBucket(3.14, 0, 4, 3));
  EXPECT_EQ(2, widthBucket(2, 0, 4, 3));
  EXPECT_EQ(4, widthBucket(kInf, 0, 4, 3));
  EXPECT_EQ(0, widthBucket(-1, 0, 3.2, 4));

  // min > max
  EXPECT_EQ(1, widthBucket(3.14, 4, 0, 3));
  EXPECT_EQ(2, widthBucket(2, 4, 0, 3));
  EXPECT_EQ(0, widthBucket(kInf, 4, 0, 3));
  EXPECT_EQ(5, widthBucket(-1, 3.2, 0, 4));

  // max - min + 1 > Long.MaxValue
  EXPECT_EQ(widthBucket(5.3, 0, 9223372036854775807, 10), 1);

  // Cases to get null result.
  EXPECT_EQ(widthBucket(3.14, 0, 4, 0), std::nullopt);
  EXPECT_EQ(widthBucket(kNan, 0, 4, 10), std::nullopt);
  EXPECT_EQ(widthBucket(3.14, kNan, 0, 10), std::nullopt);
  EXPECT_EQ(widthBucket(3.14, kInf, 0, 10), std::nullopt);
  EXPECT_EQ(widthBucket(3.14, 0, kNan, 10), std::nullopt);
  EXPECT_EQ(widthBucket(3.14, 0, kInf, 10), std::nullopt);
  EXPECT_EQ(widthBucket(3.14, 0, 0, 10), std::nullopt);
  EXPECT_EQ(widthBucket(kInf, 0, 4, kMaxInt64), std::nullopt);
  EXPECT_EQ(widthBucket(kInf, 4, 0, kMaxInt64), std::nullopt);
  EXPECT_EQ(widthBucket(5.3, 0.2, 10.6, 9223372036854775807), std::nullopt);

  // value is infinite.
  EXPECT_EQ(widthBucket(kInf, 0, 4, 3), 4);
  EXPECT_EQ(widthBucket(-kInf, 0, 4, 3), 0);
}

TEST_F(ArithmeticTest, checkedAdd) {
  assertErrorForCheckedAdd<int8_t>(INT8_MAX, 1, "Arithmetic overflow: 127 + 1");
  assertErrorForCheckedAdd<int16_t>(
      INT16_MAX, 1, "Arithmetic overflow: 32767 + 1");
  assertErrorForCheckedAdd<int32_t>(
      INT32_MAX, 1, "Arithmetic overflow: 2147483647 + 1");
  assertErrorForCheckedAdd<int64_t>(
      INT64_MAX, 1, "Arithmetic overflow: 9223372036854775807 + 1");
  EXPECT_EQ(checkedAdd<float>(kInf, 1), kInf);
  EXPECT_EQ(checkedAdd<double>(kInfDouble, 1), kInfDouble);
}

TEST_F(ArithmeticTest, checkedSubtract) {
  assertErrorForcheckedSubtract<int8_t>(
      INT8_MIN, 1, "Arithmetic overflow: -128 - 1");
  assertErrorForcheckedSubtract<int16_t>(
      INT16_MIN, 1, "Arithmetic overflow: -32768 - 1");
  assertErrorForcheckedSubtract<int32_t>(
      INT32_MIN, 1, "Arithmetic overflow: -2147483648 - 1");
  assertErrorForcheckedSubtract<int64_t>(
      INT64_MIN, 1, "Arithmetic overflow: -9223372036854775808 - 1");
  EXPECT_EQ(checkedSubtract<float>(kInf, 1), kInf);
  EXPECT_EQ(checkedSubtract<double>(kInfDouble, 1), kInfDouble);
}

TEST_F(ArithmeticTest, checkedMultiply) {
  assertErrorForCheckedMultiply<int8_t>(
      INT8_MAX, 2, "Arithmetic overflow: 127 * 2");
  assertErrorForCheckedMultiply<int16_t>(
      INT16_MAX, 2, "Arithmetic overflow: 32767 * 2");
  assertErrorForCheckedMultiply<int32_t>(
      INT32_MAX, 2, "Arithmetic overflow: 2147483647 * 2");
  assertErrorForCheckedMultiply<int64_t>(
      INT64_MAX, 2, "Arithmetic overflow: 9223372036854775807 * 2");
  EXPECT_EQ(checkedMultiply<float>(kInf, 1), kInf);
  EXPECT_EQ(checkedMultiply<double>(kInfDouble, 1), kInfDouble);
}

TEST_F(ArithmeticTest, checkedDivide) {
  assertErrorForCheckedDivide<int32_t>(1, 0, "division by zero");
  assertErrorForCheckedDivide<int8_t>(
      INT8_MIN, -1, "Arithmetic overflow: -128 / -1");
  assertErrorForCheckedDivide<int16_t>(
      INT16_MIN, -1, "Arithmetic overflow: -32768 / -1");
  assertErrorForCheckedDivide<int32_t>(
      INT32_MIN, -1, "Arithmetic overflow: -2147483648 / -1");
  assertErrorForCheckedDivide<int64_t>(
      INT64_MIN, -1, "Arithmetic overflow: -9223372036854775808 / -1");
  EXPECT_EQ(checkedDivide<float>(kInf, 1), kInf);
  EXPECT_EQ(checkedDivide<double>(kInfDouble, 1), kInfDouble);
}

class LogNTest : public SparkFunctionBaseTest {
 protected:
  static constexpr double kInf = std::numeric_limits<double>::infinity();
  static constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
};

TEST_F(LogNTest, log2) {
  const auto log2 = [&](std::optional<double> a) {
    return evaluateOnce<double>("log2(c0)", a);
  };
  EXPECT_EQ(log2(8), 3.0);
  EXPECT_EQ(log2(-1.0), std::nullopt);
  EXPECT_EQ(log2(0.0), std::nullopt);
  EXPECT_EQ(log2(kInf), kInf);
}

TEST_F(LogNTest, log10) {
  const auto log10 = [&](std::optional<double> a) {
    return evaluateOnce<double>("log10(c0)", a);
  };
  EXPECT_EQ(log10(100), 2.0);
  EXPECT_EQ(log10(0.0), std::nullopt);
  EXPECT_EQ(log10(-1.0), std::nullopt);
  EXPECT_EQ(log10(kInf), kInf);
}

TEST_F(LogNTest, log) {
  const auto log = [&](std::optional<double> a, std::optional<double> b) {
    return evaluateOnce<double>("log(c0, c1)", a, b);
  };
  const auto isNan = [&](std::optional<double> res) {
    return std::isnan(res.value());
  };
  EXPECT_EQ(log(10, 100), 2.0);

  EXPECT_EQ(log(0.0, 1.0), std::nullopt);
  EXPECT_EQ(log(1.0, 0.0), std::nullopt);
  EXPECT_EQ(log(-1.0, 1.0), std::nullopt);
  EXPECT_EQ(log(1.0, -1.0), std::nullopt);

  EXPECT_EQ(log(1.0, 3.0), kInf);

  EXPECT_TRUE(isNan(log(kNan, kNan)));
  EXPECT_TRUE(isNan(log(kInf, kNan)));
  EXPECT_TRUE(isNan(log(kNan, kInf)));
  EXPECT_TRUE(isNan(log(kInf, kInf)));

  EXPECT_EQ(log(kInf, -kInf), std::nullopt);
  EXPECT_EQ(log(-kInf, kInf), std::nullopt);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
