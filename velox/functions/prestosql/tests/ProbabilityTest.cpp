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

#include <gmock/gmock.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox {
namespace {

constexpr double kInf = std::numeric_limits<double>::infinity();
constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
constexpr double kDoubleMax = std::numeric_limits<double>::max();
constexpr double kDoubleMin = std::numeric_limits<double>::min();
constexpr int64_t kBigIntMax = std::numeric_limits<int64_t>::max();
constexpr int64_t kBigIntMin = std::numeric_limits<int64_t>::min();

MATCHER(IsNan, "is NaN") {
  return arg && std::isnan(*arg);
}

MATCHER(IsInf, "is Infinity") {
  return arg && std::isinf(*arg);
}

class ProbabilityTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename ValueType>
  auto poissonCDF(
      const std::optional<double>& lambda,
      const std::optional<ValueType>& value) {
    return evaluateOnce<double>("poisson_cdf(c0, c1)", lambda, value);
  }

  template <typename ValueType>
  auto binomialCDF(
      std::optional<ValueType> numberOfTrials,
      std::optional<double> successProbability,
      std::optional<ValueType> value) {
    return evaluateOnce<double>(
        "binomial_cdf(c0, c1, c2)", numberOfTrials, successProbability, value);
  }

  template <typename ValueType>
  void poissonCDFTests() {
    EXPECT_EQ(0.91608205796869657, poissonCDF<ValueType>(3, 5));
    EXPECT_EQ(0, poissonCDF<ValueType>(kDoubleMax, 3));
    EXPECT_EQ(
        1, poissonCDF<ValueType>(3, std::numeric_limits<ValueType>::max()));
    EXPECT_EQ(1, poissonCDF<ValueType>(kDoubleMin, 3));
    EXPECT_EQ(std::nullopt, poissonCDF<ValueType>(std::nullopt, 1));
    EXPECT_EQ(std::nullopt, poissonCDF<ValueType>(1, std::nullopt));
    EXPECT_EQ(std::nullopt, poissonCDF<ValueType>(std::nullopt, std::nullopt));
    VELOX_ASSERT_THROW(
        poissonCDF<ValueType>(kNan, 3), "lambda must be greater than 0");
    VELOX_ASSERT_THROW(
        poissonCDF<ValueType>(-3, 5), "lambda must be greater than 0");
    VELOX_ASSERT_THROW(
        poissonCDF<ValueType>(3, std::numeric_limits<ValueType>::min()),
        "value must be a non-negative integer");
    VELOX_ASSERT_THROW(
        poissonCDF<ValueType>(3, -10), "value must be a non-negative integer");
    EXPECT_THROW(poissonCDF<ValueType>(kInf, 3), VeloxUserError);
  }

  template <typename ValueType>
  void binomialCDFTests() {
    EXPECT_EQ(binomialCDF<ValueType>(5, 0.5, 5), 1.0);
    EXPECT_EQ(binomialCDF<ValueType>(5, 0.5, 0), 0.03125);
    EXPECT_EQ(binomialCDF<ValueType>(3, 0.5, 1), 0.5);
    EXPECT_EQ(binomialCDF<ValueType>(20, 1.0, 0), 0.0);
    EXPECT_EQ(binomialCDF<ValueType>(20, 0.3, 6), 0.60800981220092398);
    EXPECT_EQ(binomialCDF<ValueType>(200, 0.3, 60), 0.5348091761606989);
    EXPECT_EQ(
        binomialCDF<ValueType>(std::numeric_limits<ValueType>::max(), 0.5, 2),
        0.0);
    EXPECT_EQ(
        binomialCDF<ValueType>(
            std::numeric_limits<ValueType>::max(),
            0.5,
            std::numeric_limits<ValueType>::max()),
        0.0);
    EXPECT_EQ(
        binomialCDF<ValueType>(10, 0.5, std::numeric_limits<ValueType>::max()),
        1.0);
    EXPECT_EQ(
        binomialCDF<ValueType>(10, 0.1, std::numeric_limits<ValueType>::min()),
        0.0);
    EXPECT_EQ(binomialCDF<ValueType>(10, 0.1, -2), 0.0);
    EXPECT_EQ(binomialCDF<ValueType>(25, 0.5, -100), 0.0);

    // Invalid inputs
    VELOX_ASSERT_THROW(
        binomialCDF<ValueType>(5, -0.5, 3),
        "successProbability must be in the interval [0, 1]");
    VELOX_ASSERT_THROW(
        binomialCDF<ValueType>(5, 2, 3),
        "successProbability must be in the interval [0, 1]");
    VELOX_ASSERT_THROW(
        binomialCDF<ValueType>(5, std::numeric_limits<ValueType>::max(), 3),
        "successProbability must be in the interval [0, 1]");
    VELOX_ASSERT_THROW(
        binomialCDF<ValueType>(5, kNan, 3),
        "successProbability must be in the interval [0, 1]");
    VELOX_ASSERT_THROW(
        binomialCDF<ValueType>(-1, 0.5, 2),
        "numberOfTrials must be greater than 0");
    VELOX_ASSERT_THROW(
        binomialCDF<ValueType>(std::numeric_limits<ValueType>::min(), 0.5, 1),
        "numberOfTrials must be greater than 0");
    VELOX_ASSERT_THROW(
        binomialCDF<ValueType>(-2, 2, -1),
        "successProbability must be in the interval [0, 1]");
    VELOX_ASSERT_THROW(
        binomialCDF<ValueType>(-2, 0.5, -1),
        "numberOfTrials must be greater than 0");
  }
};

TEST_F(ProbabilityTest, betaCDF) {
  const auto betaCDF = [&](std::optional<double> a,
                           std::optional<double> b,
                           std::optional<double> value) {
    return evaluateOnce<double>("beta_cdf(c0, c1, c2)", a, b, value);
  };

  EXPECT_EQ(0.09888000000000001, betaCDF(3, 4, 0.2));
  EXPECT_EQ(0.0, betaCDF(3, 3.6, 0.0));
  EXPECT_EQ(1.0, betaCDF(3, 3.6, 1.0));
  EXPECT_EQ(0.21764809997679951, betaCDF(3, 3.6, 0.3));
  EXPECT_EQ(0.9972502881611551, betaCDF(3, 3.6, 0.9));
  EXPECT_EQ(0.0, betaCDF(kInf, 3, 0.2));
  EXPECT_EQ(0.0, betaCDF(3, kInf, 0.2));
  EXPECT_EQ(0.0, betaCDF(kInf, kInf, 0.2));
  EXPECT_EQ(0.0, betaCDF(kDoubleMax, kDoubleMax, 0.3));
  EXPECT_EQ(0.5, betaCDF(kDoubleMin, kDoubleMin, 0.3));

  VELOX_ASSERT_THROW(
      betaCDF(3, 3, kInf), "value must be in the interval [0, 1]");
  VELOX_ASSERT_THROW(betaCDF(0, 3, 0.5), "a must be > 0");
  VELOX_ASSERT_THROW(betaCDF(3, 0, 0.5), "b must be > 0");
  VELOX_ASSERT_THROW(
      betaCDF(3, 5, -0.1), "value must be in the interval [0, 1]");
  VELOX_ASSERT_THROW(
      betaCDF(3, 5, 1.1), "value must be in the interval [0, 1]");
  VELOX_ASSERT_THROW(betaCDF(kNan, 3, 0.5), "a must be > 0");
  VELOX_ASSERT_THROW(betaCDF(3, kNan, 0.5), "b must be > 0");
  VELOX_ASSERT_THROW(
      betaCDF(3, 3, kNan), "value must be in the interval [0, 1]");
}

TEST_F(ProbabilityTest, normalCDF) {
  const auto normal_cdf = [&](std::optional<double> mean,
                              std::optional<double> sd,
                              std::optional<double> value) {
    return evaluateOnce<double>("normal_cdf(c0, c1, c2)", mean, sd, value);
  };

  EXPECT_EQ(0.97500210485177963, normal_cdf(0, 1, 1.96));
  EXPECT_EQ(0.5, normal_cdf(10, 9, 10));
  EXPECT_EQ(0.0013498980316301035, normal_cdf(-1.5, 2.1, -7.8));
  EXPECT_EQ(1.0, normal_cdf(0, 1, kInf));
  EXPECT_EQ(0.0, normal_cdf(0, 1, -kInf));
  EXPECT_EQ(0.0, normal_cdf(kInf, 1, 0));
  EXPECT_EQ(1.0, normal_cdf(-kInf, 1, 0));
  EXPECT_EQ(0.5, normal_cdf(0, kInf, 0));
  EXPECT_THAT(normal_cdf(kNan, 1, 0), IsNan());
  EXPECT_THAT(normal_cdf(0, 1, kNan), IsNan());
  EXPECT_EQ(normal_cdf(0, 1, kDoubleMax), 1);
  EXPECT_EQ(normal_cdf(0, kDoubleMax, 0), 0.5);
  EXPECT_EQ(0.0, normal_cdf(kDoubleMax, 1, 0));
  EXPECT_EQ(0.5, normal_cdf(0, 1, kDoubleMin));
  EXPECT_EQ(0.5, normal_cdf(kDoubleMin, 1, 0));
  EXPECT_EQ(0, normal_cdf(1, kDoubleMin, 0));
  EXPECT_THAT(normal_cdf(kDoubleMax, kDoubleMax, kInf), IsNan());
  EXPECT_EQ(0.5, normal_cdf(kDoubleMax, kDoubleMax, kDoubleMax));
  EXPECT_EQ(0.5, normal_cdf(kDoubleMin, kDoubleMin, kDoubleMin));
  EXPECT_EQ(0.5, normal_cdf(kDoubleMax, 1, kDoubleMax));
  EXPECT_EQ(0.5, normal_cdf(10, kDoubleMax, kDoubleMax));
  EXPECT_EQ(0.5, normal_cdf(kDoubleMax, kDoubleMax, 1.96));
  EXPECT_EQ(std::nullopt, normal_cdf(std::nullopt, 1, 1.96));
  EXPECT_EQ(std::nullopt, normal_cdf(1, 1, std::nullopt));
  EXPECT_EQ(std::nullopt, normal_cdf(std::nullopt, 1, std::nullopt));
  EXPECT_EQ(std::nullopt, normal_cdf(std::nullopt, std::nullopt, std::nullopt));

  VELOX_ASSERT_THROW(normal_cdf(0, 0, 0.1985), "standardDeviation must be > 0");
  VELOX_ASSERT_THROW(
      normal_cdf(0, kNan, 0.1985), "standardDeviation must be > 0");
}

TEST_F(ProbabilityTest, cauchyCDF) {
  const auto cauchyCDF = [&](std::optional<double> median,
                             std::optional<double> scale,
                             std::optional<double> value) {
    return evaluateOnce<double>("cauchy_cdf(c0, c1, c2)", median, scale, value);
  };

  EXPECT_EQ(0.5, cauchyCDF(0.0, 1.0, 0.0));
  EXPECT_EQ(0.75, cauchyCDF(0.0, 1.0, 1.0));
  EXPECT_EQ(0.25, cauchyCDF(5.0, 2.0, 3.0));
  EXPECT_EQ(1.0, cauchyCDF(5.0, 2.0, kInf));
  EXPECT_EQ(0.5, cauchyCDF(5.0, kInf, 3.0));
  EXPECT_EQ(0.0, cauchyCDF(kInf, 2.0, 3.0));
  EXPECT_EQ(1.0, cauchyCDF(5.0, 2.0, kDoubleMax));
  EXPECT_EQ(0.5, cauchyCDF(5.0, kDoubleMax, 3.0));
  EXPECT_EQ(0.0, cauchyCDF(kDoubleMax, 1.0, 1.0));
  EXPECT_EQ(0.25, cauchyCDF(1.0, 1.0, kDoubleMin));
  EXPECT_EQ(0.5, cauchyCDF(5.0, kDoubleMin, 5.0));
  EXPECT_EQ(0.75, cauchyCDF(kDoubleMin, 1.0, 1.0));
  EXPECT_EQ(0.64758361765043326, cauchyCDF(2.5, 1.0, 3.0));
  EXPECT_THAT(cauchyCDF(kNan, 1.0, 1.0), IsNan());
  EXPECT_THAT(cauchyCDF(1.0, 1.0, kNan), IsNan());
  EXPECT_THAT(cauchyCDF(kInf, 1.0, kNan), IsNan());
  VELOX_ASSERT_THROW(cauchyCDF(1.0, kNan, 1.0), "scale must be greater than 0");
  VELOX_ASSERT_THROW(cauchyCDF(0, -1, 0), "scale must be greater than 0");
}

TEST_F(ProbabilityTest, invBetaCDF) {
  const auto invBetaCDF = [&](std::optional<double> a,
                              std::optional<double> b,
                              std::optional<double> p) {
    return evaluateOnce<double>("inverse_beta_cdf(c0, c1, c2)", a, b, p);
  };

  EXPECT_EQ(0.0, invBetaCDF(3, 3.6, 0.0));
  EXPECT_EQ(1.0, invBetaCDF(3, 3.6, 1.0));
  EXPECT_EQ(0.34696754854406159, invBetaCDF(3, 3.6, 0.3));
  EXPECT_EQ(0.76002724631002683, invBetaCDF(3, 3.6, 0.95));

  EXPECT_EQ(std::nullopt, invBetaCDF(std::nullopt, 3.6, 0.95));
  EXPECT_EQ(std::nullopt, invBetaCDF(3.6, std::nullopt, 0.95));
  EXPECT_EQ(std::nullopt, invBetaCDF(3.6, 3.6, std::nullopt));

  // Boost libraries currently throw an assert. Created the expected values via
  // Matlab. Presto currently throws an exception from Apache Math for these
  // values
  // EXPECT_EQ(0.5, invBetaCDF(kDoubleMax, kDoubleMax, 0.3));
  // EXPECT_EQ(0.0, invBetaCDF(kDoubleMin, kDoubleMin, 0.3));

  VELOX_ASSERT_THROW(invBetaCDF(kInf, 3, 0.2), "a must be > 0");
  VELOX_ASSERT_THROW(invBetaCDF(kNan, 3, 0.5), "a must be > 0");
  VELOX_ASSERT_THROW(invBetaCDF(0, 3, 0.5), "a must be > 0");

  VELOX_ASSERT_THROW(invBetaCDF(3, kInf, 0.2), "b must be > 0");
  VELOX_ASSERT_THROW(invBetaCDF(3, kNan, 0.5), "b must be > 0");
  VELOX_ASSERT_THROW(invBetaCDF(3, 0, 0.5), "b must be > 0");

  VELOX_ASSERT_THROW(
      invBetaCDF(3, 3.6, kInf), "p must be in the interval [0, 1]");
  VELOX_ASSERT_THROW(
      invBetaCDF(3, 3.6, kNan), "p must be in the interval [0, 1]");
  VELOX_ASSERT_THROW(
      invBetaCDF(3, 5, -0.1), "p must be in the interval [0, 1]");
  VELOX_ASSERT_THROW(invBetaCDF(3, 5, 1.1), "p must be in the interval [0, 1]");
}

TEST_F(ProbabilityTest, chiSquaredCDF) {
  const auto chiSquaredCDF = [&](std::optional<double> df,
                                 std::optional<double> value) {
    return evaluateOnce<double>("chi_squared_cdf(c0, c1)", df, value);
  };

  EXPECT_EQ(chiSquaredCDF(3, 0.0), 0.0);
  EXPECT_EQ(chiSquaredCDF(3, 1.0), 0.1987480430987992);
  EXPECT_EQ(chiSquaredCDF(3, 2.5), 0.52470891665697938);
  EXPECT_EQ(chiSquaredCDF(3, 4), 0.73853587005088939);
  // Invalid inputs
  VELOX_ASSERT_THROW(chiSquaredCDF(-3, 0.3), "df must be greater than 0");
  VELOX_ASSERT_THROW(chiSquaredCDF(3, -10), "value must non-negative");
}

TEST_F(ProbabilityTest, fCDF) {
  const auto fCDF = [&](std::optional<double> df1,
                        std::optional<double> df2,
                        std::optional<double> value) {
    return evaluateOnce<double>("f_cdf(c0, c1, c2)", df1, df2, value);
  };

  EXPECT_EQ(fCDF(2.0, 5.0, 0.0), 0.0);
  EXPECT_EQ(fCDF(2.0, 5.0, 0.7988), 0.50001145221750731);
  EXPECT_EQ(fCDF(2.0, 5.0, 3.7797), 0.89999935988961155);

  EXPECT_EQ(fCDF(kDoubleMax, 5.0, 3.7797), 1);
  EXPECT_EQ(fCDF(1, kDoubleMax, 97.1), 1);
  EXPECT_EQ(fCDF(82.6, 901.10, kDoubleMax), 1);
  EXPECT_EQ(fCDF(12.12, 4.2015, kDoubleMin), 0);
  EXPECT_EQ(fCDF(0.4422, kDoubleMin, 0.697), 7.9148959162596482e-306);
  EXPECT_EQ(fCDF(kDoubleMin, 50.620, 4), 1);
  EXPECT_EQ(fCDF(kBigIntMax, 5.0, 3.7797), 0.93256230095450132);
  EXPECT_EQ(fCDF(76.901, kBigIntMax, 77.97), 1);
  EXPECT_EQ(fCDF(2.0, 5.0, kBigIntMax), 1);

  EXPECT_EQ(fCDF(2.0, 5.0, std::nullopt), std::nullopt);
  EXPECT_EQ(fCDF(2.0, std::nullopt, 3.7797), std::nullopt);
  EXPECT_EQ(fCDF(std::nullopt, 5.0, 3.7797), std::nullopt);

  // Test invalid inputs for df1.
  VELOX_ASSERT_THROW(fCDF(0, 3, 0.5), "numerator df must be greater than 0");
  VELOX_ASSERT_THROW(
      fCDF(kBigIntMin, 5.0, 3.7797), "numerator df must be greater than 0");

  // Test invalid inputs for df2.
  VELOX_ASSERT_THROW(fCDF(3, 0, 0.5), "denominator df must be greater than 0");
  VELOX_ASSERT_THROW(
      fCDF(2.0, kBigIntMin, 3.7797), "denominator df must be greater than 0");

  // Test invalid inputs for value.
  VELOX_ASSERT_THROW(fCDF(3, 5, -0.1), "value must non-negative");
  VELOX_ASSERT_THROW(fCDF(2.0, 5.0, kBigIntMin), "value must non-negative");

  // Test a combination of invalid inputs.
  VELOX_ASSERT_THROW(fCDF(-1.2, 0, -0.1), "value must non-negative");
  VELOX_ASSERT_THROW(fCDF(1, -kInf, -0.1), "value must non-negative");
}

TEST_F(ProbabilityTest, laplaceCDF) {
  const auto laplaceCDF = [&](std::optional<double> location,
                              std::optional<double> scale,
                              std::optional<double> x) {
    return evaluateOnce<double>("laplace_cdf(c0, c1, c2)", location, scale, x);
  };

  EXPECT_DOUBLE_EQ(0.5, laplaceCDF(0.0, 1.0, 0.0).value());
  EXPECT_DOUBLE_EQ(0.5, laplaceCDF(5.0, 2.0, 5.0).value());
  EXPECT_DOUBLE_EQ(0.0, laplaceCDF(5.0, 2.0, -kInf).value());
  EXPECT_THAT(laplaceCDF(kNan, 1.0, 0.5), IsNan());
  EXPECT_THAT(laplaceCDF(1.0, 1.0, kNan), IsNan());
  EXPECT_THAT(laplaceCDF(kInf, 1.0, kNan), IsNan());
  EXPECT_EQ(std::nullopt, laplaceCDF(std::nullopt, 1.0, 0.5));
  EXPECT_EQ(std::nullopt, laplaceCDF(1.0, std::nullopt, 0.5));
  EXPECT_EQ(std::nullopt, laplaceCDF(1.0, 1.0, std::nullopt));
  EXPECT_EQ(0, laplaceCDF(kDoubleMax, 1.0, 0.5));
  EXPECT_EQ(0.5, laplaceCDF(1.0, kDoubleMax, 0.5));
  EXPECT_EQ(1, laplaceCDF(1.0, 1.0, kDoubleMax));
  EXPECT_NEAR(
      0.69673467014368329, laplaceCDF(kDoubleMin, 1.0, 0.5).value(), 1e-15);
  EXPECT_EQ(0, laplaceCDF(1.0, kDoubleMin, 0.5));
  EXPECT_NEAR(
      0.18393972058572117, laplaceCDF(1.0, 1.0, kDoubleMin).value(), 1e-15);
  VELOX_ASSERT_THROW(laplaceCDF(1.0, 0.0, 0.5), "scale must be greater than 0");
  VELOX_ASSERT_THROW(
      laplaceCDF(1.0, -1.0, 0.5), "scale must be greater than 0");
}

TEST_F(ProbabilityTest, gammaCDF) {
  const auto gammaCDF = [&](std::optional<double> shape,
                            std::optional<double> scale,
                            std::optional<double> value) {
    return evaluateOnce<double>("gamma_cdf(c0, c1, c2)", shape, scale, value);
  };

  EXPECT_DOUBLE_EQ(0.96675918913720599, gammaCDF(0.5, 3.0, 6.8).value());
  EXPECT_DOUBLE_EQ(0.50636537728827200, gammaCDF(1.5, 2.0, 2.4).value());
  EXPECT_DOUBLE_EQ(0.55950671493478754, gammaCDF(5.0, 2.0, 10.0).value());
  EXPECT_DOUBLE_EQ(0.01751372384616767, gammaCDF(6.5, 3.5, 8.1).value());
  EXPECT_DOUBLE_EQ(1.0, gammaCDF(5.0, 2.0, 100.0).value());
  EXPECT_DOUBLE_EQ(0.0, gammaCDF(5.0, 2.0, 0.0).value());
  EXPECT_DOUBLE_EQ(0.15085496391539036, gammaCDF(2.5, 1.0, 1.0).value());
  EXPECT_DOUBLE_EQ(1.0, gammaCDF(2.0, kInf, kInf).value());
  EXPECT_DOUBLE_EQ(0.0, gammaCDF(kInf, 3.0, 6.0).value());
  EXPECT_DOUBLE_EQ(0.0, gammaCDF(2.0, kInf, 6.0).value());
  EXPECT_DOUBLE_EQ(1.0, gammaCDF(2.0, 3.0, kInf).value());
  EXPECT_DOUBLE_EQ(0.0, gammaCDF(kDoubleMax, 3.0, 6.0).value());
  EXPECT_DOUBLE_EQ(0.0, gammaCDF(2.0, kDoubleMax, 6.0).value());
  EXPECT_DOUBLE_EQ(1.0, gammaCDF(2.0, 3.0, kDoubleMax).value());
  EXPECT_DOUBLE_EQ(1.0, gammaCDF(kDoubleMin, 3.0, 6.0).value());
  EXPECT_DOUBLE_EQ(1.0, gammaCDF(2.0, kDoubleMin, 6.0).value());
  EXPECT_DOUBLE_EQ(0.0, gammaCDF(2.0, 3.0, kDoubleMin).value());

  EXPECT_EQ(std::nullopt, gammaCDF(std::nullopt, 3.0, 6.0));
  EXPECT_EQ(std::nullopt, gammaCDF(2.0, std::nullopt, 6.0));
  EXPECT_EQ(std::nullopt, gammaCDF(2.0, 3.0, std::nullopt));

  // invalid inputs
  VELOX_ASSERT_THROW(gammaCDF(-1.0, 3.0, 6.0), "shape must be greater than 0");
  VELOX_ASSERT_THROW(gammaCDF(2.0, -1.0, 6.0), "scale must be greater than 0");
  VELOX_ASSERT_THROW(
      gammaCDF(2.0, 3.0, -1.0), "value must be greater than, or equal to, 0");
  VELOX_ASSERT_THROW(gammaCDF(kNan, 3.0, 6.0), "shape must be greater than 0");
  VELOX_ASSERT_THROW(gammaCDF(2.0, kNan, 6.0), "scale must be greater than 0");
  VELOX_ASSERT_THROW(
      gammaCDF(2.0, 3.0, kNan), "value must be greater than, or equal to, 0");
}

TEST_F(ProbabilityTest, poissonCDF) {
  poissonCDFTests<int32_t>();
  poissonCDFTests<int64_t>();
}

TEST_F(ProbabilityTest, binomialCDF) {
  binomialCDFTests<int32_t>();
  binomialCDFTests<int64_t>();
}

TEST_F(ProbabilityTest, weibullCDF) {
  const auto weibullCDF = [&](std::optional<double> a,
                              std::optional<double> b,
                              std::optional<double> value) {
    return evaluateOnce<double>("weibull_cdf(c0, c1, c2)", a, b, value);
  };

  EXPECT_EQ(weibullCDF(1.0, 1.0, 0.0), 0.0);
  EXPECT_EQ(weibullCDF(1.0, 1.0, 40.0), 1.0);
  EXPECT_EQ(weibullCDF(1.0, 0.6, 3.0), 0.99326205300091452);
  EXPECT_EQ(weibullCDF(1.0, 0.9, 2.0), 0.89163197677810413);

  EXPECT_EQ(weibullCDF(std::nullopt, 1.0, 0.3), std::nullopt);
  EXPECT_EQ(weibullCDF(1.0, std::nullopt, 0.2), std::nullopt);
  EXPECT_EQ(weibullCDF(1.0, 0.4, std::nullopt), std::nullopt);

  EXPECT_EQ(weibullCDF(kDoubleMin, 1.0, 2.0), 0.63212055882855767);
  EXPECT_EQ(weibullCDF(kDoubleMax, 1.0, 3.0), 1.0);
  EXPECT_EQ(weibullCDF(1.0, kDoubleMin, 2.0), 1.0);
  EXPECT_EQ(weibullCDF(1.0, kDoubleMax, 3.0), 1.668805393880401e-308);
  EXPECT_EQ(weibullCDF(kInf, 1.0, 3.0), 1.0);
  EXPECT_EQ(weibullCDF(1.0, kInf, 20.0), 0.0);
  EXPECT_EQ(weibullCDF(kDoubleMin, kDoubleMin, 1.0), 0.63212055882855767);
  EXPECT_EQ(weibullCDF(kDoubleMax, kDoubleMax, 4.0), 0.0);
  EXPECT_EQ(weibullCDF(kDoubleMax, kDoubleMin, kInf), 1.0);
  EXPECT_EQ(weibullCDF(kInf, kInf, 10.0), 0.0);
  EXPECT_EQ(weibullCDF(1.0, 1.0, kInf), 1.0);
  EXPECT_EQ(weibullCDF(99999999999999, 999999999999999, kInf), 1.0);
  EXPECT_EQ(weibullCDF(kInf, 1.0, 40.0), 1.0);
  EXPECT_EQ(weibullCDF(1.0, kInf, 10.0), 0.0);
  EXPECT_THAT(weibullCDF(1.0, 0.5, kNan), IsNan());
  EXPECT_THAT(weibullCDF(99999999999999.0, 999999999999999.0, kNan), IsNan());

  VELOX_ASSERT_THROW(
      weibullCDF(kNan, kNan, kDoubleMin), "a must be greater than 0");
  VELOX_ASSERT_THROW(weibullCDF(0, 3, 0.5), "a must be greater than 0");
  VELOX_ASSERT_THROW(weibullCDF(3, 0, 0.5), "b must be greater than 0");
  VELOX_ASSERT_THROW(weibullCDF(kNan, 3.0, 0.5), "a must be greater than 0");
  VELOX_ASSERT_THROW(weibullCDF(3.0, kNan, 0.5), "b must be greater than 0");
  VELOX_ASSERT_THROW(weibullCDF(-1.0, 1.0, 30.0), "a must be greater than 0");
  VELOX_ASSERT_THROW(weibullCDF(1.0, -1.0, 40.0), "b must be greater than 0");
  VELOX_ASSERT_THROW(
      weibullCDF(kNan, kDoubleMin, kDoubleMax), "a must be greater than 0");
  VELOX_ASSERT_THROW(
      weibullCDF(kDoubleMin, kNan, kDoubleMax), "b must be greater than 0");
}

} // namespace
} // namespace facebook::velox
