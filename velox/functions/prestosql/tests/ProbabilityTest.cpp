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

MATCHER(IsNan, "is NaN") {
  return arg && std::isnan(*arg);
}

MATCHER(IsInf, "is Infinity") {
  return arg && std::isinf(*arg);
}

class ProbabilityTest : public functions::test::FunctionBaseTest {};

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

} // namespace
} // namespace facebook::velox
