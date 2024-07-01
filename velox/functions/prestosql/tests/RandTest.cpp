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

namespace facebook::velox::functions {

namespace {

constexpr double kInf = std::numeric_limits<double>::infinity();
constexpr double kNan = std::numeric_limits<double>::quiet_NaN();
constexpr float kInfF = std::numeric_limits<float>::infinity();
constexpr float kNanF = std::numeric_limits<float>::quiet_NaN();
constexpr int64_t kLongMax = std::numeric_limits<int64_t>::max();
constexpr int64_t kLongMin = std::numeric_limits<int64_t>::min();

class RandTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> random(T n) {
    return evaluateOnce<T>("random(c0)", std::make_optional(n));
  }

  template <typename T>
  std::optional<T> rand(T n) {
    return evaluateOnce<T>("rand(c0)", std::make_optional(n));
  }

  template <typename T>
  std::optional<T> randomWithTry(T n) {
    return evaluateOnce<T>("try(random(c0))", std::make_optional(n));
  }

  template <typename T>
  std::optional<T> randWithTry(T n) {
    return evaluateOnce<T>("try(rand(c0))", std::make_optional(n));
  }

  template <typename T>
  std::optional<T> secureRandom(
      std::optional<T> lower,
      std::optional<T> upper) {
    return evaluateOnce<T>("secure_random(c0, c1)", lower, upper);
  }

  template <typename T>
  std::optional<T> secureRand(std::optional<T> lower, std::optional<T> upper) {
    return evaluateOnce<T>("secure_rand(c0, c1)", lower, upper);
  }
};

TEST_F(RandTest, zeroArg) {
  auto result = evaluateOnce<double>("random()", makeRowVector(ROW({}), 1));
  EXPECT_LT(result, 1.0);
  EXPECT_GE(result, 0.0);

  result = evaluateOnce<double>("rand()", makeRowVector(ROW({}), 1));
  EXPECT_LT(result, 1.0);
  EXPECT_GE(result, 0.0);
}

TEST_F(RandTest, negativeInt32) {
  VELOX_ASSERT_THROW(random(-5), "bound must be positive");
  ASSERT_EQ(randomWithTry(-5), std::nullopt);

  VELOX_ASSERT_THROW(rand(-5), "bound must be positive");
  ASSERT_EQ(randWithTry(-5), std::nullopt);
}

TEST_F(RandTest, nonNullInt64) {
  EXPECT_LT(random(346), 346);
  EXPECT_LT(rand(346), 346);
}

TEST_F(RandTest, nonNullInt8) {
  EXPECT_LT(random(4), 4);
  EXPECT_LT(rand(4), 4);
}

TEST_F(RandTest, secureRandZeroArg) {
  auto result =
      evaluateOnce<double>("secure_random()", makeRowVector(ROW({}), 1));
  EXPECT_LT(result, 1.0);
  EXPECT_GE(result, 0.0);

  result = evaluateOnce<double>("secure_rand()", makeRowVector(ROW({}), 1));
  EXPECT_LT(result, 1.0);
  EXPECT_GE(result, 0.0);
}

TEST_F(RandTest, secureRandInt64) {
  auto result =
      secureRand<int64_t>((int64_t)-2147532562, (int64_t)4611791058295013614);
  EXPECT_LT(result, 4611791058295013614);
  EXPECT_GE(result, -2147532562);

  result = secureRand<int64_t>((int64_t)0, (int64_t)46117910582950136);
  EXPECT_LT(result, 46117910582950136);
  EXPECT_GE(result, 0);

  result = secureRand<int64_t>(
      std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
  EXPECT_LT(result, std::numeric_limits<int64_t>::max());
  EXPECT_GE(result, std::numeric_limits<int64_t>::min());
}

TEST_F(RandTest, secureRandInt32) {
  auto result = secureRand<int32_t>((int32_t)8765432, (int32_t)2145613151);
  EXPECT_LT(result, 2145613151);
  EXPECT_GE(result, 8765432);

  result = secureRand<int32_t>((int32_t)0, (int32_t)21456131);
  EXPECT_LT(result, 21456131);
  EXPECT_GE(result, 0);
}

TEST_F(RandTest, secureRandInt16) {
  auto result = secureRand<int16_t>((int16_t)-100, (int16_t)23286);
  EXPECT_LT(result, 23286);
  EXPECT_GE(result, -100);

  result = secureRand<int16_t>((int16_t)0, (int16_t)23286);
  EXPECT_LT(result, 23286);
  EXPECT_GE(result, 0);
}

TEST_F(RandTest, secureRandInt8) {
  auto result = secureRand<int8_t>((int8_t)10, (int8_t)120);
  EXPECT_LT(result, 120);
  EXPECT_GE(result, 10);

  result = secureRand<int8_t>((int8_t)0, (int8_t)120);
  EXPECT_LT(result, 120);
  EXPECT_GE(result, 0);
}

TEST_F(RandTest, secureRandDouble) {
  auto result = secureRand<double>((double)10.5, (double)120.7895);
  EXPECT_LT(result, 120.7895);
  EXPECT_GE(result, 10.5);

  result = secureRand<double>((double)0.0, (double)120.7895);
  EXPECT_LT(result, 120.7895);
  EXPECT_GE(result, 0.0);
}

TEST_F(RandTest, secureRandFloat) {
  auto result = secureRand<float>((float)-10.5, (float)120.7);
  EXPECT_LT(result, 120.7);
  EXPECT_GE(result, -10.5);

  result = secureRand<float>((float)0.0, (float)120.7);
  EXPECT_LT(result, 120.7);
  EXPECT_GE(result, 0.0);
}

TEST_F(RandTest, secureRandInvalid) {
  VELOX_ASSERT_THROW(
      secureRand<int64_t>((int64_t)-5, (int64_t)-10),
      "upper bound must be greater than lower bound");
  VELOX_ASSERT_THROW(
      secureRand<int64_t>((int64_t)15, (int64_t)10),
      "upper bound must be greater than lower bound");
  VELOX_ASSERT_THROW(
      secureRand<int32_t>((int32_t)5, (int32_t)-10),
      "upper bound must be greater than lower bound");
  VELOX_ASSERT_THROW(
      secureRand<int32_t>((int32_t)15, (int32_t)10),
      "upper bound must be greater than lower bound");
  VELOX_ASSERT_THROW(
      secureRand<int16_t>((int16_t)-5, (int16_t)-10),
      "upper bound must be greater than lower bound");
  VELOX_ASSERT_THROW(
      secureRand<int16_t>((int16_t)15, (int16_t)10),
      "upper bound must be greater than lower bound");
  VELOX_ASSERT_THROW(
      secureRand<int8_t>((int8_t)5, (int8_t)-10),
      "upper bound must be greater than lower bound");
  VELOX_ASSERT_THROW(
      secureRand<int8_t>((int8_t)15, (int8_t)10),
      "upper bound must be greater than lower bound");
  VELOX_ASSERT_THROW(
      secureRand<double>(-5.7, -10.7),
      "upper bound must be greater than lower bound");
  VELOX_ASSERT_THROW(
      secureRand<double>(15.6, 10.1),
      "upper bound must be greater than lower bound");
  VELOX_ASSERT_THROW(
      secureRand<float>((float)-5.7, (float)-10.7),
      "upper bound must be greater than lower bound");
  VELOX_ASSERT_THROW(
      secureRand<float>((float)15.6, (float)10.1),
      "upper bound must be greater than lower bound");
}

TEST_F(RandTest, secureRandSpecialValues) {
  EXPECT_LT(secureRand<int64_t>(0, kLongMax), kLongMax);
  EXPECT_GE(secureRand<int64_t>(kLongMin, 0), kLongMin);

  EXPECT_TRUE(std::isnan(secureRand<float>(-kInfF, 0).value()));
  EXPECT_LE(secureRand<float>(0.0, kInfF), std::numeric_limits<float>::max());

  EXPECT_TRUE(std::isnan(secureRand<double>(-kInf, 0).value()));
  EXPECT_EQ(secureRand<double>(0.0, kInf), std::numeric_limits<double>::max());

  VELOX_ASSERT_THROW(
      secureRand<double>(0.0, kNan),
      "upper bound must be greater than lower bound");

  VELOX_ASSERT_THROW(
      secureRand<float>(0.0, kNanF),
      "upper bound must be greater than lower bound");
}

} // namespace
} // namespace facebook::velox::functions
