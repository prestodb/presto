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

#include <gtest/gtest.h>
#include <limits>
#include <optional>
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/Timestamp.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class ArrayMinTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> arrayMin(
      const std::vector<std::optional<T>>& input,
      const TypePtr& type = ARRAY(CppToType<T>::create())) {
    auto row = makeRowVector({makeNullableArrayVector(
        std::vector<std::vector<std::optional<T>>>{input}, type)});
    return evaluateOnce<T>("array_min(C0)", row);
  }
};

TEST_F(ArrayMinTest, boolean) {
  EXPECT_EQ(arrayMin<bool>({true, false}), false);
  EXPECT_EQ(arrayMin<bool>({true}), true);
  EXPECT_EQ(arrayMin<bool>({false}), false);
  EXPECT_EQ(arrayMin<bool>({}), std::nullopt);
  EXPECT_EQ(arrayMin<bool>({true, false, true, std::nullopt}), false);
  EXPECT_EQ(arrayMin<bool>({std::nullopt, true, false, true}), false);
  EXPECT_EQ(arrayMin<bool>({false, false, false}), false);
  EXPECT_EQ(arrayMin<bool>({true, true, true}), true);
}

TEST_F(ArrayMinTest, varbinary) {
  EXPECT_EQ(arrayMin<std::string>({"red", "blue"}, ARRAY(VARBINARY())), "blue");
  EXPECT_EQ(
      arrayMin<std::string>(
          {std::nullopt, "blue", "yellow", "orange"}, ARRAY(VARBINARY())),
      "blue");
  EXPECT_EQ(arrayMin<std::string>({}, ARRAY(VARBINARY())), std::nullopt);
  EXPECT_EQ(
      arrayMin<std::string>({std::nullopt}, ARRAY(VARBINARY())), std::nullopt);
}

TEST_F(ArrayMinTest, varchar) {
  EXPECT_EQ(arrayMin<std::string>({"red", "blue"}), "blue");
  EXPECT_EQ(
      arrayMin<std::string>({std::nullopt, "blue", "yellow", "orange"}),
      "blue");
  EXPECT_EQ(arrayMin<std::string>({}), std::nullopt);
  EXPECT_EQ(arrayMin<std::string>({std::nullopt}), std::nullopt);
}

// Test non-inlined (> 12 length) nullable strings.
TEST_F(ArrayMinTest, longVarchar) {
  EXPECT_EQ(
      arrayMin<std::string>({"red shiny car ahead", "blue clear sky above"}),
      "blue clear sky above");
  EXPECT_EQ(
      arrayMin<std::string>(
          {std::nullopt,
           "blue clear sky above",
           "yellow rose flowers",
           "orange beautiful sunset"}),
      "blue clear sky above");
  EXPECT_EQ(arrayMin<std::string>({}), std::nullopt);
  EXPECT_EQ(
      arrayMin<std::string>(
          {"red shiny car ahead",
           "purple is an elegant color",
           "green plants make us happy"}),
      "green plants make us happy");
}

TEST_F(ArrayMinTest, timestamp) {
  auto ts = [](int64_t micros) { return Timestamp::fromMicros(micros); };
  EXPECT_EQ(arrayMin<Timestamp>({ts(0), ts(1)}), ts(0));
  EXPECT_EQ(
      arrayMin<Timestamp>({ts(0), ts(1), Timestamp::max(), Timestamp::min()}),
      Timestamp::min());
  EXPECT_EQ(arrayMin<Timestamp>({}), std::nullopt);
  EXPECT_EQ(arrayMin<Timestamp>({ts(0), std::nullopt}), ts(0));
}

TEST_F(ArrayMinTest, complexTypes) {
  auto testExpression = [&](const VectorPtr& input, const VectorPtr& expected) {
    auto result = evaluate("array_min(c0)", makeRowVector({input}));
    assertEqualVectors(expected, result);
  };
  testExpression(
      makeNestedArrayVectorFromJson<int64_t>(
          {"[[1, 1, 1], [1, 1, 2], [1, 3, 1]]"}),
      makeArrayVectorFromJson<int64_t>({"[1, 1, 1]"}));

  testExpression(
      makeNestedArrayVectorFromJson<int64_t>(
          {"[[1, null], [null, 2], [null, null]]"}),
      makeArrayVectorFromJson<int64_t>({"[null, null]"}));

  testExpression(
      makeNestedArrayVectorFromJson<int64_t>({"[[1, null], [1, 2], [1]]"}),
      makeArrayVectorFromJson<int64_t>({"[1]"}));

  testExpression(
      makeNestedArrayVectorFromJson<int64_t>({"[[1], [null], []]"}),
      makeArrayVectorFromJson<int64_t>({"[]"}));

  testExpression(
      makeNestedArrayVectorFromJson<int64_t>({"[null, null]"}),
      makeArrayVectorFromJson<int64_t>({"null"}));
}

template <typename Type>
class ArrayMinIntegralTest : public ArrayMinTest {
 public:
  using NATIVE_TYPE = typename Type::NativeType;
};

TYPED_TEST_SUITE(ArrayMinIntegralTest, FunctionBaseTest::IntegralTypes);

TYPED_TEST(ArrayMinIntegralTest, basic) {
  using T = typename TestFixture::NATIVE_TYPE;
  EXPECT_EQ(
      this->template arrayMin<T>(
          {std::numeric_limits<T>::min(),
           0,
           1,
           2,
           3,
           std::numeric_limits<T>::max()}),
      std::numeric_limits<T>::min());
  EXPECT_EQ(
      this->template arrayMin<T>(
          {std::numeric_limits<T>::max(),
           3,
           2,
           1,
           0,
           -1,
           std::numeric_limits<T>::min()}),
      std::numeric_limits<T>::min());
  EXPECT_EQ(
      this->template arrayMin<T>(
          {101, 102, 103, std::numeric_limits<T>::max(), std::nullopt}),
      101);
  EXPECT_EQ(
      this->template arrayMin<T>(
          {std::nullopt, -1, -2, -3, std::numeric_limits<T>::min()}),
      std::numeric_limits<T>::min());
  EXPECT_EQ(this->template arrayMin<T>({}), std::nullopt);
  EXPECT_EQ(this->template arrayMin<T>({std::nullopt}), std::nullopt);
}

template <typename Type>
class ArrayMinFloatingPointTest : public ArrayMinTest {
 public:
  using NATIVE_TYPE = typename Type::NativeType;
};

TYPED_TEST_SUITE(
    ArrayMinFloatingPointTest,
    FunctionBaseTest::FloatingPointTypes);

TYPED_TEST(ArrayMinFloatingPointTest, basic) {
  using T = typename TestFixture::NATIVE_TYPE;
  static constexpr T kMin = std::numeric_limits<T>::lowest();
  static constexpr T kMax = std::numeric_limits<T>::max();
  static constexpr T kNaN = std::numeric_limits<T>::quiet_NaN();

  EXPECT_EQ(this->template arrayMin<T>({0.0000, 0.00001}), 0.0000);
  EXPECT_EQ(
      this->template arrayMin<T>({std::nullopt, 1.1, 1.11, -2.2, -1.0, kMin}),
      kMin);
  EXPECT_EQ(this->template arrayMin<T>({}), std::nullopt);
  EXPECT_EQ(
      this->template arrayMin<T>({kMin, 1.1, 1.22222, 1.33, std::nullopt}),
      kMin);
  EXPECT_FLOAT_EQ(
      this->template arrayMin<T>({-0.00001, -0.0002, 0.0001}).value(), -0.0002);
  EXPECT_FLOAT_EQ(
      this->template arrayMin<T>({-0.0001, -0.0002, kMax, kNaN}).value(),
      -0.0002);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
