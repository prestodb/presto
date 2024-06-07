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

#include <limits>
#include <optional>
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/Timestamp.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class ArrayMaxTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> arrayMax(const std::vector<std::optional<T>>& input) {
    auto row = makeRowVector({makeNullableArrayVector(
        std::vector<std::vector<std::optional<T>>>{input})});
    return evaluateOnce<T>("array_max(C0)", row);
  }
};

TEST_F(ArrayMaxTest, boolean) {
  EXPECT_EQ(arrayMax<bool>({true, false}), true);
  EXPECT_EQ(arrayMax<bool>({true}), true);
  EXPECT_EQ(arrayMax<bool>({false}), false);
  EXPECT_EQ(arrayMax<bool>({}), std::nullopt);
  EXPECT_EQ(arrayMax<bool>({true, false, true, std::nullopt}), true);
  EXPECT_EQ(arrayMax<bool>({std::nullopt, true, false, true}), true);
  EXPECT_EQ(arrayMax<bool>({false, false, false}), false);
  EXPECT_EQ(arrayMax<bool>({true, true, true}), true);
}

TEST_F(ArrayMaxTest, varchar) {
  EXPECT_EQ(arrayMax<std::string>({"red", "blue"}), "red");
  EXPECT_EQ(
      arrayMax<std::string>({std::nullopt, "blue", "yellow", "orange"}),
      "yellow");
  EXPECT_EQ(arrayMax<std::string>({}), std::nullopt);
  EXPECT_EQ(arrayMax<std::string>({std::nullopt}), std::nullopt);
}

// Test non-inlined (> 12 length) nullable strings.
TEST_F(ArrayMaxTest, longVarchar) {
  EXPECT_EQ(
      arrayMax<std::string>({"red shiny car ahead", "blue clear sky above"}),
      "red shiny car ahead");
  EXPECT_EQ(
      arrayMax<std::string>(
          {std::nullopt,
           "blue clear sky above",
           "yellow rose flowers",
           "orange beautiful sunset"}),
      "yellow rose flowers");
  EXPECT_EQ(arrayMax<std::string>({}), std::nullopt);
  EXPECT_EQ(
      arrayMax<std::string>(
          {"red shiny car ahead",
           "purple is an elegant color",
           "green plants make us happy"}),
      "red shiny car ahead");
}

TEST_F(ArrayMaxTest, timestamp) {
  auto ts = [](int64_t micros) { return Timestamp::fromMicros(micros); };
  EXPECT_EQ(arrayMax<Timestamp>({ts(0), ts(1)}), ts(1));
  EXPECT_EQ(
      arrayMax<Timestamp>({ts(0), ts(1), Timestamp::max(), Timestamp::min()}),
      Timestamp::max());
  EXPECT_EQ(arrayMax<Timestamp>({}), std::nullopt);
  EXPECT_EQ(arrayMax<Timestamp>({ts(0), std::nullopt}), ts(0));
}

template <typename Type>
class ArrayMaxIntegralTest : public ArrayMaxTest {
 public:
  using NATIVE_TYPE = typename Type::NativeType::NativeType;
};

TYPED_TEST_SUITE(ArrayMaxIntegralTest, FunctionBaseTest::IntegralTypes);

TYPED_TEST(ArrayMaxIntegralTest, basic) {
  using T = typename TestFixture::NATIVE_TYPE;
  EXPECT_EQ(
      this->template arrayMax<T>(
          {std::numeric_limits<T>::min(),
           0,
           1,
           2,
           3,
           std::numeric_limits<T>::max()}),
      std::numeric_limits<T>::max());
  EXPECT_EQ(
      this->template arrayMax<T>(
          {std::numeric_limits<T>::max(),
           3,
           2,
           1,
           0,
           -1,
           std::numeric_limits<T>::min()}),
      std::numeric_limits<T>::max());
  EXPECT_EQ(
      this->template arrayMax<T>(
          {101, 102, 103, std::numeric_limits<T>::max(), std::nullopt}),
      std::numeric_limits<T>::max());
  EXPECT_EQ(
      this->template arrayMax<T>(
          {std::nullopt, -1, -2, -3, std::numeric_limits<T>::min()}),
      -1);
  EXPECT_EQ(this->template arrayMax<T>({}), std::nullopt);
  EXPECT_EQ(this->template arrayMax<T>({std::nullopt}), std::nullopt);
}

template <typename Type>
class ArrayMaxFloatingPointTest : public ArrayMaxTest {
 public:
  using NATIVE_TYPE = typename Type::NativeType::NativeType;
};

TYPED_TEST_SUITE(
    ArrayMaxFloatingPointTest,
    FunctionBaseTest::FloatingPointTypes);

TYPED_TEST(ArrayMaxFloatingPointTest, basic) {
  using T = typename TestFixture::NATIVE_TYPE;
  static constexpr T kMin = std::numeric_limits<T>::lowest();
  static constexpr T kMax = std::numeric_limits<T>::max();
  static constexpr T kNaN = std::numeric_limits<T>::quiet_NaN();

  EXPECT_FLOAT_EQ(
      this->template arrayMax<T>({0.0000, 0.00001}).value(), 0.00001);
  EXPECT_FLOAT_EQ(
      this->template arrayMax<T>({std::nullopt, 1.1, 1.11, -2.2, -1.0, kMin})
          .value(),
      1.11);
  EXPECT_EQ(this->template arrayMax<T>({}), std::nullopt);
  EXPECT_FLOAT_EQ(
      this->template arrayMax<T>({kMin, 1.1, 1.22222, 1.33, std::nullopt})
          .value(),
      1.33);
  EXPECT_FLOAT_EQ(
      this->template arrayMax<T>({-0.00001, -0.0002, 0.0001}).value(), 0.0001);

  EXPECT_TRUE(std::isnan(
      this->template arrayMax<T>({kMin, -0.0001, -0.0002, -0.0003, kMax, kNaN})
          .value()));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
