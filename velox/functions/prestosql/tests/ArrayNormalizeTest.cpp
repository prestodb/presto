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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace {

class ArrayNormalizeTest : public FunctionBaseTest {
 protected:
  void testExpr(
      const VectorPtr& expected,
      const std::vector<VectorPtr>& input) {
    auto result = evaluate("array_normalize(C0, C1)", makeRowVector(input));
    assertEqualVectors(expected, result);
  }

  // Helper function to calculate p-norm based on its definition
  // and normalize the input array.
  template <typename T>
  std::vector<T> normalize(const std::vector<T>& values, T p) {
    T sum = 0;
    for (const auto& item : values) {
      sum += pow(abs(item), p);
    }

    T pNorm = pow(sum, 1.0 / p);

    std::vector<T> res;
    for (T value : values) {
      res.push_back(value / pNorm);
    }

    return res;
  }

  template <typename T>
  void testArrayWithElementsEqualZero() {
    auto input = makeArrayVector<T>(
        {{static_cast<T>(0.0), static_cast<T>(-0.0), static_cast<T>(0.0)}});
    auto p = makeConstant<T>(static_cast<T>(2.0), input->size());
    testExpr(input, {input, p});
  }

  template <typename T>
  void testArrayWithPLessThanZero() {
    auto input = makeArrayVector<T>(
        {{static_cast<T>(1.0), static_cast<T>(2.0), static_cast<T>(3.0)}});
    auto p = makeConstant<T>(static_cast<T>(-1.0), input->size());
    VELOX_ASSERT_THROW(
        testExpr(input, {input, p}),
        "array_normalize only supports non-negative p");
  }

  template <typename T>
  void testArrayWithPEqualZero() {
    auto vector = makeArrayVector<T>(
        {{static_cast<T>(1.0), static_cast<T>(-2.0), static_cast<T>(3.0)}});
    auto p = makeConstant<T>(static_cast<T>(0.0), vector->size());
    testExpr(vector, {vector, p});
  }

  template <typename T>
  void testArrayWithPLessThanOne() {
    T pValue = 0.5;
    std::vector<T> values = {1.0, -4.0, 9.0};
    std::vector<T> expected = normalize(values, pValue);

    auto input = makeArrayVector<T>({values});
    auto p = makeConstant<T>(pValue, input->size());
    testExpr(makeArrayVector<T>({expected}), {input, p});
  }

  template <typename T>
  void testArrayWithPEqualOne() {
    T pValue = 1.0;
    std::vector<T> values = {1, -2, 3};
    std::vector<T> expected = normalize(values, pValue);

    auto input = makeArrayVector<T>({values});
    auto p = makeConstant<T>(pValue, input->size());
    testExpr(makeArrayVector<T>({expected}), {input, p});
  }

  template <typename T>
  void testArrayWithTypeLimits() {
    auto input = makeArrayVector<T>({
        {},
        {0},
        {1},
        {std::numeric_limits<T>::infinity()},
        {std::numeric_limits<T>::quiet_NaN()},
        {std::numeric_limits<T>::lowest(), -1.0},
        {std::numeric_limits<T>::max(), 1.0},
    });
    auto p = makeConstant<T>(2.0, input->size());
    auto expected = makeArrayVector<T>(
        {{},
         {0},
         {1},
         {std::numeric_limits<T>::quiet_NaN()},
         {std::numeric_limits<T>::quiet_NaN()},
         // For the cases below, pNorm is calculated as Infinity, resulting in
         // zero values. The result matches the Presto Java version.
         {-0.0, -0.0},
         {0.0, 0.0}});
    testExpr(expected, {input, p});
  }

  template <typename T>
  void testArrayWithNullValues() {
    auto input = makeNullableArrayVector<T>(
        {{{1, 2, std::nullopt}}, {{std::nullopt}}, std::nullopt});
    auto p = makeConstant<T>(2.0, input->size());
    auto expected =
        makeNullableArrayVector<T>({std::nullopt, std::nullopt, std::nullopt});
    testExpr(expected, {input, p});
  }

  template <typename T>
  void testArrayWithDifferentValues() {
    T pValue = 4.0;

    std::vector<std::vector<T>> inputs = {
        {1.0},
        {-1.0, -1.0},
        {1.0, 4.0, 9.0},
        {0.1, 0.01, 1.0, 0.0},
        {2.34, 4.56, 8.12},
        {-2.34, 4.56, -8.12}};

    std::vector<std::vector<T>> expectedResults;
    for (const auto& input : inputs) {
      expectedResults.push_back(normalize(input, pValue));
    }

    auto input = makeArrayVector<T>(inputs);
    auto p = makeConstant<T>(pValue, input->size());
    // The test results were also calculated manually and checked against
    // Presto Java version.
    testExpr(makeArrayVector<T>(expectedResults), {input, p});
  }
};

TEST_F(ArrayNormalizeTest, arrayWithElementsZero) {
  testArrayWithElementsEqualZero<int>();
  testArrayWithElementsEqualZero<float>();
  testArrayWithElementsEqualZero<double>();
}

TEST_F(ArrayNormalizeTest, pLessThanZero) {
  testArrayWithElementsEqualZero<int>();
  testArrayWithPLessThanZero<float>();
  testArrayWithPLessThanZero<double>();
}

TEST_F(ArrayNormalizeTest, pEqualsZero) {
  testArrayWithPEqualZero<int>();
  testArrayWithPEqualZero<float>();
  testArrayWithPEqualZero<double>();
}

TEST_F(ArrayNormalizeTest, pLessThanOne) {
  testArrayWithPLessThanOne<float>();
  testArrayWithPLessThanOne<double>();
}

TEST_F(ArrayNormalizeTest, pEqualsOne) {
  testArrayWithPEqualOne<int>();
  testArrayWithPEqualOne<float>();
  testArrayWithPEqualOne<double>();
}

TEST_F(ArrayNormalizeTest, limits) {
  testArrayWithPEqualOne<int>();
  testArrayWithTypeLimits<float>();
  testArrayWithTypeLimits<double>();
}

TEST_F(ArrayNormalizeTest, nullValues) {
  testArrayWithPEqualOne<int>();
  testArrayWithNullValues<float>();
  testArrayWithNullValues<double>();
}

TEST_F(ArrayNormalizeTest, differentValues) {
  testArrayWithPEqualOne<int>();
  testArrayWithDifferentValues<float>();
  testArrayWithDifferentValues<double>();
}

} // namespace
