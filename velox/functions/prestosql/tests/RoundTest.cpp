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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;

class RoundTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename T>
  void runRoundTest(const std::vector<std::tuple<T, T>>& data) {
    auto result = evaluate<SimpleVector<T>>(
        "round(c0)", makeRowVector({makeFlatVector<T, 0>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<1>(data[i]));
    }
  }

  template <typename T>
  void runRoundWithDecimalTest(
      const std::vector<std::tuple<T, int32_t, T>>& data) {
    auto result = evaluate<SimpleVector<T>>(
        "round(c0, c1)",
        makeRowVector(
            {makeFlatVector<T, 0>(data), makeFlatVector<int32_t, 1>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<2>(data[i]));
    }
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundFloatData() {
    return {
        {1.0, 1.0},
        {1.9, 2.0},
        {1.3, 1.0},
        {0.0, 0.0},
        {0.9999, 1.0},
        {-0.9999, -1.0},
        {1.0 / 9999999, 0},
        {123123123.0 / 9999999, 12.0}};
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundIntegralData() {
    return {{1, 1}, {0, 0}, {-1, -1}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecFloatData() {
    return {
        {3.8636365, 2, 3.86},     {-2.3622048, 2, -2.36},
        {1.6806724, 2, 1.68},     {2.3648648, 2, 2.36},
        {-5.970149, 2, -5.97},    {1.5931978, 2, 1.59},
        {-5.965909, 2, -5.97},    {-1.831502, 2, -1.83},
        {-4.971341, 2, -4.97},    {-1.4285715, 2, -1.43},
        {-6.965645, 2, -6.97},    {1.122112, 0, 1},
        {1.45, 1, 1.5},           {-1.45, 1, -1.5},
        {-0.60265756, 1, -0.6},   {-0.60265756, 2, -0.6},
        {-0.60265756, 3, -0.603}, {-0.60265756, 4, -0.6027},
        {1.129, 1, 1.1},          {1.129, 2, 1.13},
        {1.0 / 3, 0, 0.0},        {1.0 / 3, 1, 0.3},
        {1.0 / 3, 2, 0.33},       {1.0 / 3, 10, 0.3333333333},
        {-1.122112, 0, -1},       {-1.129, 1, -1.1},
        {-1.129, 2, -1.13},       {-1.129, 2, -1.13},
        {-1.0 / 3, 0, 0.0},       {-1.0 / 3, 1, -0.3},
        {-1.0 / 3, 2, -0.33},     {-1.0 / 3, 10, -0.3333333333},
        {1.0, -1, 0.0},           {0.0, -2, 0.0},
        {-1.0, -3, 0.0},          {11111.0, -1, 11110.0},
        {11111.0, -2, 11100.0},   {11111.0, -3, 11000.0},
        {11111.0, -4, 10000.0},
    };
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecIntegralData() {
    return {
        {1, 0, 1},
        {0, 0, 0},
        {-1, 0, -1},
        {1, 1, 1},
        {0, 1, 0},
        {-1, 1, -1},
        {1, 10, 1},
        {0, 10, 0},
        {-1, 10, -1},
        {1, -1, 1},
        {0, -2, 0},
        {-1, -3, -1}};
  }
};

TEST_F(RoundTest, round) {
  runRoundTest<float>(testRoundFloatData<float>());
  runRoundTest<double>(testRoundFloatData<double>());

  runRoundTest<int64_t>(testRoundIntegralData<int64_t>());
  runRoundTest<int32_t>(testRoundIntegralData<int32_t>());
  runRoundTest<int16_t>(testRoundIntegralData<int16_t>());
  runRoundTest<int8_t>(testRoundIntegralData<int8_t>());
}

TEST_F(RoundTest, roundWithDecimal) {
  runRoundWithDecimalTest<float>(testRoundWithDecFloatData<float>());
  runRoundWithDecimalTest<double>(testRoundWithDecFloatData<double>());

  runRoundWithDecimalTest<int64_t>(testRoundWithDecIntegralData<int64_t>());
  runRoundWithDecimalTest<int32_t>(testRoundWithDecIntegralData<int32_t>());
  runRoundWithDecimalTest<int16_t>(testRoundWithDecIntegralData<int16_t>());
  runRoundWithDecimalTest<int8_t>(testRoundWithDecIntegralData<int8_t>());
}

TEST_F(RoundTest, roundWithDecimalLargeNumbers) {
  for (int32_t i = 0; i < 64; ++i) {
    runRoundWithDecimalTest<double>({
        {9223372036854775807, i, 9223372036854775807},
        {1941561021063124736, i, 1941561021063124736},
        {194156102106312473, i, 194156102106312473},
        {19415610210631247, i, 19415610210631247},
        {281474976710655, i, 281474976710655},
        {17592186044415, i, 17592186044415},
        {1099511627775, i, 1099511627775},
        {68719476735, i, 68719476735},
        {4294967295, i, 4294967295},
    });
    runRoundWithDecimalTest<float>({
        {9223372036854775807, i, 9223372036854775807},
        {1941561021063124736, i, 1941561021063124736},
        {194156102106312473, i, 194156102106312473},
        {19415610210631247, i, 19415610210631247},
        {281474976710655, i, 281474976710655},
        {17592186044415, i, 17592186044415},
        {1099511627775, i, 1099511627775},
        {68719476735, i, 68719476735},
        {4294967295, i, 4294967295},
        {268435455, i, 268435455},
    });
    runRoundWithDecimalTest<int64_t>({
        {9223372036854775807, i, 9223372036854775807},
        {1941561021063124736, i, 1941561021063124736},
        {194156102106312473, i, 194156102106312473},
        {19415610210631247, i, 19415610210631247},
        {281474976710655, i, 281474976710655},
        {17592186044415, i, 17592186044415},
        {1099511627775, i, 1099511627775},
        {68719476735, i, 68719476735},
        {4294967295, i, 4294967295},
        {268435455, i, 268435455},
    });
  }

  for (int32_t i = 0; i < 64; ++i) {
    runRoundWithDecimalTest<double>(
        {{std::numeric_limits<double>::max(),
          i,
          std::numeric_limits<double>::max()}});
  };

  runRoundWithDecimalTest<double>({
      {1941561021063124736.458, 1, 1941561021063124736.5},
      {194156102106312473.314, 2, 194156102106312473.31},
      {19415610210631247.874, 3, 19415610210631247.874},
      {281474976710655.87434, 4, 281474976710655.8743},
      {17592186044415.87434, 4, 17592186044415.8743},
      {1099511627775.86443, 4, 1099511627775.8644},
      {68719476735.86443, 4, 68719476735.8644},
      {4294967295.86443, 4, 4294967295.8644},
      {1941561021063124736.8636365, 2, 1941561021063124736.86},
      {194156102106312473.8636365, 2, 194156102106312473.86},
      {-19415610210631247.8636365, 2, -19415610210631247.86},
      {281474976710655.8636365, 2, 281474976710655.86},
      {17592186044415.8636365, 2, 17592186044415.86},
      {-1099511627775.8636365, 2, -1099511627775.86},
      {68719476735.8636365, 2, 68719476735.86},
      {4294967295.868636365443, 2, 4294967295.87},
      {-268435455.868636365443, 2, -268435455.87},
  });
}
