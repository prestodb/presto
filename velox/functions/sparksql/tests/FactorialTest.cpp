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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class FactorialTest : public SparkFunctionBaseTest {
 protected:
  std::optional<int64_t> factorial(std::optional<int32_t> n) {
    return evaluateOnce<int64_t>("factorial(c0)", n);
  }
};

TEST_F(FactorialTest, factorialBasic) {
  EXPECT_EQ(1, factorial(0));
  EXPECT_EQ(1, factorial(1));
  EXPECT_EQ(2, factorial(2));
  EXPECT_EQ(120, factorial(5));
  EXPECT_EQ(3628800, factorial(10));
  EXPECT_EQ(1307674368000L, factorial(15));
  EXPECT_EQ(2432902008176640000L, factorial(20));
}

TEST_F(FactorialTest, factorialNullInput) {
  EXPECT_EQ(std::nullopt, factorial(std::nullopt));
}

TEST_F(FactorialTest, factorialOutOfRange) {
  EXPECT_EQ(std::nullopt, factorial(-1));
  EXPECT_EQ(std::nullopt, factorial(21));
  EXPECT_EQ(std::nullopt, factorial(-5));
  EXPECT_EQ(std::nullopt, factorial(25));
}

TEST_F(FactorialTest, factorialMixedInputs) {
  EXPECT_EQ(6, factorial(3));
  EXPECT_EQ(120, factorial(5));
  EXPECT_EQ(std::nullopt, factorial(std::nullopt));
  EXPECT_EQ(std::nullopt, factorial(25));
  EXPECT_EQ(std::nullopt, factorial(-3));
  EXPECT_EQ(3628800, factorial(10));
  EXPECT_EQ(1307674368000L, factorial(15));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
