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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

class FloatingPointFunctionsTest : public FunctionBaseTest {};

TEST_F(FloatingPointFunctionsTest, xxHash64FunctionReal) {
  const auto xxhash64 = [&](std::optional<float> value) {
    return evaluateOnce<int64_t>("xxhash64_internal(c0)", REAL(), value);
  };

  EXPECT_EQ(std::nullopt, xxhash64(std::nullopt));
  EXPECT_EQ(8886770145897159885, xxhash64(1.0f));
  EXPECT_EQ(4246796580750024372, xxhash64(0.0f));
  EXPECT_EQ(-4550560479179327389, xxhash64(42.0f));
  EXPECT_EQ(8009987342969823011, xxhash64(-100.0f));
  // Special values
  EXPECT_EQ(6518298350859968348, xxhash64(std::numeric_limits<float>::max()));
  EXPECT_EQ(
      3321754807340147608, xxhash64(std::numeric_limits<float>::lowest()));
  EXPECT_EQ(
      -6888604247985941064, xxhash64(std::numeric_limits<float>::infinity()));
  EXPECT_EQ(
      -191236949008491052, xxhash64(-std::numeric_limits<float>::infinity()));
}

TEST_F(FloatingPointFunctionsTest, xxHash64FunctionDouble) {
  const auto xxhash64 = [&](std::optional<double> value) {
    return evaluateOnce<int64_t>("xxhash64_internal(c0)", DOUBLE(), value);
  };

  EXPECT_EQ(std::nullopt, xxhash64(std::nullopt));
  EXPECT_EQ(-7740241964680764523, xxhash64(1.0));
  EXPECT_EQ(3803688792395291579, xxhash64(0.0));
  EXPECT_EQ(3080069184023342994, xxhash64(42.0));
  EXPECT_EQ(-5665257728560723920, xxhash64(-100.0));
  // Special values
  EXPECT_EQ(3127544388062992779, xxhash64(std::numeric_limits<double>::max()));
  EXPECT_EQ(
      -4676301161353224861, xxhash64(std::numeric_limits<double>::lowest()));
  EXPECT_EQ(
      -415002444789238011, xxhash64(std::numeric_limits<double>::infinity()));
  EXPECT_EQ(
      3642075027047404498, xxhash64(-std::numeric_limits<double>::infinity()));
}
