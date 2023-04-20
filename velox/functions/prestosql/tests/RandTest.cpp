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

class RandTest : public functions::test::FunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> random(T n) {
    return evaluateOnce<T>("random(c0)", std::make_optional(n));
  }

  template <typename T>
  std::optional<T> randomWithTry(T n) {
    return evaluateOnce<T>("try(random(c0))", std::make_optional(n));
  }
};

TEST_F(RandTest, zeroArg) {
  auto result = evaluateOnce<double>("random()", makeRowVector(ROW({}), 1));
  EXPECT_LT(result, 1.0);
  EXPECT_GE(result, 0.0);
}

TEST_F(RandTest, negativeInt32) {
  VELOX_ASSERT_THROW(random(-5), "bound must be positive");
  ASSERT_EQ(randomWithTry(-5), std::nullopt);
}

TEST_F(RandTest, nonNullInt64) {
  auto result = random(346);
  EXPECT_LT(result, 346);
}

TEST_F(RandTest, nonNullInt8) {
  auto result = random(4);
  EXPECT_LT(result, 4);
}

} // namespace
} // namespace facebook::velox::functions
