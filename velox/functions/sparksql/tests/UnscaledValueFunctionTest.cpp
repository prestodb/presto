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
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class UnscaledValueFunctionTest : public SparkFunctionBaseTest {};

TEST_F(UnscaledValueFunctionTest, unscaledValue) {
  auto input =
      makeFlatVector<int64_t>({1000, 2000, -3000, -4000}, DECIMAL(18, 3));
  auto expected = makeFlatVector<int64_t>({1000, 2000, -3000, -4000});
  auto invalidInput = makeFlatVector<int64_t>({0, 0, 0, 0}, DECIMAL(20, 3));

  testEncodings(
      makeTypedExpr("unscaled_value(c0)", ROW({"c0"}, {input->type()})),
      {input},
      expected);

  VELOX_ASSERT_USER_THROW(
      evaluate("unscaled_value(c0)", makeRowVector({invalidInput})),
      "Expect short decimal type, but got: DECIMAL(20, 3)");
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
