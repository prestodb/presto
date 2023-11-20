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

namespace facebook::velox::functions::sparksql::test {
namespace {
class MakeDecimalTest : public SparkFunctionBaseTest {
 protected:
  core::CallTypedExprPtr createMakeDecimal(
      std::optional<bool> nullOnOverflow,
      const TypePtr& outputType,
      bool tryMakeDecimal) {
    std::vector<core::TypedExprPtr> inputs = {
        std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0")};
    if (nullOnOverflow.has_value()) {
      inputs.emplace_back(std::make_shared<core::ConstantTypedExpr>(
          BOOLEAN(), variant(nullOnOverflow.value())));
    }
    auto makeDecimal = std::make_shared<const core::CallTypedExpr>(
        outputType, std::move(inputs), "make_decimal");
    if (tryMakeDecimal) {
      return std::make_shared<core::CallTypedExpr>(
          outputType, std::vector<core::TypedExprPtr>{makeDecimal}, "try");
    } else {
      return makeDecimal;
    }
  }

  void testMakeDecimal(
      const VectorPtr& input,
      std::optional<bool> nullOnOverflow,
      const VectorPtr& expected,
      bool tryMakeDecimal = false) {
    auto expr =
        createMakeDecimal(nullOnOverflow, expected->type(), tryMakeDecimal);
    testEncodings(expr, {input}, expected);
  }
};

TEST_F(MakeDecimalTest, makeDecimal) {
  testMakeDecimal(
      makeFlatVector<int64_t>({1111, -1112, 9999, 0}),
      std::nullopt,
      makeFlatVector<int64_t>({1111, -1112, 9999, 0}, DECIMAL(5, 1)));
  testMakeDecimal(
      makeFlatVector<int64_t>({1111, -1112, 9999, 0}),
      true,
      makeFlatVector<int64_t>({1111, -1112, 9999, 0}, DECIMAL(5, 1)));
  testMakeDecimal(
      makeFlatVector<int64_t>(
          {11111111, -11112112, 99999999, DecimalUtil::kShortDecimalMax + 1}),
      true,
      makeFlatVector<int128_t>(
          {11111111, -11112112, 99999999, DecimalUtil::kShortDecimalMax + 1},
          DECIMAL(38, 19)));
  testMakeDecimal(
      makeFlatVector<int64_t>(
          {11111111, -11112112, 99999999, DecimalUtil::kShortDecimalMax + 1}),
      true,
      makeNullableFlatVector<int64_t>(
          {11111111, -11112112, 99999999, std::nullopt}, DECIMAL(18, 0)));
  VELOX_ASSERT_THROW(
      testMakeDecimal(
          makeFlatVector<int64_t>(
              {11111111,
               -11112112,
               99999999,
               DecimalUtil::kShortDecimalMax + 1}),
          false,
          makeNullableFlatVector<int64_t>(
              {11111111, -11112112, 99999999, std::nullopt}, DECIMAL(18, 0))),
      "Unscaled value 1000000000000000000 too large for precision 18.");
  testMakeDecimal(
      makeFlatVector<int64_t>(
          {11111111, -11112112, 99999999, DecimalUtil::kShortDecimalMax + 1}),
      false,
      makeNullableFlatVector<int64_t>(
          {11111111, -11112112, 99999999, std::nullopt}, DECIMAL(18, 0)),
      true /*tryMakeDecimal*/);
  testMakeDecimal(
      makeNullableFlatVector<int64_t>({101, std::nullopt, 1000}),
      true,
      makeNullableFlatVector<int64_t>(
          {101, std::nullopt, std::nullopt}, DECIMAL(3, 1)));
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
