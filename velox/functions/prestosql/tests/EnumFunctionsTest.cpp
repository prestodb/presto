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
#include "velox/functions/prestosql/types/BigintEnumType.h"
#include "velox/functions/prestosql/types/VarcharEnumType.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::functions::test;
using namespace facebook::velox::test;

namespace facebook::velox::functions {
namespace {

class EnumFunctionsTest : public FunctionBaseTest {
 protected:
  void SetUp() override {
    FunctionBaseTest::SetUp();
  }
  std::optional<std::string> enumKey(
      std::optional<int64_t> val,
      TypePtr inputType) {
    return evaluateOnce<std::string>("enum_key(c0)", inputType, val);
  }

  std::optional<std::string> enumKey(
      std::optional<std::string> val,
      TypePtr inputType) {
    return evaluateOnce<std::string>("enum_key(c0)", inputType, std::move(val));
  }
};

TEST_F(EnumFunctionsTest, enumKeyBigintEnum) {
  LongEnumParameter moodInfo("test.enum.mood", {{"CURIOUS", -2}, {"HAPPY", 0}});
  auto bigintEnum = BIGINT_ENUM(moodInfo);

  EXPECT_EQ("CURIOUS", enumKey(-2, bigintEnum));
  EXPECT_EQ(std::nullopt, enumKey(std::optional<int64_t>{}, bigintEnum));

  VELOX_ASSERT_THROW(
      enumKey(1, bigintEnum),
      "Value '1' not in BigintEnum: test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0})");

  VELOX_ASSERT_THROW(
      enumKey(1, BIGINT()),
      "Scalar function signature is not supported: enum_key(BIGINT). Supported signatures: (varchar_enum(E1)) -> varchar, (bigint_enum(E1)) -> varchar.");

  VELOX_ASSERT_THROW(
      evaluateOnce<std::string>(
          "enum_key(c0, c1)",
          {bigintEnum, BIGINT()},
          std::optional<int64_t>(0),
          std::optional<int64_t>(5)),
      "Scalar function signature is not supported: enum_key(test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0}), BIGINT). Supported signatures: (varchar_enum(E1)) -> varchar, (bigint_enum(E1)) -> varchar.");
}

TEST_F(EnumFunctionsTest, enumKeyVarcharEnum) {
  VarcharEnumParameter colorInfo(
      "test.enum.color",
      {{"RED", "red_value"}, {"BLUE", "blue_value"}, {"GREEN", "green_value"}});
  auto varcharEnum = VARCHAR_ENUM(colorInfo);

  EXPECT_EQ("RED", enumKey("red_value", varcharEnum));
  EXPECT_EQ(std::nullopt, enumKey(std::optional<std::string>{}, varcharEnum));

  VELOX_ASSERT_THROW(
      enumKey("non-existent value", varcharEnum),
      "Value 'non-existent value' not in VarcharEnum: test.enum.color:VarcharEnum({\"BLUE\": \"blue_value\", \"GREEN\": \"green_value\", \"RED\": \"red_value\"})");

  VELOX_ASSERT_THROW(
      enumKey("red_value", VARCHAR()),
      "Scalar function signature is not supported: enum_key(VARCHAR). Supported signatures: (varchar_enum(E1)) -> varchar, (bigint_enum(E1)) -> varchar.");

  VELOX_ASSERT_THROW(
      evaluateOnce<std::string>(
          "enum_key(c0, c1)",
          {varcharEnum, BIGINT()},
          std::optional<std::string>("red_value"),
          std::optional<int64_t>(5)),
      "Scalar function signature is not supported: enum_key(test.enum.color:VarcharEnum({\"BLUE\": \"blue_value\", \"GREEN\": \"green_value\", \"RED\": \"red_value\"}), BIGINT). Supported signatures: (varchar_enum(E1)) -> varchar, (bigint_enum(E1)) -> varchar.");
}
} // namespace
} // namespace facebook::velox::functions
