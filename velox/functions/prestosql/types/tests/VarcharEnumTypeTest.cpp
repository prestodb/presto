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
#include "velox/functions/prestosql/types/VarcharEnumType.h"
#include "velox/functions/prestosql/types/VarcharEnumRegistration.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox {
namespace {
class VarcharEnumTypeTest : public testing::Test, public test::TypeTestBase {
 protected:
  VarcharEnumTypeTest() {
    registerVarcharEnumType();
    VarcharEnumParameter colorInfo(
        "test.enum.color", {{"RED", "red"}, {"BLUE", "blue"}});
    colorEnum_ = VARCHAR_ENUM(colorInfo);
  }

  VarcharEnumTypePtr colorEnum_;
};

TEST_F(VarcharEnumTypeTest, basic) {
  ASSERT_TRUE(hasType("VARCHAR_ENUM"));
  VarcharEnumParameter colorInfo(
      "test.enum.color", {{"RED", "red"}, {"BLUE", "blue"}});
  std::vector<TypeParameter> colorParams = {TypeParameter(colorInfo)};
  ASSERT_EQ(getType("VARCHAR_ENUM", colorParams), colorEnum_);

  ASSERT_STREQ(colorEnum_->name(), "VARCHAR_ENUM");
  ASSERT_STREQ(colorEnum_->kindName(), "VARCHAR");
  ASSERT_EQ(colorEnum_->enumName(), "test.enum.color");
  ASSERT_EQ(colorEnum_->parameters().size(), 1);
  ASSERT_EQ(
      colorEnum_->toString(),
      "test.enum.color:VarcharEnum({\"BLUE\": \"blue\", \"RED\": \"red\"})");

  // Different TypeParameters with different enumName, same enumMap
  VarcharEnumParameter differentNameSameMap(
      "test.enum.color2", {{"RED", "red"}, {"BLUE", "blue"}});
  ASSERT_NE(colorEnum_, VARCHAR_ENUM(differentNameSameMap));
  EXPECT_FALSE(colorEnum_->equivalent(*VARCHAR_ENUM(differentNameSameMap)));

  // Different TypeParameters with same enumName, different enumMap
  VarcharEnumParameter sameNameDifferentMap(
      "test.enum.color",
      {{"RED", "red"}, {"BLUE", "blue"}, {"GREEN", "green"}});
  ASSERT_NE(colorEnum_, VARCHAR_ENUM(sameNameDifferentMap));
  EXPECT_FALSE(colorEnum_->equivalent(*VARCHAR_ENUM(sameNameDifferentMap)));

  // Type Parameter with duplicate value in the enum map.
  VarcharEnumParameter duplicateValuesInfo(
      "duplicate.values.enum",
      {{"RED", "red"}, {"CRIMSON", "red"}, {"BLUE", "blue"}});
  VELOX_ASSERT_THROW(
      VARCHAR_ENUM(duplicateValuesInfo),
      "Invalid enum type duplicate.values.enum, contains duplicate value red");

  // Different TypeParameters with same enumName and enumMap but in different
  // order
  VarcharEnumParameter differentOrderMapColorInfo(
      "test.enum.color", {{"BLUE", "blue"}, {"RED", "red"}});
  ASSERT_EQ(colorEnum_, VARCHAR_ENUM(differentOrderMapColorInfo));
  EXPECT_TRUE(
      colorEnum_->equivalent(*VARCHAR_ENUM(differentOrderMapColorInfo)));
}

TEST_F(VarcharEnumTypeTest, serde) {
  testTypeSerde(colorEnum_);
}
} // namespace
} // namespace facebook::velox
