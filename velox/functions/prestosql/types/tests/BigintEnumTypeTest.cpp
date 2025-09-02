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
#include "velox/functions/prestosql/types/BigintEnumType.h"
#include "velox/functions/prestosql/types/BigintEnumRegistration.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox {
namespace {
class BigintEnumTypeTest : public testing::Test, public test::TypeTestBase {
 protected:
  BigintEnumTypeTest() {
    registerBigintEnumType();
    LongEnumParameter moodInfo(
        "test.enum.mood", {{"CURIOUS", -2}, {"HAPPY", 0}});
    moodEnum_ = BIGINT_ENUM(moodInfo);
  }

  BigintEnumTypePtr moodEnum_;
};

TEST_F(BigintEnumTypeTest, basic) {
  ASSERT_TRUE(hasType("BIGINT_ENUM"));
  LongEnumParameter moodInfo("test.enum.mood", {{"CURIOUS", -2}, {"HAPPY", 0}});
  std::vector<TypeParameter> moodParams = {TypeParameter(moodInfo)};
  ASSERT_EQ(getType("BIGINT_ENUM", moodParams), moodEnum_);

  ASSERT_STREQ(moodEnum_->name(), "BIGINT_ENUM");
  ASSERT_STREQ(moodEnum_->kindName(), "BIGINT");
  ASSERT_EQ(moodEnum_->enumName(), "test.enum.mood");
  ASSERT_EQ(moodEnum_->parameters().size(), 1);
  ASSERT_EQ(
      moodEnum_->toString(),
      "test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0})");

  // Different TypeParameters with different enumName, same enumMap
  LongEnumParameter differentNameSameMap(
      "test.enum.mood2", {{"CURIOUS", -2}, {"HAPPY", 0}});
  ASSERT_NE(moodEnum_, BIGINT_ENUM(differentNameSameMap));
  EXPECT_FALSE(moodEnum_->equivalent(*BIGINT_ENUM(differentNameSameMap)));

  // Different TypeParameters with same enumName, different enumMap
  LongEnumParameter sameNameDifferentMap(
      "test.enum.mood", {{"CURIOUS", -2}, {"HAPPY", 0}, {"ANGRY", 1}});
  ASSERT_NE(moodEnum_, BIGINT_ENUM(sameNameDifferentMap));
  EXPECT_FALSE(moodEnum_->equivalent(*BIGINT_ENUM(sameNameDifferentMap)));

  // Type Parameter with duplicate value in the enum map.
  LongEnumParameter duplicateValuesInfo(
      "duplicate.values.enum", {{"HAPPY", 0}, {"SAD", 0}, {"ANGRY", 0}});
  VELOX_ASSERT_THROW(
      BIGINT_ENUM(duplicateValuesInfo),
      "Invalid enum type duplicate.values.enum, contains duplicate value 0");

  // Different TypeParameters with same enumName and enumMap but in different
  // order
  LongEnumParameter differentOrderMapMoodInfo(
      "test.enum.mood", {{"HAPPY", 0}, {"CURIOUS", -2}});
  ASSERT_EQ(moodEnum_, BIGINT_ENUM(differentOrderMapMoodInfo));
  EXPECT_TRUE(moodEnum_->equivalent(*BIGINT_ENUM(differentOrderMapMoodInfo)));
}

TEST_F(BigintEnumTypeTest, serde) {
  testTypeSerde(moodEnum_);
}

TEST_F(BigintEnumTypeTest, keyAt) {
  // Test existing values
  ASSERT_EQ(moodEnum_->keyAt(-2), "CURIOUS");
  ASSERT_EQ(moodEnum_->keyAt(0), "HAPPY");

  // Test non-existent value
  ASSERT_EQ(moodEnum_->keyAt(999), std::nullopt);
}
} // namespace
} // namespace facebook::velox
