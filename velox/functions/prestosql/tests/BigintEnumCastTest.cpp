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
#include "velox/functions/prestosql/tests/CastBaseTest.h"
#include "velox/functions/prestosql/types/BigintEnumRegistration.h"
#include "velox/functions/prestosql/types/BigintEnumType.h"

using namespace facebook::velox::test;
using namespace facebook::velox::exec;

namespace facebook::velox {
namespace {

class BigintEnumCastTest : public functions::test::CastBaseTest {
 protected:
  BigintEnumCastTest() {
    registerBigintEnumType();
    LongEnumParameter moodInfo(
        "test.enum.mood", {{"CURIOUS", -2}, {"HAPPY", 0}});
    moodEnum_ = BIGINT_ENUM(moodInfo);
  }

  BigintEnumTypePtr moodEnum_;
};

TEST_F(BigintEnumCastTest, castTo) {
  // Cast different integer types to enum type.
  testCast<int64_t, int64_t>(
      BIGINT(), moodEnum_, {0, -2, std::nullopt}, {0, -2, std::nullopt});

  testCast<int8_t, int64_t>(
      TINYINT(), moodEnum_, {0, -2, std::nullopt}, {0, -2, std::nullopt});

  testCast<int16_t, int64_t>(
      SMALLINT(), moodEnum_, {0, -2, std::nullopt}, {0, -2, std::nullopt});

  testCast<int32_t, int64_t>(
      INTEGER(), moodEnum_, {0, -2, std::nullopt}, {0, -2, std::nullopt});

  // Cast enum type to same enum type.
  testCast<int64_t, int64_t>(
      moodEnum_, moodEnum_, {0, -2, std::nullopt}, {0, -2, std::nullopt});
}

TEST_F(BigintEnumCastTest, invalidCastTo) {
  // Cast is only permitted from integer types to enum type.
  VELOX_ASSERT_THROW(
      evaluateCast(
          VARCHAR(),
          moodEnum_,
          makeRowVector({makeNullableFlatVector<StringView>(
              {"a"_sv, "b"_sv, std::nullopt})})),
      "Cannot cast VARCHAR to test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0}).");

  VELOX_ASSERT_THROW(
      evaluateCast(
          BOOLEAN(),
          moodEnum_,
          makeRowVector(
              {makeNullableFlatVector<bool>({true, false, std::nullopt})})),
      "Cannot cast BOOLEAN to test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0}).");

  // Cast base type to enum type where the value does not exist in the enum.
  VELOX_ASSERT_THROW(
      evaluateCast(
          BIGINT(),
          moodEnum_,
          makeRowVector({makeNullableFlatVector<int64_t>(
              {0, 1, std::nullopt}, BIGINT())})),
      "No value '1' in test.enum.mood");

  // Cast enum type to different enum type.
  std::unordered_map<std::string, int64_t> differentMap = {
      {"CURIOUS", -2}, {"HAPPY", 3}};
  LongEnumParameter differentEnumInfo("someEnumType", differentMap);
  VELOX_ASSERT_THROW(
      evaluateCast(
          moodEnum_,
          BIGINT_ENUM(differentEnumInfo),
          makeRowVector({makeNullableFlatVector<int64_t>(
              {0, 1, std::nullopt}, moodEnum_)})),
      "Cannot cast test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0}) to someEnumType:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 3}).");

  // Cast enum type to different enum type with same name.
  LongEnumParameter sameNameDifferentMap("test.enum.mood", differentMap);
  VELOX_ASSERT_THROW(
      evaluateCast(
          moodEnum_,
          BIGINT_ENUM(sameNameDifferentMap),
          makeRowVector({makeNullableFlatVector<int64_t>(
              {0, 1, std::nullopt}, moodEnum_)})),
      "Cannot cast test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0}) to test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 3})");
}

TEST_F(BigintEnumCastTest, fromBigintEnum) {
  // Cast enum type to base type.
  testCast<int64_t, int64_t>(
      moodEnum_, BIGINT(), {0, -2, std::nullopt}, {0, -2, std::nullopt});

  // Casting enum type to all other types including other integer types is not
  // permitted.
  auto enumInput = makeRowVector(
      {makeNullableFlatVector<int64_t>({0, 1, std::nullopt}, moodEnum_)});
  VELOX_ASSERT_THROW(
      evaluateCast(moodEnum_, TINYINT(), enumInput),
      "Cannot cast test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0}) to TINYINT.");

  VELOX_ASSERT_THROW(
      evaluateCast(moodEnum_, SMALLINT(), enumInput),
      "Cannot cast test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0}) to SMALLINT.");

  VELOX_ASSERT_THROW(
      evaluateCast(moodEnum_, INTEGER(), enumInput),
      "Cannot cast test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0}) to INTEGER.");

  VELOX_ASSERT_THROW(
      evaluateCast(moodEnum_, VARCHAR(), enumInput),
      "Cannot cast test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0}) to VARCHAR.");

  VELOX_ASSERT_THROW(
      evaluateCast(
          moodEnum_,
          BOOLEAN(),
          makeRowVector({makeNullableFlatVector<int64_t>(
              {0, 1, std::nullopt}, moodEnum_)})),
      "Cannot cast test.enum.mood:BigintEnum({\"CURIOUS\": -2, \"HAPPY\": 0}) to BOOLEAN.");
}
} // namespace
} // namespace facebook::velox
