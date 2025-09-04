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
#include "velox/functions/prestosql/types/VarcharEnumRegistration.h"
#include "velox/functions/prestosql/types/VarcharEnumType.h"

using namespace facebook::velox::test;
using namespace facebook::velox::exec;

namespace facebook::velox {
namespace {

class VarcharEnumCastTest : public functions::test::CastBaseTest {
 protected:
  VarcharEnumCastTest() {
    registerVarcharEnumType();
    VarcharEnumParameter colorInfo(
        "test.enum.color", {{"RED", "red"}, {"BLUE", "blue"}});
    colorEnum_ = VARCHAR_ENUM(colorInfo);
  }

  VarcharEnumTypePtr colorEnum_;
};

TEST_F(VarcharEnumCastTest, castTo) {
  // Cast VARCHAR type to enum type.
  testCast<StringView, StringView>(
      VARCHAR(),
      colorEnum_,
      {"red"_sv, "blue"_sv, std::nullopt},
      {"red"_sv, "blue"_sv, std::nullopt});

  // Cast enum type to same enum type.
  testCast<StringView, StringView>(
      colorEnum_,
      colorEnum_,
      {"red"_sv, "blue"_sv, std::nullopt},
      {"red"_sv, "blue"_sv, std::nullopt});
}

TEST_F(VarcharEnumCastTest, invalidCastTo) {
  // Cast is only permitted from VARCHAR types to enum type.
  testThrow<int64_t>(
      BIGINT(),
      colorEnum_,
      {1},
      "Cannot cast BIGINT to test.enum.color:VarcharEnum({\"BLUE\": \"blue\", \"RED\": \"red\"}).");

  testThrow<StringView>(
      VARBINARY(),
      colorEnum_,
      {"red"_sv, ""_sv},
      "Cannot cast VARBINARY to test.enum.color:VarcharEnum({\"BLUE\": \"blue\", \"RED\": \"red\"}).");

  testThrow<bool>(
      BOOLEAN(),
      colorEnum_,
      {1},
      "Cannot cast BOOLEAN to test.enum.color:VarcharEnum({\"BLUE\": \"blue\", \"RED\": \"red\"}).");

  // Cast base type to enum type where the value does not exist in the enum.
  // Casting is case-sensitive.
  testThrow<StringView>(
      VARCHAR(),
      colorEnum_,
      {"green"_sv},
      "No value 'green' in test.enum.color");

  testThrow<StringView>(
      VARCHAR(), colorEnum_, {"RED"_sv}, "No value 'RED' in test.enum.color");

  // Cast enum type to different enum type.
  std::unordered_map<std::string, std::string> differentMap = {
      {"RED", "red"}, {"BLUE", "navy"}};
  VarcharEnumParameter differentEnumInfo("someEnumType", differentMap);
  testThrow<StringView>(
      VARCHAR_ENUM(differentEnumInfo),
      colorEnum_,
      {"red"_sv},
      "Cannot cast someEnumType:VarcharEnum({\"BLUE\": \"navy\", \"RED\": \"red\"}) to test.enum.color:VarcharEnum({\"BLUE\": \"blue\", \"RED\": \"red\"}).");

  // Cast enum type to different enum type with same name.
  VarcharEnumParameter sameNameDifferentMap("test.enum.color", differentMap);
  testThrow<StringView>(
      VARCHAR_ENUM(sameNameDifferentMap),
      colorEnum_,
      {"red"_sv},
      "Cannot cast test.enum.color:VarcharEnum({\"BLUE\": \"navy\", \"RED\": \"red\"}) to test.enum.color:VarcharEnum({\"BLUE\": \"blue\", \"RED\": \"red\"})");
}

TEST_F(VarcharEnumCastTest, fromVarcharEnum) {
  // Cast enum type to base type.
  testCast<StringView, StringView>(
      colorEnum_,
      VARCHAR(),
      {"red"_sv, "blue"_sv, std::nullopt},
      {"red"_sv, "blue"_sv, std::nullopt});

  // Casting enum type to all other types including other string types is not
  // permitted.
  testThrow<StringView>(
      colorEnum_,
      VARBINARY(),
      {"red"_sv},
      "Cannot cast test.enum.color:VarcharEnum({\"BLUE\": \"blue\", \"RED\": \"red\"}) to VARBINARY.");

  testThrow<StringView>(
      colorEnum_,
      BIGINT(),
      {"red"_sv},
      "Cannot cast test.enum.color:VarcharEnum({\"BLUE\": \"blue\", \"RED\": \"red\"}) to BIGINT.");
}
} // namespace
} // namespace facebook::velox
