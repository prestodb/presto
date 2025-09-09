/*
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

#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/tests/MutableConfigs.h"
#include "presto_cpp/main/types/TypeParser.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/prestosql/types/BigintEnumRegistration.h"
#include "velox/functions/prestosql/types/VarcharEnumRegistration.h"


using namespace facebook::presto;
using namespace facebook::velox;

class TypeParserTest : public ::testing::Test {
  void SetUp() override {
    filesystems::registerLocalFileSystem();
    test::setupMutableSystemConfig(); 
    registerBigintEnumType();
    registerVarcharEnumType();
  }
};

// Test basical functionality of TypeParser.
// More detailed tests for Presto TypeParser are in velox/functions/prestosql/types/parser/tests/TypeParserTest.
TEST_F(TypeParserTest, parseEnumTypes) {
  TypeParser typeParser = TypeParser();

  ASSERT_EQ(
    typeParser.parse(
        "test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":2, \"HAPPY\":0})")->toString(),
    "test.enum.mood:BigintEnum({\"CURIOUS\": 2, \"HAPPY\": 0})");
  ASSERT_EQ(
    typeParser.parse(
        "test.enum.mood:VarcharEnum(test.enum.mood{\"CURIOUS\":\"ONXW2ZKWMFWHKZI=\", \"HAPPY\":\"ONXW2ZJAOZQWY5LF\" , \"SAD\":\"KNHU2RJAKZAUYVKF\"})")->toString(),
    "test.enum.mood:VarcharEnum({\"CURIOUS\": \"someValue\", \"HAPPY\": \"some value\", \"SAD\": \"SOME VALUE\"})");

  // When set to false, TypeParser will throw an unsupported error when it receives an enum type.
  SystemConfig::instance()->setValue(std::string(SystemConfig::kEnumTypesEnabled), "false");

  VELOX_ASSERT_THROW(
    typeParser.parse(
        "test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":2, \"HAPPY\":0})"),
    "Unsupported type: test.enum.mood:BigintEnum(test.enum.mood{\"CURIOUS\":2, \"HAPPY\":0})");
  VELOX_ASSERT_THROW(
    typeParser.parse(
        "test.enum.mood:VarcharEnum(test.enum.mood{\"CURIOUS\":\"ONXW2ZKWMFWHKZI=\", \"HAPPY\":\"ONXW2ZJAOZQWY5LF\" , \"SAD\":\"KNHU2RJAKZAUYVKF\"})"),
    "Unsupported type: test.enum.mood:VarcharEnum(test.enum.mood{\"CURIOUS\":\"ONXW2ZKWMFWHKZI=\", \"HAPPY\":\"ONXW2ZJAOZQWY5LF\" , \"SAD\":\"KNHU2RJAKZAUYVKF\"})");
}
