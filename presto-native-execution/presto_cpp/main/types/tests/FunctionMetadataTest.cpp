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
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

#include <gtest/gtest.h>
#include "presto_cpp/main/types/FunctionMetadata.h"

using namespace facebook::velox;
using namespace facebook::presto;

class FunctionMetadataTest : public ::testing::Test {};

TEST(FunctionMetadataTest, testCount) {
  aggregate::prestosql::registerAllAggregateFunctions("");
  std::string functionName = "count";
  json jsonMetadata;
  getJsonMetadataForFunction(functionName, jsonMetadata);
  json j = jsonMetadata.at(functionName);
  const auto numSignatures = 2;
  EXPECT_EQ(j.size(), numSignatures);

  for (auto i = 0; i < numSignatures; i++) {
    json jsonAtIdx = j.at(i);
    EXPECT_EQ(jsonAtIdx.at("functionKind"), protocol::FunctionKind::AGGREGATE);
    EXPECT_EQ(jsonAtIdx.at("docString"), functionName);
    EXPECT_EQ(jsonAtIdx.at("schema"), "default");
    EXPECT_EQ(jsonAtIdx.at("outputType"), "bigint");
    auto paramTypes = jsonAtIdx.at("paramTypes");
    VELOX_CHECK(
        ((paramTypes == std::vector<std::string>({})) ||
         (paramTypes == std::vector<std::string>({"T"}))));

    auto routineCharacteristics = jsonAtIdx.at("routineCharacteristics");
    EXPECT_EQ(
        routineCharacteristics.at("language"), protocol::Language({"CPP"}));
    EXPECT_EQ(
        routineCharacteristics.at("determinism"),
        protocol::Determinism::DETERMINISTIC);
    EXPECT_EQ(
        routineCharacteristics.at("nullCallClause"),
        protocol::NullCallClause::CALLED_ON_NULL_INPUT);

    auto aggregateMetadata = jsonAtIdx.at("aggregateMetadata");
    EXPECT_EQ(aggregateMetadata.at("intermediateType"), "bigint");
    EXPECT_EQ(aggregateMetadata.at("isOrderSensitive"), true);
  }
}

TEST(FunctionMetadataTest, testSum) {
  aggregate::prestosql::registerAllAggregateFunctions("");
  std::string functionName = "sum";
  json jsonMetadata;
  getJsonMetadataForFunction(functionName, jsonMetadata);
  json j = jsonMetadata.at(functionName);
  const auto numSignatures = 7;
  EXPECT_EQ(j.size(), numSignatures);

  for (auto i = 0; i < numSignatures; i++) {
    json jsonAtIdx = j.at(i);
    EXPECT_EQ(jsonAtIdx.at("functionKind"), protocol::FunctionKind::AGGREGATE);
    EXPECT_EQ(jsonAtIdx.at("docString"), functionName);
    EXPECT_EQ(jsonAtIdx.at("schema"), "default");
    auto outputType = jsonAtIdx.at("outputType");
    VELOX_CHECK(
        ((outputType == "real") || (outputType == "double") ||
         (outputType == "DECIMAL(38,a_scale)") || (outputType == "bigint")));
    auto paramTypes = jsonAtIdx.at("paramTypes");
    VELOX_CHECK(
        ((paramTypes == std::vector<std::string>({"real"})) ||
         (paramTypes == std::vector<std::string>({"double"})) ||
         (paramTypes ==
          std::vector<std::string>({"DECIMAL(a_precision,a_scale)"})) ||
         (paramTypes == std::vector<std::string>({"tinyint"})) ||
         (paramTypes == std::vector<std::string>({"smallint"})) ||
         (paramTypes == std::vector<std::string>({"integer"})) ||
         (paramTypes == std::vector<std::string>({"bigint"}))));

    auto routineCharacteristics = jsonAtIdx.at("routineCharacteristics");
    EXPECT_EQ(
        routineCharacteristics.at("language"), protocol::Language({"CPP"}));
    EXPECT_EQ(
        routineCharacteristics.at("determinism"),
        protocol::Determinism::DETERMINISTIC);
    EXPECT_EQ(
        routineCharacteristics.at("nullCallClause"),
        protocol::NullCallClause::CALLED_ON_NULL_INPUT);

    auto aggregateMetadata = jsonAtIdx.at("aggregateMetadata");
    auto intermediateType = aggregateMetadata.at("intermediateType");
    VELOX_CHECK(
        (intermediateType == "double") || (intermediateType == "VARBINARY") ||
        (intermediateType == "bigint"));
    EXPECT_EQ(aggregateMetadata.at("isOrderSensitive"), true);
  }
}

TEST(FunctionMetadataTest, testRank) {
  window::prestosql::registerAllWindowFunctions("");
  std::string functionName = "rank";
  json jsonMetadata;
  getJsonMetadataForFunction(functionName, jsonMetadata);
  json j = jsonMetadata.at(functionName);
  const auto numSignatures = 1;
  EXPECT_EQ(j.size(), numSignatures);

  for (auto i = 0; i < numSignatures; i++) {
    json jsonAtIdx = j.at(i);
    EXPECT_EQ(jsonAtIdx.at("functionKind"), protocol::FunctionKind::WINDOW);
    EXPECT_EQ(jsonAtIdx.at("docString"), functionName);
    EXPECT_EQ(jsonAtIdx.at("schema"), "default");
    auto outputType = jsonAtIdx.at("outputType");
    VELOX_CHECK(((outputType == "integer") || (outputType == "bigint")));
    VELOX_CHECK(jsonAtIdx.at("paramTypes") == std::vector<std::string>({}));

    auto routineCharacteristics = jsonAtIdx.at("routineCharacteristics");
    EXPECT_EQ(
        routineCharacteristics.at("language"), protocol::Language({"CPP"}));
    EXPECT_EQ(
        routineCharacteristics.at("determinism"),
        protocol::Determinism::DETERMINISTIC);
    EXPECT_EQ(
        routineCharacteristics.at("nullCallClause"),
        protocol::NullCallClause::CALLED_ON_NULL_INPUT);
  }
}

TEST(FunctionMetadataTest, testRadians) {
  functions::prestosql::registerArithmeticFunctions("");
  std::string functionName = "radians";
  json jsonMetadata;
  getJsonMetadataForFunction(functionName, jsonMetadata);
  json j = jsonMetadata.at(functionName);
  const auto numSignatures = 1;
  EXPECT_EQ(j.size(), numSignatures);

  for (auto i = 0; i < numSignatures; i++) {
    json jsonAtIdx = j.at(i);
    EXPECT_EQ(jsonAtIdx.at("functionKind"), protocol::FunctionKind::SCALAR);
    EXPECT_EQ(jsonAtIdx.at("docString"), functionName);
    EXPECT_EQ(jsonAtIdx.at("schema"), "default");
    VELOX_CHECK(jsonAtIdx.at("outputType") == "double");
    VELOX_CHECK(
        jsonAtIdx.at("paramTypes") == std::vector<std::string>({"double"}));

    auto routineCharacteristics = jsonAtIdx.at("routineCharacteristics");
    EXPECT_EQ(
        routineCharacteristics.at("language"), protocol::Language({"CPP"}));
    EXPECT_EQ(
        routineCharacteristics.at("determinism"),
        protocol::Determinism::DETERMINISTIC);
    EXPECT_EQ(
        routineCharacteristics.at("nullCallClause"),
        protocol::NullCallClause::RETURNS_NULL_ON_NULL_INPUT);
  }
}
