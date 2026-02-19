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

#include "presto_cpp/main/sidecar/properties/CudfSessionProperties.h"
#include "velox/experimental/cudf/CudfConfig.h"

using namespace facebook::presto::cudf;
using namespace facebook::velox;

class CudfSessionPropertiesTest : public testing::Test {};

TEST_F(CudfSessionPropertiesTest, propertiesInitialized) {
  // Verify all GPU session properties are initialized
  const auto sessionProps = CudfSessionProperties::instance();
  const auto& serialized = sessionProps->serialize();

  // Should have 12 properties
  EXPECT_EQ(serialized.size(), 12);

  // Verify each property is present by checking names
  std::vector<std::string> expectedNames = {
      CudfSessionProperties::kCudfEnabled,
      CudfSessionProperties::kCudfDebugEnabled,
      CudfSessionProperties::kCudfMemoryResource,
      CudfSessionProperties::kCudfMemoryPercent,
      CudfSessionProperties::kCudfFunctionNamePrefix,
      CudfSessionProperties::kCudfAstExpressionEnabled,
      CudfSessionProperties::kCudfAstExpressionPriority,
      CudfSessionProperties::kCudfJitExpressionEnabled,
      CudfSessionProperties::kCudfJitExpressionPriority,
      CudfSessionProperties::kCudfAllowCpuFallback,
      CudfSessionProperties::kCudfLogFallback,
      CudfSessionProperties::kCudfTopNBatchSize,
  };

  std::set<std::string> foundNames;
  for (const auto& prop : serialized) {
    foundNames.insert(prop["name"].get<std::string>());
  }

  for (const auto& expectedName : expectedNames) {
    EXPECT_NE(foundNames.find(expectedName), foundNames.end())
        << "Property " << expectedName << " not found in session properties";
  }
}

TEST_F(CudfSessionPropertiesTest, defaultValuesMatchConfig) {
  // Verify default values match CudfConfig
  const auto& config = cudf_velox::CudfConfig::getInstance();
  const auto sessionProps = CudfSessionProperties::instance();
  const auto& serialized = sessionProps->serialize();

  // Helper to find property value by name
  auto findPropertyValue = [&serialized](const std::string& name) {
    for (const auto& prop : serialized) {
      if (prop["name"] == name) {
        return prop["defaultValue"].get<std::string>();
      }
    }
    return std::string();
  };

  // Check enabled
  EXPECT_EQ(
      findPropertyValue(CudfSessionProperties::kCudfEnabled),
      (config.enabled ? "true" : "false"));

  // Check debug enabled
  EXPECT_EQ(
      findPropertyValue(CudfSessionProperties::kCudfDebugEnabled),
      (config.debugEnabled ? "true" : "false"));

  // Check memory resource
  EXPECT_EQ(
      findPropertyValue(CudfSessionProperties::kCudfMemoryResource),
      config.memoryResource);

  // Check memory percent
  EXPECT_EQ(
      findPropertyValue(CudfSessionProperties::kCudfMemoryPercent),
      std::to_string(config.memoryPercent));

  // Check allow CPU fallback
  EXPECT_EQ(
      findPropertyValue(CudfSessionProperties::kCudfAllowCpuFallback),
      (config.allowCpuFallback ? "true" : "false"));

  // Check log fallback
  EXPECT_EQ(
      findPropertyValue(CudfSessionProperties::kCudfLogFallback),
      (config.logFallback ? "true" : "false"));

  // Check AST enabled
  EXPECT_EQ(
      findPropertyValue(CudfSessionProperties::kCudfAstExpressionEnabled),
      (config.astExpressionEnabled ? "true" : "false"));

  // Check AST priority
  EXPECT_EQ(
      findPropertyValue(CudfSessionProperties::kCudfAstExpressionPriority),
      std::to_string(config.astExpressionPriority));

  // Check JIT enabled
  EXPECT_EQ(
      findPropertyValue(CudfSessionProperties::kCudfJitExpressionEnabled),
      (config.jitExpressionEnabled ? "true" : "false"));

  // Check JIT priority
  EXPECT_EQ(
      findPropertyValue(CudfSessionProperties::kCudfJitExpressionPriority),
      std::to_string(config.jitExpressionPriority));

  // Check TopN batch size
  EXPECT_EQ(
      findPropertyValue(CudfSessionProperties::kCudfTopNBatchSize),
      std::to_string(config.topNBatchSize));
}

TEST_F(CudfSessionPropertiesTest, propertyMetadata) {
  // Verify property metadata (name, type, hidden flag)
  const auto sessionProps = CudfSessionProperties::instance();
  const auto& serialized = sessionProps->serialize();

  // Helper to find property index by name
  auto findPropertyIndex = [&serialized](const std::string& name) -> int {
    for (size_t i = 0; i < serialized.size(); ++i) {
      if (serialized[i]["name"] == name) {
        return i;
      }
    }
    return -1;
  };

  // Check a boolean property
  int enabledIdx = findPropertyIndex(CudfSessionProperties::kCudfEnabled);
  EXPECT_NE(enabledIdx, -1);
  EXPECT_EQ(serialized[enabledIdx]["typeSignature"], "boolean");
  EXPECT_FALSE(serialized[enabledIdx]["hidden"]);

  // Check an integer property
  int memoryPercentIdx =
      findPropertyIndex(CudfSessionProperties::kCudfMemoryPercent);
  EXPECT_NE(memoryPercentIdx, -1);
  EXPECT_EQ(serialized[memoryPercentIdx]["typeSignature"], "integer");
  EXPECT_FALSE(serialized[memoryPercentIdx]["hidden"]);

  // Check a varchar property
  int memoryResourceIdx =
      findPropertyIndex(CudfSessionProperties::kCudfMemoryResource);
  EXPECT_NE(memoryResourceIdx, -1);
  EXPECT_EQ(serialized[memoryResourceIdx]["typeSignature"], "varchar");
  EXPECT_FALSE(serialized[memoryResourceIdx]["hidden"]);
}
TEST_F(CudfSessionPropertiesTest, veloxConfigMapping) {
  // Verify Presto session properties map to correct Velox QueryConfig names
  using facebook::velox::cudf_velox::CudfConfig;

  const std::map<std::string, std::string> expectedMappings = {
      {CudfSessionProperties::kCudfEnabled, CudfConfig::kCudfEnabled},
      {CudfSessionProperties::kCudfDebugEnabled, CudfConfig::kCudfDebugEnabled},
      {CudfSessionProperties::kCudfMemoryResource,
       CudfConfig::kCudfMemoryResource},
      {CudfSessionProperties::kCudfMemoryPercent,
       CudfConfig::kCudfMemoryPercent},
      {CudfSessionProperties::kCudfFunctionNamePrefix,
       CudfConfig::kCudfFunctionNamePrefix},
      {CudfSessionProperties::kCudfAstExpressionEnabled,
       CudfConfig::kCudfAstExpressionEnabled},
      {CudfSessionProperties::kCudfAstExpressionPriority,
       CudfConfig::kCudfAstExpressionPriority},
      {CudfSessionProperties::kCudfJitExpressionEnabled,
       CudfConfig::kCudfJitExpressionEnabled},
      {CudfSessionProperties::kCudfJitExpressionPriority,
       CudfConfig::kCudfJitExpressionPriority},
      {CudfSessionProperties::kCudfAllowCpuFallback,
       CudfConfig::kCudfAllowCpuFallback},
      {CudfSessionProperties::kCudfLogFallback, CudfConfig::kCudfLogFallback},
      {CudfSessionProperties::kCudfTopNBatchSize,
       CudfConfig::kCudfTopNBatchSize},
  };

  const auto sessionProperties = CudfSessionProperties::instance();
  for (const auto& [sessionProperty, expectedVeloxConfig] : expectedMappings) {
    ASSERT_EQ(
        expectedVeloxConfig, sessionProperties->toVeloxConfig(sessionProperty))
        << "Mapping for property " << sessionProperty << " is incorrect";
  }
}
