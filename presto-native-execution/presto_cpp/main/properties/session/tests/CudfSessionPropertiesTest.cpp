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

#include "presto_cpp/main/properties/session/CudfSessionProperties.h"
#include "velox/experimental/cudf/common/CudfConfig.h"

using namespace facebook::presto::cudf;
using namespace facebook::velox;

class CudfSessionPropertiesTest : public testing::Test {};

TEST_F(CudfSessionPropertiesTest, validateMapping) {
  // Verify that each Presto session property maps to the correct Velox
  // CudfConfig key
  const std::unordered_map<std::string, std::string> expectedMappings = {
      {CudfSessionProperties::kCudfEnabled,
       cudf_velox::CudfConfig::kCudfEnabledEntry.name},
      {CudfSessionProperties::kCudfDebugEnabled,
       cudf_velox::CudfConfig::kCudfDebugEnabledEntry.name},
      {CudfSessionProperties::kCudfMemoryResource,
       cudf_velox::CudfConfig::kCudfMemoryResourceEntry.name},
      {CudfSessionProperties::kCudfMemoryPercent,
       cudf_velox::CudfConfig::kCudfMemoryPercentEntry.name},
      {CudfSessionProperties::kCudfFunctionNamePrefix,
       cudf_velox::CudfConfig::kCudfFunctionNamePrefixEntry.name},
      {CudfSessionProperties::kCudfAstExpressionEnabled,
       cudf_velox::CudfConfig::kCudfAstExpressionEnabledEntry.name},
      {CudfSessionProperties::kCudfAstExpressionPriority,
       cudf_velox::CudfConfig::kCudfAstExpressionPriorityEntry.name},
      {CudfSessionProperties::kCudfJitExpressionEnabled,
       cudf_velox::CudfConfig::kCudfJitExpressionEnabledEntry.name},
      {CudfSessionProperties::kCudfJitExpressionPriority,
       cudf_velox::CudfConfig::kCudfJitExpressionPriorityEntry.name},
      {CudfSessionProperties::kCudfAllowCpuFallback,
       cudf_velox::CudfConfig::kCudfAllowCpuFallbackEntry.name},
      {CudfSessionProperties::kCudfLogFallback,
       cudf_velox::CudfConfig::kCudfLogFallbackEntry.name},
  };

  const auto sessionProperties = CudfSessionProperties::instance();
  for (const auto& [sessionProperty, expectedVeloxConfig] : expectedMappings) {
    EXPECT_EQ(
        expectedVeloxConfig, sessionProperties->toVeloxConfig(sessionProperty));
  }
}
