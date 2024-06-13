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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "presto_cpp/main/SystemSessionProperties.h"

using namespace facebook::presto;

class SystemSessionPropertiesTest : public testing::Test {};

// Test creation of list of session properties
TEST_F(SystemSessionPropertiesTest, getSystemProperties) {
  SystemSessionProperties system_session_properties;

  EXPECT_EQ(system_session_properties.getSessionProperties().size(), 18);
}

// Validate metadata of the session properties
TEST_F(SystemSessionPropertiesTest, vaildateSessionProperties) {
  SystemSessionProperties system_session_properties;

  for (const auto& property :
       system_session_properties.getSessionProperties()) {
    if (property.first == SystemSessionProperties::kExprEvalSimplified) {
      EXPECT_EQ(property.second->getType(), PropertyType::kBool);
      EXPECT_THAT(
          property.second->getDescription(),
          testing::StartsWith(
              "Native Execution only. Enable simplified path in expression"));
      EXPECT_EQ(property.second->getDefaultValue(), "false");
      EXPECT_EQ(property.second->isHidden(), false);
      EXPECT_EQ(
          property.second->getVeloxConfigName(), "expression.eval_simplified");
    } else if (property.first == SystemSessionProperties::kLegacyTimestamp) {
      EXPECT_EQ(property.second->getType(), PropertyType::kBool);
      EXPECT_THAT(
          property.second->getDescription(),
          testing::StartsWith(
              "Native Execution only. Use legacy TIME & TIMESTAMP semantics"));
      EXPECT_EQ(property.second->getDefaultValue(), "true");
      EXPECT_EQ(property.second->isHidden(), false);
      EXPECT_EQ(
          property.second->getVeloxConfigName(),
          "adjust_timestamp_to_session_timezone");
    } else if (
        property.first == SystemSessionProperties::kDriverCpuTimeSliceLimitMs) {
      EXPECT_EQ(property.second->getType(), PropertyType::kInt);
      EXPECT_THAT(
          property.second->getDescription(),
          testing::StartsWith(
              "Native Execution only. The cpu time slice limit"));
      EXPECT_EQ(property.second->getDefaultValue(), "1000");
      EXPECT_EQ(property.second->isHidden(), false);
      EXPECT_EQ(
          property.second->getVeloxConfigName(),
          "driver_cpu_time_slice_limit_ms");
    } else if (
        property.first == SystemSessionProperties::kSpillCompressionCodec) {
      EXPECT_EQ(property.second->getType(), PropertyType::kString);
      EXPECT_THAT(
          property.second->getDescription(),
          testing::StartsWith(
              "Native Execution only. The compression algorithm type"));
      EXPECT_EQ(property.second->getDefaultValue(), "none");
      EXPECT_EQ(property.second->isHidden(), false);
      EXPECT_EQ(
          property.second->getVeloxConfigName(), "spill_compression_codec");
    } else {
      // No need to validate all. Let's validate a property from each type.
    }
  }
}
