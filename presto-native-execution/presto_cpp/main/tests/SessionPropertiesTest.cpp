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

#include "presto_cpp/main/SessionProperties.h"
#include "velox/core/QueryConfig.h"
#include "velox/type/Type.h"

using namespace facebook::presto;
using namespace facebook::velox;

class SessionPropertiesTest : public testing::Test {};

TEST_F(SessionPropertiesTest, validateMapping) {
  const std::vector<std::string> names = {
      SessionProperties::kLegacyTimestamp,
      SessionProperties::kDriverCpuTimeSliceLimitMs,
      SessionProperties::kSpillCompressionCodec,
      SessionProperties::kScaleWriterRebalanceMaxMemoryUsageRatio,
      SessionProperties::kScaleWriterMaxPartitionsPerWriter,
      SessionProperties::
          kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
      SessionProperties::kScaleWriterMinProcessedBytesRebalanceThreshold,
      SessionProperties::kTableScanScaledProcessingEnabled,
      SessionProperties::kTableScanScaleUpMemoryUsageRatio,
      SessionProperties::kStreamingAggregationMinOutputBatchRows,
      SessionProperties::kRequestDataSizesMaxWaitSec};
  const std::vector<std::string> veloxConfigNames = {
      core::QueryConfig::kAdjustTimestampToTimezone,
      core::QueryConfig::kDriverCpuTimeSliceLimitMs,
      core::QueryConfig::kSpillCompressionKind,
      core::QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio,
      core::QueryConfig::kScaleWriterMaxPartitionsPerWriter,
      core::QueryConfig::
          kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
      core::QueryConfig::kScaleWriterMinProcessedBytesRebalanceThreshold,
      core::QueryConfig::kTableScanScaledProcessingEnabled,
      core::QueryConfig::kTableScanScaleUpMemoryUsageRatio,
      core::QueryConfig::kStreamingAggregationMinOutputBatchRows,
      core::QueryConfig::kRequestDataSizesMaxWaitSec};
  auto sessionProperties = SessionProperties().getSessionProperties();
  const auto len = names.size();
  for (auto i = 0; i < len; i++) {
    EXPECT_EQ(
        veloxConfigNames[i],
        sessionProperties.at(names[i])->getVeloxConfigName());
  }
}

TEST_F(SessionPropertiesTest, serializeProperty) {
  auto properties = SessionProperties();
  auto j = properties.serialize();
  for (const auto& property : j) {
    auto name = property["name"];
    json expectedProperty =
        properties.getSessionProperties().at(name)->serialize();
    EXPECT_EQ(property, expectedProperty);
  }
}
