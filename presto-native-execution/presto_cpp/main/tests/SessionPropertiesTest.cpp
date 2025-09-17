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

using namespace facebook::presto;
using namespace facebook::velox;

class SessionPropertiesTest : public testing::Test {};

TEST_F(SessionPropertiesTest, validateMapping) {
  const std::unordered_map<std::string, std::string> expectedMappings = {
      {SessionProperties::kExprEvalSimplified,
       core::QueryConfig::kExprEvalSimplified},
      {SessionProperties::kExprMaxArraySizeInReduce,
       core::QueryConfig::kExprMaxArraySizeInReduce},
      {SessionProperties::kExprMaxCompiledRegexes,
       core::QueryConfig::kExprMaxCompiledRegexes},
      {SessionProperties::kMaxPartialAggregationMemory,
       core::QueryConfig::kMaxPartialAggregationMemory},
      {SessionProperties::kMaxExtendedPartialAggregationMemory,
       core::QueryConfig::kMaxExtendedPartialAggregationMemory},
      {SessionProperties::kMaxSpillLevel, core::QueryConfig::kMaxSpillLevel},
      {SessionProperties::kMaxSpillFileSize,
       core::QueryConfig::kMaxSpillFileSize},
      {SessionProperties::kMaxSpillBytes, core::QueryConfig::kMaxSpillBytes},
      {SessionProperties::kSpillCompressionCodec,
       core::QueryConfig::kSpillCompressionKind},
      {SessionProperties::kSpillWriteBufferSize,
       core::QueryConfig::kSpillWriteBufferSize},
      {SessionProperties::kSpillFileCreateConfig,
       core::QueryConfig::kSpillFileCreateConfig},
      {SessionProperties::kJoinSpillEnabled,
       core::QueryConfig::kJoinSpillEnabled},
      {SessionProperties::kWindowSpillEnabled,
       core::QueryConfig::kWindowSpillEnabled},
      {SessionProperties::kWriterSpillEnabled,
       core::QueryConfig::kWriterSpillEnabled},
      {SessionProperties::kWriterFlushThresholdBytes,
       core::QueryConfig::kWriterFlushThresholdBytes},
      {SessionProperties::kRowNumberSpillEnabled,
       core::QueryConfig::kRowNumberSpillEnabled},
      {SessionProperties::kSpillerNumPartitionBits,
       core::QueryConfig::kSpillNumPartitionBits},
      {SessionProperties::kTopNRowNumberSpillEnabled,
       core::QueryConfig::kTopNRowNumberSpillEnabled},
      {SessionProperties::kValidateOutputFromOperators,
       core::QueryConfig::kValidateOutputFromOperators},
      {SessionProperties::kDebugDisableExpressionWithPeeling,
       core::QueryConfig::kDebugDisableExpressionWithPeeling},
      {SessionProperties::kDebugDisableCommonSubExpressions,
       core::QueryConfig::kDebugDisableCommonSubExpressions},
      {SessionProperties::kDebugDisableExpressionWithMemoization,
       core::QueryConfig::kDebugDisableExpressionWithMemoization},
      {SessionProperties::kDebugDisableExpressionWithLazyInputs,
       core::QueryConfig::kDebugDisableExpressionWithLazyInputs},
      {SessionProperties::kDebugMemoryPoolNameRegex,
       core::QueryConfig::kDebugMemoryPoolNameRegex},
      {SessionProperties::kDebugMemoryPoolWarnThresholdBytes,
       core::QueryConfig::kDebugMemoryPoolWarnThresholdBytes},
      {SessionProperties::kSelectiveNimbleReaderEnabled,
       core::QueryConfig::kSelectiveNimbleReaderEnabled},
      {SessionProperties::kQueryTraceEnabled,
       core::QueryConfig::kQueryTraceEnabled},
      {SessionProperties::kQueryTraceDir, core::QueryConfig::kQueryTraceDir},
      {SessionProperties::kQueryTraceNodeId,
       core::QueryConfig::kQueryTraceNodeId},
      {SessionProperties::kQueryTraceMaxBytes,
       core::QueryConfig::kQueryTraceMaxBytes},
      {SessionProperties::kOpTraceDirectoryCreateConfig,
       core::QueryConfig::kOpTraceDirectoryCreateConfig},
      {SessionProperties::kMaxOutputBufferSize,
       core::QueryConfig::kMaxOutputBufferSize},
      {SessionProperties::kMaxPartitionedOutputBufferSize,
       core::QueryConfig::kMaxPartitionedOutputBufferSize},
      {SessionProperties::kLegacyTimestamp,
       core::QueryConfig::kAdjustTimestampToTimezone},
      {SessionProperties::kDriverCpuTimeSliceLimitMs,
       core::QueryConfig::kDriverCpuTimeSliceLimitMs},
      {SessionProperties::kMaxLocalExchangePartitionCount,
       core::QueryConfig::kMaxLocalExchangePartitionCount},
      {SessionProperties::kSpillPrefixSortEnabled,
       core::QueryConfig::kSpillPrefixSortEnabled},
      {SessionProperties::kPrefixSortNormalizedKeyMaxBytes,
       core::QueryConfig::kPrefixSortNormalizedKeyMaxBytes},
      {SessionProperties::kPrefixSortMinRows,
       core::QueryConfig::kPrefixSortMinRows},
      {SessionProperties::kScaleWriterRebalanceMaxMemoryUsageRatio,
       core::QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio},
      {SessionProperties::kScaleWriterMaxPartitionsPerWriter,
       core::QueryConfig::kScaleWriterMaxPartitionsPerWriter},
      {SessionProperties::
           kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
       core::QueryConfig::
           kScaleWriterMinPartitionProcessedBytesRebalanceThreshold},
      {SessionProperties::kScaleWriterMinProcessedBytesRebalanceThreshold,
       core::QueryConfig::kScaleWriterMinProcessedBytesRebalanceThreshold},
      {SessionProperties::kTableScanScaledProcessingEnabled,
       core::QueryConfig::kTableScanScaledProcessingEnabled},
      {SessionProperties::kTableScanScaleUpMemoryUsageRatio,
       core::QueryConfig::kTableScanScaleUpMemoryUsageRatio},
      {SessionProperties::kStreamingAggregationMinOutputBatchRows,
       core::QueryConfig::kStreamingAggregationMinOutputBatchRows},
      {SessionProperties::kRequestDataSizesMaxWaitSec,
       core::QueryConfig::kRequestDataSizesMaxWaitSec},
      {SessionProperties::kNativeQueryMemoryReclaimerPriority,
       core::QueryConfig::kQueryMemoryReclaimerPriority},
      {SessionProperties::kMaxNumSplitsListenedTo,
       core::QueryConfig::kMaxNumSplitsListenedTo},
      {SessionProperties::kIndexLookupJoinMaxPrefetchBatches,
       core::QueryConfig::kIndexLookupJoinMaxPrefetchBatches},
      {SessionProperties::kIndexLookupJoinSplitOutput,
       core::QueryConfig::kIndexLookupJoinSplitOutput},
      {SessionProperties::kUnnestSplitOutput,
       core::QueryConfig::kUnnestSplitOutput}};

  const auto& sessionProperties =
      SessionProperties::instance()->testingSessionProperties();

  ASSERT_EQ(expectedMappings.size(), sessionProperties.size());

  for (const auto& [sessionPropertyName, expectedVeloxConfigName] :
       expectedMappings) {
    ASSERT_EQ(
        expectedVeloxConfigName,
        sessionProperties.at(sessionPropertyName)->getVeloxConfigName());
  }
}

TEST_F(SessionPropertiesTest, serializeProperty) {
  auto* sessionProperties = SessionProperties::instance();
  auto j = sessionProperties->serialize();
  for (const auto& property : j) {
    auto name = property["name"];
    json expectedProperty =
        sessionProperties->testingSessionProperties().at(name)->serialize();
    EXPECT_EQ(property, expectedProperty);
  }
}
