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

#include "presto_cpp/main/properties/session/SessionProperties.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/DefaultOutputBufferManager.h"
#include "velox/exec/ExchangeTransportRegistry.h"
#include "velox/exec/InMemoryExchangeClient.h"
#include "velox/exec/OutputTransportRegistry.h"

using namespace facebook::presto;
using namespace facebook::velox;

class SessionPropertiesTest : public testing::Test {};

TEST_F(SessionPropertiesTest, validateMapping) {
  const std::unordered_map<std::string, std::string> expectedMappings = {
      {SessionProperties::kBroadcastFileDescriptorEnabled,
       SessionProperties::kBroadcastFileDescriptorEnabled},
      {SessionProperties::kCudfExchangeEnabled,
       SessionProperties::kCudfExchangeEnabledConfig},
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
      {SessionProperties::kRpcRateLimiterAdaptiveEnabled,
       core::QueryConfig::kRpcRateLimiterAdaptiveEnabled},
      {SessionProperties::kRpcRateLimiterMinLimit,
       core::QueryConfig::kRpcRateLimiterMinLimit},
      {SessionProperties::kRpcRateLimiterDecreaseFactor,
       core::QueryConfig::kRpcRateLimiterDecreaseFactor},
      {SessionProperties::kRpcRateLimiterMaxLimit,
       core::QueryConfig::kRpcRateLimiterMaxLimit},
      {SessionProperties::kRpcRateLimiterHardLimit,
       "rpc.ratelimiter.hard_limit"},
      {SessionProperties::kRpcCongestionMaxWindow,
       core::QueryConfig::kRpcCongestionMaxWindow},
      {SessionProperties::kRpcCongestionBatchStartWindow,
       core::QueryConfig::kRpcCongestionBatchStartWindow},
      {SessionProperties::kRpcCongestionAsyncGrowthMaxWindow,
       core::QueryConfig::kRpcCongestionAsyncGrowthMaxWindow},
      {SessionProperties::kSpillCompressionCodec,
       core::QueryConfig::kSpillCompressionKind},
      {SessionProperties::kSpillWriteBufferSize,
       core::QueryConfig::kSpillWriteBufferSize},
      {SessionProperties::kSpillFileCreateConfig,
       core::QueryConfig::kSpillFileCreateConfig},
      {SessionProperties::kAggregationSpillFileCreateConfig,
       core::QueryConfig::kAggregationSpillFileCreateConfig},
      {SessionProperties::kHashJoinSpillFileCreateConfig,
       core::QueryConfig::kHashJoinSpillFileCreateConfig},
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
      {SessionProperties::kMarkDistinctSpillEnabled,
       core::QueryConfig::kMarkDistinctSpillEnabled},
      {SessionProperties::kSpillerNumPartitionBits,
       core::QueryConfig::kSpillNumPartitionBits},
      {SessionProperties::kTopNRowNumberSpillEnabled,
       core::QueryConfig::kTopNRowNumberSpillEnabled},
      {SessionProperties::kAbandonPartialAggregationMinRows,
       core::QueryConfig::kAbandonPartialAggregationMinRows},
      {SessionProperties::kAbandonPartialAggregationMinPct,
       core::QueryConfig::kAbandonPartialAggregationMinPct},
      {SessionProperties::kAbandonPartialTopNRowNumberMinRows,
       core::QueryConfig::kAbandonPartialTopNRowNumberMinRows},
      {SessionProperties::kAbandonPartialTopNRowNumberMinPct,
       core::QueryConfig::kAbandonPartialTopNRowNumberMinPct},
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
      {SessionProperties::kPartitionedOutputEagerFlush,
       core::QueryConfig::kPartitionedOutputEagerFlush},
      {SessionProperties::kMinShuffleCompressionPageSizeBytes,
       core::QueryConfig::kMinShuffleCompressionPageSizeBytes},
      {SessionProperties::kLegacyTimestamp,
       core::QueryConfig::kAdjustTimestampToTimezone},
      {SessionProperties::kLegacyTimestampWithTimezone,
       core::QueryConfig::kIgnoreTimeZoneLabel},
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
       core::QueryConfig::kUnnestSplitOutput},
      {SessionProperties::kUseVeloxGeospatialJoin,
       SessionProperties::kUseVeloxGeospatialJoin},
      {SessionProperties::kAggregationCompactionBytesThreshold,
       core::QueryConfig::kAggregationCompactionBytesThreshold},
      {SessionProperties::kAggregationCompactionUnusedMemoryRatio,
       core::QueryConfig::kAggregationCompactionUnusedMemoryRatio},
      {SessionProperties::kAggregationMemoryCompactionReclaimEnabled,
       core::QueryConfig::kAggregationMemoryCompactionReclaimEnabled},
      {SessionProperties::kMergeJoinOutputBatchStartSize,
       core::QueryConfig::kMergeJoinOutputBatchStartSize}};

  const auto sessionProperties = SessionProperties::instance();
  for (const auto& [sessionProperty, expectedVeloxConfig] : expectedMappings) {
    ASSERT_EQ(
        expectedVeloxConfig, sessionProperties->toVeloxConfig(sessionProperty));
  }
}

// The advertised default of native_cudf_exchange_enabled is false whether or
// not this worker has the UCX transport registered, so it does not depend on
// when SessionProperties is first created. An unset property is resolved per
// exchange edge by the plan converter instead.
TEST_F(SessionPropertiesTest, cudfExchangeEnabledDefaultIsFalse) {
  // Stand-ins for what registerCudf() registers on a cuDF + UCX worker.
  const std::string kUcx{core::TransportKind::kUcx};
  exec::OutputTransportRegistry::global().insert(
      kUcx,
      exec::DefaultOutputBufferManager::makeDefaultTransportEntry(),
      /*overwrite=*/true);
  exec::ExchangeTransportRegistry::global().insert(
      kUcx,
      exec::InMemoryExchangeClient::makeDefaultTransportEntry(),
      /*overwrite=*/true);
  ASSERT_TRUE(SessionProperties::isCudfExchangeAvailable());

  std::optional<std::string> defaultValue;
  for (const auto& property : SessionProperties::instance()->serialize()) {
    if (property["name"] == SessionProperties::kCudfExchangeEnabled) {
      defaultValue = property["defaultValue"].get<std::string>();
    }
  }

  exec::OutputTransportRegistry::global().erase(kUcx);
  exec::ExchangeTransportRegistry::global().erase(kUcx);

  ASSERT_TRUE(defaultValue.has_value());
  EXPECT_EQ(defaultValue.value(), "false");
}
