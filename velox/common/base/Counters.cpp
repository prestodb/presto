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

#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"

namespace facebook::velox {

void registerVeloxMetrics() {
  // Tracks hive handle generation latency in range of [0, 100s] and reports
  // P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricHiveFileHandleGenerateLatencyMs, 10, 0, 100000, 50, 90, 99, 100);

  DEFINE_METRIC(kMetricCacheShrinkCount, facebook::velox::StatType::COUNT);

  // Tracks cache shrink latency in range of [0, 100s] and reports P50, P90,
  // P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricCacheShrinkTimeMs, 10, 0, 100'000, 50, 90, 99, 100);

  // Tracks memory reclaim exec time in range of [0, 600s] and reports
  // P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricMemoryReclaimExecTimeMs, 20, 0, 600'000, 50, 90, 99, 100);

  // Tracks memory reclaim task wait time in range of [0, 60s] and reports
  // P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricMemoryReclaimWaitTimeMs, 10, 0, 60'000, 50, 90, 99, 100);

  // Tracks memory reclaim bytes.
  DEFINE_METRIC(kMetricMemoryReclaimedBytes, facebook::velox::StatType::SUM);

  // Tracks the number of times that the memory reclaim wait timeouts.
  DEFINE_METRIC(
      kMetricMemoryReclaimWaitTimeoutCount, facebook::velox::StatType::SUM);

  // Tracks the number of times that the memory reclaim fails because of
  // non-reclaimable section which is an indicator that the memory reservation
  // is not sufficient.
  DEFINE_METRIC(
      kMetricMemoryNonReclaimableCount, facebook::velox::StatType::COUNT);

  // Tracks the number of times that we hit the max spill level limit.
  DEFINE_METRIC(
      kMetricMaxSpillLevelExceededCount, facebook::velox::StatType::COUNT);
}
} // namespace facebook::velox
