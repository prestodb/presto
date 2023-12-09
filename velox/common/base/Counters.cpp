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

  // Tracks cache shrink latency in range of [0, 100s] with 10 buckets and
  // reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricCacheShrinkTimeMs, 10, 0, 100'000, 50, 90, 99, 100);

  // Tracks the number of times that we hit the max spill level limit.
  DEFINE_METRIC(
      kMetricMaxSpillLevelExceededCount, facebook::velox::StatType::COUNT);

  /// ================== Memory Arbitration Counters =================

  // Tracks memory reclaim exec time in range of [0, 600s] with 20 buckets and
  // reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricMemoryReclaimExecTimeMs, 20, 0, 600'000, 50, 90, 99, 100);

  // Tracks memory reclaim task wait time in range of [0, 60s] with 10 buckets
  // and reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricMemoryReclaimWaitTimeMs, 10, 0, 60'000, 50, 90, 99, 100);

  // Tracks memory reclaim bytes.
  DEFINE_METRIC(kMetricMemoryReclaimedBytes, facebook::velox::StatType::SUM);

  // Tracks the number of times that the memory reclaim wait timeouts.
  DEFINE_METRIC(
      kMetricMemoryReclaimWaitTimeoutCount, facebook::velox::StatType::SUM);

  // The number of times that the memory reclaim fails because the operator is
  // executing a non-reclaimable section where it is expected to have reserved
  // enough memory to execute without asking for more. Therefore, it is an
  // indicator that the memory reservation is not sufficient. It excludes
  // counting instances where the operator is in a non-reclaimable state due to
  // currently being on-thread and running or being already cancelled.
  DEFINE_METRIC(
      kMetricMemoryNonReclaimableCount, facebook::velox::StatType::COUNT);

  // The number of arbitration requests.
  DEFINE_METRIC(
      kMetricArbitratorRequestsCount, facebook::velox::StatType::COUNT);

  // The number of times a query level memory pool is aborted as a result of a
  // memory arbitration process. The memory pool aborted will eventually result
  // in a cancelling the original query.
  DEFINE_METRIC(
      kMetricArbitratorAbortedCount, facebook::velox::StatType::COUNT);

  // The number of times a memory arbitration request failed. This may occur
  // either because the requester was terminated during the processing of its
  // request, the arbitration request would surpass the maximum allowed capacity
  // for the requester, or the arbitration process couldn't release the
  // requested amount of memory.
  DEFINE_METRIC(
      kMetricArbitratorFailuresCount, facebook::velox::StatType::COUNT);

  // The distribution of the amount of time an arbitration request stays queued
  // in range of [0, 600s] with 20 buckets. It is configured to report the
  // latency at P50, P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricArbitratorQueueTimeMs, 20, 0, 600'000, 50, 90, 99, 100);

  // The distribution of the amount of time it take to complete a single
  // arbitration request stays queued in range of [0, 600s] with 20
  // buckets. It is configured to report the latency at P50, P90, P99,
  // and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricArbitratorArbitrationTimeMs, 20, 0, 600'000, 50, 90, 99, 100);

  /// Tracks the average of free memory capacity managed by the arbitrator in
  /// bytes.
  DEFINE_METRIC(
      kMetricArbitratorFreeCapacityBytes, facebook::velox::StatType::AVG);
}
} // namespace facebook::velox
