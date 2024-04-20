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
  /// ================== Task Execution Counters =================
  // The number of driver yield count when exceeds the per-driver cpu time slice
  // limit if enforced.
  DEFINE_METRIC(kMetricDriverYieldCount, facebook::velox::StatType::COUNT);

  /// ================== Cache Counters =================

  // Tracks hive handle generation latency in range of [0, 100s] and reports
  // P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricHiveFileHandleGenerateLatencyMs,
      10'000,
      0,
      100'000,
      50,
      90,
      99,
      100);

  DEFINE_METRIC(kMetricCacheShrinkCount, facebook::velox::StatType::COUNT);

  // Tracks cache shrink latency in range of [0, 100s] with 10 buckets and
  // reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricCacheShrinkTimeMs, 10'000, 0, 100'000, 50, 90, 99, 100);

  /// ================== Memory Arbitration Counters =================

  // Tracks the memory reclaim count on an operator.
  DEFINE_METRIC(kMetricMemoryReclaimCount, facebook::velox::StatType::COUNT);

  // Tracks op memory reclaim exec time in range of [0, 600s] with 20 buckets
  // and reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricMemoryReclaimExecTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // Tracks op memory reclaim bytes.
  DEFINE_METRIC(kMetricMemoryReclaimedBytes, facebook::velox::StatType::SUM);

  // Tracks the memory reclaim count on an operator.
  DEFINE_METRIC(
      kMetricTaskMemoryReclaimCount, facebook::velox::StatType::COUNT);

  // Tracks memory reclaim task wait time in range of [0, 60s] with 10 buckets
  // and reports P50, P90, P99, and P100.
  DEFINE_HISTOGRAM_METRIC(
      kMetricTaskMemoryReclaimWaitTimeMs, 6'000, 0, 60'000, 50, 90, 99, 100);

  // Tracks the number of times that the task memory reclaim wait timeouts.
  DEFINE_METRIC(
      kMetricTaskMemoryReclaimWaitTimeoutCount,
      facebook::velox::StatType::COUNT);

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

  // The number of arbitration that reclaims the used memory from the query
  // which initiates the memory arbitration request itself. It ensures the
  // memory arbitration request won't exceed its per-query memory capacity
  // limit.
  DEFINE_METRIC(
      kMetricArbitratorLocalArbitrationCount, facebook::velox::StatType::COUNT);

  // The number of arbitration which ensures the total allocated query capacity
  // won't exceed the arbitrator capacity limit. It may or may not reclaim
  // memory from the query which initiate the memory arbitration request. This
  // indicates the velox runtime doesn't have enough memory to run all the
  // queries at their peak memory usage. We have to trigger spilling to let them
  // run through completion.
  DEFINE_METRIC(
      kMetricArbitratorGlobalArbitrationCount,
      facebook::velox::StatType::COUNT);

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
      kMetricArbitratorQueueTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // The distribution of the amount of time it take to complete a single
  // arbitration request stays queued in range of [0, 600s] with 20
  // buckets. It is configured to report the latency at P50, P90, P99,
  // and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricArbitratorArbitrationTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // Tracks the average of free memory capacity managed by the arbitrator in
  // bytes.
  DEFINE_METRIC(
      kMetricArbitratorFreeCapacityBytes, facebook::velox::StatType::AVG);

  DEFINE_METRIC(
      kMetricArbitratorFreeReservedCapacityBytes,
      facebook::velox::StatType::AVG);

  // Tracks the leaf memory pool usage leak in bytes.
  DEFINE_METRIC(
      kMetricMemoryPoolUsageLeakBytes, facebook::velox::StatType::SUM);

  // Tracks the leaf memory pool reservation leak in bytes.
  DEFINE_METRIC(
      kMetricMemoryPoolReservationLeakBytes, facebook::velox::StatType::SUM);

  // The distribution of a root memory pool's initial capacity in range of [0,
  // 256MB] with 32 buckets. It is configured to report the capacity at P50,
  // P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricMemoryPoolInitialCapacityBytes,
      8L << 20,
      0,
      256L << 20,
      50,
      90,
      99,
      100);

  // The distribution of a root memory pool cappacity growth attempts through
  // memory arbitration in range of [0, 256] with 32 buckets. It is configured
  // to report the count at P50, P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricMemoryPoolCapacityGrowCount, 8, 0, 256, 50, 90, 99, 100);

  // Tracks the count of double frees in memory allocator, indicating the
  // possibility of buffer ownership issues when a buffer is freed more than
  // once.
  DEFINE_METRIC(
      kMetricMemoryAllocatorDoubleFreeCount, facebook::velox::StatType::COUNT);

  /// ================== Spill related Counters =================

  // The number of bytes in memory to spill.
  DEFINE_METRIC(kMetricSpilledInputBytes, facebook::velox::StatType::SUM);

  // The number of bytes spilled to disk which can be number of compressed
  // bytes if compression is enabled.
  DEFINE_METRIC(kMetricSpilledBytes, facebook::velox::StatType::SUM);

  // The number of spilled rows.
  DEFINE_METRIC(kMetricSpilledRowsCount, facebook::velox::StatType::COUNT);

  // The number of spilled files.
  DEFINE_METRIC(kMetricSpilledFilesCount, facebook::velox::StatType::COUNT);

  // The distribution of the amount of time spent on filling rows for spilling.
  // in range of [0, 600s] with 20 buckets. It is configured to report the
  // latency at P50, P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricSpillFillTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // The distribution of the amount of time spent on sorting rows for spilling
  // in range of [0, 600s] with 20 buckets. It is configured to report the
  // latency at P50, P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricSpillSortTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // The distribution of the amount of time spent on serializing rows for
  // spilling in range of [0, 600s] with 20 buckets. It is configured to report
  // the latency at P50, P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricSpillSerializationTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // The number of spill writes to storage, which is the number of write calls
  // to velox filesystem.
  DEFINE_METRIC(kMetricSpillWritesCount, facebook::velox::StatType::COUNT);

  // The distribution of the amount of time spent on copy out serialized
  // rows for disk write in range of [0, 600s] with 20 buckets. It is configured
  // to report the latency at P50, P90, P99, and P100 percentiles. Note:  If
  // compression is enabled, this includes the compression time.
  DEFINE_HISTOGRAM_METRIC(
      kMetricSpillFlushTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // The distribution of the amount of time spent on writing spilled rows to
  // disk in range of [0, 600s] with 20 buckets. It is configured to report the
  // latency at P50, P90, P99, and P100 percentiles.
  DEFINE_HISTOGRAM_METRIC(
      kMetricSpillWriteTimeMs, 30'000, 0, 600'000, 50, 90, 99, 100);

  // Tracks the number of times that we hit the max spill level limit.
  DEFINE_METRIC(
      kMetricMaxSpillLevelExceededCount, facebook::velox::StatType::COUNT);

  // Tracks the total number of bytes in file writers that's pre-maturely
  // flushed due to memory reclaiming.
  DEFINE_METRIC(
      kMetricFileWriterEarlyFlushedRawBytes, facebook::velox::StatType::SUM);
}
} // namespace facebook::velox
