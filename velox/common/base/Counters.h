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

#pragma once

#include <folly/Range.h>

namespace facebook::velox {

/// Velox metrics Registration.
void registerVeloxMetrics();

constexpr folly::StringPiece kMetricHiveFileHandleGenerateLatencyMs{
    "velox.hive_file_handle_generate_latency_ms"};

constexpr folly::StringPiece kMetricCacheShrinkCount{
    "velox.cache_shrink_count"};

constexpr folly::StringPiece kMetricCacheShrinkTimeMs{"velox.cache_shrink_ms"};

constexpr folly::StringPiece kMetricMaxSpillLevelExceededCount{
    "velox.spill_max_level_exceeded_count"};

constexpr folly::StringPiece kMetricMemoryReclaimExecTimeMs{
    "velox.memory_reclaim_exec_ms"};

constexpr folly::StringPiece kMetricMemoryReclaimedBytes{
    "velox.memory_reclaim_bytes"};

constexpr folly::StringPiece kMetricMemoryReclaimWaitTimeMs{
    "velox.memory_reclaim_wait_ms"};

constexpr folly::StringPiece kMetricMemoryReclaimWaitTimeoutCount{
    "velox.memory_reclaim_wait_timeout_count"};

constexpr folly::StringPiece kMetricMemoryNonReclaimableCount{
    "velox.memory_non_reclaimable_count"};

constexpr folly::StringPiece kMetricArbitratorRequestsCount{
    "velox.arbitrator_requests_count"};

constexpr folly::StringPiece kMetricArbitratorAbortedCount{
    "velox.arbitrator_aborted_count"};

constexpr folly::StringPiece kMetricArbitratorFailuresCount{
    "velox.arbitrator_failures_count"};

constexpr folly::StringPiece kMetricArbitratorQueueTimeMs{
    "velox.arbitrator_queue_time_ms"};

constexpr folly::StringPiece kMetricArbitratorArbitrationTimeMs{
    "velox.arbitrator_arbitration_time_ms"};

constexpr folly::StringPiece kMetricArbitratorFreeCapacityBytes{
    "velox.arbitrator_free_capacity_bytes"};
} // namespace facebook::velox
