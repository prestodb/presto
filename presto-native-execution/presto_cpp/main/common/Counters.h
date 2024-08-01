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
#pragma once

#include <folly/Range.h>

// Here we have all the counters presto cpp worker would export.
namespace facebook::presto {

// Sets up all the counters in the presto cpp, but specifying their types.
// See velox/common/base/StatsReporter.h for the interface.
void registerPrestoMetrics();

constexpr folly::StringPiece kCounterDriverCPUExecutorQueueSize{
    "presto_cpp.driver_cpu_executor_queue_size"};
constexpr folly::StringPiece kCounterDriverCPUExecutorLatencyMs{
    "presto_cpp.driver_cpu_executor_latency_ms"};
constexpr folly::StringPiece kCounterSpillerExecutorQueueSize{
    "presto_cpp.spiller_executor_queue_size"};
constexpr folly::StringPiece kCounterSpillerExecutorLatencyMs{
    "presto_cpp.spiller_executor_latency_ms"};
constexpr folly::StringPiece kCounterHTTPExecutorLatencyMs{
    "presto_cpp.http_executor_latency_ms"};
constexpr folly::StringPiece kCounterNumHTTPRequest{
    "presto_cpp.num_http_request"};
constexpr folly::StringPiece kCounterNumHTTPRequestError{
    "presto_cpp.num_http_request_error"};
constexpr folly::StringPiece kCounterHTTPRequestLatencyMs{
    "presto_cpp.http_request_latency_ms"};

constexpr folly::StringPiece kCounterHttpClientNumConnectionsCreated{
    "presto_cpp.http.client.num_connections_created"};
/// Peak number of bytes queued in PrestoExchangeSource waiting for consume.
constexpr folly::StringPiece kCounterExchangeSourcePeakQueuedBytes{
    "presto_cpp.exchange_source_peak_queued_bytes"};

constexpr folly::StringPiece kCounterNumQueryContexts{
    "presto_cpp.num_query_contexts"};

constexpr folly::StringPiece kCounterNumTasks{"presto_cpp.num_tasks"};
constexpr folly::StringPiece kCounterNumTasksRunning{
    "presto_cpp.num_tasks_running"};
constexpr folly::StringPiece kCounterNumTasksFinished{
    "presto_cpp.num_tasks_finished"};
constexpr folly::StringPiece kCounterNumTasksCancelled{
    "presto_cpp.num_tasks_cancelled"};
constexpr folly::StringPiece kCounterNumTasksAborted{
    "presto_cpp.num_tasks_aborted"};
constexpr folly::StringPiece kCounterNumTasksFailed{
    "presto_cpp.num_tasks_failed"};
constexpr folly::StringPiece kCounterNumZombieVeloxTasks{
    "presto_cpp.num_zombie_velox_tasks"};
constexpr folly::StringPiece kCounterNumZombiePrestoTasks{
    "presto_cpp.num_zombie_presto_tasks"};
constexpr folly::StringPiece kCounterNumTasksWithStuckOperator{
    "presto_cpp.num_tasks_with_stuck_operator"};
constexpr folly::StringPiece kCounterNumTasksDeadlock{
    "presto_cpp.num_tasks_deadlock"};
constexpr folly::StringPiece kCounterNumTaskManagerLockTimeOut{
    "presto_cpp.num_tasks_manager_lock_timeout"};

constexpr folly::StringPiece kCounterNumQueuedDrivers{
    "presto_cpp.num_queued_drivers"};
constexpr folly::StringPiece kCounterNumOnThreadDrivers{
    "presto_cpp.num_on_thread_drivers"};
constexpr folly::StringPiece kCounterNumSuspendedDrivers{
    "presto_cpp.num_suspended_drivers"};
constexpr folly::StringPiece kCounterNumBlockedWaitForConsumerDrivers{
    "presto_cpp.num_blocked_wait_for_consumer_drivers"};
constexpr folly::StringPiece kCounterNumBlockedWaitForSplitDrivers{
    "presto_cpp.num_blocked_wait_for_split_drivers"};
constexpr folly::StringPiece kCounterNumBlockedWaitForProducerDrivers{
    "presto_cpp.num_blocked_wait_for_producer_drivers"};
constexpr folly::StringPiece kCounterNumBlockedWaitForJoinBuildDrivers{
    "presto_cpp.num_blocked_wait_for_join_build_drivers"};
constexpr folly::StringPiece kCounterNumBlockedWaitForJoinProbeDrivers{
    "presto_cpp.num_blocked_wait_for_join_probe_drivers"};
constexpr folly::StringPiece kCounterNumBlockedWaitForMergeJoinRightSideDrivers{
    "presto_cpp.num_blocked_wait_for_merge_join_right_side_drivers"};
constexpr folly::StringPiece kCounterNumBlockedWaitForMemoryDrivers{
    "presto_cpp.num_blocked_wait_for_memory_drivers"};
constexpr folly::StringPiece kCounterNumBlockedWaitForConnectorDrivers{
    "presto_cpp.num_blocked_wait_for_connector_drivers"};
constexpr folly::StringPiece kCounterNumBlockedWaitForSpillDrivers{
    "presto_cpp.num_blocked_wait_for_spill_drivers"};
constexpr folly::StringPiece kCounterNumBlockedYieldDrivers{
    "presto_cpp.num_blocked_yield_drivers"};
constexpr folly::StringPiece kCounterNumStuckDrivers{
    "presto_cpp.num_stuck_drivers"};

/// Number of total OutputBuffer managed by all
/// OutputBufferManager
constexpr folly::StringPiece kCounterTotalPartitionedOutputBuffer{
    "presto_cpp.num_partitioned_output_buffer"};
/// Latency in millisecond of the get data call of a
/// OutputBufferManager.
constexpr folly::StringPiece kCounterPartitionedOutputBufferGetDataLatencyMs{
    "presto_cpp.partitioned_output_buffer_get_data_latency_ms"};

/// ================== OS Counters =================

/// User CPU time of the presto_server process in microsecond since the process
/// start.
constexpr folly::StringPiece kCounterOsUserCpuTimeMicros{
    "presto_cpp.os_user_cpu_time_micros"};
/// System CPU time of the presto_server process in microsecond since the
/// process start.
constexpr folly::StringPiece kCounterOsSystemCpuTimeMicros{
    "presto_cpp.os_system_cpu_time_micros"};
/// Total number of soft page faults of the presto_server process in microsecond
/// since the process start.
constexpr folly::StringPiece kCounterOsNumSoftPageFaults{
    "presto_cpp.os_num_soft_page_faults"};
/// Total number of hard page faults of the presto_server process in microsecond
/// since the process start.
constexpr folly::StringPiece kCounterOsNumHardPageFaults{
    "presto_cpp.os_num_hard_page_faults"};
/// Total number of voluntary context switches in the presto_server process.
constexpr folly::StringPiece kCounterOsNumVoluntaryContextSwitches{
    "presto_cpp.os_num_voluntary_context_switches"};
/// Total number of involuntary context switches in the presto_server process.
constexpr folly::StringPiece kCounterOsNumForcedContextSwitches{
    "presto_cpp.os_num_forced_context_switches"};

/// ================== HiveConnector Counters ==================
/// Format template strings use 'constexpr std::string_view' to be 'fmt::format'
/// compatible.
constexpr std::string_view kCounterHiveFileHandleCacheNumElementsFormat{
    "presto_cpp.{}.hive_file_handle_cache_num_elements"};
constexpr std::string_view kCounterHiveFileHandleCachePinnedSizeFormat{
    "presto_cpp.{}.hive_file_handle_cache_pinned_size"};
constexpr std::string_view kCounterHiveFileHandleCacheCurSizeFormat{
    "presto_cpp.{}.hive_file_handle_cache_cur_size"};
constexpr std::string_view kCounterHiveFileHandleCacheNumAccumulativeHitsFormat{
    "presto_cpp.{}.hive_file_handle_cache_num_accumulative_hits"};
constexpr std::string_view
    kCounterHiveFileHandleCacheNumAccumulativeLookupsFormat{
        "presto_cpp.{}.hive_file_handle_cache_num_accumulative_lookups"};
constexpr std::string_view kCounterHiveFileHandleCacheNumHitsFormat{
    "presto_cpp.{}.hive_file_handle_cache_num_hits"};
constexpr std::string_view kCounterHiveFileHandleCacheNumLookupsFormat{
    "presto_cpp.{}.hive_file_handle_cache_num_lookups"};
} // namespace facebook::presto
