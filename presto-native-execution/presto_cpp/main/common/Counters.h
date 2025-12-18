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

#include <string_view>

// Here we have all the counters presto cpp worker would export.
namespace facebook::presto {

// Sets up all the counters in the presto cpp, but specifying their types.
// See velox/common/base/StatsReporter.h for the interface.
void registerPrestoMetrics();

constexpr std::string_view kCounterDriverCPUExecutorQueueSize{
    "presto_cpp.driver_cpu_executor_queue_size"};
constexpr std::string_view kCounterDriverCPUExecutorLatencyMs{
    "presto_cpp.driver_cpu_executor_latency_ms"};
constexpr std::string_view kCounterSpillerExecutorQueueSize{
    "presto_cpp.spiller_executor_queue_size"};
constexpr std::string_view kCounterSpillerExecutorLatencyMs{
    "presto_cpp.spiller_executor_latency_ms"};
constexpr std::string_view kCounterHTTPExecutorLatencyMs{
    "presto_cpp.http_executor_latency_ms"};
constexpr std::string_view kCounterNumHTTPRequest{
    "presto_cpp.num_http_request"};
constexpr std::string_view kCounterNumHTTPRequestError{
    "presto_cpp.num_http_request_error"};
constexpr std::string_view kCounterHTTPRequestLatencyMs{
    "presto_cpp.http_request_latency_ms"};
constexpr std::string_view kCounterHTTPRequestSizeBytes{
    "presto_cpp.http_request_size_bytes"};

constexpr std::string_view kCounterHttpClientNumConnectionsCreated{
    "presto_cpp.http.client.num_connections_created"};
/// Number of HTTP requests that are the first request on a connection
// (seqNo == 0).
constexpr std::string_view kCounterHttpClientConnectionFirstUse{
    "presto_cpp.http.client.connection_first_use"};
/// Number of HTTP requests sent on reused connections (seqNo > 0).
constexpr std::string_view kCounterHttpClientConnectionReuse{
    "presto_cpp.http.client.connection_reuse"};
constexpr std::string_view kCounterHTTPClientTransactionCreateDelay{
    "presto_cpp.http.client.transaction_create_delay_ms"};
/// Peak number of bytes queued in PrestoExchangeSource waiting for consume.
constexpr std::string_view kCounterExchangeSourcePeakQueuedBytes{
    "presto_cpp.exchange_source_peak_queued_bytes"};

constexpr std::string_view kCounterExchangeRequestDuration{
    "presto_cpp.exchange.request.duration"};
constexpr std::string_view kCounterExchangeRequestNumTries{
    "presto_cpp.exchange.request.num_tries"};
constexpr std::string_view kCounterExchangeRequestPageSize{
    "presto_cpp.exchange.request.page_size"};

constexpr std::string_view kCounterExchangeGetDataSizeDuration{
    "presto_cpp.exchange.get_data_size.duration"};
constexpr std::string_view kCounterExchangeGetDataSizeNumTries{
    "presto_cpp.exchange.get_data_size.num_tries"};

constexpr std::string_view kCounterNumQueryContexts{
    "presto_cpp.num_query_contexts"};
/// Export total bytes used by memory manager (in queries' memory pools).
constexpr std::string_view kCounterMemoryManagerTotalBytes{
    "presto_cpp.memory_manager_total_bytes"};

constexpr std::string_view kCounterNumTasks{"presto_cpp.num_tasks"};
constexpr std::string_view kCounterNumTasksBytesProcessed{
    "presto_cpp.num_tasks_bytes_processed"};
constexpr std::string_view kCounterNumTasksRunning{
    "presto_cpp.num_tasks_running"};
constexpr std::string_view kCounterNumTasksFinished{
    "presto_cpp.num_tasks_finished"};
constexpr std::string_view kCounterNumTasksCancelled{
    "presto_cpp.num_tasks_cancelled"};
constexpr std::string_view kCounterNumTasksAborted{
    "presto_cpp.num_tasks_aborted"};
constexpr std::string_view kCounterNumTasksFailed{
    "presto_cpp.num_tasks_failed"};
/// Number of the created but not yet started tasks, including queued tasks.
constexpr std::string_view kCounterNumTasksPlanned{
    "presto_cpp.num_tasks_planned"};
/// Number of the created tasks in the task queue.
constexpr std::string_view kCounterNumTasksQueued{
    "presto_cpp.num_tasks_queued"};

constexpr std::string_view kCounterNumZombieVeloxTasks{
    "presto_cpp.num_zombie_velox_tasks"};
constexpr std::string_view kCounterNumZombiePrestoTasks{
    "presto_cpp.num_zombie_presto_tasks"};
constexpr std::string_view kCounterNumTasksWithStuckOperator{
    "presto_cpp.num_tasks_with_stuck_operator"};
constexpr std::string_view kCounterNumCancelledTasksByStuckDriver{
    "presto_cpp.num_cancelled_tasks_by_stuck_driver"};
constexpr std::string_view kCounterNumTasksDeadlock{
    "presto_cpp.num_tasks_deadlock"};
constexpr std::string_view kCounterNumTaskManagerLockTimeOut{
    "presto_cpp.num_tasks_manager_lock_timeout"};

constexpr std::string_view kCounterNumQueuedDrivers{
    "presto_cpp.num_queued_drivers"};
constexpr std::string_view kCounterNumOnThreadDrivers{
    "presto_cpp.num_on_thread_drivers"};
constexpr std::string_view kCounterNumSuspendedDrivers{
    "presto_cpp.num_suspended_drivers"};
constexpr std::string_view kCounterNumBlockedWaitForConsumerDrivers{
    "presto_cpp.num_blocked_wait_for_consumer_drivers"};
constexpr std::string_view kCounterNumBlockedWaitForSplitDrivers{
    "presto_cpp.num_blocked_wait_for_split_drivers"};
constexpr std::string_view kCounterNumBlockedWaitForProducerDrivers{
    "presto_cpp.num_blocked_wait_for_producer_drivers"};
constexpr std::string_view kCounterNumBlockedWaitForJoinBuildDrivers{
    "presto_cpp.num_blocked_wait_for_join_build_drivers"};
constexpr std::string_view kCounterNumBlockedWaitForJoinProbeDrivers{
    "presto_cpp.num_blocked_wait_for_join_probe_drivers"};
constexpr std::string_view kCounterNumBlockedWaitForMergeJoinRightSideDrivers{
    "presto_cpp.num_blocked_wait_for_merge_join_right_side_drivers"};
constexpr std::string_view kCounterNumBlockedWaitForMemoryDrivers{
    "presto_cpp.num_blocked_wait_for_memory_drivers"};
constexpr std::string_view kCounterNumBlockedWaitForConnectorDrivers{
    "presto_cpp.num_blocked_wait_for_connector_drivers"};
constexpr std::string_view kCounterNumBlockedYieldDrivers{
    "presto_cpp.num_blocked_yield_drivers"};
constexpr std::string_view kCounterNumStuckDrivers{
    "presto_cpp.num_stuck_drivers"};

/// Export 100 if worker is overloaded in terms of memory, 0 otherwise.
constexpr std::string_view kCounterOverloadedMem{"presto_cpp.overloaded_mem"};
/// Export 100 if worker is overloaded in terms of CPU, 0 otherwise.
constexpr std::string_view kCounterOverloadedCpu{"presto_cpp.overloaded_cpu"};
/// Export 100 if worker is overloaded in terms of memory or CPU, 0 otherwise.
constexpr std::string_view kCounterOverloaded{"presto_cpp.overloaded"};
/// Worker exports the average time tasks spend in the queue (considered
/// planned) in milliseconds.
constexpr std::string_view kCounterTaskPlannedTimeMs{
    "presto_cpp.task_planned_time_ms"};
/// Exports the current overloaded duration in seconds or 0 if not currently
/// overloaded.
constexpr std::string_view kCounterOverloadedDurationSec{
    "presto_cpp.overloaded_duration_sec"};

/// Number of total OutputBuffer managed by all
/// OutputBufferManager
constexpr std::string_view kCounterTotalPartitionedOutputBuffer{
    "presto_cpp.num_partitioned_output_buffer"};
/// Latency in millisecond of the get data call of a
/// OutputBufferManager.
constexpr std::string_view kCounterPartitionedOutputBufferGetDataLatencyMs{
    "presto_cpp.partitioned_output_buffer_get_data_latency_ms"};

/// ================== OS Counters =================

/// User CPU time of the presto_server process in microsecond since the process
/// start.
constexpr std::string_view kCounterOsUserCpuTimeMicros{
    "presto_cpp.os_user_cpu_time_micros"};
/// System CPU time of the presto_server process in microsecond since the
/// process start.
constexpr std::string_view kCounterOsSystemCpuTimeMicros{
    "presto_cpp.os_system_cpu_time_micros"};
/// Total number of soft page faults of the presto_server process in microsecond
/// since the process start.
constexpr std::string_view kCounterOsNumSoftPageFaults{
    "presto_cpp.os_num_soft_page_faults"};
/// Total number of hard page faults of the presto_server process in microsecond
/// since the process start.
constexpr std::string_view kCounterOsNumHardPageFaults{
    "presto_cpp.os_num_hard_page_faults"};
/// Total number of voluntary context switches in the presto_server process.
constexpr std::string_view kCounterOsNumVoluntaryContextSwitches{
    "presto_cpp.os_num_voluntary_context_switches"};
/// Total number of involuntary context switches in the presto_server process.
constexpr std::string_view kCounterOsNumForcedContextSwitches{
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

/// ================== Thread Pool Counters ====================

constexpr std::string_view kCounterThreadPoolNumThreadsFormat{
    "presto_cpp.{}.num_threads"};
constexpr std::string_view kCounterThreadPoolNumActiveThreadsFormat{
    "presto_cpp.{}.num_active_threads"};
constexpr std::string_view kCounterThreadPoolNumPendingTasksFormat{
    "presto_cpp.{}.num_pending_tasks"};
constexpr std::string_view kCounterThreadPoolNumTotalTasksFormat{
    "presto_cpp.{}.num_total_tasks"};
constexpr std::string_view kCounterThreadPoolMaxIdleTimeNsFormat{
    "presto_cpp.{}.max_idle_time_ns"};

/// ================== EVB Counters ====================
constexpr std::string_view kCounterExchangeIoEvbViolation{
    "presto_cpp.exchange_io_evb_violation_count"};
constexpr std::string_view kCounterHttpServerIoEvbViolation{
    "presto_cpp.http_server_io_evb_violation_count"};

/// ================== Memory Pushback Counters =================

/// Number of times memory pushback mechanism is triggered.
constexpr std::string_view kCounterMemoryPushbackCount{
    "presto_cpp.memory_pushback_count"};
/// Latency distribution of each memory pushback run in range of [0, 100s] and
/// reports P50, P90, P99, and P100.
constexpr std::string_view kCounterMemoryPushbackLatencyMs{
    "presto_cpp.memory_pushback_latency_ms"};
/// Distribution of actual reduction in memory usage achieved by each memory
/// pushback attempt. This is to gauge its effectiveness. In range of [0, 15GB]
/// with 150 buckets and reports P50, P90, P99, and P100.
constexpr std::string_view kCounterMemoryPushbackReductionBytes{
    "presto_cpp.memory_pushback_reduction_bytes"};
/// Distribution of expected reduction in memory usage achieved by each memory
/// pushback attempt. This is to gauge its effectiveness. In range of [0, 15GB]
/// with 150 buckets and reports P50, P90, P99, and P100. The expected reduction
/// can be different as other threads might have allocated memory in the
/// meantime.
constexpr std::string_view kCounterMemoryPushbackExpectedReductionBytes{
    "presto_cpp.memory_pushback_expected_reduction_bytes"};
} // namespace facebook::presto
