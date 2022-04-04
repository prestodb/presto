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
void registerPrestoCppCounters();

constexpr folly::StringPiece kCounterDriverCPUExecutorQueueSize{
    "presto_cpp.driver_cpu_executor_queue_size"};
constexpr folly::StringPiece kCounterDriverCPUExecutorLatencyMs{
    "presto_cpp.driver_cpu_executor_latency_ms"};

constexpr folly::StringPiece kCounterHTTPExecutorLatencyMs{
    "presto_cpp.http_executor_latency_ms"};

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
constexpr folly::StringPiece kCounterNumDrivers{"presto_cpp.num_drivers"};

// Number of bytes of memory MappedMemory currently maps (RSS). It also includes
// memory that was freed and currently not in use.
constexpr folly::StringPiece kCounterMappedMemoryBytes{
    "presto_cpp.mapped_memory_bytes"};
// Number of bytes of memory MappedMemory currently allocates. Memories in use
constexpr folly::StringPiece kCounterAllocatedMemoryBytes{
    "presto_cpp.allocated_memory_bytes"};

} // namespace facebook::presto
