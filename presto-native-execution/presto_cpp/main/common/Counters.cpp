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

#include "presto_cpp/main/common/Counters.h"
#include "velox/common/base/StatsReporter.h"

namespace facebook::presto {

void registerPrestoCppCounters() {
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterDriverCPUExecutorQueueSize, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterDriverCPUExecutorLatencyMs, facebook::velox::StatType::AVG);

  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterHTTPExecutorLatencyMs, facebook::velox::StatType::AVG);

  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumQueryContexts, facebook::velox::StatType::AVG);

  REPORT_ADD_STAT_EXPORT_TYPE(kCounterNumTasks, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumTasksRunning, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumTasksFinished, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumTasksCancelled, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumTasksAborted, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumTasksFailed, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumZombieTasks, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumZombiePrestoTasks, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumRunningDrivers, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterNumBlockedDrivers, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterMappedMemoryBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterAllocatedMemoryBytes, facebook::velox::StatType::AVG);
  REPORT_ADD_STAT_EXPORT_TYPE(
      kCounterTotalPartitionedOutputBuffer, facebook::velox::StatType::AVG);
}

} // namespace facebook::presto
