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

#include "PrestoTask.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Exception.h"
#include "presto_cpp/main/common/Utils.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/Operator.h"

using namespace facebook::velox;

namespace facebook::presto {

namespace {

#define TASK_STATS_SUM(taskStats, taskStatusSum, statsName)      \
  do {                                                           \
    for (int i = 0; i < taskStats.pipelineStats.size(); ++i) {   \
      auto& pipeline = taskStats.pipelineStats[i];               \
      for (auto j = 0; j < pipeline.operatorStats.size(); ++j) { \
        auto& op = pipeline.operatorStats[j];                    \
        (taskStatusSum) += op.statsName;                         \
      }                                                          \
    }                                                            \
  } while (0)

protocol::TaskState toPrestoTaskState(exec::TaskState state) {
  switch (state) {
    case exec::kRunning:
      return protocol::TaskState::RUNNING;
    case exec::kFinished:
      return protocol::TaskState::FINISHED;
    case exec::kCanceled:
      return protocol::TaskState::CANCELED;
    case exec::kFailed:
      return protocol::TaskState::FAILED;
    case exec::kAborted:
    default:
      return protocol::TaskState::ABORTED;
  }
}

protocol::ExecutionFailureInfo toPrestoError(std::exception_ptr ex) {
  try {
    rethrow_exception(ex);
  } catch (const VeloxException& e) {
    return VeloxToPrestoExceptionTranslator::translate(e);
  } catch (const std::exception& e) {
    return VeloxToPrestoExceptionTranslator::translate(e);
  }
}

protocol::RuntimeUnit toPrestoRuntimeUnit(RuntimeCounter::Unit unit) {
  switch (unit) {
    case RuntimeCounter::Unit::kNanos:
      return protocol::RuntimeUnit::NANO;
    case RuntimeCounter::Unit::kBytes:
      return protocol::RuntimeUnit::BYTE;
    case RuntimeCounter::Unit::kNone:
      return protocol::RuntimeUnit::NONE;
    default:
      return protocol::RuntimeUnit::NONE;
  }
}

// Presto has certain query stats logic depending on the operator names.
// To leverage this logic we need to supply Presto's operator names.
std::string toPrestoOperatorType(const std::string& operatorType) {
  if (operatorType == "MergeExchange") {
    return "MergeOperator";
  }
  if (operatorType == "Exchange") {
    return "ExchangeOperator";
  }
  if (operatorType == "TableScan") {
    return "TableScanOperator";
  }
  return operatorType;
}

void setTiming(
    const CpuWallTiming& timing,
    int64_t& count,
    protocol::Duration& wall,
    protocol::Duration& cpu) {
  count = timing.count;
  wall = protocol::Duration(timing.wallNanos, protocol::TimeUnit::NANOSECONDS);
  cpu = protocol::Duration(timing.cpuNanos, protocol::TimeUnit::NANOSECONDS);
}

// Creates a protocol runtime metric object from a raw value.
static protocol::RuntimeMetric createProtocolRuntimeMetric(
    const std::string& name,
    int64_t value,
    protocol::RuntimeUnit unit = protocol::RuntimeUnit::NONE) {
  return protocol::RuntimeMetric{name, unit, value, 1, value, value};
}

// Creates a Velox runtime metric object from a raw value.
static RuntimeMetric createVeloxRuntimeMetric(
    int64_t value,
    RuntimeCounter::Unit unit) {
  return RuntimeMetric{value, unit};
}

// Updates a Velox runtime metric in the unordered map.
static void addRuntimeMetric(
    std::unordered_map<std::string, RuntimeMetric>& runtimeMetrics,
    const std::string& name,
    const RuntimeMetric& metric) {
  auto it = runtimeMetrics.find(name);
  if (it != runtimeMetrics.end()) {
    it->second.merge(metric);
  } else {
    runtimeMetrics.emplace(name, metric);
  }
}

// Updates a Velox runtime metric in the unordered map if the value is not 0.
static void addRuntimeMetricIfNotZero(
    std::unordered_map<std::string, RuntimeMetric>& runtimeMetrics,
    const std::string& name,
    uint64_t value) {
  if (value > 0) {
    auto veloxMetric =
        createVeloxRuntimeMetric(value, RuntimeCounter::Unit::kNone);
    addRuntimeMetric(runtimeMetrics, name, veloxMetric);
  }
}

// Add 'spilling' metrics from Velox operator stats to Presto operator stats.
static void addSpillingOperatorMetrics(
    protocol::OperatorStats& opOut,
    protocol::TaskStats& prestoTaskStats,
    const exec::OperatorStats& op) {
  std::string statName =
      fmt::format("{}.{}.spilledBytes", op.operatorType, op.planNodeId);
  auto prestoMetric = createProtocolRuntimeMetric(
      statName, op.spilledBytes, protocol::RuntimeUnit::BYTE);
  opOut.runtimeStats.emplace(statName, prestoMetric);
  prestoTaskStats.runtimeStats[statName] = prestoMetric;

  statName = fmt::format("{}.{}.spilledRows", op.operatorType, op.planNodeId);
  prestoMetric = createProtocolRuntimeMetric(statName, op.spilledRows);
  opOut.runtimeStats.emplace(statName, prestoMetric);
  prestoTaskStats.runtimeStats[statName] = prestoMetric;

  statName =
      fmt::format("{}.{}.spilledPartitions", op.operatorType, op.planNodeId);
  prestoMetric = createProtocolRuntimeMetric(statName, op.spilledPartitions);
  opOut.runtimeStats.emplace(statName, prestoMetric);
  prestoTaskStats.runtimeStats[statName] = prestoMetric;

  statName = fmt::format("{}.{}.spilledFiles", op.operatorType, op.planNodeId);
  prestoMetric = createProtocolRuntimeMetric(statName, op.spilledFiles);
  opOut.runtimeStats.emplace(statName, prestoMetric);
  prestoTaskStats.runtimeStats[statName] = prestoMetric;
}

// Process the runtime stats of individual protocol::OperatorStats. Do not add
// runtime stats to protocol unless it is for one of the final task states.
static void processOperatorStats(
    protocol::TaskStats& prestoTaskStats,
    protocol::TaskState state) {
  if (SystemConfig::instance()->skipRuntimeStatsInRunningTaskInfo() &&
      !isFinalState(state)) {
    for (auto& pipelineOut : prestoTaskStats.pipelines) {
      for (auto& opOut : pipelineOut.operatorSummaries) {
        opOut.runtimeStats.clear();
      }
    }
  } else {
    const std::vector<std::string> prefixToExclude{"running", "blocked"};
    for (auto& pipelineOut : prestoTaskStats.pipelines) {
      for (auto& opOut : pipelineOut.operatorSummaries) {
        for (const auto& prefix : prefixToExclude) {
          for (auto it = opOut.runtimeStats.begin();
               it != opOut.runtimeStats.end();) {
            if (it->first.find(prefix) != std::string::npos) {
              it = opOut.runtimeStats.erase(it);
            } else {
              ++it;
            }
          }
        }
      }
    }
  }
}

// Process the runtime stats of protocol::TaskStats. Copy taskRuntimeMetrics to
// protocol::TaskStats if it is one of the final task states. Otherwise set the
// runtime stats to empty.
static void processTaskStats(
    protocol::TaskStats& prestoTaskStats,
    const std::unordered_map<std::string, RuntimeMetric>& taskRuntimeMetrics,
    protocol::TaskState state) {
  if (!SystemConfig::instance()->skipRuntimeStatsInRunningTaskInfo() ||
      isFinalState(state)) {
    for (const auto& stat : taskRuntimeMetrics) {
      prestoTaskStats.runtimeStats[stat.first] =
          toRuntimeMetric(stat.first, stat.second);
    }
  } else {
    prestoTaskStats.runtimeStats.clear();
  }
}

} // namespace

PrestoTask::PrestoTask(const std::string& taskId, const std::string& nodeId)
    : id(taskId) {
  info.taskId = taskId;
  info.nodeId = nodeId;
}

void PrestoTask::updateHeartbeatLocked() {
  lastHeartbeatMs = velox::getCurrentTimeMs();
  info.lastHeartbeat = util::toISOTimestamp(lastHeartbeatMs);
}

uint64_t PrestoTask::timeSinceLastHeartbeatMs() const {
  std::lock_guard<std::mutex> l(mutex);
  if (lastHeartbeatMs == 0UL) {
    return 0UL;
  }
  return getCurrentTimeMs() - lastHeartbeatMs;
}

protocol::TaskStatus PrestoTask::updateStatusLocked() {
  if (!taskStarted and error == nullptr) {
    protocol::TaskStatus ret = info.taskStatus;
    if (ret.state != protocol::TaskState::ABORTED) {
      ret.state = protocol::TaskState::PLANNED;
    }
    return ret;
  }

  // Error occurs when creating task or even before task is created. Set error
  // and return immediately
  if (error != nullptr) {
    if (info.taskStatus.failures.empty()) {
      info.taskStatus.failures.emplace_back(toPrestoError(error));
    }
    info.taskStatus.state = protocol::TaskState::FAILED;
    return info.taskStatus;
  }
  VELOX_CHECK_NOT_NULL(task, "task is null when updating status")
  const auto taskStats = task->taskStats();

  // Presto has a Driver per split. When splits represent partitions
  // of data, there is a queue of them per Task. We represent
  // processed/queued splits as Drivers for Presto.
  info.taskStatus.queuedPartitionedDrivers = taskStats.numQueuedSplits;
  info.taskStatus.runningPartitionedDrivers = taskStats.numRunningSplits;

  // TODO(spershin): Note, we dont' clean the stats.completedSplitGroups
  // and it seems not required now, but we might want to do it one day.
  for (auto splitGroupId : taskStats.completedSplitGroups) {
    info.taskStatus.completedDriverGroups.push_back({true, splitGroupId});
  }
  info.taskStatus.state = toPrestoTaskState(task->state());

  const auto stats = task->pool()->stats();
  info.taskStatus.memoryReservationInBytes = stats.currentBytes;
  info.taskStatus.systemMemoryReservationInBytes = 0;
  info.taskStatus.peakNodeTotalMemoryReservationInBytes = stats.peakBytes;

  TASK_STATS_SUM(
      taskStats,
      info.taskStatus.physicalWrittenDataSizeInBytes,
      physicalWrittenBytes);

  info.taskStatus.outputBufferUtilization = taskStats.outputBufferUtilization;
  info.taskStatus.outputBufferOverutilized = taskStats.outputBufferOverutilized;

  if (task->error() && info.taskStatus.failures.empty()) {
    info.taskStatus.failures.emplace_back(toPrestoError(task->error()));
  }
  return info.taskStatus;
}

protocol::TaskInfo PrestoTask::updateInfoLocked() {
  protocol::TaskStatus taskStatus = updateStatusLocked();

  // Return limited info if there is no exec task.
  if (!task) {
    return info;
  }

  const velox::exec::TaskStats taskStats = task->taskStats();
  protocol::TaskStats& taskOut = info.stats;

  // Task time related stats.
  taskOut.totalScheduledTimeInNanos = {};
  taskOut.totalCpuTimeInNanos = {};
  taskOut.totalBlockedTimeInNanos = {};

  taskOut.createTime = util::toISOTimestamp(taskStats.executionStartTimeMs);
  taskOut.firstStartTime =
      util::toISOTimestamp(taskStats.firstSplitStartTimeMs);
  taskOut.lastStartTime = util::toISOTimestamp(taskStats.lastSplitStartTimeMs);
  taskOut.lastEndTime = util::toISOTimestamp(taskStats.executionEndTimeMs);
  taskOut.endTime = util::toISOTimestamp(taskStats.executionEndTimeMs);
  if (taskStats.executionEndTimeMs > taskStats.executionStartTimeMs) {
    taskOut.elapsedTimeInNanos =
        (taskStats.executionEndTimeMs - taskStats.executionStartTimeMs) *
        1'000'000;
  }

  // Task memory related stats.
  const auto memoryStats = task->pool()->stats();
  taskOut.userMemoryReservationInBytes = memoryStats.currentBytes;
  taskOut.systemMemoryReservationInBytes = 0;
  taskOut.peakUserMemoryInBytes = memoryStats.peakBytes;
  taskOut.peakTotalMemoryInBytes = memoryStats.peakBytes;
  taskOut.peakNodeTotalMemoryInBytes = task->queryCtx()->pool()->peakBytes();
  // TODO(venkatra): Populate these memory memoryStats as well.
  taskOut.revocableMemoryReservationInBytes = {};

  // Set the lastTaskStatsUpdateMs to execution start time if it is 0.
  if (lastTaskStatsUpdateMs == 0) {
    lastTaskStatsUpdateMs = taskStats.executionStartTimeMs;
  }
  const uint64_t currentTaskStatsUpdateMs = velox::getCurrentTimeMs();
  int64_t averageMemoryForLastPeriod =
      (memoryStats.currentBytes + lastMemoryReservation) / 2;
  taskOut.cumulativeUserMemory += averageMemoryForLastPeriod *
      (currentTaskStatsUpdateMs - lastTaskStatsUpdateMs) / 1000;
  taskOut.cumulativeTotalMemory = taskOut.cumulativeUserMemory;
  lastTaskStatsUpdateMs = currentTaskStatsUpdateMs;
  lastMemoryReservation = memoryStats.currentBytes;

  // Task data related stats.
  taskOut.rawInputPositions = 0;
  taskOut.rawInputDataSizeInBytes = 0;
  taskOut.processedInputPositions = 0;
  taskOut.processedInputDataSizeInBytes = 0;
  taskOut.outputPositions = 0;
  taskOut.outputDataSizeInBytes = 0;

  // Task driver related stats.
  taskOut.totalDrivers = taskStats.numTotalSplits;
  taskOut.queuedDrivers = taskStats.numQueuedSplits;
  taskOut.runningDrivers = taskStats.numRunningSplits;
  taskOut.completedDrivers = taskStats.numFinishedSplits;

  // Task runtime stats.
  taskOut.runtimeStats.clear();
  std::unordered_map<std::string, RuntimeMetric> taskRuntimeStats;
  if (taskStats.endTimeMs >= taskStats.executionEndTimeMs) {
    taskRuntimeStats["outputConsumedDelayInNanos"].addValue(
        (taskStats.endTimeMs - taskStats.executionEndTimeMs) * 1'000'000);
    taskRuntimeStats["createTime"].addValue(taskStats.executionStartTimeMs);
    taskRuntimeStats["endTime"].addValue(taskStats.endTimeMs);
  }

  // Task runtime metrics for driver counters.
  if (!isFinalState(taskStatus.state)) {
    addRuntimeMetricIfNotZero(
        taskRuntimeStats, "drivers.total", taskStats.numTotalDrivers);
    addRuntimeMetricIfNotZero(
        taskRuntimeStats, "drivers.running", taskStats.numRunningDrivers);
    addRuntimeMetricIfNotZero(
        taskRuntimeStats, "drivers.completed", taskStats.numCompletedDrivers);
    addRuntimeMetricIfNotZero(
        taskRuntimeStats, "drivers.terminated", taskStats.numTerminatedDrivers);
    for (const auto it : taskStats.numBlockedDrivers) {
      addRuntimeMetricIfNotZero(
          taskRuntimeStats,
          fmt::format("drivers.{}", exec::blockingReasonToString(it.first)),
          it.second);
    }
  }

  taskOut.pipelines.resize(taskStats.pipelineStats.size());

  for (int i = 0; i < taskStats.pipelineStats.size(); ++i) {
    auto& pipelineOut = info.stats.pipelines[i];
    auto& pipelineStats = taskStats.pipelineStats[i];

    pipelineOut.inputPipeline = pipelineStats.inputPipeline;
    pipelineOut.outputPipeline = pipelineStats.outputPipeline;

    // Pipeline time related stats.
    pipelineOut.firstStartTime = taskOut.createTime;
    pipelineOut.lastStartTime = taskOut.endTime;
    pipelineOut.lastEndTime = taskOut.endTime;

    pipelineOut.totalScheduledTimeInNanos = {};
    pipelineOut.totalCpuTimeInNanos = {};
    pipelineOut.totalBlockedTimeInNanos = {};

    // Pipeline memory related stats.
    pipelineOut.userMemoryReservationInBytes = {};
    pipelineOut.revocableMemoryReservationInBytes = {};
    pipelineOut.systemMemoryReservationInBytes = {};

    // Pipeline data related stats.
    // tasks may fail before any operators are created;
    // collect memoryStats only when we have operators.
    if (!pipelineStats.operatorStats.empty()) {
      const auto& firstOperatorStats = pipelineStats.operatorStats[0];
      const auto& lastOperatorStats = pipelineStats.operatorStats.back();

      pipelineOut.pipelineId = firstOperatorStats.pipelineId;
      pipelineOut.totalDrivers = firstOperatorStats.numDrivers;
      pipelineOut.rawInputPositions = firstOperatorStats.rawInputPositions;
      pipelineOut.rawInputDataSizeInBytes = firstOperatorStats.rawInputBytes;
      pipelineOut.processedInputPositions = firstOperatorStats.inputPositions;
      pipelineOut.processedInputDataSizeInBytes = firstOperatorStats.inputBytes;
      pipelineOut.outputPositions = lastOperatorStats.outputPositions;
      pipelineOut.outputDataSizeInBytes = lastOperatorStats.outputBytes;
    }

    // Task cumulative stats.
    if (pipelineOut.inputPipeline) {
      taskOut.rawInputPositions += pipelineOut.rawInputPositions;
      taskOut.rawInputDataSizeInBytes += pipelineOut.rawInputDataSizeInBytes;
      taskOut.processedInputPositions += pipelineOut.processedInputPositions;
      taskOut.processedInputDataSizeInBytes +=
          pipelineOut.processedInputDataSizeInBytes;
    }
    if (pipelineOut.outputPipeline) {
      taskOut.outputPositions += pipelineOut.outputPositions;
      taskOut.outputDataSizeInBytes += pipelineOut.outputDataSizeInBytes;
    }

    pipelineOut.operatorSummaries.resize(pipelineStats.operatorStats.size());

    for (auto j = 0; j < pipelineStats.operatorStats.size(); ++j) {
      auto& operatorOut = pipelineOut.operatorSummaries[j];
      auto& operatorStats = pipelineStats.operatorStats[j];

      operatorOut.stageId = id.stageId();
      operatorOut.stageExecutionId = id.stageExecutionId();
      operatorOut.pipelineId = i;
      operatorOut.planNodeId = operatorStats.planNodeId;
      operatorOut.operatorId = operatorStats.operatorId;
      operatorOut.operatorType =
          toPrestoOperatorType(operatorStats.operatorType);

      // Operator time related stats.
      setTiming(
          operatorStats.addInputTiming,
          operatorOut.addInputCalls,
          operatorOut.addInputWall,
          operatorOut.addInputCpu);
      setTiming(
          operatorStats.getOutputTiming,
          operatorOut.getOutputCalls,
          operatorOut.getOutputWall,
          operatorOut.getOutputCpu);
      setTiming(
          operatorStats.finishTiming,
          operatorOut.finishCalls,
          operatorOut.finishWall,
          operatorOut.finishCpu);

      operatorOut.blockedWall = protocol::Duration(
          operatorStats.blockedWallNanos, protocol::TimeUnit::NANOSECONDS);

      // Operator memory related stats.
      operatorOut.userMemoryReservation = protocol::DataSize(
          operatorStats.memoryStats.userMemoryReservation,
          protocol::DataUnit::BYTE);
      operatorOut.revocableMemoryReservation = protocol::DataSize(
          operatorStats.memoryStats.revocableMemoryReservation,
          protocol::DataUnit::BYTE);
      operatorOut.systemMemoryReservation = protocol::DataSize(
          operatorStats.memoryStats.systemMemoryReservation,
          protocol::DataUnit::BYTE);
      operatorOut.peakUserMemoryReservation = protocol::DataSize(
          operatorStats.memoryStats.peakUserMemoryReservation,
          protocol::DataUnit::BYTE);
      operatorOut.peakSystemMemoryReservation = protocol::DataSize(
          operatorStats.memoryStats.peakSystemMemoryReservation,
          protocol::DataUnit::BYTE);
      operatorOut.peakTotalMemoryReservation = protocol::DataSize(
          operatorStats.memoryStats.peakTotalMemoryReservation,
          protocol::DataUnit::BYTE);
      operatorOut.spilledDataSize = protocol::DataSize(
          operatorStats.spilledBytes, protocol::DataUnit::BYTE);

      // Operator data related stats.
      operatorOut.inputPositions = operatorStats.inputPositions;
      operatorOut.sumSquaredInputPositions =
          ((double)operatorStats.inputPositions) * operatorStats.inputPositions;
      operatorOut.inputDataSize = protocol::DataSize(
          operatorStats.inputBytes, protocol::DataUnit::BYTE);
      operatorOut.rawInputPositions = operatorStats.rawInputPositions;
      operatorOut.rawInputDataSize = protocol::DataSize(
          operatorStats.rawInputBytes, protocol::DataUnit::BYTE);
      operatorOut.outputPositions = operatorStats.outputPositions;
      operatorOut.outputDataSize = protocol::DataSize(
          operatorStats.outputBytes, protocol::DataUnit::BYTE);
      // Report raw input statistics on the Project node following TableScan, if
      // exists.
      if (j == 1 && operatorStats.operatorType == "FilterProject" &&
          pipelineStats.operatorStats[0].operatorType == "TableScan") {
        const auto& scanOp = pipelineStats.operatorStats[0];
        operatorOut.rawInputPositions = scanOp.rawInputPositions;
        operatorOut.rawInputDataSize =
            protocol::DataSize(scanOp.rawInputBytes, protocol::DataUnit::BYTE);
      }

      // Operator driver related stats.
      operatorOut.totalDrivers = operatorStats.numDrivers;

      // Pipeline and task cumulative stats.
      auto wallNanos = operatorStats.addInputTiming.wallNanos +
          operatorStats.getOutputTiming.wallNanos +
          operatorStats.finishTiming.wallNanos;
      auto cpuNanos = operatorStats.addInputTiming.cpuNanos +
          operatorStats.getOutputTiming.cpuNanos +
          operatorStats.finishTiming.cpuNanos;

      pipelineOut.totalScheduledTimeInNanos += wallNanos;
      pipelineOut.totalCpuTimeInNanos += cpuNanos;
      pipelineOut.totalBlockedTimeInNanos += operatorStats.blockedWallNanos;
      pipelineOut.userMemoryReservationInBytes +=
          operatorStats.memoryStats.userMemoryReservation;
      pipelineOut.revocableMemoryReservationInBytes +=
          operatorStats.memoryStats.revocableMemoryReservation;
      pipelineOut.systemMemoryReservationInBytes +=
          operatorStats.memoryStats.systemMemoryReservation;

      taskOut.totalScheduledTimeInNanos += wallNanos;
      taskOut.totalCpuTimeInNanos += cpuNanos;
      taskOut.totalBlockedTimeInNanos += operatorStats.blockedWallNanos;

      // Operator runtime stats.
      for (const auto& stat : operatorStats.runtimeStats) {
        auto statName = fmt::format(
            "{}.{}.{}",
            operatorStats.operatorType,
            operatorStats.planNodeId,
            stat.first);
        operatorOut.runtimeStats[statName] =
            toRuntimeMetric(statName, stat.second);
        if (taskRuntimeStats.count(statName)) {
          taskRuntimeStats[statName].merge(stat.second);
        } else {
          taskRuntimeStats[statName] = stat.second;
        }
      }

      if (operatorStats.numSplits != 0) {
        const auto statName = fmt::format(
            "{}.{}.numSplits",
            operatorStats.operatorType,
            operatorStats.planNodeId);
        operatorOut.runtimeStats.emplace(
            statName,
            createProtocolRuntimeMetric(statName, operatorStats.numSplits));
      }
      if (operatorStats.inputVectors != 0) {
        auto statName = fmt::format(
            "{}.{}.{}",
            operatorStats.operatorType,
            operatorStats.planNodeId,
            "inputBatches");
        operatorOut.runtimeStats.emplace(
            statName,
            createProtocolRuntimeMetric(statName, operatorStats.inputVectors));
      }
      if (operatorStats.outputVectors != 0) {
        auto statName = fmt::format(
            "{}.{}.{}",
            operatorStats.operatorType,
            operatorStats.planNodeId,
            "outputBatches");
        operatorOut.runtimeStats.emplace(
            statName,
            createProtocolRuntimeMetric(statName, operatorStats.outputVectors));
      }

      // If Velox operator has spilling memoryStats, then add them to the Presto
      // operator memoryStats and the task memoryStats as runtime memoryStats.
      if (operatorStats.spilledBytes > 0) {
        addSpillingOperatorMetrics(operatorOut, taskOut, operatorStats);
      }
    } // pipelineStats's operators loop
  } // task's pipelines loop

  processOperatorStats(taskOut, taskStatus.state);
  processTaskStats(taskOut, taskRuntimeStats, taskStatus.state);

  return info;
}

/*static*/ std::string PrestoTask::taskNumbersToString(
    const std::array<size_t, 5>& taskNumbers) {
  // Names of five TaskState (enum defined in exec/Task.h).
  static constexpr std::array<folly::StringPiece, 5> taskStateNames{
      "Running",
      "Finished",
      "Canceled",
      "Aborted",
      "Failed",
  };

  std::string str;
  for (size_t i = 0; i < taskNumbers.size(); ++i) {
    if (taskNumbers[i] != 0) {
      folly::toAppend(
          fmt::format("{}={} ", taskStateNames[i], taskNumbers[i]), &str);
    }
  }
  return str;
}

std::string PrestoTask::toJsonString() const {
  std::lock_guard<std::mutex> l(mutex);
  folly::dynamic obj = folly::dynamic::object;
  obj["task"] = task ? task->toString() : "null";
  obj["taskStarted"] = taskStarted;
  obj["lastHeartbeatMs"] = lastHeartbeatMs;
  obj["lastTaskStatsUpdateMs"] = lastTaskStatsUpdateMs;
  obj["lastMemoryReservation"] = lastMemoryReservation;

  json j;
  to_json(j, info);
  obj["taskInfo"] = to_string(j);
  return folly::toPrettyJson(obj);
}

protocol::RuntimeMetric toRuntimeMetric(
    const std::string& name,
    const RuntimeMetric& metric) {
  return protocol::RuntimeMetric{
      name,
      toPrestoRuntimeUnit(metric.unit),
      metric.sum,
      metric.count,
      metric.max,
      metric.min};
}

bool isFinalState(protocol::TaskState state) {
  switch (state) {
    case protocol::TaskState::FINISHED:
    case protocol::TaskState::FAILED:
    case protocol::TaskState::ABORTED:
    case protocol::TaskState::CANCELED:
      return true;
    default:
      return false;
  }
}

} // namespace facebook::presto
