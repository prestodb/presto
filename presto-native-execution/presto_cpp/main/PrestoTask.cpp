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
#include "presto_cpp/main/common/Exception.h"
#include "presto_cpp/main/common/Utils.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/Operator.h"

using namespace facebook::velox;

namespace facebook::presto {

namespace {

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

void setTiming(
    const CpuWallTiming& timing,
    int64_t& count,
    protocol::Duration& wall,
    protocol::Duration& cpu) {
  count = timing.count;
  wall = protocol::Duration(timing.wallNanos, protocol::TimeUnit::NANOSECONDS);
  cpu = protocol::Duration(timing.cpuNanos, protocol::TimeUnit::NANOSECONDS);
}

} // namespace

PrestoTask::PrestoTask(const std::string& taskId) : id(taskId) {
  info.taskId = taskId;
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
  if (!taskStarted) {
    info.taskStatus.state = protocol::TaskState::RUNNING;
    return info.taskStatus;
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

  // Presto has a Driver per split. when splits represent partitions
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

  auto tracker = task->pool()->getMemoryUsageTracker();
  info.taskStatus.memoryReservationInBytes = tracker->getCurrentUserBytes();
  info.taskStatus.systemMemoryReservationInBytes =
      tracker->getCurrentSystemBytes();
  info.taskStatus.peakNodeTotalMemoryReservationInBytes =
      task->queryCtx()->pool()->getMemoryUsageTracker()->getPeakTotalBytes();

  if (task->error() && info.taskStatus.failures.empty()) {
    info.taskStatus.failures.emplace_back(toPrestoError(task->error()));
  }
  return info.taskStatus;
}

protocol::TaskInfo PrestoTask::updateInfoLocked() {
  updateStatusLocked();

  // Return limited info if there is no exec task.
  if (!task) {
    return info;
  }

  const velox::exec::TaskStats taskStats = task->taskStats();
  protocol::TaskStats& prestoTaskStats = info.stats;
  prestoTaskStats.totalScheduledTimeInNanos = {};
  prestoTaskStats.totalCpuTimeInNanos = {};
  prestoTaskStats.totalBlockedTimeInNanos = {};

  prestoTaskStats.createTime =
      util::toISOTimestamp(taskStats.executionStartTimeMs);
  prestoTaskStats.firstStartTime =
      util::toISOTimestamp(taskStats.firstSplitStartTimeMs);
  prestoTaskStats.lastStartTime =
      util::toISOTimestamp(taskStats.lastSplitStartTimeMs);
  prestoTaskStats.lastEndTime =
      util::toISOTimestamp(taskStats.executionEndTimeMs);
  prestoTaskStats.endTime = util::toISOTimestamp(taskStats.executionEndTimeMs);
  if (taskStats.executionEndTimeMs > taskStats.executionStartTimeMs) {
    prestoTaskStats.elapsedTimeInNanos =
        (taskStats.executionEndTimeMs - taskStats.executionStartTimeMs) *
        1'000'000;
  }

  auto tracker = task->pool()->getMemoryUsageTracker();
  prestoTaskStats.userMemoryReservationInBytes = tracker->getCurrentUserBytes();
  prestoTaskStats.systemMemoryReservationInBytes =
      tracker->getCurrentSystemBytes();
  prestoTaskStats.peakUserMemoryInBytes = tracker->getPeakUserBytes();
  prestoTaskStats.peakTotalMemoryInBytes = tracker->getPeakTotalBytes();

  // TODO(venkatra): Populate these memory stats as well.
  prestoTaskStats.revocableMemoryReservationInBytes = {};
  prestoTaskStats.cumulativeUserMemory = {};

  prestoTaskStats.peakNodeTotalMemoryInBytes =
      task->queryCtx()->pool()->getMemoryUsageTracker()->getPeakTotalBytes();

  prestoTaskStats.rawInputPositions = 0;
  prestoTaskStats.rawInputDataSizeInBytes = 0;
  prestoTaskStats.processedInputPositions = 0;
  prestoTaskStats.processedInputDataSizeInBytes = 0;
  prestoTaskStats.outputPositions = 0;
  prestoTaskStats.outputDataSizeInBytes = 0;

  prestoTaskStats.totalDrivers = taskStats.numTotalSplits;
  prestoTaskStats.queuedDrivers = taskStats.numQueuedSplits;
  prestoTaskStats.runningDrivers = taskStats.numRunningSplits;
  prestoTaskStats.completedDrivers = taskStats.numFinishedSplits;

  prestoTaskStats.pipelines.resize(taskStats.pipelineStats.size());

  std::unordered_map<std::string, RuntimeMetric> taskRuntimeStats;

  if (taskStats.endTimeMs >= taskStats.executionEndTimeMs) {
    taskRuntimeStats["outputConsumedDelayInNanos"].addValue(
        (taskStats.endTimeMs - taskStats.executionEndTimeMs) * 1'000'000);
    taskRuntimeStats["createTime"].addValue(taskStats.executionStartTimeMs);
    taskRuntimeStats["endTime"].addValue(taskStats.endTimeMs);
  }

  for (int i = 0; i < taskStats.pipelineStats.size(); ++i) {
    auto& pipelineOut = info.stats.pipelines[i];
    auto& pipeline = taskStats.pipelineStats[i];
    pipelineOut.inputPipeline = pipeline.inputPipeline;
    pipelineOut.outputPipeline = pipeline.outputPipeline;
    pipelineOut.firstStartTime = prestoTaskStats.createTime;
    pipelineOut.lastStartTime = prestoTaskStats.endTime;
    pipelineOut.lastEndTime = prestoTaskStats.endTime;

    pipelineOut.operatorSummaries.resize(pipeline.operatorStats.size());
    pipelineOut.totalScheduledTimeInNanos = {};
    pipelineOut.totalCpuTimeInNanos = {};
    pipelineOut.totalBlockedTimeInNanos = {};
    pipelineOut.userMemoryReservationInBytes = {};
    pipelineOut.revocableMemoryReservationInBytes = {};
    pipelineOut.systemMemoryReservationInBytes = {};

    // tasks may fail before any operators are created;
    // collect stats only when we have operators
    if (!pipeline.operatorStats.empty()) {
      const auto& firstOperatorStats = pipeline.operatorStats[0];
      const auto& lastOperatorStats = pipeline.operatorStats.back();

      pipelineOut.pipelineId = firstOperatorStats.pipelineId;
      pipelineOut.totalDrivers = firstOperatorStats.numDrivers;
      pipelineOut.rawInputPositions = firstOperatorStats.rawInputPositions;
      pipelineOut.rawInputDataSizeInBytes = firstOperatorStats.rawInputBytes;
      pipelineOut.processedInputPositions = firstOperatorStats.inputPositions;
      pipelineOut.processedInputDataSizeInBytes = firstOperatorStats.inputBytes;
      pipelineOut.outputPositions = lastOperatorStats.outputPositions;
      pipelineOut.outputDataSizeInBytes = lastOperatorStats.outputBytes;
    }

    if (pipelineOut.inputPipeline) {
      prestoTaskStats.rawInputPositions += pipelineOut.rawInputPositions;
      prestoTaskStats.rawInputDataSizeInBytes +=
          pipelineOut.rawInputDataSizeInBytes;
      prestoTaskStats.processedInputPositions +=
          pipelineOut.processedInputPositions;
      prestoTaskStats.processedInputDataSizeInBytes +=
          pipelineOut.processedInputDataSizeInBytes;
    }
    if (pipelineOut.outputPipeline) {
      prestoTaskStats.outputPositions += pipelineOut.outputPositions;
      prestoTaskStats.outputDataSizeInBytes +=
          pipelineOut.outputDataSizeInBytes;
    }

    for (auto j = 0; j < pipeline.operatorStats.size(); ++j) {
      auto& opOut = pipelineOut.operatorSummaries[j];
      auto& op = pipeline.operatorStats[j];

      opOut.stageId = id.stageId();
      opOut.stageExecutionId = id.stageExecutionId();
      opOut.pipelineId = i;
      opOut.planNodeId = op.planNodeId;
      opOut.operatorId = op.operatorId;
      opOut.operatorType = op.operatorType;

      opOut.totalDrivers = op.numDrivers;
      opOut.inputPositions = op.inputPositions;
      opOut.sumSquaredInputPositions =
          ((double)op.inputPositions) * op.inputPositions;
      opOut.inputDataSize =
          protocol::DataSize(op.inputBytes, protocol::DataUnit::BYTE);
      opOut.rawInputPositions = op.rawInputPositions;
      opOut.rawInputDataSize =
          protocol::DataSize(op.rawInputBytes, protocol::DataUnit::BYTE);

      // Report raw input statistics on the Project node following TableScan, if
      // exists.
      if (j == 1 && op.operatorType == "FilterProject" &&
          pipeline.operatorStats[0].operatorType == "TableScan") {
        const auto& scanOp = pipeline.operatorStats[0];
        opOut.rawInputPositions = scanOp.rawInputPositions;
        opOut.rawInputDataSize =
            protocol::DataSize(scanOp.rawInputBytes, protocol::DataUnit::BYTE);
      }

      opOut.outputPositions = op.outputPositions;
      opOut.outputDataSize =
          protocol::DataSize(op.outputBytes, protocol::DataUnit::BYTE);

      setTiming(
          op.addInputTiming,
          opOut.addInputCalls,
          opOut.addInputWall,
          opOut.addInputCpu);
      setTiming(
          op.getOutputTiming,
          opOut.getOutputCalls,
          opOut.getOutputWall,
          opOut.getOutputCpu);
      setTiming(
          op.finishTiming,
          opOut.finishCalls,
          opOut.finishWall,
          opOut.finishCpu);

      opOut.blockedWall = protocol::Duration(
          op.blockedWallNanos, protocol::TimeUnit::NANOSECONDS);

      opOut.userMemoryReservation = protocol::DataSize(
          op.memoryStats.userMemoryReservation, protocol::DataUnit::BYTE);
      opOut.revocableMemoryReservation = protocol::DataSize(
          op.memoryStats.revocableMemoryReservation, protocol::DataUnit::BYTE);
      opOut.systemMemoryReservation = protocol::DataSize(
          op.memoryStats.systemMemoryReservation, protocol::DataUnit::BYTE);
      opOut.peakUserMemoryReservation = protocol::DataSize(
          op.memoryStats.peakUserMemoryReservation, protocol::DataUnit::BYTE);
      opOut.peakSystemMemoryReservation = protocol::DataSize(
          op.memoryStats.peakSystemMemoryReservation, protocol::DataUnit::BYTE);
      opOut.peakTotalMemoryReservation = protocol::DataSize(
          op.memoryStats.peakTotalMemoryReservation, protocol::DataUnit::BYTE);

      opOut.spilledDataSize =
          protocol::DataSize(op.spilledBytes, protocol::DataUnit::BYTE);

      for (const auto& stat : op.runtimeStats) {
        auto statName =
            fmt::format("{}.{}.{}", op.operatorType, op.planNodeId, stat.first);
        opOut.runtimeStats[statName] = toRuntimeMetric(statName, stat.second);
        if (taskRuntimeStats.count(statName)) {
          taskRuntimeStats[statName].merge(stat.second);
        } else {
          taskRuntimeStats[statName] = stat.second;
        }
      }
      if (op.numSplits != 0) {
        const auto statName = fmt::format(
            "{}.{}.{}", op.operatorType, op.planNodeId, "numSplits");
        opOut.runtimeStats.emplace(
            statName,
            protocol::RuntimeMetric{
                statName,
                protocol::RuntimeUnit::NONE,
                op.numSplits,
                1,
                op.numSplits,
                op.numSplits});
      }

      auto wallNanos = op.addInputTiming.wallNanos +
          op.getOutputTiming.wallNanos + op.finishTiming.wallNanos;
      auto cpuNanos = op.addInputTiming.cpuNanos + op.getOutputTiming.cpuNanos +
          op.finishTiming.cpuNanos;

      pipelineOut.totalScheduledTimeInNanos += wallNanos;
      pipelineOut.totalCpuTimeInNanos += cpuNanos;
      pipelineOut.totalBlockedTimeInNanos += op.blockedWallNanos;
      pipelineOut.userMemoryReservationInBytes +=
          op.memoryStats.userMemoryReservation;
      pipelineOut.revocableMemoryReservationInBytes +=
          op.memoryStats.revocableMemoryReservation;
      pipelineOut.systemMemoryReservationInBytes +=
          op.memoryStats.systemMemoryReservation;

      prestoTaskStats.totalScheduledTimeInNanos += wallNanos;
      prestoTaskStats.totalCpuTimeInNanos += cpuNanos;
      prestoTaskStats.totalBlockedTimeInNanos += op.blockedWallNanos;
    } // pipeline's operators loop
  } // task's pipelines loop

  for (const auto& stat : taskRuntimeStats) {
    prestoTaskStats.runtimeStats[stat.first] =
        toRuntimeMetric(stat.first, stat.second);
  }

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

} // namespace facebook::presto
