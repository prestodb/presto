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

#include "presto_cpp/main/PrestoTask.h"
#include <sys/resource.h>
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

// Presto operator's node id sometimes is not equivalent to velox's.
// So when reporting task stats, we need to parse node id back to presto's.
// For example, velox's partitionedOutput operator would have "root." prefix.
std::string toPrestoPlanNodeId(const protocol::PlanNodeId& id) {
  if (FOLLY_LIKELY(id.find("root.") == std::string::npos)) {
    return id;
  }
  return id.substr(5);
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
  if (operatorType == "TableWrite") {
    return "TableWriterOperator";
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
    auto veloxMetric = RuntimeMetric(value, RuntimeCounter::Unit::kNone);
    addRuntimeMetric(runtimeMetrics, name, veloxMetric);
  }
}

RuntimeMetric fromMillis(int64_t ms) {
  return RuntimeMetric{ms * 1'000'000, velox::RuntimeCounter::Unit::kNanos};
}

RuntimeMetric fromNanos(int64_t nanos) {
  return RuntimeMetric{nanos, velox::RuntimeCounter::Unit::kNanos};
}

// Utility to generate presto runtime stat name when translating velox runtime
// stats over to presto.
std::string generateRuntimeStatName(
    const exec::OperatorStats& veloxOperatorStats,
    const std::string& statName) {
  return fmt::format(
      "{}.{}.{}",
      veloxOperatorStats.operatorType,
      veloxOperatorStats.planNodeId,
      statName);
}

// Helper to convert Velox-specific generic operator stats into Presto runtime
// stats.
struct OperatorStatsCollector {
  const exec::OperatorStats& veloxOperatorStats;
  protocol::RuntimeStats& prestoOperatorStats;
  protocol::RuntimeStats& prestoTaskStats;

  void addIfNotZero(
      const std::string& name,
      int64_t value,
      protocol::RuntimeUnit unit = protocol::RuntimeUnit::NONE) {
    if (value == 0) {
      return;
    }

    add(name, value, unit);
  }

  void add(
      const std::string& name,
      int64_t value,
      protocol::RuntimeUnit unit = protocol::RuntimeUnit::NONE) {
    const std::string statName =
        generateRuntimeStatName(veloxOperatorStats, name);
    auto prestoMetric = createProtocolRuntimeMetric(statName, value, unit);
    prestoOperatorStats.emplace(statName, prestoMetric);
    prestoTaskStats.emplace(statName, prestoMetric);
  }
};

// Add 'spilling' metrics from Velox operator stats to Presto operator stats.
void addSpillingOperatorMetrics(OperatorStatsCollector& collector) {
  auto& op = collector.veloxOperatorStats;

  collector.add("spilledBytes", op.spilledBytes, protocol::RuntimeUnit::BYTE);
  collector.add("spilledRows", op.spilledRows);
  collector.add("spilledPartitions", op.spilledPartitions);
  collector.add("spilledFiles", op.spilledFiles);
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

// Copy taskRuntimeMetrics to protocol::TaskStats if we are in a final task
// state, or we are told to not skip metrics if still running.
// Clear runtime stats otherwise.
void processTaskStats(
    protocol::TaskStats& prestoTaskStats,
    const std::unordered_map<std::string, RuntimeMetric>& taskRuntimeMetrics,
    protocol::TaskState state,
    bool tryToSkipIfRunning = true) {
  if (!tryToSkipIfRunning ||
      !SystemConfig::instance()->skipRuntimeStatsInRunningTaskInfo() ||
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

PrestoTask::PrestoTask(
    const std::string& taskId,
    const std::string& nodeId,
    long _startProcessCpuTime)
    : id(taskId),
      startProcessCpuTime{
          _startProcessCpuTime > 0 ? _startProcessCpuTime
                                   : getProcessCpuTime()} {
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

// static
long PrestoTask::getProcessCpuTime() {
  struct rusage rusageEnd;
  getrusage(RUSAGE_SELF, &rusageEnd);

  auto tvNanos = [](struct timeval tv) {
    return tv.tv_sec * 1000000000 + tv.tv_usec * 1000;
  };

  return tvNanos(rusageEnd.ru_utime) + tvNanos(rusageEnd.ru_stime);
}

void PrestoTask::recordProcessCpuTime() {
  if (processCpuTime_ > 0) {
    return;
  }

  processCpuTime_ = getProcessCpuTime() - startProcessCpuTime;
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
    recordProcessCpuTime();
    return info.taskStatus;
  }
  VELOX_CHECK_NOT_NULL(task, "task is null when updating status")
  const auto taskStats = task->taskStats();

  // Presto has a Driver per split. When splits represent partitions
  // of data, there is a queue of them per Task. We represent
  // running/queued table scan splits as partitioned drivers for Presto.
  info.taskStatus.queuedPartitionedDrivers = taskStats.numQueuedTableScanSplits;
  info.taskStatus.runningPartitionedDrivers =
      taskStats.numRunningTableScanSplits;
  // Return weights if they were supplied in the table scan splits. Coordinator
  // uses these for split scheduling.
  info.taskStatus.queuedPartitionedSplitsWeight =
      taskStats.queuedTableScanSplitWeights;
  info.taskStatus.runningPartitionedSplitsWeight =
      taskStats.runningTableScanSplitWeights;

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
  if (isFinalState(info.taskStatus.state)) {
    recordProcessCpuTime();
  }
  return info.taskStatus;
}

void PrestoTask::updateOutputBufferInfoLocked(
    const velox::exec::TaskStats& taskStats) {
  if (!taskStats.outputBufferStats.has_value()) {
    return;
  }
  const auto& outputBufferStats = taskStats.outputBufferStats.value();
  auto& outputBufferInfo = info.outputBuffers;
  outputBufferInfo.type =
      velox::core::PartitionedOutputNode::kindString(outputBufferStats.kind);
  outputBufferInfo.canAddBuffers = !outputBufferStats.noMoreBuffers;
  outputBufferInfo.canAddPages = !outputBufferStats.noMoreData;
  outputBufferInfo.totalBufferedBytes = outputBufferStats.bufferedBytes;
  outputBufferInfo.totalBufferedPages = outputBufferStats.bufferedPages;
  outputBufferInfo.totalPagesSent = outputBufferStats.totalPagesSent;
  outputBufferInfo.totalRowsSent = outputBufferStats.totalRowsSent;
  // TODO: populate state and destination buffer stats in info.outputBuffers.
}

protocol::TaskInfo PrestoTask::updateInfoLocked() {
  protocol::TaskStatus taskStatus = updateStatusLocked();

  // Return limited info if there is no exec task.
  if (task == nullptr) {
    return info;
  }

  const velox::exec::TaskStats taskStats = task->taskStats();
  updateOutputBufferInfoLocked(taskStats);
  protocol::TaskStats& prestoTaskStats = info.stats;
  // Clear the old runtime metrics as not all of them would be overwritten by
  // the new ones.
  prestoTaskStats.runtimeStats.clear();

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

  const uint64_t currentTimeMs = velox::getCurrentTimeMs();
  const uint64_t sinceLastPeriodMs = currentTimeMs - lastTaskStatsUpdateMs;

  if (taskStats.executionEndTimeMs > taskStats.executionStartTimeMs) {
    prestoTaskStats.elapsedTimeInNanos =
        (taskStats.executionEndTimeMs - taskStats.executionStartTimeMs) *
        1'000'000;
  } else {
    prestoTaskStats.elapsedTimeInNanos =
        (currentTimeMs - taskStats.executionStartTimeMs) * 1'000'000;
  }

  const auto stats = task->pool()->stats();
  prestoTaskStats.userMemoryReservationInBytes = stats.currentBytes;
  prestoTaskStats.systemMemoryReservationInBytes = 0;
  prestoTaskStats.peakUserMemoryInBytes = stats.peakBytes;
  prestoTaskStats.peakTotalMemoryInBytes = stats.peakBytes;

  // TODO(venkatra): Populate these memory stats as well.
  prestoTaskStats.revocableMemoryReservationInBytes = {};

  // Set the lastTaskStatsUpdateMs to execution start time if it is 0.
  if (lastTaskStatsUpdateMs == 0) {
    lastTaskStatsUpdateMs = taskStats.executionStartTimeMs;
  }

  const int64_t currentBytes = stats.currentBytes;

  int64_t averageMemoryForLastPeriod =
      (currentBytes + lastMemoryReservation) / 2;

  prestoTaskStats.cumulativeUserMemory +=
      (averageMemoryForLastPeriod * sinceLastPeriodMs) / 1000;

  prestoTaskStats.cumulativeTotalMemory = prestoTaskStats.cumulativeUserMemory;

  lastTaskStatsUpdateMs = currentTimeMs;
  lastMemoryReservation = currentBytes;

  prestoTaskStats.peakNodeTotalMemoryInBytes =
      task->queryCtx()->pool()->peakBytes();

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

  if (taskStats.outputBufferStats.has_value()) {
    const auto& outputBufferStats = taskStats.outputBufferStats.value();

    taskRuntimeStats.insert(
        {"averageOutputBufferWallNanos",
         fromMillis(outputBufferStats.averageBufferTimeMs)});
    taskRuntimeStats["numTopOutputBuffers"].addValue(
        outputBufferStats.numTopBuffers);
  }

  if (taskStats.memoryReclaimCount > 0) {
    taskRuntimeStats["memoryReclaimCount"].addValue(
        taskStats.memoryReclaimCount);
    taskRuntimeStats.insert(
        {"memoryReclaimWallNanos", fromMillis(taskStats.memoryReclaimMs)});
  }

  taskRuntimeStats["createTime"].addValue(taskStats.executionStartTimeMs);
  if (taskStats.endTimeMs >= taskStats.executionEndTimeMs) {
    taskRuntimeStats.insert(
        {"outputConsumedDelayInNanos",
         fromMillis(taskStats.endTimeMs - taskStats.executionEndTimeMs)});
    taskRuntimeStats["endTime"].addValue(taskStats.endTimeMs);
  }

  taskRuntimeStats.insert({"nativeProcessCpuTime", fromNanos(processCpuTime_)});

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
      opOut.planNodeId = toPrestoPlanNodeId(opOut.planNodeId);
      opOut.operatorId = op.operatorId;
      opOut.operatorType = toPrestoOperatorType(op.operatorType);

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
      CpuWallTiming finishAndBackgroundTiming;
      finishAndBackgroundTiming.add(op.finishTiming);
      finishAndBackgroundTiming.add(op.backgroundTiming);
      setTiming(
          finishAndBackgroundTiming,
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

      if (op.operatorType == "HashBuild") {
        opOut.joinBuildKeyCount = op.inputPositions;
        opOut.nullJoinBuildKeyCount = op.numNullKeys;
      }
      if (op.operatorType == "HashProbe") {
        opOut.joinProbeKeyCount = op.inputPositions;
        opOut.nullJoinProbeKeyCount = op.numNullKeys;
      }

      for (const auto& stat : op.runtimeStats) {
        auto statName = generateRuntimeStatName(op, stat.first);
        opOut.runtimeStats[statName] = toRuntimeMetric(statName, stat.second);
        addRuntimeMetric(taskRuntimeStats, statName, stat.second);
      }

      OperatorStatsCollector operatorStatsCollector{
          op, opOut.runtimeStats, prestoTaskStats.runtimeStats};

      operatorStatsCollector.addIfNotZero("numSplits", op.numSplits);
      operatorStatsCollector.addIfNotZero("inputBatches", op.inputVectors);
      operatorStatsCollector.addIfNotZero("outputBatches", op.outputVectors);
      operatorStatsCollector.addIfNotZero(
          "numMemoryAllocations", op.memoryStats.numMemoryAllocations);

      // If Velox operator has spilling stats, then add them to the Presto
      // operator stats and the task stats as runtime stats.
      if (op.spilledBytes > 0) {
        addSpillingOperatorMetrics(operatorStatsCollector);
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

  processOperatorStats(prestoTaskStats, taskStatus.state);
  processTaskStats(prestoTaskStats, taskRuntimeStats, taskStatus.state);

  // Task runtime metrics we want while the Task is not finalized.
  hasStuckOperator = false;
  if (!isFinalState(taskStatus.state)) {
    taskRuntimeStats.clear();
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
    if (taskStats.longestRunningOpCallMs != 0) {
      hasStuckOperator = true;
      addRuntimeMetricIfNotZero(
          taskRuntimeStats,
          "stuck_op." + taskStats.longestRunningOpCall,
          taskStats.numCompletedDrivers);
    }
    // These metrics we need when we are running, so do not try to skipp them.
    processTaskStats(
        prestoTaskStats, taskRuntimeStats, taskStatus.state, false);
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

folly::dynamic PrestoTask::toJson() const {
  std::lock_guard<std::mutex> l(mutex);
  folly::dynamic obj = folly::dynamic::object;
  obj["task"] = task ? task->toJson() : "null";
  obj["taskStarted"] = taskStarted;
  obj["lastHeartbeatMs"] = lastHeartbeatMs;
  obj["lastTaskStatsUpdateMs"] = lastTaskStatsUpdateMs;
  obj["lastMemoryReservation"] = lastMemoryReservation;

  json j;
  to_json(j, info);
  obj["taskInfo"] = folly::parseJson(to_string(j));
  return obj;
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
