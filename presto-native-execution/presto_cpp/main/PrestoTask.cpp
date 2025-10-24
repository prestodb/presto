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

using namespace facebook::velox;

namespace facebook::presto {

std::string prestoTaskStateString(PrestoTaskState state) {
  switch (state) {
    case PrestoTaskState::kRunning:
      return "Running";
    case PrestoTaskState::kFinished:
      return "Finished";
    case PrestoTaskState::kCanceled:
      return "Canceled";
    case PrestoTaskState::kAborted:
      return "Aborted";
    case PrestoTaskState::kFailed:
      return "Failed";
    case PrestoTaskState::kPlanned:
      return "Planned";
    default:
      return fmt::format("UNKNOWN[{}]", static_cast<int>(state));
  }
}

namespace {

#define TASK_STATS_SUM(taskStats, statsName, taskStatusSum)      \
  do {                                                           \
    for (int i = 0; i < taskStats.pipelineStats.size(); ++i) {   \
      auto& pipeline = taskStats.pipelineStats[i];               \
      for (auto j = 0; j < pipeline.operatorStats.size(); ++j) { \
        auto& op = pipeline.operatorStats[j];                    \
        (taskStatusSum) += op.statsName;                         \
      }                                                          \
    }                                                            \
  } while (0)

// Convert Velox task state to Presto task state.
PrestoTaskState toPrestoTaskState(exec::TaskState state) {
  switch (state) {
    case exec::TaskState::kRunning:
      return PrestoTaskState::kRunning;
    case exec::TaskState::kFinished:
      return PrestoTaskState::kFinished;
    case exec::TaskState::kCanceled:
      return PrestoTaskState::kCanceled;
    case exec::TaskState::kAborted:
      return PrestoTaskState::kAborted;
    case exec::TaskState::kFailed:
      return PrestoTaskState::kFailed;
  }
  // Should not be here.
  return PrestoTaskState::kAborted;
}

protocol::TaskState toProtocolTaskState(PrestoTaskState state) {
  switch (state) {
    case PrestoTaskState::kRunning:
      return protocol::TaskState::RUNNING;
    case PrestoTaskState::kFinished:
      return protocol::TaskState::FINISHED;
    case PrestoTaskState::kCanceled:
      return protocol::TaskState::CANCELED;
    case PrestoTaskState::kFailed:
      return protocol::TaskState::FAILED;
    case PrestoTaskState::kPlanned:
      return protocol::TaskState::PLANNED;
    case PrestoTaskState::kAborted:
      [[fallthrough]];
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
  if (operatorType == "HashProbe") {
    return "LookupJoinOperator";
  }
  if (operatorType == "HashBuild") {
    return "HashBuilderOperator";
  }
  if (operatorType == "TableWriteMerge") {
    return "TableWriterMergeOperator";
  }
  if (operatorType == "Unnest") {
    return "UnnestOperator";
  }
  if (operatorType == "LocalPartition") {
    return "LocalExchangeSourceOperator";
  }
  if (operatorType == "LocalExchange") {
    return "LocalExchangeSinkOperator";
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
  const exec::OperatorStats& veloxStats;
  protocol::RuntimeStats& prestoStats;

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
    const std::string statName = generateRuntimeStatName(veloxStats, name);
    auto prestoMetric = createProtocolRuntimeMetric(statName, value, unit);
    prestoStats.emplace(statName, prestoMetric);
  }
};

// Add 'spilling' metrics from Velox operator stats to Presto operator stats.
void addSpillingOperatorMetrics(OperatorStatsCollector& collector) {
  auto& op = collector.veloxStats;

  collector.add("spilledBytes", op.spilledBytes, protocol::RuntimeUnit::BYTE);
  collector.add("spilledRows", op.spilledRows);
  collector.add("spilledPartitions", op.spilledPartitions);
  collector.add("spilledFiles", op.spilledFiles);
}

// Updates the operator runtime stats in 'prestoTaskStats' based on the presto
// task state and system config. For example, if the task is running, then we
// might skip reporting operator runtime stats to control the communication data
// size with the coordinator.
void updateOperatorRuntimeStats(
    protocol::TaskState state,
    protocol::TaskStats& prestoTaskStats) {
  if (SystemConfig::instance()->skipRuntimeStatsInRunningTaskInfo() &&
      !isFinalState(state)) {
    for (auto& pipelineStats : prestoTaskStats.pipelines) {
      for (auto& opStats : pipelineStats.operatorSummaries) {
        opStats.runtimeStats.clear();
      }
    }
    return;
  }

  static const std::vector<std::string> prefixToExclude{"running", "blocked"};
  for (auto& pipelineStats : prestoTaskStats.pipelines) {
    for (auto& opStats : pipelineStats.operatorSummaries) {
      for (const auto& prefix : prefixToExclude) {
        for (auto it = opStats.runtimeStats.begin();
             it != opStats.runtimeStats.end();) {
          if (it->first.find(prefix) != std::string::npos) {
            it = opStats.runtimeStats.erase(it);
          } else {
            ++it;
          }
        }
      }
    }
  }
}

// Updates the task runtime stats in 'prestoTaskStats' based on the presto
// task state and system config. For example, if the task is running, then we
// might skip reporting task runtime stats to control the communication data
// size with the coordinator.
void updateTaskRuntimeStats(
    protocol::TaskState state,
    const std::unordered_map<std::string, RuntimeMetric>& taskRuntimeStats,
    bool tryToSkipIfRunning,
    protocol::TaskStats& prestoTaskStats) {
  if (!tryToSkipIfRunning ||
      !SystemConfig::instance()->skipRuntimeStatsInRunningTaskInfo() ||
      isFinalState(state)) {
    for (const auto& stats : taskRuntimeStats) {
      prestoTaskStats.runtimeStats[stats.first] =
          toRuntimeMetric(stats.first, stats.second);
    }
  } else {
    prestoTaskStats.runtimeStats.clear();
  }
}

presto::protocol::DynamicFilterStats toPrestoDynamicFilterStats(
    const velox::exec::OperatorStats& veloxOpStats) {
  presto::protocol::DynamicFilterStats dynamicFilterStats;
  for (const auto& nodeId : veloxOpStats.dynamicFilterStats.producerNodeIds) {
    dynamicFilterStats.producerNodeIds.emplace_back(nodeId);
  }
  return dynamicFilterStats;
}

void updateOperatorRuntimeStats(
    const exec::OperatorStats& veloxOp,
    protocol::RuntimeStats& runtimeStats) {
  OperatorStatsCollector operatorStatsCollector{veloxOp, runtimeStats};

  operatorStatsCollector.addIfNotZero("numSplits", veloxOp.numSplits);
  operatorStatsCollector.addIfNotZero("inputBatches", veloxOp.inputVectors);
  operatorStatsCollector.addIfNotZero("outputBatches", veloxOp.outputVectors);
  operatorStatsCollector.addIfNotZero(
      "numMemoryAllocations", veloxOp.memoryStats.numMemoryAllocations);

  // If Velox operator has spilling stats, then add them to the Presto
  // operator stats and the task stats as runtime stats.
  if (veloxOp.spilledBytes > 0) {
    addSpillingOperatorMetrics(operatorStatsCollector);
  }
}

void updatePipelineStats(
    const PrestoTaskId& taskId,
    const int pipelineId,
    const exec::PipelineStats& veloxPipelineStats,
    const protocol::TaskStats& prestoTaskStats,
    protocol::PipelineStats& prestoPipelineStats) {
  prestoPipelineStats.inputPipeline = veloxPipelineStats.inputPipeline;
  prestoPipelineStats.outputPipeline = veloxPipelineStats.outputPipeline;
  prestoPipelineStats.firstStartTimeInMillis = prestoTaskStats.createTimeInMillis;
  prestoPipelineStats.lastStartTimeInMillis = prestoTaskStats.endTimeInMillis;
  prestoPipelineStats.lastEndTimeInMillis = prestoTaskStats.endTimeInMillis;

  prestoPipelineStats.operatorSummaries.resize(
      veloxPipelineStats.operatorStats.size());
  prestoPipelineStats.totalScheduledTimeInNanos = {};
  prestoPipelineStats.totalCpuTimeInNanos = {};
  prestoPipelineStats.totalBlockedTimeInNanos = {};
  prestoPipelineStats.userMemoryReservationInBytes = {};
  prestoPipelineStats.revocableMemoryReservationInBytes = {};
  prestoPipelineStats.systemMemoryReservationInBytes = {};

  // tasks may fail before any operators are created;
  // collect stats only when we have operators
  if (!veloxPipelineStats.operatorStats.empty()) {
    const auto& firstVeloxOpStats = veloxPipelineStats.operatorStats[0];
    const auto& lastVeloxOpStats = veloxPipelineStats.operatorStats.back();

    prestoPipelineStats.pipelineId = firstVeloxOpStats.pipelineId;
    prestoPipelineStats.totalDrivers = firstVeloxOpStats.numDrivers;
    prestoPipelineStats.rawInputPositions = firstVeloxOpStats.rawInputPositions;
    prestoPipelineStats.rawInputDataSizeInBytes =
        firstVeloxOpStats.rawInputBytes;
    prestoPipelineStats.processedInputPositions =
        firstVeloxOpStats.inputPositions;
    prestoPipelineStats.processedInputDataSizeInBytes =
        firstVeloxOpStats.inputBytes;
    prestoPipelineStats.outputPositions = lastVeloxOpStats.outputPositions;
    prestoPipelineStats.outputDataSizeInBytes = lastVeloxOpStats.outputBytes;
  }

  for (auto j = 0; j < veloxPipelineStats.operatorStats.size(); ++j) {
    auto& prestoOp = prestoPipelineStats.operatorSummaries[j];
    auto& veloxOp = veloxPipelineStats.operatorStats[j];

    prestoOp.stageId = taskId.stageId();
    prestoOp.stageExecutionId = taskId.stageExecutionId();
    prestoOp.pipelineId = pipelineId;
    prestoOp.planNodeId = veloxOp.planNodeId;
    prestoOp.planNodeId = toPrestoPlanNodeId(prestoOp.planNodeId);
    prestoOp.operatorId = veloxOp.operatorId;
    prestoOp.operatorType = toPrestoOperatorType(veloxOp.operatorType);

    prestoOp.totalDrivers = veloxOp.numDrivers;
    prestoOp.inputPositions = veloxOp.inputPositions;
    prestoOp.sumSquaredInputPositions =
        ((double)veloxOp.inputPositions) * veloxOp.inputPositions;
    prestoOp.inputDataSizeInBytes = veloxOp.inputBytes;
    prestoOp.rawInputPositions = veloxOp.rawInputPositions;
    prestoOp.rawInputDataSizeInBytes = veloxOp.rawInputBytes;

    // Report raw input statistics on the Project node following TableScan, if
    // exists.
    if (j == 1 && veloxOp.operatorType == "FilterProject" &&
        veloxPipelineStats.operatorStats[0].operatorType == "TableScan") {
      const auto& scanOp = veloxPipelineStats.operatorStats[0];
      prestoOp.rawInputPositions = scanOp.rawInputPositions;
      prestoOp.rawInputDataSizeInBytes = scanOp.rawInputBytes;
    }

    prestoOp.outputPositions = veloxOp.outputPositions;
    prestoOp.outputDataSizeInBytes = veloxOp.outputBytes;

    setTiming(
        veloxOp.isBlockedTiming,
        prestoOp.isBlockedCalls,
        prestoOp.isBlockedWall,
        prestoOp.isBlockedCpu);
    setTiming(
        veloxOp.addInputTiming,
        prestoOp.addInputCalls,
        prestoOp.addInputWall,
        prestoOp.addInputCpu);
    setTiming(
        veloxOp.getOutputTiming,
        prestoOp.getOutputCalls,
        prestoOp.getOutputWall,
        prestoOp.getOutputCpu);
    CpuWallTiming finishAndBackgroundTiming;
    finishAndBackgroundTiming.add(veloxOp.finishTiming);
    finishAndBackgroundTiming.add(veloxOp.backgroundTiming);
    setTiming(
        finishAndBackgroundTiming,
        prestoOp.finishCalls,
        prestoOp.finishWall,
        prestoOp.finishCpu);

    prestoOp.blockedWall = protocol::Duration(
        veloxOp.blockedWallNanos, protocol::TimeUnit::NANOSECONDS);

    prestoOp.userMemoryReservationInBytes = veloxOp.memoryStats.userMemoryReservation;
    prestoOp.revocableMemoryReservationInBytes = veloxOp.memoryStats.revocableMemoryReservation;
    prestoOp.systemMemoryReservationInBytes = veloxOp.memoryStats.systemMemoryReservation;
    prestoOp.peakUserMemoryReservationInBytes = veloxOp.memoryStats.peakUserMemoryReservation;
    prestoOp.peakSystemMemoryReservationInBytes = veloxOp.memoryStats.peakSystemMemoryReservation;
    prestoOp.peakTotalMemoryReservationInBytes = veloxOp.memoryStats.peakTotalMemoryReservation;

    prestoOp.spilledDataSizeInBytes = veloxOp.spilledBytes;

    if (veloxOp.operatorType == "HashBuild") {
      prestoOp.joinBuildKeyCount = veloxOp.inputPositions;
      prestoOp.nullJoinBuildKeyCount = veloxOp.numNullKeys;
    }
    if (veloxOp.operatorType == "HashProbe") {
      prestoOp.joinProbeKeyCount = veloxOp.inputPositions;
      prestoOp.nullJoinProbeKeyCount = veloxOp.numNullKeys;
    }

    if (!veloxOp.dynamicFilterStats.empty()) {
      prestoOp.dynamicFilterStats = toPrestoDynamicFilterStats(veloxOp);
    }

    for (const auto& stat : veloxOp.runtimeStats) {
      auto statName = generateRuntimeStatName(veloxOp, stat.first);
      prestoOp.runtimeStats[statName] = toRuntimeMetric(statName, stat.second);
    }

    updateOperatorRuntimeStats(veloxOp, prestoOp.runtimeStats);

    auto wallNanos = veloxOp.isBlockedTiming.wallNanos +
        veloxOp.addInputTiming.wallNanos + veloxOp.getOutputTiming.wallNanos +
        veloxOp.finishTiming.wallNanos;
    auto cpuNanos = veloxOp.isBlockedTiming.cpuNanos +
        veloxOp.addInputTiming.cpuNanos + veloxOp.getOutputTiming.cpuNanos +
        veloxOp.finishTiming.cpuNanos;

    prestoPipelineStats.totalScheduledTimeInNanos += wallNanos;
    prestoPipelineStats.totalCpuTimeInNanos += cpuNanos;
    prestoPipelineStats.totalBlockedTimeInNanos += veloxOp.blockedWallNanos;
    prestoPipelineStats.userMemoryReservationInBytes +=
        veloxOp.memoryStats.userMemoryReservation;
    prestoPipelineStats.revocableMemoryReservationInBytes +=
        veloxOp.memoryStats.revocableMemoryReservation;
    prestoPipelineStats.systemMemoryReservationInBytes +=
        veloxOp.memoryStats.systemMemoryReservation;
  } // velox pipeline's operators loop
}

} // namespace

PrestoTask::PrestoTask(
    const std::string& taskId,
    const std::string& nodeId,
    long _startProcessCpuTime)
    : id(taskId),
      startProcessCpuTime{
          _startProcessCpuTime > 0 ? _startProcessCpuTime
                                   : util::getProcessCpuTimeNs()} {
  info.taskId = taskId;
  info.nodeId = nodeId;
  createTimeMs = getCurrentTimeMs();
}

PrestoTaskState PrestoTask::taskState() const {
  if (task != nullptr) {
    const auto prestoTaskState = toPrestoTaskState(task->state());
    // Velox Task is created with 'Running' state even though it is not running
    // until start() is called. Here we check for this and return 'Planned'
    // state if it is the case.
    if (prestoTaskState == PrestoTaskState::kRunning && !taskStarted) {
      return PrestoTaskState::kPlanned;
    }
    return prestoTaskState;
  }
  // Fallback to 'aborted' if there is no Velox task.
  return PrestoTaskState::kAborted;
}

void PrestoTask::updateHeartbeatLocked() {
  lastHeartbeatMs = velox::getCurrentTimeMs();
  info.lastHeartbeatInMillis = lastHeartbeatMs;
}

void PrestoTask::updateCoordinatorHeartbeat() {
  std::lock_guard<std::mutex> l(mutex);
  updateCoordinatorHeartbeatLocked();
}

void PrestoTask::updateCoordinatorHeartbeatLocked() {
  lastCoordinatorHeartbeatMs = velox::getCurrentTimeMs();
}

uint64_t PrestoTask::timeSinceLastHeartbeatMs() const {
  std::lock_guard<std::mutex> l(mutex);
  if (lastHeartbeatMs == 0UL) {
    return 0UL;
  }
  return getCurrentTimeMs() - lastHeartbeatMs;
}

uint64_t PrestoTask::timeSinceLastCoordinatorHeartbeatMs() const {
  std::lock_guard<std::mutex> l(mutex);
  if (lastCoordinatorHeartbeatMs == 0UL) {
    return 0UL;
  }
  return getCurrentTimeMs() - lastCoordinatorHeartbeatMs;
}

void PrestoTask::recordProcessCpuTime() {
  if (processCpuTime_ > 0) {
    return;
  }

  processCpuTime_ = util::getProcessCpuTimeNs() - startProcessCpuTime;
}

protocol::TaskStatus PrestoTask::updateStatusLocked() {
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

  // We can be here before the fragment plan is received and exec task created.
  if (task == nullptr) {
    VELOX_CHECK(!taskStarted);
    return info.taskStatus;
  }

  const auto veloxTaskStats = task->taskStats();

  info.taskStatus.state = toProtocolTaskState(taskState());

  // Presto has a Driver per split. When splits represent partitions
  // of data, there is a queue of them per Task. We represent
  // running/queued table scan splits as partitioned drivers for Presto.
  info.taskStatus.queuedPartitionedDrivers =
      veloxTaskStats.numQueuedTableScanSplits;
  info.taskStatus.runningPartitionedDrivers =
      veloxTaskStats.numRunningTableScanSplits;
  // Return weights if they were supplied in the table scan splits. Coordinator
  // uses these for split scheduling.
  info.taskStatus.queuedPartitionedSplitsWeight =
      veloxTaskStats.queuedTableScanSplitWeights;
  info.taskStatus.runningPartitionedSplitsWeight =
      veloxTaskStats.runningTableScanSplitWeights;

  info.taskStatus.completedDriverGroups.clear();
  info.taskStatus.completedDriverGroups.reserve(
      veloxTaskStats.completedSplitGroups.size());
  for (auto splitGroupId : veloxTaskStats.completedSplitGroups) {
    info.taskStatus.completedDriverGroups.push_back({true, splitGroupId});
  }

  const auto veloxTaskMemStats = task->pool()->stats();
  info.taskStatus.memoryReservationInBytes = veloxTaskMemStats.usedBytes;
  info.taskStatus.systemMemoryReservationInBytes = 0;
  // NOTE: a presto worker may run multiple tasks from the same query.
  // 'peakNodeTotalMemoryReservationInBytes' represents peak memory usage across
  // all these tasks.
  info.taskStatus.peakNodeTotalMemoryReservationInBytes =
      task->queryCtx()->pool()->peakBytes();

  TASK_STATS_SUM(
      veloxTaskStats,
      physicalWrittenBytes,
      info.taskStatus.physicalWrittenDataSizeInBytes);

  info.taskStatus.outputBufferUtilization =
      veloxTaskStats.outputBufferUtilization;
  info.taskStatus.outputBufferOverutilized =
      veloxTaskStats.outputBufferOverutilized;

  if (task->error() && info.taskStatus.failures.empty()) {
    info.taskStatus.failures.emplace_back(toPrestoError(task->error()));
  }

  if (isFinalState(info.taskStatus.state)) {
    recordProcessCpuTime();
  }
  return info.taskStatus;
}

void PrestoTask::updateOutputBufferInfoLocked(
    const velox::exec::TaskStats& veloxTaskStats,
    std::unordered_map<std::string, RuntimeMetric>& taskRuntimeStats) {
  if (!veloxTaskStats.outputBufferStats.has_value()) {
    return;
  }
  const auto& outputBufferStats = veloxTaskStats.outputBufferStats.value();
  auto& outputBufferInfo = info.outputBuffers;
  outputBufferInfo.type =
      velox::core::PartitionedOutputNode::toName(outputBufferStats.kind);
  outputBufferInfo.canAddBuffers = !outputBufferStats.noMoreBuffers;
  outputBufferInfo.canAddPages = !outputBufferStats.noMoreData;
  outputBufferInfo.totalBufferedBytes = outputBufferStats.bufferedBytes;
  outputBufferInfo.totalBufferedPages = outputBufferStats.bufferedPages;
  outputBufferInfo.totalPagesSent = outputBufferStats.totalPagesSent;
  outputBufferInfo.totalRowsSent = outputBufferStats.totalRowsSent;
  // TODO: populate state and destination buffer stats in info.outputBuffers.

  taskRuntimeStats.insert(
      {"averageOutputBufferWallNanos",
       fromMillis(outputBufferStats.averageBufferTimeMs)});
  taskRuntimeStats["numTopOutputBuffers"].addValue(
      outputBufferStats.numTopBuffers);
}

protocol::TaskInfo PrestoTask::updateInfoLocked(bool summarize) {
  const protocol::TaskStatus prestoTaskStatus = updateStatusLocked();

  // Return limited info if there is no exec task.
  if (task == nullptr) {
    return info;
  }
  const velox::exec::TaskStats veloxTaskStats = task->taskStats();
  const uint64_t currentTimeMs = velox::getCurrentTimeMs();
  // Set 'lastTaskStatsUpdateMs' to execution start time if it is 0.
  if (lastTaskStatsUpdateMs == 0) {
    lastTaskStatsUpdateMs = veloxTaskStats.executionStartTimeMs;
  }

  std::unordered_map<std::string, RuntimeMetric> taskRuntimeStats;
  protocol::TaskStats& prestoTaskStats = info.stats;
  // Clear the old runtime metrics as not all of them would be overwritten by
  // the new ones.
  prestoTaskStats.runtimeStats.clear();

  updateOutputBufferInfoLocked(veloxTaskStats, taskRuntimeStats);

  // Update time related info.
  updateTimeInfoLocked(veloxTaskStats, currentTimeMs, taskRuntimeStats);

  // Update memory related info.
  updateMemoryInfoLocked(veloxTaskStats, currentTimeMs, taskRuntimeStats);

  // Update execution related info.
  updateExecutionInfoLocked(
      veloxTaskStats,
      prestoTaskStatus,
      taskRuntimeStats,
      isFinalState(prestoTaskStatus.state) || !summarize);

  // Task runtime metrics we want while the Task is not finalized.
  hasStuckOperator = false;
  if (!isFinalState(prestoTaskStatus.state)) {
    taskRuntimeStats.clear();

    addRuntimeMetricIfNotZero(
        taskRuntimeStats, "drivers.total", veloxTaskStats.numTotalDrivers);
    addRuntimeMetricIfNotZero(
        taskRuntimeStats, "drivers.running", veloxTaskStats.numRunningDrivers);
    addRuntimeMetricIfNotZero(
        taskRuntimeStats,
        "drivers.completed",
        veloxTaskStats.numCompletedDrivers);
    addRuntimeMetricIfNotZero(
        taskRuntimeStats,
        "drivers.terminated",
        veloxTaskStats.numTerminatedDrivers);
    for (const auto it : veloxTaskStats.numBlockedDrivers) {
      addRuntimeMetricIfNotZero(
          taskRuntimeStats,
          fmt::format("drivers.{}", exec::BlockingReasonName::toName(it.first)),
          it.second);
    }
    if (veloxTaskStats.longestRunningOpCallMs != 0) {
      hasStuckOperator = true;
      addRuntimeMetricIfNotZero(
          taskRuntimeStats,
          "stuck_op." + veloxTaskStats.longestRunningOpCall,
          veloxTaskStats.numCompletedDrivers);
    }
    // These metrics we need when we are running, so do not try to skipp them.
    updateTaskRuntimeStats(
        prestoTaskStatus.state,
        taskRuntimeStats,
        /*tryToSkipIfRunning=*/false,
        prestoTaskStats);
  }

  lastTaskStatsUpdateMs = currentTimeMs;
  return info;
}

void PrestoTask::updateTimeInfoLocked(
    const velox::exec::TaskStats& veloxTaskStats,
    uint64_t currentTimeMs,
    std::unordered_map<std::string, velox::RuntimeMetric>& taskRuntimeStats) {
  protocol::TaskStats& prestoTaskStats = info.stats;

  prestoTaskStats.totalScheduledTimeInNanos = {};
  prestoTaskStats.totalCpuTimeInNanos = {};
  prestoTaskStats.totalBlockedTimeInNanos = {};

  prestoTaskStats.createTimeInMillis = createTimeMs;
  startTimeMs = veloxTaskStats.executionStartTimeMs;
  prestoTaskStats.firstStartTimeInMillis = veloxTaskStats.firstSplitStartTimeMs;
  firstSplitStartTimeMs = veloxTaskStats.firstSplitStartTimeMs;
  prestoTaskStats.lastStartTimeInMillis = veloxTaskStats.lastSplitStartTimeMs;
  prestoTaskStats.lastEndTimeInMillis = veloxTaskStats.executionEndTimeMs;
  prestoTaskStats.endTimeInMillis = veloxTaskStats.executionEndTimeMs;
  lastEndTimeMs = veloxTaskStats.executionEndTimeMs;

  if (veloxTaskStats.executionEndTimeMs > veloxTaskStats.executionStartTimeMs) {
    prestoTaskStats.elapsedTimeInNanos = (veloxTaskStats.executionEndTimeMs -
                                          veloxTaskStats.executionStartTimeMs) *
        1'000'000;
  } else {
    prestoTaskStats.elapsedTimeInNanos =
        (currentTimeMs - veloxTaskStats.executionStartTimeMs) * 1'000'000;
  }

  taskRuntimeStats["createTime"].addValue(veloxTaskStats.executionStartTimeMs);
  if (veloxTaskStats.endTimeMs >= veloxTaskStats.executionEndTimeMs) {
    taskRuntimeStats.insert(
        {"outputConsumedDelayInNanos",
         fromMillis(
             veloxTaskStats.endTimeMs - veloxTaskStats.executionEndTimeMs)});
    taskRuntimeStats["endTime"].addValue(veloxTaskStats.endTimeMs);
  }
  taskRuntimeStats.insert({"nativeProcessCpuTime", fromNanos(processCpuTime_)});
  // Represents the time between receiving first taskUpdate and task creation time
  taskRuntimeStats.insert({"taskCreationTime", fromNanos((createFinishTimeMs - firstTimeReceiveTaskUpdateMs) * 1'000'000)});
}

void PrestoTask::updateMemoryInfoLocked(
    const velox::exec::TaskStats& veloxTaskStats,
    uint64_t currentTimeMs,
    std::unordered_map<std::string, velox::RuntimeMetric>& taskRuntimeStats) {
  protocol::TaskStats& prestoTaskStats = info.stats;

  const auto veloxTaskMemStats = task->pool()->stats();
  const auto currentBytes = veloxTaskMemStats.usedBytes;
  prestoTaskStats.userMemoryReservationInBytes = currentBytes;
  prestoTaskStats.systemMemoryReservationInBytes = 0;
  prestoTaskStats.peakUserMemoryInBytes = veloxTaskMemStats.peakBytes;
  prestoTaskStats.peakTotalMemoryInBytes = veloxTaskMemStats.peakBytes;

  // TODO(venkatra): Populate these memory stats as well.
  prestoTaskStats.revocableMemoryReservationInBytes = {};

  const int64_t averageMemoryForLastPeriod =
      (currentBytes + lastMemoryReservation) / 2;
  const double sinceLastPeriodMs = currentTimeMs - lastTaskStatsUpdateMs;

  prestoTaskStats.cumulativeUserMemory +=
      (averageMemoryForLastPeriod * sinceLastPeriodMs);
  // NOTE: velox doesn't differentiate user and system memory usages.
  prestoTaskStats.cumulativeTotalMemory = prestoTaskStats.cumulativeUserMemory;
  prestoTaskStats.peakNodeTotalMemoryInBytes =
      task->queryCtx()->pool()->peakBytes();

  if (veloxTaskStats.memoryReclaimCount > 0) {
    taskRuntimeStats["taskMemoryReclaimCount"].addValue(
        veloxTaskStats.memoryReclaimCount);
    taskRuntimeStats.insert(
        {"taskMemoryReclaimWallNanos",
         fromMillis(veloxTaskStats.memoryReclaimMs)});
  }
  lastMemoryReservation = currentBytes;
}

void PrestoTask::updateExecutionInfoLocked(
    const velox::exec::TaskStats& veloxTaskStats,
    const protocol::TaskStatus& prestoTaskStatus,
    std::unordered_map<std::string, velox::RuntimeMetric>& taskRuntimeStats,
    bool includePipelineStats) {
  protocol::TaskStats& prestoTaskStats = info.stats;

  prestoTaskStats.rawInputPositions = 0;
  prestoTaskStats.rawInputDataSizeInBytes = 0;
  prestoTaskStats.processedInputPositions = 0;
  prestoTaskStats.processedInputDataSizeInBytes = 0;
  prestoTaskStats.outputPositions = 0;
  prestoTaskStats.outputDataSizeInBytes = 0;

  // NOTE: This logic is implemented in a backwards-compatible way because
  // the coordinator and worker may not be upgraded at the same time.
  //
  // To ensure safe rollout:
  // - We are introducing new fields (e.g., `totalNewDrivers`) instead of modifying or
  //   removing existing ones.
  // - The worker is updated first to populate both old and new fields.
  // - The coordinator continues to use the old fields until it is updated to handle
  //   the new ones.
  //
  // Once both coordinator and worker support the new fields, we can safely remove
  // the legacy fields in a follow-up cleanup PR.

  prestoTaskStats.totalDrivers = veloxTaskStats.numTotalSplits;
  prestoTaskStats.queuedDrivers = veloxTaskStats.numQueuedSplits;
  prestoTaskStats.runningDrivers = veloxTaskStats.numRunningDrivers;
  prestoTaskStats.completedDrivers = veloxTaskStats.numFinishedSplits;

  prestoTaskStats.totalNewDrivers = veloxTaskStats.numTotalDrivers;
  prestoTaskStats.queuedNewDrivers = veloxTaskStats.numQueuedDrivers;
  prestoTaskStats.runningNewDrivers = veloxTaskStats.numRunningDrivers;
  prestoTaskStats.completedNewDrivers = veloxTaskStats.numCompletedDrivers;

  prestoTaskStats.totalSplits = veloxTaskStats.numTotalSplits;
  prestoTaskStats.queuedSplits = veloxTaskStats.numQueuedSplits;
  prestoTaskStats.runningSplits = veloxTaskStats.numRunningSplits;
  prestoTaskStats.completedSplits = veloxTaskStats.numFinishedSplits;

  if (includePipelineStats) {
    prestoTaskStats.pipelines.resize(veloxTaskStats.pipelineStats.size());
  } else {
    prestoTaskStats.pipelines = {};
  }
  for (int i = 0; i < veloxTaskStats.pipelineStats.size(); ++i) {
    auto& veloxPipeline = veloxTaskStats.pipelineStats[i];

    // tasks may fail before any operators are created;
    // collect stats only when we have operators
    if (!veloxPipeline.operatorStats.empty()) {
      if (veloxPipeline.inputPipeline) {
        const auto& firstVeloxOpStats = veloxPipeline.operatorStats[0];
        prestoTaskStats.rawInputPositions +=
            firstVeloxOpStats.rawInputPositions;
        prestoTaskStats.rawInputDataSizeInBytes +=
            firstVeloxOpStats.rawInputBytes;
        prestoTaskStats.processedInputPositions +=
            firstVeloxOpStats.inputPositions;
        prestoTaskStats.processedInputDataSizeInBytes +=
            firstVeloxOpStats.inputBytes;
      }
      if (veloxPipeline.outputPipeline) {
        const auto& lastVeloxOpStats = veloxPipeline.operatorStats.back();
        prestoTaskStats.outputPositions += lastVeloxOpStats.outputPositions;
        prestoTaskStats.outputDataSizeInBytes += lastVeloxOpStats.outputBytes;
      }

      for (auto j = 0; j < veloxPipeline.operatorStats.size(); ++j) {
        auto& veloxOp = veloxPipeline.operatorStats[j];
        auto wallNanos = veloxOp.isBlockedTiming.wallNanos +
            veloxOp.addInputTiming.wallNanos +
            veloxOp.getOutputTiming.wallNanos + veloxOp.finishTiming.wallNanos;
        auto cpuNanos = veloxOp.isBlockedTiming.cpuNanos +
            veloxOp.addInputTiming.cpuNanos + veloxOp.getOutputTiming.cpuNanos +
            veloxOp.finishTiming.cpuNanos;

        prestoTaskStats.totalScheduledTimeInNanos += wallNanos;
        prestoTaskStats.totalCpuTimeInNanos += cpuNanos;
        prestoTaskStats.totalBlockedTimeInNanos += veloxOp.blockedWallNanos;

        for (const auto& stat : veloxOp.runtimeStats) {
          auto statName = generateRuntimeStatName(veloxOp, stat.first);
          addRuntimeMetric(taskRuntimeStats, statName, stat.second);
        }
        updateOperatorRuntimeStats(veloxOp, prestoTaskStats.runtimeStats);
      }
    }

    for (const auto& driverStat : veloxPipeline.driverStats) {
      for (const auto& [name, value] : driverStat.runtimeStats) {
        addRuntimeMetric(taskRuntimeStats, name, value);
      }
    }

    if (includePipelineStats) {
      updatePipelineStats(
          id, i, veloxPipeline, prestoTaskStats, prestoTaskStats.pipelines[i]);
    }
  } // velox task's pipelines loop

  updateOperatorRuntimeStats(prestoTaskStatus.state, prestoTaskStats);
  updateTaskRuntimeStats(
      prestoTaskStatus.state,
      taskRuntimeStats,
      /*tryToSkipIfRunning=*/true,
      prestoTaskStats);
}

/*static*/ std::string PrestoTask::taskStatesToString(
    const std::array<size_t, 6>& taskStates) {
  std::string str;
  for (size_t i = 0; i < taskStates.size(); ++i) {
    if (taskStates[i] != 0) {
      folly::toAppend(
          fmt::format(
              "{}={} ",
              prestoTaskStateString(PrestoTaskState(i)),
              taskStates[i]),
          &str);
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
  obj["createTimeMs"] = createTimeMs;
  obj["startTimeMs"] = startTimeMs;
  obj["firstSplitStartTimeMs"] = firstSplitStartTimeMs;
  obj["lastEndTimeMs"] = lastEndTimeMs;

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
      [[fallthrough]];
    case protocol::TaskState::FAILED:
      [[fallthrough]];
    case protocol::TaskState::ABORTED:
      [[fallthrough]];
    case protocol::TaskState::CANCELED:
      return true;
    default:
      return false;
  }
}

} // namespace facebook::presto
