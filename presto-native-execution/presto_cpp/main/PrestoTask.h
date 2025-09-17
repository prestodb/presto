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

#include <memory>
#include <unordered_set>
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/main/types/PrestoTaskId.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/exec/Task.h"

namespace facebook::velox {
struct RuntimeMetric;
}

namespace facebook::presto {

/// Velox Task does not have Planned state, so we add this enum to have this
/// state.
enum class PrestoTaskState : int {
  kRunning = 0,
  kFinished = 1,
  kCanceled = 2,
  kAborted = 3,
  kFailed = 4,
  kPlanned = 5
};

std::string prestoTaskStateString(PrestoTaskState state);

template <typename T>
struct PromiseHolder {
  explicit PromiseHolder(folly::Promise<T> p) : promise(std::move(p)) {}
  folly::Promise<T> promise;

  void atDestruction(
      std::function<void(folly::Promise<T> promise)> atDestruction) {
    atDestruction_ = atDestruction;
  }

  ~PromiseHolder() {
    if (atDestruction_ && !promise.isFulfilled()) {
      atDestruction_(std::move(promise));
    }
  }

 private:
  std::function<void(folly::Promise<T> promise)> atDestruction_;
};

template <typename T>
using PromiseHolderPtr = std::shared_ptr<PromiseHolder<T>>;

template <typename T>
using PromiseHolderWeakPtr = std::weak_ptr<PromiseHolder<T>>;

struct Result {
  int64_t sequence;
  int64_t nextSequence;
  std::unique_ptr<folly::IOBuf> data;
  bool complete;
  std::vector<int64_t> remainingBytes;
};

struct ResultRequest {
  PromiseHolderWeakPtr<std::unique_ptr<Result>> promise;
  std::weak_ptr<http::CallbackRequestHandlerState> state;
  protocol::TaskId taskId;
  int64_t bufferId;
  int64_t token;
  protocol::DataSize maxSize;

  ResultRequest(
      PromiseHolderWeakPtr<std::unique_ptr<Result>> _promise,
      std::weak_ptr<http::CallbackRequestHandlerState> _state,
      protocol::TaskId _taskId,
      int64_t _bufferId,
      int64_t _token,
      protocol::DataSize _maxSize)
      : promise(std::move(_promise)),
        state(std::move(_state)),
        taskId(_taskId),
        bufferId(_bufferId),
        token(_token),
        maxSize(_maxSize) {}
};

struct PrestoTask {
  const PrestoTaskId id;
  const long startProcessCpuTime;
  std::shared_ptr<velox::exec::Task> task;
  std::atomic_bool hasStuckOperator{false};

  /// Has the task been normally created and started.
  /// When you create task with error - it has never been started.
  /// When you create task from 'delete task' - it has never been started.
  /// When you create task from any other endpoint, such as 'get result' - it
  /// has not been started, until the actual 'create task' message comes.
  bool taskStarted{false};

  /// Time point (in ms) when the last message (any) came for this task.
  // TODO (spershin): Deprecate it, use only the 'lastCoordinatorHeartbeatMs'.
  uint64_t lastHeartbeatMs{0};

  /// Time point (in ms) when the last message came for this task from the
  /// Coordinator. Used to determine if the Task has been abandoned.
  uint64_t lastCoordinatorHeartbeatMs{0};

  /// Time point (in ms) when the time we updated Task stats.
  uint64_t lastTaskStatsUpdateMs{0};

  uint64_t lastMemoryReservation{0};
  /// Time point (in ms) when the time we start task creating.
  uint64_t createTimeMs{0};
  /// Time point (in ms) when the first time we receive task update.
  uint64_t firstTimeReceiveTaskUpdateMs{0};
  /// Time point (in ms) when the time we finish task creating.
  uint64_t createFinishTimeMs{0};
  uint64_t startTimeMs{0};
  uint64_t firstSplitStartTimeMs{0};
  uint64_t lastEndTimeMs{0};
  mutable std::mutex mutex;

  /// Error before task is created or when task is being created.
  std::exception_ptr error{nullptr};

  /// Contains state info but is never returned.
  protocol::TaskInfo info;

  /// Pending result requests keyed on buffer ID. May arrive before 'task' is
  /// created. May be accessed on different threads outside of 'mutex', hence
  /// shared_ptr to define lifetime.
  std::unordered_map<int64_t, std::shared_ptr<ResultRequest>> resultRequests;

  /// Pending status request. May arrive before there is a Task.
  PromiseHolderWeakPtr<std::unique_ptr<protocol::TaskStatus>> statusRequest;

  /// Info request. May arrive before there is a Task.
  PromiseHolderWeakPtr<std::unique_ptr<protocol::TaskInfo>> infoRequest;

  /// If the task has not been started yet, we collect all plan node IDs that
  /// had 'no more splits' message to process them after the task starts.
  std::unordered_set<velox::core::PlanNodeId> delayedNoMoreSplitsPlanNodes_;

  /// @param taskId Task ID.
  /// @param nodeId Node ID.
  /// @param startCpuTime CPU time in nanoseconds recorded when request to
  /// create this task arrived.
  PrestoTask(
      const std::string& taskId,
      const std::string& nodeId,
      long startProcessCpuTime = 0);

  /// Returns current task state, including 'planning'.
  /// If Velox task is null, it returns 'aborted'.
  PrestoTaskState taskState() const;

  /// Updates when this task was touched last time.
  void updateHeartbeatLocked();

  /// Updates time point (ms) when this task was touched last time by a message
  /// from the Coordinator.
  void updateCoordinatorHeartbeat();
  void updateCoordinatorHeartbeatLocked();

  /// Returns time (ms) since the task was touched last time (last heartbeat).
  /// Returns zero, if never (shouldn't happen).
  uint64_t timeSinceLastHeartbeatMs() const;

  /// Returns time (ms) since the task was touched last time by a message from
  /// the Coordinator.
  /// If above never happened, returns time since the task start or zero, if
  /// task never started.
  uint64_t timeSinceLastCoordinatorHeartbeatMs() const;

  protocol::TaskStatus updateStatus() {
    std::lock_guard<std::mutex> l(mutex);
    return updateStatusLocked();
  }

  protocol::TaskInfo updateInfo(bool summarize) {
    std::lock_guard<std::mutex> l(mutex);
    return updateInfoLocked(summarize);
  }

  /// Turns the task numbers (per state) into a string.
  static std::string taskStatesToString(
      const std::array<size_t, 6>& taskStates);

  /// Invoked to update presto task status from the updated velox task stats.
  protocol::TaskStatus updateStatusLocked();
  protocol::TaskInfo updateInfoLocked(bool summarize);

  folly::dynamic toJson() const;

 private:
  void recordProcessCpuTime();

  void updateOutputBufferInfoLocked(
      const velox::exec::TaskStats& veloxTaskStats,
      std::unordered_map<std::string, velox::RuntimeMetric>& taskRuntimeStats);

  void updateTimeInfoLocked(
      const velox::exec::TaskStats& veloxTaskStats,
      uint64_t currentTimeMs,
      std::unordered_map<std::string, velox::RuntimeMetric>& taskRuntimeStats);

  void updateExecutionInfoLocked(
      const velox::exec::TaskStats& veloxTaskStats,
      const protocol::TaskStatus& prestoTaskStatus,
      std::unordered_map<std::string, velox::RuntimeMetric>& taskRuntimeStats,
      bool includePipelineStats);

  void updateMemoryInfoLocked(
      const velox::exec::TaskStats& veloxTaskStats,
      uint64_t currentTimeMs,
      std::unordered_map<std::string, velox::RuntimeMetric>& taskRuntimeStats);

  long processCpuTime_{0};
};

using TaskMap =
    std::unordered_map<protocol::TaskId, std::shared_ptr<PrestoTask>>;

protocol::RuntimeMetric toRuntimeMetric(
    const std::string& name,
    const facebook::velox::RuntimeMetric& metric);

bool isFinalState(protocol::TaskState state);

} // namespace facebook::presto
