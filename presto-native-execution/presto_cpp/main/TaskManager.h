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

#include <folly/Synchronized.h>
#include <deque>
#include <memory>
#include "presto_cpp/main/PrestoTask.h"
#include "presto_cpp/main/QueryContextManager.h"
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/exec/OutputBufferManager.h"

namespace facebook::presto {

// One entry can hold multiple queued tasks for the same query.
using TaskQueue = std::deque<std::vector<std::weak_ptr<PrestoTask>>>;

class TaskManager {
 public:
  TaskManager(
      folly::Executor* driverExecutor,
      folly::Executor* httpSrvExecutor,
      folly::Executor* spillerExecutor);

  virtual ~TaskManager() = default;

  /// Invoked by Presto server shutdown to wait for all the tasks to complete
  /// and cleanup the completed tasks.
  void shutdown();

  void setBaseUri(const std::string& baseUri);

  void setNodeId(const std::string& nodeId);

  void setBaseSpillDirectory(const std::string& baseSpillDirectory);

  bool emptyBaseSpillDirectory() const;

  std::string getBaseSpillDirectory() const;

  /// Sets the time (ms) that a task is considered to be old for cleanup since
  /// its completion.
  void setOldTaskCleanUpMs(int32_t oldTaskCleanUpMs);

  TaskMap tasks() const;

  void abortResults(const protocol::TaskId& taskId, long bufferId);

  void
  acknowledgeResults(const protocol::TaskId& taskId, long bufferId, long token);

  // Creating an empty task that only contains the error information so that
  // next time coordinator checks for the status it retrieves the error.
  std::unique_ptr<protocol::TaskInfo> createOrUpdateErrorTask(
      const protocol::TaskId& taskId,
      const std::exception_ptr& exception,
      bool summarize,
      long startProcessCpuTime);

  std::unique_ptr<protocol::TaskInfo> createOrUpdateTask(
      const protocol::TaskId& taskId,
      const protocol::TaskUpdateRequest& updateRequest,
      const velox::core::PlanFragment& planFragment,
      bool summarize,
      std::shared_ptr<velox::core::QueryCtx> queryCtx,
      long startProcessCpuTime);

  std::unique_ptr<protocol::TaskInfo> createOrUpdateBatchTask(
      const protocol::TaskId& taskId,
      const protocol::BatchTaskUpdateRequest& batchUpdateRequest,
      const velox::core::PlanFragment& planFragment,
      bool summarize,
      std::shared_ptr<velox::core::QueryCtx> queryCtx,
      long startProcessCpuTime);

  // Iterates through a map of resultRequests and fetches data from
  // buffer manager. This method uses the getData() global call to fetch
  // data for each resultRequest bufferManager. If the output buffer for task
  // is not found, prepares the Result object with completed flags set to false
  // and notifies the future.
  // Note: This method is made public for unit testing purpose only.
  void getDataForResultRequests(
      const std::unordered_map<int64_t, std::shared_ptr<ResultRequest>>&
          resultRequests);

  std::unique_ptr<protocol::TaskInfo>
  deleteTask(const protocol::TaskId& taskId, bool abort, bool summarize);

  /// Remove old Finished, Cancelled, Failed and Aborted tasks.
  /// Old is being defined by the lifetime of the task.
  size_t cleanOldTasks();

  folly::Future<std::unique_ptr<protocol::TaskInfo>> getTaskInfo(
      const protocol::TaskId& taskId,
      bool summarize,
      std::optional<protocol::TaskState> currentState,
      std::optional<protocol::Duration> maxWait,
      std::shared_ptr<http::CallbackRequestHandlerState> state);

  folly::Future<std::unique_ptr<Result>> getResults(
      const protocol::TaskId& taskId,
      long destination,
      long token,
      protocol::DataSize maxSize,
      protocol::Duration maxWait,
      std::shared_ptr<http::CallbackRequestHandlerState> state);

  folly::Future<std::unique_ptr<protocol::TaskStatus>> getTaskStatus(
      const protocol::TaskId& taskId,
      std::optional<protocol::TaskState> currentState,
      std::optional<protocol::Duration> maxWait,
      std::shared_ptr<http::CallbackRequestHandlerState> state);

  void removeRemoteSource(
      const protocol::TaskId& taskId,
      const protocol::TaskId& remoteSourceTaskId);

  std::string toString() const;

  QueryContextManager* getQueryContextManager() {
    return queryContextManager_.get();
  }

  /// Make upto target task threads to yield. Task candidate must have been on
  /// thread for at least sliceMicros to be yieldable. Return the number of
  /// threads in tasks that were requested to yield.
  int32_t yieldTasks(int32_t numTargetThreadsToYield, int32_t timeSliceMicros);

  const QueryContextManager* getQueryContextManager() const;

  inline size_t getNumTasks() const {
    return taskMap_.rlock()->size();
  }

  /// Returns the processed input data size in bytes for tasks.
  int64_t getBytesProcessed() const;

  /// Stores the number of drivers in various states of execution.
  velox::exec::Task::DriverCounts getDriverCounts();

  /// Returns array with number of tasks for each of six PrestoTaskState (enum
  /// defined in PrestoTask.h).
  std::array<size_t, 6> getTaskNumbers(size_t& numTasks) const;

  /// Returns number of tasks in the task queue.
  size_t numQueuedTasks() const;

  /// Invoked to check the stuck operation calls in the system.  If the function
  /// fails to get the stuck call information from a task due to the lock
  /// timeout, it adds the task to 'blockedTasks'.  Otherwise, it adds all stuck
  /// call information to 'stuckOpCalls'.  The function returns false if a lock
  /// on the taskMap cannot be taken, otherwise returns true.
  bool getStuckOpCalls(
      std::vector<std::string>& deadlockTasks,
      std::vector<velox::exec::Task::OpCallInfo>& stuckOpCalls) const;

  /// Always returns tuple of non-empty string containing the spill directory
  /// and the date string directory, which is parent directory of task spill
  /// directory.
  static std::tuple<std::string, std::string> buildTaskSpillDirectoryPath(
      const std::string& baseSpillPath,
      const std::string& nodeIp,
      const std::string& nodeId,
      const std::string& queryId,
      const protocol::TaskId& taskId,
      bool includeNodeInSpillPath);

  /// Presto Server can notify the Task Manager that the former is overloaded,
  /// so the Task Manager can optionally change Task admission algorithm.
  void setServerOverloaded(bool serverOverloaded) {
    serverOverloaded_ = serverOverloaded;
  }

  /// Returns last known number of queued drivers. Used in determining if the
  /// server is CPU overloaded.
  uint32_t numQueuedDrivers() const {
    return numQueuedDrivers_;
  }

  /// Contains the logic on starting tasks if not overloaded.
  void maybeStartTaskLocked(
      std::shared_ptr<PrestoTask>& prestoTask,
      bool& startNextQueuedTask);

  /// See if we have any queued tasks that can be started.
  void maybeStartNextQueuedTask();

 protected:
  std::unique_ptr<QueryContextManager> queryContextManager_;

 private:
  static constexpr folly::StringPiece kMaxDriversPerTask{
      "max_drivers_per_task"};
  static constexpr folly::StringPiece kConcurrentLifespansPerTask{
      "concurrent_lifespans_per_task"};
  static constexpr folly::StringPiece kSessionTimezone{"session_timezone"};

  // We request cancellation for tasks which haven't been accessed by
  // coordinator for a considerable time.
  void cancelAbandonedTasks();

  std::unique_ptr<protocol::TaskInfo> createOrUpdateTaskImpl(
      const protocol::TaskId& taskId,
      const velox::core::PlanFragment& planFragment,
      const std::vector<protocol::TaskSource>& sources,
      const protocol::OutputBuffers& outputBuffers,
      bool summarize,
      std::shared_ptr<velox::core::QueryCtx> queryCtx,
      long startProcessCpuTime);

  std::shared_ptr<PrestoTask> findOrCreateTask(
      const protocol::TaskId& taskId,
      long startProcessCpuTime = 0);

  // Starting the task with task mutex already locked.
  void startTaskLocked(std::shared_ptr<PrestoTask>& prestoTask);

  std::string baseUri_;
  std::string nodeId_;
  folly::Synchronized<std::string> baseSpillDir_;
  int32_t oldTaskCleanUpMs_;
  std::shared_ptr<velox::exec::OutputBufferManager> bufferManager_;
  folly::Synchronized<TaskMap> taskMap_;
  folly::Synchronized<TaskQueue> taskQueue_;
  folly::Executor* httpSrvCpuExecutor_;
  std::atomic_bool serverOverloaded_{false};
  std::atomic_uint32_t numQueuedDrivers_{0};
};

} // namespace facebook::presto
