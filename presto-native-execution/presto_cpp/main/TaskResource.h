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

#include "presto_cpp/main/TaskManager.h"
#include "presto_cpp/main/http/HttpServer.h"
#include "velox/common/memory/Memory.h"

namespace facebook::presto {

class TaskResource {
 public:
  explicit TaskResource(
      TaskManager& taskManager,
      velox::memory::MemoryPool* pool,
      folly::Executor* httpSrvCpuExecutor)
      : httpSrvCpuExecutor_(httpSrvCpuExecutor),
        pool_{pool},
        taskManager_(taskManager) {}

  void registerUris(http::HttpServer& server);

 private:
  proxygen::RequestHandler* abortResults(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* acknowledgeResults(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  /// Creates or updates a regular task. A regular task shuffles data through
  /// standard build-in Http mechanisms.
  proxygen::RequestHandler* createOrUpdateTask(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  /// Creates or updates a batch task. A batch task has additional information
  /// that needs to be passed over in order to create custom shuffle interfaces.
  /// Please refer to
  /// https://github.com/prestodb/presto/blob/master/presto-spark-base/src/main/java/com/facebook/presto/spark/execution/BatchTaskUpdateRequest.java
  /// to see the the Http POST structure which should be passed to create a
  /// batch task.
  /// - The shuffle write information should be passed in with
  /// BatchTaskUpdateRequest.shuffleWriteInfo.
  /// - The shuffle read information should be passed in with
  /// ((RemoteSplit)(BatchTaskUpdateRequest.taskUpdateRequest.sources[x].splits.split.connectorSplit)).location.location
  ///
  /// The encoding format shall be decided by custom shuffle implementation.
  /// Example could be found in:
  /// - Passer (Java):
  /// com.facebook.presto.spark.execution.PrestoSparkLocalShuffleInfoSerializer
  /// - Receiver (C++): facebook::presto::operators::LocalShuffleInfo
  proxygen::RequestHandler* createOrUpdateBatchTask(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* createOrUpdateTaskImpl(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch,
      const std::function<std::unique_ptr<protocol::TaskInfo>(
          const protocol::TaskId&,
          const std::string&,
          long)>& createOrUpdateFunc);

  proxygen::RequestHandler* deleteTask(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* getResults(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch,
      bool getDataSize);

  proxygen::RequestHandler* getTaskStatus(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* getTaskInfo(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* removeRemoteSource(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  folly::Executor* const httpSrvCpuExecutor_;
  velox::memory::MemoryPool* const pool_;

  TaskManager& taskManager_;
};

} // namespace facebook::presto
