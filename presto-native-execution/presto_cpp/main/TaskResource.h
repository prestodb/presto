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
  explicit TaskResource(TaskManager& taskManager)
      : taskManager_(taskManager),
        pool_(velox::memory::getDefaultScopedMemoryPool()) {}

  void registerUris(http::HttpServer& server);

  velox::memory::ScopedMemoryPool* getPool() const {
    return pool_.get();
  }

 private:
  proxygen::RequestHandler* abortResults(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* acknowledgeResults(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* createOrUpdateTask(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* deleteTask(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* getResults(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* getTaskStatus(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* getTaskInfo(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  proxygen::RequestHandler* removeRemoteSource(
      proxygen::HTTPMessage* message,
      const std::vector<std::string>& pathMatch);

  TaskManager& taskManager_;
  std::unique_ptr<velox::memory::ScopedMemoryPool> pool_;
};

} // namespace facebook::presto
