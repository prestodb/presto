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

#include "presto_cpp/main/thrift/gen-cpp2/PrestoThrift.h"
#include "presto_cpp/main/TaskManager.h"
#include "presto_cpp/main/types/VeloxPlanValidator.h"
#include "velox/common/memory/Memory.h"

namespace facebook::presto::thrift {

class PrestoThriftServiceHandler
    : public facebook::presto::thrift::PrestoThriftSvIf {
 public:
  explicit PrestoThriftServiceHandler(
      std::shared_ptr<velox::memory::MemoryPool> pool,
      std::shared_ptr<VeloxPlanValidator> planValidator,
      std::shared_ptr<TaskManager> taskManager)
      : pool_(std::move(pool)),
        planValidator_(std::move(planValidator)),
        taskManager_(std::move(taskManager)) {}

  folly::Future<std::unique_ptr<::facebook::presto::thrift::TaskResult>>
  future_getTaskResults(
      std::unique_ptr<std::string> taskId,
      int64_t bufferId,
      int64_t token,
      int64_t maxSizeBytes,
      int64_t maxWaitMicros,
      bool getDataSize) override;

  folly::Future<folly::Unit> future_acknowledgeTaskResults(
      std::unique_ptr<std::string> taskId,
      int64_t bufferId,
      int64_t token) override;

  folly::Future<folly::Unit> future_abortTaskResults(
      std::unique_ptr<std::string> taskId,
      int64_t destination) override;

 private:
  std::shared_ptr<velox::memory::MemoryPool> const pool_;
  std::shared_ptr<VeloxPlanValidator> const planValidator_;
  std::shared_ptr<TaskManager> const taskManager_;
};

} // namespace facebook::presto::thrift
