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
#include "presto_cpp/main/thrift/gen-cpp2/PrestoThrift.h"
#include "presto_cpp/main/types/VeloxPlanValidator.h"
#include "velox/common/memory/Memory.h"

namespace facebook::presto::thrift {

class PrestoThriftServiceHandler
    : public facebook::presto::thrift::PrestoThriftSvIf {
 public:
  explicit PrestoThriftServiceHandler(
      velox::memory::MemoryPool* pool,
      VeloxPlanValidator* planValidator,
      TaskManager* taskManager)
      : pool_(pool), planValidator_(planValidator), taskManager_(taskManager) {
    LOG(INFO) << "=== PRESTO THRIFT SERVICE HANDLER CONSTRUCTOR CALLED ===";
  }

  // Thrift service methods
  void fake() override;

  folly::Future<std::unique_ptr<facebook::presto::thrift::TaskInfo>>
  future_createOrUpdateTask(
      std::unique_ptr<std::string> taskId,
      std::unique_ptr<facebook::presto::thrift::TaskUpdateRequest>
          taskUpdateRequest) override;

 private:
  velox::memory::MemoryPool* const pool_;
  VeloxPlanValidator* const planValidator_;
  TaskManager* taskManager_;
};

} // namespace facebook::presto::thrift
