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

#include "presto_cpp/main/PrestoThriftServiceHandler.h"
#include <folly/json.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include "presto_cpp/main/TaskManager.h"
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace facebook::presto::thrift {

void PrestoThriftServiceHandler::fake() {
  // This is a placeholder method (deprecated)
  LOG(ERROR) << "=== FAKE METHOD (DEPRECATED) CALLED - ENTRY ===";
}

folly::Future<std::unique_ptr<facebook::presto::thrift::TaskInfo>>
PrestoThriftServiceHandler::future_createOrUpdateTask(
    std::unique_ptr<std::string> taskId,
    std::unique_ptr<facebook::presto::thrift::TaskUpdateRequest>
        taskUpdateRequest) {
  LOG(ERROR) << "=== FUTURE_CREATE_OR_UPDATE_TASK CALLED - ENTRY ===";

  return folly::makeFuture().thenTry(
      [this,
       taskId = std::move(taskId),
       taskUpdateRequest = std::move(taskUpdateRequest)](
          folly::Try<folly::Unit>&& /* t */) mutable {
        try {
          LOG(ERROR) << "Thrift future_createOrUpdateTask() called with taskId: "
                    << (taskId ? *taskId : "NULL");

          if (!taskId) {
            throw std::invalid_argument("taskId is required");
          }

          if (!taskUpdateRequest) {
            throw std::invalid_argument("taskUpdateRequest is required");
          }

          protocol::TaskUpdateRequest updateRequest;
          fromThrift(*taskUpdateRequest, updateRequest);

          velox::core::PlanFragment planFragment;
          std::shared_ptr<velox::core::QueryCtx> queryCtx;
          if (updateRequest.fragment) {
            protocol::PlanFragment prestoPlan =
                json::parse(*updateRequest.fragment);

            queryCtx =
                taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
                    *taskId, updateRequest);

            VeloxInteractiveQueryPlanConverter converter(queryCtx.get(), pool_);
            planFragment = converter.toVeloxQueryPlan(
                prestoPlan, updateRequest.tableWriteInfo, *taskId);
            planValidator_->validatePlanFragment(planFragment);
          }

          // Set default values for summarize and startProcessCpuTime
          bool summarize = true; // Default to true for Thrift service
          long startProcessCpuTime = 0; // Default CPU time

          auto taskInfo = taskManager_->createOrUpdateTask(
              *taskId,
              updateRequest,
              planFragment,
              summarize,
              std::move(queryCtx),
              startProcessCpuTime);

          // Convert result to Thrift format
          auto result = std::make_unique<facebook::presto::thrift::TaskInfo>();
          toThrift(*taskInfo, *result);

          LOG(INFO)
              << "=== FUTURE_CREATE_OR_UPDATE_TASK COMPLETED SUCCESSFULLY ===";
          return result;
        } catch (const std::exception& e) {
          LOG(ERROR) << "=== FUTURE_CREATE_OR_UPDATE_TASK ERROR: " << e.what()
                     << " ===";
          throw;
        }
      });
}

} // namespace facebook::presto::thrift
