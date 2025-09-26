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
  LOG(WARNING) << "Deprecated 'fake' method called.";
}

folly::Future<std::unique_ptr<facebook::presto::thrift::TaskInfo>>
PrestoThriftServiceHandler::future_createOrUpdateTask(
    std::unique_ptr<std::string> taskId,
    std::unique_ptr<facebook::presto::thrift::TaskUpdateRequest>
        taskUpdateRequest) {
  return folly::makeFutureWith([this,
                                taskId = std::move(taskId),
                                taskUpdateRequest =
                                    std::move(taskUpdateRequest)]() mutable {
        const auto startProcessCpuTimeNs = util::getProcessCpuTimeNs();
        std::unique_ptr<protocol::TaskInfo> taskInfo;
        try {
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

          taskInfo = taskManager_->createOrUpdateTask(
              *taskId,
              updateRequest,
              planFragment,
              true,
              std::move(queryCtx),
              startProcessCpuTimeNs);          
        } catch (const velox::VeloxException& e) {
          try {
            taskInfo = taskManager_.createOrUpdateErrorTask(
                taskId,
                std::current_exception(),
                true,
                startProcessCpuTimeNs);
          } catch (const velox::VeloxUserError& e) {
            throw;
          }
        }
        auto result = std::make_unique<facebook::presto::thrift::TaskInfo>();
        toThrift(*taskInfo, *result);

        return result;
      });
}

} // namespace facebook::presto::thrift
