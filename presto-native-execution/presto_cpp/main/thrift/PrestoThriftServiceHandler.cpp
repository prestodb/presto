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

#include "presto_cpp/main/thrift/PrestoThriftServiceHandler.h"
#include "presto_cpp/main/TaskManager.h"
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/main/thrift/ThriftIO.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace facebook::presto::thrift {

folly::Future<std::unique_ptr<folly::IOBuf>>
PrestoThriftServiceHandler::future_createOrUpdateTaskBinary(
    std::unique_ptr<std::string> taskId,
    std::unique_ptr<folly::IOBuf> serializedTaskUpdateRequest,
    bool summarize) {
  return folly::makeFuture().thenTry([this,
                                      taskId = std::move(taskId),
                                      serializedTaskUpdateRequest = std::move(
                                          serializedTaskUpdateRequest),
                                      summarize](
                                         folly::Try<
                                             folly::Unit>&& /* t */) mutable {
    try {
      LOG(INFO)
          << "Binary thrift future_createOrUpdateTaskBinary() called with taskId: "
          << (taskId ? *taskId : "NULL") << ", serialized data size: "
          << (serializedTaskUpdateRequest ? serializedTaskUpdateRequest->length()
                                          : 0)
          << " bytes, summarize: " << summarize;

      if (!taskId) {
        throw std::invalid_argument("taskId is required");
      }

      if (!serializedTaskUpdateRequest) {
        throw std::invalid_argument("serializedTaskUpdateRequest is required");
      }

      protocol::TaskUpdateRequest updateRequest;
      auto thriftTaskUpdateRequest =
          std::make_shared<thrift::TaskUpdateRequest>();
      apache::thrift::CompactSerializer::deserialize(
        serializedTaskUpdateRequest->coalesce(),
        thriftTaskUpdateRequest);
      fromThrift(*thriftTaskUpdateRequest, updateRequest);

      LOG(INFO) << "Successfully deserialized binary TaskUpdateRequest";

      // Step 3: Execute the same core logic as the regular method
      velox::core::PlanFragment planFragment;
      std::shared_ptr<velox::core::QueryCtx> queryCtx;
      if (updateRequest.fragment) {
        protocol::PlanFragment prestoPlan =
            json::parse(*updateRequest.fragment);

        queryCtx = taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
            *taskId, updateRequest);

        VeloxInteractiveQueryPlanConverter converter(
            queryCtx.get(), pool_.get());
        planFragment = converter.toVeloxQueryPlan(
            prestoPlan, updateRequest.tableWriteInfo, *taskId);
        planValidator_->validatePlanFragment(planFragment);
      }

      long startProcessCpuTime = 0; // Default CPU time

      auto taskInfo = taskManager_->createOrUpdateTask(
          *taskId,
          updateRequest,
          planFragment,
          summarize,
          std::move(queryCtx),
          startProcessCpuTime);

      facebook::presto::thrift::TaskInfo thriftResult;
      // Convert result to Thrift format
      toThrift(*taskInfo, thriftResult);

      // Serialize the result back to binary format
      folly::io::IOBufQueue queue;
      apache::thrift::CompactSerializer::serialize(thriftResult, &queue);
      auto serializedResult = queue.move();
      return serializedResult;
    } catch (const std::exception& e) {
      LOG(ERROR) << "Error in future_createOrUpdateTaskBinary: " << e.what();
      throw;
    }
  });
}

} // namespace facebook::presto::thrift
