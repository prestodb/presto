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

#include "presto_cpp/main/thrift/server/PrestoThriftServiceHandler.h"
#include "presto_cpp/main/TaskManager.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace facebook::presto::thrift {

folly::Future<std::unique_ptr<::facebook::presto::thrift::TaskResult>>
PrestoThriftServiceHandler::future_getTaskResults(
    std::unique_ptr<std::string> taskId,
    int64_t bufferId,
    int64_t token,
    int64_t maxSizeBytes,
    int64_t maxWaitMicros,
    bool getDataSize) {
  protocol::Duration maxWait(maxWaitMicros, protocol::TimeUnit::MICROSECONDS);
  protocol::DataSize protoMaxSize;
  if (getDataSize) {
    protoMaxSize = protocol::DataSize(0, protocol::DataUnit::BYTE);
  } else {
    protoMaxSize = protocol::DataSize(maxSizeBytes, protocol::DataUnit::BYTE);
  }

  // Create a callback state for HTTP compatibility
  auto callbackState = std::make_shared<http::CallbackRequestHandlerState>();
  return taskManager_
      ->getResults(
          *taskId, bufferId, token, protoMaxSize, maxWait, callbackState)
      .thenValue(
          [callbackState](std::unique_ptr<Result> result)
              -> std::unique_ptr<::facebook::presto::thrift::TaskResult> {
            auto thriftResult =
                std::make_unique<::facebook::presto::thrift::TaskResult>();

            *thriftResult->sequence_ref() = result->sequence;
            *thriftResult->nextSequence_ref() = result->nextSequence;
            *thriftResult->complete_ref() = result->complete;
            if (!result->remainingBytes.empty()) {
              thriftResult->remainingBytes_ref() =
                  std::move(result->remainingBytes);
            }
            if (result->data && result->data->length() > 0) {
              thriftResult->data_ref() = std::move(result->data);
            }

            return thriftResult;
          });
}

folly::Future<folly::Unit>
PrestoThriftServiceHandler::future_acknowledgeTaskResults(
    std::unique_ptr<std::string> taskId,
    int64_t bufferId,
    int64_t token) {
  return folly::makeFutureWith(
      [this, taskId = std::move(taskId), bufferId, token]() mutable {
        taskManager_->acknowledgeResults(*taskId, bufferId, token);
        return folly::unit;
      });
}

folly::Future<folly::Unit> PrestoThriftServiceHandler::future_abortTaskResults(
    std::unique_ptr<std::string> taskId,
    int64_t destination) {
  return folly::makeFutureWith(
      [this, taskId = std::move(taskId), destination]() mutable {
        taskManager_->abortResults(*taskId, destination);
        return folly::unit;
      });
}

} // namespace facebook::presto::thrift
