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
#include "TaskResource.h"
#include <presto_cpp/main/common/Exception.h>
#include "presto_cpp/external/json/json.hpp"
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/main/thrift/ThriftIO.h"
#include "presto_cpp/main/thrift/gen-cpp2/PrestoThrift.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/time/Timer.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::presto {

namespace {

void sendOkResponse(proxygen::ResponseHandler* downstream) {
  proxygen::ResponseBuilder(downstream)
      .status(http::kHttpOk, "OK")
      .sendWithEOM();
}

void sendOkResponse(proxygen::ResponseHandler* downstream, const json& body) {
  proxygen::ResponseBuilder(downstream)
      .status(http::kHttpOk, "OK")
      .header(
          proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationJson)
      .body(body.dump())
      .sendWithEOM();
}

void sendOkThriftResponse(
    proxygen::ResponseHandler* downstream,
    const std::string& body) {
  proxygen::ResponseBuilder(downstream)
      .status(http::kHttpOk, "OK")
      .header(
          proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationThrift)
      .body(body)
      .sendWithEOM();
}

void sendErrorResponse(
    proxygen::ResponseHandler* downstream,
    const std::string& error = "",
    int status = http::kHttpInternalServerError) {
  static const int kMaxStatusSize = 1024;

  proxygen::ResponseBuilder(downstream)
      .status(status, error.substr(0, kMaxStatusSize))
      .body(error)
      .sendWithEOM();
}

void sendTaskNotFound(
    proxygen::ResponseHandler* downstream,
    const protocol::TaskId& taskId) {
  sendErrorResponse(
      downstream,
      fmt::format("Task not found: {}", taskId),
      http::kHttpNotFound);
}

std::optional<protocol::TaskState> getCurrentState(
    proxygen::HTTPMessage* message) {
  auto& headers = message->getHeaders();
  if (!headers.exists(protocol::PRESTO_CURRENT_STATE_HTTP_HEADER)) {
    return std::optional<protocol::TaskState>();
  }
  json taskStateJson =
      headers.getSingleOrEmpty(protocol::PRESTO_CURRENT_STATE_HTTP_HEADER);
  protocol::TaskState currentState;
  from_json(taskStateJson, currentState);
  return currentState;
}

std::optional<protocol::Duration> getMaxWait(proxygen::HTTPMessage* message) {
  auto& headers = message->getHeaders();
  if (!headers.exists(protocol::PRESTO_MAX_WAIT_HTTP_HEADER)) {
    return std::optional<protocol::Duration>();
  }
  return protocol::Duration(
      headers.getSingleOrEmpty(protocol::PRESTO_MAX_WAIT_HTTP_HEADER));
}
} // namespace

void TaskResource::registerUris(http::HttpServer& server) {
  server.registerDelete(
      R"(/v1/task/(.+)/results/(.+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return abortResults(message, pathMatch);
      });

  server.registerGet(
      R"(/v1/task/(.+)/results/([0-9]+)/([0-9]+)/acknowledge)",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return acknowledgeResults(message, pathMatch);
      });

  server.registerPost(
      R"(/v1/task/(.+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return createOrUpdateTask(message, pathMatch);
      });

  server.registerDelete(
      R"(/v1/task/(.+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return deleteTask(message, pathMatch);
      });

  server.registerGet(
      R"(/v1/task/(.+)/status)",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return getTaskStatus(message, pathMatch);
      });

  server.registerGet(
      R"(/v1/task/async/(.+)/results/([0-9]+)/([0-9]+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return getResults(message, pathMatch);
      });

  server.registerGet(
      R"(/v1/task/(.+)/results/([0-9]+)/([0-9]+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return getResults(message, pathMatch);
      });

  server.registerGet(
      R"(/v1/task/(.+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return getTaskInfo(message, pathMatch);
      });

  server.registerGet(
      R"(/v1/task/(.+)/remote-source/(.+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return removeRemoteSource(message, pathMatch);
      });
}

proxygen::RequestHandler* TaskResource::abortResults(
    proxygen::HTTPMessage* /*message*/,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  long bufferId = std::stol(pathMatch[2]);
  return new http::CallbackRequestHandler(
      [this, taskId, bufferId](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        try {
          taskManager_.abortResults(taskId, bufferId);
        } catch (const std::exception& e) {
          sendErrorResponse(downstream, e.what());
          return;
        }
        sendOkResponse(downstream);
      });
}

proxygen::RequestHandler* TaskResource::acknowledgeResults(
    proxygen::HTTPMessage* /*message*/,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  long bufferId = std::stol(pathMatch[2]);
  long token = std::stol(pathMatch[3]);

  return new http::CallbackRequestHandler(
      [this, taskId, bufferId, token](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        try {
          taskManager_.acknowledgeResults(taskId, bufferId, token);
        } catch (const std::exception& e) {
          sendErrorResponse(downstream, e.what());
          return;
        }
        sendOkResponse(downstream);
      });
}

proxygen::RequestHandler* TaskResource::createOrUpdateTask(
    proxygen::HTTPMessage* /*message*/,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];

  return new http::CallbackRequestHandler(
      [this, taskId](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& body,
          proxygen::ResponseHandler* downstream) {
        // TODO Avoid copy
        std::ostringstream oss;
        for (auto& buf : body) {
          oss << std::string((const char*)buf->data(), buf->length());
        }
        std::string updateJson = oss.str();

        std::unique_ptr<protocol::TaskInfo> taskInfo;
        try {
          protocol::TaskUpdateRequest taskUpdateRequest =
              json::parse(updateJson);
          velox::core::PlanFragment planFragment;
          if (taskUpdateRequest.fragment) {
            auto fragment =
                velox::encoding::Base64::decode(*taskUpdateRequest.fragment);
            protocol::PlanFragment prestoPlan = json::parse(fragment);
            VeloxQueryPlanConverter converter(pool_.get());
            planFragment = converter.toVeloxQueryPlan(
                prestoPlan, taskUpdateRequest.tableWriteInfo, taskId);
          }
          const auto& session = taskUpdateRequest.session;
          auto configs = std::unordered_map<std::string, std::string>(
              session.systemProperties.begin(), session.systemProperties.end());

          // If there's a timeZoneKey, convert to timezone name and add to the
          // configs. Throws if timeZoneKey can't be resolved.
          if (session.timeZoneKey != 0) {
            configs.emplace(
                velox::core::QueryConfig::kSessionTimezone,
                velox::util::getTimeZoneName(session.timeZoneKey));
          }

          std::unordered_map<
              std::string,
              std::unordered_map<std::string, std::string>>
              connectorConfigs;
          for (const auto& entry : session.catalogProperties) {
            connectorConfigs.insert(
                {entry.first,
                 std::unordered_map<std::string, std::string>(
                     entry.second.begin(), entry.second.end())});
          }
          taskInfo = taskManager_.createOrUpdateTask(
              taskId,
              std::move(planFragment),
              taskUpdateRequest.sources,
              taskUpdateRequest.outputIds,
              std::move(configs),
              std::move(connectorConfigs));
        } catch (const velox::VeloxException& e) {
          // Creating an empty task, putting errors inside so that next status
          // fetch from coordinator will catch the error and well categorize it.
          taskInfo = taskManager_.createOrUpdateErrorTask(
              taskId, std::current_exception());
        } catch (const std::exception& e) {
          sendErrorResponse(downstream, e.what());
          return;
        }

        json taskInfoJson = *taskInfo;
        sendOkResponse(downstream, taskInfoJson);
      });
}

proxygen::RequestHandler* TaskResource::deleteTask(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  bool abort = false;
  if (message->hasQueryParam(protocol::PRESTO_ABORT_TASK_URL_PARAM)) {
    abort =
        message->getQueryParam(protocol::PRESTO_ABORT_TASK_URL_PARAM) == "true";
  }

  return new http::CallbackRequestHandler(
      [this, taskId, abort](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        std::unique_ptr<protocol::TaskInfo> taskInfo;
        try {
          taskInfo = taskManager_.deleteTask(taskId, abort);
        } catch (const std::exception& e) {
          sendErrorResponse(downstream, e.what());
          return;
        }

        if (!taskInfo) {
          sendTaskNotFound(downstream, taskId);
          return;
        }

        json taskInfoJson = *taskInfo;
        sendOkResponse(downstream, taskInfoJson);
      });
}

proxygen::RequestHandler* TaskResource::getResults(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  long bufferId = std::stol(pathMatch[2]);
  long token = std::stol(pathMatch[3]);

  auto& headers = message->getHeaders();
  auto maxSize = protocol::DataSize(
      headers.exists(protocol::PRESTO_MAX_SIZE_HTTP_HEADER)
          ? headers.getSingleOrEmpty(protocol::PRESTO_MAX_SIZE_HTTP_HEADER)
          : protocol::PRESTO_MAX_SIZE_DEFAULT);
  auto maxWait = getMaxWait(message).value_or(
      protocol::Duration(protocol::PRESTO_MAX_WAIT_DEFAULT));

  return new http::CallbackRequestHandler(
      [this, taskId, bufferId, token, maxSize, maxWait](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        taskManager_
            .getResults(taskId, bufferId, token, maxSize, maxWait, handlerState)
            .via(folly::EventBaseManager::get()->getEventBase())
            .thenValue([downstream, taskId, handlerState](
                           std::unique_ptr<Result> result) {
              if (handlerState->requestExpired()) {
                return;
              }
              auto status = result->data && result->data->length() == 0
                  ? http::kHttpNoContent
                  : http::kHttpOk;
              proxygen::ResponseBuilder(downstream)
                  .status(status, "")
                  .header(
                      proxygen::HTTP_HEADER_CONTENT_TYPE,
                      protocol::PRESTO_PAGES_MIME_TYPE)
                  .header(protocol::PRESTO_TASK_INSTANCE_ID_HEADER, taskId)
                  .header(
                      protocol::PRESTO_PAGE_TOKEN_HEADER,
                      std::to_string(result->sequence))
                  .header(
                      protocol::PRESTO_PAGE_NEXT_TOKEN_HEADER,
                      std::to_string(result->nextSequence))
                  .header(
                      protocol::PRESTO_BUFFER_COMPLETE_HEADER,
                      result->complete ? "true" : "false")
                  .body(std::move(result->data))
                  .sendWithEOM();
            })
            .thenError(
                folly::tag_t<std::exception>{},
                [downstream, handlerState](const std::exception& e) {
                  if (!handlerState->requestExpired()) {
                    sendErrorResponse(downstream, e.what());
                  }
                });
      });
}

proxygen::RequestHandler* TaskResource::getTaskStatus(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  auto currentState = getCurrentState(message);
  auto maxWait = getMaxWait(message);

  auto& headers = message->getHeaders();
  auto acceptHeader = headers.getSingleOrEmpty(proxygen::HTTP_HEADER_ACCEPT);
  auto useThrift =
      acceptHeader.find(http::kMimeTypeApplicationThrift) != std::string::npos;

  return new http::CallbackRequestHandler(
      [this, useThrift, taskId, currentState, maxWait](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        try {
          taskManager_
              .getTaskStatus(taskId, currentState, maxWait, handlerState)
              .via(folly::EventBaseManager::get()->getEventBase())
              .thenValue([useThrift, downstream, taskId, handlerState](
                             std::unique_ptr<protocol::TaskStatus> taskStatus) {
                if (!handlerState->requestExpired()) {
                  if (useThrift) {
                    thrift::TaskStatus thriftTaskStatus;
                    toThrift(*taskStatus, thriftTaskStatus);
                    sendOkThriftResponse(
                        downstream, thriftWrite(thriftTaskStatus));
                  } else {
                    json taskStatusJson = *taskStatus;
                    sendOkResponse(downstream, taskStatusJson);
                  }
                }
              })
              .thenError(
                  folly::tag_t<std::exception>{},
                  [downstream, handlerState](const std::exception& e) {
                    if (!handlerState->requestExpired()) {
                      sendErrorResponse(downstream, e.what());
                    }
                  });
        } catch (const std::exception& e) {
          sendErrorResponse(downstream, e.what());
        }
      });
}

proxygen::RequestHandler* TaskResource::getTaskInfo(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  auto currentState = getCurrentState(message);
  auto maxWait = getMaxWait(message);
  bool summarize = message->hasQueryParam("summarize");

  return new http::CallbackRequestHandler(
      [this, taskId, currentState, maxWait, summarize](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        try {
          taskManager_
              .getTaskInfo(
                  taskId, summarize, currentState, maxWait, handlerState)
              .via(folly::EventBaseManager::get()->getEventBase())
              .thenValue([downstream, taskId, handlerState](
                             std::unique_ptr<protocol::TaskInfo> taskInfo) {
                if (!handlerState->requestExpired()) {
                  json taskInfoJson = *taskInfo;
                  sendOkResponse(downstream, taskInfoJson);
                }
              })
              .thenError(
                  folly::tag_t<std::exception>{},
                  [downstream, handlerState](const std::exception& e) {
                    if (!handlerState->requestExpired()) {
                      sendErrorResponse(downstream, e.what());
                    }
                  });
        } catch (const std::exception& e) {
          sendErrorResponse(downstream, e.what());
          return;
        }
      });
}

proxygen::RequestHandler* TaskResource::removeRemoteSource(
    proxygen::HTTPMessage* /*message*/,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  auto remoteId = pathMatch[2];

  return new http::CallbackRequestHandler(
      [this, taskId, remoteId](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        try {
          taskManager_.removeRemoteSource(taskId, remoteId);
        } catch (const std::exception& e) {
          sendErrorResponse(downstream, e.what());
          return;
        }
        sendOkResponse(downstream);
      });
}
} // namespace facebook::presto
