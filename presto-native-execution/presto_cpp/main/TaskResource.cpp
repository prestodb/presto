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
#include "presto_cpp/main/TaskResource.h"
#include <presto_cpp/main/common/Exception.h>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/main/thrift/ThriftIO.h"
#include "presto_cpp/main/thrift/gen-cpp2/PrestoThrift.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"

namespace facebook::presto {

namespace {

void sendTaskNotFound(
    proxygen::ResponseHandler* downstream,
    const protocol::TaskId& taskId) {
  http::sendErrorResponse(
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

  // task/(.+)/batch must come before the /v1/task/(.+) as it's more specific
  // otherwise all requests will be matched with /v1/task/(.+)
  server.registerPost(
      R"(/v1/task/(.+)/batch)",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return createOrUpdateBatchTask(message, pathMatch);
      });

  server.registerPost(
      R"(/v1/task/(.+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return createOrUpdateTask(message, pathMatch);
      });

  server.registerDelete(
      R"(/v1/task/(.+)/remote-source/(.+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return removeRemoteSource(message, pathMatch);
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

  server.registerHead(
      R"(/v1/task/(.+)/results/([0-9]+)/([0-9]+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return getResults(message, pathMatch, true);
      });

  server.registerGet(
      R"(/v1/task/(.+)/results/([0-9]+)/([0-9]+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return getResults(message, pathMatch, false);
      });

  server.registerGet(
      R"(/v1/task/(.+))",
      [&](proxygen::HTTPMessage* message,
          const std::vector<std::string>& pathMatch) {
        return getTaskInfo(message, pathMatch);
      });
}

proxygen::RequestHandler* TaskResource::abortResults(
    proxygen::HTTPMessage* /*message*/,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  long destination = folly::to<long>(pathMatch[2]);
  return new http::CallbackRequestHandler(
      [this, taskId, destination](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        folly::via(
            httpSrvCpuExecutor_,
            [this, taskId, destination, handlerState]() {
              taskManager_.abortResults(taskId, destination);
              return true;
            })
            .via(folly::EventBaseManager::get()->getEventBase())
            .thenValue([downstream, handlerState](auto&& /* unused */) {
              if (!handlerState->requestExpired()) {
                http::sendOkResponse(downstream);
              }
            })
            .thenError(
                folly::tag_t<std::exception>{},
                [downstream, handlerState](auto&& e) {
                  if (!handlerState->requestExpired()) {
                    http::sendErrorResponse(downstream, e.what());
                  }
                });
      });
}

proxygen::RequestHandler* TaskResource::acknowledgeResults(
    proxygen::HTTPMessage* /*message*/,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  long bufferId = folly::to<long>(pathMatch[2]);
  long token = folly::to<long>(pathMatch[3]);

  return new http::CallbackRequestHandler(
      [this, taskId, bufferId, token](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        folly::via(
            httpSrvCpuExecutor_,
            [this, taskId, bufferId, token]() {
              taskManager_.acknowledgeResults(taskId, bufferId, token);
              return true;
            })
            .via(folly::EventBaseManager::get()->getEventBase())
            .thenValue([downstream, handlerState](auto&& /* unused */) {
              if (!handlerState->requestExpired()) {
                http::sendOkResponse(downstream);
              }
            })
            .thenError(
                folly::tag_t<velox::VeloxException>{},
                [downstream](auto&& e) {
                  http::sendErrorResponse(downstream, e.what());
                })
            .thenError(
                folly::tag_t<std::exception>{},
                [downstream, handlerState](auto&& e) {
                  if (!handlerState->requestExpired()) {
                    http::sendErrorResponse(downstream, e.what());
                  }
                });
      });
}

proxygen::RequestHandler* TaskResource::createOrUpdateTaskImpl(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& pathMatch,
    const std::function<std::unique_ptr<protocol::TaskInfo>(
        const protocol::TaskId& taskId,
        const std::string& requestBody,
        const bool summarize,
        long startProcessCpuTime,
        bool receiveThrift)>& createOrUpdateFunc) {
  protocol::TaskId taskId = pathMatch[1];
  bool summarize = message->hasQueryParam("summarize");

  const auto& headers = message->getHeaders();
  const auto& acceptHeader = headers.getSingleOrEmpty(proxygen::HTTP_HEADER_ACCEPT);
  const auto sendThrift =
      acceptHeader.find(http::kMimeTypeApplicationThrift) != std::string::npos;
  const auto& contentHeader = headers.getSingleOrEmpty(proxygen::HTTP_HEADER_CONTENT_TYPE);
  const auto receiveThrift =
      contentHeader.find(http::kMimeTypeApplicationThrift) != std::string::npos;

  return new http::CallbackRequestHandler(
      [this, taskId, summarize, createOrUpdateFunc, sendThrift, receiveThrift](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& body,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        folly::via(
            httpSrvCpuExecutor_,
            [this,
             requestBody = util::extractMessageBody(body),
             taskId,
             summarize,
             createOrUpdateFunc,
             receiveThrift]() {
              const auto startProcessCpuTimeNs = util::getProcessCpuTimeNs();

              std::unique_ptr<protocol::TaskInfo> taskInfo;
              try {
                taskInfo = createOrUpdateFunc(
                    taskId, requestBody, summarize, startProcessCpuTimeNs, receiveThrift);
              } catch (const velox::VeloxException& e) {
                // Creating an empty task, putting errors inside so that next
                // status fetch from coordinator will catch the error and well
                // categorize it.
                try {
                  taskInfo = taskManager_.createOrUpdateErrorTask(
                      taskId,
                      std::current_exception(),
                      summarize,
                      startProcessCpuTimeNs);
                } catch (const velox::VeloxUserError& e) {
                  throw;
                }
              }
              return taskInfo;
            })
            .via(folly::EventBaseManager::get()->getEventBase())
            .thenValue([downstream, handlerState, sendThrift](auto taskInfo) {
              if (!handlerState->requestExpired()) {
                if (sendThrift) {
                  thrift::TaskInfo thriftTaskInfo;
                  toThrift(*taskInfo, thriftTaskInfo);
                  http::sendOkThriftResponse(
                      downstream, thriftWrite(thriftTaskInfo));
                } else {
                  http::sendOkResponse(downstream, json(*taskInfo));
                }
              }
            })
            .thenError(
                folly::tag_t<velox::VeloxException>{},
                [downstream, handlerState](auto&& e) {
                  if (!handlerState->requestExpired()) {
                    http::sendErrorResponse(downstream, e.what());
                  }
                })
            .thenError(
                folly::tag_t<std::exception>{},
                [downstream, handlerState](auto&& e) {
                  if (!handlerState->requestExpired()) {
                    http::sendErrorResponse(downstream, e.what());
                  }
                });
      });
}

proxygen::RequestHandler* TaskResource::createOrUpdateBatchTask(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& pathMatch) {
  return createOrUpdateTaskImpl(
      message,
      pathMatch,
      [&](const protocol::TaskId& taskId,
          const std::string& requestBody,
          const bool summarize,
          long startProcessCpuTime,
          bool /*receiveThrift*/) {
        protocol::BatchTaskUpdateRequest batchUpdateRequest =
            json::parse(requestBody);
        auto updateRequest = batchUpdateRequest.taskUpdateRequest;
        VELOX_USER_CHECK_NOT_NULL(updateRequest.fragment);

        auto fragment =
            velox::encoding::Base64::decode(*updateRequest.fragment);
        protocol::PlanFragment prestoPlan = json::parse(fragment);

        auto serializedShuffleWriteInfo = batchUpdateRequest.shuffleWriteInfo;
        auto broadcastBasePath = batchUpdateRequest.broadcastBasePath;
        auto shuffleName = SystemConfig::instance()->shuffleName();
        if (serializedShuffleWriteInfo) {
          VELOX_USER_CHECK(
              !shuffleName.empty(),
              "Shuffle name not provided from 'shuffle.name' property in "
              "config.properties");
        }

        auto queryCtx =
            taskManager_.getQueryContextManager()->findOrCreateQueryCtx(
                taskId, updateRequest);

        VeloxBatchQueryPlanConverter converter(
            shuffleName,
            std::move(serializedShuffleWriteInfo),
            std::move(broadcastBasePath),
            queryCtx.get(),
            pool_);
        auto planFragment = converter.toVeloxQueryPlan(
            prestoPlan, updateRequest.tableWriteInfo, taskId);

        return taskManager_.createOrUpdateBatchTask(
            taskId,
            batchUpdateRequest,
            planFragment,
            summarize,
            std::move(queryCtx),
            startProcessCpuTime);
      });
}

proxygen::RequestHandler* TaskResource::createOrUpdateTask(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& pathMatch) {
  return createOrUpdateTaskImpl(
      message,
      pathMatch,
      [&](const protocol::TaskId& taskId,
          const std::string& requestBody,
          const bool summarize,
          long startProcessCpuTime,
          bool receiveThrift) {
        protocol::TaskUpdateRequest updateRequest;
        if (receiveThrift) {
          auto thriftTaskUpdateRequest = std::make_shared<thrift::TaskUpdateRequest>();
          thriftRead(requestBody, thriftTaskUpdateRequest);
          fromThrift(*thriftTaskUpdateRequest, updateRequest);
        } else {
          updateRequest = json::parse(requestBody);
        }
        velox::core::PlanFragment planFragment;
        std::shared_ptr<velox::core::QueryCtx> queryCtx;
        if (updateRequest.fragment) {
          protocol::PlanFragment prestoPlan = json::parse(receiveThrift ? *updateRequest.fragment : velox::encoding::Base64::decode(*updateRequest.fragment));

          queryCtx =
              taskManager_.getQueryContextManager()->findOrCreateQueryCtx(
                  taskId, updateRequest);

          VeloxInteractiveQueryPlanConverter converter(queryCtx.get(), pool_);
          planFragment = converter.toVeloxQueryPlan(
              prestoPlan, updateRequest.tableWriteInfo, taskId);
          planValidator_->validatePlanFragment(planFragment);
        }

        return taskManager_.createOrUpdateTask(
            taskId,
            updateRequest,
            planFragment,
            summarize,
            std::move(queryCtx),
            startProcessCpuTime);
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
  bool summarize = message->hasQueryParam("summarize");
  const auto& headers = message->getHeaders();
  const auto& acceptHeader = headers.getSingleOrEmpty(proxygen::HTTP_HEADER_ACCEPT);
  const auto sendThrift =
      acceptHeader.find(http::kMimeTypeApplicationThrift) != std::string::npos;

  return new http::CallbackRequestHandler(
      [this, taskId, abort, summarize, sendThrift](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        folly::via(
            httpSrvCpuExecutor_,
            [this, taskId, abort, downstream, summarize]() {
              std::unique_ptr<protocol::TaskInfo> taskInfo;
              taskInfo = taskManager_.deleteTask(taskId, abort, summarize);
              return std::move(taskInfo);
            })
            .via(folly::EventBaseManager::get()->getEventBase())
            .thenValue([taskId, downstream, handlerState, sendThrift](auto&& taskInfo) {
              if (!handlerState->requestExpired()) {
                if (taskInfo == nullptr) {
                  sendTaskNotFound(downstream, taskId);
                  return;
                }
                if (sendThrift) {
                  thrift::TaskInfo thriftTaskInfo;
                  toThrift(*taskInfo, thriftTaskInfo);
                  http::sendOkThriftResponse(
                      downstream, thriftWrite(thriftTaskInfo));
                } else {
                  http::sendOkResponse(downstream, json(*taskInfo));
                }
              }
            })
            .thenError(
                folly::tag_t<velox::VeloxException>{},
                [downstream, handlerState](auto&& e) {
                  if (!handlerState->requestExpired()) {
                    http::sendErrorResponse(downstream, e.what());
                  }
                })
            .thenError(
                folly::tag_t<std::exception>{},
                [downstream, handlerState](auto&& e) {
                  if (!handlerState->requestExpired()) {
                    http::sendErrorResponse(downstream, e.what());
                  }
                });
      });
}

proxygen::RequestHandler* TaskResource::getResults(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& pathMatch,
    bool getDataSize) {
  protocol::TaskId taskId = pathMatch[1];
  long bufferId = folly::to<long>(pathMatch[2]);
  long token = folly::to<long>(pathMatch[3]);

  auto& headers = message->getHeaders();
  auto maxWait = getMaxWait(message).value_or(
      protocol::Duration(protocol::PRESTO_MAX_WAIT_DEFAULT));
  protocol::DataSize maxSize;
  if (getDataSize) {
    maxSize = protocol::DataSize(0, protocol::DataUnit::BYTE);
  } else {
    maxSize = protocol::DataSize(
        headers.exists(protocol::PRESTO_MAX_SIZE_HTTP_HEADER)
            ? headers.getSingleOrEmpty(protocol::PRESTO_MAX_SIZE_HTTP_HEADER)
            : protocol::PRESTO_MAX_SIZE_DEFAULT);
  }

  return new http::CallbackRequestHandler(
      [this, taskId, bufferId, token, maxSize, maxWait](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        auto evb = folly::EventBaseManager::get()->getEventBase();
        folly::via(
            httpSrvCpuExecutor_,
            [this,
             evb,
             taskId,
             bufferId,
             token,
             maxSize,
             maxWait,
             downstream,
             handlerState]() {
              taskManager_
                  .getResults(
                      taskId, bufferId, token, maxSize, maxWait, handlerState)
                  .via(evb)
                  .thenValue([downstream, taskId, handlerState](
                                 std::unique_ptr<Result> result) {
                    if (handlerState->requestExpired()) {
                      return;
                    }
                    auto status = result->data && result->data->length() == 0
                        ? http::kHttpNoContent
                        : http::kHttpOk;

                    proxygen::ResponseBuilder builder(downstream);
                    builder.status(status, "")
                        .header(
                            proxygen::HTTP_HEADER_CONTENT_TYPE,
                            protocol::PRESTO_PAGES_MIME_TYPE)
                        .header(
                            protocol::PRESTO_TASK_INSTANCE_ID_HEADER, taskId)
                        .header(
                            protocol::PRESTO_PAGE_TOKEN_HEADER,
                            std::to_string(result->sequence))
                        .header(
                            protocol::PRESTO_PAGE_NEXT_TOKEN_HEADER,
                            std::to_string(result->nextSequence))
                        .header(
                            protocol::PRESTO_BUFFER_COMPLETE_HEADER,
                            result->complete ? "true" : "false");
                    if (!result->remainingBytes.empty()) {
                      builder.header(
                          protocol::PRESTO_BUFFER_REMAINING_BYTES_HEADER,
                          folly::join(',', result->remainingBytes));
                    }
                    builder.body(std::move(result->data)).sendWithEOM();
                  })
                  .thenError(
                      folly::tag_t<velox::VeloxException>{},
                      [downstream,
                       handlerState](const velox::VeloxException& e) {
                        if (!handlerState->requestExpired()) {
                          http::sendErrorResponse(downstream, e.what());
                        }
                      })
                  .thenError(
                      folly::tag_t<std::exception>{},
                      [downstream, handlerState](const std::exception& e) {
                        if (!handlerState->requestExpired()) {
                          http::sendErrorResponse(downstream, e.what());
                        }
                      });
            });
      });
}

proxygen::RequestHandler* TaskResource::getTaskStatus(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  auto currentState = getCurrentState(message);
  auto maxWait = getMaxWait(message);

  const auto& headers = message->getHeaders();
  const auto& acceptHeader = headers.getSingleOrEmpty(proxygen::HTTP_HEADER_ACCEPT);
  const auto sendThrift =
      acceptHeader.find(http::kMimeTypeApplicationThrift) != std::string::npos;

  return new http::CallbackRequestHandler(
      [this, sendThrift, taskId, currentState, maxWait](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        auto evb = folly::EventBaseManager::get()->getEventBase();
        folly::via(
            httpSrvCpuExecutor_,
            [this,
             evb,
             sendThrift,
             taskId,
             currentState,
             maxWait,
             handlerState,
             downstream]() {
              taskManager_
                  .getTaskStatus(taskId, currentState, maxWait, handlerState)
                  .via(evb)
                  .thenValue(
                      [sendThrift, downstream, taskId, handlerState](
                          std::unique_ptr<protocol::TaskStatus> taskStatus) {
                        if (!handlerState->requestExpired()) {
                          if (sendThrift) {
                            thrift::TaskStatus thriftTaskStatus;
                            toThrift(*taskStatus, thriftTaskStatus);
                            http::sendOkThriftResponse(
                                downstream, thriftWrite(thriftTaskStatus));
                          } else {
                            json taskStatusJson = *taskStatus;
                            http::sendOkResponse(downstream, taskStatusJson);
                          }
                        }
                      })
                  .thenError(
                      folly::tag_t<velox::VeloxException>{},
                      [downstream,
                       handlerState](const velox::VeloxException& e) {
                        if (!handlerState->requestExpired()) {
                          http::sendErrorResponse(downstream, e.what());
                        }
                      })
                  .thenError(
                      folly::tag_t<std::exception>{},
                      [downstream, handlerState](const std::exception& e) {
                        if (!handlerState->requestExpired()) {
                          http::sendErrorResponse(downstream, e.what());
                        }
                      });
            })
            .via(folly::EventBaseManager::get()->getEventBase())
            .thenError(folly::tag_t<std::exception>{}, [downstream](auto&& e) {
              http::sendErrorResponse(downstream, e.what());
            });
      });
}

proxygen::RequestHandler* TaskResource::getTaskInfo(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  auto currentState = getCurrentState(message);
  auto maxWait = getMaxWait(message);
  bool summarize = message->hasQueryParam("summarize");

  const auto& headers = message->getHeaders();
  const auto& acceptHeader = headers.getSingleOrEmpty(proxygen::HTTP_HEADER_ACCEPT);
  const auto sendThrift =
      acceptHeader.find(http::kMimeTypeApplicationThrift) != std::string::npos;

  return new http::CallbackRequestHandler(
      [this, taskId, currentState, maxWait, summarize, sendThrift](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        folly::via(
            httpSrvCpuExecutor_,
            [this,
             evb = folly::EventBaseManager::get()->getEventBase(),
             taskId,
             currentState,
             maxWait,
             summarize,
             handlerState,
             downstream,
             sendThrift]() {
              taskManager_
                  .getTaskInfo(
                      taskId, summarize, currentState, maxWait, handlerState)
                  .via(evb)
                  .thenValue([downstream, taskId, handlerState, sendThrift](
                                 std::unique_ptr<protocol::TaskInfo> taskInfo) {
                    if (!handlerState->requestExpired()) {
                      if (sendThrift) {
                        thrift::TaskInfo thriftTaskInfo;
                        toThrift(*taskInfo, thriftTaskInfo);
                        http::sendOkThriftResponse(
                            downstream, thriftWrite(thriftTaskInfo));
                      } else {
                        http::sendOkResponse(downstream, json(*taskInfo));
                      }
                    }
                  })
                  .thenError(
                      folly::tag_t<velox::VeloxException>{},
                      [downstream,
                       handlerState](const velox::VeloxException& e) {
                        if (!handlerState->requestExpired()) {
                          http::sendErrorResponse(downstream, e.what());
                        }
                      })
                  .thenError(
                      folly::tag_t<std::exception>{},
                      [downstream, handlerState](const std::exception& e) {
                        if (!handlerState->requestExpired()) {
                          http::sendErrorResponse(downstream, e.what());
                        }
                      });
            })
            .thenError(folly::tag_t<std::exception>{}, [downstream](auto&& e) {
              http::sendErrorResponse(downstream, e.what());
            });
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
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        folly::via(
            httpSrvCpuExecutor_,
            [this, taskId, remoteId, downstream]() {
              taskManager_.removeRemoteSource(taskId, remoteId);
            })
            .via(folly::EventBaseManager::get()->getEventBase())
            .thenValue([downstream, handlerState](auto&& /* unused */) {
              if (!handlerState->requestExpired()) {
                http::sendOkResponse(downstream);
              }
            })
            .thenError(
                folly::tag_t<velox::VeloxException>{},
                [downstream, handlerState](const velox::VeloxException& e) {
                  if (!handlerState->requestExpired()) {
                    http::sendErrorResponse(downstream, e.what());
                  }
                })
            .thenError(
                folly::tag_t<std::exception>{},
                [downstream, handlerState](const std::exception& e) {
                  if (!handlerState->requestExpired()) {
                    http::sendErrorResponse(downstream, e.what());
                  }
                });
      });
}
} // namespace facebook::presto
