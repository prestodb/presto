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
#include <folly/json.h>
#include <presto_cpp/main/common/Exception.h>
#include <fstream>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/main/thrift/ThriftIO.h"
#include "presto_cpp/main/thrift/gen-cpp2/PrestoThrift.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/core/PlanConsistencyChecker.h"
#include "velox/core/PlanNode.h"
#if __has_include("filesystem")
#include <filesystem>
#else
#include <experimental/filesystem>
#endif

namespace facebook::presto {

namespace {

// Sanitize taskId into a filesystem-safe string.
std::string sanitizeTaskId(const protocol::TaskId& taskId) {
  std::string safeId;
  safeId.reserve(taskId.size());
  for (char c : taskId) {
    if (std::isalnum(static_cast<unsigned char>(c)) || c == '_') {
      safeId.push_back(c);
    } else if (c == '.' || c == ':' || c == '/') {
      safeId.push_back('_');
    }
  }
  return safeId.empty() ? std::string("task") : safeId;
}

void maybeDumpVeloxPlan(
    const protocol::TaskId& taskId,
    const velox::core::PlanNodePtr& planNode) {
  auto dirOpt = SystemConfig::instance()->planDumpDir();
  if (!dirOpt.has_value() || dirOpt->empty()) {
    return;
  }
  const std::string& dir = dirOpt.value();
  const std::string safeId = sanitizeTaskId(taskId);
  const std::string path = dir + "/" + safeId + ".json";
  try {
#if __has_include("filesystem")
    std::filesystem::create_directories(dir);
#else
    std::experimental::filesystem::create_directories(dir);
#endif
    folly::dynamic json = planNode->serialize();
    std::ofstream outFile(path);
    outFile << folly::toPrettyJson(json);
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to dump plan to " << path << ": " << e.what();
  }
}

// Dump splits (sources) for a task. Called on every task update (not only the
// first one that has the plan fragment) because Presto sends splits in multiple
// batches. Accumulates: reads existing file, merges new splits, writes back.
void maybeDumpSplits(
    const protocol::TaskId& taskId,
    const std::vector<protocol::TaskSource>& sources) {
  auto dirOpt = SystemConfig::instance()->planDumpDir();
  if (!dirOpt.has_value() || dirOpt->empty()) {
    return;
  }
  // Only dump sources that actually contain splits.
  bool hasSplits = false;
  for (const auto& source : sources) {
    if (!source.splits.empty()) {
      hasSplits = true;
      break;
    }
  }
  if (!hasSplits) {
    return;
  }
  const std::string& dir = dirOpt.value();
  const std::string safeId = sanitizeTaskId(taskId);
  const std::string path = dir + "/" + safeId + ".splits.json";
  try {
#if __has_include("filesystem")
    std::filesystem::create_directories(dir);
#else
    std::experimental::filesystem::create_directories(dir);
#endif
    // Read existing accumulated splits (if any) so we can merge.
    nlohmann::json existing = nlohmann::json::object();
    {
      std::ifstream inFile(path);
      if (inFile.good()) {
        try {
          inFile >> existing;
        } catch (...) {
          existing = nlohmann::json::object();
        }
      }
    }
    // Merge new splits into existing, grouped by planNodeId.
    for (const auto& source : sources) {
      for (const auto& split : source.splits) {
        nlohmann::json sjson;
        protocol::to_json(sjson, split);
        // Append to the array for this planNodeId.
        if (!existing.contains(source.planNodeId) ||
            !existing[source.planNodeId].is_array()) {
          existing[source.planNodeId] = nlohmann::json::array();
        }
        existing[source.planNodeId].push_back(sjson);
      }
    }
    std::ofstream outFile(path);
    outFile << existing.dump(2);
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to dump splits to " << path << ": " << e.what();
  }
}

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

bool shouldUseThrift(const proxygen::HTTPMessage& message) {
  const auto& acceptHeader =
      message.getHeaders().getSingleOrEmpty(proxygen::HTTP_HEADER_ACCEPT);
  return acceptHeader.find(http::kMimeTypeApplicationThrift) !=
      std::string::npos;
}

template <typename T, typename ThriftT>
void sendPrestoResponse(
    proxygen::ResponseHandler* downstream,
    const T& data,
    bool sendThrift) {
  if (sendThrift) {
    ThriftT thriftData;
    toThrift(data, thriftData);
    http::sendOkThriftResponse(downstream, thriftWrite(thriftData));
  } else {
    http::sendOkResponse(downstream, json(data));
  }
}

/// Creates a CallbackRequestHandler that executes a void work function on the
/// given executor, then sends an empty OK response. On exception, sends an
/// error response. Used for simple fire-and-forget handlers.
template <typename WorkFn>
proxygen::RequestHandler* executeAndRespond(
    folly::Executor* executor,
    WorkFn&& workFn) {
  return new http::CallbackRequestHandler(
      [executor, work = std::forward<WorkFn>(workFn)](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        folly::via(executor, std::move(work))
            .via(
                folly::getKeepAliveToken(
                    folly::EventBaseManager::get()->getEventBase()))
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
  return executeAndRespond(httpSrvCpuExecutor_, [this, taskId, destination]() {
    taskManager_.abortResults(taskId, destination);
  });
}

proxygen::RequestHandler* TaskResource::acknowledgeResults(
    proxygen::HTTPMessage* /*message*/,
    const std::vector<std::string>& pathMatch) {
  protocol::TaskId taskId = pathMatch[1];
  long bufferId = folly::to<long>(pathMatch[2]);
  long token = folly::to<long>(pathMatch[3]);
  return executeAndRespond(
      httpSrvCpuExecutor_, [this, taskId, bufferId, token]() {
        taskManager_.acknowledgeResults(taskId, bufferId, token);
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
  const auto sendThrift = shouldUseThrift(*message);
  const auto& contentHeader =
      headers.getSingleOrEmpty(proxygen::HTTP_HEADER_CONTENT_TYPE);
  const auto receiveThrift =
      contentHeader.find(http::kMimeTypeApplicationThrift) != std::string::npos;
  const auto contentEncoding = headers.getSingleOrEmpty("Content-Encoding");
  const auto isCompressed =
      !contentEncoding.empty() && contentEncoding != "identity";

  return new http::CallbackRequestHandler(
      [this,
       taskId,
       summarize,
       createOrUpdateFunc,
       sendThrift,
       receiveThrift,
       contentEncoding,
       isCompressed](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& body,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        folly::via(
            httpSrvCpuExecutor_,
            [this,
             requestBody = isCompressed
                 ? util::decompressMessageBody(body, contentEncoding)
                 : util::extractMessageBody(body),
             taskId,
             summarize,
             createOrUpdateFunc,
             receiveThrift]() {
              const auto startProcessCpuTimeNs = util::getProcessCpuTimeNs();

              std::unique_ptr<protocol::TaskInfo> taskInfo;
              try {
                taskInfo = createOrUpdateFunc(
                    taskId,
                    requestBody,
                    summarize,
                    startProcessCpuTimeNs,
                    receiveThrift);
              } catch (const velox::VeloxException&) {
                // Creating an empty task, putting errors inside so that next
                // status fetch from coordinator will catch the error and well
                // categorize it.
                try {
                  taskInfo = taskManager_.createOrUpdateErrorTask(
                      taskId,
                      std::current_exception(),
                      summarize,
                      startProcessCpuTimeNs);
                } catch (const velox::VeloxUserError&) {
                  throw;
                }
              }
              return taskInfo;
            })
            .via(
                folly::getKeepAliveToken(
                    folly::EventBaseManager::get()->getEventBase()))
            .thenValue([downstream, handlerState, sendThrift](auto taskInfo) {
              if (!handlerState->requestExpired()) {
                sendPrestoResponse<protocol::TaskInfo, thrift::TaskInfo>(
                    downstream, *taskInfo, sendThrift);
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
            taskManager_.getQueryContextManager()->findOrCreateBatchQueryCtx(
                taskId, updateRequest);

        VeloxBatchQueryPlanConverter converter(
            shuffleName,
            std::move(serializedShuffleWriteInfo),
            std::move(broadcastBasePath),
            queryCtx.get(),
            pool_);
        auto planFragment = converter.toVeloxQueryPlan(
            prestoPlan, updateRequest.tableWriteInfo, taskId);
        if (SystemConfig::instance()->planConsistencyCheckEnabled()) {
          velox::core::PlanConsistencyChecker::check(planFragment.planNode);
        }
        maybeDumpVeloxPlan(taskId, planFragment.planNode);
        maybeDumpSplits(taskId, updateRequest.sources);

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
          auto thriftTaskUpdateRequest =
              std::make_shared<thrift::TaskUpdateRequest>();
          thriftRead(requestBody, thriftTaskUpdateRequest);
          fromThrift(*thriftTaskUpdateRequest, updateRequest);
        } else {
          updateRequest = json::parse(requestBody);
        }
        velox::core::PlanFragment planFragment;
        std::shared_ptr<velox::core::QueryCtx> queryCtx;
        if (updateRequest.fragment) {
          protocol::PlanFragment prestoPlan = json::parse(
              receiveThrift
                  ? *updateRequest.fragment
                  : velox::encoding::Base64::decode(*updateRequest.fragment));

          queryCtx =
              taskManager_.getQueryContextManager()->findOrCreateQueryCtx(
                  taskId, updateRequest);

          VeloxInteractiveQueryPlanConverter converter(queryCtx.get(), pool_);
          planFragment = converter.toVeloxQueryPlan(
              prestoPlan, updateRequest.tableWriteInfo, taskId);
          if (SystemConfig::instance()->planConsistencyCheckEnabled()) {
            velox::core::PlanConsistencyChecker::check(planFragment.planNode);
          }
          planValidator_->validatePlanFragment(planFragment);
          maybeDumpVeloxPlan(taskId, planFragment.planNode);
        }

        // Dump splits on every task update (not only the first one with a
        // fragment) because Presto sends splits in subsequent requests.
        maybeDumpSplits(taskId, updateRequest.sources);

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
  const auto sendThrift = shouldUseThrift(*message);

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
            .via(
                folly::getKeepAliveToken(
                    folly::EventBaseManager::get()->getEventBase()))
            .thenValue([taskId, downstream, handlerState, sendThrift](
                           auto&& taskInfo) {
              if (!handlerState->requestExpired()) {
                if (taskInfo == nullptr) {
                  sendTaskNotFound(downstream, taskId);
                  return;
                }
                sendPrestoResponse<protocol::TaskInfo, thrift::TaskInfo>(
                    downstream, *taskInfo, sendThrift);
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
        folly::via(
            httpSrvCpuExecutor_,
            [this,
             evb = folly::getKeepAliveToken(
                 folly::EventBaseManager::get()->getEventBase()),
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
                    if (result->waitTimeMs > 0) {
                      builder.header(
                          protocol::PRESTO_BUFFER_WAIT_TIME_MS_HEADER,
                          std::to_string(result->waitTimeMs));
                    }
                    builder.body(std::move(result->data)).sendWithEOM();
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
  const auto sendThrift = shouldUseThrift(*message);

  return new http::CallbackRequestHandler(
      [this, sendThrift, taskId, currentState, maxWait](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        folly::via(
            httpSrvCpuExecutor_,
            [this,
             evb = folly::getKeepAliveToken(
                 folly::EventBaseManager::get()->getEventBase()),
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
                          sendPrestoResponse<
                              protocol::TaskStatus,
                              thrift::TaskStatus>(
                              downstream, *taskStatus, sendThrift);
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
  const auto sendThrift = shouldUseThrift(*message);

  return new http::CallbackRequestHandler(
      [this, taskId, currentState, maxWait, summarize, sendThrift](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        folly::via(
            httpSrvCpuExecutor_,
            [this,
             evb = folly::getKeepAliveToken(
                 folly::EventBaseManager::get()->getEventBase()),
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
                      sendPrestoResponse<protocol::TaskInfo, thrift::TaskInfo>(
                          downstream, *taskInfo, sendThrift);
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
  return executeAndRespond(httpSrvCpuExecutor_, [this, taskId, remoteId]() {
    taskManager_.removeRemoteSource(taskId, remoteId);
  });
}
} // namespace facebook::presto
