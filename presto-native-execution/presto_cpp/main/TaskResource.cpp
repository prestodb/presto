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
#include <fmt/format.h>
#include <folly/json.h>
#include <glog/logging.h>
#include <presto_cpp/main/common/Exception.h>
#include <array>
#include <cctype>
#include <fstream>
#include <mutex>
#include <typeinfo>
#include <unordered_set>
#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/thrift/ProtocolToThrift.h"
#include "presto_cpp/main/thrift/ThriftIO.h"
#include "presto_cpp/main/thrift/gen-cpp2/PrestoThrift.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "velox/common/base/Fs.h"
#include "velox/core/PlanConsistencyChecker.h"

namespace facebook::presto {

namespace {

// Query parameter on DELETE /v1/task/<taskId> with which a client states that
// it will never read from the task again, so the task can be released now
// rather than left for the periodic cleanOldTasks() sweep.
constexpr const char* kDropTaskOnDeleteUrlParam{"dropTaskOnDelete"};

// Returns the 'plan-dump-dir' directory, or nullopt if plan dumping is off.
std::optional<std::string> planDumpDir() {
  auto dir = SystemConfig::instance()->planDumpDir();
  if (!dir.has_value() || dir->empty()) {
    return std::nullopt;
  }
  return dir.value();
}

// Returns a filename stem for 'taskId'. Characters other than letters, digits,
// '_', '-' and '.' are replaced with '_'. Presto task IDs only contain those
// characters, so distinct task IDs map to distinct files.
std::string sanitizeTaskIdForPlanDumpFile(std::string_view taskId) {
  std::string safeId;
  safeId.reserve(taskId.size());
  for (char c : taskId) {
    if (std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-' ||
        c == '.') {
      safeId.push_back(c);
    } else {
      safeId.push_back('_');
    }
  }
  if (safeId.empty() || safeId == "." || safeId == "..") {
    return "task";
  }
  return safeId;
}

// Serializes writes to the same dump file across concurrent task updates. A
// fixed set of mutexes keeps memory bounded however many tasks are dumped.
std::mutex& dumpFileMutex(const std::string& path) {
  static constexpr size_t kNumMutexes = 64;
  static std::array<std::mutex, kNumMutexes> mutexes;
  return mutexes[std::hash<std::string>{}(path) % kNumMutexes];
}

std::string dumpFilePath(
    const std::string& dir,
    const protocol::TaskId& taskId,
    std::string_view suffix) {
  return fmt::format(
      "{}/{}{}", dir, sanitizeTaskIdForPlanDumpFile(taskId), suffix);
}

void writeFile(const std::string& path, const std::string& content) {
  std::ofstream outFile;
  outFile.exceptions(std::ofstream::failbit | std::ofstream::badbit);
  outFile.open(path);
  outFile << content;
  outFile.close();
}

// Reads previously dumped splits from 'path'. Returns an empty object if the
// file does not exist or cannot be parsed.
nlohmann::json readSplitsFile(const std::string& path) {
  std::ifstream inFile(path);
  if (!inFile.good()) {
    return nlohmann::json::object();
  }
  try {
    nlohmann::json existing;
    inFile >> existing;
    if (existing.is_object()) {
      return existing;
    }
    LOG(WARNING) << "Discarding splits dump " << path
                 << ": expected a JSON object";
  } catch (const std::exception& e) {
    LOG(WARNING) << "Discarding unreadable splits dump " << path << ": "
                 << e.what();
  }
  return nlohmann::json::object();
}

// Writes 'planNode' as pretty-printed JSON to '<dir>/<taskId>.json', creating
// 'dir' if needed. Failures are logged and never thrown.
void dumpVeloxPlan(
    const std::string& dir,
    const protocol::TaskId& taskId,
    const velox::core::PlanNodePtr& planNode) {
  const auto path = dumpFilePath(dir, taskId, ".json");
  try {
    const auto content = folly::toPrettyJson(planNode->serialize());
    std::lock_guard<std::mutex> lock(dumpFileMutex(path));
    fs::create_directories(dir);
    writeFile(path, content);
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to dump plan for task " << taskId << " to " << path
                 << ": " << e.what();
  }
}

// Merges the splits in 'sources' into '<dir>/<taskId>.splits.json', a JSON
// object mapping each planNodeId to an array of ScheduledSplits. Splits whose
// sequenceId is already recorded for that planNodeId are skipped, because the
// coordinator re-sends splits until the worker acknowledges them. Failures are
// logged and never thrown.
void dumpSplits(
    const std::string& dir,
    const protocol::TaskId& taskId,
    const std::vector<protocol::TaskSource>& sources) {
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

  const auto path = dumpFilePath(dir, taskId, ".splits.json");
  try {
    std::lock_guard<std::mutex> lock(dumpFileMutex(path));
    fs::create_directories(dir);
    auto existing = readSplitsFile(path);
    for (const auto& source : sources) {
      if (source.splits.empty()) {
        continue;
      }
      auto& nodeSplits = existing[source.planNodeId];
      if (!nodeSplits.is_array()) {
        nodeSplits = nlohmann::json::array();
      }
      std::unordered_set<int64_t> seenSequenceIds;
      for (const auto& split : nodeSplits) {
        if (split.contains("sequenceId")) {
          seenSequenceIds.insert(split["sequenceId"].get<int64_t>());
        }
      }
      for (const auto& split : source.splits) {
        if (!seenSequenceIds.insert(split.sequenceId).second) {
          continue;
        }
        nlohmann::json splitJson;
        protocol::to_json(splitJson, split);
        nodeSplits.push_back(std::move(splitJson));
      }
    }
    writeFile(path, existing.dump(2));
  } catch (const std::exception& e) {
    LOG(WARNING) << "Failed to dump splits for task " << taskId << " to "
                 << path << ": " << e.what();
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
              } catch (const velox::VeloxException& ex) {
                // Log VeloxException before converting to an error task so
                // the failure reason is captured in worker stderr.
                LOG(ERROR) << "createOrUpdateTask VeloxException for taskId="
                           << taskId << " bodyLen=" << requestBody.size()
                           << " what=" << ex.what();
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
              } catch (const std::exception& ex) {
                // Catch non-Velox std::exception (e.g., nlohmann::json
                // deserialization errors) and route through the same
                // error-task path. Without this, such exceptions propagate
                // past the VeloxException catch and proxygen returns HTTP
                // 500 with no log line, making the root cause invisible.
                LOG(ERROR) << "createOrUpdateTask std::exception for taskId="
                           << taskId << " bodyLen=" << requestBody.size()
                           << " type=" << typeid(ex).name()
                           << " what=" << ex.what();
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
                [downstream, handlerState, taskId](auto&& e) {
                  LOG(ERROR) << "Error creating/updating task " << taskId
                             << ": " << e.what();
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
        // Dump before checking the plan so that rejected plans are captured.
        if (const auto dumpDir = planDumpDir()) {
          dumpVeloxPlan(*dumpDir, taskId, planFragment.planNode);
          dumpSplits(*dumpDir, taskId, updateRequest.sources);
        }
        if (SystemConfig::instance()->planConsistencyCheckEnabled()) {
          velox::core::PlanConsistencyChecker::check(planFragment.planNode);
        }

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
        const auto dumpDir = planDumpDir();
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
          // Dump before checking the plan so that rejected plans are captured.
          if (dumpDir) {
            dumpVeloxPlan(*dumpDir, taskId, planFragment.planNode);
          }
          if (SystemConfig::instance()->planConsistencyCheckEnabled()) {
            velox::core::PlanConsistencyChecker::check(planFragment.planNode);
          }
          planValidator_->validatePlanFragment(planFragment);
        }

        // Dump splits on every task update (not only the first one with a
        // fragment) because Presto sends splits in subsequent requests.
        if (dumpDir) {
          dumpSplits(*dumpDir, taskId, updateRequest.sources);
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
  bool dropTaskOnDelete = false;
  if (message->hasQueryParam(kDropTaskOnDeleteUrlParam)) {
    dropTaskOnDelete =
        message->getQueryParam(kDropTaskOnDeleteUrlParam) == "true";
  }
  const auto sendThrift = shouldUseThrift(*message);

  return new http::CallbackRequestHandler(
      [this, taskId, abort, summarize, dropTaskOnDelete, sendThrift](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream,
          std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
        folly::via(
            httpSrvCpuExecutor_,
            [this, taskId, abort, downstream, summarize, dropTaskOnDelete]() {
              std::unique_ptr<protocol::TaskInfo> taskInfo;
              taskInfo = taskManager_.deleteTask(
                  taskId, abort, summarize, dropTaskOnDelete);
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
