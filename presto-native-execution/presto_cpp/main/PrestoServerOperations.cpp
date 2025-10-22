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
#include "presto_cpp/main/PrestoServerOperations.h"
#include <velox/common/base/Exceptions.h>
#include <velox/common/base/VeloxException.h>
#include <velox/common/caching/AsyncDataCache.h>
#include <velox/common/caching/SsdCache.h>
#include <velox/common/process/TraceContext.h>
#include "presto_cpp/main/PrestoServer.h"
#include "presto_cpp/main/ServerOperation.h"
#include "velox/connectors/hive/HiveConnector.h"

namespace facebook::presto {

namespace {

std::string unsupportedAction(const ServerOperation& op) {
  VELOX_USER_FAIL(
      "Target '{}' does not support action '{}'",
      ServerOperation::targetString(op.target),
      ServerOperation::actionString(op.action));
}

std::string clearConnectorCache(proxygen::HTTPMessage* message) {
  const auto name = message->getQueryParam("name");
  const auto id = message->getQueryParam("id");
  if (name == "hive") {
    // ======== HiveConnector Operations ========
    auto hiveConnector =
        std::dynamic_pointer_cast<velox::connector::hive::HiveConnector>(
            velox::connector::getConnector(id));
    VELOX_USER_CHECK_NOT_NULL(
        hiveConnector,
        "No '{}' connector found for connector id '{}'",
        name,
        id);
    return hiveConnector->clearFileHandleCache().toString();
  }
  VELOX_USER_FAIL("connector '{}' operation is not supported", name);
}

std::string getConnectorCacheStats(proxygen::HTTPMessage* message) {
  const auto name = message->getQueryParam("name");
  const auto id = message->getQueryParam("id");
  if (name == "hive") {
    // ======== HiveConnector Operations ========
    auto hiveConnector =
        std::dynamic_pointer_cast<velox::connector::hive::HiveConnector>(
            velox::connector::getConnector(id));
    VELOX_USER_CHECK_NOT_NULL(
        hiveConnector,
        "No '{}' connector found for connector id '{}'",
        name,
        id);
    return hiveConnector->fileHandleCacheStats().toString();
  }
  VELOX_USER_FAIL("connector '{}' operation is not supported", name);
}

std::string prettyJson(folly::dynamic const& dyn) {
  folly::json::serialization_opts opts;
  opts.pretty_formatting = true;
  opts.sort_keys = true;
  opts.convert_int_keys = true;
  return folly::json::serialize(dyn, opts);
}

} // namespace

void PrestoServerOperations::runOperation(
    proxygen::HTTPMessage* message,
    proxygen::ResponseHandler* downstream) {
  try {
    const ServerOperation op = buildServerOpFromHttpMsgPath(message->getPath());
    switch (op.target) {
      case ServerOperation::Target::kConnector:
        http::sendOkResponse(downstream, connectorOperation(op, message));
        break;
      case ServerOperation::Target::kSystemConfig:
        http::sendOkResponse(downstream, systemConfigOperation(op, message));
        break;
      case ServerOperation::Target::kVeloxQueryConfig:
        http::sendOkResponse(
            downstream, veloxQueryConfigOperation(op, message));
        break;
      case ServerOperation::Target::kTask:
        http::sendOkResponse(downstream, taskOperation(op, message));
        break;
      case ServerOperation::Target::kServer:
        http::sendOkResponse(downstream, serverOperation(op, message));
        break;
    }
  } catch (const velox::VeloxUserError& ex) {
    http::sendErrorResponse(downstream, ex.what());
  } catch (const velox::VeloxException& ex) {
    http::sendErrorResponse(downstream, ex.what());
  }
}

std::string PrestoServerOperations::connectorOperation(
    const ServerOperation& op,
    proxygen::HTTPMessage* message) {
  switch (op.action) {
    case ServerOperation::Action::kClearCache:
      return clearConnectorCache(message);
    case ServerOperation::Action::kGetCacheStats:
      return getConnectorCacheStats(message);
    default:
      break;
  }
  return unsupportedAction(op);
}

std::string PrestoServerOperations::systemConfigOperation(
    const ServerOperation& op,
    proxygen::HTTPMessage* message) {
  switch (op.action) {
    case ServerOperation::Action::kSetProperty: {
      const auto name = message->getQueryParam("name");
      const auto value = message->getQueryParam("value");
      VELOX_USER_CHECK(
          !name.empty() && !value.empty(),
          "Missing 'name' or 'value' parameter for '{}.{}' operation",
          ServerOperation::targetString(op.target),
          ServerOperation::actionString(op.action));
      return fmt::format(
          "Have set system property value '{}' to '{}'. Old value was '{}'.\n",
          name,
          value,
          SystemConfig::instance()
              ->setValue(name, value)
              .value_or("<default>"));
    }
    case ServerOperation::Action::kGetProperty: {
      const auto name = message->getQueryParam("name");
      VELOX_USER_CHECK(
          !name.empty(),
          "Missing 'name' parameter for '{}.{}' operation",
          ServerOperation::targetString(op.target),
          ServerOperation::actionString(op.action));
      auto valueOpt = SystemConfig::instance()->optionalProperty(name);
      VELOX_USER_CHECK(
          valueOpt.has_value(),
          fmt::format("Could not find property '{}'\n", name));
      return fmt::format("{}\n", valueOpt.value());
    }
    default:
      break;
  }
  return unsupportedAction(op);
}

std::string PrestoServerOperations::veloxQueryConfigOperation(
    const ServerOperation& op,
    proxygen::HTTPMessage* message) {
  switch (op.action) {
    case ServerOperation::Action::kSetProperty: {
      const auto name = message->getQueryParam("name");
      const auto value = message->getQueryParam("value");
      VELOX_USER_CHECK(
          !name.empty() && !value.empty(),
          "Missing 'name' or 'value' parameter for '{}.{}' operation",
          ServerOperation::targetString(op.target),
          ServerOperation::actionString(op.action));
      return fmt::format(
          "Have set system property value '{}' to '{}'. Old value was '{}'.\n",
          name,
          value,
          SystemConfig::instance()
              ->setValue(name, value)
              .value_or("<default>"));
    }
    case ServerOperation::Action::kGetProperty: {
      const auto name = message->getQueryParam("name");
      VELOX_USER_CHECK(
          !name.empty(),
          "Missing 'name' parameter for '{}.{}' operation",
          ServerOperation::targetString(op.target),
          ServerOperation::actionString(op.action));
      return fmt::format(
          "{}\n",
          SystemConfig::instance()->optionalProperty(name).value_or(
              "<default>"));
    }
    default:
      break;
  }
  return unsupportedAction(op);
}

std::string PrestoServerOperations::taskOperation(
    const ServerOperation& op,
    proxygen::HTTPMessage* message) {
  if (taskManager_ == nullptr) {
    return "Task Manager not found";
  }
  const auto taskMap = taskManager_->tasks();
  switch (op.action) {
    case ServerOperation::Action::kGetDetail: {
      const auto id = message->getQueryParam("id");
      const auto& task = taskMap.find(id);
      if (task == taskMap.end()) {
        return fmt::format("No task found with id {}", id);
      }
      return prettyJson(task->second->toJson());
    }
    case ServerOperation::Action::kListAll: {
      uint32_t limit;
      const auto& limitStr = message->getQueryParam("limit");
      try {
        limit = limitStr == proxygen::empty_string
            ? std::numeric_limits<uint32_t>::max()
            : stoi(limitStr);
      } catch (std::exception& ex) {
        VELOX_USER_FAIL("Invalid limit provided '{}'.", limitStr);
      }
      std::stringstream oss;
      if (limit < taskMap.size()) {
        oss << "Showing " << limit << "/" << taskMap.size() << " tasks:\n";
      }
      folly::dynamic arrayObj = folly::dynamic::array;
      uint32_t index = 0;
      for (auto taskItr = taskMap.begin(); taskItr != taskMap.end();
           ++taskItr) {
        const auto& veloxTask = taskItr->second->task;
        const bool atLimit = ++index >= limit;
        arrayObj.push_back(
            (veloxTask == nullptr ? "null" : veloxTask->toShortJson()));
        if (atLimit) {
          break;
        }
      }
      oss << prettyJson(arrayObj);
      return oss.str();
    }
    default:
      break;
  }
  return unsupportedAction(op);
}

std::string PrestoServerOperations::serverOperation(
    const ServerOperation& op,
    proxygen::HTTPMessage* message) {
  switch (op.action) {
    case ServerOperation::Action::kTrace:
      return serverOperationTrace();
    case ServerOperation::Action::kSetState:
      return serverOperationSetState(message);
    case ServerOperation::Action::kAnnouncer:
      return serverOperationAnnouncer(message);
    case ServerOperation::Action::kClearCache:
      return serverOperationClearCache(message);
    case ServerOperation::Action::kWriteSSD:
      return serverOperationWriteSsd(message);
    default:
      break;
  }
  return unsupportedAction(op);
}

std::string PrestoServerOperations::serverOperationTrace() {
  return velox::process::TraceContext::statusLine();
}

std::string PrestoServerOperations::serverOperationSetState(
    proxygen::HTTPMessage* message) {
  if (server_) {
    const auto& stateStr = message->getQueryParam("state");
    const auto prevState = server_->nodeState();
    NodeState newNodeState{NodeState::kActive};
    if (stateStr == "active") {
      newNodeState = NodeState::kActive;
    } else if (stateStr == "inactive") {
      newNodeState = NodeState::kInActive;
    } else if (stateStr == "shutting_down") {
      newNodeState = NodeState::kShuttingDown;
    } else {
      VELOX_USER_FAIL(
          "Invalid state '{}'. "
          "Supported states are: 'active', 'inactive', 'shutting_down'. "
          "Example: server/setState?state=shutting_down",
          stateStr);
    }
    if (newNodeState != prevState) {
      LOG(INFO) << "Setting node state to " << nodeState2String(newNodeState);
      server_->setNodeState(newNodeState);
    }
    return fmt::format(
        "New node state: '{}', previous state: '{}'.",
        nodeState2String(newNodeState),
        nodeState2String(prevState));
  }
  return "No PrestoServer to change state of (it is nullptr).";
}

std::string PrestoServerOperations::serverOperationAnnouncer(
    proxygen::HTTPMessage* message) {
  if (server_) {
    const auto& actionStr = message->getQueryParam("action");
    if (actionStr == "enable") {
      server_->enableAnnouncer(true);
      return "Announcer enabled";
    } else if (actionStr == "disable") {
      server_->enableAnnouncer(false);
      return "Announcer disabled";
    }
    VELOX_USER_FAIL(
        "Invalid action '{}'. Supported actions are: 'enable', 'disable'. "
        "Example: server/announcer?action=disable",
        actionStr);
  }
  return "No PrestoServer to change announcer of (it is nullptr).";
}

std::string PrestoServerOperations::serverOperationClearCache(
    proxygen::HTTPMessage* message) {
  static const std::string kMemoryCacheType = "memory";
  static const std::string kServerCacheType = "ssd";

  std::string type = message->getQueryParam("type");
  if (type.empty()) {
    type = kMemoryCacheType;
  }
  if (type != kMemoryCacheType && type != kServerCacheType) {
    VELOX_USER_FAIL(
        "Unknown cache type '{}' for server cache clear operation", type);
  }

  auto* cache = velox::cache::AsyncDataCache::getInstance();
  if (cache == nullptr) {
    return "No memory cache set on server";
  }

  cache->clear();
  if (type == kMemoryCacheType) {
    return "Cleared memory cache";
  }

  auto* ssdCache = cache->ssdCache();
  if (ssdCache == nullptr) {
    return "No ssd cache set on server";
  }
  ssdCache->clear();
  return "Cleared ssd cache";
}

std::string PrestoServerOperations::serverOperationWriteSsd(
    proxygen::HTTPMessage* message) {
  auto* cache = velox::cache::AsyncDataCache::getInstance();
  if (cache == nullptr) {
    return "No memory cache set on server";
  }
  auto* ssdCache = cache->ssdCache();
  if (ssdCache == nullptr) {
    return "No ssd cache set on server";
  }

  if (!ssdCache->startWrite()) {
    return "Failed to start write to ssd cache";
  }
  cache->saveToSsd(true);
  ssdCache->waitForWriteToFinish();

  if (!ssdCache->startWrite()) {
    return "Failed to start checkpoint on ssd cache";
  }
  ssdCache->checkpoint();
  ssdCache->waitForWriteToFinish();
  return "Succeeded write ssd cache";
}
} // namespace facebook::presto
