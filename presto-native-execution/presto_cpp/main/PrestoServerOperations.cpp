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
#include "presto_cpp/main/ServerOperation.h"
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/http/HttpServer.h"
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
          BaseVeloxQueryConfig::instance()
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
          BaseVeloxQueryConfig::instance()->getValue(name).value_or(
              "<default>"));
    }
    default:
      break;
  }
  return unsupportedAction(op);
}

} // namespace facebook::presto
