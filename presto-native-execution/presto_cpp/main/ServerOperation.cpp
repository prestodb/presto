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
#include "presto_cpp/main/ServerOperation.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto {

const folly::F14FastMap<std::string, ServerOperation::Action>
    ServerOperation::kActionLookup{
        {"clearCache", ServerOperation::Action::kClearCache},
        {"getCacheStats", ServerOperation::Action::kGetCacheStats},
        {"setProperty", ServerOperation::Action::kSetProperty},
        {"getProperty", ServerOperation::Action::kGetProperty},
    };

const folly::F14FastMap<ServerOperation::Action, std::string>
    ServerOperation::kReverseActionLookup{
        {ServerOperation::Action::kClearCache, "clearCache"},
        {ServerOperation::Action::kGetCacheStats, "getCacheStats"},
        {ServerOperation::Action::kSetProperty, "setProperty"},
        {ServerOperation::Action::kGetProperty, "getProperty"},
    };

const folly::F14FastMap<std::string, ServerOperation::Target>
    ServerOperation::kTargetLookup{
        {"connector", ServerOperation::Target::kConnector},
        {"systemConfig", ServerOperation::Target::kSystemConfig},
        {"veloxQueryConfig", ServerOperation::Target::kVeloxQueryConfig},
    };

const folly::F14FastMap<ServerOperation::Target, std::string>
    ServerOperation::kReverseTargetLookup{
        {ServerOperation::Target::kConnector, "connector"},
        {ServerOperation::Target::kSystemConfig, "systemConfig"},
        {ServerOperation::Target::kVeloxQueryConfig, "veloxQueryConfig"},
    };

ServerOperation::Target ServerOperation::targetFromString(
    const std::string& str) {
  auto it = kTargetLookup.find(str);
  if (it == kTargetLookup.end()) {
    VELOX_USER_FAIL("Unsupported server operation target '{}'", str);
  }
  return it->second;
}

std::string ServerOperation::targetString(ServerOperation::Target target) {
  auto it = kReverseTargetLookup.find(target);
  if (it == kReverseTargetLookup.end()) {
    VELOX_FAIL();
  }
  return it->second;
}

ServerOperation::Action ServerOperation::actionFromString(
    const std::string& str) {
  auto it = kActionLookup.find(str);
  if (it == kActionLookup.end()) {
    VELOX_USER_FAIL("Unsupported server operation action '{}'", str);
  }
  return it->second;
}

std::string ServerOperation::actionString(Action action) {
  auto it = kReverseActionLookup.find(action);
  if (it == kReverseActionLookup.end()) {
    VELOX_FAIL();
  }
  return it->second;
}

ServerOperation buildServerOpFromHttpMsgPath(const std::string& httpMsgPath) {
  static const auto targetPos = std::string("/v1/operation/").length();
  auto actionPos = httpMsgPath.find('/', targetPos);
  VELOX_USER_CHECK_NE(actionPos, std::string::npos);
  // Go beyond '/' to point to the first letter of action
  actionPos += 1;
  auto target = httpMsgPath.substr(targetPos, actionPos - 1 - targetPos);
  auto action = httpMsgPath.substr(actionPos);

  return ServerOperation{
      ServerOperation::targetFromString(target),
      ServerOperation::actionFromString(action)};
}

} // namespace facebook::presto
