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
#pragma once

#include <proxygen/lib/http/HTTPMessage.h>
#include <string>

namespace facebook::presto {

/// Defines a server operation.
struct ServerOperation {
  /// The target this operation is operating upon
  enum class Target {
    kConnector,
  };

  /// The action this operation is trying to take
  enum class Action { kClearCache, kGetCacheStats };

  static const std::unordered_map<std::string, Target> kTargetLookup;
  static const std::unordered_map<Target, std::string> kReverseTargetLookup;
  static const std::unordered_map<std::string, Action> kActionLookup;
  static const std::unordered_map<Action, std::string> kReverseActionLookup;

  static Target targetFromString(const std::string& str);
  static std::string targetString(Target target);
  static Action actionFromString(const std::string& str);
  static std::string actionString(Action action);

  Target target;
  Action action;
};

/// Builds a server operation from an HTTP request. Throws upon build failure.
ServerOperation buildServerOpFromHttpRequest(
    const proxygen::HTTPMessage* httpMsg);

} // namespace facebook::presto
