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

#include "presto_cpp/main/functions/remote/tests/server/RestFunctionRegistry.h"

#include <glog/logging.h>

namespace facebook::presto::functions::remote::rest::test {

RestFunctionRegistry& RestFunctionRegistry::getInstance() {
  static RestFunctionRegistry instance;
  return instance;
}

bool RestFunctionRegistry::registerFunction(
    const std::string& functionName,
    std::shared_ptr<RemoteFunctionRestHandler> handler) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = functionHandlers_.find(functionName);
  bool replacing = (it != functionHandlers_.end());

  if (replacing) {
    LOG(WARNING) << "Function handler for '" << functionName
                 << "' is being replaced.";
  }

  functionHandlers_[functionName] = std::move(handler);
  return replacing;
}

bool RestFunctionRegistry::unregisterFunction(const std::string& functionName) {
  std::lock_guard<std::mutex> lock(mutex_);
  return functionHandlers_.erase(functionName) > 0;
}

std::shared_ptr<RemoteFunctionRestHandler> RestFunctionRegistry::getFunction(
    const std::string& functionName) const {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = functionHandlers_.find(functionName);
  if (it != functionHandlers_.end()) {
    return it->second;
  }
  return nullptr;
}

bool RestFunctionRegistry::hasFunction(const std::string& functionName) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return functionHandlers_.find(functionName) != functionHandlers_.end();
}

void RestFunctionRegistry::clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  functionHandlers_.clear();
}

} // namespace facebook::presto::functions::remote::rest::test
