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

#include "presto_cpp/main/common/Configs.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"

namespace facebook::presto {
template <template <class> class T, typename TReturn, typename... TArgs>
void registerPrestoFunction(
    const std::string_view name,
    const std::string_view nameSpace = {},
    const std::vector<velox::exec::SignatureVariable>& constraints = {}) {
  std::string functionName;
  functionName.append(nameSpace);
  if (nameSpace.empty()) {
    auto systemConfig = SystemConfig::instance();
    functionName.append(systemConfig->prestoDefaultNamespacePrefix());
  }
  if (functionName.back() != '.') {
    functionName.append(".");
  }
  functionName.append(name);
  LOG(INFO) << "Registering function: " << functionName;
  facebook::velox::registerFunction<T, TReturn, TArgs...>(
      {functionName}, constraints, false);
}
} // namespace facebook::presto
