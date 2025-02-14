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
    const char* name,
    const char* nameSpace = "",
    const std::vector<velox::exec::SignatureVariable>& constraints = {},
    bool overwrite = true) {
  std::string cpp_nameSpace(nameSpace);
  if (cpp_nameSpace.empty()) {
    auto systemConfig = SystemConfig::instance();
    cpp_nameSpace = systemConfig->prestoDefaultNamespacePrefix();
  }
  std::string cpp_name(cpp_nameSpace);
  cpp_name.append(name);
  LOG(INFO) << "registering function: " << cpp_name;
  facebook::velox::registerFunction<T, TReturn, TArgs...>(
      {cpp_name}, constraints, overwrite);
}
} // namespace facebook::presto
