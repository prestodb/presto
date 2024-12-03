/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/expression/SpecialFormRegistry.h"

namespace facebook::velox::functions::sparksql {

extern void registerArrayFunctions(const std::string& prefix);
extern void registerBinaryFunctions(const std::string& prefix);
extern void registerBitwiseFunctions(const std::string& prefix);
extern void registerCompareFunctions(const std::string& prefix);
extern void registerDatetimeFunctions(const std::string& prefix);
extern void registerJsonFunctions(const std::string& prefix);
extern void registerMapFunctions(const std::string& prefix);
extern void registerMathFunctions(const std::string& prefix);
extern void registerMiscFunctions(const std::string& prefix);
extern void registerRegexpFunctions(const std::string& prefix);
extern void registerSpecialFormGeneralFunctions(const std::string& prefix);
extern void registerStringFunctions(const std::string& prefix);
extern void registerUrlFunctions(const std::string& prefix);

void registerFunctions(const std::string& prefix) {
  registerArrayFunctions(prefix);
  registerBinaryFunctions(prefix);
  registerBitwiseFunctions(prefix);
  registerCompareFunctions(prefix);
  registerDatetimeFunctions(prefix);
  registerJsonFunctions(prefix);
  registerMapFunctions(prefix);
  registerMathFunctions(prefix);
  registerMiscFunctions(prefix);
  registerRegexpFunctions(prefix);
  registerSpecialFormGeneralFunctions(prefix);
  registerStringFunctions(prefix);
  registerUrlFunctions(prefix);
}

std::vector<std::string> listFunctionNames() {
  std::vector<std::string> names =
      exec::specialFormRegistry().getSpecialFormNames();

  const auto& simpleFunctions = exec::simpleFunctions().getFunctionNames();
  names.insert(names.end(), simpleFunctions.begin(), simpleFunctions.end());

  exec::vectorFunctionFactories().withRLock([&](const auto& map) {
    names.reserve(names.size() + map.size());
    for (const auto& [name, _] : map) {
      names.push_back(name);
    }
  });

  return names;
}

} // namespace facebook::velox::functions::sparksql
