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

#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"

namespace facebook::velox::exec {

WindowFunctionMap& windowFunctions() {
  static WindowFunctionMap functions;
  return functions;
}

namespace {
std::optional<const WindowFunctionEntry*> getWindowFunctionEntry(
    const std::string& name) {
  auto& functionsMap = windowFunctions();
  auto it = functionsMap.find(name);
  if (it != functionsMap.end()) {
    return &it->second;
  }

  return std::nullopt;
}
} // namespace

bool registerWindowFunction(
    const std::string& name,
    std::vector<FunctionSignaturePtr> signatures,
    WindowFunctionFactory factory) {
  auto sanitizedName = sanitizeFunctionName(name);
  windowFunctions()[sanitizedName] = {
      std::move(signatures), std::move(factory)};
  return true;
}

std::optional<std::vector<FunctionSignaturePtr>> getWindowFunctionSignatures(
    const std::string& name) {
  auto sanitizedName = sanitizeFunctionName(name);
  if (auto func = getWindowFunctionEntry(sanitizedName)) {
    return func.value()->signatures;
  }
  return std::nullopt;
}

std::unique_ptr<WindowFunction> WindowFunction::create(
    const std::string& name,
    const std::vector<WindowFunctionArg>& args,
    const TypePtr& resultType,
    memory::MemoryPool* pool) {
  // Lookup the function in the new registry first.
  if (auto func = getWindowFunctionEntry(name)) {
    return func.value()->factory(args, resultType, pool);
  }

  VELOX_USER_FAIL("Window function not registered: {}", name);
}

} // namespace facebook::velox::exec
