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
#include "velox/expression/SignatureBinder.h"

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
    WindowFunction::Metadata metadata,
    WindowFunctionFactory factory) {
  auto sanitizedName = sanitizeName(name);
  windowFunctions()[sanitizedName] = {
      std::move(signatures), std::move(factory), std::move(metadata)};
  return true;
}

WindowFunction::Metadata getWindowFunctionMetadata(const std::string& name) {
  const auto sanitizedName = sanitizeName(name);
  if (auto func = getWindowFunctionEntry(sanitizedName)) {
    return func.value()->metadata;
  } else {
    VELOX_USER_FAIL(
        "Window function metadata not found for function: {}", name);
  }
}

std::optional<std::vector<FunctionSignaturePtr>> getWindowFunctionSignatures(
    const std::string& name) {
  auto sanitizedName = sanitizeName(name);
  if (auto func = getWindowFunctionEntry(sanitizedName)) {
    return func.value()->signatures;
  }
  return std::nullopt;
}

std::unique_ptr<WindowFunction> WindowFunction::create(
    const std::string& name,
    const std::vector<WindowFunctionArg>& args,
    const TypePtr& resultType,
    bool ignoreNulls,
    memory::MemoryPool* pool,
    HashStringAllocator* stringAllocator,
    const core::QueryConfig& config) {
  // Lookup the function in the new registry first.
  if (auto func = getWindowFunctionEntry(name)) {
    std::vector<TypePtr> argTypes;
    argTypes.reserve(args.size());
    for (const auto& arg : args) {
      argTypes.push_back(arg.type);
    }

    const auto& signatures = func.value()->signatures;
    for (auto& signature : signatures) {
      SignatureBinder binder(*signature, argTypes);
      if (binder.tryBind()) {
        auto type = binder.tryResolveType(signature->returnType());
        VELOX_USER_CHECK(
            type->equivalent(*resultType),
            "Unexpected return type for window function {}. Expected {}. Got {}.",
            toString(name, argTypes),
            type->toString(),
            resultType->toString());
        return func.value()->factory(
            args, resultType, ignoreNulls, pool, stringAllocator, config);
      }
    }

    VELOX_USER_FAIL(
        "Window function signature is not supported: {}. Supported signatures: {}.",
        toString(name, argTypes),
        toString(signatures));
  }

  VELOX_USER_FAIL("Window function not registered: {}", name);
}

void WindowFunction::setEmptyFramesResult(
    const SelectivityVector& validRows,
    vector_size_t resultOffset,
    const VectorPtr& defaultResult,
    const VectorPtr& result) {
  if (validRows.isAllSelected()) {
    return;
  }
  // Set the null bit for all rows to true if the defaultResult is NULL and
  // there are no valid rows.
  if (!validRows.hasSelections() && defaultResult->isNullAt(0)) {
    uint64_t* rawNulls = result->mutableRawNulls();
    bits::fillBits(
        rawNulls, resultOffset, resultOffset + validRows.size(), bits::kNull);
    return;
  }

  invalidRows_.resizeFill(validRows.size(), true);
  invalidRows_.deselect(validRows);
  invalidRows_.applyToSelected([&](auto i) {
    result->copy(defaultResult.get(), resultOffset + i, 0, 1);
  });
}

} // namespace facebook::velox::exec
