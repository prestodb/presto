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
#pragma once

#include "velox/expression/FunctionSignature.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::exec {

class WindowFunction {
 public:
  explicit WindowFunction(TypePtr resultType, memory::MemoryPool* pool)
      : resultType_(std::move(resultType)), pool_(pool) {}

  virtual ~WindowFunction() = default;

  const TypePtr& resultType() const {
    return resultType_;
  }

  memory::MemoryPool* pool() const {
    return pool_;
  }

  static std::unique_ptr<WindowFunction> create(
      const std::string& name,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& resultType,
      memory::MemoryPool* pool);

  // TODO : Add the Window function execution related APIs.

 protected:
  const TypePtr resultType_;
  memory::MemoryPool* pool_;
};

using WindowFunctionFactory = std::function<std::unique_ptr<WindowFunction>(
    const std::vector<TypePtr>& argTypes,
    const TypePtr& resultType,
    memory::MemoryPool* pool)>;

/// Register a window function with the specified name and signatures.
/// Registering a function with the same name a second time overrides the first
/// registration.
bool registerWindowFunction(
    const std::string& name,
    std::vector<FunctionSignaturePtr> signatures,
    WindowFunctionFactory factory);

/// Returns signatures of the window function with the specified name.
/// Returns empty std::optional if function with that name is not found.
std::optional<std::vector<FunctionSignaturePtr>> getWindowFunctionSignatures(
    const std::string& name);

struct WindowFunctionEntry {
  std::vector<FunctionSignaturePtr> signatures;
  WindowFunctionFactory factory;
};

using WindowFunctionMap = std::unordered_map<std::string, WindowFunctionEntry>;

/// Returns a map of all window function names to their registrations.
WindowFunctionMap& windowFunctions();
} // namespace facebook::velox::exec
