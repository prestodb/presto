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
#include "velox/type/Type.h"

namespace facebook::velox::exec {

/// Resolves generic type names in the function signature using actual input
/// types. E.g. given function signature array(T) -> T and input type
/// array(bigint), resolves T to bigint. If type resolution is successful,
/// calculates the actual return type, e.g. bigint in the previous example.
class SignatureBinder {
 public:
  SignatureBinder(
      const exec::FunctionSignature& signature,
      const std::vector<TypePtr>& actualTypes)
      : signature_{signature}, actualTypes_{actualTypes} {
    for (auto& variable : signature.typeVariableConstants()) {
      bindings_.insert({variable.name(), nullptr});
    }
    for (auto& variable : signature.variables()) {
      if (!variable.constraint().empty()) {
        constraints_.insert({variable.name(), variable.constraint()});
      }
    }
  }

  // Returns true if successfully resolved all generic type names.
  bool tryBind();

  // Returns concrete return type or null if couldn't fully resolve.
  TypePtr tryResolveReturnType() {
    return tryResolveType(signature_.returnType());
  }

  TypePtr tryResolveType(const exec::TypeSignature& typeSignature);

  static TypePtr tryResolveType(
      const exec::TypeSignature& typeSignature,
      const std::unordered_map<std::string, TypePtr>& bindings) {
    std::unordered_map<std::string, int> variables;
    return tryResolveType(typeSignature, bindings, variables);
  }
  // Returns concrete return type or null if couldn't fully resolve.
  static TypePtr tryResolveType(
      const exec::TypeSignature& typeSignature,
      const std::unordered_map<std::string, TypePtr>& bindings,
      std::unordered_map<std::string, int>& variables,
      const std::unordered_map<std::string, std::string>& constraints = {});

 private:
  bool tryBind(
      const exec::TypeSignature& typeSignature,
      const TypePtr& actualType);

  const exec::FunctionSignature& signature_;
  const std::vector<TypePtr>& actualTypes_;
  std::unordered_map<std::string, TypePtr> bindings_;
  std::unordered_map<std::string, int> variables_;
  std::unordered_map<std::string, std::string> constraints_;
};
} // namespace facebook::velox::exec
