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

class SignatureBinderBase {
 protected:
  explicit SignatureBinderBase(const exec::FunctionSignature& signature)
      : signature_{signature} {
    for (auto& variable : signature.typeVariableConstraints()) {
      // Integer parameters are updated during bind.
      if (variable.isTypeParameter()) {
        typeParameters_.insert({variable.name(), nullptr});
      } else if (variable.isIntegerParameter()) {
        integerParameters_.insert({variable.name(), {}});
      }
      if (!variable.constraint().empty()) {
        constraints_.insert({variable.name(), variable.constraint()});
      }
    }
  }

  /// Return true if actualType can bind to typeSignature and update bindings_
  /// accordingly. The number of parameters in typeSignature and actualType must
  /// match. Return false otherwise.
  bool tryBind(
      const exec::TypeSignature& typeSignature,
      const TypePtr& actualType);

  const exec::FunctionSignature& signature_;
  std::unordered_map<std::string, TypePtr> typeParameters_;
  std::unordered_map<std::string, std::optional<int>> integerParameters_;
  std::unordered_map<std::string, std::string> constraints_;

 private:
  /// If the integer parameter is set, then it must match with value.
  /// Returns false if values do not match or the parameter does not exist.
  bool checkOrSetIntegerParameter(const std::string& parameterName, int value);

  /// Try to bind the integer parameter from the actualType.
  bool tryBindIntegerParameters(
      const std::vector<exec::TypeSignature>& parameters,
      const TypePtr& actualType);
};

/// Resolves generic type names in the function signature using actual input
/// types. E.g. given function signature array(T) -> T and input type
/// array(bigint), resolves T to bigint. If type resolution is successful,
/// calculates the actual return type, e.g. bigint in the previous example.
///
/// Supports function signatures with lambdas via partial resolution. The
/// constructor allows some types in 'actualTypes' to be null (the one
/// corresponding to lambda inputs).
class SignatureBinder : private SignatureBinderBase {
 public:
  SignatureBinder(
      const exec::FunctionSignature& signature,
      const std::vector<TypePtr>& actualTypes)
      : SignatureBinderBase{signature}, actualTypes_{actualTypes} {}

  /// Returns true if successfully resolved all generic type names.
  bool tryBind();

  /// Returns concrete return type or null if couldn't fully resolve.
  TypePtr tryResolveReturnType() {
    return tryResolveType(signature_.returnType());
  }

  TypePtr tryResolveType(const exec::TypeSignature& typeSignature);

  static TypePtr tryResolveType(
      const exec::TypeSignature& typeSignature,
      const std::unordered_map<std::string, TypePtr>& typeParameters) {
    const std::unordered_map<std::string, std::string> constraints;
    std::unordered_map<std::string, std::optional<int>> integerParameters;
    return tryResolveType(
        typeSignature, typeParameters, constraints, integerParameters);
  }
  /// Returns concrete return type or null if couldn't fully resolve.
  static TypePtr tryResolveType(
      const exec::TypeSignature& typeSignature,
      const std::unordered_map<std::string, TypePtr>& typeParameters,
      const std::unordered_map<std::string, std::string>& constraints,
      std::unordered_map<std::string, std::optional<int>>& integerParameters);

 private:
  const std::vector<TypePtr>& actualTypes_;
};
} // namespace facebook::velox::exec
