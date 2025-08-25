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
#include "velox/type/TypeCoercer.h"

namespace facebook::velox::exec {

class SignatureBinderBase {
 protected:
  explicit SignatureBinderBase(const exec::FunctionSignature& signature)
      : signature_{signature} {}

  /// Return true if actualType can bind to typeSignature and update bindings_
  /// accordingly. The number of parameters in typeSignature and actualType
  /// must match. Return false otherwise.
  bool tryBind(
      const exec::TypeSignature& typeSignature,
      const TypePtr& actualType);

  /// Like 'tryBind', but allows implicit type conversion if actualType
  /// doesn't match typeSignature exactly.
  ///
  /// @param coercion Type coercion necessary to bind actualType to
  /// typeSignature if there is no exact match. 'coercion.type' is null if
  /// there is exact match.
  bool tryBindWithCoercion(
      const exec::TypeSignature& typeSignature,
      const TypePtr& actualType,
      Coercion& coercion);

  // Return the variables of the signature.
  auto& variables() const {
    return signature_.variables();
  }

  /// The function signature we are trying to bind.
  const exec::FunctionSignature& signature_;

  /// Record concrete types that are bound to type variables.
  std::unordered_map<std::string, TypePtr> typeVariablesBindings_;

  /// Record concrete values that are bound to integer variables.
  std::unordered_map<std::string, int> integerVariablesBindings_;

  /// Record concrete values that are bound to LongEnumParameter variables.
  std::unordered_map<std::string, LongEnumParameter> longEnumVariablesBindings_;

 private:
  /// If the integer parameter is set, then it must match with value.
  /// Returns false if values do not match or the parameter does not exist.
  bool checkOrSetIntegerParameter(const std::string& parameterName, int value);

  /// Try to bind the LongEnumParameter from the actualType.
  bool checkOrSetLongEnumParameter(
      const std::string& parameterName,
      const LongEnumParameter& params);

  /// Try to bind the integer parameter from the actualType.
  bool tryBindIntegerParameters(
      const std::vector<exec::TypeSignature>& parameters,
      const TypePtr& actualType);

  bool tryBind(
      const exec::TypeSignature& typeSignature,
      const TypePtr& actualType,
      bool allowCoercion,
      Coercion& coercion);
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
  // Requires that signature and actualTypes are not r-values.
  SignatureBinder(
      const exec::FunctionSignature& signature,
      const std::vector<TypePtr>& actualTypes)
      : SignatureBinderBase{signature}, actualTypes_{actualTypes} {}

  /// Returns true if successfully resolved all generic type names.
  bool tryBind();

  /// Like 'tryBind', but allows implicit type conversion if actualTypes don't
  /// match the signature exactly.
  /// @param coercions Type coercions necessary to bind actualTypes to the
  /// signature. There is one entry per argument. Coercion.type is null if no
  /// coercion is required for that argument.
  bool tryBindWithCoercions(std::vector<Coercion>& coercions);

  /// Returns concrete return type or nullptr if couldn't fully resolve.
  TypePtr tryResolveReturnType() {
    return tryResolveType(signature_.returnType());
  }

  // Try resolve type for the specified signature. Return nullptr if cannot
  // resolve.
  TypePtr tryResolveType(const exec::TypeSignature& typeSignature) {
    return tryResolveType(
        typeSignature,
        variables(),
        typeVariablesBindings_,
        integerVariablesBindings_,
        longEnumVariablesBindings_);
  }

  // Try resolve types for all specified signatures. Return empty list if some
  // signatures cannot be resolved.
  std::vector<TypePtr> tryResolveTypes(
      const folly::Range<const TypeSignature*>& typeSignatures) {
    std::vector<TypePtr> types;
    for (const auto& signature : typeSignatures) {
      auto type = tryResolveType(signature);
      if (type == nullptr) {
        return {};
      }

      types.push_back(type);
    }

    return types;
  }

  // Given a pre-computed binding for type variables resolve typeSignature if
  // possible.
  static TypePtr tryResolveType(
      const exec::TypeSignature& typeSignature,
      const std::unordered_map<std::string, SignatureVariable>& variables,
      const std::unordered_map<std::string, TypePtr>& resolvedTypeVariables) {
    std::unordered_map<std::string, int> dummyEmpty;
    std::unordered_map<std::string, LongEnumParameter> dummyEmpty2;
    return tryResolveType(
        typeSignature,
        variables,
        resolvedTypeVariables,
        dummyEmpty,
        dummyEmpty2);
  }

  // Given a pre-computed binding for type variables and integer variables,
  // resolve typeSignature if possible. integerVariablesBindings might will be
  // updated with the bound value.
  static TypePtr tryResolveType(
      const exec::TypeSignature& typeSignature,
      const std::unordered_map<std::string, SignatureVariable>& variables,
      const std::unordered_map<std::string, TypePtr>& typeVariablesBindings,
      std::unordered_map<std::string, int>& integerVariablesBindings,
      const std::unordered_map<std::string, LongEnumParameter>&
          bigintEnumVariablesBindings);

 private:
  bool tryBind(bool allowCoercions, std::vector<Coercion>& coercions);

  const std::vector<TypePtr>& actualTypes_;
};

} // namespace facebook::velox::exec
