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

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace facebook::velox::exec {

// A type name (e.g. K or V in map(K, V)) and optionally constraints, e.g.
// orderable, sortable, etc.
class TypeVariableConstraint {
 public:
  explicit TypeVariableConstraint(std::string name) : name_{std::move(name)} {}

  const std::string& name() const {
    return name_;
  }

 private:
  const std::string name_;
};

// Base type (e.g. map) and optional parameters (e.g. K, V).
class TypeSignature {
 public:
  TypeSignature(std::string baseType, std::vector<TypeSignature> parameters)
      : baseType_{std::move(baseType)}, parameters_{std::move(parameters)} {}

  const std::string& baseType() const {
    return baseType_;
  }

  const std::vector<TypeSignature>& parameters() const {
    return parameters_;
  }

  std::string toString() const;

 private:
  const std::string baseType_;
  const std::vector<TypeSignature> parameters_;
};

class FunctionSignature {
 public:
  /// @param typeVariableConstants Generic type names used in return type and
  /// argument types (and constraints if necessary).
  /// @param returnType Return type. May use generic type names, e.g. array(T).
  /// @param argumentTypes Argument types. May use generic type names, e.g.
  /// map(K,V). The type of the last argument of a function with variable number
  /// of arguments can be "any", which means that arguments of any type are
  /// accepted.
  /// @param variableArity True if function accepts variable number of
  /// arguments, e.g. concat(varchar...). Variable arity arguments can appear
  /// only at the end of the argument list and their types must match the type
  /// specified in the last entry of 'argumentTypes'. Variable arity arguments
  /// can appear zero or more times.
  FunctionSignature(
      std::vector<TypeVariableConstraint> typeVariableConstants,
      TypeSignature returnType,
      std::vector<TypeSignature> argumentTypes,
      bool variableArity);

  const std::vector<TypeVariableConstraint>& typeVariableConstants() const {
    return typeVariableConstants_;
  }

  const TypeSignature& returnType() const {
    return returnType_;
  }

  const std::vector<TypeSignature>& argumentTypes() const {
    return argumentTypes_;
  }

  bool variableArity() const {
    return variableArity_;
  }

  std::string toString() const;

 private:
  const std::vector<TypeVariableConstraint> typeVariableConstants_;
  const TypeSignature returnType_;
  const std::vector<TypeSignature> argumentTypes_;
  const bool variableArity_;
};

/// Parses a string into TypeSignature. The format of the string is type name,
/// optionally followed by type parameters enclosed in parenthesis.
/// Examples:
///     - bigint
///     - double
///     - array(T)
///     - map(K,V)
///     - row(bigint,array(tinyint),T)
///     - function(S,T,R)
TypeSignature parseTypeSignature(const std::string& signature);

/// Convenience class for creating FunctionSignature instances.
/// Example of usage:
///     - signature of "concat" function: varchar... -> varchar
///
///     exec::FunctionSignatureBuilder()
///                .returnType("varchar")
///                .argumentType("varchar")
///                .variableArity()
///                .build()
///
///     - signature of map_keys function: map(K,V) -> array(K)
///
///     exec::FunctionSignatureBuilder()
///                .typeVariable("K")
///                .typeVariable("V")
///                .returnType("array(K)")
///                .argumentType("map(K,V)")
///                .build()
class FunctionSignatureBuilder {
 public:
  FunctionSignatureBuilder& typeVariable(std::string name) {
    typeVariableConstants_.emplace_back(name);
    return *this;
  }

  FunctionSignatureBuilder& returnType(const std::string& type) {
    returnType_.emplace(parseTypeSignature(type));
    return *this;
  }

  FunctionSignatureBuilder& argumentType(const std::string& type) {
    argumentTypes_.emplace_back(parseTypeSignature(type));
    return *this;
  }

  FunctionSignatureBuilder& variableArity() {
    variableArity_ = true;
    return *this;
  }

  std::shared_ptr<FunctionSignature> build();

 private:
  std::vector<TypeVariableConstraint> typeVariableConstants_;
  std::optional<TypeSignature> returnType_;
  std::vector<TypeSignature> argumentTypes_;
  bool variableArity_{false};
};

} // namespace facebook::velox::exec
