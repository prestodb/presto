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

  bool operator==(const TypeVariableConstraint& rhs) const {
    return name_ == rhs.name_;
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

  bool operator==(const TypeSignature& rhs) const {
    return baseType_ == rhs.baseType_ && parameters_ == rhs.parameters_;
  }

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

  // This tests syntactic equality not semantic equality
  // For example, even if only the names of the typeVariableConstants are
  // different the signatures are considered not equal (array(K) != array(V))
  bool operator==(const FunctionSignature& rhs) const {
    return typeVariableConstants_ == rhs.typeVariableConstants_ &&
        returnType_ == rhs.returnType_ &&
        argumentTypes_ == rhs.argumentTypes_ &&
        variableArity_ == rhs.variableArity_;
  }

 private:
  const std::vector<TypeVariableConstraint> typeVariableConstants_;
  const TypeSignature returnType_;
  const std::vector<TypeSignature> argumentTypes_;
  const bool variableArity_;
};

using FunctionSignaturePtr = std::shared_ptr<FunctionSignature>;

class AggregateFunctionSignature : public FunctionSignature {
 public:
  AggregateFunctionSignature(
      std::vector<TypeVariableConstraint> typeVariableConstants,
      TypeSignature returnType,
      TypeSignature intermediateType,
      std::vector<TypeSignature> argumentTypes,
      bool variableArity)
      : FunctionSignature(
            std::move(typeVariableConstants),
            std::move(returnType),
            std::move(argumentTypes),
            variableArity),
        intermediateType_{std::move(intermediateType)} {}

  const TypeSignature& intermediateType() const {
    return intermediateType_;
  }

 private:
  const TypeSignature intermediateType_;
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

  FunctionSignaturePtr build();

 private:
  std::vector<TypeVariableConstraint> typeVariableConstants_;
  std::optional<TypeSignature> returnType_;
  std::vector<TypeSignature> argumentTypes_;
  bool variableArity_{false};
};

/// Convenience class for creating AggregageFunctionSignature instances.
/// Example of usage:
///
///     - signature of covar_samp aggregate function: (double, double) -> double
///
///     exec::AggregateFunctionSignatureBuilder()
///                .returnType("double")
///                .intermediateType("row(double,bigint,double,double)")
///                .argumentType("double")
///                .argumentType("double")
///                .build()
class AggregateFunctionSignatureBuilder {
 public:
  AggregateFunctionSignatureBuilder& typeVariable(std::string name) {
    typeVariableConstants_.emplace_back(name);
    return *this;
  }

  AggregateFunctionSignatureBuilder& returnType(const std::string& type) {
    returnType_.emplace(parseTypeSignature(type));
    return *this;
  }

  AggregateFunctionSignatureBuilder& argumentType(const std::string& type) {
    argumentTypes_.emplace_back(parseTypeSignature(type));
    return *this;
  }

  AggregateFunctionSignatureBuilder& intermediateType(const std::string& type) {
    intermediateType_.emplace(parseTypeSignature(type));
    return *this;
  }

  AggregateFunctionSignatureBuilder& variableArity() {
    variableArity_ = true;
    return *this;
  }

  std::shared_ptr<AggregateFunctionSignature> build();

 private:
  std::vector<TypeVariableConstraint> typeVariableConstants_;
  std::optional<TypeSignature> returnType_;
  std::optional<TypeSignature> intermediateType_;
  std::vector<TypeSignature> argumentTypes_;
  bool variableArity_{false};
};

} // namespace facebook::velox::exec

namespace std {
template <>
struct hash<facebook::velox::exec::TypeVariableConstraint> {
  using argument_type = facebook::velox::exec::TypeVariableConstraint;
  using result_type = std::size_t;

  result_type operator()(const argument_type& key) const noexcept {
    return std::hash<std::string>{}(key.name());
  }
};

template <>
struct hash<facebook::velox::exec::TypeSignature> {
  using argument_type = facebook::velox::exec::TypeSignature;
  using result_type = std::size_t;

  result_type operator()(const argument_type& key) const noexcept {
    size_t val = std::hash<std::string>{}(key.baseType());
    for (const auto& parameter : key.parameters()) {
      val = val * 31 + this->operator()(parameter);
    }
    return val;
  }
};

template <>
struct hash<facebook::velox::exec::FunctionSignature> {
  using argument_type = facebook::velox::exec::FunctionSignature;
  using result_type = std::size_t;

  result_type operator()(const argument_type& key) const noexcept {
    auto typeVariableConstraintHasher =
        std::hash<facebook::velox::exec::TypeVariableConstraint>{};
    auto typeSignatureHasher =
        std::hash<facebook::velox::exec::TypeSignature>{};

    size_t val = 0;
    for (const auto& constraint : key.typeVariableConstants()) {
      val = val * 31 + typeVariableConstraintHasher(constraint);
    }

    val = val * 31 + typeSignatureHasher(key.returnType());

    for (const auto& arg : key.argumentTypes()) {
      val = val * 31 + typeSignatureHasher(arg);
    }

    val = val * 31 + (key.variableArity() ? 0 : 1);
    return val;
  }
};

} // namespace std
