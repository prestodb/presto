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

#include <boost/algorithm/string.hpp>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/expression/TypeSignature.h"
#include "velox/expression/signature_parser/SignatureParser.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {

std::string sanitizeName(const std::string& name);

/// Return a list of primitive type names.
const std::vector<std::string> primitiveTypeNames();

enum class ParameterType : int8_t { kTypeParameter, kIntegerParameter };

/// Canonical names for functions that have special treatments in pushdowns.
enum class FunctionCanonicalName {
  kUnknown,
  kLt,
  kNot,
  kRand,
};

/// SignatureVariable holds both, type parameters (e.g. K or V in map(K,
/// V)), and integer parameters with optional constraints (e.g. "r_precision =
/// a_precision + b_precision" in decimals).
class SignatureVariable {
 public:
  explicit SignatureVariable(
      std::string name,
      std::optional<std::string> constraint,
      ParameterType type,
      bool knownTypesOnly = false,
      bool orderableTypesOnly = false,
      bool comparableTypesOnly = false);

  const std::string& name() const {
    return name_;
  }

  const std::string& constraint() const {
    return constraint_;
  }

  bool knownTypesOnly() const {
    VELOX_USER_CHECK(isTypeParameter());
    return knownTypesOnly_;
  }

  bool orderableTypesOnly() const {
    VELOX_USER_CHECK(isTypeParameter());
    return orderableTypesOnly_;
  }

  bool comparableTypesOnly() const {
    VELOX_USER_CHECK(isTypeParameter());
    return comparableTypesOnly_;
  }

  bool isTypeParameter() const {
    return type_ == ParameterType::kTypeParameter;
  }

  bool isIntegerParameter() const {
    return type_ == ParameterType::kIntegerParameter;
  }

  bool operator==(const SignatureVariable& rhs) const {
    return type_ == rhs.type_ && name_ == rhs.name_ &&
        constraint_ == rhs.constraint_ &&
        knownTypesOnly_ == rhs.knownTypesOnly_ &&
        orderableTypesOnly_ == rhs.orderableTypesOnly_ &&
        comparableTypesOnly_ == rhs.comparableTypesOnly_;
  }

 private:
  const std::string name_;
  const std::string constraint_;
  const ParameterType type_;
  // The following properties apply only to type variables and indicate whether
  // the type can bind only to known, orderable or comparable types.
  bool knownTypesOnly_ = false;
  bool orderableTypesOnly_ = false;
  bool comparableTypesOnly_ = false;
};

class FunctionSignature {
 public:
  /// @param variables_ Generic type names used in return type
  /// and argument types (and constraints if necessary).
  /// @param returnType Return type. May use generic type names, e.g.
  /// array(T).
  /// @param argumentTypes Argument types. May use generic type names, e.g.
  /// map(K,V). The type of the last argument of a function with variable
  /// number of arguments can be "any", which means that arguments of any type
  /// are accepted.
  /// @param variableArity True if function accepts variable number of
  /// arguments, e.g. concat(varchar...). Variable arity arguments can appear
  /// only at the end of the argument list and their types must match the type
  /// specified in the last entry of 'argumentTypes'. Variable arity arguments
  /// can appear zero or more times.
  FunctionSignature(
      std::unordered_map<std::string, SignatureVariable> variables,
      TypeSignature returnType,
      std::vector<TypeSignature> argumentTypes,
      std::vector<bool> constantArguments,
      bool variableArity);

  virtual ~FunctionSignature() = default;

  const TypeSignature& returnType() const {
    return returnType_;
  }

  const std::vector<TypeSignature>& argumentTypes() const {
    return argumentTypes_;
  }

  const std::vector<bool>& constantArguments() const {
    return constantArguments_;
  }

  bool hasConstantArgument() const {
    return std::any_of(
        constantArguments_.begin(), constantArguments_.end(), folly::identity);
  }

  bool variableArity() const {
    return variableArity_;
  }

  virtual std::string toString() const;

  const auto& variables() const {
    return variables_;
  }

  // This tests syntactic equality not semantic equality
  // For example, even if only the names of the variables are
  // different the signatures are considered not equal (array(K) != array(V))
  bool operator==(const FunctionSignature& rhs) const {
    return variables_ == rhs.variables_ && returnType_ == rhs.returnType_ &&
        argumentTypes_ == rhs.argumentTypes_ &&
        variableArity_ == rhs.variableArity_;
  }

 protected:
  // Return a string of the list of argument types.
  std::string argumentsToString() const;

 private:
  const std::unordered_map<std::string, SignatureVariable> variables_;
  const TypeSignature returnType_;
  const std::vector<TypeSignature> argumentTypes_;
  const std::vector<bool> constantArguments_;
  const bool variableArity_;
};

using FunctionSignaturePtr = std::shared_ptr<FunctionSignature>;

class AggregateFunctionSignature : public FunctionSignature {
 public:
  AggregateFunctionSignature(
      std::unordered_map<std::string, SignatureVariable> variables,
      TypeSignature returnType,
      TypeSignature intermediateType,
      std::vector<TypeSignature> argumentTypes,
      std::vector<bool> constantArguments,
      bool variableArity)
      : FunctionSignature(
            std::move(variables),
            std::move(returnType),
            std::move(argumentTypes),
            std::move(constantArguments),
            variableArity),
        intermediateType_{std::move(intermediateType)} {}

  const TypeSignature& intermediateType() const {
    return intermediateType_;
  }

  std::string toString() const override;

 private:
  const TypeSignature intermediateType_;
};

using AggregateFunctionSignaturePtr =
    std::shared_ptr<AggregateFunctionSignature>;

namespace {

inline void addVariable(
    std::unordered_map<std::string, SignatureVariable>& variables,
    const SignatureVariable& variable) {
  VELOX_USER_CHECK(
      !variables.count(variable.name()),
      "Variable {} declared twice",
      variable.name());

  variables.emplace(variable.name(), variable);
}

} // namespace

/// Convenience class for creating FunctionSignature instances.
///
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
///                .knownTypeVariable("K")
///                .typeVariable("V")
///                .returnType("array(K)")
///                .argumentType("map(K,V)")
///                .build()
class FunctionSignatureBuilder {
 public:
  FunctionSignatureBuilder& typeVariable(const std::string& name) {
    addVariable(
        variables_, SignatureVariable(name, "", ParameterType::kTypeParameter));
    return *this;
  }

  FunctionSignatureBuilder& variable(const SignatureVariable& variable) {
    addVariable(variables_, variable);
    return *this;
  }

  FunctionSignatureBuilder& knownTypeVariable(const std::string& name);

  /// Orderable implies comparable, this method would enable
  /// comparableTypesOnly_ too.
  FunctionSignatureBuilder& orderableTypeVariable(const std::string& name);

  FunctionSignatureBuilder& comparableTypeVariable(const std::string& name);

  FunctionSignatureBuilder& integerVariable(
      const std::string& name,
      std::optional<std::string> constraint = std::nullopt) {
    addVariable(
        variables_,
        SignatureVariable(name, constraint, ParameterType::kIntegerParameter));
    return *this;
  }

  FunctionSignatureBuilder& returnType(const std::string& type) {
    returnType_.emplace(parseTypeSignature(type));
    return *this;
  }

  FunctionSignatureBuilder& argumentType(const std::string& type) {
    argumentTypes_.emplace_back(parseTypeSignature(type));
    constantArguments_.push_back(false);
    return *this;
  }

  FunctionSignatureBuilder& constantArgumentType(const std::string& type) {
    argumentTypes_.emplace_back(parseTypeSignature(type));
    constantArguments_.push_back(true);
    return *this;
  }

  FunctionSignatureBuilder& variableArity() {
    variableArity_ = true;
    return *this;
  }

  FunctionSignaturePtr build();

 private:
  std::unordered_map<std::string, SignatureVariable> variables_;
  std::optional<TypeSignature> returnType_;
  std::vector<TypeSignature> argumentTypes_;
  std::vector<bool> constantArguments_;
  bool variableArity_{false};
};

/// Convenience class for creating AggregageFunctionSignature instances.
/// Example of usage:
///
///     - signature of covar_samp aggregate function: (double, double) ->
///     double
///
///     exec::AggregateFunctionSignatureBuilder()
///                .returnType("double")
///                .intermediateType("row(double,bigint,double,double)")
///                .argumentType("double")
///                .argumentType("double")
///                .build()
class AggregateFunctionSignatureBuilder {
 public:
  AggregateFunctionSignatureBuilder& typeVariable(const std::string& name) {
    addVariable(
        variables_, SignatureVariable(name, "", ParameterType::kTypeParameter));
    return *this;
  }

  AggregateFunctionSignatureBuilder& knownTypeVariable(const std::string& name);

  /// Orderable implies comparable, this method would enable
  /// comparableTypesOnly_ too.
  AggregateFunctionSignatureBuilder& orderableTypeVariable(
      const std::string& name);

  AggregateFunctionSignatureBuilder& comparableTypeVariable(
      const std::string& name);

  AggregateFunctionSignatureBuilder& integerVariable(
      const std::string& name,
      std::optional<std::string> constraint = std::nullopt) {
    addVariable(
        variables_,
        SignatureVariable(name, constraint, ParameterType::kIntegerParameter));
    return *this;
  }

  AggregateFunctionSignatureBuilder& returnType(const std::string& type) {
    returnType_.emplace(parseTypeSignature(type));
    return *this;
  }

  AggregateFunctionSignatureBuilder& argumentType(const std::string& type) {
    argumentTypes_.emplace_back(parseTypeSignature(type));
    constantArguments_.push_back(false);
    return *this;
  }

  AggregateFunctionSignatureBuilder& constantArgumentType(
      const std::string& type) {
    argumentTypes_.emplace_back(parseTypeSignature(type));
    constantArguments_.push_back(true);
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
  std::unordered_map<std::string, SignatureVariable> variables_;
  std::optional<TypeSignature> returnType_;
  std::optional<TypeSignature> intermediateType_;
  std::vector<TypeSignature> argumentTypes_;
  std::vector<bool> constantArguments_;
  bool variableArity_{false};
};

/// Return a string representation of function signature: name(type1,
/// type1,...). For example, foo(bigint, boolean, varchar).
std::string toString(
    const std::string& name,
    const std::vector<TypePtr>& types);

/// Return a string representation of a list of scalar function signatures.
/// For example, (boolean) -> integer, (varchar, integer) -> varchar.
std::string toString(const std::vector<FunctionSignaturePtr>& signatures);

/// Return a string representation of a list of aggregate function signatures.
/// For example, (boolean) -> varbinary -> integer, (varchar, integer) ->
/// varbinary -> varchar.
std::string toString(
    const std::vector<std::shared_ptr<AggregateFunctionSignature>>& signatures);

} // namespace facebook::velox::exec

namespace std {
template <>
struct hash<facebook::velox::exec::SignatureVariable> {
  using argument_type = facebook::velox::exec::SignatureVariable;
  using result_type = std::size_t;

  result_type operator()(const argument_type& key) const noexcept {
    return std::hash<std::string>{}(key.name()) * 31 +
        std::hash<std::string>{}(key.constraint());
  }
};

template <>
struct hash<facebook::velox::exec::TypeSignature> {
  using argument_type = facebook::velox::exec::TypeSignature;
  using result_type = std::size_t;

  result_type operator()(const argument_type& key) const noexcept {
    size_t val = std::hash<std::string>{}(key.baseName());
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
    auto variablesHasher =
        std::hash<facebook::velox::exec::SignatureVariable>{};
    auto typeSignatureHasher =
        std::hash<facebook::velox::exec::TypeSignature>{};

    size_t val = 0;
    // No need to hash keys, since they are also a field in the value.
    for (const auto& [_, variable] : key.variables()) {
      val = val * 31 + variablesHasher(variable);
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
