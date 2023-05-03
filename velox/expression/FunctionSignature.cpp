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
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/trim.hpp>

#include "velox/common/base/Exceptions.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {

std::string sanitizeName(const std::string& name) {
  std::string sanitizedName;
  sanitizedName.resize(name.size());
  std::transform(
      name.begin(), name.end(), sanitizedName.begin(), [](unsigned char c) {
        return std::tolower(c);
      });

  return sanitizedName;
}

const std::vector<std::string> primitiveTypeNames() {
  static const std::vector<std::string> kPrimitiveTypeNames = {
      "boolean",
      "bigint",
      "integer",
      "smallint",
      "tinyint",
      "real",
      "double",
      "varchar",
      "varbinary",
      "timestamp",
      "date",
  };

  return kPrimitiveTypeNames;
}

void toAppend(
    const facebook::velox::exec::TypeSignature& signature,
    std::string* result) {
  result->append(signature.toString());
}

std::string TypeSignature::toString() const {
  std::ostringstream out;
  out << baseName_;
  if (!parameters_.empty()) {
    out << "(" << folly::join(",", parameters_) << ")";
  }
  return out.str();
}

std::string FunctionSignature::argumentsToString() const {
  std::vector<std::string> arguments;
  auto size = argumentTypes_.size();
  arguments.reserve(size);
  for (auto i = 0; i < size; ++i) {
    auto arg = argumentTypes_.at(i).toString();
    if (constantArguments_.at(i)) {
      arguments.emplace_back("constant " + arg);
    } else {
      arguments.emplace_back(arg);
    }
  }
  std::ostringstream out;
  out << folly::join(",", arguments);
  if (variableArity_) {
    out << "...";
  }
  return out.str();
}

std::string FunctionSignature::toString() const {
  std::ostringstream out;
  out << "(" << argumentsToString() << ") -> " << returnType_.toString();
  return out.str();
}

size_t findNextComma(const std::string& str, size_t start) {
  int cnt = 0;
  for (auto i = start; i < str.size(); i++) {
    if (str[i] == '(') {
      cnt++;
    } else if (str[i] == ')') {
      cnt--;
    } else if (cnt == 0 && str[i] == ',') {
      return i;
    }
  }

  return std::string::npos;
}

TypeSignature parseTypeSignature(const std::string& signature) {
  auto parenPos = signature.find('(');
  if (parenPos == std::string::npos) {
    return TypeSignature(signature, {});
  }

  auto baseName = signature.substr(0, parenPos);
  std::vector<TypeSignature> nestedTypes;

  auto endParenPos = signature.rfind(')');
  VELOX_CHECK(
      endParenPos != std::string::npos,
      "Couldn't find the closing parenthesis.");

  auto prevPos = parenPos + 1;
  auto commaPos = findNextComma(signature, prevPos);
  while (commaPos != std::string::npos) {
    auto token = signature.substr(prevPos, commaPos - prevPos);
    boost::algorithm::trim(token);
    nestedTypes.emplace_back(parseTypeSignature(token));

    prevPos = commaPos + 1;
    commaPos = findNextComma(signature, prevPos);
  }

  auto token = signature.substr(prevPos, endParenPos - prevPos);
  boost::algorithm::trim(token);
  nestedTypes.emplace_back(parseTypeSignature(token));

  return TypeSignature(baseName, std::move(nestedTypes));
}

namespace {
/// Returns true only if 'str' contains digits.
bool isPositiveInteger(const std::string& str) {
  return !str.empty() &&
      std::find_if(str.begin(), str.end(), [](unsigned char c) {
        return !std::isdigit(c);
      }) == str.end();
}

void validateBaseTypeAndCollectTypeParams(
    const std::unordered_map<std::string, SignatureVariable>& variables,
    const TypeSignature& arg,
    std::unordered_set<std::string>& collectedTypeVariables,
    bool isReturnType) {
  if (!variables.count(arg.baseName())) {
    auto typeName = boost::algorithm::to_upper_copy(arg.baseName());

    if (typeName == "ANY") {
      VELOX_USER_CHECK(
          !isReturnType, "Type 'Any' cannot appear in return type");

      VELOX_USER_CHECK(
          arg.parameters().empty(), "Type 'Any' cannot have parameters")
      return;
    }

    if (!isPositiveInteger(typeName) &&
        !tryMapNameToTypeKind(typeName).has_value()) {
      VELOX_USER_CHECK(hasType(typeName), "Type doesn't exist: {}", typeName);
    }

    // Ensure all params are similarly supported.
    for (auto& param : arg.parameters()) {
      validateBaseTypeAndCollectTypeParams(
          variables, param, collectedTypeVariables, isReturnType);
    }

  } else {
    // This means base type is a TypeParameter, ensure
    // it doesn't have parameters, e.g M[T].
    VELOX_USER_CHECK(
        arg.parameters().empty(),
        "Named type cannot have parameters : {}",
        arg.toString())
    collectedTypeVariables.insert(arg.baseName());
  }
}

void validate(
    const std::unordered_map<std::string, SignatureVariable>& variables,
    const TypeSignature& returnType,
    const std::vector<TypeSignature>& argumentTypes,
    const std::vector<bool>& constantArguments) {
  std::unordered_set<std::string> usedVariables;
  // Validate the argument types.
  for (const auto& arg : argumentTypes) {
    // Is base type a type parameter or a built in type ?
    validateBaseTypeAndCollectTypeParams(variables, arg, usedVariables, false);
  }

  // All type variables should apear in the inputs arguments.
  for (auto& [name, variable] : variables) {
    if (variable.isTypeParameter()) {
      VELOX_USER_CHECK(
          usedVariables.count(name),
          "Some type variables are not used in the inputs");
    }
  }

  validateBaseTypeAndCollectTypeParams(
      variables, returnType, usedVariables, true);

  VELOX_USER_CHECK_EQ(
      usedVariables.size(),
      variables.size(),
      "Some integer variables are not used");

  VELOX_USER_CHECK_EQ(
      argumentTypes.size(),
      constantArguments.size(),
      "Argument types size is not equal to constant flags");
}

} // namespace

SignatureVariable::SignatureVariable(
    std::string name,
    std::optional<std::string> constraint,
    ParameterType type,
    bool knownTypesOnly)
    : name_{std::move(name)},
      constraint_(constraint.has_value() ? std::move(constraint.value()) : ""),
      type_{type},
      knownTypesOnly_(knownTypesOnly) {
  VELOX_CHECK(
      !knownTypesOnly_ || isTypeParameter(),
      "Non-Type variables cannot have the knownTypesOnly constraint");

  VELOX_CHECK(
      isIntegerParameter() || (isTypeParameter() && constraint_.empty()),
      "Type variables cannot have constraints");
}

FunctionSignature::FunctionSignature(
    std::unordered_map<std::string, SignatureVariable> variables,
    TypeSignature returnType,
    std::vector<TypeSignature> argumentTypes,
    std::vector<bool> constantArguments,
    bool variableArity)
    : variables_{std::move(variables)},
      returnType_{std::move(returnType)},
      argumentTypes_{std::move(argumentTypes)},
      constantArguments_{std::move(constantArguments)},
      variableArity_{variableArity} {
  validate(variables_, returnType_, argumentTypes_, constantArguments_);
}

std::string AggregateFunctionSignature::toString() const {
  std::ostringstream out;
  out << "(" << argumentsToString() << ") -> " << intermediateType_.toString()
      << " -> " << returnType().toString();
  return out.str();
}

FunctionSignaturePtr FunctionSignatureBuilder::build() {
  VELOX_CHECK(returnType_.has_value());
  return std::make_shared<FunctionSignature>(
      std::move(variables_),
      returnType_.value(),
      std::move(argumentTypes_),
      std::move(constantArguments_),
      variableArity_);
}

std::shared_ptr<AggregateFunctionSignature>
AggregateFunctionSignatureBuilder::build() {
  VELOX_CHECK(returnType_.has_value());
  VELOX_CHECK(intermediateType_.has_value());
  return std::make_shared<AggregateFunctionSignature>(
      std::move(variables_),
      returnType_.value(),
      intermediateType_.value(),
      std::move(argumentTypes_),
      std::move(constantArguments_),
      variableArity_);
}
} // namespace facebook::velox::exec
