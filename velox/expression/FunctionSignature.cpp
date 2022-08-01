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
#include "velox/expression/FunctionSignature.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/trim.hpp>
#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {

std::string sanitizeFunctionName(const std::string& name) {
  std::string sanitizedName;
  sanitizedName.resize(name.size());
  std::transform(
      name.begin(), name.end(), sanitizedName.begin(), [](unsigned char c) {
        return std::tolower(c);
      });

  return sanitizedName;
}

void toAppend(
    const facebook::velox::exec::TypeSignature& signature,
    std::string* result) {
  result->append(signature.toString());
}

std::string TypeSignature::toString() const {
  std::ostringstream out;
  out << baseType_;
  auto typeName = boost::algorithm::to_upper_copy(baseType_);
  if (isCommonDecimalName(typeName)) {
    out << "(" << variables_[0] << ", " << variables_[1] << ")";
  }
  if (!parameters_.empty()) {
    out << "(" << folly::join(",", parameters_) << ")";
  }
  return out.str();
}

std::string FunctionSignature::toString() const {
  std::ostringstream out;
  out << "(" << folly::join(",", argumentTypes_);
  if (variableArity_) {
    out << "...";
  }
  out << ") -> " << returnType_.toString();
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

  auto baseType = signature.substr(0, parenPos);

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

  auto typeName = boost::algorithm::to_upper_copy(baseType);
  if (isCommonDecimalName(typeName)) {
    std::vector<std::string> vars(2);
    vars[0] = nestedTypes[0].baseType();
    vars[1] = nestedTypes[1].baseType();
    return TypeSignature(baseType, {}, std::move(vars));
  }
  return TypeSignature(baseType, std::move(nestedTypes));
}

namespace {
void validateBaseTypeAndCollectTypeParams(
    const std::unordered_set<std::string>& typeParams,
    const TypeSignature& arg,
    std::unordered_set<std::string>& collectedTypeVariables) {
  if (!typeParams.count(arg.baseType())) {
    auto typeName = boost::algorithm::to_upper_copy(arg.baseType());

    if (typeName == "ANY") {
      VELOX_USER_CHECK(
          arg.parameters().empty(), "Type 'Any' cannot have parameters")
      return;
    }

    if (isDecimalName(typeName)) {
      VELOX_USER_FAIL("Use 'DECIMAL' in the signature.");
    }

    if (isCommonDecimalName(typeName)) {
      return;
    }

    if (!typeExists(typeName)) {
      // Check to ensure base type is supported.
      mapNameToTypeKind(typeName);
    }

    // Ensure all params are similarly supported.
    for (auto& param : arg.parameters()) {
      validateBaseTypeAndCollectTypeParams(
          typeParams, param, collectedTypeVariables);
    }

  } else {
    // This means base type is a TypeParameter, ensure
    // it doesn't have parameters, e.g M[T].
    VELOX_USER_CHECK(
        arg.parameters().empty(),
        "Named type cannot have parameters : {}",
        arg.toString())
    collectedTypeVariables.insert(arg.baseType());
  }
}

void validate(
    const std::vector<TypeVariableConstraint>& typeVariableConstants,
    const TypeSignature& returnType,
    const std::vector<TypeSignature>& argumentTypes) {
  // Validate that the type params are unique.
  std::unordered_set<std::string> typeNames(typeVariableConstants.size());
  for (const auto& variable : typeVariableConstants) {
    VELOX_USER_CHECK(
        typeNames.insert(variable.name()).second,
        "Type parameter declared twice {}",
        variable.name());
  }

  std::unordered_set<std::string> usedTypeVariables;
  // Validate the argument types.
  for (const auto& arg : argumentTypes) {
    // Is base type a type parameter or a built in type ?
    validateBaseTypeAndCollectTypeParams(typeNames, arg, usedTypeVariables);
  }

  // Similarly validate for return type.
  validateBaseTypeAndCollectTypeParams(
      typeNames, returnType, usedTypeVariables);

  VELOX_USER_CHECK_EQ(
      usedTypeVariables.size(),
      typeNames.size(),
      "Not all type parameters used");
}

} // namespace

FunctionSignature::FunctionSignature(
    std::vector<TypeVariableConstraint> typeVariableConstants,
    TypeSignature returnType,
    std::vector<TypeSignature> argumentTypes,
    bool variableArity)
    : typeVariableConstants_{std::move(typeVariableConstants)},
      returnType_{std::move(returnType)},
      argumentTypes_{std::move(argumentTypes)},
      variableArity_{variableArity} {
  validate(typeVariableConstants_, returnType_, argumentTypes_);
}

FunctionSignature::FunctionSignature(
    std::vector<TypeVariableConstraint> typeVariableConstants,
    std::vector<TypeVariableConstraint> variables,
    TypeSignature returnType,
    std::vector<TypeSignature> argumentTypes,
    bool variableArity)
    : typeVariableConstants_{std::move(typeVariableConstants)},
      variables_{std::move(variables)},
      returnType_{std::move(returnType)},
      argumentTypes_{std::move(argumentTypes)},
      variableArity_{variableArity} {
  validate(typeVariableConstants_, returnType_, argumentTypes_);
}

FunctionSignaturePtr FunctionSignatureBuilder::build() {
  VELOX_CHECK(returnType_.has_value());
  return std::make_shared<FunctionSignature>(
      std::move(typeVariableConstants_),
      std::move(variables_),
      returnType_.value(),
      std::move(argumentTypes_),
      variableArity_);
}

std::shared_ptr<AggregateFunctionSignature>
AggregateFunctionSignatureBuilder::build() {
  VELOX_CHECK(returnType_.has_value());
  VELOX_CHECK(intermediateType_.has_value());
  return std::make_shared<AggregateFunctionSignature>(
      std::move(typeVariableConstants_),
      std::move(variables_),
      returnType_.value(),
      intermediateType_.value(),
      std::move(argumentTypes_),
      variableArity_);
}

} // namespace facebook::velox::exec
