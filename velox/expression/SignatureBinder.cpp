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
#include <optional>

#include "velox/expression/SignatureBinder.h"
#include "velox/expression/type_calculation/TypeCalculation.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {

namespace {
bool isAny(const TypeSignature& typeSignature) {
  return typeSignature.baseName() == "any";
}

/// Returns true only if 'str' contains digits.
bool isPositiveInteger(const std::string& str) {
  return !str.empty() &&
      std::find_if(str.begin(), str.end(), [](unsigned char c) {
        return !std::isdigit(c);
      }) == str.end();
}

std::string buildCalculation(
    const std::string& variable,
    const std::string& calculation) {
  return fmt::format("{}={}", variable, calculation);
}

std::optional<int64_t> tryResolveLongLiteral(
    const TypeSignature& parameter,
    const std::unordered_map<std::string, SignatureVariable>& variables,
    std::unordered_map<std::string, int>& integerVariablesBindings) {
  auto variable = parameter.baseName();

  if (isPositiveInteger(variable)) {
    // Handle constant.
    return atoi(variable.c_str());
  };

  if (integerVariablesBindings.count(variable)) {
    return integerVariablesBindings.at(variable);
  }

  auto it = variables.find(variable);
  if (it == variables.end()) {
    return std::nullopt;
  }

  auto& constraints = it->second.constraint();

  if (constraints == "") {
    return std::nullopt;
  }

  // Try to assign value based on constraints.
  // Check constraints and evaluate.
  auto calculation = buildCalculation(variable, constraints);
  expression::calculation::evaluate(calculation, integerVariablesBindings);
  VELOX_CHECK(
      integerVariablesBindings.count(variable),
      "Variable {} calculation failed.",
      variable);
  return integerVariablesBindings.at(variable);
}

// If the parameter is a named field from a row, ensure the names are
// compatible. For example:
//
// > row(bigint) - binds any row with bigint as field.
// > row(foo bigint) - only binds rows where bigint field is named foo.
bool checkNamedRowField(
    const TypeSignature& signature,
    const TypePtr& actualType,
    size_t idx) {
  if (signature.rowFieldName().has_value() &&
      (*signature.rowFieldName() != asRowType(actualType)->nameOf(idx))) {
    return false;
  }
  return true;
}

} // namespace

bool SignatureBinder::tryBind() {
  const auto& formalArgs = signature_.argumentTypes();
  auto formalArgsCnt = formalArgs.size();

  if (signature_.variableArity()) {
    if (actualTypes_.size() < formalArgsCnt - 1) {
      return false;
    }

    if (!isAny(signature_.argumentTypes().back())) {
      if (actualTypes_.size() > formalArgsCnt) {
        auto& type = actualTypes_[formalArgsCnt - 1];
        for (auto i = formalArgsCnt; i < actualTypes_.size(); i++) {
          if (!type->equivalent(*actualTypes_[i]) &&
              actualTypes_[i]->kind() != TypeKind::UNKNOWN) {
            return false;
          }
        }
      }
    }
  } else {
    if (formalArgsCnt != actualTypes_.size()) {
      return false;
    }
  }

  bool allBound = true;
  for (auto i = 0; i < formalArgsCnt && i < actualTypes_.size(); i++) {
    if (actualTypes_[i]) {
      if (!SignatureBinderBase::tryBind(formalArgs[i], actualTypes_[i])) {
        allBound = false;
      }
    } else {
      allBound = false;
    }
  }
  return allBound;
}

bool SignatureBinderBase::checkOrSetIntegerParameter(
    const std::string& parameterName,
    int value) {
  if (isPositiveInteger(parameterName)) {
    return atoi(parameterName.c_str()) == value;
  }
  if (!variables().count(parameterName)) {
    // Return false if the parameter is not found in the signature.
    return false;
  }

  if (integerVariablesBindings_.count(parameterName)) {
    // Return false if the parameter is found with a different value.
    if (integerVariablesBindings_[parameterName] != value) {
      return false;
    }
  }
  // Bind the variable.
  integerVariablesBindings_[parameterName] = value;
  return true;
}

bool SignatureBinderBase::tryBind(
    const exec::TypeSignature& typeSignature,
    const TypePtr& actualType) {
  if (isAny(typeSignature)) {
    return true;
  }

  const auto baseName = typeSignature.baseName();

  if (variables().count(baseName)) {
    // Variables cannot have further parameters.
    VELOX_CHECK(
        typeSignature.parameters().empty(),
        "Variables with parameters are not supported");
    auto& variable = variables().at(baseName);
    VELOX_CHECK(variable.isTypeParameter(), "Not expecting integer variable");

    if (typeVariablesBindings_.count(baseName)) {
      // If the the variable type is already mapped to a concrete type, make
      // sure the mapped type is equivalent to the actual type.
      return typeVariablesBindings_[baseName]->equivalent(*actualType);
    }

    if (actualType->isUnKnown() && variable.knownTypesOnly()) {
      return false;
    }

    if (variable.orderableTypesOnly() && !actualType->isOrderable()) {
      return false;
    }

    if (variable.comparableTypesOnly() && !actualType->isComparable()) {
      return false;
    }

    typeVariablesBindings_[baseName] = actualType;
    return true;
  }

  // Type is not a variable.
  auto typeName = boost::algorithm::to_upper_copy(baseName);
  auto actualTypeName =
      boost::algorithm::to_upper_copy(std::string(actualType->name()));

  if (typeName != actualTypeName) {
    return false;
  }

  const auto& params = typeSignature.parameters();
  // Type Parameters can recurse.
  if (params.size() != actualType->parameters().size()) {
    return false;
  }

  for (auto i = 0; i < params.size(); i++) {
    const auto& actualParameter = actualType->parameters()[i];
    switch (actualParameter.kind) {
      case TypeParameterKind::kLongLiteral:
        if (!checkOrSetIntegerParameter(
                params[i].baseName(), actualParameter.longLiteral.value())) {
          return false;
        }
        break;
      case TypeParameterKind::kType:
        if (!checkNamedRowField(params[i], actualType, i)) {
          return false;
        }

        if (!tryBind(params[i], actualParameter.type)) {
          return false;
        }
        break;
    }
  }
  return true;
}

TypePtr SignatureBinder::tryResolveType(
    const exec::TypeSignature& typeSignature,
    const std::unordered_map<std::string, SignatureVariable>& variables,
    const std::unordered_map<std::string, TypePtr>& typeVariablesBindings,
    std::unordered_map<std::string, int>& integerVariablesBindings) {
  const auto baseName = typeSignature.baseName();

  if (variables.count(baseName)) {
    auto it = typeVariablesBindings.find(baseName);
    if (it == typeVariablesBindings.end()) {
      return nullptr;
    }
    return it->second;
  }

  // Type is not a variable.
  auto typeName = boost::algorithm::to_upper_copy(baseName);

  const auto& params = typeSignature.parameters();
  std::vector<TypeParameter> typeParameters;
  for (auto& param : params) {
    auto literal =
        tryResolveLongLiteral(param, variables, integerVariablesBindings);
    if (literal.has_value()) {
      typeParameters.push_back(TypeParameter(literal.value()));
      continue;
    }

    auto type = tryResolveType(
        param, variables, typeVariablesBindings, integerVariablesBindings);
    if (!type) {
      return nullptr;
    }
    typeParameters.push_back(TypeParameter(type));
  }

  try {
    if (auto type = getType(typeName, typeParameters)) {
      return type;
    }
  } catch (const std::exception&) {
    // TODO Perhaps, modify getType to add suppress-errors flag.
    return nullptr;
  }

  auto typeKind = tryMapNameToTypeKind(typeName);
  if (!typeKind.has_value()) {
    return nullptr;
  }

  // getType(parameters) function doesn't support OPAQUE type.
  switch (*typeKind) {
    case TypeKind::OPAQUE:
      return OpaqueType::create<void>();
    default:
      return nullptr;
  }
}
} // namespace facebook::velox::exec
