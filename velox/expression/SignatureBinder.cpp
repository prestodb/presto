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
#include "velox/expression/SignatureBinder.h"
#include <boost/algorithm/string.hpp>
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

TypePtr inferDecimalType(
    const exec::TypeSignature& typeSignature,
    const std::unordered_map<std::string, std::string>& constraints,
    std::unordered_map<std::string, std::optional<int>>& integerParameters) {
  if (typeSignature.parameters().size() != 2) {
    // Decimals must have two parameters.
    return nullptr;
  }
  const auto& precisionVar = typeSignature.parameters()[0].baseName();
  const auto& scaleVar = typeSignature.parameters()[1].baseName();
  int precision = 0;
  int scale = 0;
  // Determine precision.
  if (isPositiveInteger(precisionVar)) {
    // Handle constant.
    precision = atoi(precisionVar.c_str());
  } else if (auto it = integerParameters.find(precisionVar);
             it != integerParameters.end()) {
    // Check if it is already computed.
    if (it->second.has_value()) {
      precision = it->second.value();
    } else if (auto ct = constraints.find(precisionVar);
               ct != constraints.end()) {
      // Check constraints and evaluate.
      auto precisionCalculation = buildCalculation(precisionVar, ct->second);
      expression::calculation::evaluate(
          precisionCalculation, integerParameters);
      const auto result = integerParameters.at(precisionVar);
      VELOX_CHECK(
          result.has_value(), "Variable {} calculation failed.", precisionVar)
      precision = result.value();
    } else {
      // Cannot evaluate further.
      return nullptr;
    }
  } else {
    return nullptr;
  }
  // Determine scale.
  if (isPositiveInteger(scaleVar)) {
    // Handle constant.
    scale = atoi(scaleVar.c_str());
  } else if (auto it = integerParameters.find(scaleVar);
             it != integerParameters.end()) {
    // Check if it is already computed.
    if (it->second.has_value()) {
      scale = it->second.value();
    } else if (auto ct = constraints.find(scaleVar); ct != constraints.end()) {
      // Check constraints and evaluate.
      auto scaleCalculation = buildCalculation(scaleVar, ct->second);
      expression::calculation::evaluate(scaleCalculation, integerParameters);
      const auto result = integerParameters.at(scaleVar);
      VELOX_CHECK(
          result.has_value(), "Variable {} calculation failed.", scaleVar)
      scale = result.value();
    } else {
      // Cannot evaluate further.
      return nullptr;
    }
  } else {
    return nullptr;
  }
  return DECIMAL(precision, scale);
}

bool isConcreteType(
    const std::unordered_map<std::string, TypePtr>& typeParameters,
    const std::unordered_map<std::string, std::optional<int>>&
        integerParameters,
    const std::string& name) {
  return (typeParameters.count(name) == 0) &&
      (integerParameters.count(name) == 0);
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
      if (!tryBind(formalArgs[i], actualTypes_[i])) {
        allBound = false;
      }
    } else {
      allBound = false;
    }
  }
  return allBound;
}

bool SignatureBinder::checkOrSetIntegerParameter(
    const std::string& parameterName,
    int value) {
  auto it = integerParameters_.find(parameterName);
  // Return false if the parameter is not found.
  if (it == integerParameters_.end()) {
    return false;
  }
  // Return false if the parameter is found with a different value.
  if (it->second.has_value() && it->second.value() != value) {
    return false;
  }
  it->second = value;
  return true;
}

bool SignatureBinder::tryBindIntegerParameters(
    const std::vector<exec::TypeSignature>& parameters,
    const TypePtr& actualType) {
  // Decimal types
  if (actualType->isShortDecimal() || actualType->isLongDecimal()) {
    VELOX_CHECK_EQ(parameters.size(), 2);
    const auto& [precision, scale] = getDecimalPrecisionScale(*actualType);
    return checkOrSetIntegerParameter(parameters[0].baseName(), precision) &&
        checkOrSetIntegerParameter(parameters[1].baseName(), scale);
  }
  return false;
}

bool SignatureBinder::tryBind(
    const exec::TypeSignature& typeSignature,
    const TypePtr& actualType) {
  if (isAny(typeSignature)) {
    return true;
  }
  const auto baseName = typeSignature.baseName();
  if (isConcreteType(typeParameters_, integerParameters_, baseName)) {
    auto typeName = boost::algorithm::to_upper_copy(baseName);

    if (typeName != actualType->kindName()) {
      if (!(isCommonDecimalName(typeName) &&
            (actualType->isLongDecimal() || actualType->isShortDecimal()))) {
        return false;
      }
    }

    const auto& params = typeSignature.parameters();
    // Integer parameters have to be resolved here.
    // We assume integer parameters start from the first parameter if present.
    if (params.size() > 0 &&
        integerParameters_.count(params[0].baseName()) != 0) {
      return tryBindIntegerParameters(params, actualType);
    }
    // Type Parameters can recurse.
    for (auto i = 0; i < params.size(); i++) {
      if (!tryBind(params[i], actualType->childAt(i))) {
        return false;
      }
    }
    return true;
  }

  // Variables cannot have further parameters.
  VELOX_CHECK_EQ(
      typeSignature.parameters().size(),
      0,
      "Variables with parameters are not supported");

  // Resolve type parameters parameters.
  if (auto it = typeParameters_.find(baseName); it != typeParameters_.end()) {
    if (it->second == nullptr) {
      it->second = actualType;
      return true;
    }
    return it->second->equivalent(*actualType);
  }
  return false;
}

TypePtr SignatureBinder::tryResolveType(
    const exec::TypeSignature& typeSignature) {
  return tryResolveType(
      typeSignature, typeParameters_, constraints_, integerParameters_);
}

// static
TypePtr SignatureBinder::tryResolveType(
    const exec::TypeSignature& typeSignature,
    const std::unordered_map<std::string, TypePtr>& typeParameters,
    const std::unordered_map<std::string, std::string>& constraints,
    std::unordered_map<std::string, std::optional<int>>& integerParameters) {
  const auto baseName = typeSignature.baseName();
  if (isConcreteType(typeParameters, integerParameters, baseName)) {
    auto typeName = boost::algorithm::to_upper_copy(baseName);
    if (isDecimalName(typeName) || isCommonDecimalName(typeName)) {
      return inferDecimalType(typeSignature, constraints, integerParameters);
    }
    const auto& params = typeSignature.parameters();
    std::vector<TypePtr> children;
    children.reserve(params.size());
    for (auto& param : params) {
      auto type =
          tryResolveType(param, typeParameters, constraints, integerParameters);
      if (!type) {
        return nullptr;
      }
      children.emplace_back(type);
    }

    if (auto type = getType(typeName, children)) {
      return type;
    }

    auto typeKind = tryMapNameToTypeKind(typeName);
    if (!typeKind.has_value()) {
      return nullptr;
    }

    // createType(kind) function doesn't support ROW, UNKNOWN and OPAQUE type
    // kinds.
    if (*typeKind == TypeKind::ROW) {
      return ROW(std::move(children));
    }
    if (*typeKind == TypeKind::UNKNOWN) {
      return UNKNOWN();
    }
    if (*typeKind == TypeKind::OPAQUE) {
      return OpaqueType::create<void>();
    }
    return createType(*typeKind, std::move(children));
  } else if (typeParameters.count(baseName) != 0) {
    return typeParameters.at(baseName);
  }
  return nullptr;
}
} // namespace facebook::velox::exec
