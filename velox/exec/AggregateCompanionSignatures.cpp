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

#include "velox/exec/AggregateCompanionSignatures.h"

#include "velox/expression/FunctionSignature.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {

namespace {

// Add type variables used in `type` to `usedVariables`.
void addUsedVariablesInType(
    const TypeSignature& type,
    const std::unordered_map<std::string, SignatureVariable>& allVariables,
    std::unordered_map<std::string, SignatureVariable>& usedVariables) {
  auto iter = allVariables.find(type.baseName());
  if (iter != allVariables.end()) {
    usedVariables.emplace(iter->first, iter->second);
  }
  for (const auto& parameter : type.parameters()) {
    addUsedVariablesInType(parameter, allVariables, usedVariables);
  }
}

// Return type variables used in `types`.
std::unordered_map<std::string, SignatureVariable> usedTypeVariables(
    const std::vector<TypeSignature>& types,
    const std::unordered_map<std::string, SignatureVariable>& allVariables) {
  std::unordered_map<std::string, SignatureVariable> usedVariables;
  for (const auto& type : types) {
    addUsedVariablesInType(type, allVariables, usedVariables);
  }
  return usedVariables;
}

// Result type is resolvable from intermediate type iff all variables in the
// result type appears in the intermediate type as well.
bool isResultTypeResolvableGivenIntermediateType(
    const AggregateFunctionSignaturePtr& signature) {
  auto& allVariables = signature->variables();
  if (allVariables.empty()) {
    return true;
  }

  std::vector<TypeSignature> intermediateType{signature->intermediateType()};
  std::vector<TypeSignature> resultType{signature->returnType()};
  auto variablesInIntermediate =
      usedTypeVariables(intermediateType, allVariables);
  auto variablesInResult = usedTypeVariables(resultType, allVariables);

  for (const auto& [variable, _] : variablesInResult) {
    if (!variablesInIntermediate.count(variable)) {
      return false;
    }
  }
  return true;
}

// Return a string that is preorder traveral of `type`. For example, for
// row(bigint, array(double)), return a string "row_bigint_array_double".
std::string toSuffixString(const TypeSignature& type) {
  auto name = type.baseName();
  // For primitive and decimal types, return their names.
  if (type.parameters().empty() || isCommonDecimalName(name) ||
      isDecimalName(name)) {
    return name;
  }
  auto upperName = boost::algorithm::to_upper_copy(name);
  if (upperName == "ARRAY") {
    return "array_" + toSuffixString(type.parameters()[0]);
  }
  if (upperName == "MAP") {
    return "map_" + toSuffixString(type.parameters()[0]) + "_" +
        toSuffixString(type.parameters()[1]);
  }
  if (upperName == "ROW") {
    std::string result = "row";
    for (const auto& child : type.parameters()) {
      result = result + "_" + toSuffixString(child);
    }
    result += "_endrow";
    return result;
  }
  VELOX_UNREACHABLE("Unknown type: {}.", type.toString());
}

} // namespace

std::vector<AggregateFunctionSignaturePtr>
CompanionSignatures::partialFunctionSignatures(
    const std::vector<AggregateFunctionSignaturePtr>& signatures) {
  std::vector<AggregateFunctionSignaturePtr> partialSignatures;
  for (const auto& signature : signatures) {
    if (!isResultTypeResolvableGivenIntermediateType(signature)) {
      continue;
    }
    std::vector<TypeSignature> usedTypes = signature->argumentTypes();
    usedTypes.push_back(signature->intermediateType());
    auto variables = usedTypeVariables(usedTypes, signature->variables());

    partialSignatures.push_back(std::make_shared<AggregateFunctionSignature>(
        /*variables*/ variables,
        /*returnType*/ signature->intermediateType(),
        /*intermediateType*/ signature->intermediateType(),
        /*argumentTypes*/ signature->argumentTypes(),
        /*constantArguments*/ signature->constantArguments(),
        /*variableArity*/ signature->variableArity()));
  }
  return partialSignatures;
}

std::string CompanionSignatures::partialFunctionName(const std::string& name) {
  return name + "_partial";
}

AggregateFunctionSignaturePtr CompanionSignatures::mergeFunctionSignature(
    const AggregateFunctionSignaturePtr& signature) {
  if (!isResultTypeResolvableGivenIntermediateType(signature)) {
    return nullptr;
  }

  std::vector<TypeSignature> usedTypes = {signature->intermediateType()};
  auto variables = usedTypeVariables(usedTypes, signature->variables());
  return std::make_shared<AggregateFunctionSignature>(
      /*variables*/ variables,
      /*returnType*/ signature->intermediateType(),
      /*intermediateType*/ signature->intermediateType(),
      /*argumentTypes*/
      std::vector<TypeSignature>{signature->intermediateType()},
      /*constantArguments*/ std::vector<bool>{false},
      /*variableArity*/ false);
}

std::vector<AggregateFunctionSignaturePtr>
CompanionSignatures::mergeFunctionSignatures(
    const std::vector<AggregateFunctionSignaturePtr>& signatures) {
  return processSignaturesOfDistinctIntermediateTypes(
      signatures, [](const AggregateFunctionSignaturePtr& signature) {
        return mergeFunctionSignature(signature);
      });
}

std::string CompanionSignatures::mergeFunctionName(const std::string& name) {
  return name + "_merge";
}

bool CompanionSignatures::hasSameIntermediateTypesAcrossSignatures(
    const std::vector<AggregateFunctionSignaturePtr>& signatures) {
  std::unordered_set<TypeSignature> seenTypes;
  for (const auto& signature : signatures) {
    auto normalizdType =
        normalizeType(signature->intermediateType(), signature->variables());
    if (seenTypes.count(normalizdType)) {
      return true;
    }
    seenTypes.insert(normalizdType);
  }
  return false;
}

AggregateFunctionSignaturePtr
CompanionSignatures::mergeExtractFunctionSignature(
    const AggregateFunctionSignaturePtr& signature) {
  if (!isResultTypeResolvableGivenIntermediateType(signature)) {
    return nullptr;
  }

  std::vector<TypeSignature> usedTypes = {
      signature->intermediateType(), signature->returnType()};
  auto variables = usedTypeVariables(usedTypes, signature->variables());
  return std::make_shared<AggregateFunctionSignature>(
      /*variables*/ variables,
      /*returnType*/ signature->returnType(),
      /*intermediateType*/ signature->intermediateType(),
      /*argumentTypes*/
      std::vector<TypeSignature>{signature->intermediateType()},
      /*constantArguments*/ std::vector<bool>{false},
      /*variableArity*/ false);
}

std::vector<AggregateFunctionSignaturePtr>
CompanionSignatures::mergeExtractFunctionSignatures(
    const std::vector<AggregateFunctionSignaturePtr>& signatures) {
  return processSignaturesOfDistinctTypes<AggregateFunctionSignaturePtr>(
      signatures, [](const AggregateFunctionSignaturePtr& signature) {
        return mergeExtractFunctionSignature(signature);
      });
}

std::string CompanionSignatures::mergeExtractFunctionNameWithSuffix(
    const std::string& name,
    const TypeSignature& resultType) {
  return name + "_merge_extract_" + toSuffixString(resultType);
}

std::string CompanionSignatures::mergeExtractFunctionName(
    const std::string& name) {
  return name + "_merge_extract";
}

FunctionSignaturePtr CompanionSignatures::extractFunctionSignature(
    const AggregateFunctionSignaturePtr& signature) {
  if (!isResultTypeResolvableGivenIntermediateType(signature)) {
    return nullptr;
  }

  std::vector<TypeSignature> usedTypes = {
      signature->intermediateType(), signature->returnType()};
  auto variables = usedTypeVariables(usedTypes, signature->variables());
  return std::make_shared<FunctionSignature>(
      /*variables*/ variables,
      /*returnType*/ signature->returnType(),
      /*argumentTypes*/
      std::vector<TypeSignature>{signature->intermediateType()},
      /*constantArguments*/ std::vector<bool>{false},
      /*variableArity*/ false);
}

std::string CompanionSignatures::extractFunctionNameWithSuffix(
    const std::string& name,
    const TypeSignature& resultType) {
  return name + "_extract_" + toSuffixString(resultType);
}

std::vector<FunctionSignaturePtr>
CompanionSignatures::extractFunctionSignatures(
    const std::vector<AggregateFunctionSignaturePtr>& signatures) {
  return processSignaturesOfDistinctTypes<FunctionSignaturePtr>(
      signatures, [](const AggregateFunctionSignaturePtr& signature) {
        return extractFunctionSignature(signature);
      });
}

std::string CompanionSignatures::extractFunctionName(const std::string& name) {
  return name + "_extract";
}

std::unordered_map<TypeSignature, std::vector<AggregateFunctionSignaturePtr>>
CompanionSignatures::groupSignaturesByReturnType(
    const std::vector<AggregateFunctionSignaturePtr>& signatures) {
  std::unordered_map<TypeSignature, std::vector<AggregateFunctionSignaturePtr>>
      result;
  for (const auto& signature : signatures) {
    result[signature->returnType()].push_back(signature);
  }
  return result;
}

TypeSignature CompanionSignatures::normalizeTypeImpl(
    const TypeSignature& type,
    const std::unordered_map<std::string, SignatureVariable>& allVariables,
    std::unordered_map<std::string, std::string>& renamedVariables) {
  auto baseName = type.baseName();

  // Already renamed variables.
  if (renamedVariables.count(baseName)) {
    return TypeSignature{renamedVariables[baseName], {}};
  }
  // Variales to be renamed in consistent manner.
  if (allVariables.count(baseName)) {
    auto normalizedName = fmt::format("T{}", renamedVariables.size());
    renamedVariables[baseName] = normalizedName;
    return TypeSignature{normalizedName, {}};
  }
  // Primitive types.
  if (type.parameters().empty()) {
    return type;
  }
  // Complex types.
  std::vector<TypeSignature> normalizedParameters;
  for (const auto& param : type.parameters()) {
    normalizedParameters.push_back(
        normalizeTypeImpl(param, allVariables, renamedVariables));
  }
  return TypeSignature{baseName, normalizedParameters};
}

TypeSignature CompanionSignatures::normalizeType(
    const TypeSignature& type,
    const std::unordered_map<std::string, SignatureVariable>& allVariables) {
  std::unordered_map<std::string, std::string> renamedVariables;
  return normalizeTypeImpl(type, allVariables, renamedVariables);
}

} // namespace facebook::velox::exec
