/*
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

#include "presto_cpp/main/types/FunctionMetadata.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::presto {

namespace {

constexpr char const* kDefaultSchema = "default";

// The keys in velox function maps are of the format
// catalog.schema.function_name. This utility function extracts the
// function_name and the function visibility from this string.

// Note: A function is considered hidden if it is not found within a
// function namespace.
std::pair<std::string, protocol::SqlFunctionVisibility>
getFunctionNameAndVisibility(const std::string& registeredFunctionName) {
  std::vector<std::string> pieces;
  folly::split('.', registeredFunctionName, pieces, true);
  protocol::SqlFunctionVisibility functionVisibility = pieces.size() == 1
      ? protocol::SqlFunctionVisibility::HIDDEN
      : protocol::SqlFunctionVisibility::PUBLIC;
  return std::make_pair(pieces.back(), functionVisibility);
}

const protocol::AggregationFunctionMetadata getAggregationFunctionMetadata(
    const AggregateFunctionSignature& aggregateFunctionSignature) {
  protocol::AggregationFunctionMetadata aggregationFunctionMetadata;
  aggregationFunctionMetadata.intermediateType =
      aggregateFunctionSignature.intermediateType().toString();
  /// TODO: Set to true for now. To be read from an existing mapping of
  /// aggregate to order sensitivity which needs to be added.
  aggregationFunctionMetadata.isOrderSensitive = true;
  return aggregationFunctionMetadata;
}

const protocol::RoutineCharacteristics getRoutineCharacteristics(
    const FunctionSignature& functionSignature,
    const std::string& functionName,
    const protocol::FunctionKind& functionKind) {
  protocol::Determinism determinism;
  protocol::NullCallClause nullCallClause;
  if (functionKind == protocol::FunctionKind::SCALAR) {
    auto functionMetadata =
        getFunctionMetadata(functionName, functionSignature);
    determinism = functionMetadata.isDeterministic
        ? protocol::Determinism::DETERMINISTIC
        : protocol::Determinism::NOT_DETERMINISTIC;
    nullCallClause = functionMetadata.isDefaultNullBehavior
        ? protocol::NullCallClause::RETURNS_NULL_ON_NULL_INPUT
        : protocol::NullCallClause::CALLED_ON_NULL_INPUT;
  } else {
    // Default metadata values of DETERMINISTIC and CALLED_ON_NULL_INPUT for
    // non-scalar functions.
    determinism = protocol::Determinism::DETERMINISTIC;
    nullCallClause = protocol::NullCallClause::CALLED_ON_NULL_INPUT;
  }

  protocol::RoutineCharacteristics routineCharacteristics;
  routineCharacteristics.language =
      std::make_shared<protocol::Language>(protocol::Language({"CPP"}));
  routineCharacteristics.determinism =
      std::make_shared<protocol::Determinism>(determinism);
  routineCharacteristics.nullCallClause =
      std::make_shared<protocol::NullCallClause>(nullCallClause);
  return routineCharacteristics;
}

const std::vector<protocol::TypeVariableConstraint> getTypeVariableConstraints(
    const FunctionSignature& functionSignature) {
  std::vector<protocol::TypeVariableConstraint> typeVariableConstraints;
  const auto functionVariables = functionSignature.variables();
  for (const auto& [name, signature] : functionVariables) {
    if (signature.isTypeParameter()) {
      protocol::TypeVariableConstraint typeVariableConstraint;
      typeVariableConstraint.name = signature.name();
      typeVariableConstraint.orderableRequired = signature.orderableTypesOnly();
      typeVariableConstraint.comparableRequired =
          signature.comparableTypesOnly();
      typeVariableConstraints.emplace_back(typeVariableConstraint);
    }
  }
  return typeVariableConstraints;
}

void updateFunctionMetadata(
    const std::string& functionName,
    const FunctionSignature& functionSignature,
    protocol::JsonBasedUdfFunctionMetadata& jsonBasedUdfFunctionMetadata) {
  jsonBasedUdfFunctionMetadata.docString = functionName;
  jsonBasedUdfFunctionMetadata.schema = kDefaultSchema;
  jsonBasedUdfFunctionMetadata.outputType =
      functionSignature.returnType().toString();
  jsonBasedUdfFunctionMetadata.variableArity =
      std::make_shared<protocol::Boolean>(functionSignature.variableArity());
  const std::vector<TypeSignature> argumentTypes =
      functionSignature.argumentTypes();
  std::vector<std::string> paramTypes;
  for (const auto& argumentType : argumentTypes) {
    paramTypes.emplace_back(argumentType.toString());
  }
  jsonBasedUdfFunctionMetadata.paramTypes = paramTypes;
  jsonBasedUdfFunctionMetadata.typeVariableConstraints =
      std::make_shared<std::vector<protocol::TypeVariableConstraint>>(
          getTypeVariableConstraints(functionSignature));
}

const std::vector<protocol::JsonBasedUdfFunctionMetadata>
getAggregateFunctionMetadata(
    const std::string& functionName,
    const std::vector<AggregateFunctionSignaturePtr>&
        aggregateFunctionSignatures) {
  std::vector<protocol::JsonBasedUdfFunctionMetadata>
      jsonBasedUdfFunctionMetadataList;
  jsonBasedUdfFunctionMetadataList.reserve(aggregateFunctionSignatures.size());
  const protocol::FunctionKind functionKind = protocol::FunctionKind::AGGREGATE;

  for (const auto& aggregateFunctionSignature : aggregateFunctionSignatures) {
    protocol::JsonBasedUdfFunctionMetadata jsonBasedUdfFunctionMetadata;
    jsonBasedUdfFunctionMetadata.functionKind = functionKind;
    jsonBasedUdfFunctionMetadata.routineCharacteristics =
        getRoutineCharacteristics(
            *aggregateFunctionSignature, functionName, functionKind);
    jsonBasedUdfFunctionMetadata.aggregateMetadata =
        std::make_shared<protocol::AggregationFunctionMetadata>(
            getAggregationFunctionMetadata(*aggregateFunctionSignature));

    updateFunctionMetadata(
        functionName,
        *aggregateFunctionSignature,
        jsonBasedUdfFunctionMetadata);
    jsonBasedUdfFunctionMetadataList.emplace_back(jsonBasedUdfFunctionMetadata);
  }
  return jsonBasedUdfFunctionMetadataList;
}

const std::vector<protocol::JsonBasedUdfFunctionMetadata>
getScalarFunctionMetadata(
    const std::string& functionName,
    const std::vector<const FunctionSignature*>& functionSignatures) {
  std::vector<protocol::JsonBasedUdfFunctionMetadata>
      jsonBasedUdfFunctionMetadataList;
  jsonBasedUdfFunctionMetadataList.reserve(functionSignatures.size());
  const protocol::FunctionKind functionKind = protocol::FunctionKind::SCALAR;

  for (const auto& functionSignature : functionSignatures) {
    protocol::JsonBasedUdfFunctionMetadata jsonBasedUdfFunctionMetadata;
    jsonBasedUdfFunctionMetadata.functionKind = functionKind;
    jsonBasedUdfFunctionMetadata.routineCharacteristics =
        getRoutineCharacteristics(
            *functionSignature, functionName, functionKind);

    updateFunctionMetadata(
        functionName, *functionSignature, jsonBasedUdfFunctionMetadata);
    jsonBasedUdfFunctionMetadataList.emplace_back(jsonBasedUdfFunctionMetadata);
  }
  return jsonBasedUdfFunctionMetadataList;
}

const std::vector<protocol::JsonBasedUdfFunctionMetadata>
getWindowFunctionMetadata(
    const std::string& functionName,
    const std::vector<FunctionSignaturePtr>& windowFunctionSignatures) {
  std::vector<protocol::JsonBasedUdfFunctionMetadata>
      jsonBasedUdfFunctionMetadataList;
  jsonBasedUdfFunctionMetadataList.reserve(windowFunctionSignatures.size());
  const protocol::FunctionKind functionKind = protocol::FunctionKind::WINDOW;

  for (const auto& windowFunctionSignature : windowFunctionSignatures) {
    protocol::JsonBasedUdfFunctionMetadata jsonBasedUdfFunctionMetadata;
    jsonBasedUdfFunctionMetadata.functionKind = functionKind;
    jsonBasedUdfFunctionMetadata.routineCharacteristics =
        getRoutineCharacteristics(
            *windowFunctionSignature, functionName, functionKind);

    updateFunctionMetadata(
        functionName, *windowFunctionSignature, jsonBasedUdfFunctionMetadata);
    jsonBasedUdfFunctionMetadataList.emplace_back(jsonBasedUdfFunctionMetadata);
  }
  return jsonBasedUdfFunctionMetadataList;
}

const std::vector<protocol::JsonBasedUdfFunctionMetadata> getFunctionMetadata(
    const std::string& functionName) {
  if (auto aggregateFunctionSignatures =
          getAggregateFunctionSignatures(functionName)) {
    return getAggregateFunctionMetadata(
        functionName, aggregateFunctionSignatures.value());
  } else if (
      auto windowFunctionSignatures =
          getWindowFunctionSignatures(functionName)) {
    return getWindowFunctionMetadata(
        functionName, windowFunctionSignatures.value());
  } else {
    auto functionSignatures = getFunctionSignatures();
    if (functionSignatures.find(functionName) != functionSignatures.end()) {
      return getScalarFunctionMetadata(
          functionName, functionSignatures.at(functionName));
    }
  }
  VELOX_UNREACHABLE("Function kind cannot be determined");
}

} // namespace

void getJsonMetadataForFunction(
    const std::string& registeredFunctionName,
    nlohmann::json& jsonMetadataList) {
  auto functionMetadataList = getFunctionMetadata(registeredFunctionName);
  auto [functionName, functionVisibility] =
      getFunctionNameAndVisibility(registeredFunctionName);
  for (auto& functionMetadata : functionMetadataList) {
    functionMetadata.functionVisibility =
        std::make_shared<protocol::SqlFunctionVisibility>(functionVisibility);
    jsonMetadataList[functionName].emplace_back(functionMetadata);
  }
}

json getJsonFunctionMetadata() {
  auto registeredFunctionNames = functions::getSortedAggregateNames();
  auto scalarFunctionNames = functions::getSortedScalarNames();
  for (const auto& scalarFunction : scalarFunctionNames) {
    registeredFunctionNames.emplace_back(scalarFunction);
  }
  auto windowFunctionNames = functions::getSortedWindowNames();
  for (const auto& windowFunction : windowFunctionNames) {
    registeredFunctionNames.emplace_back(windowFunction);
  }

  nlohmann::json j;
  for (const auto& registeredFunctionName : registeredFunctionNames) {
    getJsonMetadataForFunction(registeredFunctionName, j);
  }
  return j;
}

} // namespace facebook::presto
