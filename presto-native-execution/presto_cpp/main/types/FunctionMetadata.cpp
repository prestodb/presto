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
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::presto {

namespace {

// The keys in velox function maps are of the format
// `catalog.schema.function_name`. This utility function extracts the
// three parts, {catalog, schema, function_name}, from the registered function.
const std::vector<std::string> getFunctionNameParts(
    const std::string& registeredFunction) {
  std::vector<std::string> parts;
  folly::split('.', registeredFunction, parts, true);
  VELOX_USER_CHECK(
      parts.size() == 3,
      fmt::format("Prefix missing for function {}", registeredFunction));
  return parts;
}

const protocol::AggregationFunctionMetadata getAggregationFunctionMetadata(
    const std::string& name,
    const AggregateFunctionSignature& signature) {
  protocol::AggregationFunctionMetadata metadata;
  metadata.intermediateType = signature.intermediateType().toString();
  metadata.isOrderSensitive =
      getAggregateFunctionEntry(name)->metadata.orderSensitive;
  return metadata;
}

std::optional<exec::VectorFunctionMetadata> getScalarMetadata(
    const std::string& name) {
  auto simpleFunctionMetadata =
      exec::simpleFunctions().getFunctionSignaturesAndMetadata(name);
  if (simpleFunctionMetadata.size()) {
    // Functions like abs are registered as simple functions for primitive
    // types, and as a vector function for complex types like DECIMAL. So do not
    // throw an error if function metadata is not found in simple function
    // signature map.
    return simpleFunctionMetadata.back().first;
  }

  auto vectorFunctionMetadata = exec::getVectorFunctionMetadata(name);
  if (vectorFunctionMetadata.has_value()) {
    return vectorFunctionMetadata.value();
  }
  return std::nullopt;
}

const protocol::RoutineCharacteristics getRoutineCharacteristics(
    const FunctionSignature& signature,
    const std::string& name,
    const protocol::FunctionKind& kind) {
  protocol::Determinism determinism;
  protocol::NullCallClause nullCallClause;
  if (kind == protocol::FunctionKind::SCALAR) {
    auto metadata = getScalarMetadata(name);
    VELOX_USER_CHECK(
        metadata.has_value(), "Metadata for function {} not found", name);
    determinism = metadata.value().deterministic
        ? protocol::Determinism::DETERMINISTIC
        : protocol::Determinism::NOT_DETERMINISTIC;
    nullCallClause = metadata.value().defaultNullBehavior
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

void updateFunctionMetadata(
    const std::string& name,
    const std::string& schema,
    const FunctionSignature& signature,
    protocol::JsonBasedUdfFunctionMetadata& metadata) {
  metadata.docString = name;
  metadata.schema = schema;
  metadata.outputType = signature.returnType().toString();

  const std::vector<TypeSignature> types = signature.argumentTypes();
  std::vector<std::string> paramTypes;
  for (const auto& type : types) {
    paramTypes.emplace_back(type.toString());
  }
  metadata.paramTypes = paramTypes;
}

const std::vector<protocol::JsonBasedUdfFunctionMetadata> buildScalarMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<const FunctionSignature*>& signatures) {
  std::vector<protocol::JsonBasedUdfFunctionMetadata> metadataList;
  metadataList.reserve(signatures.size());
  const protocol::FunctionKind kind = protocol::FunctionKind::SCALAR;

  for (const auto& signature : signatures) {
    protocol::JsonBasedUdfFunctionMetadata metadata;
    metadata.functionKind = kind;
    metadata.routineCharacteristics =
        getRoutineCharacteristics(*signature, name, kind);

    updateFunctionMetadata(name, schema, *signature, metadata);
    metadataList.emplace_back(metadata);
  }
  return metadataList;
}

const std::vector<protocol::JsonBasedUdfFunctionMetadata>
buildAggregateMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<AggregateFunctionSignaturePtr>& signatures) {
  std::vector<protocol::JsonBasedUdfFunctionMetadata> metadataList;
  metadataList.reserve(signatures.size());
  // All aggregate functions can be used as window functions.
  const std::vector<protocol::FunctionKind> kinds = {
      protocol::FunctionKind::AGGREGATE, protocol::FunctionKind::WINDOW};

  for (const auto& kind : kinds) {
    for (const auto& signature : signatures) {
      protocol::JsonBasedUdfFunctionMetadata metadata;
      metadata.functionKind = kind;
      metadata.routineCharacteristics =
          getRoutineCharacteristics(*signature, name, kind);
      metadata.aggregateMetadata =
          std::make_shared<protocol::AggregationFunctionMetadata>(
              getAggregationFunctionMetadata(name, *signature));

      updateFunctionMetadata(name, schema, *signature, metadata);
      metadataList.emplace_back(metadata);
    }
  }
  return metadataList;
}

const std::vector<protocol::JsonBasedUdfFunctionMetadata> buildWindowMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<FunctionSignaturePtr>& signatures) {
  std::vector<protocol::JsonBasedUdfFunctionMetadata> metadataList;
  metadataList.reserve(signatures.size());
  const protocol::FunctionKind kind = protocol::FunctionKind::WINDOW;

  for (const auto& signature : signatures) {
    protocol::JsonBasedUdfFunctionMetadata metadata;
    metadata.functionKind = kind;
    metadata.routineCharacteristics =
        getRoutineCharacteristics(*signature, name, kind);

    updateFunctionMetadata(name, schema, *signature, metadata);
    metadataList.emplace_back(metadata);
  }
  return metadataList;
}

const std::vector<protocol::JsonBasedUdfFunctionMetadata> getFunctionJson(
    const std::string& name,
    const std::string& schema) {
  if (auto aggregateSignatures = getAggregateFunctionSignatures(name)) {
    auto aggregateMetadata =
        buildAggregateMetadata(name, schema, aggregateSignatures.value());
    auto windowSignatures = getWindowFunctionSignatures(name);
    VELOX_USER_CHECK(
        windowSignatures.has_value(),
        "Aggregate function {} not registered as a window function",
        name);
    return aggregateMetadata;
  } else if (auto windowSignatures = getWindowFunctionSignatures(name)) {
    return buildWindowMetadata(name, schema, windowSignatures.value());
  } else {
    auto signatures = getFunctionSignatures();
    if (signatures.find(name) != signatures.end()) {
      return buildScalarMetadata(name, schema, signatures.at(name));
    }
  }
  VELOX_UNREACHABLE("Function kind for {} cannot be determined", name);
}

} // namespace

json getMetadataForFunction(
    const std::string& name,
    const std::string& schema) {
  auto metadataList = getFunctionJson(name, schema);
  json j = json::array();
  json tj;
  for (const auto& metadata : metadataList) {
    protocol::to_json(tj, metadata);
    j.emplace_back(tj);
  }
  return j;
}

json getFunctionsMetadata() {
  // Get all registered scalar, aggregate and window function names from velox
  // FunctionRegistry.
  auto functions = getAggregateNames();
  auto scalarFunctions = getScalarNames();
  auto windowFunctions = getWindowNames();
  functions.insert(
      functions.end(), scalarFunctions.begin(), scalarFunctions.end());
  functions.insert(
      functions.end(), windowFunctions.begin(), windowFunctions.end());

  nlohmann::json j;
  for (const auto& function : functions) {
    // Skip internal functions. They don't have any prefix.
    if (function.find("$internal$") != std::string::npos) {
      continue;
    }

    auto parts = getFunctionNameParts(function);
    auto schema = parts[1];
    auto name = parts[2];
    j[name] = getMetadataForFunction(function, schema);
  }
  return j;
}

} // namespace facebook::presto
