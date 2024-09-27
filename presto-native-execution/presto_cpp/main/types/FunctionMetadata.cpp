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

/// TODO: Similar function exists in velox/functions/CoverageUtil.cpp, refactor
/// to use this function once it is moved to velox/functions/FunctionRegistry.h
/// Reference: https://github.com/facebookincubator/velox/pull/9250/.
// A function name is a companion function's if the name is an existing
// aggregation function name followed by specific suffixes.
bool isCompanionFunctionName(
    const std::string& name,
    const std::unordered_map<std::string, exec::AggregateFunctionEntry>&
        aggregateFunctions) {
  auto suffixOffset = name.rfind("_partial");
  if (suffixOffset == std::string::npos) {
    suffixOffset = name.rfind("_merge_extract");
  }
  if (suffixOffset == std::string::npos) {
    suffixOffset = name.rfind("_merge");
  }
  if (suffixOffset == std::string::npos) {
    suffixOffset = name.rfind("_extract");
  }
  if (suffixOffset == std::string::npos) {
    return false;
  }
  return aggregateFunctions.count(name.substr(0, suffixOffset)) > 0;
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

const exec::VectorFunctionMetadata getScalarMetadata(const std::string& name) {
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
  VELOX_UNREACHABLE("Metadata for function {} not found", name);
}

const protocol::RoutineCharacteristics getRoutineCharacteristics(
    const std::string& name,
    const protocol::FunctionKind& kind) {
  protocol::Determinism determinism;
  protocol::NullCallClause nullCallClause;
  if (kind == protocol::FunctionKind::SCALAR) {
    auto metadata = getScalarMetadata(name);
    determinism = metadata.deterministic
        ? protocol::Determinism::DETERMINISTIC
        : protocol::Determinism::NOT_DETERMINISTIC;
    nullCallClause = metadata.defaultNullBehavior
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

const protocol::JsonBasedUdfFunctionMetadata buildFunctionMetadata(
    const std::string& name,
    const std::string& schema,
    const protocol::FunctionKind& kind,
    const FunctionSignature& signature,
    std::optional<AggregateFunctionSignature> aggregateSignature) {
  protocol::JsonBasedUdfFunctionMetadata metadata;
  metadata.docString = name;
  metadata.functionKind = kind;
  metadata.outputType = signature.returnType().toString();

  const std::vector<TypeSignature> types = signature.argumentTypes();
  std::vector<std::string> paramTypes;
  for (const auto& type : types) {
    paramTypes.emplace_back(type.toString());
  }
  metadata.paramTypes = paramTypes;
  metadata.schema = schema;
  metadata.routineCharacteristics = getRoutineCharacteristics(name, kind);

  if (aggregateSignature.has_value()) {
    metadata.aggregateMetadata =
        std::make_shared<protocol::AggregationFunctionMetadata>(
            getAggregationFunctionMetadata(name, aggregateSignature.value()));
  }
  return metadata;
}

json buildScalarMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<const FunctionSignature*>& signatures) {
  const protocol::FunctionKind kind = protocol::FunctionKind::SCALAR;
  json j = json::array();
  json tj;
  for (const auto& signature : signatures) {
    protocol::to_json(
        tj,
        buildFunctionMetadata(name, schema, kind, *signature, std::nullopt));
    j.push_back(tj);
  }
  return j;
}

json buildAggregateMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<AggregateFunctionSignaturePtr>& signatures) {
  // All aggregate functions can be used as window functions.
  VELOX_USER_CHECK(
      getWindowFunctionSignatures(name).has_value(),
      "Aggregate function {} not registered as a window function",
      name);
  const std::vector<protocol::FunctionKind> kinds = {
      protocol::FunctionKind::AGGREGATE, protocol::FunctionKind::WINDOW};
  json j = json::array();
  json tj;
  for (const auto& kind : kinds) {
    for (const auto& signature : signatures) {
      protocol::to_json(
          tj,
          buildFunctionMetadata(name, schema, kind, *signature, *signature));
      j.push_back(tj);
    }
  }
  return j;
}

json buildWindowMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<FunctionSignaturePtr>& signatures) {
  const protocol::FunctionKind kind = protocol::FunctionKind::WINDOW;
  json j = json::array();
  json tj;
  for (const auto& signature : signatures) {
    protocol::to_json(
        tj,
        buildFunctionMetadata(name, schema, kind, *signature, std::nullopt));
    j.push_back(tj);
  }
  return j;
}

} // namespace

json getFunctionsMetadata() {
  json j;

  // Get metadata for all registered scalar functions in velox.
  const auto signatures = getFunctionSignatures();
  static const std::unordered_set<std::string> kBlockList = {
      "row_constructor", "in", "is_null"};
  for (const auto& entry : signatures) {
    const auto name = entry.first;
    // Skip internal functions. They don't have any prefix.
    if ((kBlockList.count(name) != 0) ||
        (name.find("$internal$") != std::string::npos)) {
      continue;
    }

    const auto parts = getFunctionNameParts(name);
    const auto schema = parts[1];
    const auto function = parts[2];
    j[function] = buildScalarMetadata(name, schema, entry.second);
  }

  // Get metadata for all registered aggregate functions in velox.
  const auto aggregateFunctions = exec::aggregateFunctions().copy();
  for (const auto& entry : aggregateFunctions) {
    if (!isCompanionFunctionName(entry.first, aggregateFunctions)) {
      const auto name = entry.first;
      const auto parts = getFunctionNameParts(name);
      const auto schema = parts[1];
      const auto function = parts[2];
      j[function] =
          buildAggregateMetadata(name, schema, entry.second.signatures);
    }
  }

  // Get metadata for all registered window functions in velox. Skip aggregates
  // as they have been processed.
  const auto& functions = exec::windowFunctions();
  for (const auto& entry : functions) {
    if (aggregateFunctions.count(entry.first) == 0) {
      const auto name = entry.first;
      const auto parts = getFunctionNameParts(entry.first);
      const auto schema = parts[1];
      const auto function = parts[2];
      j[function] = buildWindowMetadata(name, schema, entry.second.signatures);
    }
  }

  return j;
}

} // namespace facebook::presto
