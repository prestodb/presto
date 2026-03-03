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

#include "presto_cpp/main/sidecar/function/BaseFunctionMetadataProvider.h"

#include <boost/algorithm/string.hpp>

#include "presto_cpp/main/common/Utils.h"

namespace facebook::presto {

namespace {

bool isValidPrestoType(const facebook::velox::exec::TypeSignature& typeSignature)
{
  if (typeSignature.parameters().empty()) {
    auto kindName = boost::algorithm::to_upper_copy(typeSignature.baseName());
    if (auto typeKind = facebook::velox::TypeKindName::tryToTypeKind(kindName)) {
      return typeKind.value() != facebook::velox::TypeKind::HUGEINT;
    }
  }
  else {
    for (const auto& paramType : typeSignature.parameters()) {
      if (!isValidPrestoType(paramType)) {
        return false;
      }
    }
  }
  return true;
}

const std::vector<protocol::TypeVariableConstraint> getTypeVariableConstraints(
    const facebook::velox::exec::FunctionSignature* functionSignature)
{
  std::vector<protocol::TypeVariableConstraint> typeVariableConstraints;
  const auto& functionVariables = functionSignature->variables();
  for (const auto& [name, signature] : functionVariables) {
    if (signature.isTypeParameter()) {
      protocol::TypeVariableConstraint typeVariableConstraint;
      typeVariableConstraint.name =
          boost::algorithm::to_lower_copy(signature.name());
      typeVariableConstraint.orderableRequired = signature.orderableTypesOnly();
      typeVariableConstraint.comparableRequired =
          signature.comparableTypesOnly();
      typeVariableConstraints.emplace_back(typeVariableConstraint);
    }
  }
  return typeVariableConstraints;
}

const std::vector<protocol::LongVariableConstraint> getLongVariableConstraints(
    const facebook::velox::exec::FunctionSignature* functionSignature)
{
  std::vector<protocol::LongVariableConstraint> longVariableConstraints;
  const auto& functionVariables = functionSignature->variables();
  for (const auto& [name, signature] : functionVariables) {
    if (signature.isIntegerParameter() && !signature.constraint().empty()) {
      protocol::LongVariableConstraint longVariableConstraint;
      longVariableConstraint.name =
          boost::algorithm::to_lower_copy(signature.name());
      longVariableConstraint.expression =
          boost::algorithm::to_lower_copy(signature.constraint());
      longVariableConstraints.emplace_back(longVariableConstraint);
    }
  }
  return longVariableConstraints;
}

const protocol::RoutineCharacteristics getRoutineCharacteristics(
    const std::optional<velox::exec::VectorFunctionMetadata>& veloxFunctionMetadata)
{
  protocol::Determinism determinism;
  protocol::NullCallClause nullCallClause;
  if (veloxFunctionMetadata.has_value()) {
    determinism = veloxFunctionMetadata->deterministic
        ? protocol::Determinism::DETERMINISTIC
        : protocol::Determinism::NOT_DETERMINISTIC;
    nullCallClause = veloxFunctionMetadata->defaultNullBehavior
        ? protocol::NullCallClause::RETURNS_NULL_ON_NULL_INPUT
        : protocol::NullCallClause::CALLED_ON_NULL_INPUT;
  }
  else {
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

} // namespace

FunctionMetadataPtr BaseFunctionMetadataProvider::buildFunctionMetadata(
    const std::string& name,
    const std::string& schema,
    const protocol::FunctionKind& kind,
    const facebook::velox::exec::FunctionSignature* signature) const
{
  protocol::JsonBasedUdfFunctionMetadata metadata;
  metadata.docString = name;
  metadata.functionKind = kind;
  if (!isValidPrestoType(signature->returnType())) {
    return nullptr;
  }
  metadata.outputType =
      boost::algorithm::to_lower_copy(signature->returnType().toString());

  const auto& argumentTypes = signature->argumentTypes();
  std::vector<std::string> paramTypes(argumentTypes.size());
  for (size_t i = 0; i < argumentTypes.size(); ++i) {
    if (!isValidPrestoType(argumentTypes.at(i))) {
      return nullptr;
    }
    paramTypes[i] =
        boost::algorithm::to_lower_copy(argumentTypes.at(i).toString());
  }
  metadata.paramTypes = paramTypes;
  metadata.schema = schema;
  metadata.variableArity = signature->variableArity();
  const auto veloxFunctionMetadata =
      kind == protocol::FunctionKind::SCALAR ? getVeloxFunctionMetadata(name)
                                             : std::nullopt;
  metadata.routineCharacteristics =
      getRoutineCharacteristics(veloxFunctionMetadata);
  metadata.typeVariableConstraints =
      std::make_shared<std::vector<protocol::TypeVariableConstraint>>(
          getTypeVariableConstraints(signature));
  metadata.longVariableConstraints =
      std::make_shared<std::vector<protocol::LongVariableConstraint>>(
          getLongVariableConstraints(signature));

  if (kind == protocol::FunctionKind::AGGREGATE) {
    const auto* aggregateFunctionSignature =
        dynamic_cast<const velox::exec::AggregateFunctionSignature*>(signature);
    metadata.aggregateMetadata =
        getAggregationFunctionMetadata(name, aggregateFunctionSignature);
  }
  return std::make_shared<protocol::JsonBasedUdfFunctionMetadata>(metadata);
}

json::array_t BaseFunctionMetadataProvider::buildScalarMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<const facebook::velox::exec::FunctionSignature*>& signatures) const
{
  json::array_t j;
  json tj;
  for (const auto& signature : signatures) {
    if (auto functionMetadata = buildFunctionMetadata(
            name, schema, protocol::FunctionKind::SCALAR, signature)) {
      protocol::to_json(tj, *functionMetadata);
      j.push_back(tj);
    }
  }
  return j;
}

json::array_t BaseFunctionMetadataProvider::buildAggregateMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<velox::exec::AggregateFunctionSignaturePtr>& signatures) const
{
  json::array_t j;
  json tj;
  for (const auto& signature : signatures) {
    if (auto functionMetadata = buildFunctionMetadata(
            name, schema, protocol::FunctionKind::AGGREGATE, signature.get())) {
      protocol::to_json(tj, *functionMetadata);
      j.push_back(tj);
    }
  }
  return j;
}

json::array_t BaseFunctionMetadataProvider::buildWindowMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<velox::exec::FunctionSignaturePtr>& signatures) const
{
  json::array_t j;
  json tj;
  for (const auto& signature : signatures) {
    if (auto functionMetadata = buildFunctionMetadata(
            name, schema, protocol::FunctionKind::WINDOW, signature.get())) {
      protocol::to_json(tj, *functionMetadata);
      j.push_back(tj);
    }
  }
  return j;
}

json BaseFunctionMetadataProvider::getFunctionsMetadata(
    const std::optional<std::string>& catalog) const
{
  json j = json::object();

  auto skipCatalog = [&catalog](const std::string& functionCatalog) {
    return catalog.has_value() && functionCatalog != catalog.value();
  };

  for (const auto& entry : scalarFunctions()) {
    const auto& name = entry.first;
    if (isInternalScalarFunction(name)) {
      continue;
    }
    const auto parts = facebook::presto::util::getFunctionNameParts(name);
    const auto& catalogName = parts[0];
    const auto& schema = parts[1];
    const auto& function = parts[2];
    if (skipCatalog(catalogName)) {
      continue;
    }
    if (!j.contains(function)) {
      j[function] = buildScalarMetadata(name, schema, entry.second);
    }
  }

  for (const auto& entry : aggregateFunctions()) {
    const auto& name = entry.first;
    const auto parts = facebook::presto::util::getFunctionNameParts(name);
    const auto& catalogName = parts[0];
    const auto& schema = parts[1];
    const auto& function = parts[2];
    if (skipCatalog(catalogName)) {
      continue;
    }
    if (!j.contains(function)) {
      j[function] = buildAggregateMetadata(name, schema, entry.second);
    }
  }

  for (const auto& entry : windowFunctions()) {
    const auto& name = entry.first;
    const auto parts = facebook::presto::util::getFunctionNameParts(name);
    const auto& catalogName = parts[0];
    const auto& schema = parts[1];
    const auto& function = parts[2];
    if (skipCatalog(catalogName)) {
      continue;
    }
    if (!j.contains(function)) {
      j[function] = buildWindowMetadata(name, schema, entry.second.signatures);
    }
  }

  return j;
}

} // namespace facebook::presto
