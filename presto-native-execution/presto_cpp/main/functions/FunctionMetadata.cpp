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
#include "presto_cpp/main/functions/FunctionMetadata.h"
#include "presto_cpp/main/tvf/spi/TableFunction.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/main/types/TypeParser.h"

#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/SimpleFunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::presto::tvf;

namespace facebook::presto {
namespace {

// Check if the Velox type is supported in Presto.
bool isValidPrestoType(const TypeSignature& typeSignature) {
  if (typeSignature.parameters().empty()) {
    // Hugeint type is not supported in Presto.
    auto kindName = boost::algorithm::to_upper_copy(typeSignature.baseName());
    if (auto typeKind = TypeKindName::tryToTypeKind(kindName)) {
      return typeKind.value() != TypeKind::HUGEINT;
    }
  } else {
    for (const auto& paramType : typeSignature.parameters()) {
      if (!isValidPrestoType(paramType)) {
        return false;
      }
    }
  }
  return true;
}

const protocol::AggregationFunctionMetadata getAggregationFunctionMetadata(
    const std::string& name,
    const AggregateFunctionSignature& signature) {
  protocol::AggregationFunctionMetadata metadata;
  metadata.intermediateType =
      boost::algorithm::to_lower_copy(signature.intermediateType().toString());
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

const std::vector<protocol::TypeVariableConstraint> getTypeVariableConstraints(
    const FunctionSignature& functionSignature) {
  std::vector<protocol::TypeVariableConstraint> typeVariableConstraints;
  const auto& functionVariables = functionSignature.variables();
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
    const FunctionSignature& functionSignature) {
  std::vector<protocol::LongVariableConstraint> longVariableConstraints;
  const auto& functionVariables = functionSignature.variables();
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

std::optional<protocol::JsonBasedUdfFunctionMetadata> buildFunctionMetadata(
    const std::string& name,
    const std::string& schema,
    const protocol::FunctionKind& kind,
    const FunctionSignature& signature,
    const AggregateFunctionSignaturePtr& aggregateSignature = nullptr) {
  protocol::JsonBasedUdfFunctionMetadata metadata;
  metadata.docString = name;
  metadata.functionKind = kind;
  if (!isValidPrestoType(signature.returnType())) {
    return std::nullopt;
  }
  metadata.outputType =
      boost::algorithm::to_lower_copy(signature.returnType().toString());

  const auto& argumentTypes = signature.argumentTypes();
  std::vector<std::string> paramTypes(argumentTypes.size());
  for (auto i = 0; i < argumentTypes.size(); i++) {
    if (!isValidPrestoType(argumentTypes.at(i))) {
      return std::nullopt;
    }
    paramTypes[i] =
        boost::algorithm::to_lower_copy(argumentTypes.at(i).toString());
  }
  metadata.paramTypes = paramTypes;
  metadata.schema = schema;
  metadata.variableArity = signature.variableArity();
  metadata.routineCharacteristics = getRoutineCharacteristics(name, kind);
  metadata.typeVariableConstraints =
      std::make_shared<std::vector<protocol::TypeVariableConstraint>>(
          getTypeVariableConstraints(signature));
  metadata.longVariableConstraints =
      std::make_shared<std::vector<protocol::LongVariableConstraint>>(
          getLongVariableConstraints(signature));

  if (aggregateSignature) {
    metadata.aggregateMetadata =
        std::make_shared<protocol::AggregationFunctionMetadata>(
            getAggregationFunctionMetadata(name, *aggregateSignature));
  }
  return metadata;
}

json buildScalarMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<const FunctionSignature*>& signatures) {
  json j = json::array();
  json tj;
  for (const auto& signature : signatures) {
    if (auto functionMetadata = buildFunctionMetadata(
            name, schema, protocol::FunctionKind::SCALAR, *signature)) {
      protocol::to_json(tj, functionMetadata.value());
      j.push_back(tj);
    }
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

  // The functions returned by this endpoint are stored as SqlInvokedFunction
  // objects, with SqlFunctionId serving as the primary key. SqlFunctionId is
  // derived from both the functionName and argumentTypes parameters. Returning
  // the same function twice—once as an aggregate function and once as a window
  // function introduces ambiguity, as functionKind is not a component of
  // SqlFunctionId. For any aggregate function utilized as a window function,
  // the function’s metadata can be obtained from the associated aggregate
  // function implementation for further processing. For additional information,
  // refer to the following: 	•
  // https://github.com/prestodb/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/function/SqlFunctionId.java
  //  •
  //  https://github.com/prestodb/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/function/SqlInvokedFunction.java

  const std::vector<protocol::FunctionKind> kinds = {
      protocol::FunctionKind::AGGREGATE};
  json j = json::array();
  json tj;
  for (const auto& kind : kinds) {
    for (const auto& signature : signatures) {
      if (auto functionMetadata = buildFunctionMetadata(
              name, schema, kind, *signature, signature)) {
        protocol::to_json(tj, functionMetadata.value());
        j.push_back(tj);
      }
    }
  }
  return j;
}

json buildWindowMetadata(
    const std::string& name,
    const std::string& schema,
    const std::vector<FunctionSignaturePtr>& signatures) {
  json j = json::array();
  json tj;
  for (const auto& signature : signatures) {
    if (auto functionMetadata = buildFunctionMetadata(
            name, schema, protocol::FunctionKind::WINDOW, *signature)) {
      protocol::to_json(tj, functionMetadata.value());
      j.push_back(tj);
    }
  }
  return j;
}

protocol::Descriptor buildDescriptor(const Descriptor& descriptor) {
  // types could be empty, pre-process that case
  auto names = descriptor.names();
  auto types = descriptor.types();
  std::vector<protocol::Field> fields;
  for (int i = 0; i < names.size(); i++) {
    std::shared_ptr<std::string> type = (i < types.size())
        ? std::make_shared<std::string>(types.at(i)->toString())
        : nullptr;
    fields.emplace_back(
        protocol::Field{std::make_shared<std::string>(names.at(i)), type});
  }
  return protocol::Descriptor{fields};
}

protocol::NativeDescriptor buildNativeDescriptor(const Descriptor& descriptor) {
  // types could be empty, pre-process that case
  auto names = descriptor.names();
  auto types = descriptor.types();
  std::vector<protocol::NativeField> fields;
  for (int i = 0; i < names.size(); i++) {
    std::shared_ptr<std::string> type = (i < types.size())
        ? std::make_shared<std::string>(types.at(i)->toString())
        : nullptr;
    fields.emplace_back(
        protocol::NativeField{
            std::make_shared<std::string>(names.at(i)), type});
  }
  return protocol::NativeDescriptor{fields};
}

std::vector<std::shared_ptr<protocol::ArgumentSpecification>>
buildArgumentSpecsList(TableArgumentSpecList argumentsSpec) {
  std::vector<std::shared_ptr<protocol::ArgumentSpecification>>
      argumentsSpecsList;
  for (const auto argumentSpec : argumentsSpec) {
    if (auto scalarArgumentSpec =
            std::dynamic_pointer_cast<ScalarArgumentSpecification>(
                argumentSpec)) {
      auto scalarArgumentSpecification =
          std::make_shared<protocol::ScalarArgumentSpecification>();
      scalarArgumentSpecification->name = scalarArgumentSpec->name();
      scalarArgumentSpecification->required = scalarArgumentSpec->required();
      scalarArgumentSpecification->type =
          scalarArgumentSpec->rowType()->toString();
      argumentsSpecsList.emplace_back(scalarArgumentSpecification);
    } else if (
        auto tableArgumentSpec =
            std::dynamic_pointer_cast<TableArgumentSpecification>(
                argumentSpec)) {
      auto tableArgumentSpecification =
          std::make_shared<protocol::TableArgumentSpecification>();
      tableArgumentSpecification->name = tableArgumentSpec->name();
      tableArgumentSpecification->passThroughColumns = false;
      tableArgumentSpecification->pruneWhenEmpty = true;
      tableArgumentSpecification->rowSemantics = true;
      argumentsSpecsList.emplace_back(tableArgumentSpecification);
    } else if (
        auto descriptorArgumentSpec =
            std::dynamic_pointer_cast<DescriptorArgumentSpecification>(
                argumentSpec)) {
      auto descriptorArgumentSpecification =
          std::make_shared<protocol::DescriptorArgumentSpecification>();
      descriptorArgumentSpecification->name = descriptorArgumentSpec->name();
      descriptorArgumentSpecification->defaultValue =
          buildDescriptor(descriptorArgumentSpec->descriptor());
      descriptorArgumentSpecification->required = false;
      argumentsSpecsList.emplace_back(descriptorArgumentSpecification);
    } else {
      VELOX_FAIL("Failed to convert to a valid argumentSpec");
    }
  }
  return argumentsSpecsList;
}

std::shared_ptr<protocol::ReturnTypeSpecification> buildReturnTypeSpecification(
    ReturnSpecPtr returnSpec) {
  auto returnTypeSpecification = returnSpec->returnType();
  if (returnTypeSpecification ==
      ReturnTypeSpecification::ReturnType::kGenericTable) {
    std::shared_ptr<protocol::GenericTableReturnTypeSpecification>
        genericTableReturnTypeSpecification =
            std::make_shared<protocol::GenericTableReturnTypeSpecification>();
    return genericTableReturnTypeSpecification;
  } else {
    std::shared_ptr<protocol::DescribedTableReturnTypeSpecification>
        describedTableReturnTypeSpecification =
            std::make_shared<protocol::DescribedTableReturnTypeSpecification>();
    auto describedTable =
        std::dynamic_pointer_cast<DescribedTableReturnType>(returnSpec);
    describedTableReturnTypeSpecification->descriptor =
        buildDescriptor(*(describedTable->descriptor()));
    return describedTableReturnTypeSpecification;
  }
}
} // namespace

json getFunctionsMetadata(const std::optional<std::string>& catalog) {
  json j;

  // Lambda to check if a function should be skipped based on catalog filter
  auto skipCatalog = [&catalog](const std::string& functionCatalog) {
    return catalog.has_value() && functionCatalog != catalog.value();
  };

  // Get metadata for all registered scalar functions in velox.
  const auto signatures = getFunctionSignatures();
  static const std::unordered_set<std::string> kBlockList = {
      "row_constructor", "in", "is_null"};
  // Exclude aggregate companion functions (extract aggregate companion
  // functions are registered as vector functions).
  const auto aggregateFunctions = exec::aggregateFunctions().copy();
  for (const auto& entry : signatures) {
    const auto name = entry.first;
    // Skip internal functions. They don't have any prefix.
    if (kBlockList.count(name) != 0 ||
        name.find("$internal$") != std::string::npos ||
        getScalarMetadata(name).companionFunction) {
      continue;
    }

    const auto parts = util::getFunctionNameParts(name);
    if (skipCatalog(parts[0])) {
      continue;
    }
    const auto schema = parts[1];
    const auto function = parts[2];
    j[function] = buildScalarMetadata(name, schema, entry.second);
  }

  // Get metadata for all registered aggregate functions in velox.
  for (const auto& entry : aggregateFunctions) {
    if (!aggregateFunctions.at(entry.first).metadata.companionFunction) {
      const auto name = entry.first;
      const auto parts = util::getFunctionNameParts(name);
      if (skipCatalog(parts[0])) {
        continue;
      }
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
      const auto parts = util::getFunctionNameParts(entry.first);
      if (skipCatalog(parts[0])) {
        continue;
      }
      const auto schema = parts[1];
      const auto function = parts[2];
      j[function] = buildWindowMetadata(name, schema, entry.second.signatures);
    }
  }

  return j;
}

json getTableValuedFunctionsMetadata() {
  json j;
  // Get metadata for all registered table valued functions in velox.
  const auto signatures = tableFunctions();
  for (const auto& entry : signatures) {
    const auto parts = util::getFunctionNameParts(entry.first);
    const auto functionName = parts[2];

    protocol::JsonBasedTableFunctionMetadata function;
    json tj;
    function.functionName = entry.first;
    function.returnTypeSpecification =
        buildReturnTypeSpecification(getTableFunctionReturnType(entry.first));
    function.arguments =
        buildArgumentSpecsList(getTableFunctionArgumentSpecs(entry.first));
    protocol::to_json(tj, function);
    j[functionName] = tj;
  }
  return j;
}

protocol::Map<protocol::String, protocol::List<protocol::Integer>>
getRequiredColumns(const tvf::TableFunctionAnalysis* tableFunctionAnalysis) {
  protocol::Map<protocol::String, protocol::List<protocol::Integer>>
      requiredColumns;
  for (auto& [k, v] : tableFunctionAnalysis->requiredColumns()) {
    std::vector<int32_t> values;
    for (int i : v) {
      values.emplace_back(i);
    }
    requiredColumns.insert({k, values});
  }
  return requiredColumns;
}

protocol::NativeTableFunctionHandle buildNativeTableFunctionHandle(
    const TableFunctionHandlePtr tableFunctionHandle,
    const std::string& functionName) {
  protocol::NativeTableFunctionHandle handle;
  handle.functionName = functionName;
  handle.serializedTableFunctionHandle =
      folly::toJson(tableFunctionHandle->serialize());
  return handle;
}

protocol::NativeTableFunctionAnalysis getNativeTableFunctionAnalysis(
    std::string functionName,
    std::unordered_map<std::string, std::shared_ptr<tvf::Argument>> args) {
  auto tableFunctionAnalysis = tvf::TableFunction::analyze(functionName, args);
  protocol::NativeTableFunctionAnalysis nativeTableFunctionAnalysis;
  nativeTableFunctionAnalysis.requiredColumns =
      getRequiredColumns(tableFunctionAnalysis.get());
  nativeTableFunctionAnalysis.returnedType =
      std::make_shared<protocol::NativeDescriptor>(
          buildNativeDescriptor(*tableFunctionAnalysis->returnType()));
  nativeTableFunctionAnalysis.handle = buildNativeTableFunctionHandle(
      tableFunctionAnalysis->tableFunctionHandle(), functionName);
  return nativeTableFunctionAnalysis;
}

json getAnalyzedTableValueFunction(
    const std::string& connectorTableMetadataJson,
    velox::memory::MemoryPool* pool) {
  TypeParser parser;
  VeloxExprConverter exprConverter{pool, &parser};
  protocol::ConnectorTableMetadata connectorTableMetadata =
      json::parse(connectorTableMetadataJson);
  std::unordered_map<std::string, std::shared_ptr<tvf::Argument>> args;
  for (const auto& entry : connectorTableMetadata.arguments) {
    std::shared_ptr<tvf::Argument> functionArg;
    if (auto scalarArgument =
            std::dynamic_pointer_cast<protocol::ScalarArgument>(entry.second)) {
      auto serializableNullableValue =
          scalarArgument->nullableValue.serializable;
      auto value = exprConverter.getConstantValue(
          parser.parse(serializableNullableValue.type),
          serializableNullableValue.block);
      functionArg = std::make_shared<tvf::ScalarArgument>(
          value.inferType(),
          BaseVector::createConstant(
              value.inferType(),
              value,
              serializableNullableValue.block.data.size(),
              pool));
    } else if (
        auto tableArgument =
            std::dynamic_pointer_cast<protocol::TableArgument>(entry.second)) {
      std::vector<std::string> fieldNames;
      std::vector<velox::TypePtr> fieldTypes;
      for (auto& arg : tableArgument->fields) {
        fieldNames.push_back(boost::algorithm::to_lower_copy(*arg.name));
        fieldTypes.push_back(parser.parse(*arg.type));
      }
      functionArg = std::make_shared<tvf::TableArgument>(
          ROW(std::move(fieldNames), std::move(fieldTypes)));
    } else if (
        auto descriptorArgument =
            std::dynamic_pointer_cast<protocol::DescriptorArgument>(
                entry.second)) {
      std::vector<std::string> fieldNames;
      std::vector<velox::TypePtr> fieldTypes;
      for (auto& arg : descriptorArgument->descriptor->fields) {
        fieldNames.push_back(boost::algorithm::to_lower_copy(*arg.name));
        fieldTypes.push_back(parser.parse(*arg.type));
      }
      functionArg = std::make_shared<tvf::Descriptor>(
          std::move(fieldNames), std::move(fieldTypes));
    } else {
      VELOX_UNSUPPORTED("Failed to convert to a valid Argument");
    }
    args[entry.first] = functionArg;
  }
  return json(getNativeTableFunctionAnalysis(
      connectorTableMetadata.functionName, args));
}
} // namespace facebook::presto
