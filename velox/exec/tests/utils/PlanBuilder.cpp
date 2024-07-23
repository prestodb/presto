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

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/duckdb/conversion/DuckParser.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/exec/TableWriter.h"
#include "velox/exec/WindowFunction.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;

namespace facebook::velox::exec::test {

namespace {

core::TypedExprPtr parseExpr(
    const std::string& text,
    const RowTypePtr& rowType,
    const parse::ParseOptions& options,
    memory::MemoryPool* pool) {
  auto untyped = parse::parseExpr(text, options);
  return core::Expressions::inferTypes(untyped, rowType, pool);
}

std::shared_ptr<HiveBucketProperty> buildHiveBucketProperty(
    const RowTypePtr rowType,
    int32_t bucketCount,
    const std::vector<std::string>& bucketColumns,
    const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortBy) {
  std::vector<TypePtr> bucketTypes;
  bucketTypes.reserve(bucketColumns.size());
  for (const auto& bucketColumn : bucketColumns) {
    bucketTypes.push_back(rowType->childAt(rowType->getChildIdx(bucketColumn)));
  }
  return std::make_shared<HiveBucketProperty>(
      HiveBucketProperty::Kind::kHiveCompatible,
      bucketCount,
      bucketColumns,
      bucketTypes,
      sortBy);
}
} // namespace

PlanBuilder& PlanBuilder::tableScan(
    const RowTypePtr& outputType,
    const std::vector<std::string>& subfieldFilters,
    const std::string& remainingFilter,
    const RowTypePtr& dataColumns,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& assignments) {
  return TableScanBuilder(*this)
      .outputType(outputType)
      .assignments(assignments)
      .subfieldFilters(subfieldFilters)
      .remainingFilter(remainingFilter)
      .dataColumns(dataColumns)
      .assignments(assignments)
      .endTableScan();
}

PlanBuilder& PlanBuilder::tableScan(
    const std::string& tableName,
    const RowTypePtr& outputType,
    const std::unordered_map<std::string, std::string>& columnAliases,
    const std::vector<std::string>& subfieldFilters,
    const std::string& remainingFilter,
    const RowTypePtr& dataColumns,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& assignments) {
  return TableScanBuilder(*this)
      .tableName(tableName)
      .outputType(outputType)
      .columnAliases(columnAliases)
      .subfieldFilters(subfieldFilters)
      .remainingFilter(remainingFilter)
      .dataColumns(dataColumns)
      .assignments(assignments)
      .endTableScan();
}

PlanBuilder& PlanBuilder::tpchTableScan(
    tpch::Table table,
    std::vector<std::string>&& columnNames,
    double scaleFactor) {
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignmentsMap;
  std::vector<TypePtr> outputTypes;

  assignmentsMap.reserve(columnNames.size());
  outputTypes.reserve(columnNames.size());

  for (const auto& columnName : columnNames) {
    assignmentsMap.emplace(
        columnName,
        std::make_shared<connector::tpch::TpchColumnHandle>(columnName));
    outputTypes.emplace_back(resolveTpchColumn(table, columnName));
  }
  auto rowType = ROW(std::move(columnNames), std::move(outputTypes));
  return TableScanBuilder(*this)
      .outputType(rowType)
      .tableHandle(std::make_shared<connector::tpch::TpchTableHandle>(
          std::string(kTpchDefaultConnectorId), table, scaleFactor))
      .assignments(assignmentsMap)
      .endTableScan();
}

core::PlanNodePtr PlanBuilder::TableScanBuilder::build(core::PlanNodeId id) {
  VELOX_CHECK_NOT_NULL(outputType_, "outputType must be specified");
  std::unordered_map<std::string, core::TypedExprPtr> typedMapping;
  bool hasAssignments = !(assignments_.empty());
  for (uint32_t i = 0; i < outputType_->size(); ++i) {
    const auto& name = outputType_->nameOf(i);
    const auto& type = outputType_->childAt(i);

    std::string hiveColumnName = name;
    auto it = columnAliases_.find(name);
    if (it != columnAliases_.end()) {
      hiveColumnName = it->second;
      typedMapping.emplace(
          name,
          std::make_shared<core::FieldAccessTypedExpr>(type, hiveColumnName));
    }

    if (!hasAssignments) {
      assignments_.insert(
          {name,
           std::make_shared<HiveColumnHandle>(
               hiveColumnName,
               HiveColumnHandle::ColumnType::kRegular,
               type,
               type)});
    }
  }

  const RowTypePtr& parseType = dataColumns_ ? dataColumns_ : outputType_;

  SubfieldFilters filters;
  filters.reserve(subfieldFilters_.size());
  auto queryCtx = core::QueryCtx::create();
  exec::SimpleExpressionEvaluator evaluator(queryCtx.get(), planBuilder_.pool_);
  for (const auto& filter : subfieldFilters_) {
    auto filterExpr =
        parseExpr(filter, parseType, planBuilder_.options_, planBuilder_.pool_);
    auto [subfield, subfieldFilter] =
        exec::toSubfieldFilter(filterExpr, &evaluator);

    auto it = columnAliases_.find(subfield.toString());
    if (it != columnAliases_.end()) {
      subfield = common::Subfield(it->second);
    }
    VELOX_CHECK_EQ(
        filters.count(subfield),
        0,
        "Duplicate subfield: {}",
        subfield.toString());

    filters[std::move(subfield)] = std::move(subfieldFilter);
  }

  core::TypedExprPtr remainingFilterExpr;
  if (!remainingFilter_.empty()) {
    remainingFilterExpr = parseExpr(
                              remainingFilter_,
                              parseType,
                              planBuilder_.options_,
                              planBuilder_.pool_)
                              ->rewriteInputNames(typedMapping);
  }

  if (!tableHandle_) {
    tableHandle_ = std::make_shared<HiveTableHandle>(
        connectorId_,
        tableName_,
        true,
        std::move(filters),
        remainingFilterExpr,
        dataColumns_);
  }
  return std::make_shared<core::TableScanNode>(
      id, outputType_, tableHandle_, assignments_);
}

PlanBuilder& PlanBuilder::values(
    const std::vector<RowVectorPtr>& values,
    bool parallelizable,
    size_t repeatTimes) {
  VELOX_CHECK_NULL(planNode_, "Values must be the source node");
  auto valuesCopy = values;
  planNode_ = std::make_shared<core::ValuesNode>(
      nextPlanNodeId(), std::move(valuesCopy), parallelizable, repeatTimes);
  return *this;
}

PlanBuilder& PlanBuilder::exchange(const RowTypePtr& outputType) {
  VELOX_CHECK_NULL(planNode_, "Exchange must be the source node");
  planNode_ =
      std::make_shared<core::ExchangeNode>(nextPlanNodeId(), outputType);
  return *this;
}

namespace {
std::pair<
    std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>,
    std::vector<core::SortOrder>>
parseOrderByClauses(
    const std::vector<std::string>& keys,
    const RowTypePtr& inputType,
    memory::MemoryPool* pool) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  for (const auto& key : keys) {
    auto [untypedExpr, sortOrder] = parse::parseOrderByExpr(key);
    auto typedExpr =
        core::Expressions::inferTypes(untypedExpr, inputType, pool);

    auto sortingKey =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(typedExpr);
    VELOX_CHECK_NOT_NULL(
        sortingKey,
        "ORDER BY clause must use a column name, not an expression: {}",
        key);
    sortingKeys.emplace_back(sortingKey);
    sortingOrders.emplace_back(sortOrder);
  }

  return {sortingKeys, sortingOrders};
}
} // namespace

PlanBuilder& PlanBuilder::mergeExchange(
    const RowTypePtr& outputType,
    const std::vector<std::string>& keys) {
  VELOX_CHECK_NULL(planNode_, "MergeExchange must be the source node");
  auto [sortingKeys, sortingOrders] =
      parseOrderByClauses(keys, outputType, pool_);

  planNode_ = std::make_shared<core::MergeExchangeNode>(
      nextPlanNodeId(), outputType, sortingKeys, sortingOrders);

  return *this;
}

PlanBuilder& PlanBuilder::optionalProject(
    const std::vector<std::string>& optionalProjections) {
  if (optionalProjections.empty()) {
    return *this;
  }
  return project(optionalProjections);
}

PlanBuilder& PlanBuilder::projectExpressions(
    const std::vector<std::shared_ptr<const core::IExpr>>& projections) {
  std::vector<core::TypedExprPtr> expressions;
  std::vector<std::string> projectNames;
  for (auto i = 0; i < projections.size(); ++i) {
    expressions.push_back(inferTypes(projections[i]));
    if (projections[i]->alias().has_value()) {
      projectNames.push_back(projections[i]->alias().value());
    } else if (
        auto fieldExpr =
            dynamic_cast<const core::FieldAccessExpr*>(projections[i].get())) {
      projectNames.push_back(fieldExpr->getFieldName());
    } else {
      projectNames.push_back(fmt::format("p{}", i));
    }
  }
  planNode_ = std::make_shared<core::ProjectNode>(
      nextPlanNodeId(),
      std::move(projectNames),
      std::move(expressions),
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::project(const std::vector<std::string>& projections) {
  VELOX_CHECK_NOT_NULL(planNode_, "Project cannot be the source node");
  std::vector<std::shared_ptr<const core::IExpr>> expressions;
  for (auto i = 0; i < projections.size(); ++i) {
    expressions.push_back(parse::parseExpr(projections[i], options_));
  }
  return projectExpressions(expressions);
}

PlanBuilder& PlanBuilder::appendColumns(
    const std::vector<std::string>& newColumns) {
  VELOX_CHECK_NOT_NULL(planNode_, "Project cannot be the source node");
  std::vector<std::string> allProjections = planNode_->outputType()->names();
  for (const auto& column : newColumns) {
    allProjections.push_back(column);
  }

  return project(allProjections);
}

PlanBuilder& PlanBuilder::optionalFilter(const std::string& optionalFilter) {
  if (optionalFilter.empty()) {
    return *this;
  }
  return filter(optionalFilter);
}

PlanBuilder& PlanBuilder::filter(const std::string& filter) {
  VELOX_CHECK_NOT_NULL(planNode_, "Filter cannot be the source node");
  planNode_ = std::make_shared<core::FilterNode>(
      nextPlanNodeId(),
      parseExpr(filter, planNode_->outputType(), options_, pool_),
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::tableWrite(
    const std::string& outputDirectoryPath,
    const dwio::common::FileFormat fileFormat,
    const std::vector<std::string>& aggregates,
    const std::shared_ptr<dwio::common::WriterOptions>& options) {
  return tableWrite(
      outputDirectoryPath,
      {},
      0,
      {},
      {},
      fileFormat,
      aggregates,
      kHiveDefaultConnectorId,
      {},
      options);
}

PlanBuilder& PlanBuilder::tableWrite(
    const std::string& outputDirectoryPath,
    const std::vector<std::string>& partitionBy,
    const dwio::common::FileFormat fileFormat,
    const std::vector<std::string>& aggregates,
    const std::shared_ptr<dwio::common::WriterOptions>& options) {
  return tableWrite(
      outputDirectoryPath,
      partitionBy,
      0,
      {},
      {},
      fileFormat,
      aggregates,
      kHiveDefaultConnectorId,
      {},
      options);
}

PlanBuilder& PlanBuilder::tableWrite(
    const std::string& outputDirectoryPath,
    const std::vector<std::string>& partitionBy,
    int32_t bucketCount,
    const std::vector<std::string>& bucketedBy,
    const dwio::common::FileFormat fileFormat,
    const std::vector<std::string>& aggregates,
    const std::shared_ptr<dwio::common::WriterOptions>& options) {
  return tableWrite(
      outputDirectoryPath,
      partitionBy,
      bucketCount,
      bucketedBy,
      {},
      fileFormat,
      aggregates,
      kHiveDefaultConnectorId,
      {},
      options);
}

PlanBuilder& PlanBuilder::tableWrite(
    const std::string& outputDirectoryPath,
    const std::vector<std::string>& partitionBy,
    int32_t bucketCount,
    const std::vector<std::string>& bucketedBy,
    const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortBy,
    const dwio::common::FileFormat fileFormat,
    const std::vector<std::string>& aggregates,
    const std::string_view& connectorId,
    const std::unordered_map<std::string, std::string>& serdeParameters,
    const std::shared_ptr<dwio::common::WriterOptions>& options) {
  VELOX_CHECK_NOT_NULL(planNode_, "TableWrite cannot be the source node");
  auto rowType = planNode_->outputType();

  std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>>
      columnHandles;
  for (auto i = 0; i < rowType->size(); ++i) {
    const auto column = rowType->nameOf(i);
    const bool isPartitionKey =
        std::find(partitionBy.begin(), partitionBy.end(), column) !=
        partitionBy.end();
    columnHandles.push_back(std::make_shared<connector::hive::HiveColumnHandle>(
        column,
        isPartitionKey
            ? connector::hive::HiveColumnHandle::ColumnType::kPartitionKey
            : connector::hive::HiveColumnHandle::ColumnType::kRegular,
        rowType->childAt(i),
        rowType->childAt(i)));
  }

  auto locationHandle = std::make_shared<connector::hive::LocationHandle>(
      outputDirectoryPath,
      outputDirectoryPath,
      connector::hive::LocationHandle::TableType::kNew);
  std::shared_ptr<HiveBucketProperty> bucketProperty;
  if (!partitionBy.empty() && bucketCount != 0) {
    bucketProperty =
        buildHiveBucketProperty(rowType, bucketCount, bucketedBy, sortBy);
  }
  auto hiveHandle = std::make_shared<connector::hive::HiveInsertTableHandle>(
      columnHandles,
      locationHandle,
      fileFormat,
      bucketProperty,
      common::CompressionKind_NONE,
      serdeParameters,
      options);

  auto insertHandle = std::make_shared<core::InsertTableHandle>(
      std::string(connectorId), hiveHandle);

  std::shared_ptr<core::AggregationNode> aggregationNode;
  if (!aggregates.empty()) {
    auto aggregatesAndNames = createAggregateExpressionsAndNames(
        aggregates, {}, core::AggregationNode::Step::kPartial);
    aggregationNode = std::make_shared<core::AggregationNode>(
        nextPlanNodeId(),
        core::AggregationNode::Step::kPartial,
        std::vector<core::FieldAccessTypedExprPtr>{}, // groupingKeys
        std::vector<core::FieldAccessTypedExprPtr>{}, // preGroupedKeys
        aggregatesAndNames.names, // ignoreNullKeys
        aggregatesAndNames.aggregates,
        false,
        planNode_);
  }

  planNode_ = std::make_shared<core::TableWriteNode>(
      nextPlanNodeId(),
      rowType,
      rowType->names(),
      aggregationNode,
      insertHandle,
      false,
      TableWriteTraits::outputType(aggregationNode),
      connector::CommitStrategy::kNoCommit,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::tableWriteMerge(
    const std::shared_ptr<core::AggregationNode>& aggregationNode) {
  planNode_ = std::make_shared<core::TableWriteMergeNode>(
      nextPlanNodeId(),
      TableWriteTraits::outputType(aggregationNode),
      aggregationNode,
      planNode_);
  return *this;
}

namespace {

std::string throwAggregateFunctionDoesntExist(const std::string& name) {
  std::stringstream error;
  error << "Aggregate function doesn't exist: " << name << ".";
  exec::aggregateFunctions().withRLock([&](const auto& functionsMap) {
    if (functionsMap.empty()) {
      error << " Registry of aggregate functions is empty. "
               "Make sure to register some aggregate functions.";
    }
  });
  VELOX_USER_FAIL(error.str());
}

std::string throwAggregateFunctionSignatureNotSupported(
    const std::string& name,
    const std::vector<TypePtr>& types,
    const std::vector<std::shared_ptr<AggregateFunctionSignature>>&
        signatures) {
  std::stringstream error;
  error << "Aggregate function signature is not supported: "
        << toString(name, types)
        << ". Supported signatures: " << toString(signatures) << ".";
  VELOX_USER_FAIL(error.str());
}

TypePtr resolveAggregateType(
    const std::string& aggregateName,
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& rawInputTypes,
    bool nullOnFailure) {
  if (auto signatures = exec::getAggregateFunctionSignatures(aggregateName)) {
    for (const auto& signature : signatures.value()) {
      exec::SignatureBinder binder(*signature, rawInputTypes);
      if (binder.tryBind()) {
        return binder.tryResolveType(
            exec::isPartialOutput(step) ? signature->intermediateType()
                                        : signature->returnType());
      }
    }

    if (nullOnFailure) {
      return nullptr;
    }

    throwAggregateFunctionSignatureNotSupported(
        aggregateName, rawInputTypes, signatures.value());
  }

  // We may be parsing lambda expression used in a lambda aggregate function. In
  // this case, 'aggregateName' would refer to a scalar function.
  //
  // TODO Enhance the parser to allow for specifying separate resolver for
  // lambda expressions.
  if (auto type =
          exec::resolveTypeForSpecialForm(aggregateName, rawInputTypes)) {
    return type;
  }

  if (auto type = parse::resolveScalarFunctionType(
          aggregateName, rawInputTypes, true)) {
    return type;
  }

  if (nullOnFailure) {
    return nullptr;
  }

  throwAggregateFunctionDoesntExist(aggregateName);
  return nullptr;
}

class AggregateTypeResolver {
 public:
  explicit AggregateTypeResolver(core::AggregationNode::Step step)
      : step_(step), previousHook_(core::Expressions::getResolverHook()) {
    core::Expressions::setTypeResolverHook(
        [&](const auto& inputs, const auto& expr, bool nullOnFailure) {
          return resolveType(inputs, expr, nullOnFailure);
        });
  }

  ~AggregateTypeResolver() {
    core::Expressions::setTypeResolverHook(previousHook_);
  }

  void setRawInputTypes(const std::vector<TypePtr>& types) {
    rawInputTypes_ = types;
  }

 private:
  TypePtr resolveType(
      const std::vector<core::TypedExprPtr>& inputs,
      const std::shared_ptr<const core::CallExpr>& expr,
      bool nullOnFailure) const {
    auto functionName = expr->getFunctionName();

    // Use raw input types (if available) to resolve intermediate and final
    // result types.
    if (exec::isRawInput(step_)) {
      std::vector<TypePtr> types;
      for (auto& input : inputs) {
        types.push_back(input->type());
      }

      return resolveAggregateType(functionName, step_, types, nullOnFailure);
    }

    if (!rawInputTypes_.empty()) {
      return resolveAggregateType(
          functionName, step_, rawInputTypes_, nullOnFailure);
    }

    if (!nullOnFailure) {
      VELOX_USER_FAIL(
          "Cannot resolve aggregation function return type without raw input types: {}",
          functionName);
    }
    return nullptr;
  }

  const core::AggregationNode::Step step_;
  const core::Expressions::TypeResolverHook previousHook_;
  std::vector<TypePtr> rawInputTypes_;
};

} // namespace

core::PlanNodePtr PlanBuilder::createIntermediateOrFinalAggregation(
    core::AggregationNode::Step step,
    const core::AggregationNode* partialAggNode) {
  // Create intermediate or final aggregation using same grouping keys and same
  // aggregate function names.
  const auto& partialAggregates = partialAggNode->aggregates();
  const auto& groupingKeys = partialAggNode->groupingKeys();

  auto numAggregates = partialAggregates.size();
  auto numGroupingKeys = groupingKeys.size();

  std::vector<core::AggregationNode::Aggregate> aggregates;
  aggregates.reserve(numAggregates);
  for (auto i = 0; i < numAggregates; i++) {
    // Resolve final or intermediate aggregation result type using raw input
    // types for the partial aggregation.
    auto name = partialAggregates[i].call->name();
    auto rawInputs = partialAggregates[i].call->inputs();

    core::AggregationNode::Aggregate aggregate;
    for (auto& rawInput : rawInputs) {
      aggregate.rawInputTypes.push_back(rawInput->type());
    }

    auto type =
        resolveAggregateType(name, step, aggregate.rawInputTypes, false);
    std::vector<core::TypedExprPtr> inputs = {field(numGroupingKeys + i)};

    // Add lambda inputs.
    for (const auto& rawInput : rawInputs) {
      if (rawInput->type()->kind() == TypeKind::FUNCTION) {
        inputs.push_back(rawInput);
      }
    }

    aggregate.call =
        std::make_shared<core::CallTypedExpr>(type, std::move(inputs), name);
    aggregates.emplace_back(aggregate);
  }

  return std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      step,
      groupingKeys,
      partialAggNode->preGroupedKeys(),
      partialAggNode->aggregateNames(),
      aggregates,
      partialAggNode->ignoreNullKeys(),
      planNode_);
}

namespace {
/// Checks that specified plan node is a partial or intermediate aggregation or
/// local exchange over the same. Returns a pointer to core::AggregationNode.
const core::AggregationNode* findPartialAggregation(
    const core::PlanNode* planNode) {
  const core::AggregationNode* aggNode;
  if (auto exchange = dynamic_cast<const core::LocalPartitionNode*>(planNode)) {
    aggNode = dynamic_cast<const core::AggregationNode*>(
        exchange->sources()[0].get());
  } else if (auto merge = dynamic_cast<const core::LocalMergeNode*>(planNode)) {
    aggNode =
        dynamic_cast<const core::AggregationNode*>(merge->sources()[0].get());
  } else {
    aggNode = dynamic_cast<const core::AggregationNode*>(planNode);
  }
  VELOX_CHECK_NOT_NULL(
      aggNode,
      "Current plan node must be one of: partial or intermediate aggregation, "
      "local merge or exchange. Got: {}",
      planNode->toString());
  VELOX_CHECK(exec::isPartialOutput(aggNode->step()));
  return aggNode;
}
} // namespace

PlanBuilder& PlanBuilder::intermediateAggregation() {
  const auto* aggNode = findPartialAggregation(planNode_.get());
  VELOX_CHECK(exec::isRawInput(aggNode->step()));

  auto step = core::AggregationNode::Step::kIntermediate;

  planNode_ = createIntermediateOrFinalAggregation(step, aggNode);
  return *this;
}

PlanBuilder& PlanBuilder::finalAggregation() {
  const auto* aggNode = findPartialAggregation(planNode_.get());

  if (!exec::isRawInput(aggNode->step())) {
    // If aggregation node is not the partial aggregation, keep looking again.
    aggNode = findPartialAggregation(aggNode->sources()[0].get());
    if (!exec::isRawInput(aggNode->step())) {
      VELOX_CHECK_NOT_NULL(
          aggNode,
          "Plan node before current plan node must be a partial aggregation.");
      VELOX_CHECK(exec::isRawInput(aggNode->step()));
      VELOX_CHECK(exec::isPartialOutput(aggNode->step()));
    }
  }

  auto step = core::AggregationNode::Step::kFinal;

  planNode_ = createIntermediateOrFinalAggregation(step, aggNode);
  return *this;
}

PlanBuilder::AggregatesAndNames PlanBuilder::createAggregateExpressionsAndNames(
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    core::AggregationNode::Step step,
    const std::vector<std::vector<TypePtr>>& rawInputTypes) {
  if (step == core::AggregationNode::Step::kPartial ||
      step == core::AggregationNode::Step::kSingle) {
    VELOX_CHECK(
        rawInputTypes.empty(),
        "Do not provide raw inputs types for partial or single aggregation");
  } else {
    VELOX_CHECK_EQ(
        aggregates.size(),
        rawInputTypes.size(),
        "Do provide raw inputs types for final or intermediate aggregation");
  }

  std::vector<core::AggregationNode::Aggregate> aggs;

  AggregateTypeResolver resolver(step);
  std::vector<std::string> names;
  aggs.reserve(aggregates.size());
  names.reserve(aggregates.size());

  duckdb::ParseOptions options;
  options.parseIntegerAsBigint = options_.parseIntegerAsBigint;

  for (auto i = 0; i < aggregates.size(); i++) {
    auto& aggregate = aggregates[i];

    if (!rawInputTypes.empty()) {
      resolver.setRawInputTypes(rawInputTypes[i]);
    }

    auto untypedExpr = duckdb::parseAggregateExpr(aggregate, options);

    core::AggregationNode::Aggregate agg;

    agg.call = std::dynamic_pointer_cast<const core::CallTypedExpr>(
        inferTypes(untypedExpr.expr));

    if (step == core::AggregationNode::Step::kPartial ||
        step == core::AggregationNode::Step::kSingle) {
      for (const auto& input : agg.call->inputs()) {
        agg.rawInputTypes.push_back(input->type());
      }
    } else {
      agg.rawInputTypes = rawInputTypes[i];
    }

    if (untypedExpr.maskExpr != nullptr) {
      auto maskExpr =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
              inferTypes(untypedExpr.maskExpr));
      VELOX_CHECK_NOT_NULL(
          maskExpr,
          "FILTER clause must use a column name, not an expression: {}",
          aggregate);
      agg.mask = maskExpr;
    }

    if (i < masks.size() && !masks[i].empty()) {
      VELOX_CHECK_NULL(
          agg.mask,
          "Aggregation mask should be specified only once (either explicitly or using FILTER clause)");
      agg.mask = field(masks[i]);
    }

    agg.distinct = untypedExpr.distinct;

    if (!untypedExpr.orderBy.empty()) {
      auto* entry = exec::getAggregateFunctionEntry(agg.call->name());
      const auto& metadata = entry->metadata;
      if (metadata.orderSensitive) {
        VELOX_CHECK(
            step == core::AggregationNode::Step::kSingle,
            "Order sensitive aggregation over sorted inputs cannot be split "
            "into partial and final: {}.",
            aggregate)
      }
    }

    for (const auto& [keyExpr, order] : untypedExpr.orderBy) {
      auto sortingKey =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
              inferTypes(keyExpr));
      VELOX_CHECK_NOT_NULL(
          sortingKey,
          "ORDER BY clause must use a column name, not an expression: {}",
          aggregate);

      agg.sortingKeys.push_back(sortingKey);
      agg.sortingOrders.push_back(order);
    }

    aggs.emplace_back(agg);

    if (untypedExpr.expr->alias().has_value()) {
      names.push_back(untypedExpr.expr->alias().value());
    } else {
      names.push_back(fmt::format("a{}", i));
    }
  }

  return {aggs, names};
}

PlanBuilder& PlanBuilder::aggregation(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& preGroupedKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    core::AggregationNode::Step step,
    bool ignoreNullKeys,
    const std::vector<std::vector<TypePtr>>& rawInputTypes) {
  auto aggregatesAndNames = createAggregateExpressionsAndNames(
      aggregates, masks, step, rawInputTypes);

  // If the aggregationNode is over a GroupId, then global grouping sets
  // need to be populated.
  std::vector<vector_size_t> globalGroupingSets;
  std::optional<core::FieldAccessTypedExprPtr> groupId;
  if (auto groupIdNode =
          dynamic_cast<const core::GroupIdNode*>(planNode_.get())) {
    for (auto i = 0; i < groupIdNode->groupingSets().size(); i++) {
      if (groupIdNode->groupingSets().at(i).empty()) {
        globalGroupingSets.push_back(i);
      }
    }

    if (!globalGroupingSets.empty()) {
      // GroupId is the last column of the GroupIdNode.
      groupId = field(groupIdNode->outputType()->names().back());
    }
  }

  planNode_ = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      step,
      fields(groupingKeys),
      fields(preGroupedKeys),
      aggregatesAndNames.names,
      aggregatesAndNames.aggregates,
      globalGroupingSets,
      groupId,
      ignoreNullKeys,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::streamingAggregation(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    core::AggregationNode::Step step,
    bool ignoreNullKeys) {
  auto aggregatesAndNames =
      createAggregateExpressionsAndNames(aggregates, masks, step);
  planNode_ = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      step,
      fields(groupingKeys),
      fields(groupingKeys),
      aggregatesAndNames.names,
      aggregatesAndNames.aggregates,
      ignoreNullKeys,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::groupId(
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::vector<std::string>>& groupingSets,
    const std::vector<std::string>& aggregationInputs,
    std::string groupIdName) {
  std::vector<core::GroupIdNode::GroupingKeyInfo> groupingKeyInfos;
  groupingKeyInfos.reserve(groupingKeys.size());
  for (const auto& groupingKey : groupingKeys) {
    auto untypedExpr = parse::parseExpr(groupingKey, options_);
    const auto* fieldAccessExpr =
        dynamic_cast<const core::FieldAccessExpr*>(untypedExpr.get());
    VELOX_USER_CHECK(
        fieldAccessExpr,
        "Grouping key {} is not valid projection",
        groupingKey);
    std::string inputField = fieldAccessExpr->getFieldName();
    std::string outputField = untypedExpr->alias().has_value()
        ?
        // This is a projection with a column alias with the format
        // "input_col as output_col".
        untypedExpr->alias().value()
        :
        // This is a projection without a column alias.
        fieldAccessExpr->getFieldName();

    core::GroupIdNode::GroupingKeyInfo keyInfos;
    keyInfos.output = outputField;
    keyInfos.input = field(inputField);
    groupingKeyInfos.push_back(keyInfos);
  }

  planNode_ = std::make_shared<core::GroupIdNode>(
      nextPlanNodeId(),
      groupingSets,
      std::move(groupingKeyInfos),
      fields(aggregationInputs),
      std::move(groupIdName),
      planNode_);

  return *this;
}

namespace {
core::PlanNodePtr createLocalMergeNode(
    const core::PlanNodeId& id,
    const std::vector<std::string>& keys,
    std::vector<core::PlanNodePtr> sources,
    memory::MemoryPool* pool) {
  const auto& inputType = sources[0]->outputType();
  auto [sortingKeys, sortingOrders] =
      parseOrderByClauses(keys, inputType, pool);

  return std::make_shared<core::LocalMergeNode>(
      id, std::move(sortingKeys), std::move(sortingOrders), std::move(sources));
}
} // namespace

PlanBuilder& PlanBuilder::localMerge(const std::vector<std::string>& keys) {
  planNode_ = createLocalMergeNode(nextPlanNodeId(), keys, {planNode_}, pool_);
  return *this;
}

PlanBuilder& PlanBuilder::expand(
    const std::vector<std::vector<std::string>>& projections) {
  VELOX_CHECK(!projections.empty(), "projections must not be empty.");
  const auto numColumns = projections[0].size();
  const auto numRows = projections.size();
  std::vector<std::string> aliases;
  aliases.reserve(numColumns);

  std::vector<std::vector<core::TypedExprPtr>> projectExprs;
  projectExprs.reserve(projections.size());

  for (auto i = 0; i < numRows; i++) {
    std::vector<core::TypedExprPtr> projectExpr;
    VELOX_CHECK_EQ(numColumns, projections[i].size())
    for (auto j = 0; j < numColumns; j++) {
      auto untypedExpression = parse::parseExpr(projections[i][j], options_);
      auto typedExpression = inferTypes(untypedExpression);

      if (i == 0) {
        if (untypedExpression->alias().has_value()) {
          aliases.push_back(untypedExpression->alias().value());
        } else {
          auto fieldExpr = dynamic_cast<const core::FieldAccessExpr*>(
              untypedExpression.get());
          VELOX_CHECK_NOT_NULL(fieldExpr);
          aliases.push_back(fieldExpr->getFieldName());
        }
        projectExpr.push_back(typedExpression);
      } else {
        // The types of values in 2nd and subsequent rows must much types in the
        //  1st row.
        const auto& expectedType = projectExprs[0][j]->type();
        if (typedExpression->type()->equivalent(*expectedType)) {
          projectExpr.push_back(typedExpression);
        } else {
          auto constantExpr =
              dynamic_cast<const core::ConstantExpr*>(untypedExpression.get());
          VELOX_CHECK_NOT_NULL(constantExpr);
          VELOX_CHECK(constantExpr->value().isNull());
          projectExpr.push_back(std::make_shared<core::ConstantTypedExpr>(
              expectedType, variant::null(expectedType->kind())));
        }
      }
    }
    projectExprs.push_back(projectExpr);
  }

  planNode_ = std::make_shared<core::ExpandNode>(
      nextPlanNodeId(), projectExprs, std::move(aliases), planNode_);

  return *this;
}

PlanBuilder& PlanBuilder::localMerge(
    const std::vector<std::string>& keys,
    std::vector<core::PlanNodePtr> sources) {
  VELOX_CHECK_NULL(planNode_, "localMerge() must be the first call");
  VELOX_CHECK_GE(
      sources.size(), 1, "localMerge() requires at least one source");

  planNode_ =
      createLocalMergeNode(nextPlanNodeId(), keys, std::move(sources), pool_);
  return *this;
}

PlanBuilder& PlanBuilder::orderBy(
    const std::vector<std::string>& keys,
    bool isPartial) {
  VELOX_CHECK_NOT_NULL(planNode_, "OrderBy cannot be the source node");
  auto [sortingKeys, sortingOrders] =
      parseOrderByClauses(keys, planNode_->outputType(), pool_);

  planNode_ = std::make_shared<core::OrderByNode>(
      nextPlanNodeId(), sortingKeys, sortingOrders, isPartial, planNode_);

  return *this;
}

PlanBuilder& PlanBuilder::topN(
    const std::vector<std::string>& keys,
    int32_t count,
    bool isPartial) {
  VELOX_CHECK_NOT_NULL(planNode_, "TopN cannot be the source node");
  auto [sortingKeys, sortingOrders] =
      parseOrderByClauses(keys, planNode_->outputType(), pool_);
  planNode_ = std::make_shared<core::TopNNode>(
      nextPlanNodeId(),
      sortingKeys,
      sortingOrders,
      count,
      isPartial,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::limit(int64_t offset, int64_t count, bool isPartial) {
  planNode_ = std::make_shared<core::LimitNode>(
      nextPlanNodeId(), offset, count, isPartial, planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::enforceSingleRow() {
  planNode_ =
      std::make_shared<core::EnforceSingleRowNode>(nextPlanNodeId(), planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::assignUniqueId(
    const std::string& idName,
    const int32_t taskUniqueId) {
  planNode_ = std::make_shared<core::AssignUniqueIdNode>(
      nextPlanNodeId(), idName, taskUniqueId, planNode_);
  return *this;
}

namespace {
core::PartitionFunctionSpecPtr createPartitionFunctionSpec(
    const RowTypePtr& inputType,
    const std::vector<core::TypedExprPtr>& keys,
    memory::MemoryPool* pool) {
  if (keys.empty()) {
    return std::make_shared<core::GatherPartitionFunctionSpec>();
  } else {
    std::vector<column_index_t> keyIndices;
    keyIndices.reserve(keys.size());

    std::vector<VectorPtr> constValues;
    constValues.reserve(keys.size());

    for (const auto& key : keys) {
      if (auto field =
              std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                  key)) {
        keyIndices.push_back(inputType->getChildIdx(field->name()));
      } else if (
          auto constant =
              std::dynamic_pointer_cast<const core::ConstantTypedExpr>(key)) {
        keyIndices.push_back(kConstantChannel);
        constValues.push_back(constant->toConstantVector(pool));
      } else {
        VELOX_UNREACHABLE();
      }
    }
    return std::make_shared<HashPartitionFunctionSpec>(
        inputType, std::move(keyIndices), std::move(constValues));
  }
}

RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b) {
  std::vector<std::string> names = a->names();
  std::vector<TypePtr> types = a->children();
  names.insert(names.end(), b->names().begin(), b->names().end());
  types.insert(types.end(), b->children().begin(), b->children().end());
  return ROW(std::move(names), std::move(types));
}

RowTypePtr extract(
    const RowTypePtr& type,
    const std::vector<std::string>& childNames) {
  std::vector<std::string> names = childNames;

  std::vector<TypePtr> types;
  types.reserve(childNames.size());
  for (const auto& name : childNames) {
    types.emplace_back(type->findChild(name));
  }
  return ROW(std::move(names), std::move(types));
}

// Rename columns in the given row type.
RowTypePtr rename(
    const RowTypePtr& type,
    const std::vector<std::string>& newNames) {
  VELOX_CHECK_EQ(
      type->size(),
      newNames.size(),
      "Number of types and new type names should be the same");
  std::vector<std::string> names{newNames};
  std::vector<TypePtr> types{type->children()};
  return ROW(std::move(names), std::move(types));
}

core::PlanNodePtr createLocalPartitionNode(
    const core::PlanNodeId& planNodeId,
    const std::vector<core::TypedExprPtr>& keys,
    const std::vector<core::PlanNodePtr>& sources,
    memory::MemoryPool* pool) {
  auto partitionFunctionFactory =
      createPartitionFunctionSpec(sources[0]->outputType(), keys, pool);
  return std::make_shared<core::LocalPartitionNode>(
      planNodeId,
      keys.empty() ? core::LocalPartitionNode::Type::kGather
                   : core::LocalPartitionNode::Type::kRepartition,
      partitionFunctionFactory,
      sources);
}
} // namespace

PlanBuilder& PlanBuilder::partitionedOutput(
    const std::vector<std::string>& keys,
    int numPartitions,
    const std::vector<std::string>& outputLayout) {
  return partitionedOutput(keys, numPartitions, false, outputLayout);
}

PlanBuilder& PlanBuilder::partitionedOutput(
    const std::vector<std::string>& keys,
    int numPartitions,
    bool replicateNullsAndAny,
    const std::vector<std::string>& outputLayout) {
  VELOX_CHECK_NOT_NULL(
      planNode_, "PartitionedOutput cannot be the source node");

  auto keyExprs = exprs(keys, planNode_->outputType());
  return partitionedOutput(
      keys,
      numPartitions,
      replicateNullsAndAny,
      createPartitionFunctionSpec(planNode_->outputType(), keyExprs, pool_),
      outputLayout);
}

PlanBuilder& PlanBuilder::partitionedOutput(
    const std::vector<std::string>& keys,
    int numPartitions,
    bool replicateNullsAndAny,
    core::PartitionFunctionSpecPtr partitionFunctionSpec,
    const std::vector<std::string>& outputLayout) {
  VELOX_CHECK_NOT_NULL(
      planNode_, "PartitionedOutput cannot be the source node");
  auto outputType = outputLayout.empty()
      ? planNode_->outputType()
      : extract(planNode_->outputType(), outputLayout);
  planNode_ = std::make_shared<core::PartitionedOutputNode>(
      nextPlanNodeId(),
      core::PartitionedOutputNode::Kind::kPartitioned,
      exprs(keys, planNode_->outputType()),
      numPartitions,
      replicateNullsAndAny,
      std::move(partitionFunctionSpec),
      outputType,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::partitionedOutputBroadcast(
    const std::vector<std::string>& outputLayout) {
  VELOX_CHECK_NOT_NULL(
      planNode_, "PartitionedOutput cannot be the source node");
  auto outputType = outputLayout.empty()
      ? planNode_->outputType()
      : extract(planNode_->outputType(), outputLayout);
  planNode_ = core::PartitionedOutputNode::broadcast(
      nextPlanNodeId(), 1, outputType, planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::partitionedOutputArbitrary(
    const std::vector<std::string>& outputLayout) {
  VELOX_CHECK_NOT_NULL(
      planNode_, "PartitionedOutput cannot be the source node");
  auto outputType = outputLayout.empty()
      ? planNode_->outputType()
      : extract(planNode_->outputType(), outputLayout);
  planNode_ = core::PartitionedOutputNode::arbitrary(
      nextPlanNodeId(), outputType, planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::localPartition(
    const std::vector<std::string>& keys,
    const std::vector<core::PlanNodePtr>& sources) {
  VELOX_CHECK_NULL(planNode_, "localPartition() must be the first call");
  planNode_ = createLocalPartitionNode(
      nextPlanNodeId(), exprs(keys, sources[0]->outputType()), sources, pool_);
  return *this;
}

PlanBuilder& PlanBuilder::localPartition(const std::vector<std::string>& keys) {
  planNode_ = createLocalPartitionNode(
      nextPlanNodeId(),
      exprs(keys, planNode_->outputType()),
      {planNode_},
      pool_);
  return *this;
}

PlanBuilder& PlanBuilder::localPartitionByBucket(
    const std::shared_ptr<connector::hive::HiveBucketProperty>&
        bucketProperty) {
  VELOX_CHECK_NOT_NULL(planNode_, "LocalPartition cannot be the source node");
  std::vector<column_index_t> bucketChannels;
  for (const auto& bucketColumn : bucketProperty->bucketedBy()) {
    bucketChannels.push_back(
        planNode_->outputType()->getChildIdx(bucketColumn));
  }
  auto hivePartitionFunctionFactory =
      std::make_shared<HivePartitionFunctionSpec>(
          bucketProperty->bucketCount(),
          bucketChannels,
          std::vector<VectorPtr>{});
  planNode_ = std::make_shared<core::LocalPartitionNode>(
      nextPlanNodeId(),
      core::LocalPartitionNode::Type::kRepartition,
      std::move(hivePartitionFunctionFactory),
      std::vector<core::PlanNodePtr>{planNode_});
  return *this;
}

namespace {
core::PlanNodePtr createLocalPartitionRoundRobinNode(
    const core::PlanNodeId& planNodeId,
    const std::vector<core::PlanNodePtr>& sources) {
  return std::make_shared<core::LocalPartitionNode>(
      planNodeId,
      core::LocalPartitionNode::Type::kRepartition,
      std::make_shared<RoundRobinPartitionFunctionSpec>(),
      sources);
}
} // namespace

PlanBuilder& PlanBuilder::localPartitionRoundRobin(
    const std::vector<core::PlanNodePtr>& sources) {
  VELOX_CHECK_NULL(
      planNode_, "localPartitionRoundRobin() must be the first call");
  planNode_ = createLocalPartitionRoundRobinNode(nextPlanNodeId(), sources);
  return *this;
}

PlanBuilder& PlanBuilder::localPartitionRoundRobin() {
  planNode_ = createLocalPartitionRoundRobinNode(nextPlanNodeId(), {planNode_});
  return *this;
}

namespace {
class RoundRobinRowPartitionFunction : public core::PartitionFunction {
 public:
  explicit RoundRobinRowPartitionFunction(int numPartitions)
      : numPartitions_{numPartitions} {}

  std::optional<uint32_t> partition(
      const RowVector& input,
      std::vector<uint32_t>& partitions) override {
    auto size = input.size();
    partitions.resize(size);
    for (auto i = 0; i < size; ++i) {
      partitions[i] = counter_ % numPartitions_;
      ++counter_;
    }
    return std::nullopt;
  }

 private:
  const int numPartitions_;
  uint32_t counter_{0};
};

class RoundRobinRowPartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  std::unique_ptr<core::PartitionFunction> create(
      int numPartitions) const override {
    return std::make_unique<RoundRobinRowPartitionFunction>(numPartitions);
  }

  std::string toString() const override {
    return "ROUND ROBIN ROW";
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("RoundRobinRowPartitionFunctionSpec");
    return obj;
  }

  static core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& /*obj*/,
      void* /*context*/) {
    return std::make_shared<RoundRobinRowPartitionFunctionSpec>();
  }
};
} // namespace

PlanBuilder& PlanBuilder::localPartitionRoundRobinRow() {
  planNode_ = std::make_shared<core::LocalPartitionNode>(
      nextPlanNodeId(),
      core::LocalPartitionNode::Type::kRepartition,
      std::make_shared<RoundRobinRowPartitionFunctionSpec>(),
      std::vector<core::PlanNodePtr>{planNode_});
  return *this;
}

PlanBuilder& PlanBuilder::hashJoin(
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const core::PlanNodePtr& build,
    const std::string& filter,
    const std::vector<std::string>& outputLayout,
    core::JoinType joinType,
    bool nullAware) {
  VELOX_CHECK_NOT_NULL(planNode_, "HashJoin cannot be the source node");
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());

  auto leftType = planNode_->outputType();
  auto rightType = build->outputType();
  auto resultType = concat(leftType, rightType);
  core::TypedExprPtr filterExpr;
  if (!filter.empty()) {
    filterExpr = parseExpr(filter, resultType, options_, pool_);
  }

  RowTypePtr outputType;
  if (isLeftSemiProjectJoin(joinType) || isRightSemiProjectJoin(joinType)) {
    std::vector<std::string> names = outputLayout;

    // Last column in 'outputLayout' must be a boolean 'match'.
    std::vector<TypePtr> types;
    types.reserve(outputLayout.size());
    for (auto i = 0; i < outputLayout.size() - 1; ++i) {
      types.emplace_back(resultType->findChild(outputLayout[i]));
    }
    types.emplace_back(BOOLEAN());

    outputType = ROW(std::move(names), std::move(types));
  } else {
    outputType = extract(resultType, outputLayout);
  }

  auto leftKeyFields = fields(leftType, leftKeys);
  auto rightKeyFields = fields(rightType, rightKeys);

  planNode_ = std::make_shared<core::HashJoinNode>(
      nextPlanNodeId(),
      joinType,
      nullAware,
      leftKeyFields,
      rightKeyFields,
      std::move(filterExpr),
      std::move(planNode_),
      build,
      outputType);
  return *this;
}

PlanBuilder& PlanBuilder::mergeJoin(
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const core::PlanNodePtr& build,
    const std::string& filter,
    const std::vector<std::string>& outputLayout,
    core::JoinType joinType) {
  VELOX_CHECK_NOT_NULL(planNode_, "MergeJoin cannot be the source node");
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());

  auto leftType = planNode_->outputType();
  auto rightType = build->outputType();
  auto resultType = concat(leftType, rightType);
  core::TypedExprPtr filterExpr;
  if (!filter.empty()) {
    filterExpr = parseExpr(filter, resultType, options_, pool_);
  }
  auto outputType = extract(resultType, outputLayout);
  auto leftKeyFields = fields(leftType, leftKeys);
  auto rightKeyFields = fields(rightType, rightKeys);

  planNode_ = std::make_shared<core::MergeJoinNode>(
      nextPlanNodeId(),
      joinType,
      leftKeyFields,
      rightKeyFields,
      std::move(filterExpr),
      std::move(planNode_),
      build,
      outputType);
  return *this;
}

PlanBuilder& PlanBuilder::nestedLoopJoin(
    const core::PlanNodePtr& right,
    const std::vector<std::string>& outputLayout,
    core::JoinType joinType) {
  return nestedLoopJoin(right, "", outputLayout, joinType);
}

PlanBuilder& PlanBuilder::nestedLoopJoin(
    const core::PlanNodePtr& right,
    const std::string& joinCondition,
    const std::vector<std::string>& outputLayout,
    core::JoinType joinType) {
  VELOX_CHECK_NOT_NULL(planNode_, "NestedLoopJoin cannot be the source node");
  auto resultType = concat(planNode_->outputType(), right->outputType());
  auto outputType = extract(resultType, outputLayout);

  core::TypedExprPtr joinConditionExpr{};
  if (!joinCondition.empty()) {
    joinConditionExpr = parseExpr(joinCondition, resultType, options_, pool_);
  }

  planNode_ = std::make_shared<core::NestedLoopJoinNode>(
      nextPlanNodeId(),
      joinType,
      std::move(joinConditionExpr),
      std::move(planNode_),
      right,
      outputType);
  return *this;
}

PlanBuilder& PlanBuilder::unnest(
    const std::vector<std::string>& replicateColumns,
    const std::vector<std::string>& unnestColumns,
    const std::optional<std::string>& ordinalColumn) {
  VELOX_CHECK_NOT_NULL(planNode_, "Unnest cannot be the source node");
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
      replicateFields;
  replicateFields.reserve(replicateColumns.size());
  for (const auto& name : replicateColumns) {
    replicateFields.emplace_back(field(name));
  }

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> unnestFields;
  unnestFields.reserve(unnestColumns.size());
  for (const auto& name : unnestColumns) {
    unnestFields.emplace_back(field(name));
  }

  std::vector<std::string> unnestNames;
  for (const auto& name : unnestColumns) {
    auto input = planNode_->outputType()->findChild(name);
    if (input->isArray()) {
      unnestNames.push_back(name + "_e");
    } else if (input->isMap()) {
      unnestNames.push_back(name + "_k");
      unnestNames.push_back(name + "_v");
    } else {
      VELOX_NYI(
          "Unsupported type of unnest variable. Expected ARRAY or MAP, but got {}.",
          input->toString());
    }
  }

  planNode_ = std::make_shared<core::UnnestNode>(
      nextPlanNodeId(),
      replicateFields,
      unnestFields,
      unnestNames,
      ordinalColumn,
      planNode_);
  return *this;
}

namespace {
std::string throwWindowFunctionDoesntExist(const std::string& name) {
  std::stringstream error;
  error << "Window function doesn't exist: " << name << ".";
  if (exec::windowFunctions().empty()) {
    error << " Registry of window functions is empty. "
             "Make sure to register some window functions.";
  }
  VELOX_USER_FAIL(error.str());
}

std::string throwWindowFunctionSignatureNotSupported(
    const std::string& name,
    const std::vector<TypePtr>& types,
    const std::vector<FunctionSignaturePtr>& signatures) {
  std::stringstream error;
  error << "Window function signature is not supported: "
        << toString(name, types)
        << ". Supported signatures: " << toString(signatures) << ".";
  VELOX_USER_FAIL(error.str());
}

TypePtr resolveWindowType(
    const std::string& windowFunctionName,
    const std::vector<TypePtr>& inputTypes,
    bool nullOnFailure) {
  if (auto signatures = exec::getWindowFunctionSignatures(windowFunctionName)) {
    for (const auto& signature : signatures.value()) {
      exec::SignatureBinder binder(*signature, inputTypes);
      if (binder.tryBind()) {
        return binder.tryResolveType(signature->returnType());
      }
    }

    if (nullOnFailure) {
      return nullptr;
    }
    throwWindowFunctionSignatureNotSupported(
        windowFunctionName, inputTypes, signatures.value());
  }

  if (nullOnFailure) {
    return nullptr;
  }
  throwWindowFunctionDoesntExist(windowFunctionName);
  return nullptr;
}

class WindowTypeResolver {
 public:
  explicit WindowTypeResolver()
      : previousHook_(core::Expressions::getResolverHook()) {
    core::Expressions::setTypeResolverHook(
        [&](const auto& inputs, const auto& expr, bool nullOnFailure) {
          return resolveType(inputs, expr, nullOnFailure);
        });
  }

  ~WindowTypeResolver() {
    core::Expressions::setTypeResolverHook(previousHook_);
  }

 private:
  TypePtr resolveType(
      const std::vector<core::TypedExprPtr>& inputs,
      const std::shared_ptr<const core::CallExpr>& expr,
      bool nullOnFailure) const {
    std::vector<TypePtr> types;
    for (auto& input : inputs) {
      types.push_back(input->type());
    }

    auto functionName = expr->getFunctionName();

    return resolveWindowType(functionName, types, nullOnFailure);
  }

  const core::Expressions::TypeResolverHook previousHook_;
};

const core::WindowNode::Frame createWindowFrame(
    const duckdb::IExprWindowFrame& windowFrame,
    const TypePtr& inputRow,
    memory::MemoryPool* pool) {
  core::WindowNode::Frame frame;
  frame.type = (windowFrame.type == duckdb::WindowType::kRows)
      ? core::WindowNode::WindowType::kRows
      : core::WindowNode::WindowType::kRange;

  auto boundTypeConversion =
      [](duckdb::BoundType boundType) -> core::WindowNode::BoundType {
    switch (boundType) {
      case duckdb::BoundType::kCurrentRow:
        return core::WindowNode::BoundType::kCurrentRow;
      case duckdb::BoundType::kFollowing:
        return core::WindowNode::BoundType::kFollowing;
      case duckdb::BoundType::kPreceding:
        return core::WindowNode::BoundType::kPreceding;
      case duckdb::BoundType::kUnboundedFollowing:
        return core::WindowNode::BoundType::kUnboundedFollowing;
      case duckdb::BoundType::kUnboundedPreceding:
        return core::WindowNode::BoundType::kUnboundedPreceding;
    }
    VELOX_UNREACHABLE();
  };
  frame.startType = boundTypeConversion(windowFrame.startType);
  frame.startValue = windowFrame.startValue
      ? core::Expressions::inferTypes(windowFrame.startValue, inputRow, pool)
      : nullptr;
  frame.endType = boundTypeConversion(windowFrame.endType);
  frame.endValue = windowFrame.endValue
      ? core::Expressions::inferTypes(windowFrame.endValue, inputRow, pool)
      : nullptr;
  return frame;
}

std::vector<core::FieldAccessTypedExprPtr> parsePartitionKeys(
    const duckdb::IExprWindowFunction& windowExpr,
    const std::string& windowString,
    const TypePtr& inputRow,
    memory::MemoryPool* pool) {
  std::vector<core::FieldAccessTypedExprPtr> partitionKeys;
  for (const auto& partitionKey : windowExpr.partitionBy) {
    auto typedExpr =
        core::Expressions::inferTypes(partitionKey, inputRow, pool);
    auto typedPartitionKey =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(typedExpr);
    VELOX_CHECK_NOT_NULL(
        typedPartitionKey,
        "PARTITION BY clause must use a column name, not an expression: {}",
        windowString);
    partitionKeys.emplace_back(typedPartitionKey);
  }
  return partitionKeys;
}

std::pair<
    std::vector<core::FieldAccessTypedExprPtr>,
    std::vector<core::SortOrder>>
parseOrderByKeys(
    const duckdb::IExprWindowFunction& windowExpr,
    const std::string& windowString,
    const TypePtr& inputRow,
    memory::MemoryPool* pool) {
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;

  for (const auto& [untypedExpr, sortOrder] : windowExpr.orderBy) {
    auto typedExpr = core::Expressions::inferTypes(untypedExpr, inputRow, pool);
    auto sortingKey =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(typedExpr);
    VELOX_CHECK_NOT_NULL(
        sortingKey,
        "ORDER BY clause must use a column name, not an expression: {}",
        windowString);
    sortingKeys.emplace_back(sortingKey);
    sortingOrders.emplace_back(sortOrder);
  }
  return {sortingKeys, sortingOrders};
}

bool equalFieldAccessTypedExprPtrList(
    const std::vector<core::FieldAccessTypedExprPtr>& lhs,
    const std::vector<core::FieldAccessTypedExprPtr>& rhs) {
  return std::equal(
      lhs.begin(),
      lhs.end(),
      rhs.begin(),
      [](const core::FieldAccessTypedExprPtr& e1,
         const core::FieldAccessTypedExprPtr& e2) {
        return e1->name() == e2->name();
      });
}

bool equalSortOrderList(
    const std::vector<core::SortOrder>& lhs,
    const std::vector<core::SortOrder>& rhs) {
  return std::equal(
      lhs.begin(),
      lhs.end(),
      rhs.begin(),
      [](const core::SortOrder& s1, const core::SortOrder& s2) {
        return s1.isAscending() == s2.isAscending() &&
            s1.isNullsFirst() == s2.isNullsFirst();
      });
}

} // namespace

PlanBuilder& PlanBuilder::window(
    const std::vector<std::string>& windowFunctions,
    bool inputSorted) {
  VELOX_CHECK_NOT_NULL(planNode_, "Window cannot be the source node");
  VELOX_CHECK_GT(
      windowFunctions.size(),
      0,
      "Window Node requires at least one window function.");

  std::vector<core::FieldAccessTypedExprPtr> partitionKeys;
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  std::vector<core::WindowNode::Function> windowNodeFunctions;
  std::vector<std::string> windowNames;

  bool first = true;
  auto inputType = planNode_->outputType();
  int i = 0;

  auto errorOnMismatch = [&](const std::string& windowString,
                             const std::string& mismatchTypeString) -> void {
    std::stringstream error;
    error << "Window function invocations " << windowString << " and "
          << windowFunctions[0] << " do not match " << mismatchTypeString
          << " clauses.";
    VELOX_USER_FAIL(error.str());
  };

  WindowTypeResolver windowResolver;
  facebook::velox::duckdb::ParseOptions options;
  options.parseIntegerAsBigint = options_.parseIntegerAsBigint;
  for (const auto& windowString : windowFunctions) {
    const auto& windowExpr = duckdb::parseWindowExpr(windowString, options);
    // All window function SQL strings in the list are expected to have the same
    // PARTITION BY and ORDER BY clauses. Validate this assumption.
    if (first) {
      partitionKeys =
          parsePartitionKeys(windowExpr, windowString, inputType, pool_);
      auto sortPair =
          parseOrderByKeys(windowExpr, windowString, inputType, pool_);
      sortingKeys = sortPair.first;
      sortingOrders = sortPair.second;
      first = false;
    } else {
      auto latestPartitionKeys =
          parsePartitionKeys(windowExpr, windowString, inputType, pool_);
      auto [latestSortingKeys, latestSortingOrders] =
          parseOrderByKeys(windowExpr, windowString, inputType, pool_);

      if (!equalFieldAccessTypedExprPtrList(
              partitionKeys, latestPartitionKeys)) {
        errorOnMismatch(windowString, "PARTITION BY");
      }

      if (!equalFieldAccessTypedExprPtrList(sortingKeys, latestSortingKeys)) {
        errorOnMismatch(windowString, "ORDER BY");
      }

      if (!equalSortOrderList(sortingOrders, latestSortingOrders)) {
        errorOnMismatch(windowString, "ORDER BY");
      }
    }

    auto windowCall = std::dynamic_pointer_cast<const core::CallTypedExpr>(
        core::Expressions::inferTypes(
            windowExpr.functionCall, planNode_->outputType(), pool_));
    windowNodeFunctions.push_back(
        {std::move(windowCall),
         createWindowFrame(windowExpr.frame, planNode_->outputType(), pool_),
         windowExpr.ignoreNulls});
    if (windowExpr.functionCall->alias().has_value()) {
      windowNames.push_back(windowExpr.functionCall->alias().value());
    } else {
      windowNames.push_back(fmt::format("w{}", i++));
    }
  }

  planNode_ = std::make_shared<core::WindowNode>(
      nextPlanNodeId(),
      partitionKeys,
      sortingKeys,
      sortingOrders,
      windowNames,
      windowNodeFunctions,
      inputSorted,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::window(
    const std::vector<std::string>& windowFunctions) {
  return window(windowFunctions, false);
}

PlanBuilder& PlanBuilder::streamingWindow(
    const std::vector<std::string>& windowFunctions) {
  return window(windowFunctions, true);
}

PlanBuilder& PlanBuilder::rowNumber(
    const std::vector<std::string>& partitionKeys,
    std::optional<int32_t> limit,
    const bool generateRowNumber) {
  std::optional<std::string> rowNumberColumnName;
  if (generateRowNumber) {
    rowNumberColumnName = "row_number";
  }
  planNode_ = std::make_shared<core::RowNumberNode>(
      nextPlanNodeId(),
      fields(partitionKeys),
      rowNumberColumnName,
      limit,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::topNRowNumber(
    const std::vector<std::string>& partitionKeys,
    const std::vector<std::string>& sortingKeys,
    int32_t limit,
    bool generateRowNumber) {
  VELOX_CHECK_NOT_NULL(planNode_, "TopNRowNumber cannot be the source node");
  auto [sortingFields, sortingOrders] =
      parseOrderByClauses(sortingKeys, planNode_->outputType(), pool_);
  std::optional<std::string> rowNumberColumnName;
  if (generateRowNumber) {
    rowNumberColumnName = "row_number";
  }
  planNode_ = std::make_shared<core::TopNRowNumberNode>(
      nextPlanNodeId(),
      fields(partitionKeys),
      sortingFields,
      sortingOrders,
      rowNumberColumnName,
      limit,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::markDistinct(
    std::string markerKey,
    const std::vector<std::string>& distinctKeys) {
  VELOX_CHECK_NOT_NULL(planNode_, "MarkDistinct cannot be the source node");
  planNode_ = std::make_shared<core::MarkDistinctNode>(
      nextPlanNodeId(),
      std::move(markerKey),
      fields(planNode_->outputType(), distinctKeys),

      planNode_);
  return *this;
}

core::PlanNodeId PlanBuilder::nextPlanNodeId() {
  return planNodeIdGenerator_->next();
}

// static
std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    const RowTypePtr& inputType,
    const std::string& name) {
  auto index = inputType->getChildIdx(name);
  return field(inputType, index);
}

// static
std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    const RowTypePtr& inputType,
    column_index_t index) {
  auto name = inputType->names()[index];
  auto type = inputType->childAt(index);
  return std::make_shared<core::FieldAccessTypedExpr>(type, name);
}

// static
std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(
    const RowTypePtr& inputType,
    const std::vector<std::string>& names) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields;
  for (const auto& name : names) {
    fields.push_back(field(inputType, name));
  }
  return fields;
}

// static
std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& indices) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields;
  for (auto& index : indices) {
    fields.push_back(field(inputType, index));
  }
  return fields;
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    column_index_t index) {
  VELOX_CHECK_NOT_NULL(planNode_);
  return field(planNode_->outputType(), index);
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    const std::string& name) {
  VELOX_CHECK_NOT_NULL(planNode_);
  return field(planNode_->outputType(), name);
}

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(const std::vector<std::string>& names) {
  VELOX_CHECK_NOT_NULL(planNode_);
  return fields(planNode_->outputType(), names);
}

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(const std::vector<column_index_t>& indices) {
  VELOX_CHECK_NOT_NULL(planNode_);
  return fields(planNode_->outputType(), indices);
}

std::vector<core::TypedExprPtr> PlanBuilder::exprs(
    const std::vector<std::string>& expressions,
    const RowTypePtr& inputType) {
  std::vector<core::TypedExprPtr> typedExpressions;
  for (auto& expr : expressions) {
    auto typedExpression = core::Expressions::inferTypes(
        parse::parseExpr(expr, options_), inputType, pool_);

    if (auto field = dynamic_cast<const core::FieldAccessTypedExpr*>(
            typedExpression.get())) {
      typedExpressions.push_back(typedExpression);
    } else if (
        auto constant = dynamic_cast<const core::ConstantTypedExpr*>(
            typedExpression.get())) {
      typedExpressions.push_back(typedExpression);
    } else {
      VELOX_FAIL("Expected field name or constant: {}", expr);
    }
  }

  return typedExpressions;
}

core::TypedExprPtr PlanBuilder::inferTypes(
    const std::shared_ptr<const core::IExpr>& untypedExpr) {
  VELOX_CHECK_NOT_NULL(planNode_);
  return core::Expressions::inferTypes(
      untypedExpr, planNode_->outputType(), pool_);
}
} // namespace facebook::velox::exec::test
