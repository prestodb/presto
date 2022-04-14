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
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/duckdb/conversion/DuckParser.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/parse/Expressions.h"
#include "velox/type/tests/FilterBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;

namespace facebook::velox::exec::test {

namespace {

std::shared_ptr<const core::ITypedExpr> parseExpr(
    const std::string& text,
    const RowTypePtr& rowType,
    memory::MemoryPool* pool) {
  auto untyped = duckdb::parseExpr(text);
  return core::Expressions::inferTypes(untyped, rowType, pool);
}

template <TypeKind FromKind, TypeKind ToKind>
typename TypeTraits<ToKind>::NativeType cast(const variant& v) {
  bool nullOutput;
  return util::Converter<ToKind, void, false>::cast(
      v.value<FromKind>(), nullOutput);
}

VectorPtr toConstant(
    const std::shared_ptr<const core::ITypedExpr>& expr,
    const std::shared_ptr<core::QueryCtx>& queryCtx) {
  auto data = std::make_shared<RowVector>(
      queryCtx->pool(), ROW({}, {}), nullptr, 1, std::vector<VectorPtr>{});
  core::ExecCtx execCtx{queryCtx->pool(), queryCtx.get()};
  ExprSet exprSet({expr}, &execCtx);
  exec::EvalCtx evalCtx(&execCtx, &exprSet, data.get());

  SelectivityVector rows(1);
  std::vector<VectorPtr> results(1);
  exprSet.eval(rows, &evalCtx, &results);

  return results[0];
}

template <typename T>
T singleValue(const VectorPtr& vector) {
  auto simpleVector = vector->as<SimpleVector<T>>();
  VELOX_CHECK_NOT_NULL(simpleVector);
  return simpleVector->valueAt(0);
}

const core::FieldAccessTypedExpr* asField(
    const core::ITypedExpr* expr,
    int index) {
  return dynamic_cast<const core::FieldAccessTypedExpr*>(
      expr->inputs()[index].get());
}

const core::CallTypedExpr* asCall(const core::ITypedExpr* expr) {
  return dynamic_cast<const core::CallTypedExpr*>(expr);
}

common::BigintRange* asBigintRange(std::unique_ptr<common::Filter>& filter) {
  return dynamic_cast<common::BigintRange*>(filter.get());
}

common::BigintMultiRange* asBigintMultiRange(
    std::unique_ptr<common::Filter>& filter) {
  return dynamic_cast<common::BigintMultiRange*>(filter.get());
}

template <typename T, typename U>
std::unique_ptr<T> asUniquePtr(std::unique_ptr<U> ptr) {
  return std::unique_ptr<T>(static_cast<T*>(ptr.release()));
}

std::unique_ptr<common::Filter> makeOrFilter(
    std::unique_ptr<common::Filter> a,
    std::unique_ptr<common::Filter> b) {
  if (asBigintRange(a) && asBigintRange(b)) {
    return common::test::bigintOr(
        asUniquePtr<common::BigintRange>(std::move(a)),
        asUniquePtr<common::BigintRange>(std::move(b)));
  }

  if (asBigintRange(a) && asBigintMultiRange(b)) {
    const auto& ranges = asBigintMultiRange(b)->ranges();
    std::vector<std::unique_ptr<common::BigintRange>> newRanges;
    newRanges.emplace_back(asUniquePtr<common::BigintRange>(std::move(a)));
    for (const auto& range : ranges) {
      newRanges.emplace_back(asUniquePtr<common::BigintRange>(range->clone()));
    }

    std::sort(
        newRanges.begin(), newRanges.end(), [](const auto& a, const auto& b) {
          return a->lower() < b->lower();
        });

    return std::make_unique<common::BigintMultiRange>(
        std::move(newRanges), false);
  }

  if (asBigintMultiRange(a) && asBigintRange(b)) {
    return makeOrFilter(std::move(b), std::move(a));
  }

  return common::test::orFilter(std::move(a), std::move(b));
}

std::unique_ptr<common::Filter> makeLessThanOrEqualFilter(
    const std::shared_ptr<const core::ITypedExpr>& upperExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto upper = toConstant(upperExpr, queryCtx);
  switch (upper->typeKind()) {
    case TypeKind::INTEGER:
      return common::test::lessThanOrEqual(singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      return common::test::lessThanOrEqual(singleValue<int64_t>(upper));
    case TypeKind::DOUBLE:
      return common::test::lessThanOrEqualDouble(singleValue<double>(upper));
    case TypeKind::REAL:
      return common::test::lessThanOrEqualFloat(singleValue<float>(upper));
    case TypeKind::VARCHAR:
      return common::test::lessThanOrEqual(singleValue<StringView>(upper));
    case TypeKind::DATE:
      return common::test::lessThanOrEqual(singleValue<Date>(upper).days());
    default:
      VELOX_NYI(
          "Unsupported value for less than or equals filter: {} <= {}",
          upper->type()->toString(),
          upper->toString(0));
  }
}

std::unique_ptr<common::Filter> makeLessThanFilter(
    const std::shared_ptr<const core::ITypedExpr>& upperExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto upper = toConstant(upperExpr, queryCtx);
  switch (upper->typeKind()) {
    case TypeKind::INTEGER:
      return common::test::lessThan(singleValue<int32_t>(upper));
    case TypeKind::BIGINT:
      return common::test::lessThan(singleValue<int64_t>(upper));
    case TypeKind::DOUBLE:
      return common::test::lessThanDouble(singleValue<double>(upper));
    case TypeKind::REAL:
      return common::test::lessThanFloat(singleValue<float>(upper));
    case TypeKind::VARCHAR:
      return common::test::lessThan(singleValue<StringView>(upper));
    case TypeKind::DATE:
      return common::test::lessThan(singleValue<Date>(upper).days());
    default:
      VELOX_NYI(
          "Unsupported value for less than filter: {} <= {}",
          upper->type()->toString(),
          upper->toString(0));
  }
}

std::unique_ptr<common::Filter> makeGreaterThanOrEqualFilter(
    const std::shared_ptr<const core::ITypedExpr>& lowerExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto lower = toConstant(lowerExpr, queryCtx);
  switch (lower->typeKind()) {
    case TypeKind::INTEGER:
      return common::test::greaterThanOrEqual(singleValue<int32_t>(lower));
    case TypeKind::BIGINT:
      return common::test::greaterThanOrEqual(singleValue<int64_t>(lower));
    case TypeKind::DOUBLE:
      return common::test::greaterThanOrEqualDouble(singleValue<double>(lower));
    case TypeKind::REAL:
      return common::test::greaterThanOrEqualFloat(singleValue<float>(lower));
    case TypeKind::VARCHAR:
      return common::test::greaterThanOrEqual(singleValue<StringView>(lower));
    case TypeKind::DATE:
      return common::test::greaterThanOrEqual(singleValue<Date>(lower).days());
    default:
      VELOX_NYI(
          "Unsupported value for greater than or equals filter: {} >= {}",
          lower->type()->toString(),
          lower->toString(0));
  }
}

std::unique_ptr<common::Filter> makeGreaterThanFilter(
    const std::shared_ptr<const core::ITypedExpr>& lowerExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto lower = toConstant(lowerExpr, queryCtx);
  switch (lower->typeKind()) {
    case TypeKind::INTEGER:
      return common::test::greaterThan(singleValue<int32_t>(lower));
    case TypeKind::BIGINT:
      return common::test::greaterThan(singleValue<int64_t>(lower));
    case TypeKind::DOUBLE:
      return common::test::greaterThanDouble(singleValue<double>(lower));
    case TypeKind::REAL:
      return common::test::greaterThanFloat(singleValue<float>(lower));
    case TypeKind::VARCHAR:
      return common::test::greaterThan(singleValue<StringView>(lower));
    case TypeKind::DATE:
      return common::test::greaterThan(singleValue<Date>(lower).days());
    default:
      VELOX_NYI(
          "Unsupported value for greater than filter: {} > {}",
          lower->type()->toString(),
          lower->toString(0));
  }
}

std::unique_ptr<common::Filter> makeEqualFilter(
    const std::shared_ptr<const core::ITypedExpr>& valueExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto value = toConstant(valueExpr, queryCtx);
  switch (value->typeKind()) {
    case TypeKind::BOOLEAN:
      return common::test::boolEqual(singleValue<bool>(value));
    case TypeKind::INTEGER:
      return common::test::equal(singleValue<int32_t>(value));
    case TypeKind::BIGINT:
      return common::test::equal(singleValue<int64_t>(value));
    case TypeKind::VARCHAR:
      return common::test::equal(singleValue<StringView>(value));
    case TypeKind::DATE:
      return common::test::equal(singleValue<Date>(value).days());
    default:
      VELOX_NYI(
          "Unsupported value for equals filter: {} = {}",
          value->type()->toString(),
          value->toString(0));
  }
}

std::unique_ptr<common::Filter> makeBetweenFilter(
    const std::shared_ptr<const core::ITypedExpr>& lowerExpr,
    const std::shared_ptr<const core::ITypedExpr>& upperExpr) {
  auto queryCtx = core::QueryCtx::createForTest();
  auto lower = toConstant(lowerExpr, queryCtx);
  auto upper = toConstant(upperExpr, queryCtx);
  switch (lower->typeKind()) {
    case TypeKind::BIGINT:
      return common::test::between(
          singleValue<int64_t>(lower), singleValue<int64_t>(upper));
    case TypeKind::DOUBLE:
      return common::test::betweenDouble(
          singleValue<double>(lower), singleValue<double>(upper));
    case TypeKind::REAL:
      return common::test::betweenFloat(
          singleValue<float>(lower), singleValue<float>(upper));
    case TypeKind::DATE:
      return common::test::between(
          singleValue<Date>(lower).days(), singleValue<Date>(upper).days());
    default:
      VELOX_NYI(
          "Unsupported value for 'between' filter: {} BETWEEN {} AND {}",
          lower->type()->toString(),
          lower->toString(0),
          upper->toString(0));
  }
}

std::pair<common::Subfield, std::unique_ptr<common::Filter>> toSubfieldFilter(
    const std::shared_ptr<const core::ITypedExpr>& expr) {
  using common::Subfield;

  if (auto call = asCall(expr.get())) {
    if (call->name() == "or") {
      auto left = toSubfieldFilter(call->inputs()[0]);
      auto right = toSubfieldFilter(call->inputs()[1]);
      VELOX_CHECK(left.first == right.first);
      return {
          std::move(left.first),
          makeOrFilter(std::move(left.second), std::move(right.second))};
    } else if (call->name() == "eq") {
      if (auto field = asField(call, 0)) {
        return {Subfield(field->name()), makeEqualFilter(call->inputs()[1])};
      }
    } else if (call->name() == "lte") {
      if (auto field = asField(call, 0)) {
        return {
            Subfield(field->name()),
            makeLessThanOrEqualFilter(call->inputs()[1])};
      }
    } else if (call->name() == "lt") {
      if (auto field = asField(call, 0)) {
        return {Subfield(field->name()), makeLessThanFilter(call->inputs()[1])};
      }
    } else if (call->name() == "gte") {
      if (auto field = asField(call, 0)) {
        return {
            Subfield(field->name()),
            makeGreaterThanOrEqualFilter(call->inputs()[1])};
      }
    } else if (call->name() == "gt") {
      if (auto field = asField(call, 0)) {
        return {
            Subfield(field->name()), makeGreaterThanFilter(call->inputs()[1])};
      }
    } else if (call->name() == "between") {
      if (auto field = asField(call, 0)) {
        return {
            Subfield(field->name()),
            makeBetweenFilter(call->inputs()[1], call->inputs()[2])};
      }
    } else if (call->name() == "is_null") {
      if (auto field = asField(call, 0)) {
        return {Subfield(field->name()), common::test::isNull()};
      }
    } else if (call->name() == "not") {
      if (auto nestedCall = asCall(call->inputs()[0].get())) {
        if (nestedCall->name() == "is_null") {
          if (auto field = asField(nestedCall, 0)) {
            return {Subfield(field->name()), common::test::isNotNull()};
          }
        }
      }
    }
  }

  VELOX_NYI("Unsupported expression for range filter: {}", expr->toString());
}
} // namespace

PlanBuilder& PlanBuilder::tableScan(
    const RowTypePtr& outputType,
    const std::vector<std::string>& subfieldFilters,
    const std::string& remainingFilter) {
  return tableScan(
      "hive_table", outputType, {}, subfieldFilters, remainingFilter);
}

PlanBuilder& PlanBuilder::tableScan(
    const std::string& tableName,
    const RowTypePtr& outputType,
    const std::unordered_map<std::string, std::string>& columnAliases,
    const std::vector<std::string>& subfieldFilters,
    const std::string& remainingFilter) {
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (uint32_t i = 0; i < outputType->size(); ++i) {
    const auto& name = outputType->nameOf(i);
    const auto& type = outputType->childAt(i);

    std::string hiveColumnName = name;
    auto it = columnAliases.find(name);
    if (it != columnAliases.end()) {
      hiveColumnName = it->second;
    }

    assignments.insert(
        {name,
         std::make_shared<HiveColumnHandle>(
             hiveColumnName, HiveColumnHandle::ColumnType::kRegular, type)});
  }

  SubfieldFilters filters;
  filters.reserve(subfieldFilters.size());
  for (const auto& filter : subfieldFilters) {
    auto filterExpr = parseExpr(filter, outputType, pool_);
    auto [subfield, subfieldFilter] = toSubfieldFilter(filterExpr);

    auto it = columnAliases.find(subfield.toString());
    if (it != columnAliases.end()) {
      subfield = common::Subfield(it->second);
    }

    filters[std::move(subfield)] = std::move(subfieldFilter);
  }

  std::shared_ptr<const core::ITypedExpr> remainingFilterExpr;
  if (!remainingFilter.empty()) {
    remainingFilterExpr = parseExpr(remainingFilter, outputType, pool_)
                              ->rewriteInputNames(columnAliases);
  }

  auto tableHandle = std::make_shared<HiveTableHandle>(
      tableName, true, std::move(filters), remainingFilterExpr);
  return tableScan(outputType, tableHandle, assignments);
}

PlanBuilder& PlanBuilder::tableScan(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& assignments) {
  planNode_ = std::make_shared<core::TableScanNode>(
      nextPlanNodeId(), outputType, tableHandle, assignments);
  return *this;
}

PlanBuilder& PlanBuilder::values(
    const std::vector<RowVectorPtr>& values,
    bool parallelizable) {
  auto valuesCopy = values;
  planNode_ = std::make_shared<core::ValuesNode>(
      nextPlanNodeId(), std::move(valuesCopy), parallelizable);
  return *this;
}

PlanBuilder& PlanBuilder::exchange(const RowTypePtr& outputType) {
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
    auto [untypedExpr, sortOrder] = duckdb::parseOrderByExpr(key);
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
  auto [sortingKeys, sortingOrders] =
      parseOrderByClauses(keys, outputType, pool_);

  planNode_ = std::make_shared<core::MergeExchangeNode>(
      nextPlanNodeId(), outputType, sortingKeys, sortingOrders);

  return *this;
}

PlanBuilder& PlanBuilder::project(const std::vector<std::string>& projections) {
  std::vector<std::shared_ptr<const core::ITypedExpr>> expressions;
  std::vector<std::string> projectNames;
  for (auto i = 0; i < projections.size(); ++i) {
    auto untypedExpr = duckdb::parseExpr(projections[i]);
    expressions.push_back(inferTypes(untypedExpr));
    if (untypedExpr->alias().has_value()) {
      projectNames.push_back(untypedExpr->alias().value());
    } else if (
        auto fieldExpr =
            dynamic_cast<const core::FieldAccessExpr*>(untypedExpr.get())) {
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

PlanBuilder& PlanBuilder::filter(const std::string& filter) {
  planNode_ = std::make_shared<core::FilterNode>(
      nextPlanNodeId(),
      parseExpr(filter, planNode_->outputType(), pool_),
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::tableWrite(
    const std::vector<std::string>& columnNames,
    const std::shared_ptr<core::InsertTableHandle>& insertHandle,
    const std::string& rowCountColumnName) {
  return tableWrite(
      planNode_->outputType(), columnNames, insertHandle, rowCountColumnName);
}

PlanBuilder& PlanBuilder::tableWrite(
    const RowTypePtr& columns,
    const std::vector<std::string>& columnNames,
    const std::shared_ptr<core::InsertTableHandle>& insertHandle,
    const std::string& rowCountColumnName) {
  auto outputType =
      ROW({rowCountColumnName, "fragments", "commitcontext"},
          {BIGINT(), VARBINARY(), VARBINARY()});
  planNode_ = std::make_shared<core::TableWriteNode>(
      nextPlanNodeId(),
      columns,
      columnNames,
      insertHandle,
      outputType,
      planNode_);
  return *this;
}

namespace {

template <TypeKind Kind>
TypePtr nameToType() {
  using T = typename TypeTraits<Kind>::NativeType;
  return CppToType<T>::create();
}

TypePtr resolveAggregateType(
    const std::string& aggregateName,
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& rawInputTypes) {
  if (auto signatures = exec::getAggregateFunctionSignatures(aggregateName)) {
    for (const auto& signature : signatures.value()) {
      exec::SignatureBinder binder(*signature, rawInputTypes);
      if (binder.tryBind()) {
        return binder.tryResolveType(
            exec::isPartialOutput(step) ? signature->intermediateType()
                                        : signature->returnType());
      }
    }
  }

  return nullptr;
}

class AggregateTypeResolver {
 public:
  explicit AggregateTypeResolver(core::AggregationNode::Step step)
      : step_(step), previousHook_(core::Expressions::getResolverHook()) {
    core::Expressions::setTypeResolverHook(
        [&](const auto& inputs, const auto& expr) {
          return resolveType(inputs, expr);
        });
  }

  ~AggregateTypeResolver() {
    core::Expressions::setTypeResolverHook(previousHook_);
  }

  void setResultType(const TypePtr& type) {
    resultType_ = type;
  }

 private:
  std::shared_ptr<const Type> resolveType(
      const std::vector<std::shared_ptr<const core::ITypedExpr>>& inputs,
      const std::shared_ptr<const core::CallExpr>& expr) const {
    if (resultType_) {
      return resultType_;
    }

    std::vector<TypePtr> types;
    for (auto& input : inputs) {
      types.push_back(input->type());
    }

    auto functionName = expr->getFunctionName();

    // Use raw input types (if available) to resolve intermediate and final
    // result types.
    if (exec::isRawInput(step_)) {
      if (auto type = resolveAggregateType(functionName, step_, types)) {
        return type;
      }
    }

    return nullptr;
  }

  const core::AggregationNode::Step step_;
  const core::Expressions::TypeResolverHook previousHook_;
  TypePtr resultType_;
};

} // namespace

std::shared_ptr<core::PlanNode>
PlanBuilder::createIntermediateOrFinalAggregation(
    core::AggregationNode::Step step,
    const core::AggregationNode* partialAggNode) {
  // Create intermediate or final aggregation using same grouping keys and same
  // aggregate function names.
  const auto& partialAggregates = partialAggNode->aggregates();
  const auto& groupingKeys = partialAggNode->groupingKeys();

  auto numAggregates = partialAggregates.size();
  auto numGroupingKeys = groupingKeys.size();

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> masks(
      numAggregates);

  std::vector<std::shared_ptr<const core::CallTypedExpr>> aggregates;
  aggregates.reserve(numAggregates);
  for (auto i = 0; i < numAggregates; i++) {
    // Resolve final or intermediate aggregation result type using raw input
    // types for the partial aggregation.
    auto name = partialAggregates[i]->name();
    auto rawInputs = partialAggregates[i]->inputs();

    std::vector<TypePtr> rawInputTypes;
    for (auto& rawInput : rawInputs) {
      rawInputTypes.push_back(rawInput->type());
    }

    auto type = resolveAggregateType(name, step, rawInputTypes);
    VELOX_CHECK_NOT_NULL(
        type, "Failed to resolve result type for aggregate function {}", name);
    std::vector<std::shared_ptr<const core::ITypedExpr>> inputs = {
        field(numGroupingKeys + i)};
    aggregates.emplace_back(
        std::make_shared<core::CallTypedExpr>(type, std::move(inputs), name));
  }

  return std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      step,
      groupingKeys,
      partialAggNode->preGroupedKeys(),
      partialAggNode->aggregateNames(),
      aggregates,
      masks,
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
  } else {
    aggNode = dynamic_cast<const core::AggregationNode*>(planNode);
  }
  VELOX_CHECK_NOT_NULL(
      aggNode,
      "Current plan node must be a partial or intermediate aggregation or local exchange over the same. Got: {}",
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

PlanBuilder::AggregateExpressionsAndNames
PlanBuilder::createAggregateExpressionsAndNames(
    const std::vector<std::string>& aggregates,
    core::AggregationNode::Step step,
    const std::vector<TypePtr>& resultTypes) {
  AggregateTypeResolver resolver(step);
  std::vector<std::shared_ptr<const core::CallTypedExpr>> exprs;
  std::vector<std::string> names;
  exprs.reserve(aggregates.size());
  names.reserve(aggregates.size());
  for (auto i = 0; i < aggregates.size(); i++) {
    auto& agg = aggregates[i];
    if (i < resultTypes.size()) {
      resolver.setResultType(resultTypes[i]);
    }

    auto untypedExpr = duckdb::parseExpr(agg);

    auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(
        inferTypes(untypedExpr));
    exprs.emplace_back(expr);

    if (untypedExpr->alias().has_value()) {
      names.push_back(untypedExpr->alias().value());
    } else {
      names.push_back(fmt::format("a{}", i));
    }
  }

  return {exprs, names};
}

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::createAggregateMasks(
    size_t numAggregates,
    const std::vector<std::string>& masks) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> maskExprs(
      numAggregates);
  if (masks.empty()) {
    return maskExprs;
  }

  VELOX_CHECK_EQ(numAggregates, masks.size());
  for (auto i = 0; i < numAggregates; ++i) {
    if (!masks[i].empty()) {
      maskExprs[i] = field(masks[i]);
    }
  }

  return maskExprs;
}

PlanBuilder& PlanBuilder::aggregation(
    const std::vector<ChannelIndex>& groupingKeys,
    const std::vector<ChannelIndex>& preGroupedKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    core::AggregationNode::Step step,
    bool ignoreNullKeys,
    const std::vector<TypePtr>& resultTypes) {
  auto numAggregates = aggregates.size();
  auto aggregatesAndNames =
      createAggregateExpressionsAndNames(aggregates, step, resultTypes);
  planNode_ = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      step,
      fields(groupingKeys),
      fields(preGroupedKeys),
      aggregatesAndNames.names,
      aggregatesAndNames.aggregates,
      createAggregateMasks(numAggregates, masks),
      ignoreNullKeys,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::streamingAggregation(
    const std::vector<ChannelIndex>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& masks,
    core::AggregationNode::Step step,
    bool ignoreNullKeys,
    const std::vector<TypePtr>& resultTypes) {
  auto numAggregates = aggregates.size();
  auto aggregatesAndNames =
      createAggregateExpressionsAndNames(aggregates, step, resultTypes);
  planNode_ = std::make_shared<core::AggregationNode>(
      nextPlanNodeId(),
      step,
      fields(groupingKeys),
      fields(groupingKeys),
      aggregatesAndNames.names,
      aggregatesAndNames.aggregates,
      createAggregateMasks(numAggregates, masks),
      ignoreNullKeys,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::localMerge(
    const std::vector<std::string>& keys,
    std::vector<std::shared_ptr<const core::PlanNode>> sources) {
  VELOX_CHECK_NULL(planNode_, "localMerge() must be the first call");
  VELOX_CHECK_GE(
      sources.size(), 1, "localMerge() requires at least one source");

  const auto& inputType = sources[0]->outputType();
  auto [sortingKeys, sortingOrders] =
      parseOrderByClauses(keys, inputType, pool_);

  planNode_ = std::make_shared<core::LocalMergeNode>(
      nextPlanNodeId(), sortingKeys, sortingOrders, std::move(sources));

  return *this;
}

PlanBuilder& PlanBuilder::orderBy(
    const std::vector<std::string>& keys,
    bool isPartial) {
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

PlanBuilder& PlanBuilder::limit(int32_t offset, int32_t count, bool isPartial) {
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
core::PartitionFunctionFactory createPartitionFunctionFactory(
    const RowTypePtr& inputType,
    const std::vector<std::string>& keys) {
  if (keys.empty()) {
    return
        [](auto /*numPartitions*/) -> std::unique_ptr<core::PartitionFunction> {
          VELOX_UNREACHABLE();
        };
  } else {
    std::vector<ChannelIndex> keyIndices;
    keyIndices.reserve(keys.size());
    for (const auto& key : keys) {
      keyIndices.push_back(inputType->getChildIdx(key));
    }
    return [inputType, keyIndices](
               auto numPartitions) -> std::unique_ptr<core::PartitionFunction> {
      return std::make_unique<exec::HashPartitionFunction>(
          numPartitions, inputType, keyIndices);
    };
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
  auto outputType = outputLayout.empty()
      ? planNode_->outputType()
      : extract(planNode_->outputType(), outputLayout);
  auto partitionFunctionFactory =
      createPartitionFunctionFactory(planNode_->outputType(), keys);
  planNode_ = std::make_shared<core::PartitionedOutputNode>(
      nextPlanNodeId(),
      fields(keys),
      numPartitions,
      false,
      replicateNullsAndAny,
      std::move(partitionFunctionFactory),
      outputType,
      planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::partitionedOutputBroadcast(
    const std::vector<std::string>& outputLayout) {
  auto outputType = outputLayout.empty()
      ? planNode_->outputType()
      : extract(planNode_->outputType(), outputLayout);
  planNode_ = core::PartitionedOutputNode::broadcast(
      nextPlanNodeId(), 1, outputType, planNode_);
  return *this;
}

PlanBuilder& PlanBuilder::localPartition(
    const std::vector<std::string>& keys,
    const std::vector<std::shared_ptr<const core::PlanNode>>& sources,
    const std::vector<std::string>& outputLayout) {
  VELOX_CHECK_NULL(planNode_, "localPartition() must be the first call");
  auto inputType = sources[0]->outputType();
  auto outputType = outputLayout.empty() ? sources[0]->outputType()
                                         : extract(inputType, outputLayout);
  auto partitionFunctionFactory =
      createPartitionFunctionFactory(inputType, keys);
  planNode_ = std::make_shared<core::LocalPartitionNode>(
      nextPlanNodeId(),
      keys.empty() ? core::LocalPartitionNode::Type::kGather
                   : core::LocalPartitionNode::Type::kRepartition,
      partitionFunctionFactory,
      outputType,
      sources);
  return *this;
}

PlanBuilder& PlanBuilder::localPartitionRoundRobin(
    const std::vector<std::shared_ptr<const core::PlanNode>>& sources,
    const std::vector<std::string>& outputLayout) {
  VELOX_CHECK_NULL(planNode_, "localPartition() must be the first call");
  auto inputType = sources[0]->outputType();
  auto outputType = outputLayout.empty() ? sources[0]->outputType()
                                         : extract(inputType, outputLayout);
  auto partitionFunctionFactory = [](auto numPartitions) {
    return std::make_unique<velox::exec::RoundRobinPartitionFunction>(
        numPartitions);
  };
  planNode_ = std::make_shared<core::LocalPartitionNode>(
      nextPlanNodeId(),
      core::LocalPartitionNode::Type::kRepartition,
      partitionFunctionFactory,
      outputType,
      sources);
  return *this;
}

PlanBuilder& PlanBuilder::hashJoin(
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const std::shared_ptr<facebook::velox::core::PlanNode>& build,
    const std::string& filterText,
    const std::vector<std::string>& output,
    core::JoinType joinType) {
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());

  auto leftType = planNode_->outputType();
  auto rightType = build->outputType();
  auto resultType = concat(leftType, rightType);
  std::shared_ptr<const core::ITypedExpr> filterExpr;
  if (!filterText.empty()) {
    filterExpr = parseExpr(filterText, resultType, pool_);
  }
  auto outputType = extract(resultType, output);
  auto leftKeyFields = fields(leftType, leftKeys);
  auto rightKeyFields = fields(rightType, rightKeys);

  planNode_ = std::make_shared<core::HashJoinNode>(
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

PlanBuilder& PlanBuilder::mergeJoin(
    const std::vector<std::string>& leftKeys,
    const std::vector<std::string>& rightKeys,
    const std::shared_ptr<facebook::velox::core::PlanNode>& build,
    const std::string& filterText,
    const std::vector<std::string>& output,
    core::JoinType joinType) {
  VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());

  auto leftType = planNode_->outputType();
  auto rightType = build->outputType();
  auto resultType = concat(leftType, rightType);
  std::shared_ptr<const core::ITypedExpr> filterExpr;
  if (!filterText.empty()) {
    filterExpr = parseExpr(filterText, resultType, pool_);
  }
  auto outputType = extract(resultType, output);
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

PlanBuilder& PlanBuilder::crossJoin(
    const std::shared_ptr<core::PlanNode>& build,
    const std::vector<std::string>& output) {
  auto resultType = concat(planNode_->outputType(), build->outputType());
  auto outputType = extract(resultType, output);

  planNode_ = std::make_shared<core::CrossJoinNode>(
      nextPlanNodeId(), std::move(planNode_), build, outputType);
  return *this;
}

PlanBuilder& PlanBuilder::unnest(
    const std::vector<std::string>& replicateColumns,
    const std::vector<std::string>& unnestColumns,
    const std::optional<std::string>& ordinalColumn) {
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

std::string PlanBuilder::nextPlanNodeId() {
  return fmt::format("{}", planNodeIdGenerator_->next());
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    int index) {
  return field(planNode_->outputType(), index);
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    const std::string& name) {
  return field(planNode_->outputType(), name);
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    const RowTypePtr& inputType,
    const std::string& name) {
  auto index = inputType->getChildIdx(name);
  return field(inputType, index);
}

std::shared_ptr<const core::FieldAccessTypedExpr> PlanBuilder::field(
    const RowTypePtr& inputType,
    int index) {
  auto name = inputType->names()[index];
  auto type = inputType->childAt(index);
  return std::make_shared<core::FieldAccessTypedExpr>(type, name);
}

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(const std::vector<std::string>& names) {
  return fields(planNode_->outputType(), names);
}

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

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(
    const RowTypePtr& inputType,
    const std::vector<ChannelIndex>& indices) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields;
  for (auto& index : indices) {
    fields.push_back(field(inputType, index));
  }
  return fields;
}

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
PlanBuilder::fields(const std::vector<ChannelIndex>& indices) {
  return fields(planNode_->outputType(), indices);
}

std::shared_ptr<const core::ITypedExpr> PlanBuilder::inferTypes(
    const std::shared_ptr<const core::IExpr>& untypedExpr) {
  return core::Expressions::inferTypes(
      untypedExpr, planNode_->outputType(), pool_);
}
} // namespace facebook::velox::exec::test
