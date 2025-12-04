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

// clang-format off
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include <velox/type/TypeUtil.h>
#include <velox/type/Filter.h>
#include "velox/core/QueryCtx.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/expression/Expr.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/core/Expressions.h"
// clang-format on

#include "presto_cpp/main/SessionProperties.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnectorUtils.h"
#include "presto_cpp/main/operators/BroadcastWrite.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "presto_cpp/main/tvf/core/TableFunctionProcessorNode.h"

#include "presto_cpp/main/types/TypeParser.h"
#include "velox/exec/TraceUtil.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::presto {

namespace {

std::vector<std::string> getNames(const protocol::Assignments& assignments) {
  std::vector<std::string> names;
  names.reserve(assignments.assignments.size());

  for (const auto& [reference, _] : assignments.assignments) {
    names.emplace_back(reference.name);
  }

  return names;
}

RowTypePtr toRowType(
    const std::vector<protocol::VariableReferenceExpression>& variables,
    const TypeParser& typeParser,
    const std::unordered_set<std::string>& excludeNames = {}) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(variables.size());
  types.reserve(variables.size());

  for (const auto& variable : variables) {
    if (excludeNames.count(variable.name)) {
      continue;
    }
    names.emplace_back(variable.name);
    types.emplace_back(stringToType(variable.type, typeParser));
  }

  return ROW(std::move(names), std::move(types));
}

std::shared_ptr<connector::ColumnHandle> toColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) {
  const auto& connector = getPrestoToVeloxConnector(column->_type);
  return connector.toVeloxColumnHandle(column, typeParser);
}

std::shared_ptr<connector::ConnectorTableHandle> toConnectorTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) {
  const auto& connector =
      getPrestoToVeloxConnector(tableHandle.connectorHandle->_type);
  return connector.toVeloxTableHandle(tableHandle, exprConverter, typeParser);
}

std::vector<core::TypedExprPtr> getProjections(
    const VeloxExprConverter& exprConverter,
    const protocol::Assignments& assignments) {
  std::vector<core::TypedExprPtr> expressions;
  expressions.reserve(assignments.assignments.size());
  for (const auto& [_, expression] : assignments.assignments) {
    expressions.emplace_back(exprConverter.toVeloxExpr(expression));
  }

  return expressions;
}

template <TypeKind KIND>
void setCellFromVariantByKind(
    const VectorPtr& column,
    vector_size_t row,
    const variant& value) {
  using T = typename TypeTraits<KIND>::NativeType;

  auto flatVector = column->as<FlatVector<T>>();
  flatVector->set(row, value.value<T>());
}

template <>
void setCellFromVariantByKind<TypeKind::VARBINARY>(
    const VectorPtr& column,
    vector_size_t row,
    const variant& value) {
  auto values = column->as<FlatVector<StringView>>();
  values->set(row, StringView(value.value<TypeKind::VARBINARY>()));
}

template <>
void setCellFromVariantByKind<TypeKind::VARCHAR>(
    const VectorPtr& column,
    vector_size_t row,
    const variant& value) {
  auto values = column->as<FlatVector<StringView>>();
  values->set(row, StringView(value.value<TypeKind::VARCHAR>()));
}

void setCellFromVariant(
    const RowVectorPtr& data,
    vector_size_t row,
    vector_size_t column,
    const variant& value) {
  auto columnVector = data->childAt(column);
  if (value.isNull()) {
    columnVector->setNull(row, true);
    return;
  }
  if (columnVector->typeKind() == TypeKind::HUGEINT) {
    setCellFromVariantByKind<TypeKind::HUGEINT>(columnVector, row, value);
    return;
  }
  VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      setCellFromVariantByKind,
      columnVector->typeKind(),
      columnVector,
      row,
      value);
}

void setCellFromVariant(
    const VectorPtr& data,
    vector_size_t row,
    const variant& value) {
  if (value.isNull()) {
    data->setNull(row, true);
    return;
  }
  VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      setCellFromVariantByKind, data->typeKind(), data, row, value);
}

core::SortOrder toVeloxSortOrder(const protocol::SortOrder& sortOrder) {
  switch (sortOrder) {
    case protocol::SortOrder::ASC_NULLS_FIRST:
      return core::SortOrder(true, true);
    case protocol::SortOrder::ASC_NULLS_LAST:
      return core::SortOrder(true, false);
    case protocol::SortOrder::DESC_NULLS_FIRST:
      return core::SortOrder(false, true);
    case protocol::SortOrder::DESC_NULLS_LAST:
      return core::SortOrder(false, false);
    default:
      VELOX_UNSUPPORTED(
          "Unsupported sort order: {}.", fmt::underlying(sortOrder));
  }
}

bool isFixedPartition(
    const std::shared_ptr<const protocol::ExchangeNode>& node,
    const protocol::SystemPartitionFunction partitionFunction) {
  if (node->type != protocol::ExchangeNodeType::REPARTITION) {
    return false;
  }

  const auto connectorHandle =
      node->partitioningScheme.partitioning.handle.connectorHandle;
  const auto handle =
      std::dynamic_pointer_cast<protocol::SystemPartitioningHandle>(
          connectorHandle);
  if (!handle) {
    return false;
  }
  if (handle->partitioning != protocol::SystemPartitioning::FIXED) {
    return false;
  }
  if (handle->function != partitionFunction) {
    return false;
  }
  return true;
}

bool isHashPartition(
    const std::shared_ptr<const protocol::ExchangeNode>& node) {
  return isFixedPartition(node, protocol::SystemPartitionFunction::HASH);
}

bool isRoundRobinPartition(
    const std::shared_ptr<const protocol::ExchangeNode>& node) {
  return isFixedPartition(node, protocol::SystemPartitionFunction::ROUND_ROBIN);
}

std::vector<core::FieldAccessTypedExprPtr> toFieldExprs(
    const std::vector<std::shared_ptr<protocol::RowExpression>>& expressions,
    const VeloxExprConverter& exprConverter) {
  std::vector<core::FieldAccessTypedExprPtr> fields;
  fields.reserve(expressions.size());
  for (const auto& expr : expressions) {
    auto field = std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
        exprConverter.toVeloxExpr(expr));
    VELOX_CHECK_NOT_NULL(
        field,
        "Unexpected expression type: {}. Expected variable.",
        expr->_type);
    fields.emplace_back(std::move(field));
  }
  return fields;
}

std::vector<core::TypedExprPtr> toTypedExprs(
    const std::vector<std::shared_ptr<protocol::RowExpression>>& expressions,
    const VeloxExprConverter& exprConverter) {
  std::vector<core::TypedExprPtr> typedExprs;
  typedExprs.reserve(expressions.size());
  for (const auto& expr : expressions) {
    auto typedExpr = exprConverter.toVeloxExpr(expr);
    auto field =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(typedExpr);
    if (field == nullptr) {
      auto constant =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(typedExpr);
      VELOX_CHECK_NOT_NULL(
          constant,
          "Unexpected expression type: {}. Expected variable or constant.",
          expr->_type);
    }
    typedExprs.emplace_back(std::move(typedExpr));
  }
  return typedExprs;
}

std::vector<column_index_t> toChannels(
    const RowTypePtr& type,
    const std::vector<core::FieldAccessTypedExprPtr>& fields) {
  std::vector<column_index_t> channels;
  channels.reserve(fields.size());
  for (const auto& field : fields) {
    auto channel = type->getChildIdx(field->name());
    channels.emplace_back(channel);
  }
  return channels;
}

column_index_t exprToChannel(
    const core::ITypedExpr* expr,
    const TypePtr& type) {
  if (const auto field =
          dynamic_cast<const core::FieldAccessTypedExpr*>(expr)) {
    return type->as<TypeKind::ROW>().getChildIdx(field->name());
  }
  if (dynamic_cast<const core::ConstantTypedExpr*>(expr)) {
    return kConstantChannel;
  }
  VELOX_CHECK(false, "Expression must be field access or constant");
  return 0; // not reached.
}

core::WindowNode::WindowType toVeloxWindowType(
    protocol::WindowType windowType) {
  switch (windowType) {
    case protocol::WindowType::RANGE:
      return core::WindowNode::WindowType::kRange;
    case protocol::WindowType::ROWS:
      return core::WindowNode::WindowType::kRows;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported window type: {}", fmt::underlying(windowType));
  }
}

core::WindowNode::BoundType toVeloxBoundType(protocol::BoundType boundType) {
  switch (boundType) {
    case protocol::BoundType::CURRENT_ROW:
      return core::WindowNode::BoundType::kCurrentRow;
    case protocol::BoundType::PRECEDING:
      return core::WindowNode::BoundType::kPreceding;
    case protocol::BoundType::FOLLOWING:
      return core::WindowNode::BoundType::kFollowing;
    case protocol::BoundType::UNBOUNDED_PRECEDING:
      return core::WindowNode::BoundType::kUnboundedPreceding;
    case protocol::BoundType::UNBOUNDED_FOLLOWING:
      return core::WindowNode::BoundType::kUnboundedFollowing;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported window bound type: {}", fmt::underlying(boundType));
  }
}

core::LocalPartitionNode::Type toLocalExchangeType(
    protocol::ExchangeNodeType type) {
  switch (type) {
    case protocol::ExchangeNodeType::GATHER:
      return core::LocalPartitionNode::Type::kGather;
    case protocol::ExchangeNodeType::REPARTITION:
      return core::LocalPartitionNode::Type::kRepartition;
    default:
      VELOX_UNSUPPORTED("Unsupported exchange type: {}", toJsonString(type));
  }
}

VectorSerde::Kind toVeloxSerdeKind(protocol::ExchangeEncoding encoding) {
  switch (encoding) {
    case protocol::ExchangeEncoding::COLUMNAR:
      return VectorSerde::Kind::kPresto;
    case protocol::ExchangeEncoding::ROW_WISE:
      return VectorSerde::Kind::kCompactRow;
  }
  VELOX_UNSUPPORTED("Unsupported encoding: {}.", fmt::underlying(encoding));
}

std::shared_ptr<core::LocalPartitionNode> buildLocalSystemPartitionNode(
    const std::shared_ptr<const protocol::ExchangeNode>& node,
    core::LocalPartitionNode::Type type,
    bool scaleWriters,
    const RowTypePtr& outputType,
    std::vector<core::PlanNodePtr>&& sourceNodes,
    const VeloxExprConverter& exprConverter) {
  if (isHashPartition(node)) {
    const auto partitionKeys = toFieldExprs(
        node->partitioningScheme.partitioning.arguments, exprConverter);
    auto keyChannels = toChannels(outputType, partitionKeys);
    return std::make_shared<core::LocalPartitionNode>(
        node->id,
        type,
        scaleWriters,
        std::make_shared<HashPartitionFunctionSpec>(outputType, keyChannels),
        std::move(sourceNodes));
  }

  if (isRoundRobinPartition(node)) {
    return std::make_shared<core::LocalPartitionNode>(
        node->id,
        type,
        scaleWriters,
        std::make_shared<RoundRobinPartitionFunctionSpec>(),
        std::move(sourceNodes));
  }

  VELOX_UNSUPPORTED(
      "Unsupported flavor of local exchange with system partitioning handle: {}",
      toJsonString(node));
}
} // namespace

core::PlanNodePtr VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::ExchangeNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  VELOX_USER_CHECK(
      node->scope == protocol::ExchangeNodeScope::LOCAL,
      "Unsupported ExchangeNode scope");

  std::vector<core::PlanNodePtr> sourceNodes;
  sourceNodes.reserve(node->sources.size());
  for (const auto& source : node->sources) {
    sourceNodes.emplace_back(toVeloxQueryPlan(source, tableWriteInfo, taskId));
  }

  if (node->orderingScheme) {
    std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
    std::vector<core::SortOrder> sortingOrders;
    sortingKeys.reserve(node->orderingScheme->orderBy.size());
    sortingOrders.reserve(node->orderingScheme->orderBy.size());
    for (const auto& [variable, sortOrder] : node->orderingScheme->orderBy) {
      sortingKeys.emplace_back(exprConverter_.toVeloxExpr(variable));
      sortingOrders.emplace_back(toVeloxSortOrder(sortOrder));
    }
    return std::make_shared<core::LocalMergeNode>(
        node->id, sortingKeys, sortingOrders, std::move(sourceNodes));
  }

  const auto type = toLocalExchangeType(node->type);
  const auto outputType =
      toRowType(node->partitioningScheme.outputLayout, typeParser_);

  // Different source nodes may have different output layouts.
  // Add ProjectNode on top of each source node to re-arrange the output columns
  // to match the output layout of the LocalExchangeNode.
  for (auto i = 0; i < sourceNodes.size(); ++i) {
    auto names = outputType->names();
    std::vector<core::TypedExprPtr> projections;
    projections.reserve(outputType->size());

    const auto desiredSourceOutput = toRowType(node->inputs[i], typeParser_);

    for (auto j = 0; j < outputType->size(); ++j) {
      projections.emplace_back(
          std::make_shared<core::FieldAccessTypedExpr>(
              outputType->childAt(j), desiredSourceOutput->nameOf(j)));
    }

    sourceNodes[i] = std::make_shared<core::ProjectNode>(
        fmt::format("{}.{}", node->id, i),
        std::move(names),
        std::move(projections),
        sourceNodes[i]);
  }

  if (type == core::LocalPartitionNode::Type::kGather) {
    return core::LocalPartitionNode::gather(node->id, std::move(sourceNodes));
  }

  const auto& partitioningScheme = node->partitioningScheme;
  const auto& connectorHandle =
      partitioningScheme.partitioning.handle.connectorHandle;
  const bool scaleWriters = partitioningScheme.scaleWriters;
  if (std::dynamic_pointer_cast<protocol::SystemPartitioningHandle>(
          connectorHandle) != nullptr) {
    return buildLocalSystemPartitionNode(
        node,
        type,
        scaleWriters,
        outputType,
        std::move(sourceNodes),
        exprConverter_);
  }

  const auto partitionKeys = toFieldExprs(
      node->partitioningScheme.partitioning.arguments, exprConverter_);
  const auto keyChannels = toChannels(outputType, partitionKeys);

  auto& connector = getPrestoToVeloxConnector(connectorHandle->_type);
  bool effectivelyGather{false};
  auto spec = connector.createVeloxPartitionFunctionSpec(
      connectorHandle.get(),
      {},
      keyChannels,
      std::vector<VectorPtr>{},
      effectivelyGather);
  if (effectivelyGather) {
    return core::LocalPartitionNode::gather(node->id, std::move(sourceNodes));
  }
  return std::make_shared<core::LocalPartitionNode>(
      node->id,
      type,
      scaleWriters,
      std::shared_ptr(std::move(spec)),
      std::move(sourceNodes));
}

namespace {
bool equal(
    const std::shared_ptr<protocol::RowExpression>& actual,
    const protocol::VariableReferenceExpression& expected) {
  if (const auto variableReference =
          std::dynamic_pointer_cast<protocol::VariableReferenceExpression>(
              actual)) {
    return (
        variableReference->name == expected.name &&
        variableReference->type == expected.type);
  }
  return false;
}

// Returns the cast function call if row 'expression' is the specified special
// 'functionName'.
std::shared_ptr<protocol::CallExpression> isFunctionCall(
    const std::shared_ptr<protocol::RowExpression>& expression,
    const std::string_view& functionName) {
  if (auto call =
          std::dynamic_pointer_cast<protocol::CallExpression>(expression)) {
    if (const auto builtin =
            std::dynamic_pointer_cast<protocol::BuiltInFunctionHandle>(
                call->functionHandle)) {
      if (builtin->signature.kind == protocol::FunctionKind::SCALAR &&
          builtin->signature.name == functionName) {
        return call;
      }
    }
  }
  return nullptr;
}

// Returns the cast special form if row 'expression' is the specified special
// 'form'.
std::shared_ptr<protocol::SpecialFormExpression> isSpecialForm(
    const std::shared_ptr<protocol::RowExpression>& expression,
    const protocol::Form& form) {
  if (const auto specialForm =
          std::dynamic_pointer_cast<protocol::SpecialFormExpression>(
              expression)) {
    if (specialForm->form == form) {
      return specialForm;
    }
  }
  return nullptr;
}

// Check if input RowExpression is a 'NOT x' expression and returns it as
// CallExpression. Returns nullptr if input expression is something else.
std::shared_ptr<protocol::CallExpression> isNot(
    const std::shared_ptr<protocol::RowExpression>& expression) {
  static const std::string prestoDefaultNamespacePrefix =
      SystemConfig::instance()->prestoDefaultNamespacePrefix();
  static const std::string kNot =
      util::addDefaultNamespacePrefix(prestoDefaultNamespacePrefix, "not");
  return isFunctionCall(expression, kNot);
}

// Check if input RowExpression is an 'a > b' expression and returns it as
// CallExpression. Returns nullptr if input expression is something else.
std::shared_ptr<protocol::CallExpression> isGreaterThan(
    const std::shared_ptr<protocol::RowExpression>& expression) {
  static constexpr std::string_view kGreaterThan =
      "presto.default.$operator$greater_than";
  return isFunctionCall(expression, kGreaterThan);
}

// Check if input RowExpression is a 'between' expression and returns it as
// CallExpression. Returns nullptr if input expression is something else.
std::shared_ptr<protocol::CallExpression> isBetween(
    const std::shared_ptr<protocol::RowExpression>& expression) {
  static constexpr std::string_view kBetween =
      "presto.default.$operator$between";
  return isFunctionCall(expression, kBetween);
}

// Check if input RowExpression is a 'contains' expression and returns it as
// CallExpression. Returns nullptr if input expression is something else.
std::shared_ptr<protocol::CallExpression> isContains(
    const std::shared_ptr<protocol::RowExpression>& expression) {
  static constexpr std::string_view kContains = "presto.default.contains";
  return isFunctionCall(expression, kContains);
}

// Check if input RowExpression is an 'equal' expression and returns it as
// CallExpression. Returns nullptr if input expression is something else.
std::shared_ptr<protocol::CallExpression> isEqual(
    const std::shared_ptr<protocol::RowExpression>& expression) {
  static constexpr std::string_view kEqual = "presto.default.$operator$equal";
  return isFunctionCall(expression, kEqual);
}

// Check if input RowExpression is an 'and' expression and returns it as
// SpecialFormExpression. Returns nullptr if input expression is something else.
std::shared_ptr<protocol::SpecialFormExpression> isAnd(
    const std::shared_ptr<protocol::RowExpression>& expression) {
  return isSpecialForm(expression, protocol::Form::AND);
}

// Check if input RowExpression is an 'or' expression and returns it as
// SpecialFormExpression. Returns nullptr if input expression is something else.
std::shared_ptr<protocol::SpecialFormExpression> isOr(
    const std::shared_ptr<protocol::RowExpression>& expression) {
  return isSpecialForm(expression, protocol::Form::OR);
}

// Checks if input PlanNode represents a local exchange with single source and
// returns it as ExchangeNode. Returns nullptr if input node is something else.
std::shared_ptr<const protocol::ExchangeNode> isLocalSingleSourceExchange(
    const std::shared_ptr<const protocol::PlanNode>& node) {
  if (auto exchange =
          std::dynamic_pointer_cast<const protocol::ExchangeNode>(node)) {
    if (exchange->scope == protocol::ExchangeNodeScope::LOCAL &&
        exchange->sources.size() == 1) {
      return exchange;
    }
  }

  return nullptr;
}

// Checks if input PlanNode represents an identity projection and returns it as
// ProjectNode. Returns nullptr if input node is something else.
std::shared_ptr<const protocol::ProjectNode> isIdentityProjection(
    const std::shared_ptr<const protocol::PlanNode>& node) {
  if (auto project =
          std::dynamic_pointer_cast<const protocol::ProjectNode>(node)) {
    for (const auto& [reference, expression] :
         project->assignments.assignments) {
      if (!equal(expression, reference)) {
        return nullptr;
      }
    }
    return project;
  }

  return nullptr;
}
} // namespace

core::PlanNodePtr VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::FilterNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  // In Presto, semi and anti joins are implemented using two operators:
  // SemiJoin followed by Filter. SemiJoin operator returns all probe rows plus
  // an extra boolean column which indicates whether there is a match for a
  // given row. Then a filter on the boolean column is used to select a subset
  // of probe rows that match (semi join) or don't match (anti join).
  //
  // In Velox, semi and anti joins are implemented using a single operator:
  // HashJoin which returns a subset of probe rows that match (semi) or don't
  // match (anti) the build side. Hence, we convert FilterNode over SemiJoinNode
  // into ProjectNode over HashJoinNode. Project node adds an extra boolean
  // column with constant value of 'true' for semi join and 'false' for anti
  // join.
  if (const auto semiJoin =
          std::dynamic_pointer_cast<const protocol::SemiJoinNode>(
              node->source)) {
    std::optional<core::JoinType> joinType = std::nullopt;
    if (equal(node->predicate, semiJoin->semiJoinOutput)) {
      joinType = core::JoinType::kLeftSemiFilter;
    } else if (const auto notCall = isNot(node->predicate)) {
      if (equal(notCall->arguments[0], semiJoin->semiJoinOutput)) {
        joinType = core::JoinType::kAnti;
      }
    }

    // No clear join type - fallback to the standard 'to velox expr'.
    if (!joinType.has_value()) {
      return std::make_shared<core::FilterNode>(
          node->id,
          exprConverter_.toVeloxExpr(node->predicate),
          toVeloxQueryPlan(semiJoin, tableWriteInfo, taskId));
    }

    std::vector<core::FieldAccessTypedExprPtr> leftKeys = {
        exprConverter_.toVeloxExpr(semiJoin->sourceJoinVariable)};
    std::vector<core::FieldAccessTypedExprPtr> rightKeys = {
        exprConverter_.toVeloxExpr(semiJoin->filteringSourceJoinVariable)};

    auto left = toVeloxQueryPlan(semiJoin->source, tableWriteInfo, taskId);
    auto right =
        toVeloxQueryPlan(semiJoin->filteringSource, tableWriteInfo, taskId);

    const auto& leftNames = left->outputType()->names();
    const auto& leftTypes = left->outputType()->children();

    auto names = leftNames;
    names.push_back(semiJoin->semiJoinOutput.name);

    std::vector<core::TypedExprPtr> projections;
    projections.reserve(leftNames.size() + 1);
    for (auto i = 0; i < leftNames.size(); i++) {
      projections.emplace_back(
          std::make_shared<core::FieldAccessTypedExpr>(
              leftTypes[i], leftNames[i]));
    }
    const bool constantValue =
        joinType.value() == core::JoinType::kLeftSemiFilter;
    projections.emplace_back(
        std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), constantValue));

    return std::make_shared<core::ProjectNode>(
        node->id,
        std::move(names),
        std::move(projections),
        std::make_shared<core::HashJoinNode>(
            semiJoin->id,
            joinType.value(),
            joinType == core::JoinType::kAnti ? true : false,
            leftKeys,
            rightKeys,
            nullptr, // filter
            left,
            right,
            left->outputType()));
  }

  return std::make_shared<core::FilterNode>(
      node->id,
      exprConverter_.toVeloxExpr(node->predicate),
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::ProjectNode>
VeloxQueryPlanConverterBase::tryConvertOffsetLimit(
    const std::shared_ptr<const protocol::ProjectNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  // Presto plans OFFSET n LIMIT m queries as
  // Project(drop row_number column)
  //  -> LocalExchange(1-to-N)
  //    -> Limit(m)
  //      -> LocalExchange(N-to-1)
  //        -> Filter(rowNumber > n)
  //          -> LocalExchange(1-to-N)
  //            -> RowNumberNode
  // Velox supports OFFSET-LIMIT via a single LimitNode(n, m).
  //
  // Detect the pattern above and convert it to:
  // Project(as-is)
  //  -> Limit(n-m)

  // TODO Relax the check to only ensure that no expression is using row_number
  // column.
  auto identityProjections = isIdentityProjection(node);
  if (!identityProjections) {
    return nullptr;
  }

  auto exchangeBeforeProject = isLocalSingleSourceExchange(node->source);
  if (!exchangeBeforeProject || !isRoundRobinPartition(exchangeBeforeProject)) {
    return nullptr;
  }

  auto limit = std::dynamic_pointer_cast<protocol::LimitNode>(
      exchangeBeforeProject->sources[0]);
  if (!limit) {
    return nullptr;
  }

  auto exchangeBeforeLimit = isLocalSingleSourceExchange(limit->source);
  if (!exchangeBeforeLimit) {
    return nullptr;
  }

  auto filter = std::dynamic_pointer_cast<const protocol::FilterNode>(
      exchangeBeforeLimit->sources[0]);
  if (!filter) {
    return nullptr;
  }

  auto exchangeBeforeFilter = isLocalSingleSourceExchange(filter->source);
  if (!exchangeBeforeFilter) {
    return nullptr;
  }

  auto rowNumber = std::dynamic_pointer_cast<const protocol::RowNumberNode>(
      exchangeBeforeFilter->sources[0]);
  if (!rowNumber) {
    return nullptr;
  }

  auto rowNumberVariable = rowNumber->rowNumberVariable;

  auto gt = isGreaterThan(filter->predicate);
  if (gt && equal(gt->arguments[0], rowNumberVariable)) {
    auto offsetExpr = exprConverter_.toVeloxExpr(gt->arguments[1]);
    if (auto offsetConstExpr =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                offsetExpr)) {
      if (!offsetConstExpr->type()->isBigint()) {
        return nullptr;
      }

      auto offset = offsetConstExpr->value().value<int64_t>();

      // Check that Project node drops row_number column.
      for (const auto& [_, expression] : node->assignments.assignments) {
        if (equal(expression, rowNumberVariable)) {
          return nullptr;
        }
      }

      return std::make_shared<core::ProjectNode>(
          node->id,
          getNames(node->assignments),
          getProjections(exprConverter_, node->assignments),
          std::make_shared<core::LimitNode>(
              limit->id,
              offset,
              limit->count,
              limit->step == protocol::LimitNodeStep::PARTIAL,
              toVeloxQueryPlan(rowNumber->source, tableWriteInfo, taskId)));
    }
  }

  return nullptr;
}

std::shared_ptr<const core::ProjectNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::ProjectNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  if (auto limit = tryConvertOffsetLimit(node, tableWriteInfo, taskId)) {
    return limit;
  }

  return std::make_shared<core::ProjectNode>(
      node->id,
      getNames(node->assignments),
      getProjections(exprConverter_, node->assignments),
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

VectorPtr VeloxQueryPlanConverterBase::evaluateConstantExpression(
    const core::TypedExprPtr& expression) {
  const auto emptyRowVector = BaseVector::create<RowVector>(ROW({}), 1, pool_);
  core::ExecCtx execCtx{pool_, queryCtx_};
  ExprSet exprSet{{expression}, &execCtx};
  EvalCtx context(&execCtx, &exprSet, emptyRowVector.get());

  const SelectivityVector rows{1};
  std::vector<VectorPtr> result(1);
  exprSet.eval(rows, context, result);
  return result[0];
}

std::optional<core::ColumnStatsSpec>
VeloxQueryPlanConverterBase::toColumnStatsSpec(
    const std::shared_ptr<protocol::StatisticAggregations>&
        statisticsAggregation,
    core::AggregationNode::Step step,
    const protocol::PlanNodeId& id,
    const core::PlanNodePtr& sourceVeloxPlan,
    const std::shared_ptr<protocol::TableWriteInfo>& /*tableWriteInfo*/,
    const protocol::TaskId& /*taskId*/) {
  if (statisticsAggregation == nullptr) {
    return std::nullopt;
  }
  const auto outputVariables = statisticsAggregation->outputVariables;
  const auto aggregationMap = statisticsAggregation->aggregations;
  auto groupingVariables = statisticsAggregation->groupingVariables;
  VELOX_CHECK_EQ(
      aggregationMap.size(),
      outputVariables.size(),
      "TableWriterNode's aggregations and outputVariables should be the same size");
  VELOX_CHECK(
      !outputVariables.empty(),
      "TableWriterNode's outputVariables shouldn't be empty");

  std::vector<std::string> aggregateNames;
  std::vector<core::AggregationNode::Aggregate> aggregates;
  toAggregations(outputVariables, aggregationMap, aggregates, aggregateNames);
  const auto aggregationNode = std::make_shared<core::AggregationNode>(
      id,
      step,
      toVeloxExprs(statisticsAggregation->groupingVariables),
      std::vector<core::FieldAccessTypedExprPtr>{},
      aggregateNames,
      aggregates,
      /*ignoreNullKeys=*/false,
      sourceVeloxPlan);

  // Sanity checks on aggregation node.
  VELOX_CHECK(!aggregationNode->ignoreNullKeys());
  VELOX_CHECK(!aggregationNode->groupId().has_value());
  VELOX_CHECK(!aggregationNode->isPreGrouped());
  VELOX_CHECK(aggregationNode->globalGroupingSets().empty());
  VELOX_CHECK(!aggregationNode->aggregateNames().empty());
  VELOX_CHECK_EQ(
      aggregationNode->aggregateNames().size(),
      aggregationNode->aggregates().size());
  return core::ColumnStatsSpec{
      aggregationNode->groupingKeys(),
      aggregationNode->step(),
      aggregationNode->aggregateNames(),
      aggregationNode->aggregates()};
}

std::vector<protocol::VariableReferenceExpression>
VeloxQueryPlanConverterBase::generateOutputVariables(
    const std::vector<protocol::VariableReferenceExpression>&
        nonStatisticsOutputVariables,
    const std::shared_ptr<protocol::StatisticAggregations>&
        statisticsAggregation) {
  std::vector<protocol::VariableReferenceExpression> outputVariables;
  outputVariables.insert(
      outputVariables.end(),
      nonStatisticsOutputVariables.begin(),
      nonStatisticsOutputVariables.end());
  if (statisticsAggregation == nullptr) {
    return outputVariables;
  }
  const auto statisticsOutputVariables = statisticsAggregation->outputVariables;
  auto statisticsGroupingVariables = statisticsAggregation->groupingVariables;
  outputVariables.insert(
      outputVariables.end(),
      statisticsGroupingVariables.begin(),
      statisticsGroupingVariables.end());
  for (auto const& variable : statisticsOutputVariables) {
    outputVariables.push_back(variable);
  }
  return outputVariables;
}

void VeloxQueryPlanConverterBase::toAggregations(
    const std::vector<protocol::VariableReferenceExpression>& outputVariables,
    const std::map<
        protocol::VariableReferenceExpression,
        protocol::Aggregation>& aggregationMap,
    std::vector<core::AggregationNode::Aggregate>& aggregates,
    std::vector<std::string>& aggregateNames) {
  aggregateNames.reserve(aggregates.size());
  aggregates.reserve(aggregates.size());
  for (const auto& entry : outputVariables) {
    aggregateNames.emplace_back(entry.name);

    const auto& prestoAggregation = aggregationMap.at(entry);

    core::AggregationNode::Aggregate aggregate;
    aggregate.call = std::dynamic_pointer_cast<const core::CallTypedExpr>(
        exprConverter_.toVeloxExpr(prestoAggregation.call));

    if (const auto builtin =
            std::dynamic_pointer_cast<protocol::BuiltInFunctionHandle>(
                prestoAggregation.functionHandle)) {
      const auto& signature = builtin->signature;
      aggregate.rawInputTypes.reserve(signature.argumentTypes.size());
      for (const auto& argumentType : signature.argumentTypes) {
        aggregate.rawInputTypes.push_back(
            stringToType(argumentType, typeParser_));
      }
    } else if (
        auto sqlFunction =
            std::dynamic_pointer_cast<protocol::SqlFunctionHandle>(
                prestoAggregation.functionHandle)) {
      parseSqlFunctionHandle(sqlFunction, aggregate.rawInputTypes, typeParser_);
    } else {
      VELOX_USER_FAIL(
          "Unsupported aggregate function handle: {}",
          toJsonString(prestoAggregation.functionHandle));
    }

    aggregate.distinct = prestoAggregation.distinct;

    if (prestoAggregation.mask != nullptr) {
      aggregate.mask = exprConverter_.toVeloxExpr(prestoAggregation.mask);
    }

    if (prestoAggregation.orderBy != nullptr) {
      for (const auto& [variable, sortOrder] :
           prestoAggregation.orderBy->orderBy) {
        aggregate.sortingKeys.emplace_back(
            exprConverter_.toVeloxExpr(variable));
        aggregate.sortingOrders.emplace_back(toVeloxSortOrder(sortOrder));
      }
    }

    aggregates.emplace_back(aggregate);
  }
}

std::shared_ptr<const core::ValuesNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::ValuesNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& /*tableWriteInfo*/,
    const protocol::TaskId& /*taskId*/) {
  auto rowType = toRowType(node->outputVariables, typeParser_);
  vector_size_t numRows = node->rows.size();
  const auto numColumns = rowType->size();
  std::vector<VectorPtr> vectors;
  vectors.reserve(numColumns);

  for (int i = 0; i < numColumns; ++i) {
    auto base = BaseVector::create(rowType->childAt(i), numRows, pool_);
    vectors.emplace_back(base);
  }

  const auto rowVector = std::make_shared<RowVector>(
      pool_, rowType, BufferPtr(), numRows, std::move(vectors), 0);

  for (int row = 0; row < numRows; ++row) {
    for (int column = 0; column < numColumns; ++column) {
      auto expr = exprConverter_.toVeloxExpr(node->rows[row][column]);

      if (const auto constantExpr =
              std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
        if (!constantExpr->hasValueVector()) {
          setCellFromVariant(rowVector, row, column, constantExpr->value());
        } else {
          const auto& columnVector = rowVector->childAt(column);
          columnVector->copy(constantExpr->valueVector().get(), row, 0, 1);
        }
      } else {
        // Evaluate the expression.
        auto value = evaluateConstantExpression(expr);

        const auto& columnVector = rowVector->childAt(column);
        columnVector->copy(value.get(), row, 0, 1);
      }
    }
  }

  return std::make_shared<core::ValuesNode>(node->id, std::vector{rowVector});
}

std::shared_ptr<const core::TableScanNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::TableScanNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& /*tableWriteInfo*/,
    const protocol::TaskId& /*taskId*/) {
  auto rowType = toRowType(node->outputVariables, typeParser_);
  velox::connector::ColumnHandleMap assignments;
  for (const auto& [variable, columnHandle] : node->assignments) {
    assignments.emplace(
        variable.name, toColumnHandle(columnHandle.get(), typeParser_));
  }
  auto connectorTableHandle =
      toConnectorTableHandle(node->table, exprConverter_, typeParser_);
  return std::make_shared<core::TableScanNode>(
      node->id, rowType, connectorTableHandle, assignments);
}

std::vector<core::FieldAccessTypedExprPtr>
VeloxQueryPlanConverterBase::toVeloxExprs(
    const std::vector<protocol::VariableReferenceExpression>& variables) {
  std::vector<core::FieldAccessTypedExprPtr> fields;
  fields.reserve(variables.size());
  for (const auto& variable : variables) {
    fields.emplace_back(exprConverter_.toVeloxExpr(variable));
  }
  return fields;
}

std::shared_ptr<const core::AggregationNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::AggregationNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<std::string> aggregateNames;
  std::vector<core::AggregationNode::Aggregate> aggregates;

  std::vector<protocol::VariableReferenceExpression> outputVariables;
  for (const auto& [variable, _] : node->aggregations) {
    outputVariables.push_back(variable);
  }
  toAggregations(
      outputVariables, node->aggregations, aggregates, aggregateNames);

  core::AggregationNode::Step step;
  switch (node->step) {
    case protocol::AggregationNodeStep::PARTIAL:
      step = core::AggregationNode::Step::kPartial;
      break;
    case protocol::AggregationNodeStep::FINAL:
      step = core::AggregationNode::Step::kFinal;
      break;
    case protocol::AggregationNodeStep::INTERMEDIATE:
      step = core::AggregationNode::Step::kIntermediate;
      break;
    case protocol::AggregationNodeStep::SINGLE:
      step = core::AggregationNode::Step::kSingle;
      break;
    default:
      VELOX_UNSUPPORTED("Unsupported aggregation step");
  }

  const bool streamable = !node->preGroupedVariables.empty() &&
      node->groupingSets.groupingSetCount == 1 &&
      node->groupingSets.globalGroupingSets.empty();

  // groupIdField and globalGroupingSets are required for producing default
  // output rows for global grouping sets when there are no input rows.
  // Global grouping sets can be present without groupIdField in Final
  // aggregations. But the default output is generated only for Single and
  // Partial aggregations. Set both fields only when required for the
  // aggregation.
  std::optional<core::FieldAccessTypedExprPtr> groupIdField;
  std::vector<vector_size_t> globalGroupingSets;
  if (node->groupIdVariable && !node->groupingSets.globalGroupingSets.empty()) {
    groupIdField = toVeloxExprs({*node->groupIdVariable})[0];
    globalGroupingSets = node->groupingSets.globalGroupingSets;
  }

  return std::make_shared<core::AggregationNode>(
      node->id,
      step,
      toVeloxExprs(node->groupingSets.groupingKeys),
      streamable ? toVeloxExprs(node->preGroupedVariables)
                 : std::vector<core::FieldAccessTypedExprPtr>{},
      aggregateNames,
      aggregates,
      globalGroupingSets,
      groupIdField,
      /*ignoreNullKeys=*/false,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::GroupIdNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::GroupIdNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  // protocol::GroupIdNode.groupingSets uses output names for the grouping
  // keys. protocol::GroupIdNode.groupingColumns maps output name of a
  // grouping key to its input name.

  // Example:
  //  - GroupId[[orderstatus], [orderpriority]] =>
  //  [orderstatus$gid:varchar(1), orderpriority$gid:varchar(15),
  //  orderkey:bigint, groupid:bigint]
  //      orderstatus$gid := orderstatus (10:20)
  //      orderpriority$gid := orderpriority (10:35)
  //
  //  Here, groupingSets = [[orderstatus$gid], [orderpriority$gid]]
  //    and groupingColumns = [orderstatus$gid => orderstatus,
  //    orderpriority$gid
  //    => orderpriority]

  // core::GroupIdNode.groupingSets is defined using output field names.
  // core::GroupIdNode.groupingKeys maps output name of a
  // grouping key to the corresponding input field.

  std::vector<std::vector<std::string>> groupingSets;
  groupingSets.reserve(node->groupingSets.size());
  for (const auto& groupingSet : node->groupingSets) {
    std::vector<std::string> groupingKeys;
    groupingKeys.reserve(groupingSet.size());
    // Use the output key name in the GroupingSet as there could be
    // multiple output keys mapping to the same input column.
    for (const auto& groupingKey : groupingSet) {
      groupingKeys.emplace_back(groupingKey.name);
    }
    groupingSets.emplace_back(std::move(groupingKeys));
  }

  std::vector<core::GroupIdNode::GroupingKeyInfo> groupingKeys;
  groupingKeys.reserve(node->groupingColumns.size());
  for (const auto& [output, input] : node->groupingColumns) {
    groupingKeys.emplace_back(
        core::GroupIdNode::GroupingKeyInfo{
            output.name, exprConverter_.toVeloxExpr(input)});
  }

  return std::make_shared<core::GroupIdNode>(
      node->id,
      std::move(groupingSets),
      std::move(groupingKeys),
      toVeloxExprs(node->aggregationArguments),
      node->groupIdVariable.name,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::PlanNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::DistinctLimitNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  // Convert to Limit(Aggregation)
  return std::make_shared<core::LimitNode>(
      // Make sure to use unique plan node IDs.
      fmt::format("{}.limit", node->id),
      0,
      node->limit,
      node->partial,
      std::make_shared<core::AggregationNode>(
          // Use the ID of the DistinctLimit plan node here to propagate the
          // stats.
          node->id,
          core::AggregationNode::Step::kSingle,
          toVeloxExprs(node->distinctVariables),
          std::vector<core::FieldAccessTypedExprPtr>{},
          /*aggregateNames=*/std::vector<std::string>{},
          /*aggregates=*/std::vector<core::AggregationNode::Aggregate>{},
          /*ignoreNullKeys=*/false,
          toVeloxQueryPlan(node->source, tableWriteInfo, taskId)));
}

namespace {
core::JoinType toJoinType(protocol::JoinType type) {
  switch (type) {
    case protocol::JoinType::INNER:
      return core::JoinType::kInner;
    case protocol::JoinType::LEFT:
      return core::JoinType::kLeft;
    case protocol::JoinType::RIGHT:
      return core::JoinType::kRight;
    case protocol::JoinType::FULL:
      return core::JoinType::kFull;
    case protocol::JoinType::SOURCE_OUTER:
      return core::JoinType::kLeft;
  }

  VELOX_UNSUPPORTED("Unknown join type");
}

core::JoinType toJoinType(protocol::SpatialJoinType type) {
  switch (type) {
    case protocol::SpatialJoinType::INNER:
      return core::JoinType::kInner;
    case protocol::SpatialJoinType::LEFT:
      return core::JoinType::kLeft;
  }

  VELOX_UNSUPPORTED("Unknown spatial join type");
}
} // namespace

core::PlanNodePtr VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::JoinNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto joinType = toJoinType(node->type);
  if (node->criteria.empty()) {
    const bool isNestedJoinType = core::isInnerJoin(joinType) ||
        core::isLeftJoin(joinType) || core::isRightJoin(joinType) ||
        core::isFullJoin(joinType);
    if (isNestedJoinType) {
      return std::make_shared<core::NestedLoopJoinNode>(
          node->id,
          joinType,
          node->filter ? exprConverter_.toVeloxExpr(*node->filter) : nullptr,
          toVeloxQueryPlan(node->left, tableWriteInfo, taskId),
          toVeloxQueryPlan(node->right, tableWriteInfo, taskId),
          toRowType(node->outputVariables, typeParser_));
    }
    VELOX_UNSUPPORTED(
        "JoinNode has empty criteria that cannot be "
        "satisfied by NestedJoinNode or HashJoinNode");
  }

  std::vector<core::FieldAccessTypedExprPtr> leftKeys;
  std::vector<core::FieldAccessTypedExprPtr> rightKeys;

  leftKeys.reserve(node->criteria.size());
  rightKeys.reserve(node->criteria.size());
  for (const auto& [left, right] : node->criteria) {
    leftKeys.emplace_back(exprConverter_.toVeloxExpr(left));
    rightKeys.emplace_back(exprConverter_.toVeloxExpr(right));
  }

  return std::make_shared<core::HashJoinNode>(
      node->id,
      joinType,
      false,
      leftKeys,
      rightKeys,
      node->filter ? exprConverter_.toVeloxExpr(*node->filter) : nullptr,
      toVeloxQueryPlan(node->left, tableWriteInfo, taskId),
      toVeloxQueryPlan(node->right, tableWriteInfo, taskId),
      toRowType(node->outputVariables, typeParser_));
}

core::PlanNodePtr VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::SemiJoinNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> leftKeys;
  std::vector<core::FieldAccessTypedExprPtr> rightKeys;

  leftKeys.push_back(exprConverter_.toVeloxExpr(node->sourceJoinVariable));
  rightKeys.push_back(
      exprConverter_.toVeloxExpr(node->filteringSourceJoinVariable));

  auto left = toVeloxQueryPlan(node->source, tableWriteInfo, taskId);
  auto right = toVeloxQueryPlan(node->filteringSource, tableWriteInfo, taskId);

  std::vector<std::string> outputNames = left->outputType()->names();
  outputNames.push_back(node->semiJoinOutput.name);
  std::vector<TypePtr> outputTypes = left->outputType()->children();
  outputTypes.push_back(BOOLEAN());

  return std::make_shared<core::HashJoinNode>(
      node->id,
      core::JoinType::kLeftSemiProject,
      /*nullAware=*/true,
      leftKeys,
      rightKeys,
      /*filter=*/nullptr,
      left,
      right,
      ROW(std::move(outputNames), std::move(outputTypes)));
}

core::PlanNodePtr VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::SpatialJoinNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto joinType = toJoinType(node->type);
  std::optional<core::FieldAccessTypedExprPtr> radiusVariable = std::nullopt;
  if (node->radiusVariable) {
    radiusVariable = exprConverter_.toVeloxExpr(*node->radiusVariable);
  }

  if (SessionProperties::instance()->useVeloxGeospatialJoin()) {
    return std::make_shared<core::SpatialJoinNode>(
        node->id,
        joinType,
        exprConverter_.toVeloxExpr(node->filter),
        exprConverter_.toVeloxExpr(node->probeGeometryVariable),
        exprConverter_.toVeloxExpr(node->buildGeometryVariable),
        radiusVariable,
        toVeloxQueryPlan(node->left, tableWriteInfo, taskId),
        toVeloxQueryPlan(node->right, tableWriteInfo, taskId),
        toRowType(node->outputVariables, typeParser_));
  }
  return std::make_shared<core::NestedLoopJoinNode>(
      node->id,
      joinType,
      exprConverter_.toVeloxExpr(node->filter),
      toVeloxQueryPlan(node->left, tableWriteInfo, taskId),
      toVeloxQueryPlan(node->right, tableWriteInfo, taskId),
      toRowType(node->outputVariables, typeParser_));
}

std::shared_ptr<const core::IndexLookupJoinNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::IndexJoinNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  VELOX_USER_CHECK(
      node->type == protocol::JoinType::INNER ||
          node->type == protocol::JoinType::SOURCE_OUTER,
      "Unsupported index lookup join type.");

  std::vector<core::FieldAccessTypedExprPtr> leftKeys;
  std::vector<core::FieldAccessTypedExprPtr> rightKeys;

  leftKeys.reserve(node->criteria.size());
  rightKeys.reserve(node->criteria.size());
  for (const auto& clause : node->criteria) {
    leftKeys.emplace_back(exprConverter_.toVeloxExpr(clause.left));
    rightKeys.emplace_back(exprConverter_.toVeloxExpr(clause.right));
  }

  const auto left = toVeloxQueryPlan(node->probeSource, tableWriteInfo, taskId);
  const auto right =
      toVeloxQueryPlan(node->indexSource, tableWriteInfo, taskId);

  std::vector<core::IndexLookupConditionPtr> joinConditionPtrs{};
  std::vector<core::TypedExprPtr> unsupportedConditions{};
  if (node->filter) {
    parseIndexLookupCondition(
        *node->filter,
        node->lookupVariables,
        exprConverter_,
        /*acceptConstant=*/false,
        joinConditionPtrs,
        unsupportedConditions);
  }

  // Combine unsupported conditions into a single filter using AND
  core::TypedExprPtr joinFilter;
  if (!unsupportedConditions.empty()) {
    if (unsupportedConditions.size() == 1) {
      joinFilter = unsupportedConditions[0];
    } else {
      joinFilter = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(), unsupportedConditions, "and");
    }
  }

  return std::make_shared<core::IndexLookupJoinNode>(
      node->id,
      toJoinType(node->type),
      leftKeys,
      rightKeys,
      joinConditionPtrs,
      joinFilter,
      false,
      left,
      std::dynamic_pointer_cast<const core::TableScanNode>(right),
      type::concatRowTypes({left->outputType(), right->outputType()}));
}

std::shared_ptr<const core::TableScanNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::IndexSourceNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto rowType = toRowType(node->outputVariables, typeParser_);
  connector::ColumnHandleMap assignments;
  for (const auto& [variable, columnHandle] : node->assignments) {
    assignments.emplace(
        variable.name, toColumnHandle(columnHandle.get(), typeParser_));
  }
  auto connectorTableHandle =
      toConnectorTableHandle(node->tableHandle, exprConverter_, typeParser_);
  return std::make_shared<core::TableScanNode>(
      node->id, rowType, connectorTableHandle, assignments);
}

core::PlanNodePtr VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::MarkDistinctNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  return std::make_shared<core::MarkDistinctNode>(
      node->id,
      node->markerVariable.name,
      toVeloxExprs(node->distinctVariables),
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

core::PlanNodePtr VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::MergeJoinNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto joinType = toJoinType(node->type);

  std::vector<core::FieldAccessTypedExprPtr> leftKeys;
  std::vector<core::FieldAccessTypedExprPtr> rightKeys;

  leftKeys.reserve(node->criteria.size());
  rightKeys.reserve(node->criteria.size());
  for (const auto& [left, right] : node->criteria) {
    leftKeys.emplace_back(exprConverter_.toVeloxExpr(left));
    rightKeys.emplace_back(exprConverter_.toVeloxExpr(right));
  }

  return std::make_shared<core::MergeJoinNode>(
      node->id,
      joinType,
      leftKeys,
      rightKeys,
      node->filter ? exprConverter_.toVeloxExpr(*node->filter) : nullptr,
      toVeloxQueryPlan(node->left, tableWriteInfo, taskId),
      toVeloxQueryPlan(node->right, tableWriteInfo, taskId),
      toRowType(node->outputVariables, typeParser_));
}

std::shared_ptr<const core::TopNNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::TopNNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  sortingKeys.reserve(node->orderingScheme.orderBy.size());
  sortingOrders.reserve(node->orderingScheme.orderBy.size());
  for (const auto& [variable, sortOrder] : node->orderingScheme.orderBy) {
    sortingKeys.emplace_back(exprConverter_.toVeloxExpr(variable));
    sortingOrders.emplace_back(toVeloxSortOrder(sortOrder));
  }
  return std::make_shared<core::TopNNode>(
      node->id,
      sortingKeys,
      sortingOrders,
      node->count,
      node->step == protocol::Step::PARTIAL,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::LimitNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::LimitNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  return std::make_shared<core::LimitNode>(
      node->id,
      0,
      node->count,
      node->step == protocol::LimitNodeStep::PARTIAL,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::OrderByNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::SortNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
  std::vector<core::SortOrder> sortingOrders;
  for (const auto& [variable, sortOrder] : node->orderingScheme.orderBy) {
    sortingKeys.emplace_back(exprConverter_.toVeloxExpr(variable));
    sortingOrders.emplace_back(toVeloxSortOrder(sortOrder));
  }

  return std::make_shared<core::OrderByNode>(
      node->id,
      sortingKeys,
      sortingOrders,
      node->isPartial,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::TableWriteNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::TableWriterNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::string connectorId;
  std::shared_ptr<connector::ConnectorInsertTableHandle> connectorInsertHandle;
  if (const auto createHandle =
          std::dynamic_pointer_cast<protocol::CreateHandle>(
              tableWriteInfo->writerTarget)) {
    connectorId = createHandle->handle.connectorId;
    auto& connector =
        getPrestoToVeloxConnector(createHandle->handle.connectorHandle->_type);
    auto veloxHandle =
        connector.toVeloxInsertTableHandle(createHandle.get(), typeParser_);
    connectorInsertHandle = std::shared_ptr(std::move(veloxHandle));
  } else if (
      const auto insertHandle =
          std::dynamic_pointer_cast<protocol::InsertHandle>(
              tableWriteInfo->writerTarget)) {
    connectorId = insertHandle->handle.connectorId;
    auto& connector =
        getPrestoToVeloxConnector(insertHandle->handle.connectorHandle->_type);
    auto veloxHandle =
        connector.toVeloxInsertTableHandle(insertHandle.get(), typeParser_);
    connectorInsertHandle = std::shared_ptr(std::move(veloxHandle));
  }

  if (!connectorInsertHandle) {
    VELOX_UNSUPPORTED(
        "Unsupported table writer handle: {}",
        toJsonString(tableWriteInfo->writerTarget));
  }

  auto insertTableHandle = std::make_shared<core::InsertTableHandle>(
      connectorId, connectorInsertHandle);

  const auto outputType = toRowType(
      generateOutputVariables(
          {node->rowCountVariable,
           node->fragmentVariable,
           node->tableCommitContextVariable},
          node->statisticsAggregation),
      typeParser_);
  const auto sourceVeloxPlan =
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId);
  std::optional<core::ColumnStatsSpec> columnStatsSpec = toColumnStatsSpec(
      node->statisticsAggregation,
      core::AggregationNode::Step::kPartial,
      node->id,
      sourceVeloxPlan,
      tableWriteInfo,
      taskId);
  return std::make_shared<core::TableWriteNode>(
      node->id,
      toRowType(node->columns, typeParser_),
      node->columnNames,
      columnStatsSpec,
      std::move(insertTableHandle),
      node->partitioningScheme != nullptr,
      outputType,
      getCommitStrategy(),
      sourceVeloxPlan);
}

std::shared_ptr<const core::TableWriteNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::DeleteNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::string connectorId;
  std::shared_ptr<connector::ConnectorInsertTableHandle> connectorInsertHandle;
  if (const auto deleteHandle =
          std::dynamic_pointer_cast<protocol::DeleteHandle>(
              tableWriteInfo->writerTarget)) {
    connectorId = deleteHandle->handle.connectorId;
    auto& connector =
        getPrestoToVeloxConnector(deleteHandle->handle.connectorHandle->_type);
    auto veloxHandle =
        connector.toVeloxInsertTableHandle(deleteHandle.get(), typeParser_);
    connectorInsertHandle = std::shared_ptr(std::move(veloxHandle));
  }

  if (!connectorInsertHandle) {
    VELOX_UNSUPPORTED(
        "Unsupported table writer handle: {}",
        toJsonString(tableWriteInfo->writerTarget));
  }

  auto insertTableHandle = std::make_shared<core::InsertTableHandle>(
      connectorId, connectorInsertHandle);

  const auto outputType = toRowType(
      generateOutputVariables(node->outputVariables, nullptr), typeParser_);

  const auto sourceVeloxPlan =
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId);

  // get columns and partition keys from InputDistribution
  const auto inputDistribution =
      std::dynamic_pointer_cast<protocol::BaseInputDistribution>(
          node->inputDistribution);

  if (!inputDistribution) {
    VELOX_UNSUPPORTED(
        "Unsupported input distribution type: {}",
        toJsonString(node->inputDistribution));
  }

  auto inputColumns = toRowType(inputDistribution->inputVariables, typeParser_);

  return std::make_shared<core::TableWriteNode>(
      node->id,
      inputColumns,
      inputColumns->names(),
      /*columnStatsSpec=*/std::nullopt,
      std::move(insertTableHandle),
      true, // delete only supported on partitioned tables
      outputType,
      getCommitStrategy(),
      sourceVeloxPlan);
}

std::shared_ptr<const core::TableWriteMergeNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::TableWriterMergeNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  const auto outputType = toRowType(
      generateOutputVariables(
          {node->rowCountVariable,
           node->fragmentVariable,
           node->tableCommitContextVariable},
          node->statisticsAggregation),
      typeParser_);
  const auto sourceVeloxPlan =
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId);
  std::optional<core::ColumnStatsSpec> columnStatsSpec = toColumnStatsSpec(
      node->statisticsAggregation,
      core::AggregationNode::Step::kIntermediate,
      node->id,
      sourceVeloxPlan,
      tableWriteInfo,
      taskId);

  return std::make_shared<core::TableWriteMergeNode>(
      node->id, outputType, columnStatsSpec, sourceVeloxPlan);
}

std::shared_ptr<const core::UnnestNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::UnnestNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> unnestFields;
  unnestFields.reserve(node->unnestVariables.size());
  std::vector<std::string> unnestNames;
  for (const auto& [unnestField, outputVariables] : node->unnestVariables) {
    unnestFields.emplace_back(exprConverter_.toVeloxExpr(unnestField));
    for (const auto& output : outputVariables) {
      unnestNames.emplace_back(output.name);
    }
  }

  return std::make_shared<core::UnnestNode>(
      node->id,
      toVeloxExprs(node->replicateVariables),
      unnestFields,
      unnestNames,
      node->ordinalityVariable ? std::optional{node->ordinalityVariable->name}
                               : std::nullopt,
      std::nullopt,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::EnforceSingleRowNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::EnforceSingleRowNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  return std::make_shared<core::EnforceSingleRowNode>(
      node->id, toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::AssignUniqueIdNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::AssignUniqueId>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  const auto prestoTaskId = PrestoTaskId(taskId);
  // `taskUniqueId` is an integer to uniquely identify the generated id
  // across all the nodes executing the same query stage in a distributed
  // query execution.
  //
  // 10 bit for stageId && 14-bit for taskId should be sufficient
  // given the max stage per query is 100 by default.

  // taskUniqueId = last 10 bit of stageId | last 14 bits of taskId
  int32_t taskUniqueId = (prestoTaskId.stageId() & ((1 << 10) - 1)) << 14 |
      (prestoTaskId.id() & ((1 << 14) - 1));
  return std::make_shared<core::AssignUniqueIdNode>(
      node->id,
      node->idVariable.name,
      taskUniqueId,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

core::WindowNode::Function VeloxQueryPlanConverterBase::toVeloxWindowFunction(
    const protocol::Function& func) {
  core::WindowNode::Function windowFunc;
  windowFunc.functionCall =
      std::dynamic_pointer_cast<const core::CallTypedExpr>(
          exprConverter_.toVeloxExpr(func.functionCall));
  windowFunc.ignoreNulls = func.ignoreNulls;

  windowFunc.frame.type = toVeloxWindowType(func.frame.type);
  windowFunc.frame.startType = toVeloxBoundType(func.frame.startType);
  windowFunc.frame.startValue = func.frame.startValue
      ? exprConverter_.toVeloxExpr(func.frame.startValue)
      : nullptr;

  windowFunc.frame.endType = toVeloxBoundType(func.frame.endType);
  windowFunc.frame.endValue = func.frame.endValue
      ? exprConverter_.toVeloxExpr(func.frame.endValue)
      : nullptr;

  return windowFunc;
}

namespace {
std::pair<
    std::vector<core::FieldAccessTypedExprPtr>,
    std::vector<core::SortOrder>>
toSortFieldsAndOrders(
    const protocol::OrderingScheme* orderingScheme,
    const VeloxExprConverter& exprConverter,
    const std::unordered_set<std::string>& partitionKeys) {
  std::vector<core::FieldAccessTypedExprPtr> sortFields;
  std::vector<core::SortOrder> sortOrders;
  if (orderingScheme != nullptr) {
    auto nodeSpecOrdering = orderingScheme->orderBy;
    sortFields.reserve(nodeSpecOrdering.size());
    sortOrders.reserve(nodeSpecOrdering.size());
    for (const auto& [variable, sortOrder] : nodeSpecOrdering) {
      // Drop sorting keys that are present in partitioning keys.
      if (partitionKeys.count(variable.name) == 0) {
        sortFields.emplace_back(exprConverter.toVeloxExpr(variable));
        sortOrders.emplace_back(toVeloxSortOrder(sortOrder));
      }
    }
  }
  return {sortFields, sortOrders};
}
} // namespace

std::shared_ptr<const core::WindowNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::WindowNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> partitionFields;
  partitionFields.reserve(node->specification.partitionBy.size());
  std::unordered_set<std::string> partitionFieldNames;
  for (const auto& entry : node->specification.partitionBy) {
    partitionFields.emplace_back(exprConverter_.toVeloxExpr(entry));
    partitionFieldNames.emplace(partitionFields.back()->name());
  }

  auto [sortFields, sortOrders] = toSortFieldsAndOrders(
      node->specification.orderingScheme.get(),
      exprConverter_,
      partitionFieldNames);

  std::vector<std::string> windowNames;
  std::vector<core::WindowNode::Function> windowFunctions;
  windowNames.reserve(node->windowFunctions.size());
  windowFunctions.reserve(node->windowFunctions.size());
  for (const auto& [variable, function] : node->windowFunctions) {
    windowNames.emplace_back(variable.name);
    windowFunctions.emplace_back(toVeloxWindowFunction(function));
  }

  // TODO(spershin): Supply proper 'inputsSorted' argument to WindowNode
  // constructor instead of 'false'.
  return std::make_shared<core::WindowNode>(
      node->id,
      partitionFields,
      sortFields,
      sortOrders,
      windowNames,
      windowFunctions,
      false,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::RowNumberNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::RowNumberNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> partitionFields;
  partitionFields.reserve(node->partitionBy.size());
  for (const auto& entry : node->partitionBy) {
    partitionFields.emplace_back(exprConverter_.toVeloxExpr(entry));
  }

  std::optional<int32_t> limit;
  if (node->maxRowCountPerPartition) {
    limit = *node->maxRowCountPerPartition;
  }

  std::optional<std::string> rowNumberColumnName;
  if (!node->partial) {
    rowNumberColumnName = node->rowNumberVariable.name;
  }

  return std::make_shared<core::RowNumberNode>(
      node->id,
      partitionFields,
      rowNumberColumnName,
      limit,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::PlanNode>
VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::TopNRowNumberNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<core::FieldAccessTypedExprPtr> partitionFields;
  partitionFields.reserve(node->specification.partitionBy.size());
  std::unordered_set<std::string> partitionFieldNames;
  for (const auto& entry : node->specification.partitionBy) {
    partitionFields.emplace_back(exprConverter_.toVeloxExpr(entry));
    partitionFieldNames.emplace(partitionFields.back()->name());
  }

  auto [sortFields, sortOrders] = toSortFieldsAndOrders(
      node->specification.orderingScheme.get(),
      exprConverter_,
      partitionFieldNames);

  std::optional<std::string> rowNumberColumnName;
  if (!node->partial) {
    rowNumberColumnName = node->rowNumberVariable.name;
  }

  if (sortFields.empty()) {
    // May happen if all sorting keys are also used as partition keys.
    return std::make_shared<core::RowNumberNode>(
        node->id,
        partitionFields,
        rowNumberColumnName,
        node->maxRowCountPerPartition,
        toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
  }

  return std::make_shared<core::TopNRowNumberNode>(
      node->id,
      core::TopNRowNumberNode::RankFunction::kRowNumber,
      partitionFields,
      sortFields,
      sortOrders,
      rowNumberColumnName,
      node->maxRowCountPerPartition,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

core::PlanNodePtr VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::PlanNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  if (auto exchange =
          std::dynamic_pointer_cast<const protocol::ExchangeNode>(node)) {
    return toVeloxQueryPlan(exchange, tableWriteInfo, taskId);
  }
  if (auto filter =
          std::dynamic_pointer_cast<const protocol::FilterNode>(node)) {
    return toVeloxQueryPlan(filter, tableWriteInfo, taskId);
  }
  if (auto project =
          std::dynamic_pointer_cast<const protocol::ProjectNode>(node)) {
    return toVeloxQueryPlan(project, tableWriteInfo, taskId);
  }
  if (auto values =
          std::dynamic_pointer_cast<const protocol::ValuesNode>(node)) {
    return toVeloxQueryPlan(values, tableWriteInfo, taskId);
  }
  if (auto tableScan =
          std::dynamic_pointer_cast<const protocol::TableScanNode>(node)) {
    return toVeloxQueryPlan(tableScan, tableWriteInfo, taskId);
  }
  if (auto aggregation =
          std::dynamic_pointer_cast<const protocol::AggregationNode>(node)) {
    return toVeloxQueryPlan(aggregation, tableWriteInfo, taskId);
  }
  if (auto groupId =
          std::dynamic_pointer_cast<const protocol::GroupIdNode>(node)) {
    return toVeloxQueryPlan(groupId, tableWriteInfo, taskId);
  }
  if (auto distinctLimit =
          std::dynamic_pointer_cast<const protocol::DistinctLimitNode>(node)) {
    return toVeloxQueryPlan(distinctLimit, tableWriteInfo, taskId);
  }
  if (auto join = std::dynamic_pointer_cast<const protocol::JoinNode>(node)) {
    return toVeloxQueryPlan(join, tableWriteInfo, taskId);
  }
  if (auto join =
          std::dynamic_pointer_cast<const protocol::SemiJoinNode>(node)) {
    return toVeloxQueryPlan(join, tableWriteInfo, taskId);
  }
  if (auto join =
          std::dynamic_pointer_cast<const protocol::IndexJoinNode>(node)) {
    return toVeloxQueryPlan(join, tableWriteInfo, taskId);
  }
  if (auto indexSource =
          std::dynamic_pointer_cast<const protocol::IndexSourceNode>(node)) {
    return toVeloxQueryPlan(indexSource, tableWriteInfo, taskId);
  }
  if (auto join =
          std::dynamic_pointer_cast<const protocol::MergeJoinNode>(node)) {
    return toVeloxQueryPlan(join, tableWriteInfo, taskId);
  }
  if (auto spatialJoin =
          std::dynamic_pointer_cast<const protocol::SpatialJoinNode>(node)) {
    return toVeloxQueryPlan(spatialJoin, tableWriteInfo, taskId);
  }
  if (auto remoteSource =
          std::dynamic_pointer_cast<const protocol::RemoteSourceNode>(node)) {
    return toVeloxQueryPlan(remoteSource, tableWriteInfo, taskId);
  }
  if (auto topN = std::dynamic_pointer_cast<const protocol::TopNNode>(node)) {
    return toVeloxQueryPlan(topN, tableWriteInfo, taskId);
  }
  if (auto limit = std::dynamic_pointer_cast<const protocol::LimitNode>(node)) {
    return toVeloxQueryPlan(limit, tableWriteInfo, taskId);
  }
  if (auto sort = std::dynamic_pointer_cast<const protocol::SortNode>(node)) {
    return toVeloxQueryPlan(sort, tableWriteInfo, taskId);
  }
  if (auto unnest =
          std::dynamic_pointer_cast<const protocol::UnnestNode>(node)) {
    return toVeloxQueryPlan(unnest, tableWriteInfo, taskId);
  }
  if (auto enforceSingleRow =
          std::dynamic_pointer_cast<const protocol::EnforceSingleRowNode>(
              node)) {
    return toVeloxQueryPlan(enforceSingleRow, tableWriteInfo, taskId);
  }
  if (auto tableWriter =
          std::dynamic_pointer_cast<const protocol::TableWriterNode>(node)) {
    return toVeloxQueryPlan(tableWriter, tableWriteInfo, taskId);
  }
  if (auto deleteNode =
          std::dynamic_pointer_cast<const protocol::DeleteNode>(node)) {
    return toVeloxQueryPlan(deleteNode, tableWriteInfo, taskId);
  }
  if (auto tableWriteMerger =
          std::dynamic_pointer_cast<const protocol::TableWriterMergeNode>(
              node)) {
    return toVeloxQueryPlan(tableWriteMerger, tableWriteInfo, taskId);
  }
  if (auto assignUniqueId =
          std::dynamic_pointer_cast<const protocol::AssignUniqueId>(node)) {
    return toVeloxQueryPlan(assignUniqueId, tableWriteInfo, taskId);
  }
  if (auto window =
          std::dynamic_pointer_cast<const protocol::WindowNode>(node)) {
    return toVeloxQueryPlan(window, tableWriteInfo, taskId);
  }
  if (auto rowNumber =
          std::dynamic_pointer_cast<const protocol::RowNumberNode>(node)) {
    return toVeloxQueryPlan(rowNumber, tableWriteInfo, taskId);
  }
  if (auto topNRowNumber =
          std::dynamic_pointer_cast<const protocol::TopNRowNumberNode>(node)) {
    return toVeloxQueryPlan(topNRowNumber, tableWriteInfo, taskId);
  }
  if (auto markDistinct =
          std::dynamic_pointer_cast<const protocol::MarkDistinctNode>(node)) {
    return toVeloxQueryPlan(markDistinct, tableWriteInfo, taskId);
  }
  if (auto sampleNode =
          std::dynamic_pointer_cast<const protocol::SampleNode>(node)) {
    // SampleNode (used for System TABLESAMPLE) is a no-op at this layer.
    // SYSTEM Tablesample is implemented by sampling when generating splits
    // since it skips in units of logical segments of data.
    // The sampled splits are correctly passed to the TableScanNode which
    // is the source of this SampleNode.
    // BERNOULLI sampling is implemented as a filter on the TableScan
    // directly, and does not have the intermediate SampleNode.
    return toVeloxQueryPlan(sampleNode->source, tableWriteInfo, taskId);
  }
  VELOX_UNSUPPORTED("Unknown plan node type {}", node->_type);
}

namespace {
core::ExecutionStrategy toStrategy(protocol::StageExecutionStrategy strategy) {
  switch (strategy) {
    case protocol::StageExecutionStrategy::UNGROUPED_EXECUTION:
      return core::ExecutionStrategy::kUngrouped;

    case protocol::StageExecutionStrategy::
        FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION:
    case protocol::StageExecutionStrategy::
        DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION:
      return core::ExecutionStrategy::kGrouped;

    case protocol::StageExecutionStrategy::RECOVERABLE_GROUPED_EXECUTION:
      VELOX_UNSUPPORTED(
          "RECOVERABLE_GROUPED_EXECUTION "
          "Stage Execution Strategy is not supported");
  }
  VELOX_UNSUPPORTED(
      "Unknown Stage Execution Strategy type {}", static_cast<int>(strategy));
}

// Presto doesn't have PartitionedOutputNode and assigns its source node's plan
// node id to PartitionedOutputOperator.
// However, Velox has PartitionedOutputNode and doesn't allow duplicate node
// ids. Hence, we use "root." + source node's plan id as
// PartitionedOutputNode's.
// For example, if source node plan id is "10", then the associated
// partitioned output node id is "root.10".
protocol::PlanNodeId toPartitionedOutputNodeId(const protocol::PlanNodeId& id) {
  return "root." + id;
}

} // namespace

core::PlanFragment VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const protocol::PlanFragment& fragment,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  core::PlanFragment planFragment;

  // Convert the fragment info first.
  const auto& descriptor = fragment.stageExecutionDescriptor;
  planFragment.executionStrategy =
      toStrategy(fragment.stageExecutionDescriptor.stageExecutionStrategy);
  planFragment.numSplitGroups = descriptor.totalLifespans;
  for (const auto& planNodeId : descriptor.groupedExecutionScanNodes) {
    planFragment.groupedExecutionLeafNodeIds.emplace(planNodeId);
  }
  if (planFragment.executionStrategy == core::ExecutionStrategy::kGrouped) {
    VELOX_CHECK(
        !planFragment.groupedExecutionLeafNodeIds.empty(),
        "groupedExecutionScanNodes cannot be empty if stage execution strategy "
        "is grouped execution");
  }

  if (auto output = std::dynamic_pointer_cast<const protocol::OutputNode>(
          fragment.root)) {
    planFragment.planNode = toVeloxQueryPlan(output, tableWriteInfo, taskId);
    return planFragment;
  }

  auto partitioningScheme = fragment.partitioningScheme;
  auto partitioningHandle =
      partitioningScheme.partitioning.handle.connectorHandle;

  auto partitioningKeys =
      toTypedExprs(partitioningScheme.partitioning.arguments, exprConverter_);

  auto sourceNode = toVeloxQueryPlan(fragment.root, tableWriteInfo, taskId);
  auto inputType = sourceNode->outputType();

  std::vector<column_index_t> keyChannels;
  std::vector<VectorPtr> constValues;
  keyChannels.reserve(partitioningKeys.size());
  for (const auto& expr : partitioningKeys) {
    auto channel = exprToChannel(expr.get(), inputType);
    keyChannels.push_back(channel);
    // For constant channels create a base vector, add single value to it from
    // our variant and add it to the list of constant expressions.
    if (channel == kConstantChannel) {
      auto constExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr);
      if (constExpr->hasValueVector()) {
        constValues.emplace_back(constExpr->valueVector());
      } else {
        constValues.emplace_back(BaseVector::create(expr->type(), 1, pool_));
        setCellFromVariant(constValues.back(), 0, constExpr->value());
      }
    }
  }
  auto outputType = toRowType(partitioningScheme.outputLayout, typeParser_);
  const auto partitionedOutputNodeId =
      toPartitionedOutputNodeId(fragment.root->id);

  if (auto systemPartitioningHandle =
          std::dynamic_pointer_cast<protocol::SystemPartitioningHandle>(
              partitioningHandle)) {
    switch (systemPartitioningHandle->partitioning) {
      case protocol::SystemPartitioning::SINGLE:
        VELOX_CHECK(
            systemPartitioningHandle->function ==
                protocol::SystemPartitionFunction::SINGLE,
            "Unsupported partitioning function: {}",
            toJsonString(systemPartitioningHandle->function));
        planFragment.planNode = core::PartitionedOutputNode::single(
            partitionedOutputNodeId,
            outputType,
            toVeloxSerdeKind(partitioningScheme.encoding),
            sourceNode);
        return planFragment;
      case protocol::SystemPartitioning::FIXED: {
        switch (systemPartitioningHandle->function) {
          case protocol::SystemPartitionFunction::ROUND_ROBIN: {
            auto numPartitions = partitioningScheme.bucketToPartition
                ? partitioningScheme.bucketToPartition->size()
                : 1;

            if (numPartitions == 1) {
              planFragment.planNode = core::PartitionedOutputNode::single(
                  partitionedOutputNodeId,
                  outputType,
                  toVeloxSerdeKind(partitioningScheme.encoding),
                  sourceNode);
              return planFragment;
            }
            planFragment.planNode =
                std::make_shared<core::PartitionedOutputNode>(
                    partitionedOutputNodeId,
                    core::PartitionedOutputNode::Kind::kPartitioned,
                    partitioningKeys,
                    numPartitions,
                    partitioningScheme.replicateNullsAndAny,
                    std::make_shared<RoundRobinPartitionFunctionSpec>(),
                    outputType,
                    toVeloxSerdeKind(partitioningScheme.encoding),
                    sourceNode);
            return planFragment;
          }
          case protocol::SystemPartitionFunction::HASH: {
            auto numPartitions = partitioningScheme.bucketToPartition
                ? partitioningScheme.bucketToPartition->size()
                : 1;

            if (numPartitions == 1) {
              planFragment.planNode = core::PartitionedOutputNode::single(
                  partitionedOutputNodeId,
                  outputType,
                  toVeloxSerdeKind(partitioningScheme.encoding),
                  sourceNode);
              return planFragment;
            }
            planFragment.planNode =
                std::make_shared<core::PartitionedOutputNode>(
                    partitionedOutputNodeId,
                    core::PartitionedOutputNode::Kind::kPartitioned,
                    partitioningKeys,
                    numPartitions,
                    partitioningScheme.replicateNullsAndAny,
                    std::make_shared<HashPartitionFunctionSpec>(
                        inputType, keyChannels, constValues),
                    outputType,
                    toVeloxSerdeKind(partitioningScheme.encoding),
                    sourceNode);
            return planFragment;
          }
          case protocol::SystemPartitionFunction::BROADCAST: {
            planFragment.planNode = core::PartitionedOutputNode::broadcast(
                partitionedOutputNodeId,
                1,
                outputType,
                toVeloxSerdeKind(partitioningScheme.encoding),
                sourceNode);
            return planFragment;
          }
          default:
            VELOX_UNSUPPORTED(
                "Unsupported partitioning function: {}",
                toJsonString(systemPartitioningHandle->function));
        }
      }
      case protocol::SystemPartitioning::SCALED: {
        VELOX_CHECK(
            systemPartitioningHandle->function ==
                protocol::SystemPartitionFunction::ROUND_ROBIN,
            "Unsupported partitioning function: {}",
            toJsonString(systemPartitioningHandle->function));
        planFragment.planNode = core::PartitionedOutputNode::arbitrary(
            partitionedOutputNodeId,
            std::move(outputType),
            toVeloxSerdeKind(partitioningScheme.encoding),
            std::move(sourceNode));
        return planFragment;
      }
      default:
        VELOX_FAIL(
            "Unsupported kind of SystemPartitioning: {}",
            toJsonString(systemPartitioningHandle->partitioning));
    }
  }

  const auto& bucketToPartition = *partitioningScheme.bucketToPartition;
  auto numPartitions =
      *std::max_element(bucketToPartition.begin(), bucketToPartition.end()) + 1;

  if (numPartitions == 1) {
    planFragment.planNode = core::PartitionedOutputNode::single(
        partitionedOutputNodeId,
        outputType,
        toVeloxSerdeKind(partitioningScheme.encoding),
        sourceNode);
    return planFragment;
  }

  auto& connector = getPrestoToVeloxConnector(partitioningHandle->_type);
  auto spec = connector.createVeloxPartitionFunctionSpec(
      partitioningHandle.get(), bucketToPartition, keyChannels, constValues);
  planFragment.planNode = std::make_shared<core::PartitionedOutputNode>(
      partitionedOutputNodeId,
      core::PartitionedOutputNode::Kind::kPartitioned,
      partitioningKeys,
      numPartitions,
      partitioningScheme.replicateNullsAndAny,
      std::shared_ptr(std::move(spec)),
      toRowType(partitioningScheme.outputLayout, typeParser_),
      toVeloxSerdeKind(partitioningScheme.encoding),
      sourceNode);
  return planFragment;
}

core::PlanNodePtr VeloxQueryPlanConverterBase::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::OutputNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  return core::PartitionedOutputNode::single(
      node->id,
      toRowType(node->outputVariables, typeParser_),
      VectorSerde::Kind::kPresto,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

core::PlanNodePtr VeloxInteractiveQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::RemoteSourceNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& /*tableWriteInfo*/,
    const protocol::TaskId& taskId) {
  auto rowType = toRowType(node->outputVariables, typeParser_);
  if (node->orderingScheme) {
    std::vector<core::FieldAccessTypedExprPtr> sortingKeys;
    std::vector<core::SortOrder> sortingOrders;
    sortingKeys.reserve(node->orderingScheme->orderBy.size());
    sortingOrders.reserve(node->orderingScheme->orderBy.size());

    for (const auto& [variable, sortOrder] : node->orderingScheme->orderBy) {
      sortingKeys.emplace_back(exprConverter_.toVeloxExpr(variable));
      sortingOrders.emplace_back(toVeloxSortOrder(sortOrder));
    }
    return std::make_shared<core::MergeExchangeNode>(
        node->id,
        rowType,
        sortingKeys,
        sortingOrders,
        toVeloxSerdeKind(node->encoding));
  }
  return std::make_shared<core::ExchangeNode>(
      node->id, rowType, toVeloxSerdeKind(node->encoding));
}

connector::CommitStrategy
VeloxInteractiveQueryPlanConverter::getCommitStrategy() const {
  return connector::CommitStrategy::kNoCommit;
}

core::PlanFragment VeloxBatchQueryPlanConverter::toVeloxQueryPlan(
    const protocol::PlanFragment& fragment,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto planFragment = VeloxQueryPlanConverterBase::toVeloxQueryPlan(
      fragment, tableWriteInfo, taskId);

  const auto partitionedOutputNode =
      std::dynamic_pointer_cast<const core::PartitionedOutputNode>(
          planFragment.planNode);

  VELOX_USER_CHECK_NOT_NULL(
      partitionedOutputNode, "PartitionedOutputNode is required");

  if (partitionedOutputNode->isBroadcast()) {
    VELOX_USER_CHECK_NOT_NULL(
        broadcastBasePath_, "broadcastBasePath is required");
    const auto maxBroadcastBytesOpt =
        SystemConfig::instance()->taskMaxStorageBroadcastBytes();
    // TODO - Use original plan node with root node and aggregate operator
    // stats for additional nodes.
    auto broadcastWriteNode = std::make_shared<operators::BroadcastWriteNode>(
        fmt::format("{}.bw", partitionedOutputNode->id()),
        *broadcastBasePath_,
        maxBroadcastBytesOpt.has_value() ? maxBroadcastBytesOpt.value()
                                         : std::numeric_limits<uint64_t>::max(),
        partitionedOutputNode->outputType(),
        core::LocalPartitionNode::gather(
            "broadcast-write-gather",
            std::vector{partitionedOutputNode->sources()}));

    planFragment.planNode = core::PartitionedOutputNode::broadcast(
        partitionedOutputNode->id(),
        1,
        broadcastWriteNode->outputType(),
        VectorSerde::Kind::kPresto,
        {broadcastWriteNode});
    return planFragment;
  }

  // If the serializedShuffleWriteInfo is not nullptr, it means this fragment
  // ends with a shuffle stage. We convert the PartitionedOutputNode to a
  // chain of following nodes:
  // (1) A PartitionAndSerializeNode.
  // (2) A "gather" LocalPartitionNode that gathers results from multiple
  //     threads to one thread.
  // (3) A ShuffleWriteNode.
  // To be noted, whether the last node of the plan is PartitionedOutputNode
  // can't guarantee the query has shuffle stage, for example a plan with
  // TableWriteNode can also have PartitionedOutputNode to distribute the
  // metadata to coordinator.
  if (serializedShuffleWriteInfo_ == nullptr) {
    VELOX_USER_CHECK_EQ(1, partitionedOutputNode->numPartitions());
    return planFragment;
  }

  // Convert outputOrderingScheme to sortingKeys and sortingOrders
  std::optional<std::vector<velox::core::SortOrder>> sortingOrders;
  std::optional<std::vector<velox::core::FieldAccessTypedExprPtr>> sortingKeys;

  if (fragment.outputOrderingScheme) {
    std::vector<velox::core::SortOrder> orders;
    std::vector<velox::core::FieldAccessTypedExprPtr> keys;

    orders.reserve(fragment.outputOrderingScheme->orderBy.size());
    keys.reserve(fragment.outputOrderingScheme->orderBy.size());

    for (const auto& ordering : fragment.outputOrderingScheme->orderBy) {
      keys.emplace_back(exprConverter_.toVeloxExpr(ordering.variable));
      orders.emplace_back(toVeloxSortOrder(ordering.sortOrder));
    }

    sortingKeys = std::move(keys);
    sortingOrders = std::move(orders);
  }

  const auto partitionAndSerializeNode =
      std::make_shared<operators::PartitionAndSerializeNode>(
          fmt::format("{}.ps", partitionedOutputNode->id()),
          partitionedOutputNode->keys(),
          partitionedOutputNode->numPartitions(),
          partitionedOutputNode->outputType(),
          partitionedOutputNode->sources()[0],
          partitionedOutputNode->isReplicateNullsAndAny(),
          partitionedOutputNode->partitionFunctionSpecPtr(),
          sortingOrders,
          sortingKeys);

  planFragment.planNode = std::make_shared<operators::ShuffleWriteNode>(
      fmt::format("{}.sw", partitionedOutputNode->id()),
      partitionedOutputNode->numPartitions(),
      shuffleName_,
      std::move(*serializedShuffleWriteInfo_),
      core::LocalPartitionNode::gather(
          fmt::format("{}.g", partitionedOutputNode->id()),
          std::vector<core::PlanNodePtr>{partitionAndSerializeNode}));
  return planFragment;
}

core::PlanNodePtr VeloxBatchQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::RemoteSourceNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& /* tableWriteInfo */,
    const protocol::TaskId& taskId) {
  auto rowType = toRowType(node->outputVariables, typeParser_);
  // Broadcast exchange source.
  if (node->exchangeType == protocol::ExchangeNodeType::REPLICATE) {
    return std::make_shared<core::ExchangeNode>(
        node->id, rowType, VectorSerde::Kind::kPresto);
  }
  // Partitioned shuffle exchange source.
  return std::make_shared<operators::ShuffleReadNode>(node->id, rowType);
}

connector::CommitStrategy VeloxBatchQueryPlanConverter::getCommitStrategy()
    const {
  return connector::CommitStrategy::kTaskCommit;
}

void registerPrestoPlanNodeSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();

  registry.Register(
      "PartitionAndSerializeNode",
      presto::operators::PartitionAndSerializeNode::create);
  registry.Register(
      "ShuffleReadNode", presto::operators::ShuffleReadNode::create);
  registry.Register(
      "ShuffleWriteNode", presto::operators::ShuffleWriteNode::create);
  registry.Register(
      "BroadcastWriteNode", presto::operators::BroadcastWriteNode::create);
  registry.Register(
      "TableFunctionProcessorNode",
      presto::tvf::TableFunctionProcessorNode::create);
}

void parseSqlFunctionHandle(
    const std::shared_ptr<protocol::SqlFunctionHandle>& sqlFunction,
    std::vector<TypePtr>& rawInputTypes,
    TypeParser& typeParser) {
  const auto& functionId = sqlFunction->functionId;
  // functionId format is function-name;arg-type1;arg-type2;...
  // For example: foo;INTEGER;VARCHAR.
  auto start = functionId.find(';');
  if (start != std::string::npos) {
    for (;;) {
      auto pos = functionId.find(';', start + 1);
      if (pos == std::string::npos) {
        auto argumentType = functionId.substr(start + 1);
        if (!argumentType.empty()) {
          rawInputTypes.push_back(stringToType(argumentType, typeParser));
        }
        break;
      }
      auto argumentType = functionId.substr(start + 1, pos - start - 1);
      VELOX_CHECK(!argumentType.empty());
      rawInputTypes.push_back(stringToType(argumentType, typeParser));
      start = pos;
    }
  }
}

void handleUnsupportedIndexLookupCondition(
    const std::shared_ptr<protocol::RowExpression>& filter,
    const VeloxExprConverter& exprConverter,
    std::vector<core::TypedExprPtr>& unsupportedConditions) {
  // OR conditions cannot be converted to index lookup conditions
  if (const auto orForm = isOr(filter)) {
    VELOX_UNSUPPORTED(
        "Unsupported index lookup condition: {}", toJsonString(filter));
  }
  unsupportedConditions.push_back(exprConverter.toVeloxExpr(filter));
}

void parseIndexLookupCondition(
    const std::shared_ptr<protocol::RowExpression>& filter,
    const std::vector<protocol::VariableReferenceExpression>& lookupVariables,
    const VeloxExprConverter& exprConverter,
    bool acceptConstant,
    std::vector<core::IndexLookupConditionPtr>& joinConditionPtrs,
    std::vector<core::TypedExprPtr>& unsupportedConditions) {
  if (const auto andForm = isAnd(filter)) {
    VELOX_CHECK_EQ(andForm->arguments.size(), 2);
    for (const auto& child : andForm->arguments) {
      parseIndexLookupCondition(
          child,
          lookupVariables,
          exprConverter,
          acceptConstant,
          joinConditionPtrs,
          unsupportedConditions);
    }
    return;
  }

  // Helper function to check if a variable is in the lookup variables
  auto isLookupVariable =
      [&](const protocol::VariableReferenceExpression& var) {
        if (lookupVariables.empty()) {
          return true; // If empty, treat all variables as lookup variables for
                       // compatibility
        }
        return std::find_if(
                   lookupVariables.begin(),
                   lookupVariables.end(),
                   [&var](
                       const protocol::VariableReferenceExpression& lookupVar) {
                     return var.name == lookupVar.name &&
                         var.type == lookupVar.type;
                   }) != lookupVariables.end();
      };
  if (const auto between = isBetween(filter)) {
    VELOX_CHECK_EQ(between->arguments.size(), 3);
    const auto keyArgument =
        std::dynamic_pointer_cast<protocol::VariableReferenceExpression>(
            between->arguments[0]);
    VELOX_CHECK_NOT_NULL(
        keyArgument, "{}", toJsonString(between->arguments[0]));

    // Only create IndexLookupCondition if the variable is in lookup variables
    if (isLookupVariable(*keyArgument)) {
      const auto keyColumnExpr = exprConverter.toVeloxExpr(keyArgument);
      const auto lowerExpr = exprConverter.toVeloxExpr(between->arguments[1]);
      const auto upperExpr = exprConverter.toVeloxExpr(between->arguments[2]);

      if (acceptConstant ||
          !(core::TypedExprs::isConstant(lowerExpr) &&
            core::TypedExprs::isConstant(upperExpr))) {
        joinConditionPtrs.push_back(
            std::make_shared<core::BetweenIndexLookupCondition>(
                keyColumnExpr, lowerExpr, upperExpr));
        return;
      }
    }
  }

  if (const auto contains = isContains(filter)) {
    VELOX_CHECK_EQ(contains->arguments.size(), 2);
    const auto keyArgument =
        std::dynamic_pointer_cast<protocol::VariableReferenceExpression>(
            contains->arguments[1]);
    VELOX_CHECK_NOT_NULL(
        keyArgument, "{}", toJsonString(contains->arguments[1]));

    // Only create IndexLookupCondition if the variable is in lookup variables
    if (isLookupVariable(*keyArgument)) {
      const auto keyColumnExpr = exprConverter.toVeloxExpr(keyArgument);
      const auto conditionColumnExpr =
          exprConverter.toVeloxExpr(contains->arguments[0]);

      if (acceptConstant ||
          !core::TypedExprs::isConstant(conditionColumnExpr)) {
        joinConditionPtrs.push_back(
            std::make_shared<core::InIndexLookupCondition>(
                keyColumnExpr, conditionColumnExpr));
        return;
      }
    }
  }

  if (const auto equals = isEqual(filter)) {
    VELOX_CHECK_EQ(equals->arguments.size(), 2);
    const auto leftExpr = exprConverter.toVeloxExpr(equals->arguments[0]);
    const auto rightExpr = exprConverter.toVeloxExpr(equals->arguments[1]);

    const bool leftIsConstant = core::TypedExprs::isConstant(leftExpr);
    const bool rightIsConstant = core::TypedExprs::isConstant(rightExpr);

    VELOX_CHECK(
        !(leftIsConstant && rightIsConstant),
        "The equal condition must have at least one side to be non-constant: {}",
        toJsonString(filter));

    // Check if the equal condition is a constant expression and a field access.
    std::shared_ptr<protocol::VariableReferenceExpression> keyArgument;
    core::TypedExprPtr constantExpr;
    if (core::TypedExprs::isFieldAccess(leftExpr) && rightIsConstant) {
      keyArgument =
          std::dynamic_pointer_cast<protocol::VariableReferenceExpression>(
              equals->arguments[0]);
      constantExpr = rightExpr;
    } else if (core::TypedExprs::isFieldAccess(rightExpr) && leftIsConstant) {
      keyArgument =
          std::dynamic_pointer_cast<protocol::VariableReferenceExpression>(
              equals->arguments[1]);
      constantExpr = leftExpr;
    }

    if (keyArgument != nullptr && constantExpr != nullptr) {
      // Only create IndexLookupCondition if the variable is in lookup variables
      if (isLookupVariable(*keyArgument)) {
        const auto keyColumnExpr = exprConverter.toVeloxExpr(keyArgument);
        VELOX_CHECK_NOT_NULL(
            keyColumnExpr,
            "Key argument must be a variable reference: {}",
            toJsonString(keyArgument));
        joinConditionPtrs.push_back(
            std::make_shared<core::EqualIndexLookupCondition>(
                keyColumnExpr, constantExpr));
        return;
      }
    }
  }

  // For unsupported conditions, add to the vector or throw error.
  handleUnsupportedIndexLookupCondition(
      filter, exprConverter, unsupportedConditions);
}
} // namespace facebook::presto
