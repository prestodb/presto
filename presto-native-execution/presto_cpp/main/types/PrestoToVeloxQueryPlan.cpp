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
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "presto_cpp/main/types/TypeSignatureTypeConverter.h"
// clang-format on

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

using namespace facebook::velox;

namespace facebook::presto {

namespace {

std::shared_ptr<const velox::Type> stringToType(const std::string& typeString) {
  return TypeSignatureTypeConverter::parse(typeString);
}

std::vector<std::string> getNames(const protocol::Assignments& assignments) {
  std::vector<std::string> names;
  names.reserve(assignments.assignments.size());

  for (const auto& assignment : assignments.assignments) {
    names.emplace_back(assignment.first.name);
  }

  return names;
}

std::shared_ptr<const RowType> toRowType(
    const std::vector<protocol::VariableReferenceExpression>& variables,
    const std::unordered_set<std::string>& excludeNames = {}) {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(variables.size());
  types.reserve(variables.size());

  for (const auto& variable : variables) {
    if (excludeNames.count(variable.name)) {
      continue;
    }
    names.emplace_back(variable.name);
    types.emplace_back(stringToType(variable.type));
  }

  return ROW(std::move(names), std::move(types));
}

connector::hive::HiveColumnHandle::ColumnType toHiveColumnType(
    protocol::ColumnType type) {
  switch (type) {
    case protocol::ColumnType::PARTITION_KEY:
      return connector::hive::HiveColumnHandle::ColumnType::kPartitionKey;
    case protocol::ColumnType::REGULAR:
      return connector::hive::HiveColumnHandle::ColumnType::kRegular;
    case protocol::ColumnType::SYNTHESIZED:
      return connector::hive::HiveColumnHandle::ColumnType::kSynthesized;
    default:
      throw std::invalid_argument("Unknown Hive column type");
  }
}

std::shared_ptr<connector::ColumnHandle> toColumnHandle(
    const std::shared_ptr<const protocol::ColumnHandle>& column) {
  if (auto hiveColumn =
          std::dynamic_pointer_cast<const protocol::HiveColumnHandle>(column)) {
    return std::make_shared<connector::hive::HiveColumnHandle>(
        hiveColumn->name,
        toHiveColumnType(hiveColumn->columnType),
        stringToType(hiveColumn->typeSignature));
  }

  throw std::invalid_argument("Unknown column handle type: " + column->_type);
}

int64_t toInt64(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto value = exprConverter.getConstantValue(type, *block);
  return VariantConverter::convert<velox::TypeKind::BIGINT>(value)
      .value<int64_t>();
}

std::unique_ptr<common::BigintRange> bigintRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowUnbounded = range.low.valueBlock == nullptr;
  auto low = lowUnbounded ? std::numeric_limits<int64_t>::min()
                          : toInt64(range.low.valueBlock, exprConverter, type);
  if (!lowUnbounded && range.low.bound == protocol::Bound::ABOVE) {
    low++;
  }

  bool highUnbounded = range.high.valueBlock == nullptr;
  auto high = highUnbounded
      ? std::numeric_limits<int64_t>::max()
      : toInt64(range.high.valueBlock, exprConverter, type);
  if (!highUnbounded && range.high.bound == protocol::Bound::BELOW) {
    high--;
  }
  return std::make_unique<common::BigintRange>(low, high, nullAllowed);
}

double toDouble(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto variant = exprConverter.getConstantValue(type, *block);
  return variant.value<double>();
}

float toFloat(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto variant = exprConverter.getConstantValue(type, *block);
  return variant.value<float>();
}

std::string toString(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto value = exprConverter.getConstantValue(type, *block);
  return value.value<std::string>();
}

bool toBoolean(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto variant = exprConverter.getConstantValue(type, *block);
  return variant.value<bool>();
}

std::unique_ptr<common::Filter> boolRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowValue = toBoolean(range.low.valueBlock, exprConverter, type);
  bool highValue = toBoolean(range.high.valueBlock, exprConverter, type);
  if (lowValue != highValue) {
    VELOX_CHECK(!nullAllowed, "Unexpected range of all values");
    return std::make_unique<common::IsNotNull>();
  }
  return std::make_unique<common::BoolValue>(lowValue, nullAllowed);
}

std::unique_ptr<common::DoubleRange> doubleRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowExclusive = range.low.bound == protocol::Bound::ABOVE;
  bool lowUnbounded = range.low.valueBlock == nullptr && lowExclusive;
  auto low = lowUnbounded ? std::numeric_limits<double>::lowest()
                          : toDouble(range.low.valueBlock, exprConverter, type);

  bool highExclusive = range.high.bound == protocol::Bound::BELOW;
  bool highUnbounded = range.high.valueBlock == nullptr && highExclusive;
  auto high = highUnbounded
      ? std::numeric_limits<double>::max()
      : toDouble(range.high.valueBlock, exprConverter, type);
  return std::make_unique<common::DoubleRange>(
      low,
      lowUnbounded,
      lowExclusive,
      high,
      highUnbounded,
      highExclusive,
      nullAllowed);
}

std::unique_ptr<common::FloatRange> floatRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowExclusive = range.low.bound == protocol::Bound::ABOVE;
  bool lowUnbounded = range.low.valueBlock == nullptr && lowExclusive;
  auto low = lowUnbounded ? std::numeric_limits<float>::lowest()
                          : toFloat(range.low.valueBlock, exprConverter, type);

  bool highExclusive = range.high.bound == protocol::Bound::BELOW;
  bool highUnbounded = range.high.valueBlock == nullptr && highExclusive;
  auto high = highUnbounded
      ? std::numeric_limits<float>::max()
      : toFloat(range.high.valueBlock, exprConverter, type);
  return std::make_unique<common::FloatRange>(
      low,
      lowUnbounded,
      lowExclusive,
      high,
      highUnbounded,
      highExclusive,
      nullAllowed);
}

std::unique_ptr<common::BytesRange> varcharRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowExclusive = range.low.bound == protocol::Bound::ABOVE;
  bool lowUnbounded = range.low.valueBlock == nullptr && lowExclusive;
  auto low =
      lowUnbounded ? "" : toString(range.low.valueBlock, exprConverter, type);

  bool highExclusive = range.high.bound == protocol::Bound::BELOW;
  bool highUnbounded = range.high.valueBlock == nullptr && highExclusive;
  auto high =
      highUnbounded ? "" : toString(range.high.valueBlock, exprConverter, type);
  return std::make_unique<common::BytesRange>(
      low,
      lowUnbounded,
      lowExclusive,
      high,
      highUnbounded,
      highExclusive,
      nullAllowed);
}

std::unique_ptr<common::Filter> toFilter(
    const std::shared_ptr<const Type>& type,
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter) {
  switch (type->kind()) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      return bigintRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::DOUBLE:
      return doubleRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::VARCHAR:
      return varcharRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::BOOLEAN:
      return boolRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::REAL:
      return floatRangeToFilter(range, nullAllowed, exprConverter, type);
    default:
      VELOX_UNSUPPORTED("Unsupported range type: {}", type->toString());
  }
}

std::unique_ptr<common::Filter> toFilter(
    const protocol::Domain& domain,
    const VeloxExprConverter& exprConverter) {
  auto nullAllowed = domain.nullAllowed;
  auto sortedRangeSet =
      std::dynamic_pointer_cast<protocol::SortedRangeSet>(domain.values);
  VELOX_CHECK(sortedRangeSet, "Unsupported ValueSet type");
  auto type = stringToType(sortedRangeSet->type);
  auto ranges = sortedRangeSet->ranges;

  if (ranges.empty()) {
    VELOX_CHECK(nullAllowed, "Unexpected always-false filter");
    return std::make_unique<common::IsNull>();
  }

  if (ranges.size() == 1) {
    // 'is not null' arrives as unbounded range with 'nulls not allowed'.
    // We catch this case and create 'is not null' filter instead of the range
    // filter.
    const auto& range = ranges[0];
    bool lowExclusive = range.low.bound == protocol::Bound::ABOVE;
    bool lowUnbounded = range.low.valueBlock == nullptr && lowExclusive;
    bool highExclusive = range.high.bound == protocol::Bound::BELOW;
    bool highUnbounded = range.high.valueBlock == nullptr && highExclusive;
    if (lowUnbounded && highUnbounded && !nullAllowed) {
      return std::make_unique<common::IsNotNull>();
    }

    return toFilter(type, ranges[0], nullAllowed, exprConverter);
  }

  if (type->kind() == TypeKind::BIGINT || type->kind() == TypeKind::INTEGER ||
      type->kind() == TypeKind::SMALLINT || type->kind() == TypeKind::TINYINT) {
    std::vector<std::unique_ptr<common::BigintRange>> bigintFilters;
    bigintFilters.reserve(ranges.size());
    for (const auto& range : ranges) {
      bigintFilters.emplace_back(
          bigintRangeToFilter(range, nullAllowed, exprConverter, type));
    }

    bool allSingleValue = std::all_of(
        bigintFilters.begin(), bigintFilters.end(), [](const auto& range) {
          return range->isSingleValue();
        });

    if (allSingleValue) {
      std::vector<int64_t> values;
      values.reserve(bigintFilters.size());
      for (const auto& filter : bigintFilters) {
        values.emplace_back(filter->lower());
      }
      return common::createBigintValues(values, nullAllowed);
    }

    return std::make_unique<common::BigintMultiRange>(
        std::move(bigintFilters), nullAllowed);
  }

  if (type->kind() == TypeKind::VARCHAR) {
    std::vector<std::unique_ptr<common::BytesRange>> bytesFilters;
    bytesFilters.reserve(ranges.size());
    for (const auto& range : ranges) {
      bytesFilters.emplace_back(
          varcharRangeToFilter(range, nullAllowed, exprConverter, type));
    }

    bool allSingleValue = std::all_of(
        bytesFilters.begin(), bytesFilters.end(), [](const auto& range) {
          return range->isSingleValue();
        });

    if (allSingleValue) {
      std::vector<std::string> values;
      values.reserve(bytesFilters.size());
      for (const auto& filter : bytesFilters) {
        values.emplace_back(filter->lower());
      }
      return std::make_unique<common::BytesValues>(values, nullAllowed);
    }
  }

  std::vector<std::unique_ptr<common::Filter>> filters;
  filters.reserve(ranges.size());
  for (const auto& range : ranges) {
    filters.emplace_back(toFilter(type, range, nullAllowed, exprConverter));
  }

  return std::make_unique<common::MultiRange>(
      std::move(filters), nullAllowed, false);
}

std::shared_ptr<connector::ConnectorTableHandle> toConnectorTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter) {
  if (auto hiveLayout =
          std::dynamic_pointer_cast<const protocol::HiveTableLayoutHandle>(
              tableHandle.connectorTableLayout)) {
    VELOX_CHECK(
        hiveLayout->pushdownFilterEnabled,
        "Table scan with filter pushdown disabled is not supported");

    connector::hive::SubfieldFilters subfieldFilters;
    auto domains = hiveLayout->domainPredicate.domains;
    for (const auto& domain : *domains) {
      auto filter = domain.second;
      subfieldFilters[common::Subfield(domain.first)] =
          toFilter(domain.second, exprConverter);
    }

    auto remainingFilter =
        exprConverter.toVeloxExpr(hiveLayout->remainingPredicate);
    if (auto constant =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                remainingFilter)) {
      bool value = constant->value().value<bool>();
      VELOX_CHECK(value, "Unexpected always-false remaining predicate");

      // Use null for always-true filter.
      remainingFilter = nullptr;
    }

    auto hiveTableHandle =
        std::dynamic_pointer_cast<const protocol::HiveTableHandle>(
            tableHandle.connectorHandle);
    VELOX_CHECK_NOT_NULL(hiveTableHandle);

    // Use fully qualified name if available.
    std::string tableName = hiveTableHandle->schemaName.empty()
        ? hiveTableHandle->tableName
        : fmt::format(
              "{}.{}", hiveTableHandle->schemaName, hiveTableHandle->tableName);

    return std::make_shared<connector::hive::HiveTableHandle>(
        tableName, true, std::move(subfieldFilters), remainingFilter);
  }

  throw std::invalid_argument("Unsupported TableHandle type");
}

std::vector<std::shared_ptr<const velox::core::ITypedExpr>> getProjections(
    const VeloxExprConverter& exprConverter,
    const protocol::Assignments& assignments) {
  std::vector<std::shared_ptr<const velox::core::ITypedExpr>> expressions;
  expressions.reserve(assignments.assignments.size());
  for (const auto& assignment : assignments.assignments) {
    expressions.emplace_back(exprConverter.toVeloxExpr(assignment.second));
  }

  return expressions;
}

template <TypeKind KIND>
void setCellFromVariantByKind(
    const VectorPtr& column,
    vector_size_t row,
    const velox::variant& value) {
  using T = typename TypeTraits<KIND>::NativeType;

  auto flatVector = column->as<FlatVector<T>>();
  flatVector->set(row, value.value<T>());
}

template <>
void setCellFromVariantByKind<TypeKind::VARBINARY>(
    const VectorPtr& /*column*/,
    vector_size_t /*row*/,
    const velox::variant& value) {
  throw std::invalid_argument("Return of VARBINARY data is not supported");
}

template <>
void setCellFromVariantByKind<TypeKind::VARCHAR>(
    const VectorPtr& column,
    vector_size_t row,
    const velox::variant& value) {
  auto values = column->as<FlatVector<StringView>>();
  values->set(row, StringView(value.value<Varchar>()));
}

void setCellFromVariant(
    const RowVectorPtr& data,
    vector_size_t row,
    vector_size_t column,
    const velox::variant& value) {
  auto columnVector = data->childAt(column);
  if (value.isNull()) {
    columnVector->setNull(row, true);
    return;
  }
  VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      setCellFromVariantByKind,
      columnVector->typeKind(),
      columnVector,
      row,
      value);
}

velox::core::SortOrder toVeloxSortOrder(const protocol::SortOrder& sortOrder) {
  switch (sortOrder) {
    case protocol::SortOrder::ASC_NULLS_FIRST:
      return SortOrder(true, true);
    case protocol::SortOrder::ASC_NULLS_LAST:
      return SortOrder(true, false);
    case protocol::SortOrder::DESC_NULLS_FIRST:
      return SortOrder(false, true);
    case protocol::SortOrder::DESC_NULLS_LAST:
      return SortOrder(false, false);
    default:
      throw std::invalid_argument("Unknown sort order");
  }
}

bool isFixedPartition(
    const std::shared_ptr<const protocol::ExchangeNode>& node,
    protocol::SystemPartitionFunction partitionFunction) {
  if (node->type != protocol::ExchangeNodeType::REPARTITION) {
    return false;
  }

  auto connectorHandle =
      node->partitioningScheme.partitioning.handle.connectorHandle;
  auto handle = std::dynamic_pointer_cast<protocol::SystemPartitioningHandle>(
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

std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> toFieldExprs(
    const std::vector<std::shared_ptr<protocol::RowExpression>>& expressions,
    const VeloxExprConverter& exprConverter) {
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields;
  fields.reserve(expressions.size());
  for (auto& expr : expressions) {
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

std::vector<ChannelIndex> toChannels(
    const RowTypePtr& type,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        fields) {
  std::vector<ChannelIndex> channels;
  channels.reserve(fields.size());
  for (const auto& field : fields) {
    auto channel = type->getChildIdx(field->name());
    channels.emplace_back(channel);
  }
  return channels;
}
} // namespace

std::shared_ptr<const velox::core::PlanNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::ExchangeNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  VELOX_USER_CHECK(
      node->scope == protocol::ExchangeNodeScope::LOCAL,
      "Unsupported ExchangeNode scope");

  if (node->orderingScheme) {
    VELOX_USER_CHECK_EQ(
        node->sources.size(),
        1,
        "ExchangeNode with ordering scheme and multiple sources is not supported yet");

    std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys;
    std::vector<SortOrder> sortingOrders;
    sortingKeys.reserve(node->orderingScheme->orderBy.size());
    sortingOrders.reserve(node->orderingScheme->orderBy.size());
    for (const auto& orderBy : node->orderingScheme->orderBy) {
      sortingKeys.emplace_back(exprConverter_.toVeloxExpr(orderBy.variable));
      sortingOrders.emplace_back(toVeloxSortOrder(orderBy.sortOrder));
    }
    std::vector<std::shared_ptr<const PlanNode>> sources = {
        toVeloxQueryPlan(node->sources[0], tableWriteInfo, taskId)};
    return std::make_shared<velox::core::LocalMergeNode>(
        node->id, sortingKeys, sortingOrders, std::move(sources));
  }

  if (isHashPartition(node)) {
    std::vector<std::shared_ptr<const PlanNode>> sourceNodes;
    sourceNodes.reserve(node->sources.size());
    for (const auto& source : node->sources) {
      sourceNodes.emplace_back(
          toVeloxQueryPlan(source, tableWriteInfo, taskId));
    }

    auto partitionKeys = toFieldExprs(
        node->partitioningScheme.partitioning.arguments, exprConverter_);

    auto inputType = sourceNodes[0]->outputType();
    auto keyChannels = toChannels(inputType, partitionKeys);
    auto partitionFunctionFactory = [inputType,
                                     keyChannels](auto numPartitions) {
      return std::make_unique<velox::exec::HashPartitionFunction>(
          numPartitions, inputType, keyChannels);
    };

    return std::make_shared<velox::core::LocalPartitionNode>(
        node->id,
        partitionFunctionFactory,
        toRowType(node->partitioningScheme.outputLayout),
        std::move(sourceNodes));
  }

  VELOX_USER_CHECK_EQ(
      node->sources.size(),
      1,
      "ExchangeNode with multiple sources is not supported yet");

  return toVeloxQueryPlan(node->sources[0], tableWriteInfo, taskId);
}

namespace {
bool equal(
    const std::shared_ptr<protocol::RowExpression>& actual,
    const protocol::VariableReferenceExpression& expected) {
  if (auto variableReference =
          std::dynamic_pointer_cast<protocol::VariableReferenceExpression>(
              actual)) {
    return (
        variableReference->name == expected.name &&
        variableReference->type == expected.type);
  }
  return false;
}

std::shared_ptr<protocol::CallExpression> isFunctionCall(
    const std::shared_ptr<protocol::RowExpression>& expression,
    const std::string_view& functionName) {
  if (auto call =
          std::dynamic_pointer_cast<protocol::CallExpression>(expression)) {
    if (auto builtin =
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

/// Check if input RowExpression is a 'NOT x' expression and returns it as
/// CallExpression. Returns nullptr if input expression is something else.
std::shared_ptr<protocol::CallExpression> isNot(
    const std::shared_ptr<protocol::RowExpression>& expression) {
  static const std::string_view kNot = "presto.default.not";
  return isFunctionCall(expression, kNot);
}

/// Check if input RowExpression is an 'a > b' expression and returns it as
/// CallExpression. Returns nullptr if input expression is something else.
std::shared_ptr<protocol::CallExpression> isGreaterThan(
    const std::shared_ptr<protocol::RowExpression>& expression) {
  static const std::string_view kGreaterThan =
      "presto.default.$operator$greater_than";
  return isFunctionCall(expression, kGreaterThan);
}

template <typename T>
std::string toJsonString(const T& value) {
  return ((json)value).dump();
}

/// Checks if input PlanNode represents a local exchange with single source and
/// returns it as ExchangeNode. Returns nullptr if input node is something else.
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

/// Checks if input PlanNode represents an identity projection and returns it as
/// ProjectNode. Returns nullptr if input node is something else.
std::shared_ptr<const protocol::ProjectNode> isIdentityProjection(
    const std::shared_ptr<const protocol::PlanNode>& node) {
  if (auto project =
          std::dynamic_pointer_cast<const protocol::ProjectNode>(node)) {
    for (auto entry : project->assignments.assignments) {
      if (!equal(entry.second, entry.first)) {
        return nullptr;
      }
    }
    return project;
  }

  return nullptr;
}
} // namespace

std::shared_ptr<const velox::core::PlanNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
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
  if (auto semiJoin = std::dynamic_pointer_cast<const protocol::SemiJoinNode>(
          node->source)) {
    std::optional<velox::core::JoinType> joinType = std::nullopt;
    if (equal(node->predicate, semiJoin->semiJoinOutput)) {
      joinType = JoinType::kSemi;
    } else if (auto notCall = isNot(node->predicate)) {
      if (equal(notCall->arguments[0], semiJoin->semiJoinOutput)) {
        joinType = JoinType::kAnti;
      }
    }

    if (!joinType.has_value()) {
      VELOX_UNSUPPORTED(
          "Unsupported Filter over SemiJoin: {}",
          toJsonString(node->predicate));
    }

    std::vector<std::shared_ptr<const FieldAccessTypedExpr>> leftKeys = {
        exprConverter_.toVeloxExpr(semiJoin->sourceJoinVariable)};
    std::vector<std::shared_ptr<const FieldAccessTypedExpr>> rightKeys = {
        exprConverter_.toVeloxExpr(semiJoin->filteringSourceJoinVariable)};

    auto left = toVeloxQueryPlan(semiJoin->source, tableWriteInfo, taskId);
    auto right =
        toVeloxQueryPlan(semiJoin->filteringSource, tableWriteInfo, taskId);

    const auto& leftNames = left->outputType()->names();
    const auto& leftTypes = left->outputType()->children();

    std::vector<std::string> names;
    names.reserve(leftNames.size() + 1);
    std::copy(leftNames.begin(), leftNames.end(), std::back_inserter(names));
    names.emplace_back(semiJoin->semiJoinOutput.name);

    std::vector<std::shared_ptr<const ITypedExpr>> projections;
    projections.reserve(leftNames.size() + 1);
    for (auto i = 0; i < leftNames.size(); i++) {
      projections.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(
          leftTypes[i], leftNames[i]));
    }
    const bool constantValue = joinType.value() == JoinType::kSemi;
    projections.emplace_back(
        std::make_shared<core::ConstantTypedExpr>(constantValue));

    return std::make_shared<velox::core::ProjectNode>(
        node->id,
        std::move(names),
        std::move(projections),
        std::make_shared<velox::core::HashJoinNode>(
            semiJoin->id,
            joinType.value(),
            leftKeys,
            rightKeys,
            nullptr, // filter
            left,
            right,
            left->outputType()));
  }

  return std::make_shared<velox::core::FilterNode>(
      node->id,
      exprConverter_.toVeloxExpr(node->predicate),
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const velox::core::PlanNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::OutputNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  return PartitionedOutputNode::single(
      node->id,
      toRowType(node->outputVariables),
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const velox::core::ProjectNode>
VeloxQueryPlanConverter::tryConvertOffsetLimit(
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
            std::dynamic_pointer_cast<const ConstantTypedExpr>(offsetExpr)) {
      if (!offsetConstExpr->type()->isBigint()) {
        return nullptr;
      }

      auto offset = offsetConstExpr->value().value<int64_t>();

      // Check that Project node drops row_number column.
      for (auto entry : node->assignments.assignments) {
        if (equal(entry.second, rowNumberVariable)) {
          return nullptr;
        }
      }

      return std::make_shared<velox::core::ProjectNode>(
          node->id,
          getNames(node->assignments),
          getProjections(exprConverter_, node->assignments),
          std::make_shared<velox::core::LimitNode>(
              limit->id,
              offset,
              limit->count,
              limit->step == protocol::LimitNodeStep::PARTIAL,
              toVeloxQueryPlan(rowNumber->source, tableWriteInfo, taskId)));
    }
  }

  return nullptr;
}

std::shared_ptr<const velox::core::ProjectNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::ProjectNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  if (auto limit = tryConvertOffsetLimit(node, tableWriteInfo, taskId)) {
    return limit;
  }

  return std::make_shared<velox::core::ProjectNode>(
      node->id,
      getNames(node->assignments),
      getProjections(exprConverter_, node->assignments),
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const velox::core::ValuesNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::ValuesNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& /* tableWriteInfo */,
    const protocol::TaskId& taskId) {
  auto rowType = toRowType(node->outputVariables);
  vector_size_t numRows = node->rows.size();
  auto numColumns = rowType->size();
  std::vector<velox::VectorPtr> vectors;
  vectors.reserve(numColumns);

  for (int i = 0; i < numColumns; ++i) {
    auto base = velox::BaseVector::create(rowType->childAt(i), numRows, pool_);
    vectors.emplace_back(base);
  }

  auto rowVector = std::make_shared<RowVector>(
      pool_, rowType, BufferPtr(), numRows, std::move(vectors), 0);

  for (int row = 0; row < numRows; ++row) {
    for (int column = 0; column < numColumns; ++column) {
      auto expr = exprConverter_.toVeloxExpr(node->rows[row][column]);

      if (auto constantExpr =
              std::dynamic_pointer_cast<const ConstantTypedExpr>(expr)) {
        if (!constantExpr->hasValueVector()) {
          setCellFromVariant(rowVector, row, column, constantExpr->value());
        } else {
          VELOX_UNSUPPORTED(
              "Values node with complex type values is not supported yet");
        }
      } else {
        VELOX_FAIL("Expected constant expression");
      }
    }
  }

  return std::make_shared<velox::core::ValuesNode>(
      node->id, std::vector<RowVectorPtr>{rowVector});
}

std::shared_ptr<const velox::core::ExchangeNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::RemoteSourceNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& /* tableWriteInfo */,
    const protocol::TaskId& taskId) {
  auto rowType = toRowType(node->outputVariables);
  if (node->orderingScheme) {
    std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys;
    std::vector<SortOrder> sortingOrders;
    sortingKeys.reserve(node->orderingScheme->orderBy.size());
    sortingOrders.reserve(node->orderingScheme->orderBy.size());

    for (const auto& orderBy : node->orderingScheme->orderBy) {
      sortingKeys.emplace_back(exprConverter_.toVeloxExpr(orderBy.variable));
      sortingOrders.emplace_back(toVeloxSortOrder(orderBy.sortOrder));
    }
    return std::make_shared<velox::core::MergeExchangeNode>(
        node->id, rowType, sortingKeys, sortingOrders);
  }
  return std::make_shared<velox::core::ExchangeNode>(node->id, rowType);
}

std::shared_ptr<const velox::core::TableScanNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::TableScanNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& /* tableWriteInfo */,
    const protocol::TaskId& taskId) {
  auto rowType = toRowType(node->outputVariables);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (const auto& entry : node->assignments) {
    assignments.emplace(entry.first.name, toColumnHandle(entry.second));
  }
  return std::make_shared<velox::core::TableScanNode>(
      node->id,
      rowType,
      toConnectorTableHandle(node->table, exprConverter_),
      assignments);
}

std::vector<std::shared_ptr<const FieldAccessTypedExpr>>
VeloxQueryPlanConverter::toVeloxExprs(
    const std::vector<protocol::VariableReferenceExpression>& variables) {
  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> fields;
  fields.reserve(variables.size());
  for (const auto& variable : variables) {
    fields.emplace_back(exprConverter_.toVeloxExpr(variable));
  }
  return fields;
}

std::shared_ptr<const velox::core::AggregationNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::AggregationNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<std::string> aggregateNames;
  std::vector<std::shared_ptr<const CallTypedExpr>> aggregates;
  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> aggrMasks;
  aggregateNames.reserve(node->aggregations.size());
  aggregates.reserve(node->aggregations.size());
  aggrMasks.reserve(node->aggregations.size());
  for (const auto& entry : node->aggregations) {
    aggregateNames.emplace_back(entry.first.name);
    auto callExpr = std::dynamic_pointer_cast<const CallTypedExpr>(
        exprConverter_.toVeloxExpr(entry.second.call));

    // Drop "accuracy" parameter from approx_percentile call.
    // TODO Remove this once Velox adds support for "accuracy".
    if (callExpr->name() == "approx_percentile") {
      const auto& inputs = callExpr->inputs();

      // Here are all possible signatures:
      //      approx_percentile(x, percentage)
      //      approx_percentile(x, percentage, accuracy)
      //      approx_percentile(x, percentages)
      //      approx_percentile(x, percentages, accuracy)
      //      approx_percentile(x, w, percentage)
      //      approx_percentile(x, w, percentage, accuracy)
      //      approx_percentile(x, w, percentages)
      //      approx_percentile(x, w, percentages, accuracy)
      // "accuracy" is always the last parameter. All 4-parameter signatures
      // have "accuracy" parameter. Among 3-parameter signatures, only
      // signatures without the weight parameter have accuracy parameter. We
      // check if 2-nd argument is of type BIGINT to detect the "weight"
      // parameter.
      if (inputs.size() == 4 ||
          (inputs.size() == 3 && inputs[1]->type() != BIGINT())) {
        std::vector<std::shared_ptr<const ITypedExpr>> newInputs;
        newInputs.reserve(inputs.size() - 1);
        for (auto i = 0; i < inputs.size() - 1; ++i) {
          newInputs.push_back(callExpr->inputs()[i]);
        }
        callExpr = std::make_shared<CallTypedExpr>(
            callExpr->type(), std::move(newInputs), callExpr->name());
      }
    }
    aggregates.emplace_back(std::move(callExpr));
    if (entry.second.mask == nullptr) {
      aggrMasks.emplace_back(nullptr);
    } else {
      aggrMasks.emplace_back(exprConverter_.toVeloxExpr(entry.second.mask));
    }
  }

  velox::core::AggregationNode::Step step;
  switch (node->step) {
    case protocol::AggregationNodeStep::PARTIAL:
      step = velox::core::AggregationNode::Step::kPartial;
      break;
    case protocol::AggregationNodeStep::FINAL:
      step = velox::core::AggregationNode::Step::kFinal;
      break;
    case protocol::AggregationNodeStep::INTERMEDIATE:
      step = velox::core::AggregationNode::Step::kIntermediate;
      break;
    case protocol::AggregationNodeStep::SINGLE:
      step = velox::core::AggregationNode::Step::kSingle;
      break;
    default:
      VELOX_UNSUPPORTED("Unsupported aggregation step");
  }

  bool streamable = !node->preGroupedVariables.empty() &&
      node->groupingSets.groupingSetCount == 1 &&
      node->groupingSets.globalGroupingSets.empty();

  return std::make_shared<velox::core::AggregationNode>(
      node->id,
      step,
      toVeloxExprs(node->groupingSets.groupingKeys),
      streamable ? toVeloxExprs(node->preGroupedVariables)
                 : std::vector<std::shared_ptr<const FieldAccessTypedExpr>>{},
      aggregateNames,
      aggregates,
      aggrMasks,
      false, // ignoreNullKeys
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const core::PlanNode> VeloxQueryPlanConverter::toVeloxQueryPlan(
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
          std::vector<std::shared_ptr<const FieldAccessTypedExpr>>{},
          std::vector<std::string>{}, // aggregateNames
          std::vector<std::shared_ptr<const CallTypedExpr>>{}, // aggregates
          std::vector<
              std::shared_ptr<const FieldAccessTypedExpr>>{}, // aggrMasks
          false, // ignoreNullKeys
          toVeloxQueryPlan(node->source, tableWriteInfo, taskId)));
}

namespace {
velox::core::JoinType toJoinType(protocol::JoinNodeType type) {
  switch (type) {
    case protocol::JoinNodeType::INNER:
      return velox::core::JoinType::kInner;
    case protocol::JoinNodeType::LEFT:
      return velox::core::JoinType::kLeft;
    case protocol::JoinNodeType::RIGHT:
      return velox::core::JoinType::kRight;
    case protocol::JoinNodeType::FULL:
      return velox::core::JoinType::kFull;
  }

  VELOX_UNSUPPORTED("Unknown join type");
}
} // namespace

std::shared_ptr<const velox::core::PlanNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::JoinNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto joinType = toJoinType(node->type);

  if (node->criteria.empty() && velox::core::isInnerJoin(joinType) &&
      !node->filter) {
    return std::make_shared<velox::core::CrossJoinNode>(
        node->id,
        toVeloxQueryPlan(node->left, tableWriteInfo, taskId),
        toVeloxQueryPlan(node->right, tableWriteInfo, taskId),
        toRowType(node->outputVariables));
  }

  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> leftKeys;
  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> rightKeys;

  leftKeys.reserve(node->criteria.size());
  rightKeys.reserve(node->criteria.size());
  for (const auto& clause : node->criteria) {
    leftKeys.emplace_back(exprConverter_.toVeloxExpr(clause.left));
    rightKeys.emplace_back(exprConverter_.toVeloxExpr(clause.right));
  }

  return std::make_shared<velox::core::HashJoinNode>(
      node->id,
      joinType,
      leftKeys,
      rightKeys,
      node->filter ? exprConverter_.toVeloxExpr(*node->filter) : nullptr,
      toVeloxQueryPlan(node->left, tableWriteInfo, taskId),
      toVeloxQueryPlan(node->right, tableWriteInfo, taskId),
      toRowType(node->outputVariables));
}

std::shared_ptr<const velox::core::TopNNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::TopNNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys;
  std::vector<SortOrder> sortingOrders;
  sortingKeys.reserve(node->orderingScheme.orderBy.size());
  sortingOrders.reserve(node->orderingScheme.orderBy.size());
  for (const auto& orderBy : node->orderingScheme.orderBy) {
    sortingKeys.emplace_back(exprConverter_.toVeloxExpr(orderBy.variable));
    sortingOrders.emplace_back(toVeloxSortOrder(orderBy.sortOrder));
  }
  return std::make_shared<velox::core::TopNNode>(
      node->id,
      sortingKeys,
      sortingOrders,
      node->count,
      node->step == protocol::Step::PARTIAL,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const velox::core::LimitNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::LimitNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  return std::make_shared<velox::core::LimitNode>(
      node->id,
      0,
      node->count,
      node->step == protocol::LimitNodeStep::PARTIAL,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const velox::core::OrderByNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::SortNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys;
  std::vector<SortOrder> sortingOrders;
  for (const auto& orderBy : node->orderingScheme.orderBy) {
    sortingKeys.emplace_back(exprConverter_.toVeloxExpr(orderBy.variable));
    sortingOrders.emplace_back(toVeloxSortOrder(orderBy.sortOrder));
  }

  return std::make_shared<velox::core::OrderByNode>(
      node->id,
      sortingKeys,
      sortingOrders,
      node->isPartial,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const velox::core::TableWriteNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::TableWriterNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto outputTableHandle = std::dynamic_pointer_cast<protocol::CreateHandle>(
                               tableWriteInfo->writerTarget)
                               ->handle;

  auto hiveOutputTableHandle =
      std::dynamic_pointer_cast<protocol::HiveOutputTableHandle>(
          outputTableHandle.connectorHandle);

  auto uuid = boost::uuids::random_generator()();
  auto fileName = boost::uuids::to_string(uuid);

  auto filePath =
      fs::path(hiveOutputTableHandle->locationHandle.writePath) / fileName;

  auto hiveTableHandle =
      std::make_shared<velox::connector::hive::HiveInsertTableHandle>(filePath);
  auto insertTableHandle = std::make_shared<velox::core::InsertTableHandle>(
      outputTableHandle.connectorId, hiveTableHandle);

  auto outputType = toRowType(
      {node->rowCountVariable,
       node->fragmentVariable,
       node->tableCommitContextVariable});

  return std::make_shared<velox::core::TableWriteNode>(
      node->id,
      toRowType(node->columns),
      node->columnNames,
      insertTableHandle,
      outputType,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const velox::core::UnnestNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::UnnestNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  std::vector<std::shared_ptr<const FieldAccessTypedExpr>> unnestFields;
  unnestFields.reserve(node->unnestVariables.size());
  std::vector<std::string> unnestNames;
  for (const auto& entry : node->unnestVariables) {
    unnestFields.emplace_back(exprConverter_.toVeloxExpr(entry.first));
    for (const auto& output : entry.second) {
      unnestNames.emplace_back(output.name);
    }
  }

  return std::make_shared<velox::core::UnnestNode>(
      node->id,
      toVeloxExprs(node->replicateVariables),
      unnestFields,
      unnestNames,
      node->ordinalityVariable ? std::optional{node->ordinalityVariable->name}
                               : std::nullopt,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const velox::core::EnforceSingleRowNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::EnforceSingleRowNode>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  return std::make_shared<velox::core::EnforceSingleRowNode>(
      node->id, toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const velox::core::AssignUniqueIdNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
    const std::shared_ptr<const protocol::AssignUniqueId>& node,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  auto prestoTaskId = PrestoTaskId(taskId);
  // `taskUniqueId` is an integer to uniquely identify the generated id
  // across all the nodes executing the same query stage in a distributed
  // query execution.
  //
  // 10 bit for stageId && 14-bit for taskId should be sufficient
  // given the max stage per query is 100 by default.

  // taskUniqueId = last 10 bit of stageId | last 14 bits of taskId
  int32_t taskUniqueId = (prestoTaskId.stageId() & ((1 << 10) - 1)) << 14 |
      (prestoTaskId.id() & ((1 << 14) - 1));
  return std::make_shared<velox::core::AssignUniqueIdNode>(
      node->id,
      node->idVariable.name,
      taskUniqueId,
      toVeloxQueryPlan(node->source, tableWriteInfo, taskId));
}

std::shared_ptr<const velox::core::PlanNode>
VeloxQueryPlanConverter::toVeloxQueryPlan(
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
  if (auto distinctLimit =
          std::dynamic_pointer_cast<const protocol::DistinctLimitNode>(node)) {
    return toVeloxQueryPlan(distinctLimit, tableWriteInfo, taskId);
  }
  if (auto join = std::dynamic_pointer_cast<const protocol::JoinNode>(node)) {
    return toVeloxQueryPlan(join, tableWriteInfo, taskId);
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
  if (auto assignUniqueId =
          std::dynamic_pointer_cast<const protocol::AssignUniqueId>(node)) {
    return toVeloxQueryPlan(assignUniqueId, tableWriteInfo, taskId);
  }
  VELOX_UNSUPPORTED("Unknown plan node type {}", node->_type);
}

namespace {
velox::core::ExecutionStrategy toStrategy(
    protocol::StageExecutionStrategy strategy) {
  switch (strategy) {
    case protocol::StageExecutionStrategy::UNGROUPED_EXECUTION:
      return velox::core::ExecutionStrategy::kUngrouped;

    case protocol::StageExecutionStrategy::
        FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION:
      VELOX_UNSUPPORTED(
          "FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION "
          "Stage Execution Strategy is not supported");

    case protocol::StageExecutionStrategy::
        DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION:
      return velox::core::ExecutionStrategy::kGrouped;

    case protocol::StageExecutionStrategy::RECOVERABLE_GROUPED_EXECUTION:
      VELOX_UNSUPPORTED(
          "RECOVERABLE_GROUPED_EXECUTION "
          "Stage Execution Strategy is not supported");
  }
  VELOX_UNSUPPORTED("Unknown Stage Execution Strategy type {}", (int)strategy);
}
} // namespace

velox::core::PlanFragment VeloxQueryPlanConverter::toVeloxQueryPlan(
    const protocol::PlanFragment& fragment,
    const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
    const protocol::TaskId& taskId) {
  velox::core::PlanFragment planFragment;

  // Convert the fragment info first.
  const auto& descriptor = fragment.stageExecutionDescriptor;
  planFragment.executionStrategy =
      toStrategy(fragment.stageExecutionDescriptor.stageExecutionStrategy);
  planFragment.numSplitGroups = descriptor.totalLifespans;

  if (auto output = std::dynamic_pointer_cast<const protocol::OutputNode>(
          fragment.root)) {
    planFragment.planNode = toVeloxQueryPlan(output, tableWriteInfo, taskId);
    return planFragment;
  }

  auto partitioningScheme = fragment.partitioningScheme;
  auto partitioningHandle =
      partitioningScheme.partitioning.handle.connectorHandle;

  auto partitioningKeys =
      toFieldExprs(partitioningScheme.partitioning.arguments, exprConverter_);

  auto sourceNode = toVeloxQueryPlan(fragment.root, tableWriteInfo, taskId);
  auto inputType = sourceNode->outputType();

  auto keyChannels = toChannels(inputType, partitioningKeys);
  auto outputType = toRowType(partitioningScheme.outputLayout);

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
        planFragment.planNode =
            PartitionedOutputNode::single("root", outputType, sourceNode);
        return planFragment;
      case protocol::SystemPartitioning::FIXED: {
        switch (systemPartitioningHandle->function) {
          case protocol::SystemPartitionFunction::ROUND_ROBIN: {
            auto numPartitions = partitioningScheme.bucketToPartition->size();

            if (numPartitions == 1) {
              planFragment.planNode =
                  PartitionedOutputNode::single("root", outputType, sourceNode);
              return planFragment;
            }

            auto partitionFunctionFactory = [](auto numPartitions) {
              return std::make_unique<velox::exec::RoundRobinPartitionFunction>(
                  numPartitions);
            };

            planFragment.planNode = std::make_shared<PartitionedOutputNode>(
                "root",
                partitioningKeys,
                numPartitions,
                false, // broadcast
                partitioningScheme.replicateNullsAndAny,
                partitionFunctionFactory,
                outputType,
                sourceNode);
            return planFragment;
          }
          case protocol::SystemPartitionFunction::HASH: {
            auto numPartitions = partitioningScheme.bucketToPartition->size();

            if (numPartitions == 1) {
              planFragment.planNode =
                  PartitionedOutputNode::single("root", outputType, sourceNode);
              return planFragment;
            }

            auto partitionFunctionFactory = [inputType,
                                             keyChannels](auto numPartitions) {
              return std::make_unique<velox::exec::HashPartitionFunction>(
                  numPartitions, inputType, keyChannels);
            };

            planFragment.planNode = std::make_shared<PartitionedOutputNode>(
                "root",
                partitioningKeys,
                numPartitions,
                false, // broadcast
                partitioningScheme.replicateNullsAndAny,
                partitionFunctionFactory,
                outputType,
                sourceNode);
            return planFragment;
          }
          case protocol::SystemPartitionFunction::BROADCAST: {
            planFragment.planNode = PartitionedOutputNode::broadcast(
                "root", 1, outputType, sourceNode);
            return planFragment;
          }
          default:
            VELOX_UNSUPPORTED(
                "Unsupported partitioning function: {}",
                toJsonString(systemPartitioningHandle->function));
        }
      }
      default:
        VELOX_FAIL("Unsupported kind of SystemPartitioning");
    }
  } else if (
      auto hivePartitioningHandle =
          std::dynamic_pointer_cast<protocol::HivePartitioningHandle>(
              partitioningHandle)) {
    const auto& bucketToPartition = *partitioningScheme.bucketToPartition;
    auto numPartitions =
        *std::max_element(bucketToPartition.begin(), bucketToPartition.end()) +
        1;

    if (numPartitions == 1) {
      planFragment.planNode =
          PartitionedOutputNode::single("root", outputType, sourceNode);
      return planFragment;
    }

    VELOX_USER_CHECK(
        hivePartitioningHandle->bucketFunctionType ==
            protocol::BucketFunctionType::HIVE_COMPATIBLE,
        "Unsupported Hive bucket function type: {}",
        toJsonString(hivePartitioningHandle->bucketFunctionType))

    auto partitionFunctionFactory = [numBuckets =
                                         hivePartitioningHandle->bucketCount,
                                     bucketToPartition,
                                     keyChannels](auto /* numPartitions */) {
      return std::make_unique<velox::connector::hive::HivePartitionFunction>(
          numBuckets, bucketToPartition, keyChannels);
    };

    planFragment.planNode = std::make_shared<PartitionedOutputNode>(
        "root",
        partitioningKeys,
        numPartitions,
        false, // broadcast
        partitioningScheme.replicateNullsAndAny,
        partitionFunctionFactory,
        toRowType(partitioningScheme.outputLayout),
        sourceNode);
    return planFragment;
  } else {
    VELOX_UNSUPPORTED(
        "Unsupported partitioning handle: {}",
        toJsonString(partitioningHandle));
  }
}
} // namespace facebook::presto
