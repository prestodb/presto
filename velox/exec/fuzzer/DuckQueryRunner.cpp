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
#include "velox/exec/fuzzer/DuckQueryRunner.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

namespace facebook::velox::exec::test {

namespace {

void appendComma(int32_t i, std::stringstream& sql) {
  if (i > 0) {
    sql << ", ";
  }
}

std::string toCallSql(const core::CallTypedExprPtr& call) {
  std::stringstream sql;
  sql << call->name() << "(";
  for (auto i = 0; i < call->inputs().size(); ++i) {
    appendComma(i, sql);
    sql << std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
               call->inputs()[i])
               ->name();
  }
  sql << ")";
  return sql.str();
}

std::string toAggregateCallSql(
    const core::CallTypedExprPtr& call,
    const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys,
    const std::vector<core::SortOrder>& sortingOrders,
    bool distinct) {
  std::stringstream sql;
  sql << call->name() << "(";

  if (distinct) {
    sql << "distinct ";
  }

  for (auto i = 0; i < call->inputs().size(); ++i) {
    appendComma(i, sql);
    sql << std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
               call->inputs()[i])
               ->name();
  }

  if (!sortingKeys.empty()) {
    sql << " order by ";
    for (auto i = 0; i < sortingKeys.size(); ++i) {
      appendComma(i, sql);
      sql << sortingKeys[i]->name() << " " << sortingOrders[i].toString();
    }
  }

  sql << ")";
  return sql.str();
}

bool isSupported(const TypePtr& type) {
  // DuckDB doesn't support nanosecond precision for timestamps.
  if (type->kind() == TypeKind::TIMESTAMP) {
    return false;
  }
  for (auto i = 0; i < type->size(); ++i) {
    if (!isSupported(type->childAt(i))) {
      return false;
    }
  }

  return true;
}

std::unordered_set<std::string> getAggregateFunctions() {
  std::string sql =
      "SELECT distinct on(function_name) function_name "
      "FROM duckdb_functions() "
      "WHERE function_type = 'aggregate'";

  DuckDbQueryRunner queryRunner;
  auto result = queryRunner.executeOrdered(sql, ROW({VARCHAR()}));

  std::unordered_set<std::string> names;
  for (const auto& row : result) {
    names.insert(row[0].value<std::string>());
  }

  return names;
}
} // namespace

DuckQueryRunner::DuckQueryRunner()
    : aggregateFunctionNames_{getAggregateFunctions()} {}

void DuckQueryRunner::disableAggregateFunctions(
    const std::vector<std::string>& names) {
  for (const auto& name : names) {
    aggregateFunctionNames_.erase(name);
  }
}

std::multiset<std::vector<velox::variant>> DuckQueryRunner::execute(
    const std::string& sql,
    const std::vector<RowVectorPtr>& input,
    const RowTypePtr& resultType) {
  DuckDbQueryRunner queryRunner;
  queryRunner.createTable("tmp", input);
  return queryRunner.execute(sql, resultType);
}

std::multiset<std::vector<velox::variant>> DuckQueryRunner::execute(
    const std::string& sql,
    const std::vector<RowVectorPtr>& probeInput,
    const std::vector<RowVectorPtr>& buildInput,
    const RowTypePtr& resultType) {
  DuckDbQueryRunner queryRunner;
  queryRunner.createTable("t", probeInput);
  queryRunner.createTable("u", buildInput);
  return queryRunner.execute(sql, resultType);
}

std::optional<std::string> DuckQueryRunner::toSql(
    const core::PlanNodePtr& plan) {
  if (!isSupported(plan->outputType())) {
    return std::nullopt;
  }

  for (const auto& source : plan->sources()) {
    if (!isSupported(source->outputType())) {
      return std::nullopt;
    }
  }

  if (const auto projectNode =
          std::dynamic_pointer_cast<const core::ProjectNode>(plan)) {
    return toSql(projectNode);
  }

  if (const auto windowNode =
          std::dynamic_pointer_cast<const core::WindowNode>(plan)) {
    return toSql(windowNode);
  }

  if (const auto aggregationNode =
          std::dynamic_pointer_cast<const core::AggregationNode>(plan)) {
    return toSql(aggregationNode);
  }

  if (const auto rowNumberNode =
          std::dynamic_pointer_cast<const core::RowNumberNode>(plan)) {
    return toSql(rowNumberNode);
  }

  if (const auto joinNode =
          std::dynamic_pointer_cast<const core::HashJoinNode>(plan)) {
    return toSql(joinNode);
  }

  if (const auto joinNode =
          std::dynamic_pointer_cast<const core::NestedLoopJoinNode>(plan)) {
    return toSql(joinNode);
  }

  VELOX_NYI();
}

namespace {
bool containsMap(const TypePtr& type) {
  if (type->isMap()) {
    return true;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (containsMap(type->childAt(i))) {
      return true;
    }
  }

  return false;
}
} // namespace

std::optional<std::string> DuckQueryRunner::toSql(
    const std::shared_ptr<const core::AggregationNode>& aggregationNode) {
  // Assume plan is Aggregation over Values.
  VELOX_CHECK(aggregationNode->step() == core::AggregationNode::Step::kSingle);

  for (const auto& agg : aggregationNode->aggregates()) {
    if (aggregateFunctionNames_.count(agg.call->name()) == 0) {
      return std::nullopt;
    }
  }

  std::vector<std::string> groupingKeys;
  for (const auto& key : aggregationNode->groupingKeys()) {
    // Aggregations with group by keys that contain maps are buggy.
    if (containsMap(key->type())) {
      return std::nullopt;
    }
    groupingKeys.push_back(key->name());
  }

  std::stringstream sql;
  sql << "SELECT " << folly::join(", ", groupingKeys);

  const auto& aggregates = aggregationNode->aggregates();
  if (!aggregates.empty()) {
    if (!groupingKeys.empty()) {
      sql << ", ";
    }

    for (auto i = 0; i < aggregates.size(); ++i) {
      appendComma(i, sql);
      const auto& aggregate = aggregates[i];
      sql << toAggregateCallSql(
          aggregate.call,
          aggregate.sortingKeys,
          aggregate.sortingOrders,
          aggregate.distinct);

      if (aggregate.mask != nullptr) {
        sql << " filter (where " << aggregate.mask->name() << ")";
      }
      sql << " as " << aggregationNode->aggregateNames()[i];
    }
  }

  sql << " FROM tmp";

  if (!groupingKeys.empty()) {
    sql << " GROUP BY " << folly::join(", ", groupingKeys);
  }

  return sql.str();
}

std::optional<std::string> DuckQueryRunner::toSql(
    const std::shared_ptr<const core::ProjectNode>& projectNode) {
  auto sourceSql = toSql(projectNode->sources()[0]);
  if (!sourceSql.has_value()) {
    return std::nullopt;
  }

  std::stringstream sql;
  sql << "SELECT ";

  for (auto i = 0; i < projectNode->names().size(); ++i) {
    appendComma(i, sql);
    auto projection = projectNode->projections()[i];
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                projection)) {
      sql << field->name();
    } else if (
        auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(projection)) {
      sql << toCallSql(call);
    } else {
      VELOX_NYI();
    }

    sql << " as " << projectNode->names()[i];
  }

  sql << " FROM (" << sourceSql.value() << ")";
  return sql.str();
}

std::optional<std::string> DuckQueryRunner::toSql(
    const std::shared_ptr<const core::WindowNode>& windowNode) {
  std::stringstream sql;
  sql << "SELECT ";

  const auto& inputType = windowNode->sources()[0]->outputType();
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, sql);
    sql << inputType->nameOf(i);
  }

  sql << ", ";

  const auto& functions = windowNode->windowFunctions();
  for (auto i = 0; i < functions.size(); ++i) {
    appendComma(i, sql);
    sql << toCallSql(functions[i].functionCall);
  }
  sql << " OVER (";

  const auto& partitionKeys = windowNode->partitionKeys();
  if (!partitionKeys.empty()) {
    sql << "partition by ";
    for (auto i = 0; i < partitionKeys.size(); ++i) {
      appendComma(i, sql);
      sql << partitionKeys[i]->name();
    }
  }

  const auto& sortingKeys = windowNode->sortingKeys();
  const auto& sortingOrders = windowNode->sortingOrders();

  if (!sortingKeys.empty()) {
    sql << " order by ";
    for (auto i = 0; i < sortingKeys.size(); ++i) {
      appendComma(i, sql);
      sql << sortingKeys[i]->name() << " " << sortingOrders[i].toString();
    }
  }

  sql << ") FROM tmp";

  return sql.str();
}

std::optional<std::string> DuckQueryRunner::toSql(
    const std::shared_ptr<const core::RowNumberNode>& rowNumberNode) {
  std::stringstream sql;
  sql << "SELECT ";

  const auto& inputType = rowNumberNode->sources()[0]->outputType();
  for (auto i = 0; i < inputType->size(); ++i) {
    appendComma(i, sql);
    sql << inputType->nameOf(i);
  }

  sql << ", row_number() OVER (";

  const auto& partitionKeys = rowNumberNode->partitionKeys();
  if (!partitionKeys.empty()) {
    sql << "partition by ";
    for (auto i = 0; i < partitionKeys.size(); ++i) {
      appendComma(i, sql);
      sql << partitionKeys[i]->name();
    }
  }

  sql << ") as row_number FROM tmp";

  return sql.str();
}

std::optional<std::string> DuckQueryRunner::toSql(
    const std::shared_ptr<const core::HashJoinNode>& joinNode) {
  const auto& joinKeysToSql = [](auto keys) {
    std::stringstream out;
    for (auto i = 0; i < keys.size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << keys[i]->name();
    }
    return out.str();
  };

  const auto& equiClausesToSql = [](auto joinNode) {
    std::stringstream out;
    for (auto i = 0; i < joinNode->leftKeys().size(); ++i) {
      if (i > 0) {
        out << " AND ";
      }
      out << joinNode->leftKeys()[i]->name() << " = "
          << joinNode->rightKeys()[i]->name();
    }
    return out.str();
  };

  const auto& outputNames = joinNode->outputType()->names();

  std::stringstream sql;
  if (joinNode->isLeftSemiProjectJoin()) {
    sql << "SELECT "
        << folly::join(", ", outputNames.begin(), --outputNames.end());
  } else {
    sql << "SELECT " << folly::join(", ", outputNames);
  }

  switch (joinNode->joinType()) {
    case core::JoinType::kInner:
      sql << " FROM t INNER JOIN u ON " << equiClausesToSql(joinNode);
      break;
    case core::JoinType::kLeft:
      sql << " FROM t LEFT JOIN u ON " << equiClausesToSql(joinNode);
      break;
    case core::JoinType::kFull:
      sql << " FROM t FULL OUTER JOIN u ON " << equiClausesToSql(joinNode);
      break;
    case core::JoinType::kLeftSemiFilter:
      if (joinNode->leftKeys().size() > 1) {
        return std::nullopt;
      }
      sql << " FROM t WHERE " << joinKeysToSql(joinNode->leftKeys())
          << " IN (SELECT " << joinKeysToSql(joinNode->rightKeys())
          << " FROM u)";
      break;
    case core::JoinType::kLeftSemiProject:
      if (joinNode->isNullAware()) {
        sql << ", " << joinKeysToSql(joinNode->leftKeys()) << " IN (SELECT "
            << joinKeysToSql(joinNode->rightKeys()) << " FROM u) FROM t";
      } else {
        sql << ", EXISTS (SELECT * FROM u WHERE " << equiClausesToSql(joinNode)
            << ") FROM t";
      }
      break;
    case core::JoinType::kAnti:
      if (joinNode->isNullAware()) {
        sql << " FROM t WHERE " << joinKeysToSql(joinNode->leftKeys())
            << " NOT IN (SELECT " << joinKeysToSql(joinNode->rightKeys())
            << " FROM u)";
      } else {
        sql << " FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE "
            << equiClausesToSql(joinNode) << ")";
      }
      break;
    default:
      VELOX_UNREACHABLE(
          "Unknown join type: {}", static_cast<int>(joinNode->joinType()));
  }

  return sql.str();
}

std::optional<std::string> DuckQueryRunner::toSql(
    const std::shared_ptr<const core::NestedLoopJoinNode>& joinNode) {
  const auto& joinKeysToSql = [](auto keys) {
    std::stringstream out;
    for (auto i = 0; i < keys.size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << keys[i]->name();
    }
    return out.str();
  };

  const auto& outputNames = joinNode->outputType()->names();
  std::stringstream sql;

  // Nested loop join without filter.
  VELOX_CHECK(
      joinNode->joinCondition() == nullptr,
      "This code path should be called only for nested loop join without filter");
  const std::string joinCondition{"(1 = 1)"};
  switch (joinNode->joinType()) {
    case core::JoinType::kInner:
      sql << " FROM t INNER JOIN u ON " << joinCondition;
      break;
    case core::JoinType::kLeft:
      sql << " FROM t LEFT JOIN u ON " << joinCondition;
      break;
    case core::JoinType::kFull:
      sql << " FROM t FULL OUTER JOIN u ON " << joinCondition;
      break;
    default:
      VELOX_UNREACHABLE(
          "Unknown join type: {}", static_cast<int>(joinNode->joinType()));
  }

  return sql.str();
}
} // namespace facebook::velox::exec::test
