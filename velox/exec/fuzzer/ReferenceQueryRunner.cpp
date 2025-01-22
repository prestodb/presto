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
#include <optional>
#include <set>
#include <unordered_map>

#include "velox/core/PlanNode.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/exec/fuzzer/ToSQLUtil.h"

namespace facebook::velox::exec::test {

namespace {

std::string joinKeysToSql(
    const std::vector<core::FieldAccessTypedExprPtr>& keys) {
  std::vector<std::string> keyNames;
  keyNames.reserve(keys.size());
  for (const core::FieldAccessTypedExprPtr& key : keys) {
    keyNames.push_back(key->name());
  }
  return folly::join(", ", keyNames);
}

std::string filterToSql(const core::TypedExprPtr& filter) {
  auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(filter);
  return toCallSql(call);
}

std::string joinConditionAsSql(const core::AbstractJoinNode& joinNode) {
  std::stringstream out;
  for (auto i = 0; i < joinNode.leftKeys().size(); ++i) {
    if (i > 0) {
      out << " AND ";
    }
    out << joinNode.leftKeys()[i]->name() << " = "
        << joinNode.rightKeys()[i]->name();
  }
  if (joinNode.filter()) {
    if (!joinNode.leftKeys().empty()) {
      out << " AND ";
    }
    out << filterToSql(joinNode.filter());
  }
  return out.str();
}

} // namespace

bool ReferenceQueryRunner::isSupportedDwrfType(const TypePtr& type) {
  if (type->isDate() || type->isIntervalDayTime() || type->isUnKnown()) {
    return false;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (!isSupportedDwrfType(type->childAt(i))) {
      return false;
    }
  }

  return true;
}

std::unordered_map<std::string, std::vector<velox::RowVectorPtr>>
ReferenceQueryRunner::getAllTables(const core::PlanNodePtr& plan) {
  std::unordered_map<std::string, std::vector<velox::RowVectorPtr>> result;
  if (const auto valuesNode =
          std::dynamic_pointer_cast<const core::ValuesNode>(plan)) {
    result.insert({getTableName(valuesNode), valuesNode->values()});
  } else {
    for (const auto& source : plan->sources()) {
      auto tablesAndNames = getAllTables(source);
      result.insert(tablesAndNames.begin(), tablesAndNames.end());
    }
  }
  return result;
}

std::optional<std::string> ReferenceQueryRunner::joinSourceToSql(
    const core::PlanNodePtr& planNode) {
  const std::optional<std::string> subQuery = toSql(planNode);
  if (subQuery) {
    return subQuery->find(" ") != std::string::npos
        ? fmt::format("({})", *subQuery)
        : *subQuery;
  }
  return std::nullopt;
}

std::optional<std::string> ReferenceQueryRunner::toSql(
    const core::ValuesNodePtr& valuesNode) {
  if (!isSupportedDwrfType(valuesNode->outputType())) {
    return std::nullopt;
  }
  return getTableName(valuesNode);
}

std::optional<std::string> ReferenceQueryRunner::toSql(
    const core::TableScanNodePtr& tableScanNode) {
  return tableScanNode->tableHandle()->name();
}

std::optional<std::string> ReferenceQueryRunner::toSql(
    const std::shared_ptr<const core::HashJoinNode>& joinNode) {
  if (!isSupportedDwrfType(joinNode->sources()[0]->outputType()) ||
      !isSupportedDwrfType(joinNode->sources()[1]->outputType())) {
    return std::nullopt;
  }

  std::optional<std::string> probeTableName =
      joinSourceToSql(joinNode->sources()[0]);
  std::optional<std::string> buildTableName =
      joinSourceToSql(joinNode->sources()[1]);
  if (!probeTableName || !buildTableName) {
    return std::nullopt;
  }

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
      sql << " FROM " << *probeTableName << " INNER JOIN " << *buildTableName
          << " ON " << joinConditionAsSql(*joinNode);
      break;
    case core::JoinType::kLeft:
      sql << " FROM " << *probeTableName << " LEFT JOIN " << *buildTableName
          << " ON " << joinConditionAsSql(*joinNode);
      break;
    case core::JoinType::kFull:
      sql << " FROM " << *probeTableName << " FULL OUTER JOIN "
          << *buildTableName << " ON " << joinConditionAsSql(*joinNode);
      break;
    case core::JoinType::kLeftSemiFilter:
      // Multiple columns returned by a scalar subquery is not supported. A
      // scalar subquery expression is a subquery that returns one result row
      // from exactly one column for every input row.
      if (joinNode->leftKeys().size() > 1) {
        return std::nullopt;
      }
      sql << " FROM " << *probeTableName << " WHERE "
          << joinKeysToSql(joinNode->leftKeys()) << " IN (SELECT "
          << joinKeysToSql(joinNode->rightKeys()) << " FROM "
          << *buildTableName;
      if (joinNode->filter()) {
        sql << " WHERE " << filterToSql(joinNode->filter());
      }
      sql << ")";
      break;
    case core::JoinType::kLeftSemiProject:
      if (joinNode->isNullAware()) {
        sql << ", " << joinKeysToSql(joinNode->leftKeys()) << " IN (SELECT "
            << joinKeysToSql(joinNode->rightKeys()) << " FROM "
            << *buildTableName;
        if (joinNode->filter()) {
          sql << " WHERE " << filterToSql(joinNode->filter());
        }
        sql << ") FROM " << *probeTableName;
      } else {
        sql << ", EXISTS (SELECT * FROM " << *buildTableName << " WHERE "
            << joinConditionAsSql(*joinNode);
        sql << ") FROM " << *probeTableName;
      }
      break;
    case core::JoinType::kAnti:
      if (joinNode->isNullAware()) {
        sql << " FROM " << *probeTableName << " WHERE "
            << joinKeysToSql(joinNode->leftKeys()) << " NOT IN (SELECT "
            << joinKeysToSql(joinNode->rightKeys()) << " FROM "
            << *buildTableName;
        if (joinNode->filter()) {
          sql << " WHERE " << filterToSql(joinNode->filter());
        }
        sql << ")";
      } else {
        sql << " FROM " << *probeTableName
            << " WHERE NOT EXISTS (SELECT * FROM " << *buildTableName
            << " WHERE " << joinConditionAsSql(*joinNode);
        sql << ")";
      }
      break;
    default:
      VELOX_UNREACHABLE(
          "Unknown join type: {}", static_cast<int>(joinNode->joinType()));
  }
  return sql.str();
}

std::optional<std::string> ReferenceQueryRunner::toSql(
    const std::shared_ptr<const core::NestedLoopJoinNode>& joinNode) {
  std::optional<std::string> probeTableName =
      joinSourceToSql(joinNode->sources()[0]);
  std::optional<std::string> buildTableName =
      joinSourceToSql(joinNode->sources()[1]);
  if (!probeTableName || !buildTableName) {
    return std::nullopt;
  }

  std::stringstream sql;
  sql << "SELECT " << folly::join(", ", joinNode->outputType()->names());

  // Nested loop join without filter.
  VELOX_CHECK_NULL(
      joinNode->joinCondition(),
      "This code path should be called only for nested loop join without filter");
  const std::string joinCondition{"(1 = 1)"};
  switch (joinNode->joinType()) {
    case core::JoinType::kInner:
      sql << " FROM " << *probeTableName << " INNER JOIN " << *buildTableName
          << " ON " << joinCondition;
      break;
    case core::JoinType::kLeft:
      sql << " FROM " << *probeTableName << " LEFT JOIN " << *buildTableName
          << " ON " << joinCondition;
      break;
    case core::JoinType::kFull:
      sql << " FROM " << *probeTableName << " FULL OUTER JOIN "
          << *buildTableName << " ON " << joinCondition;
      break;
    default:
      VELOX_UNREACHABLE(
          "Unknown join type: {}", static_cast<int>(joinNode->joinType()));
  }
  return sql.str();
}

} // namespace facebook::velox::exec::test
