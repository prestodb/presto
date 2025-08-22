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

#include "velox/exec/fuzzer/PrestoSql.h"

#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/vector/SimpleVector.h"

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

std::optional<std::string> toJoinSourceSql(std::optional<std::string>&& sql) {
  return sql.has_value() && sql->find(" ") != std::string::npos
      ? fmt::format("({})", *sql)
      : sql;
}

} // namespace

void appendComma(int32_t i, std::stringstream& sql) {
  if (i > 0) {
    sql << ", ";
  }
}

// Returns the SQL string of the given type.
std::string toTypeSql(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::ARRAY:
      return fmt::format("ARRAY({})", toTypeSql(type->childAt(0)));
    case TypeKind::MAP:
      return fmt::format(
          "MAP({}, {})",
          toTypeSql(type->childAt(0)),
          toTypeSql(type->childAt(1)));
    case TypeKind::ROW: {
      const auto& rowType = type->asRow();
      std::stringstream sql;
      sql << "ROW(";
      for (auto i = 0; i < type->size(); ++i) {
        appendComma(i, sql);
        // TODO Field names may need to be quoted.
        sql << rowType.nameOf(i) << " " << toTypeSql(type->childAt(i));
      }
      sql << ")";
      return sql.str();
    }
    default:
      if (type->isPrimitiveType()) {
        return type->toString();
      }
      VELOX_UNSUPPORTED("Type is not supported: {}", type->toString());
  }
}

std::string toLambdaSql(const core::LambdaTypedExprPtr& lambda) {
  std::stringstream sql;
  const auto& signature = lambda->signature();

  sql << "(";
  for (auto j = 0; j < signature->size(); ++j) {
    appendComma(j, sql);
    sql << signature->nameOf(j);
  }

  sql << ") -> ";
  toCallInputsSql({lambda->body()}, sql);
  return sql.str();
}

namespace {
std::string toDereferenceSql(const core::DereferenceTypedExpr& dereference) {
  std::stringstream sql;
  toCallInputsSql(dereference.inputs(), sql);
  sql << "." << dereference.name();
  return sql.str();
}
} // namespace

void toCallInputsSql(
    const std::vector<core::TypedExprPtr>& inputs,
    std::stringstream& sql) {
  for (auto i = 0; i < inputs.size(); ++i) {
    appendComma(i, sql);

    const auto& input = inputs.at(i);
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                input)) {
      if (field->isInputColumn()) {
        sql << field->name();
      } else {
        toCallInputsSql(field->inputs(), sql);
        sql << fmt::format(".{}", field->name());
      }
    } else if (
        auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(input)) {
      sql << toCallSql(call);
    } else if (
        auto lambda =
            std::dynamic_pointer_cast<const core::LambdaTypedExpr>(input)) {
      sql << toLambdaSql(lambda);
    } else if (
        auto constantArg =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(input)) {
      sql << toConstantSql(*constantArg);
    } else if (
        auto castArg =
            std::dynamic_pointer_cast<const core::CastTypedExpr>(input)) {
      sql << toCastSql(*castArg);
    } else if (
        auto concatArg =
            std::dynamic_pointer_cast<const core::ConcatTypedExpr>(input)) {
      sql << toConcatSql(*concatArg);
    } else if (
        auto dereferenceArg =
            std::dynamic_pointer_cast<const core::DereferenceTypedExpr>(
                input)) {
      sql << toDereferenceSql(*dereferenceArg);
    } else {
      VELOX_NYI("Unsupported input expression: {}.", input->toString());
    }
  }
}

// Returns a mapping from Velox function names to the corresponding unary
// operators supported in Presto SQL.
const std::unordered_map<std::string, std::string>& unaryOperatorMap() {
  static std::unordered_map<std::string, std::string> unaryOperatorMap{
      {"negate", "-"},
      {"not", "not"},
  };
  return unaryOperatorMap;
}

// Returns a mapping from Velox function names to the corresponding binary
// operators supported in Presto SQL.
const std::unordered_map<std::string, std::string>& binaryOperatorMap() {
  static std::unordered_map<std::string, std::string> binaryOperatorMap{
      {"plus", "+"},
      {"subtract", "-"},
      {"minus", "-"},
      {"multiply", "*"},
      {"divide", "/"},
      {"eq", "="},
      {"neq", "<>"},
      {"lt", "<"},
      {"gt", ">"},
      {"lte", "<="},
      {"gte", ">="},
      {"distinct_from", "is distinct from"},
  };
  return binaryOperatorMap;
}

std::string toCallSql(const core::CallTypedExprPtr& call) {
  std::stringstream sql;
  // Some functions require special SQL syntax, so handle them first.
  const auto& unaryOperators = unaryOperatorMap();
  const auto& binaryOperators = binaryOperatorMap();
  if (unaryOperators.count(call->name()) > 0) {
    VELOX_CHECK_EQ(
        call->inputs().size(), 1, "Expected one argument to a unary operator");
    sql << "(";
    sql << fmt::format("{} ", unaryOperators.at(call->name()));
    toCallInputsSql({call->inputs()[0]}, sql);
    sql << ")";
  } else if (binaryOperators.count(call->name()) > 0) {
    VELOX_CHECK_EQ(
        call->inputs().size(),
        2,
        "Expected two arguments to a binary operator");
    sql << "(";
    toCallInputsSql({call->inputs()[0]}, sql);
    sql << fmt::format(" {} ", binaryOperators.at(call->name()));
    toCallInputsSql({call->inputs()[1]}, sql);
    sql << ")";
  } else if (call->name() == "is_null" || call->name() == "not_null") {
    sql << "(";
    VELOX_CHECK_EQ(
        call->inputs().size(),
        1,
        "Expected one argument to function 'is_null' or 'not_null'");
    toCallInputsSql({call->inputs()[0]}, sql);
    sql << fmt::format(" is{} null", call->name() == "not_null" ? " not" : "");
    sql << ")";
  } else if (call->name() == "in") {
    VELOX_CHECK_GE(
        call->inputs().size(),
        2,
        "Expected at least two arguments to function 'in'");
    toCallInputsSql({call->inputs()[0]}, sql);
    sql << " in (";
    for (auto i = 1; i < call->inputs().size(); ++i) {
      appendComma(i - 1, sql);
      toCallInputsSql({call->inputs()[i]}, sql);
    }
    sql << ")";
  } else if (call->name() == "like") {
    VELOX_CHECK_GE(
        call->inputs().size(),
        2,
        "Expected at least two arguments to function 'like'");
    VELOX_CHECK_LE(
        call->inputs().size(),
        3,
        "Expected at most three arguments to function 'like'");
    sql << "(";
    toCallInputsSql({call->inputs()[0]}, sql);
    sql << " like ";
    toCallInputsSql({call->inputs()[1]}, sql);
    if (call->inputs().size() == 3) {
      sql << " escape ";
      toCallInputsSql({call->inputs()[2]}, sql);
    }
    sql << ")";
  } else if (call->name() == "or" || call->name() == "and") {
    VELOX_CHECK_GE(
        call->inputs().size(),
        2,
        "Expected at least two arguments to function 'or' or 'and'");
    sql << "(";
    const auto& inputs = call->inputs();
    for (auto i = 0; i < inputs.size(); ++i) {
      if (i > 0) {
        sql << fmt::format(" {} ", call->name());
      }
      toCallInputsSql({inputs[i]}, sql);
    }
    sql << ")";
  } else if (call->name() == "array_constructor") {
    sql << "ARRAY[";
    toCallInputsSql(call->inputs(), sql);
    sql << "]";
  } else if (call->name() == "between") {
    VELOX_CHECK_EQ(
        call->inputs().size(),
        3,
        "Expected three arguments to function 'between'");
    sql << "(";
    const auto& inputs = call->inputs();
    toCallInputsSql({inputs[0]}, sql);
    sql << " between ";
    toCallInputsSql({inputs[1]}, sql);
    sql << " and ";
    toCallInputsSql({inputs[2]}, sql);
    sql << ")";
  } else if (call->name() == "row_constructor") {
    VELOX_CHECK_GE(
        call->inputs().size(),
        1,
        "Expected at least one argument to function 'row_constructor'");
    sql << "row(";
    toCallInputsSql(call->inputs(), sql);
    sql << ")";
  } else if (call->name() == "subscript") {
    VELOX_CHECK_EQ(
        call->inputs().size(),
        2,
        "Expected two arguments to function 'subscript'");
    toCallInputsSql({call->inputs()[0]}, sql);
    sql << "[";
    toCallInputsSql({call->inputs()[1]}, sql);
    sql << "]";
  } else if (call->name() == "switch") {
    VELOX_CHECK_GE(
        call->inputs().size(),
        2,
        "Expected at least two arguments to function 'switch'");
    sql << "case";
    int i = 0;
    for (; i < call->inputs().size() - 1; i += 2) {
      sql << " when ";
      toCallInputsSql({call->inputs()[i]}, sql);
      sql << " then ";
      toCallInputsSql({call->inputs()[i + 1]}, sql);
    }
    if (i < call->inputs().size()) {
      sql << " else ";
      toCallInputsSql({call->inputs()[i]}, sql);
    }
    sql << " end";
  } else {
    // Regular function call syntax.
    sql << call->name() << "(";
    toCallInputsSql(call->inputs(), sql);
    sql << ")";
  }
  return sql.str();
}

std::string toCastSql(const core::CastTypedExpr& cast) {
  std::stringstream sql;
  if (cast.isTryCast()) {
    sql << "try_cast(";
  } else {
    sql << "cast(";
  }
  toCallInputsSql(cast.inputs(), sql);
  sql << " as " << toTypeSql(cast.type());
  sql << ")";
  return sql.str();
}

std::string toConcatSql(const core::ConcatTypedExpr& concat) {
  std::stringstream input;
  toCallInputsSql(concat.inputs(), input);
  return fmt::format(
      "cast(row({}) as {})", input.str(), toTypeSql(concat.type()));
}

template <typename T>
T getConstantValue(const core::ConstantTypedExpr& expr) {
  if (expr.hasValueVector()) {
    return expr.valueVector()->as<SimpleVector<T>>()->valueAt(0);
  } else {
    return expr.value().value<T>();
  }
}

template <>
std::string getConstantValue(const core::ConstantTypedExpr& expr) {
  if (expr.hasValueVector()) {
    return expr.valueVector()
        ->as<SimpleVector<StringView>>()
        ->valueAt(0)
        .getString();
  } else {
    return expr.value().value<std::string>();
  }
}

std::string toConstantSql(const core::ConstantTypedExpr& constant) {
  const auto& type = constant.type();
  const auto typeSql = toTypeSql(type);

  std::stringstream sql;
  if (constant.isNull()) {
    // Syntax like BIGINT 'null' for typed null is not supported, so use cast
    // instead.
    sql << fmt::format("cast(null as {})", typeSql);
  } else if (type->isVarchar() || type->isVarbinary()) {
    // Escape single quote in string literals used in SQL texts.
    if (type->isVarbinary()) {
      sql << typeSql << " ";
    }
    sql << std::quoted(getConstantValue<std::string>(constant), '\'', '\'');
  } else if (type->isIntervalYearMonth()) {
    sql << fmt::format("INTERVAL '{}' YEAR TO MONTH", constant.toString());
  } else if (type->isIntervalDayTime()) {
    sql << fmt::format("INTERVAL '{}' DAY TO SECOND", constant.toString());
  } else if (type->isBigint()) {
    sql << getConstantValue<int64_t>(constant);
  } else if (type->isPrimitiveType()) {
    sql << fmt::format("{} '{}'", typeSql, constant.toString());
  } else {
    VELOX_NYI(
        "Constant expressions of {} are not supported yet.", type->toString());
  }
  return sql.str();
}

std::string toAggregateCallSql(
    const core::CallTypedExprPtr& call,
    const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys,
    const std::vector<core::SortOrder>& sortingOrders,
    bool distinct) {
  VELOX_CHECK_EQ(sortingKeys.size(), sortingOrders.size());
  std::stringstream sql;
  sql << call->name() << "(";

  if (distinct) {
    sql << "distinct ";
  }

  toCallInputsSql(call->inputs(), sql);

  if (!sortingKeys.empty()) {
    sql << " ORDER BY ";

    for (int i = 0; i < sortingKeys.size(); i++) {
      appendComma(i, sql);
      sql << sortingKeys[i]->name() << " " << sortingOrders[i].toString();
    }
  }

  sql << ")";
  return sql.str();
}

void PrestoSqlPlanNodeVisitor::visit(
    const core::ValuesNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);
  if (!PrestoQueryRunner::isSupportedDwrfType(node.outputType())) {
    visitorContext.sql = std::nullopt;
  } else {
    visitorContext.sql = ReferenceQueryRunner::getTableName(node);
  }
}

void PrestoSqlPlanNodeVisitor::visit(
    const core::TableScanNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx).sql =
      node.tableHandle()->name();
}

void PrestoSqlPlanNodeVisitor::visit(
    const core::HashJoinNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  if (!PrestoQueryRunner::isSupportedDwrfType(
          node.sources()[0]->outputType()) ||
      !PrestoQueryRunner::isSupportedDwrfType(
          node.sources()[1]->outputType())) {
    visitorContext.sql = std::nullopt;
    return;
  }

  std::optional<std::string> probeTableName =
      toJoinSourceSql(toSql(node.sources()[0]));
  std::optional<std::string> buildTableName =
      toJoinSourceSql(toSql(node.sources()[1]));
  if (!probeTableName || !buildTableName) {
    visitorContext.sql = std::nullopt;
    return;
  }

  const auto& outputNames = node.outputType()->names();

  std::stringstream sql;
  if (node.isLeftSemiProjectJoin()) {
    sql << "SELECT "
        << folly::join(", ", outputNames.begin(), --outputNames.end());
  } else {
    sql << "SELECT " << folly::join(", ", outputNames);
  }

  switch (node.joinType()) {
    case core::JoinType::kInner:
      sql << " FROM " << *probeTableName << " INNER JOIN " << *buildTableName
          << " ON " << joinConditionAsSql(node);
      break;
    case core::JoinType::kLeft:
      sql << " FROM " << *probeTableName << " LEFT JOIN " << *buildTableName
          << " ON " << joinConditionAsSql(node);
      break;
    case core::JoinType::kFull:
      sql << " FROM " << *probeTableName << " FULL OUTER JOIN "
          << *buildTableName << " ON " << joinConditionAsSql(node);
      break;
    case core::JoinType::kLeftSemiFilter:
      // Multiple columns returned by a scalar subquery is not supported. A
      // scalar subquery expression is a subquery that returns one result row
      // from exactly one column for every input row.
      if (node.leftKeys().size() > 1) {
        visitorContext.sql = std::nullopt;
        return;
      }
      sql << " FROM " << *probeTableName << " WHERE "
          << joinKeysToSql(node.leftKeys()) << " IN (SELECT "
          << joinKeysToSql(node.rightKeys()) << " FROM " << *buildTableName;
      if (node.filter()) {
        sql << " WHERE " << filterToSql(node.filter());
      }
      sql << ")";
      break;
    case core::JoinType::kLeftSemiProject:
      if (node.isNullAware()) {
        sql << ", " << joinKeysToSql(node.leftKeys()) << " IN (SELECT "
            << joinKeysToSql(node.rightKeys()) << " FROM " << *buildTableName;
        if (node.filter()) {
          sql << " WHERE " << filterToSql(node.filter());
        }
        sql << ") FROM " << *probeTableName;
      } else {
        sql << ", EXISTS (SELECT * FROM " << *buildTableName << " WHERE "
            << joinConditionAsSql(node);
        sql << ") FROM " << *probeTableName;
      }
      break;
    case core::JoinType::kAnti:
      if (node.isNullAware()) {
        sql << " FROM " << *probeTableName << " WHERE "
            << joinKeysToSql(node.leftKeys()) << " NOT IN (SELECT "
            << joinKeysToSql(node.rightKeys()) << " FROM " << *buildTableName;
        if (node.filter()) {
          sql << " WHERE " << filterToSql(node.filter());
        }
        sql << ")";
      } else {
        sql << " FROM " << *probeTableName
            << " WHERE NOT EXISTS (SELECT * FROM " << *buildTableName
            << " WHERE " << joinConditionAsSql(node);
        sql << ")";
      }
      break;
    default:
      VELOX_UNREACHABLE(
          "Unknown join type: {}", static_cast<int>(node.joinType()));
  }
  visitorContext.sql = sql.str();
}

void PrestoSqlPlanNodeVisitor::visit(
    const core::NestedLoopJoinNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  PrestoSqlPlanNodeVisitorContext& visitorContext =
      static_cast<PrestoSqlPlanNodeVisitorContext&>(ctx);

  std::optional<std::string> probeTableName =
      toJoinSourceSql(toSql(node.sources()[0]));
  std::optional<std::string> buildTableName =
      toJoinSourceSql(toSql(node.sources()[1]));
  if (!probeTableName || !buildTableName) {
    visitorContext.sql = std::nullopt;
    return;
  }

  std::stringstream sql;
  sql << "SELECT " << folly::join(", ", node.outputType()->names());

  // Nested loop join without filter.
  VELOX_CHECK_NULL(
      node.joinCondition(),
      "This code path should be called only for nested loop join without filter");
  const std::string joinCondition{"(1 = 1)"};
  switch (node.joinType()) {
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
          "Unknown join type: {}", static_cast<int>(node.joinType()));
  }
  visitorContext.sql = sql.str();
}

void PrestoSqlPlanNodeVisitor::visit(
    const core::SpatialJoinNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  VELOX_NYI("SpatialJoinNode is not yet supported in SQL conversion");
}

std::optional<std::string> PrestoSqlPlanNodeVisitor::toSql(
    const core::PlanNodePtr& node) const {
  PrestoSqlPlanNodeVisitorContext sourceContext;
  node->accept(*this, sourceContext);
  return sourceContext.sql;
}

} // namespace facebook::velox::exec::test
