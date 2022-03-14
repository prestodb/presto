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
#include "velox/duckdb/conversion/DuckParser.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/PlanNode.h"
#include "velox/duckdb/conversion/DuckConversion.h"
#include "velox/expression/CastExpr.h"
#include "velox/external/duckdb/duckdb.hpp"
#include "velox/parse/Expressions.h"
#include "velox/type/Variant.h"

namespace facebook::velox::duckdb {

using ::duckdb::BetweenExpression;
using ::duckdb::CaseExpression;
using ::duckdb::CastExpression;
using ::duckdb::ColumnRefExpression;
using ::duckdb::ComparisonExpression;
using ::duckdb::ConjunctionExpression;
using ::duckdb::ConstantExpression;
using ::duckdb::ExpressionClass;
using ::duckdb::ExpressionType;
using ::duckdb::FunctionExpression;
using ::duckdb::LogicalTypeId;
using ::duckdb::LogicalTypeIdToString;
using ::duckdb::OperatorExpression;
using ::duckdb::ParsedExpression;
using ::duckdb::Parser;
using ::duckdb::ParserOptions;
using ::duckdb::StringUtil;
using ::duckdb::Value;

namespace {

std::shared_ptr<const core::IExpr> parseExpr(ParsedExpression& expr);

std::string normalizeFuncName(std::string input) {
  static std::map<std::string, std::string> kLookup{
      {"+", "plus"},
      {"-", "minus"},
      {"*", "multiply"},
      {"/", "divide"},
      {"%", "mod"},
      {"<", "lt"},
      {"<=", "lte"},
      {">", "gt"},
      {">=", "gte"},
      {"=", "eq"},
      {"!", "not"},
      {"!=", "neq"},
      {"<>", "neq"},
      {"and", "and"},
      {"or", "or"},
      {"is", "is"},
      {"~~", "like"},
      {"like_escape", "like"},
  };
  auto it = kLookup.find(input);
  return (it == kLookup.end()) ? input : it->second;
}

// Convert duckDB operator name to Velox function. Coalesce and subscript needs
// special treatment because `ExpressionTypeToOperator` returns an empty string.
std::string duckOperatorToVelox(ExpressionType type) {
  switch (type) {
    case ExpressionType::OPERATOR_IS_NULL:
      return "is_null";
    case ExpressionType::OPERATOR_COALESCE:
      return "coalesce";
    case ExpressionType::ARRAY_EXTRACT:
      return "subscript";
    case ExpressionType::COMPARE_IN:
      return "in";
    default:
      return normalizeFuncName(ExpressionTypeToOperator(type));
  }
}

std::optional<std::string> getAlias(const ParsedExpression& expr) {
  const auto& alias = expr.alias;
  return alias.empty() ? std::optional<std::string>() : alias;
}

std::shared_ptr<const core::CallExpr> callExpr(
    std::string name,
    std::vector<std::shared_ptr<const core::IExpr>> params,
    std::optional<std::string> alias) {
  return std::make_shared<const core::CallExpr>(
      std::move(name), std::move(params), std::move(alias));
}

std::shared_ptr<const core::CallExpr> callExpr(
    std::string name,
    const std::shared_ptr<const core::IExpr>& param,
    std::optional<std::string> alias) {
  std::vector<std::shared_ptr<const core::IExpr>> params = {param};
  return std::make_shared<const core::CallExpr>(
      std::move(name), std::move(params), std::move(alias));
}

// Parse a constant (1, 99.8, "string", etc).
std::shared_ptr<const core::IExpr> parseConstantExpr(ParsedExpression& expr) {
  auto& constantExpr = dynamic_cast<ConstantExpression&>(expr);
  auto& value = constantExpr.value;

  // This is a hack to make DuckDB more compatible with the old Koski-based
  // parser. By default literal integer constants in DuckDB parser are INTEGER,
  // while in Koski parser they were BIGINT.
  if (value.type().id() == LogicalTypeId::INTEGER) {
    value = Value::BIGINT(value.GetValue<int32_t>());
  }
  return std::make_shared<const core::ConstantExpr>(
      duckValueToVariant(constantExpr.value), getAlias(expr));
}

// Parse a column reference (col1, "col2", tbl.col, etc).
std::shared_ptr<const core::IExpr> parseColumnRefExpr(ParsedExpression& expr) {
  const auto& colRefExpr = dynamic_cast<ColumnRefExpression&>(expr);
  if (!colRefExpr.IsQualified()) {
    return std::make_shared<const core::FieldAccessExpr>(
        colRefExpr.GetColumnName(), getAlias(expr));
  }
  return std::make_shared<const core::FieldAccessExpr>(
      colRefExpr.GetColumnName(),
      getAlias(expr),
      std::vector<std::shared_ptr<const core::IExpr>>{
          std::make_shared<const core::FieldAccessExpr>(
              colRefExpr.GetTableName(), std::nullopt)});
}

// Parse a function call (avg(a), func(1, b), etc).
// Arithmetic operators also follow this path (a + b, a * b, etc).
std::shared_ptr<const core::IExpr> parseFunctionExpr(ParsedExpression& expr) {
  const auto& functionExpr = dynamic_cast<FunctionExpression&>(expr);
  std::vector<std::shared_ptr<const core::IExpr>> params;
  params.reserve(functionExpr.children.size());

  for (const auto& c : functionExpr.children) {
    params.emplace_back(parseExpr(*c));
  }
  return callExpr(
      normalizeFuncName(functionExpr.function_name),
      std::move(params),
      getAlias(expr));
}

// Parse a comparison (a > b, a = b, etc).
std::shared_ptr<const core::IExpr> parseComparisonExpr(ParsedExpression& expr) {
  const auto& compExpr = dynamic_cast<ComparisonExpression&>(expr);
  std::vector<std::shared_ptr<const core::IExpr>> params{
      parseExpr(*compExpr.left), parseExpr(*compExpr.right)};
  return callExpr(
      normalizeFuncName(ExpressionTypeToOperator(expr.GetExpressionType())),
      std::move(params),
      getAlias(expr));
}

// Parse x between lower and upper
std::shared_ptr<const core::IExpr> parseBetweenExpr(ParsedExpression& expr) {
  const auto& betweenExpr = dynamic_cast<BetweenExpression&>(expr);
  return callExpr(
      "between",
      {parseExpr(*betweenExpr.input),
       parseExpr(*betweenExpr.lower),
       parseExpr(*betweenExpr.upper)},
      getAlias(expr));
}

// Parse a conjunction (AND or OR).
std::shared_ptr<const core::IExpr> parseConjunctionExpr(
    ParsedExpression& expr) {
  const auto& conjExpr = dynamic_cast<ConjunctionExpression&>(expr);
  std::string conjName =
      StringUtil::Lower(ExpressionTypeToOperator(expr.GetExpressionType()));

  if (conjExpr.children.size() < 2) {
    throw std::invalid_argument(folly::sformat(
        "Malformed conjunction expression "
        "(expected at least 2 input columns, got {}).",
        conjExpr.children.size()));
  }

  // DuckDB's parser returns conjunction involving multiple input in a flat
  // expression, in the form `AND(a, b, d, e)`, but internally we expect
  // conjunctions to have exactly 2 input. This code converts that input into
  // `AND(AND(AND(a, b), d), e)` (so it's executed in the same order).
  std::shared_ptr<const core::IExpr> current;
  for (size_t i = 1; i < conjExpr.children.size(); ++i) {
    std::vector<std::shared_ptr<const core::IExpr>> params;
    params.reserve(2);

    if (current == nullptr) {
      params.emplace_back(parseExpr(*conjExpr.children[0]));
      params.emplace_back(parseExpr(*conjExpr.children[1]));
    } else {
      params.emplace_back(current);
      params.emplace_back(parseExpr(*conjExpr.children[i]));
    }
    current = callExpr(conjName, std::move(params), getAlias(expr));
  }
  return current;
}

// Parse an "operator", like NOT.
std::shared_ptr<const core::IExpr> parseOperatorExpr(ParsedExpression& expr) {
  const auto& operExpr = dynamic_cast<OperatorExpression&>(expr);

  // Code for array literal parsing (e.g. "ARRAY[1, 2, 3]")
  if (expr.GetExpressionType() == ExpressionType::ARRAY_CONSTRUCTOR) {
    std::vector<variant> arrayElements;
    arrayElements.reserve(operExpr.children.size());

    for (const auto& c : operExpr.children) {
      if (auto constantExpr = dynamic_cast<ConstantExpression*>(c.get())) {
        arrayElements.emplace_back(duckValueToVariant(constantExpr->value));
      } else {
        VELOX_UNSUPPORTED("Array literal elements need to be constant");
      }
    }
    return std::make_shared<const core::ConstantExpr>(
        variant::array(arrayElements), getAlias(expr));
  }

  // Check if the operator is "IN" or "NOT IN".
  if (expr.GetExpressionType() == ExpressionType::COMPARE_IN ||
      expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) {
    auto numValues = operExpr.children.size() - 1;

    std::vector<variant> values;
    values.reserve(numValues);
    for (auto i = 0; i < numValues; i++) {
      if (auto constantExpr = dynamic_cast<ConstantExpression*>(
              operExpr.children[i + 1].get())) {
        values.emplace_back(duckValueToVariant(constantExpr->value));
      } else {
        VELOX_UNSUPPORTED("IN list values need to be constant");
      }
    }

    std::vector<std::shared_ptr<const core::IExpr>> params;
    params.emplace_back(parseExpr(*operExpr.children[0]));
    params.emplace_back(std::make_shared<const core::ConstantExpr>(
        variant::array(values), std::nullopt));
    auto inExpr = callExpr("in", std::move(params), getAlias(expr));
    // Translate COMPARE_NOT_IN into NOT(IN()).
    return (expr.GetExpressionType() == ExpressionType::COMPARE_IN)
        ? inExpr
        : callExpr("not", inExpr, std::nullopt);
  }

  std::vector<std::shared_ptr<const core::IExpr>> params;
  params.reserve(operExpr.children.size());

  for (const auto& c : operExpr.children) {
    params.emplace_back(parseExpr(*c));
  }

  // STRUCT_EXTRACT(struct, 'entry') resolves nested field access such as
  // (a).b.c, (a.b).c
  if (expr.GetExpressionType() == ExpressionType::STRUCT_EXTRACT) {
    VELOX_CHECK_EQ(params.size(), 2);
    std::vector<std::shared_ptr<const core::IExpr>> input = {params[0]};

    if (auto constantExpr =
            std::dynamic_pointer_cast<const core::ConstantExpr>(params[1])) {
      auto fieldName = constantExpr->value().value<std::string>();

      return std::make_shared<const core::FieldAccessExpr>(
          fieldName, getAlias(expr), std::move(input));
    } else {
      VELOX_UNSUPPORTED("STRUCT_EXTRACT field name must be constant");
    }
  }

  if (expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
    return callExpr(
        "not",
        callExpr("is_null", std::move(params), std::nullopt),
        getAlias(expr));
  }

  return callExpr(
      duckOperatorToVelox(expr.GetExpressionType()),
      std::move(params),
      getAlias(expr));
}

namespace {
bool isNullConstant(const std::shared_ptr<const core::IExpr>& expr) {
  if (auto constExpr =
          std::dynamic_pointer_cast<const core::ConstantExpr>(expr)) {
    return constExpr->value().isNull();
  }

  return false;
}
} // namespace

// Parse an IF()/CASE expression.
std::shared_ptr<const core::IExpr> parseCaseExpr(ParsedExpression& expr) {
  const auto& caseExpr = dynamic_cast<CaseExpression&>(expr);
  const auto& checks = caseExpr.case_checks;

  if (checks.size() == 1) {
    const auto& check = checks.front();

    std::vector<std::shared_ptr<const core::IExpr>> params{
        parseExpr(*check.when_expr),
        parseExpr(*check.then_expr),
        parseExpr(*caseExpr.else_expr),
    };
    return callExpr("if", std::move(params), getAlias(expr));
  }

  std::vector<std::shared_ptr<const core::IExpr>> inputs;
  inputs.reserve(checks.size() * 2 + 1);
  for (auto& check : checks) {
    inputs.emplace_back(parseExpr(*check.when_expr));
    inputs.emplace_back(parseExpr(*check.then_expr));
  }

  auto elseExpr = parseExpr(*caseExpr.else_expr);
  if (!isNullConstant(elseExpr)) {
    inputs.emplace_back(elseExpr);
  }

  return callExpr("switch", std::move(inputs), getAlias(expr));
}

// Parse an CAST expression.
std::shared_ptr<const core::IExpr> parseCastExpr(ParsedExpression& expr) {
  const auto& castExpr = dynamic_cast<CastExpression&>(expr);
  std::vector<std::shared_ptr<const core::IExpr>> params{
      parseExpr(*castExpr.child)};
  // We may need to expand toVeloxType in the future to support
  // Map and Array and Struct properly.
  auto targetType = toVeloxType(castExpr.cast_type);
  const bool nullOnFailure = castExpr.try_cast;
  VELOX_CHECK(!params.empty());
  return std::make_shared<const core::CastExpr>(
      targetType, params[0], nullOnFailure, getAlias(expr));
}

std::shared_ptr<const core::IExpr> parseExpr(ParsedExpression& expr) {
  switch (expr.GetExpressionClass()) {
    case ExpressionClass::CONSTANT:
      return parseConstantExpr(expr);

    case ExpressionClass::COLUMN_REF:
      return parseColumnRefExpr(expr);

    case ExpressionClass::FUNCTION:
      return parseFunctionExpr(expr);

    case ExpressionClass::COMPARISON:
      return parseComparisonExpr(expr);

    case ExpressionClass::BETWEEN:
      return parseBetweenExpr(expr);

    case ExpressionClass::CONJUNCTION:
      return parseConjunctionExpr(expr);

    case ExpressionClass::OPERATOR:
      return parseOperatorExpr(expr);

    case ExpressionClass::CASE:
      return parseCaseExpr(expr);

    case ExpressionClass::CAST:
      return parseCastExpr(expr);

    default:
      throw std::invalid_argument(
          "Unsupported expression type for DuckDB -> velox conversion: " +
          ::duckdb::ExpressionTypeToString(expr.GetExpressionType()));
  }
}

} // namespace

std::shared_ptr<const core::IExpr> parseExpr(const std::string& exprString) {
  ParserOptions options;
  options.preserve_identifier_case = false;
  auto parsedExpressions = Parser::ParseExpressionList(exprString, options);
  if (parsedExpressions.size() != 1) {
    throw std::invalid_argument(folly::sformat(
        "Expecting exactly one input expression, found {}.",
        parsedExpressions.size()));
  }

  return parseExpr(*parsedExpressions.front());
}

namespace {
bool isAscending(::duckdb::OrderType orderType, const std::string& exprString) {
  switch (orderType) {
    case ::duckdb::OrderType::ASCENDING:
      return true;
    case ::duckdb::OrderType::DESCENDING:
      return false;
    case ::duckdb::OrderType::ORDER_DEFAULT:
      // ASC is the default.
      return true;
    case ::duckdb::OrderType::INVALID:
    default:
      VELOX_FAIL("Cannot parse ORDER BY clause: {}", exprString)
  }
}

bool isNullsFirst(
    ::duckdb::OrderByNullType orderByNullType,
    const std::string& exprString) {
  switch (orderByNullType) {
    case ::duckdb::OrderByNullType::NULLS_FIRST:
      return true;
    case ::duckdb::OrderByNullType::NULLS_LAST:
      return false;
    case ::duckdb::OrderByNullType::ORDER_DEFAULT:
      // NULLS LAST is the default.
      return false;
    case ::duckdb::OrderByNullType::INVALID:
    default:
      VELOX_FAIL("Cannot parse ORDER BY clause: {}", exprString)
  }

  VELOX_UNREACHABLE();
}
} // namespace

std::pair<std::shared_ptr<const core::IExpr>, core::SortOrder> parseOrderByExpr(
    const std::string& exprString) {
  ParserOptions options;
  options.preserve_identifier_case = false;
  auto orderByNodes = Parser::ParseOrderList(exprString, options);
  if (orderByNodes.size() != 1) {
    throw std::invalid_argument(folly::sformat(
        "Expecting exactly one input expression, found {}.",
        orderByNodes.size()));
  }

  const auto& orderByNode = orderByNodes[0];

  const bool ascending = isAscending(orderByNode.type, exprString);
  const bool nullsFirst = isNullsFirst(orderByNode.null_order, exprString);

  return {
      parseExpr(*orderByNode.expression),
      core::SortOrder(ascending, nullsFirst)};
}

} // namespace facebook::velox::duckdb
