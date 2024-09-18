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
#include "velox/parse/Expressions.h"
#include "velox/type/Variant.h"

#include <duckdb.hpp> // @manual
#include <duckdb/parser/expression/between_expression.hpp> // @manual
#include <duckdb/parser/expression/case_expression.hpp> // @manual
#include <duckdb/parser/expression/cast_expression.hpp> // @manual
#include <duckdb/parser/expression/comparison_expression.hpp> // @manual
#include <duckdb/parser/expression/conjunction_expression.hpp> // @manual
#include <duckdb/parser/expression/constant_expression.hpp> // @manual
#include <duckdb/parser/expression/function_expression.hpp> // @manual
#include <duckdb/parser/expression/lambda_expression.hpp> // @manual
#include <duckdb/parser/expression/operator_expression.hpp> // @manual
#include <duckdb/parser/expression/window_expression.hpp> // @manual
#include <duckdb/parser/parser.hpp> // @manual
#include <duckdb/parser/parser_options.hpp> // @manual

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
using ::duckdb::WindowBoundary;
using ::duckdb::WindowExpression;

namespace {

std::shared_ptr<const core::IExpr> parseExpr(
    ParsedExpression& expr,
    const ParseOptions& options);

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
      {"!~~", "notlike"},
      {"like_escape", "like"},
      {"not_like_escape", "notlike"},
      {"IS DISTINCT FROM", "distinct_from"},
      {"count_star", "count"},
  };
  auto it = kLookup.find(input);
  return (it == kLookup.end()) ? input : it->second;
}

// Convert duckDB operator name to Velox function. Some expression types such as
// coalesce and subscript need special treatment because
// `ExpressionTypeToOperator` returns an empty string.
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
    case ExpressionType::OPERATOR_NOT:
      return "not";
    default:
      return normalizeFuncName(ExpressionTypeToOperator(type));
  }
}

// SQL functions could be registered with different prefixes.
// This function returns a full Velox function name with the registered prefix.
std::string toFullFunctionName(
    const std::string& functionName,
    const std::string& prefix) {
  // Special forms are registered without a prefix.
  static const std::unordered_set<std::string> specialForms{
      "cast", "and", "coalesce", "if", "or", "switch", "try"};
  if (specialForms.count(functionName)) {
    return functionName;
  }
  return prefix + functionName;
}

std::optional<std::string> getAlias(const ParsedExpression& expr) {
  const auto& alias = expr.alias;
  return alias.empty() ? std::optional<std::string>() : alias;
}

std::shared_ptr<const core::CallExpr> callExpr(
    std::string name,
    std::vector<std::shared_ptr<const core::IExpr>> params,
    std::optional<std::string> alias,
    const ParseOptions& options) {
  return std::make_shared<const core::CallExpr>(
      toFullFunctionName(name, options.functionPrefix),
      std::move(params),
      std::move(alias));
}

std::shared_ptr<const core::CallExpr> callExpr(
    std::string name,
    const std::shared_ptr<const core::IExpr>& param,
    std::optional<std::string> alias,
    const ParseOptions& options) {
  std::vector<std::shared_ptr<const core::IExpr>> params = {param};
  return std::make_shared<const core::CallExpr>(
      toFullFunctionName(name, options.functionPrefix),
      std::move(params),
      std::move(alias));
}

// Parse a constant (1, 99.8, "string", etc).
std::shared_ptr<const core::IExpr> parseConstantExpr(
    ParsedExpression& expr,
    const ParseOptions& options) {
  auto& constantExpr = dynamic_cast<ConstantExpression&>(expr);
  auto& value = constantExpr.value;

  // This is a hack to make DuckDB more compatible with the old Koski-based
  // parser. By default literal integer constants in DuckDB parser are INTEGER,
  // while in Koski parser they were BIGINT.
  if (value.type().id() == LogicalTypeId::INTEGER &&
      options.parseIntegerAsBigint) {
    value = Value::BIGINT(value.GetValue<int32_t>());
  }

  if (options.parseDecimalAsDouble &&
      value.type().id() == duckdb::LogicalTypeId::DECIMAL) {
    value = Value::DOUBLE(value.GetValue<double>());
  }

  return std::make_shared<const core::ConstantExpr>(
      toVeloxType(value.type()), duckValueToVariant(value), getAlias(expr));
}

// Parse a column reference (col1, "col2", tbl.col, etc).
std::shared_ptr<const core::IExpr> parseColumnRefExpr(
    ParsedExpression& expr,
    const ParseOptions& options) {
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

std::shared_ptr<const core::ConstantExpr> tryParseInterval(
    const std::string& functionName,
    const std::shared_ptr<const core::IExpr>& input,
    std::optional<std::string> alias) {
  std::optional<int64_t> value;
  if (auto constInput = dynamic_cast<const core::ConstantExpr*>(input.get())) {
    if (constInput->type()->isBigint() && !constInput->value().isNull()) {
      value = constInput->value().value<int64_t>();
    }
  } else if (
      auto castInput = dynamic_cast<const core::CastExpr*>(input.get())) {
    if (castInput->type()->isBigint()) {
      if (auto constInput = dynamic_cast<const core::ConstantExpr*>(
              castInput->getInput().get())) {
        if (constInput->type()->isBigint() && !constInput->value().isNull()) {
          value = constInput->value().value<int64_t>();
        }
      }
    }
  }

  if (!value.has_value()) {
    return nullptr;
  }

  int64_t multiplier;
  if (functionName == "to_hours") {
    multiplier = 60 * 60 * 1'000;
  } else if (functionName == "to_minutes") {
    multiplier = 60 * 1'000;
  } else if (functionName == "to_seconds") {
    multiplier = 1'000;
  } else if (functionName == "to_milliseconds") {
    multiplier = 1;
  } else {
    return nullptr;
  }

  return std::make_shared<core::ConstantExpr>(
      INTERVAL_DAY_TIME(), variant(value.value() * multiplier), alias);
}

// Parse a function call (avg(a), func(1, b), etc).
// Arithmetic operators also follow this path (a + b, a * b, etc).
std::shared_ptr<const core::IExpr> parseFunctionExpr(
    ParsedExpression& expr,
    const ParseOptions& options) {
  const auto& functionExpr = dynamic_cast<FunctionExpression&>(expr);
  std::vector<std::shared_ptr<const core::IExpr>> params;
  params.reserve(functionExpr.children.size());

  for (const auto& c : functionExpr.children) {
    params.emplace_back(parseExpr(*c, options));
  }
  auto func = normalizeFuncName(functionExpr.function_name);

  if (params.size() == 1) {
    if (auto interval = tryParseInterval(func, params[0], getAlias(expr))) {
      return interval;
    }
  }

  // NOT LIKE function needs special handling as it maps to two functions
  // "not" and "like".
  if (func == "notlike") {
    auto likeParams = params;
    params.clear();
    params.emplace_back(callExpr("like", std::move(likeParams), {}, options));
    func = "not";
  }
  return callExpr(func, std::move(params), getAlias(expr), options);
}

// Parse a comparison (a > b, a = b, etc).
std::shared_ptr<const core::IExpr> parseComparisonExpr(
    ParsedExpression& expr,
    const ParseOptions& options) {
  const auto& compExpr = dynamic_cast<ComparisonExpression&>(expr);
  std::vector<std::shared_ptr<const core::IExpr>> params{
      parseExpr(*compExpr.left, options), parseExpr(*compExpr.right, options)};
  return callExpr(
      normalizeFuncName(ExpressionTypeToOperator(expr.GetExpressionType())),
      std::move(params),
      getAlias(expr),
      options);
}

// Parse x between lower and upper
std::shared_ptr<const core::IExpr> parseBetweenExpr(
    ParsedExpression& expr,
    const ParseOptions& options) {
  const auto& betweenExpr = dynamic_cast<BetweenExpression&>(expr);
  return callExpr(
      "between",
      {parseExpr(*betweenExpr.input, options),
       parseExpr(*betweenExpr.lower, options),
       parseExpr(*betweenExpr.upper, options)},
      getAlias(expr),
      options);
}

// Parse a conjunction (AND or OR).
std::shared_ptr<const core::IExpr> parseConjunctionExpr(
    ParsedExpression& expr,
    const ParseOptions& options) {
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
      params.emplace_back(parseExpr(*conjExpr.children[0], options));
      params.emplace_back(parseExpr(*conjExpr.children[1], options));
    } else {
      params.emplace_back(current);
      params.emplace_back(parseExpr(*conjExpr.children[i], options));
    }
    current = callExpr(conjName, std::move(params), getAlias(expr), options);
  }
  return current;
}

static bool areAllChildrenConstant(const OperatorExpression& operExpr) {
  for (const auto& child : operExpr.children) {
    if (child->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
      return false;
    }
  }
  return true;
}

// Parse an "operator", like NOT.
std::shared_ptr<const core::IExpr> parseOperatorExpr(
    ParsedExpression& expr,
    const ParseOptions& options) {
  const auto& operExpr = dynamic_cast<OperatorExpression&>(expr);

  // Code for array literal parsing (e.g. "ARRAY[1, 2, 3]")
  if (expr.GetExpressionType() == ExpressionType::ARRAY_CONSTRUCTOR) {
    if (areAllChildrenConstant(operExpr)) {
      std::vector<variant> arrayElements;
      arrayElements.reserve(operExpr.children.size());

      TypePtr valueType = UNKNOWN();
      for (const auto& child : operExpr.children) {
        if (auto constantExpr =
                dynamic_cast<ConstantExpression*>(child.get())) {
          auto& value = constantExpr->value;
          if (options.parseDecimalAsDouble &&
              value.type().id() == duckdb::LogicalTypeId::DECIMAL) {
            value = Value::DOUBLE(value.GetValue<double>());
          }
          arrayElements.emplace_back(duckValueToVariant(value));
          if (!value.IsNull()) {
            valueType = toVeloxType(value.type());
          }
        } else {
          VELOX_UNREACHABLE();
        }
      }
      return std::make_shared<const core::ConstantExpr>(
          ARRAY(valueType), variant::array(arrayElements), getAlias(expr));
    } else {
      std::vector<std::shared_ptr<const core::IExpr>> params;
      params.reserve(operExpr.children.size());

      for (const auto& child : operExpr.children) {
        params.emplace_back(parseExpr(*child, options));
      }
      return callExpr(
          "array_constructor", std::move(params), getAlias(expr), options);
    }
  }

  // Check if the operator is "IN" or "NOT IN".
  if (expr.GetExpressionType() == ExpressionType::COMPARE_IN ||
      expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) {
    auto numValues = operExpr.children.size() - 1;

    std::vector<variant> values;
    values.reserve(numValues);

    TypePtr valueType = UNKNOWN();
    for (auto i = 0; i < numValues; i++) {
      auto valueExpr = operExpr.children[i + 1].get();
      if (const auto castExpr = dynamic_cast<CastExpression*>(valueExpr)) {
        if (castExpr->child->GetExpressionType() ==
            ExpressionType::VALUE_CONSTANT) {
          auto constExpr =
              dynamic_cast<ConstantExpression*>(castExpr->child.get());
          auto value = constExpr->value.DefaultCastAs(
              castExpr->cast_type, !castExpr->try_cast);
          values.emplace_back(duckValueToVariant(value));
          valueType = toVeloxType(castExpr->cast_type);
          continue;
        }
      }

      if (auto constantExpr = dynamic_cast<ConstantExpression*>(valueExpr)) {
        auto& value = constantExpr->value;
        if (options.parseDecimalAsDouble &&
            value.type().id() == duckdb::LogicalTypeId::DECIMAL) {
          value = Value::DOUBLE(value.GetValue<double>());
        }
        values.emplace_back(duckValueToVariant(value));
        if (!value.IsNull()) {
          valueType = toVeloxType(value.type());
        }
        continue;
      }

      VELOX_UNSUPPORTED("IN list values need to be constant");
    }

    std::vector<std::shared_ptr<const core::IExpr>> params;
    params.emplace_back(parseExpr(*operExpr.children[0], options));
    params.emplace_back(std::make_shared<const core::ConstantExpr>(
        ARRAY(valueType), variant::array(values), std::nullopt));
    auto inExpr = callExpr("in", std::move(params), getAlias(expr), options);
    // Translate COMPARE_NOT_IN into NOT(IN()).
    return (expr.GetExpressionType() == ExpressionType::COMPARE_IN)
        ? inExpr
        : callExpr("not", inExpr, std::nullopt, options);
  }

  std::vector<std::shared_ptr<const core::IExpr>> params;
  params.reserve(operExpr.children.size());

  for (const auto& child : operExpr.children) {
    params.emplace_back(parseExpr(*child, options));
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
        callExpr("is_null", std::move(params), std::nullopt, options),
        getAlias(expr),
        options);
  }

  return callExpr(
      duckOperatorToVelox(expr.GetExpressionType()),
      std::move(params),
      getAlias(expr),
      options);
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
std::shared_ptr<const core::IExpr> parseCaseExpr(
    ParsedExpression& expr,
    const ParseOptions& options) {
  const auto& caseExpr = dynamic_cast<CaseExpression&>(expr);
  const auto& checks = caseExpr.case_checks;

  if (checks.size() == 1) {
    const auto& check = checks.front();

    std::vector<std::shared_ptr<const core::IExpr>> params{
        parseExpr(*check.when_expr, options),
        parseExpr(*check.then_expr, options),
        parseExpr(*caseExpr.else_expr, options),
    };
    return callExpr("if", std::move(params), getAlias(expr), options);
  }

  std::vector<std::shared_ptr<const core::IExpr>> inputs;
  inputs.reserve(checks.size() * 2 + 1);
  for (auto& check : checks) {
    inputs.emplace_back(parseExpr(*check.when_expr, options));
    inputs.emplace_back(parseExpr(*check.then_expr, options));
  }

  auto elseExpr = parseExpr(*caseExpr.else_expr, options);
  if (!isNullConstant(elseExpr)) {
    inputs.emplace_back(elseExpr);
  }

  return callExpr("switch", std::move(inputs), getAlias(expr), options);
}

// Parse an CAST expression.
std::shared_ptr<const core::IExpr> parseCastExpr(
    ParsedExpression& expr,
    const ParseOptions& options) {
  const auto& castExpr = dynamic_cast<CastExpression&>(expr);
  std::vector<std::shared_ptr<const core::IExpr>> params{
      parseExpr(*castExpr.child, options)};
  // We may need to expand toVeloxType in the future to support
  // Map and Array and Struct properly.
  auto targetType = toVeloxType(castExpr.cast_type);
  VELOX_CHECK(!params.empty());

  // Convert cast(NULL as <type>) into a constant NULL.
  if (auto* constant =
          dynamic_cast<const core::ConstantExpr*>(params[0].get())) {
    if (constant->value().isNull()) {
      return std::make_shared<const core::ConstantExpr>(
          targetType, variant::null(targetType->kind()), getAlias(expr));
    }

    // DuckDB parses BOOLEAN literal as cast expression.  Try to restore it back
    // to constant expression here.
    if (targetType->isBoolean() && constant->type()->isVarchar()) {
      const auto& value = constant->value();
      const auto& s = value.value<TypeKind::VARCHAR>();

      if (s == "t") {
        return std::make_shared<const core::ConstantExpr>(
            BOOLEAN(),
            variant::create<TypeKind::BOOLEAN>(true),
            getAlias(expr));
      }

      if (s == "f") {
        return std::make_shared<const core::ConstantExpr>(
            BOOLEAN(),
            variant::create<TypeKind::BOOLEAN>(false),
            getAlias(expr));
      }
    }
  }

  const bool nullOnFailure = castExpr.try_cast;
  return std::make_shared<const core::CastExpr>(
      targetType, params[0], nullOnFailure, getAlias(expr));
}

std::shared_ptr<const core::IExpr> parseLambdaExpr(
    ParsedExpression& expr,
    const ParseOptions& options) {
  const auto& lambdaExpr = dynamic_cast<::duckdb::LambdaExpression&>(expr);
  auto capture = parseExpr(*lambdaExpr.lhs, options);
  auto body = parseExpr(*lambdaExpr.expr, options);

  // capture is either a core::FieldAccessExpr or a 'row' core::CallExpr with 2
  // or more core::FieldAccessExpr inputs.

  std::vector<std::string> names;
  if (auto fieldExpr =
          std::dynamic_pointer_cast<const core::FieldAccessExpr>(capture)) {
    names.push_back(fieldExpr->getFieldName());
  } else if (
      auto callExpr =
          std::dynamic_pointer_cast<const core::CallExpr>(capture)) {
    VELOX_CHECK_EQ(
        toFullFunctionName("row", options.functionPrefix),
        callExpr->getFunctionName());
    for (auto& input : callExpr->getInputs()) {
      auto fieldExpr =
          std::dynamic_pointer_cast<const core::FieldAccessExpr>(input);
      VELOX_CHECK_NOT_NULL(fieldExpr);
      names.push_back(fieldExpr->getFieldName());
    }
  } else {
    VELOX_FAIL(
        "Unexpected left-hand-side expression for the lambda expression: {}",
        capture->toString());
  }

  return std::make_shared<const core::LambdaExpr>(
      std::move(names), std::move(body));
}

std::shared_ptr<const core::IExpr> parseExpr(
    ParsedExpression& expr,
    const ParseOptions& options) {
  switch (expr.GetExpressionClass()) {
    case ExpressionClass::CONSTANT:
      return parseConstantExpr(expr, options);

    case ExpressionClass::COLUMN_REF:
      return parseColumnRefExpr(expr, options);

    case ExpressionClass::FUNCTION:
      return parseFunctionExpr(expr, options);

    case ExpressionClass::COMPARISON:
      return parseComparisonExpr(expr, options);

    case ExpressionClass::BETWEEN:
      return parseBetweenExpr(expr, options);

    case ExpressionClass::CONJUNCTION:
      return parseConjunctionExpr(expr, options);

    case ExpressionClass::OPERATOR:
      return parseOperatorExpr(expr, options);

    case ExpressionClass::CASE:
      return parseCaseExpr(expr, options);

    case ExpressionClass::CAST:
      return parseCastExpr(expr, options);

    case ExpressionClass::LAMBDA:
      return parseLambdaExpr(expr, options);

    default:
      throw std::invalid_argument(
          "Unsupported expression type for DuckDB -> velox conversion: " +
          ::duckdb::ExpressionTypeToString(expr.GetExpressionType()));
  }
}

::duckdb::vector<::duckdb::unique_ptr<::duckdb::ParsedExpression>>
parseExpression(const std::string& exprString) {
  ParserOptions options;
  options.preserve_identifier_case = false;

  try {
    return Parser::ParseExpressionList(exprString, options);
  } catch (const std::exception& e) {
    VELOX_FAIL("Cannot parse expression: {}. {}", exprString, e.what());
  }
}

std::unique_ptr<::duckdb::ParsedExpression> parseSingleExpression(
    const std::string& exprString) {
  auto parsed = parseExpression(exprString);
  VELOX_CHECK_EQ(
      1, parsed.size(), "Expected exactly one expression: {}.", exprString);
  return std::move(parsed.front());
}
} // namespace

std::shared_ptr<const core::IExpr> parseExpr(
    const std::string& exprString,
    const ParseOptions& options) {
  auto parsed = parseSingleExpression(exprString);
  return parseExpr(*parsed, options);
}

std::vector<std::shared_ptr<const core::IExpr>> parseMultipleExpressions(
    const std::string& exprString,
    const ParseOptions& options) {
  auto parsedExpressions = parseExpression(exprString);
  VELOX_CHECK_GT(parsedExpressions.size(), 0);
  std::vector<std::shared_ptr<const core::IExpr>> exprs;
  exprs.reserve(parsedExpressions.size());
  for (const auto& parsedExpr : parsedExpressions) {
    exprs.push_back(parseExpr(*parsedExpr, options));
  }
  return exprs;
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
      VELOX_FAIL("Cannot parse ORDER BY clause: {}", exprString);
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
      VELOX_FAIL("Cannot parse ORDER BY clause: {}", exprString);
  }

  VELOX_UNREACHABLE();
}
} // namespace

std::pair<std::shared_ptr<const core::IExpr>, core::SortOrder> parseOrderByExpr(
    const std::string& exprString) {
  ParserOptions options;
  ParseOptions parseOptions;
  options.preserve_identifier_case = false;
  auto orderByNodes = Parser::ParseOrderList(exprString, options);
  VELOX_CHECK_EQ(
      1,
      orderByNodes.size(),
      "Expected exactly one expression: {}.",
      exprString);

  const auto& orderByNode = orderByNodes[0];

  const bool ascending = isAscending(orderByNode.type, exprString);
  const bool nullsFirst = isNullsFirst(orderByNode.null_order, exprString);

  return {
      parseExpr(*orderByNode.expression, parseOptions),
      core::SortOrder(ascending, nullsFirst)};
}

AggregateExpr parseAggregateExpr(
    const std::string& exprString,
    const ParseOptions& options) {
  auto parsedExpr = parseSingleExpression(exprString);

  auto& functionExpr = dynamic_cast<FunctionExpression&>(*parsedExpr);

  AggregateExpr aggregateExpr;
  aggregateExpr.expr = parseExpr(*parsedExpr, options);
  aggregateExpr.distinct = functionExpr.distinct;

  if (functionExpr.order_bys) {
    for (const auto& orderByNode : functionExpr.order_bys->orders) {
      const bool ascending = isAscending(orderByNode.type, exprString);
      const bool nullsFirst = isNullsFirst(orderByNode.null_order, exprString);
      aggregateExpr.orderBy.emplace_back(
          parseExpr(*orderByNode.expression, options),
          core::SortOrder(ascending, nullsFirst));
    }
  }

  if (functionExpr.filter) {
    aggregateExpr.maskExpr = parseExpr(*functionExpr.filter, options);
  }

  return aggregateExpr;
}

namespace {
WindowType parseWindowType(const WindowExpression& expr) {
  auto windowType = [&](const WindowBoundary& boundary) -> WindowType {
    if (boundary == WindowBoundary::CURRENT_ROW_ROWS ||
        boundary == WindowBoundary::EXPR_FOLLOWING_ROWS ||
        boundary == WindowBoundary::EXPR_PRECEDING_ROWS) {
      return WindowType::kRows;
    }
    return WindowType::kRange;
  };

  auto startType = windowType(expr.start);
  if (startType == WindowType::kRows) {
    return startType;
  }
  return windowType(expr.end);
}

BoundType parseBoundType(WindowBoundary boundary) {
  switch (boundary) {
    case WindowBoundary::CURRENT_ROW_RANGE:
    case WindowBoundary::CURRENT_ROW_ROWS:
      return BoundType::kCurrentRow;
    case WindowBoundary::EXPR_PRECEDING_ROWS:
    case WindowBoundary::EXPR_PRECEDING_RANGE:
      return BoundType::kPreceding;
    case WindowBoundary::EXPR_FOLLOWING_ROWS:
    case WindowBoundary::EXPR_FOLLOWING_RANGE:
      return BoundType::kFollowing;
    case WindowBoundary::UNBOUNDED_FOLLOWING:
      return BoundType::kUnboundedFollowing;
    case WindowBoundary::UNBOUNDED_PRECEDING:
      return BoundType::kUnboundedPreceding;
    case WindowBoundary::INVALID:
      VELOX_UNREACHABLE();
  }
  VELOX_UNREACHABLE();
}

} // namespace

IExprWindowFunction parseWindowExpr(
    const std::string& windowString,
    const ParseOptions& options) {
  auto parsedExpr = parseSingleExpression(windowString);
  VELOX_CHECK(
      parsedExpr->IsWindow(),
      "Invalid window function expression: {}",
      windowString);

  IExprWindowFunction windowIExpr;
  auto& windowExpr = dynamic_cast<WindowExpression&>(*parsedExpr);
  for (int i = 0; i < windowExpr.partitions.size(); i++) {
    windowIExpr.partitionBy.push_back(
        parseExpr(*(windowExpr.partitions[i].get()), options));
  }

  for (const auto& orderByNode : windowExpr.orders) {
    const bool ascending = isAscending(orderByNode.type, windowString);
    const bool nullsFirst = isNullsFirst(orderByNode.null_order, windowString);
    windowIExpr.orderBy.emplace_back(
        parseExpr(*orderByNode.expression, options),
        core::SortOrder(ascending, nullsFirst));
  }

  std::vector<std::shared_ptr<const core::IExpr>> params;
  params.reserve(windowExpr.children.size());
  for (const auto& c : windowExpr.children) {
    params.emplace_back(parseExpr(*c, options));
  }

  // Lead and Lag functions have extra offset and default_value arguments.
  if (windowExpr.offset_expr) {
    params.emplace_back(parseExpr(*windowExpr.offset_expr, options));
  }
  if (windowExpr.default_expr) {
    params.emplace_back(parseExpr(*windowExpr.default_expr, options));
  }

  auto func = normalizeFuncName(windowExpr.function_name);
  windowIExpr.functionCall =
      callExpr(func, std::move(params), getAlias(windowExpr), options);

  windowIExpr.ignoreNulls = windowExpr.ignore_nulls;

  windowIExpr.frame.type = parseWindowType(windowExpr);
  windowIExpr.frame.startType = parseBoundType(windowExpr.start);
  if (windowExpr.start_expr) {
    windowIExpr.frame.startValue =
        parseExpr(*windowExpr.start_expr.get(), options);
  }

  windowIExpr.frame.endType = parseBoundType(windowExpr.end);
  if (windowExpr.end_expr) {
    windowIExpr.frame.endValue = parseExpr(*windowExpr.end_expr.get(), options);
  }
  return windowIExpr;
}

} // namespace facebook::velox::duckdb
