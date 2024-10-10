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

#include "velox/exec/fuzzer/ToSQLUtil.h"

#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::exec::test {

void appendComma(int32_t i, std::stringstream& sql) {
  if (i > 0) {
    sql << ", ";
  }
}

// Returns the SQL string of the given type.
std::string toTypeSql(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::ARRAY:
      return fmt::format("array({})", toTypeSql(type->childAt(0)));
    case TypeKind::MAP:
      return fmt::format(
          "map({}, {})",
          toTypeSql(type->childAt(0)),
          toTypeSql(type->childAt(1)));
    case TypeKind::ROW: {
      const auto& rowType = type->asRow();
      std::stringstream sql;
      sql << "row(";
      for (auto i = 0; i < type->size(); ++i) {
        appendComma(i, sql);
        sql << rowType.nameOf(i) << " " << toTypeSql(type->childAt(i));
      }
      sql << ")";
      return sql.str();
    }
    case TypeKind::VARCHAR:
      if (isJsonType(type)) {
        return "json";
      } else {
        return "varchar";
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

void toCallInputsSql(
    const std::vector<core::TypedExprPtr>& inputs,
    std::stringstream& sql) {
  for (auto i = 0; i < inputs.size(); ++i) {
    appendComma(i, sql);

    const auto& input = inputs.at(i);
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                input)) {
      sql << field->name();
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
      sql << toConstantSql(constantArg);
    } else if (
        auto castArg =
            std::dynamic_pointer_cast<const core::CastTypedExpr>(input)) {
      sql << toCastSql(castArg);
    } else if (
        auto concatArg =
            std::dynamic_pointer_cast<const core::ConcatTypedExpr>(input)) {
      sql << toConcatSql(concatArg);
    } else {
      VELOX_NYI("Unsupported input expression: {}.", input->toString());
    }
  }
}

std::string toCallSql(const core::CallTypedExprPtr& call) {
  std::stringstream sql;
  // Some functions require special SQL syntax, so handle them first.
  if (call->name() == "in") {
    VELOX_CHECK_GE(call->inputs().size(), 2);
    toCallInputsSql({call->inputs()[0]}, sql);
    sql << " in (";
    for (auto i = 1; i < call->inputs().size(); ++i) {
      appendComma(i - 1, sql);
      toCallInputsSql({call->inputs()[i]}, sql);
    }
    sql << ")";
  } else if (call->name() == "like") {
    toCallInputsSql({call->inputs()[0]}, sql);
    sql << " like ";
    toCallInputsSql({call->inputs()[1]}, sql);
    if (call->inputs().size() == 3) {
      sql << " escape ";
      toCallInputsSql({call->inputs()[2]}, sql);
    }
  } else if (call->name() == "or" || call->name() == "and") {
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
  } else {
    // Regular function call syntax.
    sql << call->name() << "(";
    toCallInputsSql(call->inputs(), sql);
    sql << ")";
  }
  return sql.str();
}

std::string toCastSql(const core::CastTypedExprPtr& cast) {
  std::stringstream sql;
  if (cast->nullOnFailure()) {
    sql << "try_cast(";
  } else {
    sql << "cast(";
  }
  toCallInputsSql(cast->inputs(), sql);
  sql << " as " << toTypeSql(cast->type());
  sql << ")";
  return sql.str();
}

std::string toConcatSql(const core::ConcatTypedExprPtr& concat) {
  std::stringstream sql;
  sql << "concat(";
  toCallInputsSql(concat->inputs(), sql);
  sql << ")";
  return sql.str();
}

// Constant expressions of complex types, timestamp with timezone, interval, and
// decimal types are not supported yet.
std::string toConstantSql(const core::ConstantTypedExprPtr& constant) {
  // Escape single quote in string literals used in SQL texts.
  auto escape = [](const std::string& input) -> std::string {
    std::string result;
    result.reserve(input.size());
    for (auto i = 0; i < input.size(); ++i) {
      if (input[i] == '\'') {
        result.push_back('\'');
      }
      result.push_back(input[i]);
    }
    return result;
  };

  std::stringstream sql;
  if (constant->toString() == "null") {
    // Syntax like BIGINT 'null' for typed null is not supported, so use cast
    // instead.
    sql << fmt::format("cast(null as {})", toTypeSql(constant->type()));
  } else if (constant->type()->isVarchar() || constant->type()->isVarbinary()) {
    sql << fmt::format(
        "{} '{}'",
        toTypeSql(constant->type()),
        escape(constant->valueVector()->toString(0)));
  } else if (constant->type()->isPrimitiveType()) {
    sql << fmt::format(
        "{} '{}'", toTypeSql(constant->type()), constant->toString());
  } else {
    VELOX_NYI(
        "Constant expressions of {} are not supported yet.",
        constant->type()->toString());
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

} // namespace facebook::velox::exec::test
