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

namespace facebook::velox::exec::test {

void appendComma(int32_t i, std::stringstream& sql) {
  if (i > 0) {
    sql << ", ";
  }
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
      const auto& signature = lambda->signature();
      const auto& body =
          std::dynamic_pointer_cast<const core::CallTypedExpr>(lambda->body());
      VELOX_CHECK_NOT_NULL(body);

      sql << "(";
      for (auto j = 0; j < signature->size(); ++j) {
        appendComma(j, sql);
        sql << signature->nameOf(j);
      }

      sql << ") -> " << toCallSql(body);
    } else {
      VELOX_NYI("Unsupported input expression: {}.", input->toString());
    }
  }
}

std::string toCallSql(const core::CallTypedExprPtr& call) {
  std::stringstream sql;
  sql << call->name() << "(";
  toCallInputsSql(call->inputs(), sql);
  sql << ")";
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
