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
#pragma once

#include "velox/core/PlanNode.h"

namespace facebook::velox::exec::test {

/// Appends a comma to a given stringstream if the provided integer is greater
/// than 0.
void appendComma(int32_t i, std::stringstream& sql);

/// Return the SQL string of type.
std::string toTypeSql(const TypePtr& type);

/// Converts input expressions into SQL string and appends to a given
/// stringstream.
void toCallInputsSql(
    const std::vector<core::TypedExprPtr>& inputs,
    std::stringstream& sql);

/// Converts a call expression into a SQL string.
std::string toCallSql(const core::CallTypedExprPtr& call);

/// Convert a cast expression into a SQL string.
std::string toCastSql(const core::CastTypedExpr& cast);

/// Convert a concat expression into a SQL string.
std::string toConcatSql(const core::ConcatTypedExpr& concat);

/// Convert a constant expression into a SQL string.
///
/// Constant expressions of complex types, timestamp with timezone, interval,
/// and decimal types are not supported yet.
std::string toConstantSql(const core::ConstantTypedExpr& constant);

// Converts aggregate call expression into a SQL string.
std::string toAggregateCallSql(
    const core::CallTypedExprPtr& call,
    const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys,
    const std::vector<core::SortOrder>& sortingOrders,
    bool distinct);

class PrestoSqlPlanNodeVisitorContext : public core::PlanNodeVisitorContext {
 public:
  std::optional<std::string> sql;
};

class PrestoSqlPlanNodeVisitor : public core::PlanNodeVisitor {
 public:
  explicit PrestoSqlPlanNodeVisitor() {}

  void visit(const core::HashJoinNode& node, core::PlanNodeVisitorContext& ctx)
      const override;

  void visit(
      const core::NestedLoopJoinNode& node,
      core::PlanNodeVisitorContext& ctx) const override;

  void visit(
      const core::SpatialJoinNode& node,
      core::PlanNodeVisitorContext& ctx) const override;

  void visit(const core::TableScanNode& node, core::PlanNodeVisitorContext& ctx)
      const override;

  void visit(const core::ValuesNode& node, core::PlanNodeVisitorContext& ctx)
      const override;

 protected:
  std::optional<std::string> toSql(const core::PlanNodePtr& node) const;
};
} // namespace facebook::velox::exec::test
