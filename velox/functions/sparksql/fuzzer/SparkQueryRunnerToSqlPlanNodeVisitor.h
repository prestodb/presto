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

namespace facebook::velox::functions::sparksql::fuzzer {

class SparkQueryRunnerToSqlPlanNodeVisitor
    : public exec::test::PrestoSqlPlanNodeVisitor {
 public:
  explicit SparkQueryRunnerToSqlPlanNodeVisitor()
      : exec::test::PrestoSqlPlanNodeVisitor() {}

  void visit(
      const core::AggregationNode& node,
      core::PlanNodeVisitorContext& ctx) const override;

  void visit(const core::ArrowStreamNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::AssignUniqueIdNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::EnforceSingleRowNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::ExchangeNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::ExpandNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::FilterNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::GroupIdNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::HashJoinNode& node, core::PlanNodeVisitorContext& ctx)
      const override {
    PrestoSqlPlanNodeVisitor::visit(node, ctx);
  }

  void visit(const core::IndexLookupJoinNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::LimitNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::LocalMergeNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::LocalPartitionNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::MarkDistinctNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::MergeExchangeNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::MergeJoinNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(
      const core::NestedLoopJoinNode& node,
      core::PlanNodeVisitorContext& ctx) const override {
    PrestoSqlPlanNodeVisitor::visit(node, ctx);
  }

  void visit(const core::OrderByNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::PartitionedOutputNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::ProjectNode& node, core::PlanNodeVisitorContext& ctx)
      const override;

  void visit(const core::ParallelProjectNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::RowNumberNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::TableScanNode& node, core::PlanNodeVisitorContext& ctx)
      const override {
    PrestoSqlPlanNodeVisitor::visit(node, ctx);
  }

  void visit(const core::TableWriteNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::TableWriteMergeNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::TopNNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::TopNRowNumberNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::TraceScanNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::UnnestNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  void visit(const core::ValuesNode& node, core::PlanNodeVisitorContext& ctx)
      const override;

  void visit(const core::WindowNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }

  // Used to visit custom PlanNodes that extend the set provided by Velox.
  void visit(const core::PlanNode&, core::PlanNodeVisitorContext&)
      const override {
    VELOX_NYI();
  }
};

} // namespace facebook::velox::functions::sparksql::fuzzer
