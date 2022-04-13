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
#include <velox/core/Expressions.h>
#include <velox/core/ITypedExpr.h>
#include <velox/core/PlanFragment.h>
#include <velox/core/PlanNode.h>
#include "velox/common/memory/Memory.h"

namespace facebook::velox::core {
class IExpr;
}

namespace facebook::velox::exec::test {

/// Generates unique sequential plan node IDs starting with zero or specified
/// value.
class PlanNodeIdGenerator {
 public:
  explicit PlanNodeIdGenerator(int startId = 0) : nextId_{startId} {}

  int next() {
    return nextId_++;
  }

  void reset(int startId = 0) {
    nextId_ = startId;
  }

 private:
  int nextId_;
};

class PlanBuilder {
 public:
  explicit PlanBuilder(
      std::shared_ptr<PlanNodeIdGenerator> planNodeIdGenerator,
      memory::MemoryPool* pool = nullptr)
      : planNodeIdGenerator_{std::move(planNodeIdGenerator)}, pool_{pool} {}

  explicit PlanBuilder(memory::MemoryPool* pool = nullptr)
      : PlanBuilder(std::make_shared<PlanNodeIdGenerator>(), pool) {}

  /// Add a TableScanNode to scan a Hive table.
  ///
  /// @param outputType List of column names and types to read from the table.
  /// @param subfieldFilters A list of SQL expressions for the range filters to
  /// apply to individual columns. Supported filters are: column <= value,
  /// column < value, column >= value, column > value, column = value, column IN
  /// (v1, v2,.. vN), column < v1 OR column >= v2.
  /// @param remainingFilter SQL expression for the additional conjunct. May
  /// include multiple columns and SQL functions. The remainingFilter is AND'ed
  /// with all the subfieldFilters.
  PlanBuilder& tableScan(
      const RowTypePtr& outputType,
      const std::vector<std::string>& subfieldFilters = {},
      const std::string& remainingFilter = "");

  PlanBuilder& tableScan(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& assignments);

  PlanBuilder& values(
      const std::vector<RowVectorPtr>& values,
      bool parallelizable = false);

  PlanBuilder& exchange(const RowTypePtr& outputType);

  /// Adds a MergeExchangeNode using specified ORDER BY clauses.
  ///
  /// For example,
  ///
  ///     .mergeExchange(outputRowType, {"a", "b DESC", "c ASC NULLS FIRST"})
  ///
  /// By default, uses ASC NULLS LAST sort order, e.g. column "a" above will use
  /// ASC NULLS LAST and column "b" will use DESC NULLS LAST.
  PlanBuilder& mergeExchange(
      const RowTypePtr& outputType,
      const std::vector<std::string>& keys);

  /// Adds a ProjectNode using specified SQL expressions.
  ///
  /// For example,
  ///
  ///     .project({"a + b", "c * 3"})
  ///
  /// The names of the projections can be specified using SQL statement AS:
  ///
  ///     .project({"a + b AS sum_ab", "c * 3 AS triple_c"})
  ///
  /// If AS statement is not used, the names of the projections will be
  /// generated as p0, p1, p2, etc. Names of columns projected as is will be
  /// preserved.
  ///
  /// For example,
  ///
  ///     project({"a + b AS sum_ab", "c", "d * 7")
  ///
  /// will produce projected columns named sum_ab, c and p2.
  PlanBuilder& project(const std::vector<std::string>& projections);

  PlanBuilder& filter(const std::string& filter);

  PlanBuilder& tableWrite(
      const std::vector<std::string>& columnNames,
      const std::shared_ptr<core::InsertTableHandle>& insertHandle,
      const std::string& rowCountColumnName = "rowCount");

  PlanBuilder& tableWrite(
      const RowTypePtr& columns,
      const std::vector<std::string>& columnNames,
      const std::shared_ptr<core::InsertTableHandle>& insertHandle,
      const std::string& rowCountColumnName = "rowCount");

  /// Adds an AggregationNode representing partial aggregation with the
  /// specified grouping keys, aggregates and optional masks.
  ///
  /// Grouping keys are specified using zero-based indices into the input
  /// columns.
  ///
  /// Aggregates are specified as function calls over unmodified input columns,
  /// e.g. sum(a), avg(b), min(c). SQL statement AS can be used to specify names
  /// for the aggregation result columns. In the absence of AS statement, result
  /// columns are named a0, a1, a2, etc.
  ///
  /// For example,
  ///
  ///     partialAggregation({}, {"min(a) AS min_a", "max(b)"})
  ///
  /// will produce output columns min_a and a1, while
  ///
  ///     partialAggregation({0, 1}, {"min(a) AS min_a", "max(b)"})
  ///
  /// will produce output columns k1, k2, min_a and a1, assuming the names of
  /// the first two input columns are k1 and k2.
  PlanBuilder& partialAggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks = {}) {
    return aggregation(
        groupingKeys,
        {},
        aggregates,
        masks,
        core::AggregationNode::Step::kPartial,
        false);
  }

  /// Add final aggregation plan node to match the current partial aggregation
  /// node. Should be called directly after partialAggregation() method or
  /// directly after intermediateAggregation() that follows
  /// partialAggregation().
  PlanBuilder& finalAggregation();

  // @param resultTypes Optional list of result types for the aggregates. Use it
  // to specify the result types for aggregates which cannot infer result type
  // solely from the types of the intermediate results.
  PlanBuilder& finalAggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<TypePtr>& resultTypes) {
    return aggregation(
        groupingKeys,
        {},
        aggregates,
        {},
        core::AggregationNode::Step::kFinal,
        false,
        resultTypes);
  }

  /// Add intermediate aggregation plan node to match the current partial
  /// aggregation node. Should be called directly after partialAggregation()
  /// method.
  PlanBuilder& intermediateAggregation();

  PlanBuilder& intermediateAggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<TypePtr>& resultTypes) {
    return aggregation(
        groupingKeys,
        {},
        aggregates,
        {},
        core::AggregationNode::Step::kIntermediate,
        false,
        resultTypes);
  }

  PlanBuilder& singleAggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates) {
    return aggregation(
        groupingKeys,
        {},
        aggregates,
        {},
        core::AggregationNode::Step::kSingle,
        false);
  }

  PlanBuilder& aggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      core::AggregationNode::Step step,
      bool ignoreNullKeys,
      const std::vector<TypePtr>& resultTypes = {}) {
    return aggregation(
        groupingKeys, {}, aggregates, masks, step, ignoreNullKeys, resultTypes);
  }

  PlanBuilder& aggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<ChannelIndex>& preGroupedKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      core::AggregationNode::Step step,
      bool ignoreNullKeys,
      const std::vector<TypePtr>& resultTypes = {});

  PlanBuilder& partialStreamingAggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks = {}) {
    return streamingAggregation(
        groupingKeys,
        aggregates,
        masks,
        core::AggregationNode::Step::kPartial,
        false);
  }

  PlanBuilder& finalStreamingAggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<TypePtr>& resultTypes = {}) {
    return streamingAggregation(
        groupingKeys,
        aggregates,
        {},
        core::AggregationNode::Step::kFinal,
        false,
        resultTypes);
  }

  PlanBuilder& streamingAggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      core::AggregationNode::Step step,
      bool ignoreNullKeys,
      const std::vector<TypePtr>& resultTypes = {});

  /// Adds a LocalMergeNode using specified ORDER BY clauses.
  ///
  /// For example,
  ///
  ///     .localMerge({"a", "b DESC", "c ASC NULLS FIRST"})
  ///
  /// By default, uses ASC NULLS LAST sort order, e.g. column "a" above will use
  /// ASC NULLS LAST and column "b" will use DESC NULLS LAST.
  PlanBuilder& localMerge(
      const std::vector<std::string>& keys,
      std::vector<std::shared_ptr<const core::PlanNode>> sources);

  /// Adds an OrderByNode using specified ORDER BY clauses.
  ///
  /// For example,
  ///
  ///     .orderBy({"a", "b DESC", "c ASC NULLS FIRST"})
  ///
  /// By default, uses ASC NULLS LAST sort order, e.g. column "a" above will use
  /// ASC NULLS LAST and column "b" will use DESC NULLS LAST.
  PlanBuilder& orderBy(const std::vector<std::string>& keys, bool isPartial);

  /// Adds a TopNNode using specified N and ORDER BY clauses.
  ///
  /// For example,
  ///
  ///     .topN({"a", "b DESC", "c ASC NULLS FIRST"}, 10, true)
  ///
  /// By default, uses ASC NULLS LAST sort order, e.g. column "a" above will use
  /// ASC NULLS LAST and column "b" will use DESC NULLS LAST.
  PlanBuilder&
  topN(const std::vector<std::string>& keys, int32_t count, bool isPartial);

  PlanBuilder& limit(int32_t offset, int32_t count, bool isPartial);

  PlanBuilder& enforceSingleRow();

  PlanBuilder& assignUniqueId(
      const std::string& idName = "unique",
      const int32_t taskUniqueId = 1);

  std::shared_ptr<const core::FieldAccessTypedExpr> field(int index);

  std::shared_ptr<const core::FieldAccessTypedExpr> field(
      const std::string& name);

  PlanBuilder& partitionedOutput(
      const std::vector<std::string>& keys,
      int numPartitions,
      const std::vector<std::string>& outputLayout = {});

  PlanBuilder& partitionedOutputBroadcast(
      const std::vector<std::string>& outputLayout = {});

  PlanBuilder& partitionedOutput(
      const std::vector<std::string>& keys,
      int numPartitions,
      bool replicateNullsAndAny,
      const std::vector<std::string>& outputLayout = {});

  PlanBuilder& localPartition(
      const std::vector<std::string>& keys,
      const std::vector<std::shared_ptr<const core::PlanNode>>& sources,
      const std::vector<std::string>& outputLayout = {});

  PlanBuilder& localPartitionRoundRobin(
      const std::vector<std::shared_ptr<const core::PlanNode>>& sources,
      const std::vector<std::string>& outputLayout = {});

  // 'leftKeys' and 'rightKeys' are column names of the output of the
  // previous PlanNode and 'build', respectively. 'output' is a subset of
  // column names from the left and right sides of the join to project out.
  // 'filterText', if non-empty, is an expression over
  // the concatenation of columns of the previous PlanNode and
  // 'build'. This may be wider than output.
  PlanBuilder& hashJoin(
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const std::shared_ptr<core::PlanNode>& build,
      const std::string& filterText,
      const std::vector<std::string>& output,
      core::JoinType joinType = core::JoinType::kInner);

  PlanBuilder& mergeJoin(
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const std::shared_ptr<core::PlanNode>& build,
      const std::string& filterText,
      const std::vector<std::string>& output,
      core::JoinType joinType = core::JoinType::kInner);

  PlanBuilder& crossJoin(
      const std::shared_ptr<core::PlanNode>& build,
      const std::vector<std::string>& output);

  PlanBuilder& unnest(
      const std::vector<std::string>& replicateColumns,
      const std::vector<std::string>& unnestColumns,
      const std::optional<std::string>& ordinalColumn = std::nullopt);

  PlanBuilder& capturePlanNodeId(core::PlanNodeId& id) {
    VELOX_CHECK_NOT_NULL(planNode_);
    id = planNode_->id();
    return *this;
  }

  const std::shared_ptr<core::PlanNode>& planNode() const {
    return planNode_;
  }

  core::PlanFragment planFragment() const {
    return core::PlanFragment{planNode_};
  }

  // Adds a user defined PlanNode as the root of the plan. 'func' takes
  // the current root of the plan and returns the new root.
  PlanBuilder& addNode(std::function<std::shared_ptr<core::PlanNode>(
                           std::string nodeId,
                           std::shared_ptr<const core::PlanNode>)> func) {
    planNode_ = func(nextPlanNodeId(), planNode_);
    return *this;
  }

 private:
  std::string nextPlanNodeId();

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const std::vector<std::string>& names);

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const RowTypePtr& inputType,
      const std::vector<std::string>& names);

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const std::vector<ChannelIndex>& indices);

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const RowTypePtr& inputType,
      const std::vector<ChannelIndex>& indices);

  std::shared_ptr<const core::FieldAccessTypedExpr> field(
      const RowTypePtr& inputType,
      int index);

  std::shared_ptr<const core::FieldAccessTypedExpr> field(
      const RowTypePtr& inputType,
      const std::string& name);

  std::shared_ptr<core::PlanNode> createIntermediateOrFinalAggregation(
      core::AggregationNode::Step step,
      const core::AggregationNode* partialAggNode);

  std::shared_ptr<const core::ITypedExpr> inferTypes(
      const std::shared_ptr<const core::IExpr>& untypedExpr);

  struct AggregateExpressionsAndNames {
    std::vector<std::shared_ptr<const core::CallTypedExpr>> aggregates;
    std::vector<std::string> names;
  };

  AggregateExpressionsAndNames createAggregateExpressionsAndNames(
      const std::vector<std::string>& aggregates,
      core::AggregationNode::Step step,
      const std::vector<TypePtr>& resultTypes);

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
  createAggregateMasks(
      size_t numAggregates,
      const std::vector<std::string>& masks);

  std::shared_ptr<PlanNodeIdGenerator> planNodeIdGenerator_;
  std::shared_ptr<core::PlanNode> planNode_;
  memory::MemoryPool* pool_;
};
} // namespace facebook::velox::exec::test
