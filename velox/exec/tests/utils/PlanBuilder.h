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
#include <velox/core/PlanNode.h>
#include "velox/common/memory/Memory.h"

namespace facebook::velox::exec::test {

class PlanBuilder {
 public:
  PlanBuilder(int planNodeId = 0, memory::MemoryPool* pool = nullptr)
      : planNodeId_{planNodeId}, pool_{pool} {}

  explicit PlanBuilder(memory::MemoryPool* pool)
      : planNodeId_{0}, pool_{pool} {}

  PlanBuilder& tableScan(const RowTypePtr& outputType);

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

  PlanBuilder& mergeExchange(
      const RowTypePtr& outputType,
      const std::vector<ChannelIndex>& keyIndices,
      const std::vector<core::SortOrder>& sortOrder);

  PlanBuilder& project(
      const std::vector<std::string>& projections,
      const std::vector<std::string>& names = {});

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

  PlanBuilder& partialAggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks = {},
      const std::vector<TypePtr>& resultTypes = {}) {
    return aggregation(
        groupingKeys,
        aggregates,
        masks,
        core::AggregationNode::Step::kPartial,
        false,
        resultTypes);
  }

  // @param resultTypes Optional list of result types for the aggregates. Use it
  // to specify the result types for aggregates which cannot infer result type
  // solely from the types of the intermediate results.
  PlanBuilder& finalAggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<TypePtr>& resultTypes = {}) {
    return aggregation(
        groupingKeys,
        aggregates,
        {},
        core::AggregationNode::Step::kFinal,
        false,
        resultTypes);
  }

  PlanBuilder& intermediateAggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<TypePtr>& resultTypes = {}) {
    return aggregation(
        groupingKeys,
        aggregates,
        {},
        core::AggregationNode::Step::kIntermediate,
        false,
        resultTypes);
  }

  PlanBuilder& singleAggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<TypePtr>& resultTypes = {}) {
    return aggregation(
        groupingKeys,
        aggregates,
        {},
        core::AggregationNode::Step::kSingle,
        false,
        resultTypes);
  }

  PlanBuilder& aggregation(
      const std::vector<ChannelIndex>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      core::AggregationNode::Step step,
      bool ignoreNullKeys,
      const std::vector<TypePtr>& resultTypes = {});

  PlanBuilder& localMerge(
      const std::vector<ChannelIndex>& keyIndices,
      const std::vector<core::SortOrder>& sortOrder);

  PlanBuilder& orderBy(
      const std::vector<ChannelIndex>& keyIndices,
      const std::vector<core::SortOrder>& sortOrder,
      bool isPartial);

  PlanBuilder& topN(
      const std::vector<ChannelIndex>& keyIndices,
      const std::vector<core::SortOrder>& sortOrder,
      int32_t count,
      bool isPartial);

  PlanBuilder& limit(int32_t offset, int32_t count, bool isPartial);

  PlanBuilder& enforceSingleRow();

  PlanBuilder& assignUniqueId();

  std::shared_ptr<const core::FieldAccessTypedExpr> field(int index);

  std::shared_ptr<const core::FieldAccessTypedExpr> field(
      const std::string& name);

  PlanBuilder& partitionedOutput(
      const std::vector<ChannelIndex>& keyIndices,
      int numPartitions,
      const std::vector<ChannelIndex>& outputLayout = {});

  PlanBuilder& partitionedOutputBroadcast(
      const std::vector<ChannelIndex>& outputLayout = {});

  PlanBuilder& partitionedOutput(
      const std::vector<ChannelIndex>& keyIndices,
      int numPartitions,
      bool replicateNullsAndAny,
      const std::vector<ChannelIndex>& outputLayout = {});

  PlanBuilder& localPartition(
      const std::vector<ChannelIndex>& keyIndices,
      const std::vector<std::shared_ptr<const core::PlanNode>>& sources,
      const std::vector<ChannelIndex>& outputLayout = {});

  // 'leftKeys' and 'rightKeys' are indices into the output type of the
  // previous PlanNode and 'build', respectively.  'output' is indices
  // into the concatenation of the previous node's output and the
  // output of 'build'.  'filterText', if non-empty, is an expression over
  // the concatenation of columns of the previous PlanNode and
  // 'build'. This may be wider than output.
  PlanBuilder& hashJoin(
      const std::vector<ChannelIndex>& leftKeys,
      const std::vector<ChannelIndex>& rightKeys,
      const std::shared_ptr<core::PlanNode>& build,
      const std::string& filterText,
      const std::vector<ChannelIndex>& output,
      core::JoinType joinType = core::JoinType::kInner);

  PlanBuilder& mergeJoin(
      const std::vector<ChannelIndex>& leftKeys,
      const std::vector<ChannelIndex>& rightKeys,
      const std::shared_ptr<core::PlanNode>& build,
      const std::string& filterText,
      const std::vector<ChannelIndex>& output,
      core::JoinType joinType = core::JoinType::kInner);

  PlanBuilder& crossJoin(
      const std::shared_ptr<core::PlanNode>& build,
      const std::vector<ChannelIndex>& output);

  PlanBuilder& unnest(
      const std::vector<std::string>& replicateColumns,
      const std::vector<std::string>& unnestColumns,
      const std::optional<std::string>& ordinalColumn = std::nullopt);

  const std::shared_ptr<core::PlanNode>& planNode() const {
    return planNode_;
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
      const std::vector<ChannelIndex>& indices);
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const RowTypePtr inputType,
      const std::vector<ChannelIndex>& indices);

  std::shared_ptr<const core::FieldAccessTypedExpr> field(
      const RowTypePtr& outputType,
      int index);

  int planNodeId_;
  std::shared_ptr<core::PlanNode> planNode_;
  memory::MemoryPool* pool_;
};
} // namespace facebook::velox::exec::test
