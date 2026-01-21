/*
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

#include <stdexcept>
#include <vector>
#include "presto_cpp/main/operators/ShuffleInterface.h"
#include "presto_cpp/main/tvf/core/TableFunctionProcessorNode.h"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Variant.h"

#include "presto_cpp/main/types/PrestoTaskId.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/main/types/TypeParser.h"

namespace facebook::presto {

class VeloxQueryPlanConverterBase {
 public:
  VeloxQueryPlanConverterBase(
      velox::core::QueryCtx* queryCtx,
      velox::memory::MemoryPool* pool)
      : pool_(pool), queryCtx_{queryCtx}, exprConverter_(pool, &typeParser_) {}

  virtual ~VeloxQueryPlanConverterBase() = default;

  virtual velox::core::PlanFragment toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  // visible for testing
  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::PlanNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

 protected:
  virtual velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::RemoteSourceNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId) = 0;

  virtual velox::connector::CommitStrategy getCommitStrategy() const = 0;

  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::OutputNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::ExchangeNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::FilterNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::ProjectNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::ProjectNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::ValuesNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::ValuesNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::TableScanNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::TableScanNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::AggregationNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::AggregationNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::GroupIdNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::GroupIdNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::DistinctLimitNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::JoinNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::SemiJoinNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::SpatialJoinNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::IndexLookupJoinNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::IndexJoinNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::TableScanNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::IndexSourceNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::MarkDistinctNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::MergeJoinNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::TopNNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::TopNNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::LimitNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::LimitNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::OrderByNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::SortNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::TableWriteNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::TableWriterNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::TableWriteNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::DeleteNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::TableWriteMergeNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::TableWriterMergeNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::UnnestNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::UnnestNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::EnforceSingleRowNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::EnforceSingleRowNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::AssignUniqueIdNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::AssignUniqueId>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::WindowNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::WindowNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::RowNumberNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::RowNumberNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::TopNRowNumberNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const tvf::TableFunctionProcessorNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::TableFunctionProcessorNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::vector<velox::core::FieldAccessTypedExprPtr> toVeloxExprs(
      const std::vector<protocol::VariableReferenceExpression>& variables);

  std::shared_ptr<const velox::core::ProjectNode> tryConvertOffsetLimit(
      const std::shared_ptr<const protocol::ProjectNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::WindowNode::Function toVeloxWindowFunction(
      const protocol::Function& func);

  velox::VectorPtr evaluateConstantExpression(
      const velox::core::TypedExprPtr& expression);

  std::optional<velox::core::ColumnStatsSpec> toColumnStatsSpec(
      const std::shared_ptr<protocol::StatisticAggregations>&
          statisticsAggregation,
      velox::core::AggregationNode::Step step,
      const protocol::PlanNodeId& id,
      const velox::core::PlanNodePtr& sourceVeloxPlan,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::vector<protocol::VariableReferenceExpression> generateOutputVariables(
      const std::vector<protocol::VariableReferenceExpression>&
          nonStatisticsOutputVariables,
      const std::shared_ptr<protocol::StatisticAggregations>&
          statisticsAggregation);

  void toAggregations(
      const std::vector<protocol::VariableReferenceExpression>& outputVariables,
      const std::map<
          protocol::VariableReferenceExpression,
          protocol::Aggregation>& aggregationMap,
      std::vector<velox::core::AggregationNode::Aggregate>& aggregates,
      std::vector<std::string>& aggregateNames);

  velox::memory::MemoryPool* const pool_;
  velox::core::QueryCtx* const queryCtx_;
  VeloxExprConverter exprConverter_;
  TypeParser typeParser_;
};

class VeloxInteractiveQueryPlanConverter : public VeloxQueryPlanConverterBase {
 public:
  using VeloxQueryPlanConverterBase::toVeloxQueryPlan;

  explicit VeloxInteractiveQueryPlanConverter(
      velox::core::QueryCtx* queryCtx,
      velox::memory::MemoryPool* pool)
      : VeloxQueryPlanConverterBase(queryCtx, pool) {}

 protected:
  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::RemoteSourceNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId) override;

  velox::connector::CommitStrategy getCommitStrategy() const override;
};

class VeloxBatchQueryPlanConverter : public VeloxQueryPlanConverterBase {
 public:
  using VeloxQueryPlanConverterBase::toVeloxQueryPlan;

  VeloxBatchQueryPlanConverter(
      const std::string& shuffleName,
      std::shared_ptr<std::string>&& serializedShuffleWriteInfo,
      std::shared_ptr<std::string>&& broadcastBasePath,
      velox::core::QueryCtx* queryCtx,
      velox::memory::MemoryPool* pool)
      : VeloxQueryPlanConverterBase(queryCtx, pool),
        shuffleName_(shuffleName),
        serializedShuffleWriteInfo_(std::move(serializedShuffleWriteInfo)),
        broadcastBasePath_(std::move(broadcastBasePath)) {}

  velox::core::PlanFragment toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId) override;

 protected:
  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::RemoteSourceNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId) override;

  velox::connector::CommitStrategy getCommitStrategy() const override;

 private:
  const std::string shuffleName_;
  const std::shared_ptr<std::string> serializedShuffleWriteInfo_;
  const std::shared_ptr<std::string> broadcastBasePath_;
};

void registerPrestoPlanNodeSerDe();

void registerPrestoTraceNodeFactories();

void parseSqlFunctionHandle(
    const std::shared_ptr<protocol::SqlFunctionHandle>& sqlFunction,
    std::vector<velox::TypePtr>& rawInputTypes,
    TypeParser& typeParser);

void parseIndexLookupCondition(
    const std::shared_ptr<protocol::RowExpression>& filter,
    const std::vector<protocol::VariableReferenceExpression>& lookupVariables,
    const VeloxExprConverter& exprConverter,
    bool acceptConstant,
    std::vector<velox::core::IndexLookupConditionPtr>& joinConditionPtrs,
    std::vector<velox::core::TypedExprPtr>& unsupportedConditions);
} // namespace facebook::presto
