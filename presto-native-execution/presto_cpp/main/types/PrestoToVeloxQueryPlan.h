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
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Variant.h"

#include "presto_cpp/main/types/PrestoTaskId.h"
// TypeSignatureTypeConverter.h must be included after presto_protocol.h
// because it changes the macro EOF in some way (maybe deleting it?) which
// is used in third_party/json/json.hpp
//
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"

namespace facebook::presto {

class VeloxQueryPlanConverter {
 public:
  explicit VeloxQueryPlanConverter(velox::memory::MemoryPool* pool)
      : pool_(pool), exprConverter_(pool) {}

  velox::core::PlanFragment toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  // visible for testing
  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::PlanNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

 private:
  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::ExchangeNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::ExchangeNode> toVeloxQueryPlan(
      const std::shared_ptr<const protocol::RemoteSourceNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::FilterNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::PlanNodePtr toVeloxQueryPlan(
      const std::shared_ptr<const protocol::OutputNode>& node,
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

  std::vector<velox::core::FieldAccessTypedExprPtr> toVeloxExprs(
      const std::vector<protocol::VariableReferenceExpression>& variables);

  std::shared_ptr<const velox::core::ProjectNode> tryConvertOffsetLimit(
      const std::shared_ptr<const protocol::ProjectNode>& node,
      const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
      const protocol::TaskId& taskId);

  velox::core::WindowNode::Function toVeloxWindowFunction(
      const protocol::Function& func);

  velox::memory::MemoryPool* pool_;
  VeloxExprConverter exprConverter_;
};

} // namespace facebook::presto
