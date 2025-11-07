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
#include "presto_cpp/main/tool/trace/PartitionAndSerializeReplayer.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "velox/tool/trace/TraceReplayTaskRunner.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::presto;

namespace facebook::velox::tool::trace {

PartitionAndSerializeReplayer::PartitionAndSerializeReplayer(
    const std::string& traceDir,
    const std::string& queryId,
    const std::string& taskId,
    const std::string& nodeId,
    const std::string& nodeName,
    const std::string& spillBaseDir,
    const std::string& driverIds,
    uint64_t queryCapacity,
    folly::Executor* executor)
    : OperatorReplayerBase(
          traceDir,
          queryId,
          taskId,
          nodeId,
          nodeName,
          spillBaseDir,
          driverIds,
          queryCapacity,
          executor) {}

RowVectorPtr PartitionAndSerializeReplayer::run(bool copyResults) {
  TraceReplayTaskRunner traceTaskRunner(createPlan(), createQueryCtx());
  auto [task, result] =
      traceTaskRunner.maxDrivers(driverIds_.size()).run(copyResults);
  printStats(task);
  return result;
}

core::PlanNodePtr PartitionAndSerializeReplayer::createPlanNode(
    const core::PlanNode* node,
    const core::PlanNodeId& nodeId,
    const core::PlanNodePtr& source) const {
  const auto partitionAndSerializeNode =
      dynamic_cast<const presto::operators::PartitionAndSerializeNode*>(node);
  VELOX_CHECK_NOT_NULL(partitionAndSerializeNode);

  return std::make_shared<presto::operators::PartitionAndSerializeNode>(
      nodeId,
      partitionAndSerializeNode->keys(),
      partitionAndSerializeNode->numPartitions(),
      partitionAndSerializeNode->serializedRowType(),
      source,
      partitionAndSerializeNode->isReplicateNullsAndAny(),
      partitionAndSerializeNode->partitionFunctionFactory(),
      partitionAndSerializeNode->sortingOrders(),
      partitionAndSerializeNode->sortingKeys());
}

} // namespace facebook::velox::tool::trace
