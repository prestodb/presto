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
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 */
#include "presto_cpp/main/tool/trace/BroadcastWriteReplayer.h"

#include "presto_cpp/main/operators/BroadcastWrite.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::velox::tool::trace {

BroadcastWriteReplayer::BroadcastWriteReplayer(
    const std::string& traceDir,
    const std::string& queryId,
    const std::string& taskId,
    const std::string& nodeId,
    const std::string& nodeName,
    const std::string& driverIds,
    uint64_t queryCapacity,
    folly::Executor* executor,
    const std::string& replayOutputDir)
    : OperatorReplayerBase(
          traceDir,
          queryId,
          taskId,
          nodeId,
          nodeName,
          "",
          driverIds,
          queryCapacity,
          executor),
      replayOutputDir_(replayOutputDir) {
  VELOX_CHECK(!replayOutputDir_.empty());
}

core::PlanNodePtr BroadcastWriteReplayer::createPlanNode(
    const core::PlanNode* node,
    const core::PlanNodeId& nodeId,
    const core::PlanNodePtr& source) const {
  const auto* broadcastWriteNode =
      dynamic_cast<const facebook::presto::operators::BroadcastWriteNode*>(
          node);
  VELOX_CHECK_NOT_NULL(broadcastWriteNode);

  return std::make_shared<facebook::presto::operators::BroadcastWriteNode>(
      nodeId,
      replayOutputDir_,
      broadcastWriteNode->maxBroadcastBytes(),
      broadcastWriteNode->serdeRowType(),
      source);
}

} // namespace facebook::velox::tool::trace
