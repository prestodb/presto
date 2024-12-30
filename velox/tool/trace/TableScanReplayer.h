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

#include <utility>

#include "velox/core/PlanNode.h"
#include "velox/exec/Split.h"
#include "velox/tool/trace/OperatorReplayerBase.h"

namespace facebook::velox::tool::trace {

/// The replayer to replay the traced 'TableScan' operators.
class TableScanReplayer final : public OperatorReplayerBase {
 public:
  TableScanReplayer(
      const std::string& traceDir,
      const std::string& queryId,
      const std::string& taskId,
      const std::string& nodeId,
      const std::string& operatorType,
      const std::string& driverIds,
      uint64_t queryCapacity,
      folly::Executor* executor)
      : OperatorReplayerBase(
            traceDir,
            queryId,
            taskId,
            nodeId,
            operatorType,
            driverIds,
            queryCapacity,
            executor) {}

  RowVectorPtr run(bool copyResults = true) override;

 private:
  core::PlanNodePtr createPlanNode(
      const core::PlanNode* node,
      const core::PlanNodeId& nodeId,
      const core::PlanNodePtr& /*source*/) const override;

  std::vector<exec::Split> getSplits() const;
};

} // namespace facebook::velox::tool::trace
