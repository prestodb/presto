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
#include "velox/tool/trace/OperatorReplayerBase.h"

namespace facebook::velox::tool::trace {

/// The replayer to replay the traced 'TableWriter' operator.
class TableWriterReplayer final : public OperatorReplayerBase {
 public:
  TableWriterReplayer(
      const std::string& rootDir,
      const std::string& taskId,
      const std::string& nodeId,
      const int32_t pipelineId,
      const std::string& operatorType,
      const std::string& replayOutputDir)
      : OperatorReplayerBase(rootDir, taskId, nodeId, pipelineId, operatorType),
        replayOutputDir_(replayOutputDir) {
    VELOX_CHECK(!replayOutputDir_.empty());
  }

 private:
  core::PlanNodePtr createPlanNode(
      const core::PlanNode* node,
      const core::PlanNodeId& nodeId,
      const core::PlanNodePtr& source) const override;

  const std::string replayOutputDir_;
};

} // namespace facebook::velox::tool::trace
