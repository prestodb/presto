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

#include "velox/common/file/FileSystems.h"
#include "velox/core/PlanNode.h"

namespace facebook::velox::tool::trace {
class OperatorReplayerBase {
 public:
  OperatorReplayerBase(
      std::string rootDir,
      std::string taskId,
      std::string nodeId,
      int32_t pipelineId,
      std::string operatorType);
  virtual ~OperatorReplayerBase() = default;

  OperatorReplayerBase(const OperatorReplayerBase& other) = delete;
  OperatorReplayerBase& operator=(const OperatorReplayerBase& other) = delete;
  OperatorReplayerBase(OperatorReplayerBase&& other) noexcept = delete;
  OperatorReplayerBase& operator=(OperatorReplayerBase&& other) noexcept =
      delete;

  RowVectorPtr run() const;

  static void printSummary(
      const std::string& rootDir,
      const std::string& taskId,
      bool shortSummary);

  static std::string usage();

 protected:
  virtual core::PlanNodePtr createPlan() const;

  virtual std::function<core::PlanNodePtr(std::string, core::PlanNodePtr)>
  addReplayNode(const core::PlanNode* node) const;

  virtual core::PlanNodePtr createPlanNode(
      const core::PlanNode* node,
      const core::PlanNodeId& nodeId,
      const core::PlanNodePtr& source) const = 0;

  const std::string rootDir_;
  const std::string taskId_;
  const std::string nodeId_;
  const int32_t pipelineId_;
  const std::string operatorType_;

  std::unordered_map<std::string, std::string> queryConfigs_;
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>>
      connectorConfigs_;
  core::PlanNodePtr planFragment_;
  std::shared_ptr<filesystems::FileSystem> fs_;
  int32_t maxDrivers_{1};
};

} // namespace facebook::velox::tool::trace
