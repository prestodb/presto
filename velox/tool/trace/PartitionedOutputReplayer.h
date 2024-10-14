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
#include "velox/exec/OutputBufferManager.h"
#include "velox/tool/trace/OperatorReplayerBase.h"

namespace facebook::velox::tool::trace {

/// Concurrently gets all partitioned buffer content (vec<IOBuf>) for every
/// partition.
void consumeAllData(
    const std::shared_ptr<exec::OutputBufferManager>& bufferManager,
    const std::string& taskId,
    uint32_t numPartitions,
    folly::Executor* executor,
    folly::ThreadPoolExecutor* consumerExecutor,
    std::function<void(uint32_t, std::unique_ptr<folly::IOBuf>)> consumer);

/// The replayer to replay the traced 'PartitionedOutput' operator.
class PartitionedOutputReplayer final : public OperatorReplayerBase {
 public:
  using ConsumerCallBack =
      std::function<void(uint32_t, std::unique_ptr<folly::IOBuf>)>;

  PartitionedOutputReplayer(
      const std::string& rootDir,
      const std::string& taskId,
      const std::string& nodeId,
      const int32_t pipelineId,
      const std::string& operatorType,
      const ConsumerCallBack& consumerCb = [](auto partition, auto page) {});

  RowVectorPtr run() override;

 private:
  core::PlanNodePtr createPlanNode(
      const core::PlanNode* node,
      const core::PlanNodeId& nodeId,
      const core::PlanNodePtr& source) const override;

  const core::PartitionedOutputNode* const originalNode_;
  const std::shared_ptr<exec::OutputBufferManager> bufferManager_{
      exec::OutputBufferManager::getInstance().lock()};
  const std::unique_ptr<folly::Executor> executor_{
      std::make_unique<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency(),
          std::make_shared<folly::NamedThreadFactory>("Driver"))};
  const ConsumerCallBack consumerCb_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> consumerExecutor_;
};
} // namespace facebook::velox::tool::trace
