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

#include <folly/executors/IOThreadPoolExecutor.h>

#include "velox/common/memory/Memory.h"
#include "velox/exec/PartitionedOutput.h"
#include "velox/exec/QueryTraceUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/tool/trace/PartitionedOutputReplayer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::tool::trace {
namespace {
std::shared_ptr<core::QueryCtx> createQueryContext(
    const std::unordered_map<std::string, std::string>& config,
    folly::Executor* executor) {
  return core::QueryCtx::create(executor, core::QueryConfig(std::move(config)));
}

std::vector<std::unique_ptr<folly::IOBuf>> getData(
    const std::shared_ptr<exec::OutputBufferManager>& bufferManager,
    const std::string& taskId,
    int destination,
    int64_t sequence,
    folly::Executor* executor) {
  auto [promise, semiFuture] =
      folly::makePromiseContract<std::vector<std::unique_ptr<folly::IOBuf>>>();
  VELOX_CHECK(bufferManager->getData(
      taskId,
      destination,
      exec::PartitionedOutput::kMinDestinationSize,
      sequence,
      [result = std::make_shared<
           folly::Promise<std::vector<std::unique_ptr<folly::IOBuf>>>>(
           std::move(promise))](
          std::vector<std::unique_ptr<folly::IOBuf>> pages,
          int64_t /*inSequence*/,
          std::vector<int64_t> /*remainingBytes*/) {
        result->setValue(std::move(pages));
      }));
  auto future = std::move(semiFuture).via(executor);
  future.wait();
  VELOX_CHECK(future.isReady());
  return std::move(future).value();
}
} // namespace

void consumeAllData(
    const std::shared_ptr<exec::OutputBufferManager>& bufferManager,
    const std::string& taskId,
    uint32_t numPartitions,
    folly::Executor* driverExecutor,
    folly::ThreadPoolExecutor* consumerExecutor,
    std::function<void(uint32_t, std::unique_ptr<folly::IOBuf>)> consumer) {
  std::vector<std::thread> consumerThreads;
  std::vector<int64_t> sequences;
  consumerThreads.reserve(numPartitions);
  sequences.reserve(numPartitions);
  sequences.resize(numPartitions);
  for (uint32_t i = 0; i < numPartitions; i++) {
    consumerExecutor->add([&, partition = i]() {
      bool finished{false};
      while (!finished) {
        std::vector<std::unique_ptr<folly::IOBuf>> pages;
        {
          pages = getData(
              bufferManager,
              taskId,
              partition,
              sequences[partition],
              driverExecutor);
        }
        for (auto& page : pages) {
          if (page) {
            consumer(partition, std::move(page));
            sequences[partition]++;
          } else {
            // Null page indicates this buffer is finished.
            bufferManager->deleteResults(taskId, partition);
            finished = true;
          }
        }
      }
    });
  }
  consumerExecutor->join();
}

PartitionedOutputReplayer::PartitionedOutputReplayer(
    const std::string& rootDir,
    const std::string& taskId,
    const std::string& nodeId,
    const int32_t pipelineId,
    const std::string& operatorType,
    const ConsumerCallBack& consumerCb)
    : OperatorReplayerBase(rootDir, taskId, nodeId, pipelineId, operatorType),
      originalNode_(dynamic_cast<const core::PartitionedOutputNode*>(
          core::PlanNode::findFirstNode(
              planFragment_.get(),
              [this](const core::PlanNode* node) {
                return node->id() == nodeId_;
              }))),
      consumerCb_(consumerCb) {
  VELOX_CHECK_NOT_NULL(originalNode_);
  consumerExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
      originalNode_->numPartitions(),
      std::make_shared<folly::NamedThreadFactory>("Consumer"));
}

RowVectorPtr PartitionedOutputReplayer::run() {
  auto task = Task::create(
      "local://partitioned-output-replayer",
      core::PlanFragment{createPlan()},
      0,
      createQueryContext(queryConfigs_, executor_.get()),
      Task::ExecutionMode::kParallel);
  task->start(maxDrivers_);

  consumeAllData(
      bufferManager_,
      task->taskId(),
      originalNode_->numPartitions(),
      executor_.get(),
      consumerExecutor_.get(),
      consumerCb_);
  return nullptr;
}

core::PlanNodePtr PartitionedOutputReplayer::createPlanNode(
    const core::PlanNode* node,
    const core::PlanNodeId& nodeId,
    const core::PlanNodePtr& source) const {
  auto originalNode = dynamic_cast<const core::PartitionedOutputNode*>(node);
  VELOX_CHECK_NOT_NULL(originalNode);
  return std::make_shared<core::PartitionedOutputNode>(
      nodeId,
      originalNode->kind(),
      originalNode->keys(),
      originalNode->numPartitions(),
      originalNode->isReplicateNullsAndAny(),
      originalNode->partitionFunctionSpecPtr(),
      originalNode->outputType(),
      source);
}

} // namespace facebook::velox::tool::trace
