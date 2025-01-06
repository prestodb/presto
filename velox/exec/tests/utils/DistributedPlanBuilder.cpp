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

#include "velox/exec/tests/utils/DistributedPlanBuilder.h"

namespace facebook::velox::exec::test {

DistributedPlanBuilder::DistributedPlanBuilder(
    const runner::MultiFragmentPlan::Options& options,
    std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator,
    memory::MemoryPool* pool)
    : PlanBuilder(planNodeIdGenerator, pool), options_(options), root_(this) {
  root_->stack_.push_back(this);
  newFragment();
  current_->width = options_.numWorkers;
}

DistributedPlanBuilder::DistributedPlanBuilder(DistributedPlanBuilder& root)
    : PlanBuilder(root.planNodeIdGenerator(), root.pool()),
      options_(root.options_),
      root_(&root) {
  root_->stack_.push_back(this);
  newFragment();
  current_->width = options_.numWorkers;
}

DistributedPlanBuilder::~DistributedPlanBuilder() {
  VELOX_CHECK_EQ(root_->stack_.size(), 1);
}

std::vector<runner::ExecutableFragment> DistributedPlanBuilder::fragments() {
  newFragment();
  return std::move(fragments_);
}

void DistributedPlanBuilder::newFragment() {
  if (current_) {
    gatherScans(planNode_);
    current_->fragment = core::PlanFragment(std::move(planNode_));
    fragments_.push_back(std::move(*current_));
  }
  current_ = std::make_unique<runner::ExecutableFragment>(
      fmt::format("{}.{}", options_.queryId, root_->fragmentCounter_++));
  planNode_ = nullptr;
}

PlanBuilder& DistributedPlanBuilder::shufflePartitioned(
    const std::vector<std::string>& partitionKeys,
    int numPartitions,
    bool replicateNullsAndAny,
    const std::vector<std::string>& outputLayout) {
  partitionedOutput(
      partitionKeys, numPartitions, replicateNullsAndAny, outputLayout);
  auto* output =
      dynamic_cast<const core::PartitionedOutputNode*>(planNode_.get());
  VELOX_CHECK_NOT_NULL(output);
  auto producerPrefix = current_->taskPrefix;
  newFragment();
  current_->width = numPartitions;
  exchange(output->outputType(), VectorSerde::Kind::kPresto);
  auto* exchange = dynamic_cast<const core::ExchangeNode*>(planNode_.get());
  VELOX_CHECK_NOT_NULL(exchange);
  current_->inputStages.push_back(
      runner::InputStage{exchange->id(), producerPrefix});
  return *this;
}

core::PlanNodePtr DistributedPlanBuilder::shufflePartitionedResult(
    const std::vector<std::string>& partitionKeys,
    int numPartitions,
    bool replicateNullsAndAny,
    const std::vector<std::string>& outputLayout) {
  partitionedOutput(
      partitionKeys, numPartitions, replicateNullsAndAny, outputLayout);
  auto* output =
      dynamic_cast<const core::PartitionedOutputNode*>(planNode_.get());
  VELOX_CHECK_NOT_NULL(output);
  const auto producerPrefix = current_->taskPrefix;
  auto result = planNode_;
  newFragment();
  root_->stack_.pop_back();
  auto* consumer = root_->stack_.back();
  if (consumer->current_->width != 0) {
    VELOX_CHECK_EQ(
        numPartitions,
        consumer->current_->width,
        "The consumer width should match the producer fanout");
  } else {
    consumer->current_->width = numPartitions;
  }

  for (auto& fragment : fragments_) {
    root_->fragments_.push_back(std::move(fragment));
  }
  exchange(output->outputType(), VectorSerde::Kind::kPresto);
  auto* exchange = dynamic_cast<const core::ExchangeNode*>(planNode_.get());
  consumer->current_->inputStages.push_back(
      runner::InputStage{exchange->id(), producerPrefix});
  return std::move(planNode_);
}

core::PlanNodePtr DistributedPlanBuilder::shuffleBroadcastResult() {
  partitionedOutputBroadcast();
  auto* output =
      dynamic_cast<const core::PartitionedOutputNode*>(planNode_.get());
  VELOX_CHECK_NOT_NULL(output);
  const auto producerPrefix = current_->taskPrefix;
  auto result = planNode_;
  newFragment();

  VELOX_CHECK_GE(root_->stack_.size(), 2);
  root_->stack_.pop_back();
  auto* consumer = root_->stack_.back();
  VELOX_CHECK_GE(consumer->current_->width, 1);
  VELOX_CHECK_EQ(fragments_.back().numBroadcastDestinations, 0);
  fragments_.back().numBroadcastDestinations = consumer->current_->width;

  for (auto& fragment : fragments_) {
    root_->fragments_.push_back(std::move(fragment));
  }
  exchange(output->outputType(), VectorSerde::Kind::kPresto);
  auto* exchange = dynamic_cast<const core::ExchangeNode*>(planNode_.get());
  VELOX_CHECK_NOT_NULL(exchange);
  consumer->current_->inputStages.push_back(
      runner::InputStage{exchange->id(), producerPrefix});
  return std::move(planNode_);
}

void DistributedPlanBuilder::gatherScans(const core::PlanNodePtr& plan) {
  if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(plan)) {
    current_->scans.push_back(scan);
    return;
  }
  for (auto& source : plan->sources()) {
    gatherScans(source);
  }
}
} // namespace facebook::velox::exec::test
