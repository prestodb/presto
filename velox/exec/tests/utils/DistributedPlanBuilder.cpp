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
    : PlanBuilder(std::move(planNodeIdGenerator), pool),
      options_(options),
      root_(this) {
  root_->stack_.push_back(this);
  newFragment(options_.numWorkers);
}

DistributedPlanBuilder::DistributedPlanBuilder(DistributedPlanBuilder& root)
    : PlanBuilder(root.planNodeIdGenerator(), root.pool()),
      options_(root.options_),
      root_(&root) {
  root_->stack_.push_back(this);
  newFragment(options_.numWorkers);
}

DistributedPlanBuilder::~DistributedPlanBuilder() {
  VELOX_CHECK_EQ(root_->stack_.size(), 1);
}

std::vector<runner::ExecutableFragment> DistributedPlanBuilder::fragments() {
  newFragment();
  return std::move(fragments_);
}

runner::MultiFragmentPlanPtr DistributedPlanBuilder::build() {
  return std::make_shared<runner::MultiFragmentPlan>(fragments(), options_);
}

namespace {
void gatherScans(
    const core::PlanNodePtr& plan,
    std::vector<core::TableScanNodePtr>& scans) {
  if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(plan)) {
    scans.push_back(scan);
    return;
  }
  for (auto& source : plan->sources()) {
    gatherScans(source, scans);
  }
}
} // namespace

void DistributedPlanBuilder::newFragment(int32_t width) {
  if (current_) {
    gatherScans(planNode_, current_->scans);
    current_->fragment = core::PlanFragment(std::move(planNode_));
    fragments_.push_back(std::move(*current_));
  }

  current_ = std::make_unique<runner::ExecutableFragment>(
      fmt::format("{}.{}", options_.queryId, root_->fragmentCounter_++));
  current_->width = width;

  planNode_ = nullptr;
}

namespace {

template <typename TNode>
const TNode* as(const core::PlanNodePtr& node) {
  const auto* result = dynamic_cast<const TNode*>(node.get());
  VELOX_CHECK_NOT_NULL(result);

  return result;
}

} // namespace

void DistributedPlanBuilder::addExchange(
    const RowTypePtr& producerType,
    const std::string& producerPrefix,
    runner::ExecutableFragment& fragment) {
  exchange(producerType, VectorSerde::Kind::kPresto);
  auto* exchange = as<core::ExchangeNode>(planNode_);

  fragment.inputStages.push_back(
      runner::InputStage{exchange->id(), producerPrefix});
}

DistributedPlanBuilder& DistributedPlanBuilder::shufflePartitioned(
    const std::vector<std::string>& partitionKeys,
    int numPartitions,
    bool replicateNullsAndAny,
    const std::vector<std::string>& outputLayout) {
  partitionedOutput(
      partitionKeys, numPartitions, replicateNullsAndAny, outputLayout);
  auto* output = as<core::PartitionedOutputNode>(planNode_);

  const auto producerPrefix = current_->taskPrefix;

  newFragment(numPartitions);

  addExchange(output->outputType(), producerPrefix, *current_);
  return *this;
}

void DistributedPlanBuilder::appendFragments(
    std::vector<runner::ExecutableFragment> fragments) {
  for (auto& fragment : fragments) {
    fragments_.emplace_back(std::move(fragment));
  }
}

core::PlanNodePtr DistributedPlanBuilder::shufflePartitionedResult(
    const std::vector<std::string>& partitionKeys,
    int numPartitions,
    bool replicateNullsAndAny,
    const std::vector<std::string>& outputLayout) {
  partitionedOutput(
      partitionKeys, numPartitions, replicateNullsAndAny, outputLayout);
  auto* output = as<core::PartitionedOutputNode>(planNode_);

  const auto producerPrefix = current_->taskPrefix;

  newFragment();

  root_->stack_.pop_back(); // Remove self.

  auto* consumer = root_->stack_.back();
  if (consumer->current_->width != 0) {
    VELOX_CHECK_EQ(
        numPartitions,
        consumer->current_->width,
        "The consumer width should match the producer fanout");
  } else {
    consumer->current_->width = numPartitions;
  }

  root_->appendFragments(std::move(fragments_));

  addExchange(output->outputType(), producerPrefix, *consumer->current_);
  return std::move(planNode_);
}

core::PlanNodePtr DistributedPlanBuilder::shuffleBroadcastResult() {
  partitionedOutputBroadcast();
  auto* output = as<core::PartitionedOutputNode>(planNode_);

  const auto producerPrefix = current_->taskPrefix;
  auto result = planNode_;
  newFragment();

  VELOX_CHECK_GE(root_->stack_.size(), 2);
  root_->stack_.pop_back(); // Remove self.
  auto* consumer = root_->stack_.back();

  VELOX_CHECK_GE(consumer->current_->width, 1);
  VELOX_CHECK_EQ(fragments_.back().numBroadcastDestinations, 0);
  fragments_.back().numBroadcastDestinations = consumer->current_->width;

  root_->appendFragments(std::move(fragments_));

  addExchange(output->outputType(), producerPrefix, *consumer->current_);
  return std::move(planNode_);
}

} // namespace facebook::velox::exec::test
