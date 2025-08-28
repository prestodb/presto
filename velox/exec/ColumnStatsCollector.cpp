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

#include "velox/exec/ColumnStatsCollector.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/AggregateInfo.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

ColumnStatsCollector::ColumnStatsCollector(
    const core::ColumnStatsSpec& statsSpec,
    const RowTypePtr& inputType,
    const core::QueryConfig* queryConfig,
    memory::MemoryPool* pool,
    tsan_atomic<bool>* nonReclaimableSection)
    : statsSpec_(statsSpec),
      inputType_(inputType),
      queryConfig_(queryConfig),
      pool_(pool),
      nonReclaimableSection_(nonReclaimableSection),
      maxOutputBatchRows_(
          statsSpec_.groupingKeys.empty() ? 1 : kMaxOutputBatchRows) {
  VELOX_CHECK_NOT_NULL(inputType_);
  VELOX_CHECK_NOT_NULL(queryConfig_);
  VELOX_CHECK_NOT_NULL(pool_);
  VELOX_CHECK_NOT_NULL(nonReclaimableSection_);
}

void ColumnStatsCollector::initialize() {
  if (initialized_) {
    return;
  }
  SCOPE_EXIT {
    initialized_ = true;
  };
  setOutputType();
  VELOX_CHECK_NOT_NULL(outputType_);
  createGroupingSet();
  VELOX_CHECK_NOT_NULL(groupingSet_);
}

// static
RowTypePtr ColumnStatsCollector::outputType(
    const core::ColumnStatsSpec& statsSpec) {
  // Create output type based on the column stats collection specs.
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  const auto numAggregates = statsSpec.aggregates.size();
  const auto outputTypeSize = statsSpec.groupingKeys.size() + numAggregates;
  names.reserve(outputTypeSize);
  types.reserve(outputTypeSize);
  for (const auto& key : statsSpec.groupingKeys) {
    names.push_back(key->name());
    types.push_back(key->type());
  }
  for (auto i = 0; i < numAggregates; ++i) {
    names.push_back(statsSpec.aggregateNames[i]);
    types.push_back(statsSpec.aggregates[i].call->type());
  }
  return ROW(std::move(names), std::move(types));
}

void ColumnStatsCollector::setOutputType() {
  VELOX_CHECK_NULL(outputType_);
  outputType_ = outputType(statsSpec_);
}

std::pair<std::vector<column_index_t>, std::vector<column_index_t>>
ColumnStatsCollector::setupGroupingKeyChannelProjections() const {
  const auto& groupingKeys = statsSpec_.groupingKeys;
  std::vector<column_index_t> groupingKeyInputChannels;
  groupingKeyInputChannels.reserve(groupingKeys.size());
  for (auto i = 0; i < groupingKeys.size(); ++i) {
    groupingKeyInputChannels.push_back(
        exprToChannel(groupingKeys[i].get(), inputType_));
  }

  std::vector<column_index_t> groupingKeyOutputChannels(groupingKeys.size());
  std::iota(
      groupingKeyOutputChannels.begin(), groupingKeyOutputChannels.end(), 0);
  return std::make_pair(groupingKeyInputChannels, groupingKeyOutputChannels);
}

std::vector<AggregateInfo> ColumnStatsCollector::createAggregates(
    size_t numGroupingKeys) {
  VELOX_CHECK_NOT_NULL(outputType_);
  const auto step = statsSpec_.aggregationStep;
  const auto numAggregates = statsSpec_.aggregates.size();
  std::vector<AggregateInfo> aggregateInfos;
  aggregateInfos.reserve(numAggregates);
  for (auto i = 0; i < numAggregates; ++i) {
    const auto& aggregate = statsSpec_.aggregates[i];
    AggregateInfo info;
    auto& channels = info.inputs;
    auto& constants = info.constantInputs;
    for (const auto& arg : aggregate.call->inputs()) {
      if (auto field =
              dynamic_cast<const core::FieldAccessTypedExpr*>(arg.get())) {
        channels.push_back(inputType_->getChildIdx(field->name()));
        constants.push_back(nullptr);
      } else {
        VELOX_FAIL(
            "Aggregation expression must be field access for column stats collection: {}",
            arg->toString());
      }
    }
    VELOX_CHECK(!aggregate.distinct);
    info.intermediateType = resolveAggregateFunction(
                                aggregate.call->name(), aggregate.rawInputTypes)
                                .second;
    // Column stats collection doesn't support aggregation mask.
    VELOX_CHECK_NULL(aggregate.mask);
    info.mask = std::nullopt;
    const auto outputChannel = numGroupingKeys + i;
    const auto& aggResultType = outputType_->childAt(outputChannel);
    info.function = Aggregate::create(
        aggregate.call->name(),
        isPartialOutput(step) ? core::AggregationNode::Step::kPartial
                              : core::AggregationNode::Step::kSingle,
        aggregate.rawInputTypes,
        aggResultType,
        *queryConfig_);
    VELOX_CHECK(aggregate.sortingKeys.empty());
    VELOX_CHECK(aggregate.sortingOrders.empty());
    info.output = outputChannel;
    aggregateInfos.emplace_back(std::move(info));
  }
  return aggregateInfos;
}

void ColumnStatsCollector::createGroupingSet() {
  VELOX_CHECK_NULL(groupingSet_);

  auto [groupingKeyInputChannels, groupingKeyOutputChannels] =
      setupGroupingKeyChannelProjections();

  auto hashers = createVectorHashers(inputType_, groupingKeyInputChannels);
  const auto numHashers = hashers.size();

  // Setup aggregates based on the column stats specifications
  auto aggregateInfos = createAggregates(numHashers);
  // Create the grouping set for aggregation execution.
  groupingSet_ = std::make_unique<GroupingSet>(
      inputType_,
      std::move(hashers),
      /*preGroupedKeys=*/std::vector<column_index_t>{},
      std::move(groupingKeyOutputChannels),
      std::move(aggregateInfos),
      /*ignoreNullKey=*/false,
      /*isPartial=*/isPartialOutput(statsSpec_.aggregationStep),
      /*isRawInput=*/isRawInput(statsSpec_.aggregationStep),
      /*globalGroupingSets=*/std::vector<vector_size_t>{},
      /*groupIdChannel=*/std::nullopt,
      /*spillConfig=*/nullptr,
      nonReclaimableSection_,
      queryConfig_,
      pool_,
      /*spillStats=*/nullptr);
}

void ColumnStatsCollector::addInput(RowVectorPtr input) {
  VELOX_CHECK_NOT_NULL(input);
  VELOX_CHECK(initialized_);

  if (input->size() == 0) {
    return;
  }

  // Add input to the grouping set
  groupingSet_->addInput(input, /*mayPushdown=*/false);
}

void ColumnStatsCollector::noMoreInput() {
  if (!noMoreInput_) {
    noMoreInput_ = true;
    if (groupingSet_) {
      groupingSet_->noMoreInput();
    }
  }
}

void ColumnStatsCollector::prepareOutput() {
  if (output_) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, maxOutputBatchRows_);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(outputType_, maxOutputBatchRows_, pool_));
  }
}

RowVectorPtr ColumnStatsCollector::getOutput() {
  VELOX_CHECK(initialized_);

  if (!groupingSet_ || !noMoreInput_ || finished_) {
    return nullptr;
  }

  prepareOutput();
  const bool hasMoreOutput = groupingSet_->getOutput(
      maxOutputBatchRows_, kMaxOutputBatchBytes, outputIterator_, output_);
  if (!hasMoreOutput) {
    finished_ = true;
    return nullptr;
  }
  return output_;
}

bool ColumnStatsCollector::finished() const {
  return finished_;
}

void ColumnStatsCollector::close() {
  if (groupingSet_) {
    groupingSet_.reset();
  }
}

} // namespace facebook::velox::exec
