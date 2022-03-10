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
#include "velox/exec/HashAggregation.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

HashAggregation::HashAggregation(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::AggregationNode>& aggregationNode)
    : Operator(
          driverCtx,
          aggregationNode->outputType(),
          operatorId,
          aggregationNode->id(),
          aggregationNode->step() == core::AggregationNode::Step::kPartial
              ? "PartialAggregation"
              : "Aggregation"),
      outputBatchSize_{driverCtx->queryConfig().preferredOutputBatchSize()},
      maxPartialAggregationMemoryUsage_(
          driverCtx->queryConfig().maxPartialAggregationMemoryUsage()),
      isPartialOutput_(isPartialOutput(aggregationNode->step())),
      isDistinct_(aggregationNode->aggregates().empty()),
      isGlobal_(aggregationNode->groupingKeys().empty()),
      hasPreGroupedKeys_(!aggregationNode->preGroupedKeys().empty()) {
  auto inputType = aggregationNode->sources()[0]->outputType();

  auto numHashers = aggregationNode->groupingKeys().size();
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.reserve(numHashers);
  for (const auto& key : aggregationNode->groupingKeys()) {
    auto channel = exprToChannel(key.get(), inputType);
    VELOX_CHECK_NE(
        channel,
        kConstantChannel,
        "Aggregation doesn't allow constant grouping keys");
    hashers.push_back(VectorHasher::create(key->type(), channel));
  }

  std::vector<ChannelIndex> preGroupedChannels;
  preGroupedChannels.reserve(aggregationNode->preGroupedKeys().size());
  for (const auto& key : aggregationNode->preGroupedKeys()) {
    auto channel = exprToChannel(key.get(), inputType);
    preGroupedChannels.push_back(channel);
  }

  auto numAggregates = aggregationNode->aggregates().size();
  std::vector<std::unique_ptr<Aggregate>> aggregates;
  aggregates.reserve(numAggregates);
  std::vector<std::optional<ChannelIndex>> aggrMaskChannels;
  aggrMaskChannels.reserve(numAggregates);
  std::vector<std::vector<ChannelIndex>> args;
  std::vector<std::vector<VectorPtr>> constantLists;
  for (auto i = 0; i < numAggregates; i++) {
    const auto& aggregate = aggregationNode->aggregates()[i];

    std::vector<ChannelIndex> channels;
    std::vector<VectorPtr> constants;
    std::vector<TypePtr> argTypes;
    for (auto& arg : aggregate->inputs()) {
      argTypes.push_back(arg->type());
      channels.push_back(exprToChannel(arg.get(), inputType));
      if (channels.back() == kConstantChannel) {
        auto constant = dynamic_cast<const core::ConstantTypedExpr*>(arg.get());
        constants.push_back(BaseVector::createConstant(
            constant->value(), 1, operatorCtx_->pool()));
      } else {
        constants.push_back(nullptr);
      }
    }

    // Setup aggregation mask: convert the Variable Reference name to the
    // channel (projection) index, if there is a mask.
    const auto& aggrMask = aggregationNode->aggregateMasks()[i];
    if (aggrMask == nullptr) {
      aggrMaskChannels.emplace_back(std::optional<ChannelIndex>{});
    } else {
      aggrMaskChannels.emplace_back(
          inputType->asRow().getChildIdx(aggrMask->name()));
    }

    const auto& resultType = outputType_->childAt(numHashers + i);
    aggregates.push_back(Aggregate::create(
        aggregate->name(), aggregationNode->step(), argTypes, resultType));
    args.push_back(channels);
    constantLists.push_back(constants);
  }

  // Check that aggregate result type match the output type
  for (auto i = 0; i < aggregates.size(); i++) {
    const auto& aggResultType = aggregates[i]->resultType();
    const auto& expectedType = outputType_->childAt(numHashers + i);
    VELOX_CHECK(
        aggResultType->kindEquals(expectedType),
        "Unexpected result type for an aggregation: {}, expected {}, step {}",
        aggResultType->toString(),
        expectedType->toString(),
        static_cast<int32_t>(aggregationNode->step()));
  }

  if (isDistinct_) {
    for (ChannelIndex i = 0; i < hashers.size(); ++i) {
      identityProjections_.emplace_back(hashers[i]->channel(), i);
    }
  }

  groupingSet_ = std::make_unique<GroupingSet>(
      std::move(hashers),
      std::move(preGroupedChannels),
      std::move(aggregates),
      std::move(aggrMaskChannels),
      std::move(args),
      std::move(constantLists),
      aggregationNode->ignoreNullKeys(),
      isRawInput(aggregationNode->step()),
      operatorCtx_.get());
}

void HashAggregation::addInput(RowVectorPtr input) {
  input_ = input;
  if (!pushdownChecked_) {
    mayPushdown_ = operatorCtx_->driver()->mayPushdownAggregation(this);
    pushdownChecked_ = true;
  }
  groupingSet_->addInput(input_, mayPushdown_);
  if (isPartialOutput_ &&
      groupingSet_->allocatedBytes() > maxPartialAggregationMemoryUsage_) {
    partialFull_ = true;
  }
  newDistincts_ = isDistinct_ && !groupingSet_->hashLookup().newGroups.empty();
}

RowVectorPtr HashAggregation::getOutput() {
  if (finished_) {
    input_ = nullptr;
    return nullptr;
  }

  // Produce results if one of the following is true:
  // - received no-more-input message;
  // - partial aggregation reached memory limit;
  // - distinct aggregation has new keys;
  // - running in partial streaming mode and have some output ready.
  if (!noMoreInput_ && !partialFull_ && !newDistincts_ &&
      !groupingSet_->hasOutput()) {
    input_ = nullptr;
    return nullptr;
  }

  if (isDistinct_) {
    if (!newDistincts_) {
      if (noMoreInput_) {
        finished_ = true;
      }
      return nullptr;
    }

    auto lookup = groupingSet_->hashLookup();
    auto size = lookup.newGroups.size();
    BufferPtr indices = allocateIndices(size, operatorCtx_->pool());
    auto indicesPtr = indices->asMutable<vector_size_t>();
    std::copy(lookup.newGroups.begin(), lookup.newGroups.end(), indicesPtr);
    newDistincts_ = false;
    auto output = fillOutput(size, indices);

    // Drop reference to input_ to make it singly-referenced at the producer and
    // allow for memory reuse.
    input_ = nullptr;

    if (partialFull_) {
      groupingSet_->resetPartial();
      partialFull_ = false;
    }
    return output;
  }

  auto batchSize = isGlobal_ ? 1 : outputBatchSize_;

  // TODO Figure out how to re-use 'result' safely.
  auto result = std::static_pointer_cast<RowVector>(
      BaseVector::create(outputType_, batchSize, operatorCtx_->pool()));

  bool hasData = groupingSet_->getOutput(
      batchSize, isPartialOutput_, &resultIterator_, result);
  if (!hasData) {
    resultIterator_.reset();

    if (partialFull_) {
      partialFull_ = false;
      groupingSet_->resetPartial();
    }

    if (noMoreInput_) {
      finished_ = true;
    }
    return nullptr;
  }
  return result;
}

bool HashAggregation::isFinished() {
  return finished_;
}
} // namespace facebook::velox::exec
