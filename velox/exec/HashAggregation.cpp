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
#include <optional>
#include "velox/exec/Aggregate.h"
#include "velox/exec/OperatorUtils.h"
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
      isPartialOutput_(isPartialOutput(aggregationNode->step())),
      isDistinct_(aggregationNode->aggregates().empty()),
      isGlobal_(aggregationNode->groupingKeys().empty()),
      memoryTracker_(operatorCtx_->pool()->getMemoryUsageTracker()),
      partialAggregationGoodPct_(
          driverCtx->queryConfig().partialAggregationGoodPct()),
      maxExtendedPartialAggregationMemoryUsage_(
          driverCtx->queryConfig().maxExtendedPartialAggregationMemoryUsage()),
      spillConfig_(makeOperatorSpillConfig(
          *operatorCtx_->task()->queryCtx(),
          *operatorCtx_,
          core::QueryConfig::kAggregationSpillEnabled,
          operatorId)),
      maxPartialAggregationMemoryUsage_(
          driverCtx->queryConfig().maxPartialAggregationMemoryUsage()) {
  VELOX_CHECK_NOT_NULL(memoryTracker_, "Memory usage tracker is not set");
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

  std::vector<column_index_t> preGroupedChannels;
  preGroupedChannels.reserve(aggregationNode->preGroupedKeys().size());
  for (const auto& key : aggregationNode->preGroupedKeys()) {
    auto channel = exprToChannel(key.get(), inputType);
    preGroupedChannels.push_back(channel);
  }

  auto numAggregates = aggregationNode->aggregates().size();
  std::vector<std::unique_ptr<Aggregate>> aggregates;
  aggregates.reserve(numAggregates);
  std::vector<std::optional<column_index_t>> aggrMaskChannels;
  aggrMaskChannels.reserve(numAggregates);
  auto numMasks = aggregationNode->aggregateMasks().size();
  std::vector<std::vector<column_index_t>> args;
  std::vector<std::vector<VectorPtr>> constantLists;
  std::vector<TypePtr> intermediateTypes;
  for (auto i = 0; i < numAggregates; i++) {
    const auto& aggregate = aggregationNode->aggregates()[i];

    std::vector<column_index_t> channels;
    std::vector<VectorPtr> constants;
    std::vector<TypePtr> argTypes;
    for (auto& arg : aggregate->inputs()) {
      argTypes.push_back(arg->type());
      channels.push_back(exprToChannel(arg.get(), inputType));
      if (channels.back() == kConstantChannel) {
        auto constant = dynamic_cast<const core::ConstantTypedExpr*>(arg.get());
        if (constant->hasValueVector()) {
          constants.push_back(
              BaseVector::wrapInConstant(1, 0, constant->valueVector()));
        } else {
          constants.push_back(BaseVector::createConstant(
              constant->value(), 1, operatorCtx_->pool()));
        }
      } else {
        constants.push_back(nullptr);
      }
    }
    if (isRawInput(aggregationNode->step())) {
      intermediateTypes.push_back(
          Aggregate::intermediateType(aggregate->name(), argTypes));
    } else {
      VELOX_DCHECK(!argTypes.empty());
      intermediateTypes.push_back(argTypes[0]);
      VELOX_CHECK_EQ(
          argTypes.size(),
          1,
          "Intermediate aggregates must have a single argument");
    }
    // Setup aggregation mask: convert the Variable Reference name to the
    // channel (projection) index, if there is a mask.
    if (i < numMasks) {
      const auto& aggrMask = aggregationNode->aggregateMasks()[i];
      if (aggrMask == nullptr) {
        aggrMaskChannels.emplace_back(std::nullopt);
      } else {
        aggrMaskChannels.emplace_back(
            inputType->asRow().getChildIdx(aggrMask->name()));
      }
    } else {
      aggrMaskChannels.emplace_back(std::nullopt);
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
        core::AggregationNode::stepName(aggregationNode->step()));
  }

  if (isDistinct_) {
    for (auto i = 0; i < hashers.size(); ++i) {
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
      std::move(intermediateTypes),
      aggregationNode->ignoreNullKeys(),
      isPartialOutput_,
      isRawInput(aggregationNode->step()),
      spillConfig_.has_value() ? &spillConfig_.value() : nullptr,
      operatorCtx_.get());
}

void HashAggregation::addInput(RowVectorPtr input) {
  if (!pushdownChecked_) {
    mayPushdown_ = operatorCtx_->driver()->mayPushdownAggregation(this);
    pushdownChecked_ = true;
  }
  groupingSet_->addInput(input, mayPushdown_);
  numInputRows_ += input->size();
  auto spilledStats = groupingSet_->spilledStats();
  stats_.spilledBytes = spilledStats.spilledBytes;
  stats_.spilledRows = spilledStats.spilledRows;
  stats_.spilledPartitions = spilledStats.spilledPartitions;

  // NOTE: we should not trigger partial output flush in case of global
  // aggregation as the final aggregator will handle it the same way as the
  // partial aggregator. Hence, we have to use more memory anyway.
  if (isPartialOutput_ && !isGlobal_ &&
      groupingSet_->allocatedBytes() > maxPartialAggregationMemoryUsage_) {
    partialFull_ = true;
  }

  if (isDistinct_) {
    newDistincts_ = !groupingSet_->hashLookup().newGroups.empty();

    if (newDistincts_) {
      // Save input to use for output in getOutput().
      input_ = input;
    }
  }
}

void HashAggregation::prepareOutput(vector_size_t size) {
  if (output_) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, size);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(outputType_, size, pool()));
  }
}

void HashAggregation::resetPartialOutputIfNeed() {
  if (!partialFull_) {
    return;
  }
  VELOX_DCHECK(!isGlobal_);
  stats().addRuntimeStat("flushRowCount", RuntimeCounter(numOutputRows_));
  const double aggregationPct =
      numOutputRows_ == 0 ? 0 : (numOutputRows_ * 1.0) / numInputRows_ * 100;
  stats().addRuntimeStat(
      "partialAggregationPct", RuntimeCounter(aggregationPct));
  groupingSet_->resetPartial();
  partialFull_ = false;
  numOutputRows_ = 0;
  numInputRows_ = 0;
  if (!finished_) {
    maybeIncreasePartialAggregationMemoryUsage(aggregationPct);
  }
}

void HashAggregation::maybeIncreasePartialAggregationMemoryUsage(
    double aggregationPct) {
  VELOX_DCHECK(isPartialOutput_);
  // Do not increase the aggregation memory usage further if we have already
  // achieved good aggregation ratio with the current size.
  if (aggregationPct < partialAggregationGoodPct_ ||
      maxPartialAggregationMemoryUsage_ >=
          maxExtendedPartialAggregationMemoryUsage_) {
    return;
  }
  const int64_t extendedPartialAggregationMemoryUsage = std::min(
      maxPartialAggregationMemoryUsage_ * 2,
      maxExtendedPartialAggregationMemoryUsage_);
  // Calculate the memory to reserve to bump up the aggregation buffer size. If
  // the memory reservation below succeeds, it ensures the partial aggregator
  // can allocate that much memory in next run.
  const int64_t memoryToReserve = std::max<int64_t>(
      0,
      extendedPartialAggregationMemoryUsage - groupingSet_->allocatedBytes());
  if (!memoryTracker_->maybeReserve(memoryToReserve)) {
    return;
  }
  // Update the aggregation memory usage size limit on memory reservation
  // success.
  maxPartialAggregationMemoryUsage_ = extendedPartialAggregationMemoryUsage;
  stats().addRuntimeStat(
      "maxExtendedPartialAggregationMemoryUsage",
      RuntimeCounter(
          maxPartialAggregationMemoryUsage_, RuntimeCounter::Unit::kBytes));
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
    numOutputRows_ += size;

    // Drop reference to input_ to make it singly-referenced at the producer and
    // allow for memory reuse.
    input_ = nullptr;

    resetPartialOutputIfNeed();
    return output;
  }

  auto batchSize = isGlobal_ ? 1 : outputBatchSize_;

  // Reuse output vectors if possible.
  prepareOutput(batchSize);

  bool hasData = groupingSet_->getOutput(batchSize, resultIterator_, output_);
  if (!hasData) {
    resultIterator_.reset();
    if (noMoreInput_) {
      finished_ = true;
    }
    resetPartialOutputIfNeed();
    return nullptr;
  }
  numOutputRows_ += output_->size();
  return output_;
}

bool HashAggregation::isFinished() {
  return finished_;
}
} // namespace facebook::velox::exec
