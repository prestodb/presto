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
              : "Aggregation",
          aggregationNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      isPartialOutput_(isPartialOutput(aggregationNode->step())),
      isDistinct_(aggregationNode->aggregates().empty()),
      isGlobal_(aggregationNode->groupingKeys().empty()),
      maxExtendedPartialAggregationMemoryUsage_(
          driverCtx->queryConfig().maxExtendedPartialAggregationMemoryUsage()),
      maxPartialAggregationMemoryUsage_(
          driverCtx->queryConfig().maxPartialAggregationMemoryUsage()),
      abandonPartialAggregationMinRows_(
          driverCtx->queryConfig().abandonPartialAggregationMinRows()),
      abandonPartialAggregationMinPct_(
          driverCtx->queryConfig().abandonPartialAggregationMinPct()) {
  VELOX_CHECK(pool()->trackUsage());

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
        constants.push_back(constant->toConstantVector(pool()));
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
      &nonReclaimableSection_,
      operatorCtx_.get());
}

bool HashAggregation::abandonPartialAggregationEarly(int64_t numOutput) const {
  return numInputRows_ > abandonPartialAggregationMinRows_ &&
      100 * numOutput / numInputRows_ >= abandonPartialAggregationMinPct_;
}

void HashAggregation::addInput(RowVectorPtr input) {
  if (!pushdownChecked_) {
    mayPushdown_ = operatorCtx_->driver()->mayPushdownAggregation(this);
    pushdownChecked_ = true;
  }
  if (abandonedPartialAggregation_) {
    input_ = input;
    numInputRows_ += input->size();
    return;
  }
  groupingSet_->addInput(input, mayPushdown_);
  numInputRows_ += input->size();
  {
    const auto hashTableStats = groupingSet_->hashTableStats();
    auto lockedStats = stats_.wlock();
    lockedStats->runtimeStats["hashtable.capacity"] =
        RuntimeMetric(hashTableStats.capacity);
    lockedStats->runtimeStats["hashtable.numRehashes"] =
        RuntimeMetric(hashTableStats.numRehashes);
    lockedStats->runtimeStats["hashtable.numDistinct"] =
        RuntimeMetric(hashTableStats.numDistinct);
    lockedStats->runtimeStats["hashtable.numTombstones"] =
        RuntimeMetric(hashTableStats.numTombstones);
  }

  // NOTE: we should not trigger partial output flush in case of global
  // aggregation as the final aggregator will handle it the same way as the
  // partial aggregator. Hence, we have to use more memory anyway.
  const bool abandonPartialEarly =
      abandonPartialAggregationEarly(groupingSet_->numDistinct());
  if (isPartialOutput_ && !isGlobal_ &&
      (abandonPartialEarly ||
       groupingSet_->isPartialFull(maxPartialAggregationMemoryUsage_))) {
    partialFull_ = true;
  }

  if (isDistinct_) {
    newDistincts_ = !groupingSet_->hashLookup().newGroups.empty();

    if (newDistincts_) {
      // Save input to use for output in getOutput().
      input_ = input;
    } else {
      // In case of 'no new distinct groups' the only reason we can have
      // 'partial full' true is due to 'abandoning partial aggregation early'
      // being true. If that's not the case, then it is a bug.
      VELOX_CHECK_EQ(partialFull_, abandonPartialEarly);
      // If no new distinct groups (meaning we don't have anything to output)
      // and we are abandoning the partial aggregation, then we need to ensure
      // we 'need input'. For that we need to reset the 'partial full' flag.
      partialFull_ = false;
    }
  }
}

void HashAggregation::recordSpillStats() {
  const auto spillStats = groupingSet_->spilledStats();
  auto lockedStats = stats_.wlock();
  lockedStats->spilledBytes = spillStats.spilledBytes;
  lockedStats->spilledRows = spillStats.spilledRows;
  lockedStats->spilledPartitions = spillStats.spilledPartitions;
  lockedStats->spilledFiles = spillStats.spilledFiles;
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
  const double aggregationPct =
      numOutputRows_ == 0 ? 0 : (numOutputRows_ * 1.0) / numInputRows_ * 100;
  {
    auto lockedStats = stats_.wlock();
    lockedStats->addRuntimeStat(
        "flushRowCount", RuntimeCounter(numOutputRows_));
    lockedStats->addRuntimeStat("flushTimes", RuntimeCounter(1));
    lockedStats->addRuntimeStat(
        "partialAggregationPct", RuntimeCounter(aggregationPct));
  }
  groupingSet_->resetPartial();
  partialFull_ = false;
  if (!finished_) {
    maybeIncreasePartialAggregationMemoryUsage(aggregationPct);
  }
  numOutputRows_ = 0;
  numInputRows_ = 0;
}

void HashAggregation::maybeIncreasePartialAggregationMemoryUsage(
    double aggregationPct) {
  // If more than this many are unique at full memory, give up on partial agg.
  constexpr int32_t kPartialMinFinalPct = 40;
  VELOX_DCHECK(isPartialOutput_);
  // If size is at max and there still is not enough reduction, abandon partial
  // aggregation.
  if (abandonPartialAggregationEarly(numOutputRows_) ||
      (aggregationPct > kPartialMinFinalPct &&
       maxPartialAggregationMemoryUsage_ >=
           maxExtendedPartialAggregationMemoryUsage_)) {
    groupingSet_->abandonPartialAggregation();
    pool()->release();
    addRuntimeStat("abandonedPartialAggregation", RuntimeCounter(1));
    abandonedPartialAggregation_ = true;
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
  if (!pool()->maybeReserve(memoryToReserve)) {
    return;
  }
  // Update the aggregation memory usage size limit on memory reservation
  // success.
  maxPartialAggregationMemoryUsage_ = extendedPartialAggregationMemoryUsage;
  addRuntimeStat(
      "maxExtendedPartialAggregationMemoryUsage",
      RuntimeCounter(
          maxPartialAggregationMemoryUsage_, RuntimeCounter::Unit::kBytes));
}

RowVectorPtr HashAggregation::getOutput() {
  if (finished_) {
    input_ = nullptr;
    return nullptr;
  }
  if (abandonedPartialAggregation_) {
    if (noMoreInput_) {
      finished_ = true;
    }
    if (!input_) {
      return nullptr;
    }
    prepareOutput(input_->size());
    groupingSet_->toIntermediate(input_, output_);
    numOutputRows_ += input_->size();
    input_ = nullptr;
    return output_;
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

  const auto batchSize =
      isGlobal_ ? 1 : outputBatchRows(groupingSet_->estimateRowSize());

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

void HashAggregation::noMoreInput() {
  groupingSet_->noMoreInput();
  recordSpillStats();
  Operator::noMoreInput();
}

bool HashAggregation::isFinished() {
  return finished_;
}

void HashAggregation::reclaim(uint64_t targetBytes) {
  VELOX_CHECK(canReclaim());
  auto* driver = operatorCtx_->driver();

  /// NOTE: an aggregation operator is reclaimable if it hasn't started output
  /// processing and is not under non-reclaimable execution section.
  if (noMoreInput_ || nonReclaimableSection_) {
    // TODO: add stats to record the non-reclaimable case and reduce the log
    // frequency if it is too verbose.
    LOG(WARNING) << "Can't reclaim from aggregation operator, noMoreInput_["
                 << noMoreInput_ << "], nonReclaimableSection_["
                 << nonReclaimableSection_ << "], " << toString();
    return;
  }

  // TODO: support fine-grain disk spilling based on 'targetBytes' after having
  // row container memory compaction support later.
  groupingSet_->spill(0, targetBytes);
  VELOX_CHECK_EQ(groupingSet_->numRows(), 0);
  VELOX_CHECK_EQ(groupingSet_->numDistinct(), 0);
  // Release the minimum reserved memory.
  pool()->release();
}

void HashAggregation::close() {
  Operator::close();

  output_ = nullptr;
  groupingSet_.reset();
}
} // namespace facebook::velox::exec
