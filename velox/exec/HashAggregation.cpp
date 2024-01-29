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
#include "velox/exec/Task.h"
#include "velox/expression/Expr.h"

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
      aggregationNode_(aggregationNode),
      isPartialOutput_(isPartialOutput(aggregationNode->step())),
      isGlobal_(aggregationNode->groupingKeys().empty()),
      isDistinct_(!isGlobal_ && aggregationNode->aggregates().empty()),
      maxExtendedPartialAggregationMemoryUsage_(
          driverCtx->queryConfig().maxExtendedPartialAggregationMemoryUsage()),
      abandonPartialAggregationMinRows_(
          driverCtx->queryConfig().abandonPartialAggregationMinRows()),
      abandonPartialAggregationMinPct_(
          driverCtx->queryConfig().abandonPartialAggregationMinPct()),
      maxPartialAggregationMemoryUsage_(
          driverCtx->queryConfig().maxPartialAggregationMemoryUsage()) {}

void HashAggregation::initialize() {
  Operator::initialize();

  VELOX_CHECK(pool()->trackUsage());

  const auto& inputType = aggregationNode_->sources()[0]->outputType();
  auto hashers =
      createVectorHashers(inputType, aggregationNode_->groupingKeys());
  auto numHashers = hashers.size();

  std::vector<column_index_t> preGroupedChannels;
  preGroupedChannels.reserve(aggregationNode_->preGroupedKeys().size());
  for (const auto& key : aggregationNode_->preGroupedKeys()) {
    auto channel = exprToChannel(key.get(), inputType);
    preGroupedChannels.push_back(channel);
  }

  std::shared_ptr<core::ExpressionEvaluator> expressionEvaluator;
  std::vector<AggregateInfo> aggregateInfos = toAggregateInfo(
      *aggregationNode_, *operatorCtx_, numHashers, expressionEvaluator);

  // Check that aggregate result type match the output type.
  for (auto i = 0; i < aggregateInfos.size(); i++) {
    const auto& aggResultType = aggregateInfos[i].function->resultType();
    const auto& expectedType = outputType_->childAt(numHashers + i);
    VELOX_CHECK(
        aggResultType->kindEquals(expectedType),
        "Unexpected result type for an aggregation: {}, expected {}, step {}",
        aggResultType->toString(),
        expectedType->toString(),
        core::AggregationNode::stepName(aggregationNode_->step()));
  }

  if (isDistinct_) {
    for (auto i = 0; i < hashers.size(); ++i) {
      identityProjections_.emplace_back(hashers[i]->channel(), i);
    }
  }

  std::optional<column_index_t> groupIdChannel;
  if (aggregationNode_->groupId().has_value()) {
    groupIdChannel = outputType_->getChildIdxIfExists(
        aggregationNode_->groupId().value()->name());
    VELOX_CHECK(groupIdChannel.has_value());
  }

  groupingSet_ = std::make_unique<GroupingSet>(
      inputType,
      std::move(hashers),
      std::move(preGroupedChannels),
      std::move(aggregateInfos),
      aggregationNode_->ignoreNullKeys(),
      isPartialOutput_,
      isRawInput(aggregationNode_->step()),
      aggregationNode_->globalGroupingSets(),
      groupIdChannel,
      spillConfig_.has_value() ? &spillConfig_.value() : nullptr,
      &nonReclaimableSection_,
      operatorCtx_.get());

  aggregationNode_.reset();
}

bool HashAggregation::abandonPartialAggregationEarly(int64_t numOutput) const {
  VELOX_CHECK(isPartialOutput_ && !isGlobal_);
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

  updateRuntimeStats();

  // NOTE: we should not trigger partial output flush in case of global
  // aggregation as the final aggregator will handle it the same way as the
  // partial aggregator. Hence, we have to use more memory anyway.
  const bool abandonPartialEarly = isPartialOutput_ && !isGlobal_ &&
      abandonPartialAggregationEarly(groupingSet_->numDistinct());
  if (isPartialOutput_ && !isGlobal_ &&
      (abandonPartialEarly ||
       groupingSet_->isPartialFull(maxPartialAggregationMemoryUsage_))) {
    partialFull_ = true;
  }

  if (isDistinct_) {
    newDistincts_ = !groupingSet_->hasSpilled() &&
        !groupingSet_->hashLookup().newGroups.empty();

    if (newDistincts_) {
      // Save input to use for output in getOutput().
      input_ = input;
    } else {
      // If no new distinct groups (meaning we don't have anything to output),
      // then we need to ensure we 'need input'. For that we need to reset
      // the 'partial full' flag.
      partialFull_ = false;
    }
  }
}

void HashAggregation::updateRuntimeStats() {
  // Report range sizes and number of distinct values for the group-by keys.
  const auto& hashers = groupingSet_->hashLookup().hashers;
  uint64_t asRange;
  uint64_t asDistinct;
  const auto hashTableStats = groupingSet_->hashTableStats();

  auto lockedStats = stats_.wlock();
  auto& runtimeStats = lockedStats->runtimeStats;

  for (auto i = 0; i < hashers.size(); i++) {
    hashers[i]->cardinality(0, asRange, asDistinct);
    if (asRange != VectorHasher::kRangeTooLarge) {
      runtimeStats[fmt::format("rangeKey{}", i)] = RuntimeMetric(asRange);
    }
    if (asDistinct != VectorHasher::kRangeTooLarge) {
      runtimeStats[fmt::format("distinctKey{}", i)] = RuntimeMetric(asDistinct);
    }
  }

  runtimeStats["hashtable.capacity"] = RuntimeMetric(hashTableStats.capacity);
  runtimeStats["hashtable.numRehashes"] =
      RuntimeMetric(hashTableStats.numRehashes);
  runtimeStats["hashtable.numDistinct"] =
      RuntimeMetric(hashTableStats.numDistinct);
  runtimeStats["hashtable.numTombstones"] =
      RuntimeMetric(hashTableStats.numTombstones);
}

void HashAggregation::recordSpillStats() {
  auto spillStatsOr = groupingSet_->spilledStats();
  if (spillStatsOr.has_value()) {
    Operator::recordSpillStats(spillStatsOr.value());
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
  groupingSet_->resetTable();
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
    return getDistinctOutput();
  }

  const auto& queryConfig = operatorCtx_->driverCtx()->queryConfig();
  const auto maxOutputRows =
      isGlobal_ ? 1 : outputBatchRows(estimatedOutputRowSize_);
  // Reuse output vectors if possible.
  prepareOutput(maxOutputRows);

  const bool hasData = groupingSet_->getOutput(
      maxOutputRows,
      queryConfig.preferredOutputBatchBytes(),
      resultIterator_,
      output_);
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

RowVectorPtr HashAggregation::getDistinctOutput() {
  VELOX_CHECK(isDistinct_);
  VELOX_CHECK(!finished_);

  if (newDistincts_) {
    VELOX_CHECK_NOT_NULL(input_);

    auto& lookup = groupingSet_->hashLookup();
    const auto size = lookup.newGroups.size();
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
  VELOX_CHECK(!newDistincts_);

  if (!groupingSet_->hasSpilled()) {
    if (noMoreInput_) {
      finished_ = true;
      if (auto numRows = groupingSet_->numDefaultGlobalGroupingSetRows()) {
        prepareOutput(numRows.value());
        if (groupingSet_->getDefaultGlobalGroupingSetOutput(
                resultIterator_, output_)) {
          numOutputRows_ += output_->size();
          return output_;
        }
      }
    }
    return nullptr;
  }

  if (!noMoreInput_) {
    return nullptr;
  }

  const auto& queryConfig = operatorCtx_->driverCtx()->queryConfig();
  const auto maxOutputRows = outputBatchRows(estimatedOutputRowSize_);
  prepareOutput(maxOutputRows);
  if (!groupingSet_->getOutput(
          maxOutputRows,
          queryConfig.preferredOutputBatchBytes(),
          resultIterator_,
          output_)) {
    finished_ = true;
    return nullptr;
  }
  numOutputRows_ += output_->size();
  return output_;
}

void HashAggregation::noMoreInput() {
  updateEstimatedOutputRowSize();
  groupingSet_->noMoreInput();
  Operator::noMoreInput();
  recordSpillStats();
  // Release the extra reserved memory right after processing all the inputs.
  pool()->release();
}

bool HashAggregation::isFinished() {
  return finished_;
}

void HashAggregation::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK(canReclaim());
  VELOX_CHECK(!nonReclaimableSection_);

  if (groupingSet_ == nullptr) {
    return;
  }

  updateEstimatedOutputRowSize();

  if (noMoreInput_) {
    if (groupingSet_->hasSpilled()) {
      LOG(WARNING)
          << "Can't reclaim from aggregation operator which has spilled and is under output processing, pool "
          << pool()->name()
          << ", memory usage: " << succinctBytes(pool()->currentBytes())
          << ", reservation: " << succinctBytes(pool()->reservedBytes());
      return;
    }
    if (isDistinct_) {
      // Since we have seen all the input, we can safely reset the hash table.
      groupingSet_->resetTable();
      // Release the minimum reserved memory.
      pool()->release();
      return;
    }

    // Spill all the rows starting from the next output row pointed by
    // 'resultIterator_'.
    groupingSet_->spill(resultIterator_);
    // NOTE: we will only spill once during the output processing stage so
    // record stats here.
    recordSpillStats();
  } else {
    // TODO: support fine-grain disk spilling based on 'targetBytes' after
    // having row container memory compaction support later.
    groupingSet_->spill();
  }
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

void HashAggregation::abort() {
  close();
}

void HashAggregation::updateEstimatedOutputRowSize() {
  const auto optionalRowSize = groupingSet_->estimateOutputRowSize();
  if (!optionalRowSize.has_value()) {
    return;
  }

  const auto rowSize = optionalRowSize.value();

  if (!estimatedOutputRowSize_.has_value()) {
    estimatedOutputRowSize_ = rowSize;
  } else if (rowSize > estimatedOutputRowSize_.value()) {
    estimatedOutputRowSize_ = rowSize;
  }
}
} // namespace facebook::velox::exec
