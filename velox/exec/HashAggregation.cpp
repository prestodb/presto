/*
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
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

namespace {
bool allAreSinglyReferenced(
    const std::vector<ChannelIndex>& argList,
    const std::unordered_map<ChannelIndex, int>& channelUseCount) {
  for (auto channel : argList) {
    if (channelUseCount.find(channel)->second > 1) {
      return false;
    }
  }
  return true;
}

// Returns true if all vectors are Lazy vectors, possibly wrapped, that haven't
// been loaded yet.
bool areAllLazyNotLoaded(const std::vector<VectorPtr>& vectors) {
  for (const auto& vector : vectors) {
    if (!isLazyNotLoaded(*vector)) {
      return false;
    }
  }
  return true;
}
} // namespace

GroupingSet::GroupingSet(
    std::vector<std::unique_ptr<VectorHasher>>&& hashers,
    std::vector<std::unique_ptr<Aggregate>>&& aggregates,
    std::vector<std::vector<ChannelIndex>>&& channelLists,
    std::vector<std::vector<VectorPtr>>&& constantLists,
    bool ignoreNullKeys,
    OperatorCtx* operatorCtx)
    : hashers_(std::move(hashers)),
      isGlobal_(hashers_.empty()),
      aggregates_(std::move(aggregates)),
      channelLists_(std::move(channelLists)),
      constantLists_(std::move(constantLists)),
      ignoreNullKeys_(ignoreNullKeys),
      driverCtx_(operatorCtx->driverCtx()),
      mappedMemory_(operatorCtx->mappedMemory()),
      stringAllocator_(mappedMemory_),
      rows_(mappedMemory_),
      isAdaptive_(operatorCtx->task()->queryCtx()->hashAdaptivityEnabled()) {
  for (auto& hasher : hashers_) {
    keyChannels_.push_back(hasher->channel());
  }
  std::unordered_map<ChannelIndex, int> channelUseCount;
  for (auto argList : channelLists_) {
    for (int channel : argList) {
      ++channelUseCount[channel];
    }
  }
  for (auto& argList : channelLists_) {
    mayPushdown_.push_back(allAreSinglyReferenced(argList, channelUseCount));
  }
}

void GroupingSet::addInput(RowVectorPtr input, bool mayPushdown) {
  auto numRows = input->size();
  activeRows_.resize(numRows);
  activeRows_.setAll();
  if (isGlobal_) {
    // global aggregation
    if (numAdded_ == 0) {
      initializeGlobalAggregation();
    }
    numAdded_ += numRows;
    for (int32_t i = 0; i < aggregates_.size(); ++i) {
      populateTempVectors(i, input);
      aggregates_[i]->updateSingleGroup(
          lookup_->hits[0],
          activeRows_,
          tempVectors_,
          mayPushdown && mayPushdown_[i] && areAllLazyNotLoaded(tempVectors_));
    }
    tempVectors_.clear();
    return;
  }

  bool rehash = false;
  if (!table_) {
    rehash = true;
    if (ignoreNullKeys_) {
      table_ = HashTable<true>::createForAggregation(
          std::move(hashers_), aggregates_, mappedMemory_);
    } else {
      table_ = HashTable<false>::createForAggregation(
          std::move(hashers_), aggregates_, mappedMemory_);
    }
    lookup_ = std::make_unique<HashLookup>(table_->hashers());
    if (!isAdaptive_ && table_->hashMode() != BaseHashTable::HashMode::kHash) {
      table_->forceGenericHashMode();
    }
  }
  auto& hashers = lookup_->hashers;
  lookup_->reset(numRows);
  auto mode = table_->hashMode();
  if (ignoreNullKeys_) {
    // A null in any of the keys disables the row.
    deselectRowsWithNulls(*input, keyChannels_, activeRows_);
    for (int32_t i = 0; i < hashers.size(); ++i) {
      auto key = input->loadedChildAt(hashers[i]->channel());
      if (mode != BaseHashTable::HashMode::kHash) {
        if (!hashers[i]->computeValueIds(*key, activeRows_, &lookup_->hashes)) {
          rehash = true;
        }
      } else {
        hashers[i]->hash(*key, activeRows_, i > 0, &lookup_->hashes);
      }
    }
    lookup_->rows.clear();
    bits::forEachSetBit(
        activeRows_.asRange().bits(),
        0,
        activeRows_.size(),
        [&](vector_size_t row) { lookup_->rows.push_back(row); });
  } else {
    for (int32_t i = 0; i < hashers.size(); ++i) {
      auto key = input->loadedChildAt(hashers[i]->channel());
      if (mode != BaseHashTable::HashMode::kHash) {
        if (!hashers[i]->computeValueIds(*key, activeRows_, &lookup_->hashes)) {
          rehash = true;
        }
      } else {
        hashers[i]->hash(*key, activeRows_, i > 0, &lookup_->hashes);
      }
    }
    std::iota(lookup_->rows.begin(), lookup_->rows.end(), 0);
  }
  if (rehash) {
    if (table_->hashMode() != BaseHashTable::HashMode::kHash) {
      table_->decideHashMode(input->size());
    }
    addInput(input, mayPushdown);
    return;
  }
  numAdded_ += lookup_->rows.size();
  table_->groupProbe(*lookup_);
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    populateTempVectors(i, input);
    aggregates_[i]->update(
        lookup_->hits.data(),
        activeRows_,
        tempVectors_,
        mayPushdown && mayPushdown_[i] && areAllLazyNotLoaded(tempVectors_));
  }
  tempVectors_.clear();
}

void GroupingSet::initializeGlobalAggregation() {
  lookup_ = std::make_unique<HashLookup>(hashers_);
  lookup_->reset(1);

  // Row layout is:
  //  - null flags - one bit per aggregate,
  //  - fixed-width accumulators - one per aggregate
  int32_t offset = bits::nbytes(aggregates_.size());
  int32_t nullOffset = 0;

  for (auto& aggregate : aggregates_) {
    aggregate->setAllocator(&stringAllocator_);
    aggregate->setOffsets(
        offset,
        RowContainer::nullByte(nullOffset),
        RowContainer::nullMask(nullOffset));
    offset += aggregate->accumulatorFixedWidthSize();
    ++nullOffset;
  }

  auto singleGroup = std::vector<vector_size_t>{0};
  lookup_->hits[0] = rows_.allocateFixed(offset);
  for (auto& aggregate : aggregates_) {
    aggregate->initializeNewGroups(lookup_->hits.data(), singleGroup);
  }
}

void GroupingSet::populateTempVectors(
    int32_t aggregateIndex,
    const RowVectorPtr& input) {
  auto& channels = channelLists_[aggregateIndex];
  tempVectors_.resize(channels.size());
  for (int32_t channelIndex = 0; channelIndex < channels.size();
       ++channelIndex) {
    if (channels[channelIndex] == kConstantChannel) {
      tempVectors_[channelIndex] = constantLists_[aggregateIndex][channelIndex];
    } else {
      // No load of lazy vectors; The aggregate may decide to push down.
      tempVectors_[channelIndex] = input->childAt(channels[channelIndex]);
    }
  }
}

bool GroupingSet::getOutput(
    int32_t batchSize,
    bool isPartial,
    RowContainerIterator* iterator,
    RowVectorPtr result) {
  if (isGlobal_) {
    // global aggregation
    VELOX_CHECK_EQ(batchSize, 1);
    if (iterator->allocationIndex != 0) {
      return false;
    }

    if (numAdded_ == 0) {
      initializeGlobalAggregation();
    }

    auto groups = lookup_->hits.data();
    for (int32_t i = 0; i < aggregates_.size(); ++i) {
      aggregates_[i]->finalize(groups, 1);
      if (isPartial) {
        aggregates_[i]->extractAccumulators(groups, 1, &result->childAt(i));
      } else {
        aggregates_[i]->extractValues(groups, 1, &result->childAt(i));
      }
    }
    iterator->allocationIndex = std::numeric_limits<int32_t>::max();
    return true;
  }

  // @lint-ignore CLANGTIDY
  char* groups[batchSize];
  int32_t numGroups =
      table_ ? table_->rows()->listRows(iterator, batchSize, groups) : 0;
  if (!numGroups) {
    return false;
  }
  result->resize(numGroups);
  auto totalKeys = lookup_->hashers.size();
  for (int32_t i = 0; i < totalKeys; ++i) {
    auto keyVector = result->childAt(i);
    table_->rows()->extractColumn(groups, numGroups, i, keyVector);
  }
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    aggregates_[i]->finalize(groups, numGroups);
    auto aggregateVector = result->childAt(i + totalKeys);
    if (isPartial) {
      aggregates_[i]->extractAccumulators(groups, numGroups, &aggregateVector);
    } else {
      aggregates_[i]->extractValues(groups, numGroups, &aggregateVector);
    }
  }
  return true;
}

void GroupingSet::resetPartial() {
  if (table_) {
    table_->clear();
  }
}

uint64_t GroupingSet::allocatedBytes() const {
  if (table_) {
    return table_->allocatedBytes();
  }

  return stringAllocator_.retainedSize() + rows_.allocatedBytes();
}

const HashLookup& GroupingSet::hashLookup() const {
  return *lookup_;
}

HashAggregation::HashAggregation(
    int32_t operatorId,
    DriverCtx* driverCtx,
    std::shared_ptr<const core::AggregationNode> aggregationNode)
    : Operator(
          driverCtx,
          aggregationNode->outputType(),
          operatorId,
          aggregationNode->id(),
          aggregationNode->step() == core::AggregationNode::Step::kPartial
              ? "PartialAggregation"
              : "Aggregation"),
      isPartialOutput_(isPartialOutput(aggregationNode->step())),
      isDistinct_(aggregationNode->aggregates().empty()),
      isGlobal_(aggregationNode->groupingKeys().empty()),
      maxPartialAggregationMemoryUsage_(
          operatorCtx_->task()
              ->queryCtx()
              ->maxPartialAggregationMemoryUsage()) {
  auto inputType = aggregationNode->sources()[0]->outputType();

  auto numHashers = aggregationNode->groupingKeys().size();
  std::vector<std::unique_ptr<VectorHasher>> hashers;
  hashers.reserve(numHashers);
  for (auto& key : aggregationNode->groupingKeys()) {
    auto channel = exprToChannel(key.get(), inputType);
    VELOX_CHECK(
        channel != kConstantChannel,
        "Aggregation doesn't allow constant grouping keys");
    hashers.push_back(VectorHasher::create(key->type(), channel));
  }

  auto numAggregates = aggregationNode->aggregates().size();
  std::vector<std::unique_ptr<Aggregate>> aggregates;
  aggregates.reserve(numAggregates);
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
            constant->value(), BaseVector::kMaxElements, operatorCtx_->pool()));
      } else {
        constants.push_back(nullptr);
      }
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
        "Unexpected result type for an aggregation: {}, expected {}",
        aggResultType->toString(),
        expectedType->toString());
  }

  if (isDistinct_) {
    for (ChannelIndex i = 0; i < hashers.size(); ++i) {
      identityProjections_.emplace_back(hashers[i]->channel(), i);
    }
  }

  groupingSet_ = std::make_unique<GroupingSet>(
      std::move(hashers),
      std::move(aggregates),
      std::move(args),
      std::move(constantLists),
      aggregationNode->ignoreNullKeys(),
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
  if (finished_ || (!isFinishing_ && !partialFull_ && !newDistincts_)) {
    input_ = nullptr;
    return nullptr;
  }

  if (isDistinct_) {
    if (!newDistincts_) {
      if (isFinishing_) {
        finished_ = true;
      }
      return nullptr;
    }

    auto lookup = groupingSet_->hashLookup();
    auto size = lookup.newGroups.size();
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(size, operatorCtx_->pool());
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

  auto batchSize = isGlobal_ ? 1 : kOutputBatchSize;

  // TODO Figure out how to re-use 'result' safely.
  auto result = std::static_pointer_cast<RowVector>(
      BaseVector::create(outputType_, batchSize, operatorCtx_->pool()));

  bool hasData = groupingSet_->getOutput(
      batchSize, isPartialOutput_, &resultIterator_, result);
  if (!hasData) {
    resultIterator_.reset();
    if (isPartialOutput_) {
      partialFull_ = false;
      groupingSet_->resetPartial();
      if (isFinishing_) {
        finished_ = true;
      }
    } else {
      finished_ = true;
    }
    return nullptr;
  }
  return result;
}

} // namespace facebook::velox::exec
