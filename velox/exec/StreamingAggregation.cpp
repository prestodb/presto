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
#include "velox/exec/StreamingAggregation.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/RowContainer.h"

namespace facebook::velox::exec {

StreamingAggregation::StreamingAggregation(
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
      step_{aggregationNode->step()} {
  auto numKeys = aggregationNode->groupingKeys().size();
  decodedKeys_.resize(numKeys);

  auto inputType = aggregationNode->sources()[0]->outputType();

  std::vector<TypePtr> groupingKeyTypes;
  groupingKeyTypes.reserve(numKeys);

  groupingKeys_.reserve(numKeys);
  for (const auto& key : aggregationNode->groupingKeys()) {
    auto channel = exprToChannel(key.get(), inputType);
    groupingKeys_.push_back(channel);
    groupingKeyTypes.push_back(inputType->childAt(channel));
  }

  auto numAggregates = aggregationNode->aggregates().size();
  aggregates_.reserve(numAggregates);
  std::vector<std::optional<column_index_t>> maskChannels;
  maskChannels.reserve(numAggregates);
  for (auto i = 0; i < numAggregates; i++) {
    const auto& aggregate = aggregationNode->aggregates()[i];

    std::vector<column_index_t> channels;
    std::vector<VectorPtr> constants;
    std::vector<TypePtr> argTypes;
    for (auto& arg : aggregate->inputs()) {
      argTypes.push_back(arg->type());
      channels.push_back(exprToChannel(arg.get(), inputType));
      if (channels.back() == kConstantChannel) {
        auto constant = static_cast<const core::ConstantTypedExpr*>(arg.get());
        constants.push_back(BaseVector::createConstant(
            constant->value(), 1, operatorCtx_->pool()));
      } else {
        constants.push_back(nullptr);
      }
    }

    const auto& mask = aggregationNode->aggregateMasks()[i];
    if (mask == nullptr) {
      maskChannels.emplace_back(std::nullopt);
    } else {
      maskChannels.emplace_back(inputType->asRow().getChildIdx(mask->name()));
    }

    const auto& aggResultType = outputType_->childAt(numKeys + i);
    aggregates_.push_back(Aggregate::create(
        aggregate->name(), aggregationNode->step(), argTypes, aggResultType));
    args_.push_back(channels);
    constantArgs_.push_back(constants);
  }

  if (aggregationNode->ignoreNullKeys()) {
    VELOX_NYI("Streaming aggregation doesn't support ignoring null keys yet");
  }

  masks_ = std::make_unique<AggregationMasks>(std::move(maskChannels));

  rows_ = std::make_unique<RowContainer>(
      groupingKeyTypes,
      !aggregationNode->ignoreNullKeys(),
      aggregates_,
      std::vector<TypePtr>{},
      false,
      false,
      false,
      false,
      operatorCtx_->mappedMemory(),
      ContainerRowSerde::instance());
}

void StreamingAggregation::close() {
  for (int32_t i = 0; i < aggregates_.size(); ++i) {
    if (aggregates_[i]->accumulatorUsesExternalMemory()) {
      aggregates_[i]->destroy(folly::Range(groups_.data(), groups_.size()));
    }
  }
  Operator::close();
}

void StreamingAggregation::addInput(RowVectorPtr input) {
  input_ = std::move(input);
}

namespace {
// Compares a row in one vector with another row in another vector and returns
// true if two rows match in all grouping key columns.
bool equalKeys(
    const std::vector<column_index_t>& keys,
    const RowVectorPtr& batch,
    vector_size_t index,
    const RowVectorPtr& otherBatch,
    vector_size_t otherIndex) {
  for (auto key : keys) {
    if (!batch->childAt(key)->equalValueAt(
            otherBatch->childAt(key).get(), index, otherIndex)) {
      return false;
    }
  }

  return true;
}
} // namespace

char* StreamingAggregation::startNewGroup(vector_size_t index) {
  if (numGroups_ < groups_.size()) {
    auto group = groups_[numGroups_++];
    rows_->initializeRow(group, true);
    storeKeys(group, index);
    return group;
  }

  auto* newGroup = rows_->newRow();
  storeKeys(newGroup, index);

  groups_.resize(numGroups_ + 1);
  groups_[numGroups_++] = newGroup;
  return newGroup;
}

void StreamingAggregation::storeKeys(char* group, vector_size_t index) {
  for (auto i = 0; i < groupingKeys_.size(); ++i) {
    rows_->store(decodedKeys_[i], index, group, i);
  }
}

RowVectorPtr StreamingAggregation::createOutput(size_t numGroups) {
  auto output = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(outputType_, numGroups, pool()));

  for (auto i = 0; i < groupingKeys_.size(); ++i) {
    rows_->extractColumn(groups_.data(), numGroups, i, output->childAt(i));
  }

  auto numKeys = groupingKeys_.size();
  for (auto i = 0; i < aggregates_.size(); ++i) {
    auto& aggregate = aggregates_[i];
    aggregate->finalize(groups_.data(), numGroups);
    auto& result = output->childAt(numKeys + i);
    if (isPartialOutput(step_)) {
      aggregate->extractAccumulators(groups_.data(), numGroups, &result);
    } else {
      aggregate->extractValues(groups_.data(), numGroups, &result);
    }
  }

  return output;
}

void StreamingAggregation::assignGroups() {
  auto numInput = input_->size();

  inputGroups_.resize(numInput);

  // Look for the end of the last group.
  vector_size_t index = 0;
  if (prevInput_) {
    auto prevIndex = prevInput_->size() - 1;
    auto* prevGroup = groups_[numGroups_ - 1];
    for (; index < numInput; ++index) {
      if (equalKeys(groupingKeys_, prevInput_, prevIndex, input_, index)) {
        inputGroups_[index] = prevGroup;
      } else {
        break;
      }
    }
  }

  if (index < numInput) {
    for (auto i = 0; i < groupingKeys_.size(); ++i) {
      decodedKeys_[i].decode(*input_->childAt(groupingKeys_[i]), inputRows_);
    }

    auto* newGroup = startNewGroup(index);
    inputGroups_[index] = newGroup;

    for (auto i = index + 1; i < numInput; ++i) {
      if (equalKeys(groupingKeys_, input_, index, input_, i)) {
        inputGroups_[i] = inputGroups_[index];
      } else {
        newGroup = startNewGroup(i);
        inputGroups_[i] = newGroup;
        index = i;
      }
    }
  }
}

const SelectivityVector& StreamingAggregation::getSelectivityVector(
    size_t aggregateIndex) const {
  auto* rows = masks_->activeRows(aggregateIndex);

  // No mask? Use the current selectivity vector for this aggregation.
  return rows ? *rows : inputRows_;
}

void StreamingAggregation::evaluateAggregates() {
  for (auto i = 0; i < aggregates_.size(); ++i) {
    auto& aggregate = aggregates_[i];

    std::vector<VectorPtr> args;
    for (auto j = 0; j < args_[i].size(); ++j) {
      if (args_[i][j] == kConstantChannel) {
        args.push_back(constantArgs_[i][j]);
      } else {
        args.push_back(input_->childAt(args_[i][j]));
      }
    }

    const auto& rows = getSelectivityVector(i);

    if (isRawInput(step_)) {
      aggregate->addRawInput(inputGroups_.data(), rows, args, false);
    } else {
      aggregate->addIntermediateResults(inputGroups_.data(), rows, args, false);
    }
  }
}

bool StreamingAggregation::isFinished() {
  return noMoreInput_ && input_ == nullptr && numGroups_ == 0;
}

RowVectorPtr StreamingAggregation::getOutput() {
  if (!input_) {
    if (noMoreInput_ && numGroups_ > 0) {
      auto output = createOutput(numGroups_);
      numGroups_ = 0;
      return output;
    }
    return nullptr;
  }

  auto numInput = input_->size();
  inputRows_.resize(numInput);
  inputRows_.setAll();

  masks_->addInput(input_, inputRows_);

  auto numPrevGroups = numGroups_;

  assignGroups();

  // Initialize aggregates for the new groups.
  std::vector<vector_size_t> newGroups;
  newGroups.resize(numGroups_ - numPrevGroups);
  std::iota(newGroups.begin(), newGroups.end(), numPrevGroups);

  for (auto i = 0; i < aggregates_.size(); ++i) {
    auto& aggregate = aggregates_[i];

    aggregate->initializeNewGroups(
        groups_.data(), folly::Range(newGroups.data(), newGroups.size()));
  }

  evaluateAggregates();

  RowVectorPtr output;
  if (numGroups_ > outputBatchSize_) {
    output = createOutput(outputBatchSize_);

    // Rotate the entries in the groups_ vector to move the remaining groups to
    // the beginning and place re-usable groups at the end.
    std::vector<char*> copy(groups_.size());
    std::copy(groups_.begin() + outputBatchSize_, groups_.end(), copy.begin());
    std::copy(
        groups_.begin(),
        groups_.begin() + outputBatchSize_,
        copy.begin() + groups_.size() - outputBatchSize_);
    groups_ = std::move(copy);
    numGroups_ -= outputBatchSize_;
  }

  prevInput_ = input_;
  input_ = nullptr;

  return output;
}

} // namespace facebook::velox::exec
