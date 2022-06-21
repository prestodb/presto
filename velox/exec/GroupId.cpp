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
#include "velox/exec/GroupId.h"

namespace facebook::velox::exec {

GroupId::GroupId(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::GroupIdNode>& groupIdNode)
    : Operator(
          driverCtx,
          groupIdNode->outputType(),
          operatorId,
          groupIdNode->id(),
          "GroupId") {
  const auto& inputType = groupIdNode->sources()[0]->outputType();

  std::unordered_map<std::string, column_index_t>
      inputToOutputGroupingKeyMapping;
  for (const auto& [output, input] : groupIdNode->outputGroupingKeyNames()) {
    inputToOutputGroupingKeyMapping[input->name()] =
        outputType_->getChildIdx(output);
  }

  auto numGroupingSets = groupIdNode->groupingSets().size();
  groupingKeyMappings_.reserve(numGroupingSets);

  auto numGroupingKeys = groupIdNode->numGroupingKeys();

  for (const auto& groupingSet : groupIdNode->groupingSets()) {
    std::vector<column_index_t> mappings(numGroupingKeys, kMissingGroupingKey);
    for (const auto& groupingKey : groupingSet) {
      auto outputChannel =
          inputToOutputGroupingKeyMapping.at(groupingKey->name());
      auto inputChannel = inputType->getChildIdx(groupingKey->name());
      mappings[outputChannel] = inputChannel;
    }

    groupingKeyMappings_.emplace_back(std::move(mappings));
  }

  const auto& aggregationInputs = groupIdNode->aggregationInputs();
  aggregationInputs_.reserve(aggregationInputs.size());
  for (auto i = 0; i < aggregationInputs.size(); ++i) {
    const auto& input = aggregationInputs[i];
    aggregationInputs_.push_back(inputType->getChildIdx(input->name()));
  }
}

bool GroupId::needsInput() const {
  return !noMoreInput_ && input_ == nullptr;
}

void GroupId::addInput(RowVectorPtr input) {
  // Load Lazy vectors.
  for (auto& child : input->children()) {
    child->loadedVector();
  }

  input_ = std::move(input);
}

RowVectorPtr GroupId::getOutput() {
  if (!input_) {
    return nullptr;
  }

  // Make a copy of input for the grouping set at 'groupingSetIndex_'.
  auto numInput = input_->size();

  std::vector<VectorPtr> outputColumns(outputType_->size());

  const auto& mapping = groupingKeyMappings_[groupingSetIndex_];
  auto numGroupingKeys = mapping.size();

  // Fill in grouping keys.
  for (auto i = 0; i < numGroupingKeys; ++i) {
    if (mapping[i] == kMissingGroupingKey) {
      // Add null column.
      outputColumns[i] = BaseVector::createNullConstant(
          outputType_->childAt(i), numInput, pool());
    } else {
      outputColumns[i] = input_->childAt(mapping[i]);
    }
  }

  // Fill in aggregation inputs.
  for (auto i = 0; i < aggregationInputs_.size(); ++i) {
    outputColumns[numGroupingKeys + i] = input_->childAt(aggregationInputs_[i]);
  }

  // Add groupId column.
  outputColumns[outputType_->size() - 1] =
      BaseVector::createConstant((int64_t)groupingSetIndex_, numInput, pool());

  ++groupingSetIndex_;
  if (groupingSetIndex_ == groupingKeyMappings_.size()) {
    groupingSetIndex_ = 0;
    input_ = nullptr;
  }

  return std::make_shared<RowVector>(
      pool(), outputType_, nullptr, numInput, std::move(outputColumns));
}

} // namespace facebook::velox::exec
