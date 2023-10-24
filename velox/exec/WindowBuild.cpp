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

#include "velox/exec/WindowBuild.h"

#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

namespace {
std::vector<column_index_t> reorderInputChannels(
    const RowTypePtr& inputType,
    const std::vector<core::FieldAccessTypedExprPtr>& partitionKeys,
    const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys) {
  const auto size = inputType->size();

  std::vector<column_index_t> channels;
  channels.reserve(size);

  std::unordered_set<std::string> keyNames;

  for (const auto& key : partitionKeys) {
    channels.push_back(exprToChannel(key.get(), inputType));
    keyNames.insert(key->name());
  }

  for (const auto& key : sortingKeys) {
    channels.push_back(exprToChannel(key.get(), inputType));
    keyNames.insert(key->name());
  }

  for (auto i = 0; i < size; ++i) {
    if (keyNames.count(inputType->nameOf(i)) == 0) {
      channels.push_back(i);
    }
  }

  return channels;
}

RowTypePtr reorderInputType(
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& channels) {
  const auto size = inputType->size();

  VELOX_CHECK_EQ(size, channels.size());

  std::vector<std::string> names;
  names.reserve(size);

  std::vector<TypePtr> types;
  types.reserve(size);

  for (auto channel : channels) {
    names.push_back(inputType->nameOf(channel));
    types.push_back(inputType->childAt(channel));
  }

  return ROW(std::move(names), std::move(types));
}
}; // namespace

WindowBuild::WindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    velox::memory::MemoryPool* pool)
    : inputChannels_{reorderInputChannels(
          windowNode->inputType(),
          windowNode->partitionKeys(),
          windowNode->sortingKeys())},
      inputType_{reorderInputType(windowNode->inputType(), inputChannels_)},
      data_(std::make_unique<RowContainer>(inputType_->children(), pool)),
      decodedInputVectors_(inputType_->size()) {
  for (int i = 0; i < windowNode->inputType()->size(); i++) {
    const auto index =
        inputType_->getChildIdx(windowNode->inputType()->nameOf(i));
    inputColumns_.emplace_back(data_->columnAt(index));
  }

  const auto numPartitionKeys = windowNode->partitionKeys().size();
  for (auto i = 0; i < numPartitionKeys; ++i) {
    partitionKeyInfo_.push_back(std::make_pair(i, core::SortOrder{true, true}));
  }

  for (auto i = 0; i < windowNode->sortingKeys().size(); ++i) {
    sortKeyInfo_.push_back(
        std::make_pair(numPartitionKeys + i, windowNode->sortingOrders()[i]));
  }
}

bool WindowBuild::compareRowsWithKeys(
    const char* lhs,
    const char* rhs,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& keys) {
  if (lhs == rhs) {
    return false;
  }
  for (auto& key : keys) {
    if (auto result = data_->compare(
            lhs,
            rhs,
            key.first,
            {key.second.isNullsFirst(), key.second.isAscending(), false})) {
      return result < 0;
    }
  }
  return false;
}

} // namespace facebook::velox::exec
