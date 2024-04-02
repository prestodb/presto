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
std::tuple<std::vector<column_index_t>, std::vector<column_index_t>, RowTypePtr>
reorderInputChannels(
    const RowTypePtr& inputType,
    const std::vector<core::FieldAccessTypedExprPtr>& partitionKeys,
    const std::vector<core::FieldAccessTypedExprPtr>& sortingKeys) {
  const auto size = inputType->size();

  std::vector<column_index_t> channels;
  std::vector<column_index_t> inversedChannels;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  channels.reserve(size);
  inversedChannels.resize(size);
  names.reserve(size);
  types.reserve(size);

  std::unordered_set<std::string> keyNames;

  auto appendChannel =
      [&inputType, &channels, &inversedChannels, &names, &types](
          column_index_t channel) {
        channels.push_back(channel);
        inversedChannels[channel] = channels.size() - 1;
        names.push_back(inputType->nameOf(channel));
        types.push_back(inputType->childAt(channel));
      };

  for (const auto& key : partitionKeys) {
    auto channel = exprToChannel(key.get(), inputType);
    appendChannel(channel);
    keyNames.insert(key->name());
  }

  for (const auto& key : sortingKeys) {
    auto channel = exprToChannel(key.get(), inputType);
    appendChannel(channel);
    keyNames.insert(key->name());
  }

  for (auto i = 0; i < size; ++i) {
    if (keyNames.count(inputType->nameOf(i)) == 0) {
      appendChannel(i);
    }
  }

  return std::make_tuple(
      channels, inversedChannels, ROW(std::move(names), std::move(types)));
}

// Returns a [start, end) slice of the 'types' vector.
std::vector<TypePtr>
slice(const std::vector<TypePtr>& types, int32_t start, int32_t end) {
  std::vector<TypePtr> result;
  result.reserve(end - start);
  for (auto i = start; i < end; ++i) {
    result.push_back(types[i]);
  }
  return result;
}

} // namespace

WindowBuild::WindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    velox::memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection)
    : spillConfig_{spillConfig},
      nonReclaimableSection_{nonReclaimableSection},
      decodedInputVectors_(windowNode->inputType()->size()) {
  std::tie(inputChannels_, inversedInputChannels_, inputType_) =
      reorderInputChannels(
          windowNode->inputType(),
          windowNode->partitionKeys(),
          windowNode->sortingKeys());

  const auto numPartitionKeys = windowNode->partitionKeys().size();
  const auto numSortingKeys = windowNode->sortingKeys().size();
  const auto numKeys = numPartitionKeys + numSortingKeys;
  data_ = std::make_unique<RowContainer>(
      slice(inputType_->children(), 0, numKeys),
      slice(inputType_->children(), numKeys, inputType_->size()),
      pool);

  for (auto i = 0; i < numPartitionKeys; ++i) {
    partitionKeyInfo_.push_back(std::make_pair(i, core::SortOrder{true, true}));
  }

  for (auto i = 0; i < numSortingKeys; ++i) {
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
