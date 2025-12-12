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

#include "presto_cpp/main/tvf/exec/TablePartitionBuild.h"
#include <iostream>

#include "velox/exec/Operator.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;

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
    auto channel = exec::exprToChannel(key.get(), inputType);
    appendChannel(channel);
    keyNames.insert(key->name());
  }

  for (const auto& key : sortingKeys) {
    auto channel = exec::exprToChannel(key.get(), inputType);
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

std::vector<CompareFlags> makeCompareFlags(
    int32_t numPartitionKeys,
    const std::vector<core::SortOrder>& sortingOrders) {
  std::vector<CompareFlags> compareFlags;
  compareFlags.reserve(numPartitionKeys + sortingOrders.size());

  for (auto i = 0; i < numPartitionKeys; ++i) {
    compareFlags.push_back({});
  }

  for (const auto& order : sortingOrders) {
    compareFlags.push_back(
        {order.isNullsFirst(), order.isAscending(), false /*equalsOnly*/});
  }

  return compareFlags;
}

} // namespace

TablePartitionBuild::TablePartitionBuild(
    const RowTypePtr& inputType,
    std::vector<core::FieldAccessTypedExprPtr> partitionKeys,
    std::vector<core::FieldAccessTypedExprPtr> sortingKeys,
    std::vector<core::SortOrder> sortingOrders,
    memory::MemoryPool* pool,
    common::PrefixSortConfig&& prefixSortConfig)
    : pool_(pool),
      inputType_(inputType),
      compareFlags_{makeCompareFlags(partitionKeys.size(), sortingOrders)},
      prefixSortConfig_(prefixSortConfig),
      decodedInputVectors_(inputType->size()),
      sortedRows_(0, memory::StlAllocator<char*>(*pool)),
      partitionStartRows_(0, memory::StlAllocator<char*>(*pool)) {
  VELOX_CHECK_NOT_NULL(pool_);
  std::tie(inputChannels_, inversedInputChannels_, inputType_) =
      reorderInputChannels(inputType, partitionKeys, sortingKeys);

  const auto numPartitionKeys = partitionKeys.size();
  const auto numSortingKeys = sortingKeys.size();
  const auto numKeys = numPartitionKeys + numSortingKeys;

  data_ = std::make_unique<exec::RowContainer>(
      slice(inputType_->children(), 0, numKeys),
      slice(inputType_->children(), numKeys, inputType_->size()),
      pool);

  for (auto i = 0; i < numPartitionKeys; ++i) {
    partitionKeyInfo_.push_back(std::make_pair(i, core::SortOrder{true, true}));
  }

  for (auto i = 0; i < numSortingKeys; ++i) {
    sortKeyInfo_.push_back(
        std::make_pair(numPartitionKeys + i, sortingOrders[i]));
  }
}

void TablePartitionBuild::addInput(RowVectorPtr input) {
  for (auto i = 0; i < inputChannels_.size(); ++i) {
    decodedInputVectors_[i].decode(*input->childAt(inputChannels_[i]));
  }

  // Add all the rows into the RowContainer.
  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < inputChannels_.size(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }
  }
  numRows_ += input->size();
}

void TablePartitionBuild::noMoreInput() {
  if (numRows_ == 0) {
    return;
  }

  // At this point we have seen all the input rows. The operator is
  // being prepared to output rows now.
  // To prepare the rows for output in SortWindowBuild they need to
  // be separated into partitions and sort by ORDER BY keys within
  // the partition. This will order the rows for getOutput().
  sortPartitions();
}

void TablePartitionBuild::sortPartitions() {
  // This is a very inefficient but easy implementation to order the input rows
  // by partition keys + sort keys.
  // Sort the pointers to the rows in RowContainer (data_) instead of sorting
  // the rows.
  sortedRows_.resize(numRows_);
  exec::RowContainerIterator iter;
  data_->listRows(&iter, numRows_, sortedRows_.data());

  exec::PrefixSort::sort(
      data_.get(), compareFlags_, prefixSortConfig_, pool_, sortedRows_);

  computePartitionStartRows();
}

void TablePartitionBuild::computePartitionStartRows() {
  partitionStartRows_.reserve(numRows_);

  // Using a sequential traversal to find changing partitions.
  // This algorithm is inefficient and can be changed
  // i) Use a binary search kind of strategy.
  // ii) If we use a Hashtable instead of a full sort then the count
  // of rows in the partition can be directly used.
  partitionStartRows_.push_back(0);

  VELOX_CHECK_GT(sortedRows_.size(), 0);

  vector_size_t start = 0;
  while (start < sortedRows_.size()) {
    auto next = findNextPartitionStartRow(start);
    partitionStartRows_.push_back(next);
    start = next;
  }
}

bool TablePartitionBuild::compareRowsWithKeys(
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

// Use double front and back search algorithm to find next partition start row.
// It is more efficient than linear or binary search.
// This algorithm is described at
// https://medium.com/@insomniocode/search-algorithm-double-front-and-back-20f5f28512e7
vector_size_t TablePartitionBuild::findNextPartitionStartRow(
    vector_size_t start) {
  auto partitionCompare = [&](const char* lhs, const char* rhs) -> bool {
    return compareRowsWithKeys(lhs, rhs, partitionKeyInfo_);
  };

  auto left = start;
  auto right = left + 1;
  auto lastPosition = sortedRows_.size();
  while (right < lastPosition) {
    auto distance = 1;
    for (; distance < lastPosition - left; distance *= 2) {
      right = left + distance;
      if (partitionCompare(sortedRows_[left], sortedRows_[right]) != 0) {
        lastPosition = right;
        break;
      }
    }
    left += distance / 2;
    right = left + 1;
  }
  return right;
}

std::shared_ptr<TableFunctionPartition> TablePartitionBuild::nextPartition() {
  VELOX_CHECK(
      !partitionStartRows_.empty(), "No table function partitions available");

  currentPartition_++;
  VELOX_CHECK_LE(
      currentPartition_,
      partitionStartRows_.size() - 2,
      "All table function partitions consumed");

  // There is partition data available now.
  auto partitionSize = partitionStartRows_[currentPartition_ + 1] -
      partitionStartRows_[currentPartition_];
  auto partition = folly::Range(
      sortedRows_.data() + partitionStartRows_[currentPartition_],
      partitionSize);
  return std::make_shared<TableFunctionPartition>(
      data_.get(), partition, inversedInputChannels_);
}

bool TablePartitionBuild::hasNextPartition() {
  return partitionStartRows_.size() > 0 &&
      currentPartition_ < static_cast<int>(partitionStartRows_.size() - 2);
}

} // namespace facebook::presto::tvf
