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
#include "velox/exec/SortedAggregations.h"

namespace facebook::velox::exec {

namespace {

/// Stores a list of char* pointers to rows in RowContainer.
struct RowPointers {
  HashStringAllocator::Header* firstBlock{nullptr};
  HashStringAllocator::Position currentBlock{nullptr, nullptr};
  size_t size{0};

  void append(char* row, HashStringAllocator& allocator) {
    ByteStream stream(&allocator);
    if (firstBlock == nullptr) {
      // Allocate first block.
      currentBlock = allocator.newWrite(stream);
      firstBlock = currentBlock.header;
    } else {
      allocator.extendWrite(currentBlock, stream);
    }

    stream.appendOne(reinterpret_cast<uintptr_t>(row));
    currentBlock = allocator.finishWrite(stream, 1024);

    ++size;
  }

  void free(HashStringAllocator& allocator) {
    if (firstBlock != nullptr) {
      allocator.free(firstBlock);
      firstBlock = nullptr;
      currentBlock = {nullptr, nullptr};
    }
  }

  std::vector<char*> read(HashStringAllocator& allocator) {
    ByteStream stream(&allocator);
    HashStringAllocator::prepareRead(firstBlock, stream);

    std::vector<char*> rows(size);
    for (auto i = 0; i < size; ++i) {
      rows[i] = reinterpret_cast<char*>(stream.read<uintptr_t>());
    }
    return rows;
  }
};
} // namespace

SortedAggregations::SortedAggregations(
    std::vector<AggregateInfo*> aggregates,
    const RowTypePtr& inputType,
    memory::MemoryPool* pool)
    : aggregates_{std::move(aggregates)} {
  // Collect inputs and sorting keys from all aggregates.
  std::unordered_set<column_index_t> allInputs;
  for (const auto* aggregate : aggregates_) {
    for (auto i = 0; i < aggregate->inputs.size(); ++i) {
      auto input = aggregate->inputs[i];
      if (input != kConstantChannel) {
        allInputs.insert(input);
      } else {
        VELOX_USER_CHECK_NOT_NULL(aggregate->constantInputs[i]);
      }
    }

    for (auto sortingKey : aggregate->sortingKeys) {
      VELOX_USER_CHECK_NE(sortingKey, kConstantChannel);
      allInputs.insert(sortingKey);
    }
  }

  inputMapping_.resize(inputType->size());

  std::vector<TypePtr> types;
  for (auto input : allInputs) {
    types.push_back(inputType->childAt(input));

    inputMapping_[input] = inputs_.size();
    inputs_.push_back(input);
  }

  inputData_ = std::make_unique<RowContainer>(types, pool);
  decodedInputs_.resize(inputs_.size());
}

Accumulator SortedAggregations::accumulator() const {
  return {
      false,
      sizeof(RowPointers),
      false,
      1,
      [this](folly::Range<char**> groups) {
        for (auto* group : groups) {
          auto* accumulator = reinterpret_cast<RowPointers*>(group + offset_);
          accumulator->free(*allocator_);
        }
      }};
}

void SortedAggregations::initializeNewGroups(
    char** groups,
    folly::Range<const vector_size_t*> indices) {
  for (auto i : indices) {
    groups[i][nullByte_] |= nullMask_;
    new (groups[i] + offset_) RowPointers();
  }

  for (auto i = 0; i < aggregates_.size(); ++i) {
    const auto& aggregate = *aggregates_[i];
    aggregate.function->initializeNewGroups(groups, indices);
  }
}

void SortedAggregations::addInput(char** groups, const RowVectorPtr& input) {
  for (auto i = 0; i < inputs_.size(); ++i) {
    decodedInputs_[i].decode(*input->childAt(inputs_[i]));
  }

  // Add all the rows into the RowContainer.
  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = inputData_->newRow();

    for (auto i = 0; i < inputs_.size(); ++i) {
      inputData_->store(decodedInputs_[i], row, newRow, i);
    }

    addNewRow(groups[row], newRow);
  }
}

void SortedAggregations::addNewRow(char* group, char* newRow) {
  auto* accumulator = reinterpret_cast<RowPointers*>(group + offset_);
  RowSizeTracker<char, uint32_t> tracker(group[rowSizeOffset_], *allocator_);
  accumulator->append(newRow, *allocator_);
}

void SortedAggregations::addSingleGroupInput(
    char* group,
    const RowVectorPtr& input) {
  for (auto i = 0; i < inputs_.size(); ++i) {
    decodedInputs_[i].decode(*input->childAt(inputs_[i]));
  }

  // Add all the rows into the RowContainer.
  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = inputData_->newRow();

    for (auto i = 0; i < inputs_.size(); ++i) {
      inputData_->store(decodedInputs_[i], row, newRow, i);
    }

    addNewRow(group, newRow);
  }
}

bool SortedAggregations::compareRowsWithKeys(
    const char* lhs,
    const char* rhs,
    const std::vector<std::pair<column_index_t, core::SortOrder>>& keys) {
  if (lhs == rhs) {
    return false;
  }
  for (auto& key : keys) {
    if (auto result = inputData_->compare(
            lhs,
            rhs,
            key.first,
            {key.second.isNullsFirst(), key.second.isAscending(), false})) {
      return result < 0;
    }
  }
  return false;
}

void SortedAggregations::noMoreInput() {}

void SortedAggregations::sortSingleGroup(
    std::vector<char*>& groupRows,
    const AggregateInfo& aggregate) {
  std::vector<std::pair<column_index_t, core::SortOrder>> keys;
  for (auto i = 0; i < aggregate.sortingKeys.size(); ++i) {
    keys.push_back(
        {inputMapping_[aggregate.sortingKeys[i]], aggregate.sortingOrders[i]});
  }

  std::sort(
      groupRows.begin(),
      groupRows.end(),
      [&](const char* leftRow, const char* rightRow) {
        return compareRowsWithKeys(leftRow, rightRow, keys);
      });
}

std::vector<VectorPtr> SortedAggregations::extractSingleGroup(
    std::vector<char*>& groupRows,
    const AggregateInfo& aggregate) {
  const auto numInputs = aggregate.inputs.size();
  std::vector<VectorPtr> inputVectors(numInputs);
  for (auto i = 0; i < numInputs; ++i) {
    if (aggregate.inputs[i] == kConstantChannel) {
      inputVectors[i] = aggregate.constantInputs[i];
    } else {
      const auto columnIndex = inputMapping_[aggregate.inputs[i]];
      inputVectors[i] = BaseVector::create(
          inputData_->keyTypes()[columnIndex],
          groupRows.size(),
          inputData_->pool());
      inputData_->extractColumn(
          groupRows.data(), groupRows.size(), columnIndex, inputVectors[i]);
    }
  }

  return inputVectors;
}

void SortedAggregations::extractValues(
    folly::Range<char**> groups,
    const RowVectorPtr& result) {
  // TODO Identify aggregates with same order by and sort once.

  SelectivityVector rows;
  for (auto i = 0; i < aggregates_.size(); ++i) {
    const auto& aggregate = *aggregates_[i];

    // For each group, sort inputs, add them to aggregate.
    for (auto* group : groups) {
      auto groupRows =
          reinterpret_cast<RowPointers*>(group + offset_)->read(*allocator_);

      sortSingleGroup(groupRows, aggregate);

      // TODO Process group rows in batches to avoid creating very large input
      // vectors.
      auto inputVectors = extractSingleGroup(groupRows, aggregate);

      rows.resize(groupRows.size());
      aggregate.function->addSingleGroupRawInput(
          group, rows, inputVectors, false);
    }

    aggregate.function->extractValues(
        groups.data(), groups.size(), &result->childAt(aggregate.output));

    // Release memory back to HashStringAllocator to allow next aggregate to
    // re-use it.
    aggregate.function->destroy(groups);
  }
}

} // namespace facebook::velox::exec
