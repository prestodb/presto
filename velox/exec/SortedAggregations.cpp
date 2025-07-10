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
#include "velox/common/memory/RawVector.h"

namespace facebook::velox::exec {

namespace {

/// Stores a list of char* pointers to rows in RowContainer.
struct RowPointers {
  HashStringAllocator::Header* firstBlock{nullptr};
  HashStringAllocator::Position currentBlock{nullptr, nullptr};
  size_t size{0};

  void append(char* row, HashStringAllocator& allocator) {
    ByteOutputStream stream(&allocator);
    if (firstBlock == nullptr) {
      // Allocate first block.
      currentBlock = allocator.newWrite(stream);
      firstBlock = currentBlock.header;
    } else {
      allocator.extendWrite(currentBlock, stream);
    }

    stream.appendOne(reinterpret_cast<uintptr_t>(row));
    currentBlock = allocator.finishWrite(stream, 1024).second;

    ++size;
  }

  void free(HashStringAllocator& allocator) {
    if (firstBlock != nullptr) {
      allocator.free(firstBlock);
      firstBlock = nullptr;
      currentBlock = {nullptr, nullptr};
    }
  }

  void read(folly::Range<char**> rows) {
    HashStringAllocator::InputStream stream(firstBlock);

    for (auto i = 0; i < size; ++i) {
      rows[i] = reinterpret_cast<char*>(stream.read<uintptr_t>());
    }
  }
};
} // namespace

SortedAggregations::SortedAggregations(
    const std::vector<const AggregateInfo*>& aggregates,
    const RowTypePtr& inputType,
    memory::MemoryPool* pool)
    : pool_(pool) {
  // Collect inputs and sorting keys from all aggregates.
  std::unordered_set<column_index_t> allInputs;
  for (const auto* aggregate : aggregates) {
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

    if (aggregate->mask) {
      allInputs.insert(aggregate->mask.value());
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

  for (const auto& aggregate : aggregates) {
    auto it =
        aggregates_
            .emplace(
                toSortingSpec(*aggregate), std::vector<const AggregateInfo*>{})
            .first;
    it->second.push_back(aggregate);
  }
}

std::unique_ptr<SortedAggregations> SortedAggregations::create(
    const std::vector<AggregateInfo>& aggregates,
    const RowTypePtr& inputType,
    memory::MemoryPool* pool) {
  std::vector<const AggregateInfo*> sortedAggs;
  for (auto& aggregate : aggregates) {
    if (!aggregate.sortingKeys.empty()) {
      VELOX_USER_CHECK(
          !aggregate.distinct,
          "Aggregations over sorted unique values are not supported yet");
      sortedAggs.push_back(&aggregate);
    }
  }

  if (sortedAggs.empty()) {
    return nullptr;
  }

  return std::make_unique<SortedAggregations>(sortedAggs, inputType, pool);
}

SortedAggregations::SortingSpec SortedAggregations::toSortingSpec(
    const AggregateInfo& aggregate) const {
  SortingSpec sortingSpec;
  for (auto i = 0; i < aggregate.sortingKeys.size(); ++i) {
    sortingSpec.push_back(
        {inputMapping_[aggregate.sortingKeys[i]], aggregate.sortingOrders[i]});
  }
  return sortingSpec;
}

Accumulator SortedAggregations::accumulator() const {
  return {
      false,
      sizeof(RowPointers),
      false,
      1,
      ARRAY(VARBINARY()),
      [this](folly::Range<char**> groups, VectorPtr& result) {
        extractForSpill(groups, result);
      },
      [this](folly::Range<char**> groups) {
        for (auto* group : groups) {
          auto* accumulator = reinterpret_cast<RowPointers*>(group + offset_);
          accumulator->free(*allocator_);
        }
      }};
}

void SortedAggregations::extractForSpill(
    folly::Range<char**> groups,
    VectorPtr& result) const {
  auto* arrayVector = result->as<ArrayVector>();
  arrayVector->resize(groups.size());

  auto* rawOffsets =
      arrayVector->mutableOffsets(groups.size())->asMutable<vector_size_t>();
  auto* rawSizes =
      arrayVector->mutableSizes(groups.size())->asMutable<vector_size_t>();

  vector_size_t offset = 0;
  for (auto i = 0; i < groups.size(); ++i) {
    auto* accumulator = reinterpret_cast<RowPointers*>(groups[i] + offset_);
    rawSizes[i] = accumulator->size;
    rawOffsets[i] = offset;
    offset += accumulator->size;
  }

  std::vector<char*> groupRows(offset);

  offset = 0;
  for (auto i = 0; i < groups.size(); ++i) {
    auto* accumulator = reinterpret_cast<RowPointers*>(groups[i] + offset_);
    accumulator->read(
        folly::Range(groupRows.data() + offset, accumulator->size));
    offset += accumulator->size;
  }

  auto& elementsVector = arrayVector->elements();
  elementsVector->resize(offset);
  inputData_->extractSerializedRows(
      folly::Range(groupRows.data(), groupRows.size()), elementsVector);
}

void SortedAggregations::clear() {
  inputData_->clear();
}

void SortedAggregations::initializeNewGroups(
    char** groups,
    folly::Range<const vector_size_t*> indices) {
  for (auto i : indices) {
    groups[i][nullByte_] |= nullMask_;
    new (groups[i] + offset_) RowPointers();
    groups[i][initializedByte_] |= initializedMask_;
  }

  for (const auto& [sortingSpec, aggregates] : aggregates_) {
    for (const auto& aggregate : aggregates) {
      aggregate->function->initializeNewGroups(groups, indices);
    }
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

void SortedAggregations::addSingleGroupSpillInput(
    char* group,
    const VectorPtr& input,
    vector_size_t index) {
  auto* arrayVector = input->as<ArrayVector>();
  auto* elementsVector = arrayVector->elements()->asFlatVector<StringView>();

  const auto size = arrayVector->sizeAt(index);
  const auto offset = arrayVector->offsetAt(index);
  for (auto i = 0; i < size; ++i) {
    char* newRow = inputData_->newRow();
    inputData_->storeSerializedRow(*elementsVector, offset + i, newRow);
    addNewRow(group, newRow);
  }
}

bool SortedAggregations::compareRowsWithKeys(
    const char* lhs,
    const char* rhs,
    const SortingSpec& sortingSpec) {
  if (lhs == rhs) {
    return false;
  }
  for (auto& key : sortingSpec) {
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

void SortedAggregations::sortSingleGroup(
    std::vector<char*>& groupRows,
    const SortingSpec& sortingSpec) {
  std::sort(
      groupRows.begin(),
      groupRows.end(),
      [&](const char* leftRow, const char* rightRow) {
        return compareRowsWithKeys(leftRow, rightRow, sortingSpec);
      });
}

vector_size_t SortedAggregations::extractSingleGroup(
    std::vector<char*>& groupRows,
    const AggregateInfo& aggregate,
    std::vector<VectorPtr>& inputVectors) {
  const auto numGroupRows = groupRows.size();

  std::vector<vector_size_t> rowNumbers;
  if (aggregate.mask) {
    FlatVectorPtr<bool> mask = BaseVector::create<FlatVector<bool>>(
        BOOLEAN(), numGroupRows, inputData_->pool());
    inputData_->extractColumn(
        groupRows.data(),
        numGroupRows,
        inputMapping_[aggregate.mask.value()],
        mask);

    rowNumbers.reserve(numGroupRows);
    for (auto i = 0; i < numGroupRows; ++i) {
      if (!mask->isNullAt(i) && mask->valueAt(i)) {
        rowNumbers.push_back(i);
      }
    }

    if (rowNumbers.empty()) {
      return 0;
    }
  }

  const auto numInputs = aggregate.inputs.size();
  VELOX_CHECK_EQ(numInputs, inputVectors.size());

  const auto numRows = aggregate.mask ? rowNumbers.size() : numGroupRows;

  for (auto i = 0; i < numInputs; ++i) {
    if (aggregate.inputs[i] == kConstantChannel) {
      inputVectors[i] = aggregate.constantInputs[i];
    } else {
      const auto columnIndex = inputMapping_[aggregate.inputs[i]];
      if (inputVectors[i] == nullptr) {
        inputVectors[i] = BaseVector::create(
            inputData_->keyTypes()[columnIndex], numRows, inputData_->pool());
      } else {
        BaseVector::prepareForReuse(inputVectors[i], numRows);
      }

      if (aggregate.mask) {
        inputData_->extractColumn(
            groupRows.data(),
            folly::Range(rowNumbers.data(), rowNumbers.size()),
            columnIndex,
            0, // resultOffset
            inputVectors[i]);
      } else {
        inputData_->extractColumn(
            groupRows.data(), numRows, columnIndex, inputVectors[i]);
      }
    }
  }

  return numRows;
}

void SortedAggregations::extractValues(
    folly::Range<char**> groups,
    const RowVectorPtr& result) {
  raw_vector<int32_t> indices(pool_);
  SelectivityVector rows;
  std::vector<char*> groupRows;
  for (const auto& [sortingSpec, aggregates] : aggregates_) {
    std::vector<VectorPtr> inputVectors;
    size_t numInputColumns = 0;
    for (const auto& aggregate : aggregates) {
      numInputColumns += aggregate->inputs.size();
    }
    inputVectors.resize(numInputColumns);

    // For each group, sort inputs, add them to aggregate.
    for (auto* group : groups) {
      auto* accumulator = reinterpret_cast<RowPointers*>(group + offset_);
      if (accumulator->size == 0) {
        continue;
      }

      groupRows.resize(accumulator->size);
      accumulator->read(folly::Range(groupRows.data(), groupRows.size()));

      sortSingleGroup(groupRows, sortingSpec);

      size_t firstInputColumn = 0;
      for (const auto& aggregate : aggregates) {
        std::vector<VectorPtr> aggregateInputs;
        aggregateInputs.reserve(aggregate->inputs.size());
        for (auto i = 0; i < aggregate->inputs.size(); ++i) {
          aggregateInputs.push_back(
              std::move(inputVectors[firstInputColumn + i]));
        }

        // TODO Process group rows in batches to avoid creating very large input
        // vectors.
        const auto numRows =
            extractSingleGroup(groupRows, *aggregate, aggregateInputs);
        if (numRows == 0) {
          firstInputColumn += aggregateInputs.size();
          // Mask must be false for all 'groupRows'.
          continue;
        }

        rows.resize(numRows);
        aggregate->function->addSingleGroupRawInput(
            group, rows, aggregateInputs, false);

        for (auto i = 0; i < aggregate->inputs.size(); ++i) {
          inputVectors[firstInputColumn + i] = std::move(aggregateInputs[i]);
        }

        firstInputColumn += aggregateInputs.size();
      }
    }

    for (const auto& aggregate : aggregates) {
      aggregate->function->extractValues(
          groups.data(), groups.size(), &result->childAt(aggregate->output));

      // Release memory back to HashStringAllocator to allow next aggregate to
      // re-use it.
      aggregate->function->destroy(groups);
      // Overwrite empty groups over the destructed groups to keep the container
      // in a well formed state.
      aggregate->function->initializeNewGroups(
          groups.data(),
          folly::Range<const int32_t*>(
              iota(groups.size(), indices), groups.size()));
    }
  }
}
} // namespace facebook::velox::exec
