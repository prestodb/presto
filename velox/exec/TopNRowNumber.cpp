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
#include "velox/exec/TopNRowNumber.h"

namespace facebook::velox::exec {

TopNRowNumber::TopNRowNumber(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::TopNRowNumberNode>& node)
    : Operator(
          driverCtx,
          node->outputType(),
          operatorId,
          node->id(),
          "TopNRowNumber"),
      limit_{node->limit()},
      generateRowNumber_{node->generateRowNumber()},
      inputType_{node->sources()[0]->outputType()},
      data_(std::make_unique<RowContainer>(inputType_->children(), pool())),
      comparator_(
          inputType_,
          node->sortingKeys(),
          node->sortingOrders(),
          data_.get()),
      decodedVectors_(inputType_->children().size()) {
  const auto& keys = node->partitionKeys();
  const auto numKeys = keys.size();

  if (numKeys > 0) {
    Accumulator accumulator{true, sizeof(TopRows), false, 1, [](auto) {}};

    table_ = std::make_unique<HashTable<false>>(
        createVectorHashers(inputType_, keys),
        std::vector<Accumulator>{accumulator},
        std::vector<TypePtr>{},
        false, // allowDuplicates
        false, // isJoinBuild
        false, // hasProbedFlag
        pool());
    partitionOffset_ = table_->rows()->columnAt(numKeys).offset();
    lookup_ = std::make_unique<HashLookup>(table_->hashers());
  } else {
    allocator_ = std::make_unique<HashStringAllocator>(pool());
    singlePartition_ = std::make_unique<TopRows>(allocator_.get(), comparator_);
  }

  identityProjections_.reserve(inputType_->size());
  for (auto i = 0; i < inputType_->size(); ++i) {
    identityProjections_.emplace_back(i, i);
  }

  if (generateRowNumber_) {
    resultProjections_.emplace_back(0, inputType_->size());
    results_.resize(1);
  }
}

void TopNRowNumber::addInput(RowVectorPtr input) {
  const auto numInput = input->size();

  for (auto i = 0; i < inputType_->size(); ++i) {
    decodedVectors_[i].decode(*input->childAt(i));
  }

  if (table_) {
    SelectivityVector rows(numInput);
    table_->prepareForProbe(*lookup_, input, rows, false);
    table_->groupProbe(*lookup_);

    // Initialize new partitions.
    initializeNewPartitions();

    // Process input rows. For each row, lookup the partition. If number of rows
    // in that partition is less than limit, add the new row. Otherwise, check
    // if row should replace an existing row or be discarded.
    for (auto i = 0; i < numInput; ++i) {
      auto& partition = partitionAt(lookup_->hits[i]);
      processInputRow(input, i, partition);
    }
  } else {
    for (auto i = 0; i < numInput; ++i) {
      processInputRow(input, i, *singlePartition_);
    }
  }
}

void TopNRowNumber::initializeNewPartitions() {
  for (auto index : lookup_->newGroups) {
    new (lookup_->hits[index] + partitionOffset_)
        TopRows(table_->stringAllocator(), comparator_);
  }
}

void TopNRowNumber::processInputRow(
    const RowVectorPtr& input,
    vector_size_t index,
    TopRows& partition) {
  auto& topRows = partition.rows;

  char* newRow = nullptr;
  if (topRows.size() < limit_) {
    newRow = data_->newRow();
  } else {
    char* topRow = topRows.top();

    if (!comparator_(decodedVectors_, index, topRow)) {
      // Drop this input row.
      return;
    }

    // Replace existing row.
    topRows.pop();

    // Reuse the topRow's memory.
    newRow = data_->initializeRow(topRow, true /* reuse */);
  }

  for (auto col = 0; col < input->childrenSize(); ++col) {
    data_->store(decodedVectors_[col], index, newRow, col);
  }

  topRows.push(newRow);
}

void TopNRowNumber::noMoreInput() {
  Operator::noMoreInput();

  auto rowSize = data_->estimateRowSize();
  if (rowSize && generateRowNumber_) {
    rowSize.value() += sizeof(int64_t);
  }

  outputBatchSize_ = outputBatchRows(rowSize);
  outputRows_.resize(outputBatchSize_);
}

TopNRowNumber::TopRows* TopNRowNumber::nextPartition() {
  if (!table_) {
    if (!currentPartition_) {
      currentPartition_ = 0;
      return singlePartition_.get();
    }
    return nullptr;
  }

  if (!currentPartition_) {
    numPartitions_ = table_->listAllRows(
        &partitionIt_,
        partitions_.size(),
        RowContainer::kUnlimited,
        partitions_.data());
    if (numPartitions_ == 0) {
      // No more partitions.
      return nullptr;
    }

    currentPartition_ = 0;
  } else {
    ++currentPartition_.value();
    if (currentPartition_ >= numPartitions_) {
      currentPartition_.reset();
      return nextPartition();
    }
  }

  return &currentPartition();
}

TopNRowNumber::TopRows& TopNRowNumber::currentPartition() {
  VELOX_CHECK(currentPartition_.has_value());

  if (!table_) {
    return *singlePartition_;
  }

  return partitionAt(partitions_[currentPartition_.value()]);
}

void TopNRowNumber::appendPartitionRows(
    TopRows& partition,
    vector_size_t start,
    vector_size_t size,
    vector_size_t outputOffset,
    FlatVector<int64_t>* rowNumbers) {
  // Append 'size' partition rows in reverse order starting from 'start' row.
  auto rowNumber = partition.rows.size() - start;
  for (auto i = 0; i < size; ++i) {
    const auto index = outputOffset + size - i - 1;
    if (rowNumbers) {
      rowNumbers->set(index, rowNumber--);
    }
    outputRows_[index] = partition.rows.top();
    partition.rows.pop();
  }
}

RowVectorPtr TopNRowNumber::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }

  // Loop over partitions and emit sorted rows along with row numbers.
  auto output =
      BaseVector::create<RowVector>(outputType_, outputBatchSize_, pool());
  FlatVector<int64_t>* rowNumbers = nullptr;
  if (generateRowNumber_) {
    rowNumbers = output->children().back()->as<FlatVector<int64_t>>();
    rowNumbers->resize(outputBatchSize_);
  }

  vector_size_t offset = 0;
  if (remainingRowsInPartition_ > 0) {
    auto& partition = currentPartition();
    auto start = partition.rows.size() - remainingRowsInPartition_;
    auto numRows =
        std::min<vector_size_t>(outputBatchSize_, remainingRowsInPartition_);
    appendPartitionRows(partition, start, numRows, offset, rowNumbers);
    offset += numRows;
    remainingRowsInPartition_ -= numRows;
  }

  while (offset < outputBatchSize_) {
    auto* partition = nextPartition();
    if (!partition) {
      break;
    }

    auto numRows = partition->rows.size();
    if (offset + numRows > outputBatchSize_) {
      remainingRowsInPartition_ = offset + numRows - outputBatchSize_;

      // Add a subset of partition rows.
      numRows -= remainingRowsInPartition_;
      appendPartitionRows(*partition, 0, numRows, offset, rowNumbers);
      offset += numRows;
      break;
    }

    // Add all partition rows.
    appendPartitionRows(*partition, 0, numRows, offset, rowNumbers);
    offset += numRows;
    remainingRowsInPartition_ = 0;
  }

  if (offset == 0) {
    finished_ = true;
    return nullptr;
  }

  if (rowNumbers) {
    rowNumbers->resize(offset);
  }
  output->resize(offset);

  for (int i = 0; i < inputType_->size(); ++i) {
    data_->extractColumn(outputRows_.data(), offset, i, output->childAt(i));
  }

  return output;
}

bool TopNRowNumber::isFinished() {
  return finished_;
}

} // namespace facebook::velox::exec
