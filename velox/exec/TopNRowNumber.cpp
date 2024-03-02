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
#include "velox/exec/OperatorUtils.h"

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

std::vector<CompareFlags> makeSpillCompareFlags(
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

TopNRowNumber::TopNRowNumber(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::TopNRowNumberNode>& node)
    : Operator(
          driverCtx,
          node->outputType(),
          operatorId,
          node->id(),
          "TopNRowNumber",
          node->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      limit_{node->limit()},
      generateRowNumber_{node->generateRowNumber()},
      numPartitionKeys_{node->partitionKeys().size()},
      inputChannels_{reorderInputChannels(
          node->inputType(),
          node->partitionKeys(),
          node->sortingKeys())},
      inputType_{reorderInputType(node->inputType(), inputChannels_)},
      spillCompareFlags_{
          makeSpillCompareFlags(numPartitionKeys_, node->sortingOrders())},
      abandonPartialMinRows_(
          driverCtx->queryConfig().abandonPartialTopNRowNumberMinRows()),
      abandonPartialMinPct_(
          driverCtx->queryConfig().abandonPartialTopNRowNumberMinPct()),
      data_(std::make_unique<RowContainer>(
          slice(inputType_->children(), 0, spillCompareFlags_.size()),
          slice(
              inputType_->children(),
              spillCompareFlags_.size(),
              inputType_->size()),
          pool())),
      comparator_(
          inputType_,
          node->sortingKeys(),
          node->sortingOrders(),
          data_.get()),
      decodedVectors_(inputType_->size()) {
  const auto& keys = node->partitionKeys();
  const auto numKeys = keys.size();

  if (numKeys > 0) {
    Accumulator accumulator{
        true,
        sizeof(TopRows),
        false,
        1,
        nullptr,
        [](auto, auto) { VELOX_UNREACHABLE(); },
        [](auto) {}};

    table_ = std::make_unique<HashTable<false>>(
        createVectorHashers(node->inputType(), keys),
        std::vector<Accumulator>{accumulator},
        std::vector<TypePtr>{},
        false, // allowDuplicates
        false, // isJoinBuild
        false, // hasProbedFlag
        0, // minTableSizeForParallelJoinBuild
        pool());
    partitionOffset_ = table_->rows()->columnAt(numKeys).offset();
    lookup_ = std::make_unique<HashLookup>(table_->hashers());
  } else {
    allocator_ = std::make_unique<HashStringAllocator>(pool());
    singlePartition_ = std::make_unique<TopRows>(allocator_.get(), comparator_);
  }

  if (generateRowNumber_) {
    results_.resize(1);
  }
}

void TopNRowNumber::addInput(RowVectorPtr input) {
  if (abandonedPartial_) {
    input_ = std::move(input);
    return;
  }

  const auto numInput = input->size();

  for (auto i = 0; i < inputChannels_.size(); ++i) {
    decodedVectors_[i].decode(*input->childAt(inputChannels_[i]));
  }

  if (table_) {
    ensureInputFits(input);

    SelectivityVector rows(numInput);
    table_->prepareForGroupProbe(
        *lookup_,
        input,
        rows,
        false,
        BaseHashTable::kNoSpillInputStartPartitionBit);
    table_->groupProbe(*lookup_);

    // Initialize new partitions.
    initializeNewPartitions();

    // Process input rows. For each row, lookup the partition. If number of rows
    // in that partition is less than limit, add the new row. Otherwise, check
    // if row should replace an existing row or be discarded.
    for (auto i = 0; i < numInput; ++i) {
      auto& partition = partitionAt(lookup_->hits[i]);
      processInputRow(i, partition);
    }

    if (abandonPartialEarly()) {
      abandonedPartial_ = true;
      addRuntimeStat("abandonedPartial", RuntimeCounter(1));

      updateEstimatedOutputRowSize();
      outputBatchSize_ = outputBatchRows(estimatedOutputRowSize_);
      outputRows_.resize(outputBatchSize_);
    }
  } else {
    for (auto i = 0; i < numInput; ++i) {
      processInputRow(i, *singlePartition_);
    }
  }
}

bool TopNRowNumber::abandonPartialEarly() const {
  if (table_ == nullptr || generateRowNumber_ || spiller_ != nullptr) {
    return false;
  }

  const auto numInput = stats_.rlock()->inputPositions;
  if (numInput < abandonPartialMinRows_) {
    return false;
  }

  const auto numOutput = data_->numRows();
  return (100 * numOutput / numInput) >= abandonPartialMinPct_;
}

void TopNRowNumber::initializeNewPartitions() {
  for (auto index : lookup_->newGroups) {
    new (lookup_->hits[index] + partitionOffset_)
        TopRows(table_->stringAllocator(), comparator_);
  }
}

void TopNRowNumber::processInputRow(vector_size_t index, TopRows& partition) {
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

  for (auto col = 0; col < decodedVectors_.size(); ++col) {
    data_->store(decodedVectors_[col], index, newRow, col);
  }

  topRows.push(newRow);
}

void TopNRowNumber::noMoreInput() {
  Operator::noMoreInput();

  updateEstimatedOutputRowSize();
  outputBatchSize_ = outputBatchRows(estimatedOutputRowSize_);

  if (spiller_ != nullptr) {
    // Spill remaining data to avoid running out of memory while sort-merging
    // spilled data.
    spill();

    VELOX_CHECK_NULL(merge_);
    auto spillPartition = spiller_->finishSpill();
    merge_ = spillPartition.createOrderedReader(pool());
    recordSpillStats(spiller_->stats());
  } else {
    outputRows_.resize(outputBatchSize_);
  }
}

void TopNRowNumber::updateEstimatedOutputRowSize() {
  const auto optionalRowSize = data_->estimateRowSize();
  if (!optionalRowSize.has_value()) {
    return;
  }

  auto rowSize = optionalRowSize.value();

  if (rowSize && generateRowNumber_) {
    rowSize += sizeof(int64_t);
  }

  if (!estimatedOutputRowSize_.has_value()) {
    estimatedOutputRowSize_ = rowSize;
  } else if (rowSize > estimatedOutputRowSize_.value()) {
    estimatedOutputRowSize_ = rowSize;
  }
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
  if (finished_) {
    return nullptr;
  }

  if (abandonedPartial_) {
    if (input_ != nullptr) {
      auto output = std::move(input_);
      input_.reset();
      return output;
    }

    // We may have input accumulated in 'data_'.
    if (data_->numRows() > 0) {
      return getOutputFromMemory();
    }

    if (noMoreInput_) {
      finished_ = true;
    }

    return nullptr;
  }

  if (!noMoreInput_) {
    return nullptr;
  }

  RowVectorPtr output;
  if (merge_ != nullptr) {
    output = getOutputFromSpill();
  } else {
    output = getOutputFromMemory();
  }

  if (output == nullptr) {
    finished_ = true;
  }

  return output;
}

RowVectorPtr TopNRowNumber::getOutputFromMemory() {
  VELOX_CHECK_GT(outputBatchSize_, 0);

  // Loop over partitions and emit sorted rows along with row numbers.
  auto output =
      BaseVector::create<RowVector>(outputType_, outputBatchSize_, pool());
  FlatVector<int64_t>* rowNumbers = nullptr;
  if (generateRowNumber_) {
    rowNumbers = output->children().back()->as<FlatVector<int64_t>>();
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
    data_->clear();
    if (table_ != nullptr) {
      table_->clear();
    }
    pool()->release();
    return nullptr;
  }

  if (rowNumbers) {
    rowNumbers->resize(offset);
  }
  output->resize(offset);

  for (int i = 0; i < inputChannels_.size(); ++i) {
    data_->extractColumn(
        outputRows_.data(), offset, i, output->childAt(inputChannels_[i]));
  }

  return output;
}

bool TopNRowNumber::isNewPartition(
    const RowVectorPtr& output,
    vector_size_t index,
    SpillMergeStream* next) {
  VELOX_CHECK_GT(index, 0);

  for (auto i = 0; i < numPartitionKeys_; ++i) {
    if (!output->childAt(inputChannels_[i])
             ->equalValueAt(
                 next->current().childAt(i).get(),
                 index - 1,
                 next->currentIndex())) {
      return true;
    }
  }
  return false;
}

void TopNRowNumber::setupNextOutput(
    const RowVectorPtr& output,
    int32_t rowNumber) {
  nextRowNumber_ = rowNumber;

  auto lookAhead = merge_->next();
  if (lookAhead == nullptr) {
    nextRowNumber_ = 0;
    return;
  }

  if (isNewPartition(output, output->size(), lookAhead)) {
    nextRowNumber_ = 0;
    return;
  }

  if (nextRowNumber_ < limit_) {
    return;
  }

  // Skip remaining rows for this partition.
  lookAhead->pop();

  while (auto next = merge_->next()) {
    if (isNewPartition(output, output->size(), next)) {
      nextRowNumber_ = 0;
      return;
    }
    next->pop();
  }

  // This partition is the last partition.
  nextRowNumber_ = 0;
}

RowVectorPtr TopNRowNumber::getOutputFromSpill() {
  VELOX_CHECK_NOT_NULL(merge_);

  // merge_->next() produces data sorted by partition keys, then sorting keys.
  // All rows from the same partition will appear together.
  // We'll identify partition boundaries by comparing partition keys of the
  // current row with the previous row. When new partition starts, we'll reset
  // row number to zero. Once row number reaches the 'limit_', we'll start
  // dropping rows until the next partition starts.
  // We'll emit output every time we accumulate 'outputBatchSize_' rows.

  auto output =
      BaseVector::create<RowVector>(outputType_, outputBatchSize_, pool());
  FlatVector<int64_t>* rowNumbers = nullptr;
  if (generateRowNumber_) {
    rowNumbers = output->children().back()->as<FlatVector<int64_t>>();
  }

  // Index of the next row to append to output.
  vector_size_t index = 0;

  // Row number of the next row in the current partition.
  vector_size_t rowNumber = nextRowNumber_;
  VELOX_CHECK_LT(rowNumber, limit_);
  for (;;) {
    auto next = merge_->next();
    if (next == nullptr) {
      break;
    }

    // Check if this row comes from a new partition.
    if (index > 0 && isNewPartition(output, index, next)) {
      rowNumber = 0;
    }

    if (rowNumber < limit_) {
      for (auto i = 0; i < inputChannels_.size(); ++i) {
        output->childAt(inputChannels_[i])
            ->copy(
                next->current().childAt(i).get(),
                index,
                next->currentIndex(),
                1);
      }
      if (rowNumbers) {
        // Row numbers start with 1.
        rowNumbers->set(index, rowNumber + 1);
      }
      ++index;
    } else {
      // Drop the row.
    }

    ++rowNumber;
    next->pop();

    if (index == outputBatchSize_) {
      // Check if next row is from a new partition. Reset 'nextRowNumber_' if
      // so. Check if next row is from the current partition, but we have
      // reached the 'limit_'. Skip to the start of the next partition if so.
      setupNextOutput(output, rowNumber);

      return output;
    }
  }

  if (index > 0) {
    output->resize(index);
  } else {
    output = nullptr;
  }

  finished_ = true;
  return output;
}

bool TopNRowNumber::isFinished() {
  return finished_;
}

void TopNRowNumber::close() {
  Operator::close();

  if (table_) {
    partitionIt_.reset();
    partitions_.resize(1000);
    while (auto numPartitions = table_->listAllRows(
               &partitionIt_,
               partitions_.size(),
               RowContainer::kUnlimited,
               partitions_.data())) {
      for (auto i = 0; i < numPartitions; ++i) {
        std::destroy_at(
            reinterpret_cast<TopRows*>(partitions_[i] + partitionOffset_));
      }
    }
  }
}

void TopNRowNumber::reclaim(
    uint64_t /*targetBytes*/,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK(canReclaim());
  VELOX_CHECK(!nonReclaimableSection_);

  if (data_->numRows() == 0) {
    // Nothing to spill.
    return;
  }

  if (noMoreInput_) {
    ++stats.numNonReclaimableAttempts;
    // TODO Add support for spilling after noMoreInput().
    LOG(WARNING)
        << "Can't reclaim from topNRowNumber operator which has started producing output: "
        << pool()->name()
        << ", usage: " << succinctBytes(pool()->currentBytes())
        << ", reservation: " << succinctBytes(pool()->reservedBytes());
    return;
  }

  if (abandonedPartial_) {
    return;
  }

  spill();
}

void TopNRowNumber::ensureInputFits(const RowVectorPtr& input) {
  if (!spillEnabled()) {
    // Spilling is disabled.
    return;
  }

  if (data_->numRows() == 0) {
    // Nothing to spill.
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill()) {
    spill();
    return;
  }

  auto [freeRows, outOfLineFreeBytes] = data_->freeSpace();
  const auto outOfLineBytes =
      data_->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const auto outOfLineBytesPerRow = outOfLineBytes / data_->numRows();

  const auto currentUsage = pool()->currentBytes();
  const auto minReservationBytes =
      currentUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = pool()->availableReservation();
  const auto tableIncrementBytes = table_->hashTableSizeIncrease(input->size());
  const auto incrementBytes =
      data_->sizeIncrement(
          input->size(), outOfLineBytesPerRow * input->size()) +
      tableIncrementBytes;

  // First to check if we have sufficient minimal memory reservation.
  if (availableReservationBytes >= minReservationBytes) {
    if ((tableIncrementBytes == 0) && (freeRows > input->size()) &&
        (outOfLineBytes == 0 ||
         outOfLineFreeBytes >= outOfLineBytesPerRow * input->size())) {
      // Enough free rows for input rows and enough variable length free space.
      return;
    }
  }

  // Check if we can increase reservation. The increment is the largest of twice
  // the maximum increment from this input and 'spillableReservationGrowthPct_'
  // of the current memory usage.
  const auto targetIncrementBytes = std::max<int64_t>(
      incrementBytes * 2,
      currentUsage * spillConfig_->spillableReservationGrowthPct / 100);
  {
    ReclaimableSectionGuard guard(this);
    if (pool()->maybeReserve(targetIncrementBytes)) {
      return;
    }
  }

  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << pool()->name()
               << ", usage: " << succinctBytes(pool()->currentBytes())
               << ", reservation: " << succinctBytes(pool()->reservedBytes());
}

void TopNRowNumber::spill() {
  if (spiller_ == nullptr) {
    setupSpiller();
  }

  updateEstimatedOutputRowSize();

  spiller_->spill();
  table_->clear();
  data_->clear();
  pool()->release();
}

void TopNRowNumber::setupSpiller() {
  VELOX_CHECK_NULL(spiller_);
  VELOX_CHECK(spillConfig_.has_value());

  spiller_ = std::make_unique<Spiller>(
      // TODO Replace Spiller::Type::kOrderBy.
      Spiller::Type::kOrderByInput,
      data_.get(),
      inputType_,
      spillCompareFlags_.size(),
      spillCompareFlags_,
      &spillConfig_.value());
}
} // namespace facebook::velox::exec
