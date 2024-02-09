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
#include "velox/exec/RowNumber.h"
#include "velox/exec/OperatorUtils.h"

namespace facebook::velox::exec {

RowNumber::RowNumber(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::RowNumberNode>& rowNumberNode)
    : Operator(
          driverCtx,
          rowNumberNode->outputType(),
          operatorId,
          rowNumberNode->id(),
          "RowNumber",
          rowNumberNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt),
      limit_{rowNumberNode->limit()},
      generateRowNumber_{rowNumberNode->generateRowNumber()} {
  const auto& inputType = rowNumberNode->sources()[0]->outputType();
  const auto& keys = rowNumberNode->partitionKeys();
  const auto numKeys = keys.size();

  if (numKeys > 0) {
    table_ = std::make_unique<HashTable<false>>(
        createVectorHashers(inputType, keys),
        std::vector<Accumulator>{},
        std::vector<TypePtr>{BIGINT()},
        false, // allowDuplicates
        false, // isJoinBuild
        false, // hasProbedFlag
        0, // minTableSizeForParallelJoinBuild
        pool());
    lookup_ = std::make_unique<HashLookup>(table_->hashers());

    const auto numRowsColumn = table_->rows()->columnAt(numKeys);
    numRowsOffset_ = numRowsColumn.offset();

    inputType_ = rowNumberNode->sources()[0]->outputType();
  }

  identityProjections_.reserve(inputType->size());
  for (auto i = 0; i < inputType->size(); ++i) {
    identityProjections_.emplace_back(i, i);
  }

  if (generateRowNumber_) {
    resultProjections_.emplace_back(0, inputType->size());
    results_.resize(1);
  }
}

void RowNumber::addInput(RowVectorPtr input) {
  const auto numInput = input->size();

  if (table_) {
    ensureInputFits(input);

    if (inputSpiller_ != nullptr) {
      spillInput(input, pool());
      return;
    }

    SelectivityVector rows(numInput);
    table_->prepareForGroupProbe(
        *lookup_,
        input,
        rows,
        false,
        BaseHashTable::kNoSpillInputStartPartitionBit);
    table_->groupProbe(*lookup_);

    // Initialize new partitions with zeros.
    for (auto i : lookup_->newGroups) {
      setNumRows(lookup_->hits[i], 0);
    }
  }

  input_ = std::move(input);
}

void RowNumber::addSpillInput() {
  const auto numInput = input_->size();
  SelectivityVector rows(numInput);
  table_->prepareForGroupProbe(
      *lookup_, input_, rows, false, spillConfig_->startPartitionBit);
  table_->groupProbe(*lookup_);

  // Initialize new partitions with zeros.
  for (auto i : lookup_->newGroups) {
    setNumRows(lookup_->hits[i], 0);
  }

  // TODO Add support for recursive spilling.
}

void RowNumber::noMoreInput() {
  Operator::noMoreInput();

  if (inputSpiller_ != nullptr) {
    inputSpiller_->finishSpill(spillInputPartitionSet_);

    recordSpillStats(hashTableSpiller_->stats());
    recordSpillStats(inputSpiller_->stats());

    // Remove empty partitions.
    auto it = spillInputPartitionSet_.begin();
    while (it != spillInputPartitionSet_.end()) {
      if (it->second->numFiles() > 0) {
        ++it;
      } else {
        it = spillInputPartitionSet_.erase(it);
      }
    }

    restoreNextSpillPartition();
  }
}

void RowNumber::restoreNextSpillPartition() {
  if (spillInputPartitionSet_.empty()) {
    return;
  }

  auto it = spillInputPartitionSet_.begin();
  spillInputReader_ = it->second->createUnorderedReader(pool());

  // Find matching partition for the hash table.
  auto hashTableIt = spillHashTablePartitionSet_.find(it->first);
  if (hashTableIt != spillHashTablePartitionSet_.end()) {
    spillHashTableReader_ = hashTableIt->second->createUnorderedReader(pool());

    RowVectorPtr data;
    while (spillHashTableReader_->nextBatch(data)) {
      // 'data' contains partition-by keys and count. Transform 'data' to match
      // 'inputType_' so it can be added to the 'table_'. Move partition-by
      // columns and leave other columns unset.
      std::vector<VectorPtr> columns(inputType_->size());

      const auto& hashers = table_->hashers();
      for (auto i = 0; i < hashers.size(); ++i) {
        columns[hashers[i]->channel()] = data->childAt(i);
      }

      auto input = std::make_shared<RowVector>(
          pool(), inputType_, nullptr, data->size(), std::move(columns));

      const auto numInput = input->size();
      SelectivityVector rows(numInput);
      table_->prepareForGroupProbe(
          *lookup_, input, rows, false, spillConfig_->startPartitionBit);
      table_->groupProbe(*lookup_);

      auto* counts = data->children().back()->as<FlatVector<int64_t>>();

      for (auto i = 0; i < numInput; ++i) {
        auto* partition = lookup_->hits[i];
        setNumRows(partition, counts->valueAt(i));
      }
    }
  }

  spillInputPartitionSet_.erase(it);

  spillInputReader_->nextBatch(input_);
  addSpillInput();
}

void RowNumber::ensureInputFits(const RowVectorPtr& input) {
  if (!spillEnabled()) {
    // Spilling is disabled.
    return;
  }

  if (table_ == nullptr) {
    // No hash table. Nothing to spill.
    return;
  }

  const auto numDistinct = table_->numDistinct();
  if (numDistinct == 0) {
    // Table is empty. Nothing to spill.
    return;
  }

  auto* rows = table_->rows();
  auto [freeRows, outOfLineFreeBytes] = rows->freeSpace();
  const auto outOfLineBytes =
      rows->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const auto outOfLineBytesPerRow = outOfLineBytes / numDistinct;

  // Test-only spill path.
  if (spillConfig_->testSpillPct > 0) {
    spill();
    return;
  }

  const auto currentUsage = pool()->currentBytes();
  const auto minReservationBytes =
      currentUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = pool()->availableReservation();
  const auto tableIncrementBytes = table_->hashTableSizeIncrease(input->size());
  const auto incrementBytes =
      rows->sizeIncrement(input->size(), outOfLineBytesPerRow * input->size()) +
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
    Operator::ReclaimableSectionGuard guard(this);
    if (pool()->maybeReserve(targetIncrementBytes)) {
      return;
    }
  }

  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << pool()->name()
               << ", usage: " << succinctBytes(pool()->currentBytes())
               << ", reservation: " << succinctBytes(pool()->reservedBytes());
}

FlatVector<int64_t>& RowNumber::getOrCreateRowNumberVector(vector_size_t size) {
  VectorPtr& result = results_[0];
  if (result && result.unique()) {
    BaseVector::prepareForReuse(result, size);
  } else {
    result = BaseVector::create(BIGINT(), size, pool());
  }
  return *result->as<FlatVector<int64_t>>();
}

RowVectorPtr RowNumber::getOutput() {
  if (input_ == nullptr) {
    return nullptr;
  }

  if (!table_) {
    // No partition keys.
    return getOutputForSinglePartition();
  }

  const auto numInput = input_->size();

  BufferPtr mapping;
  vector_size_t* rawMapping;
  vector_size_t index = 0;
  if (limit_) {
    mapping = allocateIndices(numInput, pool());
    rawMapping = mapping->asMutable<vector_size_t>();
  }

  // Compute row numbers if needed.
  FlatVector<int64_t>* rowNumbers = nullptr;
  if (generateRowNumber_) {
    rowNumbers = &getOrCreateRowNumberVector(numInput);
  }

  for (auto i = 0; i < numInput; ++i) {
    auto* partition = lookup_->hits[i];
    const auto rowNumber = numRows(partition) + 1;

    if (limit_) {
      if (rowNumber > limit_) {
        // Exceeded the limit for this partition. Drop rows.
        continue;
      }
      rawMapping[index++] = i;
    }

    if (generateRowNumber_) {
      rowNumbers->set(i, rowNumber);
    }
    setNumRows(partition, rowNumber);
  }

  RowVectorPtr output;
  if (limit_) {
    if (index == 0) {
      // Drop all rows.
      output = nullptr;
    } else {
      output = fillOutput(index, mapping);
    }
  } else {
    output = fillOutput(numInput, nullptr);
  }

  if (spillInputReader_ != nullptr) {
    if (spillInputReader_->nextBatch(input_)) {
      addSpillInput();
    } else {
      input_ = nullptr;
      spillInputReader_ = nullptr;
      table_->clear();
      restoreNextSpillPartition();
    }
  } else {
    input_ = nullptr;
  }

  return output;
}

RowVectorPtr RowNumber::getOutputForSinglePartition() {
  const auto numInput = input_->size();

  vector_size_t numOutput;
  if (limit_) {
    VELOX_CHECK_LT(numTotalInput_, limit_.value());
    numOutput =
        std::min<vector_size_t>(numInput, limit_.value() - numTotalInput_);

    if (numTotalInput_ + numOutput == limit_.value()) {
      finishedEarly_ = true;
    }
  } else {
    numOutput = numInput;
  }

  if (generateRowNumber_) {
    auto& rowNumbers = getOrCreateRowNumberVector(numOutput);
    for (auto i = 0; i < numOutput; ++i) {
      rowNumbers.set(i, ++numTotalInput_);
    }
  }

  auto output = fillOutput(numOutput, nullptr);
  input_ = nullptr;
  return output;
}

int64_t RowNumber::numRows(char* partition) {
  return *reinterpret_cast<int64_t*>(partition + numRowsOffset_);
}

void RowNumber::setNumRows(char* partition, int64_t numRows) {
  *reinterpret_cast<int64_t*>(partition + numRowsOffset_) = numRows;
}

void RowNumber::reclaim(
    uint64_t /*targetBytes*/,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK(canReclaim());
  VELOX_CHECK(!nonReclaimableSection_);

  if (table_ == nullptr || table_->numDistinct() == 0) {
    // Nothing to spill.
    return;
  }

  if (hashTableSpiller_) {
    // Already spilled.
    return;
  }

  spill();
}

void RowNumber::setupHashTableSpiller() {
  // TODO Replace joinPartitionBits and Spiller::Type::kHashJoinBuild.

  const auto& spillConfig = spillConfig_.value();
  HashBitRange hashBits(
      spillConfig.startPartitionBit,
      spillConfig.startPartitionBit + spillConfig.joinPartitionBits);

  auto columnTypes = table_->rows()->columnTypes();
  auto tableType = ROW(std::move(columnTypes));

  hashTableSpiller_ = std::make_unique<Spiller>(
      Spiller::Type::kHashJoinBuild,
      table_->rows(),
      tableType,
      std::move(hashBits),
      &spillConfig,
      spillConfig.maxFileSize);
}

void RowNumber::setupInputSpiller() {
  const auto& spillConfig = spillConfig_.value();
  const auto& hashBits = hashTableSpiller_->hashBits();

  // TODO Replace Spiller::Type::kHashJoinProbe.
  inputSpiller_ = std::make_unique<Spiller>(
      Spiller::Type::kHashJoinProbe,
      inputType_,
      hashBits,
      &spillConfig,
      spillConfig.maxFileSize);

  const auto& hashers = table_->hashers();

  std::vector<column_index_t> keyChannels;
  keyChannels.reserve(hashers.size());
  for (const auto& hasher : hashers) {
    keyChannels.push_back(hasher->channel());
  }

  spillHashFunction_ = std::make_unique<HashPartitionFunction>(
      inputSpiller_->hashBits(), inputType_, keyChannels);
}

void RowNumber::spill() {
  VELOX_CHECK(spillEnabled());
  VELOX_CHECK_NULL(hashTableSpiller_);
  VELOX_CHECK_NULL(inputSpiller_);

  setupHashTableSpiller();
  setupInputSpiller();

  hashTableSpiller_->spill();
  hashTableSpiller_->finishSpill(spillHashTablePartitionSet_);

  table_->clear();
  pool()->release();

  inputSpiller_->setPartitionsSpilled(
      hashTableSpiller_->state().spilledPartitionSet());

  if (input_ != nullptr) {
    spillInput(input_, memory::spillMemoryPool());
    input_ = nullptr;
  }
}

void RowNumber::spillInput(
    const RowVectorPtr& input,
    memory::MemoryPool* pool) {
  const auto numInput = input->size();

  std::vector<uint32_t> spillPartitions(numInput);
  const auto singlePartition =
      spillHashFunction_->partition(*input, spillPartitions);

  const auto numPartitions = spillHashFunction_->numPartitions();

  std::vector<BufferPtr> partitionIndices(numPartitions);
  std::vector<vector_size_t*> rawPartitionIndices(numPartitions);

  for (auto i = 0; i < numPartitions; ++i) {
    partitionIndices[i] = allocateIndices(numInput, pool);
    rawPartitionIndices[i] = partitionIndices[i]->asMutable<vector_size_t>();
  }

  std::vector<vector_size_t> numSpillInputs(numPartitions, 0);

  for (auto row = 0; row < numInput; ++row) {
    const auto partition = singlePartition.has_value() ? singlePartition.value()
                                                       : spillPartitions[row];
    rawPartitionIndices[partition][numSpillInputs[partition]++] = row;
  }

  // Ensure vector are lazy loaded before spilling.
  for (auto i = 0; i < input->childrenSize(); ++i) {
    input->childAt(i)->loadedVector();
  }

  for (int32_t partition = 0; partition < numSpillInputs.size(); ++partition) {
    const auto numInputs = numSpillInputs[partition];
    if (numInputs == 0) {
      continue;
    }

    inputSpiller_->spill(
        partition, wrap(numInputs, partitionIndices[partition], input));
  }
}

} // namespace facebook::velox::exec
