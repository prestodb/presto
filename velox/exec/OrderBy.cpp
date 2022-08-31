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
#include "velox/exec/OrderBy.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

namespace {
CompareFlags fromSortOrderToCompareFlags(const core::SortOrder& sortOrder) {
  return {sortOrder.isNullsFirst(), sortOrder.isAscending(), false, false};
}
} // namespace

OrderBy::OrderBy(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::OrderByNode>& orderByNode)
    : Operator(
          driverCtx,
          orderByNode->outputType(),
          operatorId,
          orderByNode->id(),
          "OrderBy"),
      mappedMemory_(operatorCtx_->mappedMemory()),
      numSortKeys_(orderByNode->sortingKeys().size()),
      spillPath_(makeSpillPath(operatorId)),
      spillExecutor_(operatorCtx_->task()->queryCtx()->spillExecutor()),
      testSpillPct_(
          operatorCtx_->execCtx()->queryCtx()->config().testingSpillPct()),
      spillableReservationGrowthPct_(operatorCtx_->driverCtx()
                                         ->queryConfig()
                                         .spillableReservationGrowthPct()),
      spillFileSizeFactor_(
          operatorCtx_->driverCtx()->queryConfig().spillFileSizeFactor()) {
  std::vector<TypePtr> keyTypes;
  std::vector<TypePtr> dependentTypes;
  std::vector<TypePtr> types;
  std::vector<std::string> names;
  // Setup column projections to store sort key columns in row container first.
  // This enables to use the sorting facility provided by the row container. It
  // also facilitates the sort merge read handling required by disk spilling.
  std::unordered_set<column_index_t> keyChannelSet;
  columnMap_.reserve(numSortKeys_);
  keyCompareFlags_.reserve(numSortKeys_);
  for (int i = 0; i < numSortKeys_; ++i) {
    const auto channel =
        exprToChannel(orderByNode->sortingKeys()[i].get(), outputType_);
    VELOX_CHECK(
        channel != kConstantChannel,
        "OrderBy doesn't allow constant sorting keys");
    columnMap_.emplace_back(i, channel);
    keyTypes.push_back(outputType_->childAt(channel));
    types.push_back(keyTypes.back());
    names.push_back(outputType_->nameOf(channel));
    keyCompareFlags_.push_back(
        fromSortOrderToCompareFlags(orderByNode->sortingOrders()[i]));
    keyChannelSet.emplace(channel);
  }

  // Store non-sort key columns as dependents in row container.
  for (column_index_t outputChannel = 0, nextInputChannel = numSortKeys_;
       outputChannel < outputType_->size();
       ++outputChannel) {
    if (keyChannelSet.count(outputChannel) != 0) {
      continue;
    }
    columnMap_.emplace_back(nextInputChannel++, outputChannel);
    dependentTypes.push_back(outputType_->childAt(outputChannel));
    types.push_back(dependentTypes.back());
    names.push_back(outputType_->nameOf(outputChannel));
  }

  // Create row container.
  data_ = std::make_unique<RowContainer>(
      keyTypes, dependentTypes, operatorCtx_->mappedMemory());
  internalStoreType_ = ROW(std::move(names), std::move(types));
#ifndef NDEBUG
  for (int i = 0; i < internalStoreType_->children().size(); ++i) {
    VELOX_DCHECK_EQ(internalStoreType_->childAt(i), data_->columnTypes()[i]);
  }
#endif

  outputBatchSize_ = std::max<uint32_t>(
      operatorCtx_->execCtx()->queryCtx()->config().preferredOutputBatchSize(),
      data_->estimatedNumRowsPerBatch(kBatchSizeInBytes));
}

void OrderBy::addInput(RowVectorPtr input) {
  ensureInputFits(input);

  SelectivityVector allRows(input->size());
  std::vector<char*> rows(input->size());
  for (int row = 0; row < input->size(); ++row) {
    rows[row] = data_->newRow();
  }
  for (const auto& columnProjection : columnMap_) {
    DecodedVector decoded(
        *input->childAt(columnProjection.outputChannel), allRows);
    for (int i = 0; i < input->size(); ++i) {
      data_->store(decoded, i, rows[i], columnProjection.inputChannel);
    }
  }

  numRows_ += allRows.size();
  if (spiller_ != nullptr) {
    const auto stats = spiller_->stats();
    stats_.spilledBytes = stats.spilledBytes;
    stats_.spilledRows = stats.spilledRows;
    stats_.spilledPartitions = stats.spilledPartitions;
    VELOX_DCHECK_LE(stats_.spilledPartitions, 1);
  }
}

void OrderBy::ensureInputFits(const RowVectorPtr& input) {
  // Check if spilling is enabled or not.
  if (!spillPath_.has_value()) {
    return;
  }

  const int64_t numRows = data_->numRows();
  if (numRows == 0) {
    // 'data_' is empty. Nothing to spill.
    return;
  }
  auto [freeRows, outOfLineFreeBytes] = data_->freeSpace();
  const auto outOfLineBytes =
      data_->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const int64_t outOfLineBytesPerRow = outOfLineBytes / numRows;
  const int64_t flatInputBytes = input->estimateFlatSize();

  // Test-only spill path.
  if (numRows > 0 && testSpillPct_ &&
      (folly::hasher<uint64_t>()(++spillTestCounter_)) % 100 <= testSpillPct_) {
    const int64_t rowsToSpill = std::max<int64_t>(1, numRows / 10);
    spill(
        numRows - rowsToSpill,
        outOfLineBytes - (rowsToSpill * outOfLineBytesPerRow));
    return;
  }

  if (freeRows > input->size() &&
      (outOfLineBytes == 0 || outOfLineFreeBytes >= flatInputBytes)) {
    // Enough free rows for input rows and enough variable length free
    // space for the flat size of the whole vector. If outOfLineBytes
    // is 0 there is no need for variable length space.
    return;
  }

  // If there is variable length data we take the flat size of the input as a
  // cap on the new variable length data needed.
  const int64_t incrementBytes =
      data_->sizeIncrement(input->size(), outOfLineBytes ? flatInputBytes : 0);

  auto tracker = mappedMemory_->tracker();
  VELOX_CHECK_NOT_NULL(tracker);
  // There must be at least 2x the increment in reservation.
  if (tracker->getAvailableReservation() > 2 * incrementBytes) {
    return;
  }

  // Check if can increase reservation. The increment is the larger of twice the
  // maximum increment from this input and 'spillableReservationGrowthPct_' of
  // the current reservation.
  const auto targetIncrementBytes = std::max<int64_t>(
      incrementBytes * 2,
      tracker->getCurrentUserBytes() * spillableReservationGrowthPct_ / 100);
  if (tracker->maybeReserve(targetIncrementBytes)) {
    return;
  }
  const int64_t rowsToSpill = std::max<int64_t>(
      1, targetIncrementBytes / (data_->fixedRowSize() + outOfLineBytesPerRow));
  spill(
      std::max<int64_t>(0, numRows - rowsToSpill),
      std::max<int64_t>(
          0, outOfLineBytes - (rowsToSpill * outOfLineBytesPerRow)));
}

void OrderBy::spill(int64_t targetRows, int64_t targetBytes) {
  VELOX_CHECK_GE(targetRows, 0);
  VELOX_CHECK_GE(targetBytes, 0);

  if (spiller_ == nullptr) {
    assert(mappedMemory_->tracker()); // lint
    const auto spillFileSize =
        mappedMemory_->tracker()->getCurrentUserBytes() * spillFileSizeFactor_;
    spiller_ = std::make_unique<Spiller>(
        Spiller::Type::kOrderBy,
        *data_,
        [&](folly::Range<char**> rows) { data_->eraseRows(rows); },
        internalStoreType_,
        data_->keyTypes().size(),
        keyCompareFlags_,
        spillPath_.value(),
        spillFileSize,
        Spiller::spillPool(),
        spillExecutor_);
    VELOX_CHECK_EQ(spiller_->state().maxPartitions(), 1);
  }
  spiller_->spill(targetRows, targetBytes);
}

void OrderBy::noMoreInput() {
  Operator::noMoreInput();

  // No data.
  if (numRows_ == 0) {
    finished_ = true;
    return;
  }

  if (spiller_ == nullptr) {
    VELOX_CHECK_EQ(numRows_, data_->numRows());
    // Sort the pointers to the rows in RowContainer (data_) instead of sorting
    // the rows.
    returningRows_.resize(numRows_);
    RowContainerIterator iter;
    data_->listRows(&iter, numRows_, returningRows_.data());
    std::sort(
        returningRows_.begin(),
        returningRows_.end(),
        [this](const char* leftRow, const char* rightRow) {
          for (vector_size_t index = 0; index < numSortKeys_; ++index) {
            if (auto result = data_->compare(
                    leftRow, rightRow, index, keyCompareFlags_[index])) {
              return result < 0;
            }
          }
          return false;
        });

  } else {
    // Finish spill, and we shouldn't get any rows from non-spilled partition as
    // there is only one hash partition for orderBy operator.
    Spiller::SpillRows nonSpilledRows = spiller_->finishSpill();
    VELOX_CHECK(nonSpilledRows.empty());
    VELOX_CHECK_NULL(spillMerge_);

    spillMerge_ = spiller_->startMerge(0);
    spillSources_.resize(outputBatchSize_);
    spillSourceRows_.resize(outputBatchSize_);
  }
}

RowVectorPtr OrderBy::getOutput() {
  if (finished_ || !noMoreInput_ || numRows_ == numRowsReturned_) {
    return nullptr;
  }
  prepareOutput();

  if (spiller_ != nullptr) {
    getOutputWithSpill();
  } else {
    getOutputWithoutSpill();
  }
  finished_ = (numRowsReturned_ == numRows_);
  return output_;
}

void OrderBy::getOutputWithoutSpill() {
  VELOX_CHECK_GT(output_->size(), 0);
  VELOX_DCHECK_LE(output_->size(), outputBatchSize_);
  VELOX_CHECK_LE(output_->size() + numRowsReturned_, numRows_);
  VELOX_DCHECK_EQ(numRows_, returningRows_.size());
  VELOX_DCHECK(!finished_);

  for (const auto& columnProjection : columnMap_) {
    data_->extractColumn(
        returningRows_.data() + numRowsReturned_,
        output_->size(),
        columnProjection.inputChannel,
        output_->childAt(columnProjection.outputChannel));
  }
  numRowsReturned_ += output_->size();
}

void OrderBy::getOutputWithSpill() {
  VELOX_CHECK_NOT_NULL(spillMerge_);
  VELOX_DCHECK(!finished_);
  VELOX_DCHECK_EQ(returningRows_.size(), 0);
  VELOX_DCHECK_EQ(spillSources_.size(), outputBatchSize_);
  VELOX_DCHECK_EQ(spillSourceRows_.size(), outputBatchSize_);

  int32_t outputRow = 0;
  int32_t outputSize = 0;
  bool isEndOfBatch = false;
  while (outputRow + outputSize < output_->size()) {
    VELOX_DCHECK_LT(outputRow, output_->size());
    VELOX_DCHECK_LT(outputRow + outputSize, output_->size());

    SpillMergeStream* stream = spillMerge_->next();
    VELOX_CHECK_NOT_NULL(stream);

    spillSources_[outputSize] = &stream->current();
    spillSourceRows_[outputSize] = stream->currentIndex(&isEndOfBatch);
    ++outputSize;
    if (FOLLY_UNLIKELY(isEndOfBatch)) {
      // The stream is at end of input batch. Need to copy out the rows before
      // fetching next batch in 'pop'.
      gatherCopy(
          output_.get(),
          outputRow,
          outputSize,
          spillSources_,
          spillSourceRows_,
          columnMap_);
      outputRow += outputSize;
      outputSize = 0;
    }

    // Advance the stream.
    stream->pop();
  }
  VELOX_CHECK_EQ(outputRow + outputSize, output_->size());

  if (FOLLY_LIKELY(outputSize != 0)) {
    gatherCopy(
        output_.get(),
        outputRow,
        outputSize,
        spillSources_,
        spillSourceRows_,
        columnMap_);
  }

  numRowsReturned_ += output_->size();
}

void OrderBy::prepareOutput() {
  VELOX_CHECK_GT(numRows_, numRowsReturned_);

  const vector_size_t batchSize =
      std::min<vector_size_t>(numRows_ - numRowsReturned_, outputBatchSize_);
  if (output_ != nullptr) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, batchSize);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(outputType_, batchSize, pool()));
  }

  for (auto& child : output_->children()) {
    child->resize(batchSize);
  }
}

std::optional<std::string> OrderBy::makeSpillPath(int32_t operatorId) const {
  auto basePath = operatorCtx_->task()->queryCtx()->config().spillPath();
  if (!basePath.has_value()) {
    return std::nullopt;
  }
  return makeOperatorSpillPath(
      basePath.value(),
      operatorCtx_->task()->taskId(),
      operatorCtx_->driverCtx()->driverId,
      operatorId);
}

} // namespace facebook::velox::exec
