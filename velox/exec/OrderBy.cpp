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
      numSortKeys_(orderByNode->sortingKeys().size()) {
  std::vector<TypePtr> keyTypes;
  std::vector<TypePtr> dependentTypes;
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
    columnMap_.emplace_back(channel, i);
    keyTypes.push_back(outputType_->childAt(channel));
    keyCompareFlags_.push_back(
        fromSortOrderToCompareFlags(orderByNode->sortingOrders()[i]));
    keyChannelSet.emplace(channel);
  }

  // Store non-sort key columns as dependents in row container.
  for (column_index_t inputChannel = 0, nextOutputChannel = numSortKeys_;
       inputChannel < outputType_->size();
       ++inputChannel) {
    if (keyChannelSet.count(inputChannel) != 0) {
      continue;
    }
    columnMap_.emplace_back(inputChannel, nextOutputChannel++);
    dependentTypes.push_back(outputType_->childAt(inputChannel));
  }

  // Create row container.
  data_ = std::make_unique<RowContainer>(
      keyTypes, dependentTypes, operatorCtx_->mappedMemory());

  outputBatchSize_ = std::max<uint32_t>(
      operatorCtx_->execCtx()->queryCtx()->config().preferredOutputBatchSize(),
      data_->estimatedNumRowsPerBatch(kBatchSizeInBytes));
}

void OrderBy::addInput(RowVectorPtr input) {
  SelectivityVector allRows(input->size());
  std::vector<char*> rows(input->size());
  for (int row = 0; row < input->size(); ++row) {
    rows[row] = data_->newRow();
  }
  for (const auto& columnProjection : columnMap_) {
    DecodedVector decoded(
        *input->childAt(columnProjection.inputChannel), allRows);
    for (int i = 0; i < input->size(); ++i) {
      data_->store(decoded, i, rows[i], columnProjection.outputChannel);
    }
  }

  numRows_ += allRows.size();
}

void OrderBy::noMoreInput() {
  Operator::noMoreInput();

  // No data.
  if (numRows_ == 0) {
    finished_ = true;
    return;
  }

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
}

RowVectorPtr OrderBy::getOutput() {
  if (finished_ || !noMoreInput_ || numRows_ == numRowsReturned_) {
    return nullptr;
  }

  prepareOutput();

  for (const auto& identityProjection : columnMap_) {
    data_->extractColumn(
        returningRows_.data() + numRowsReturned_,
        output_->size(),
        identityProjection.outputChannel,
        output_->childAt(identityProjection.inputChannel));
  }
  numRowsReturned_ += output_->size();

  finished_ = (numRowsReturned_ == numRows_);
  return output_;
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

} // namespace facebook::velox::exec
