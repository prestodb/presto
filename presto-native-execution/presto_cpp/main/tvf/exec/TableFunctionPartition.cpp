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

#include "presto_cpp/main/tvf/exec/TableFunctionPartition.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;
using namespace facebook::velox::exec;

TableFunctionPartition::TableFunctionPartition(
    RowContainer* data,
    const folly::Range<char**>& rows,
    const std::vector<velox::column_index_t>& inputMapping,
    const std::vector<velox::RowTypePtr>& requiredColumnTypes,
    const std::vector<std::vector<velox::column_index_t>>& requiredColumns,
    const std::unordered_map<velox::column_index_t, velox::column_index_t>&
        markerChannels,
    const std::vector<
        TableFunctionProcessorNode::PassThroughColumnSpecification>&
        passThroughColumns,
    const velox::RowTypePtr& outputType,
    velox::memory::MemoryPool* pool)
    : data_(data),
      partition_(rows),
      inputMapping_(inputMapping),
      requiredColumnTypes_(requiredColumnTypes),
      requiredColumns_(requiredColumns),
      markerChannels_(markerChannels),
      passThroughSpecifications_(passThroughColumns),
      outputType_(outputType),
      pool_(pool) {
  initNullPositions();
}

TableFunctionPartition::~TableFunctionPartition() {
  partition_.clear();
}

void TableFunctionPartition::initNullPositions() {
  std::vector<column_index_t> referencedChannels;
  for (const auto& channels : requiredColumns_) {
    for (const auto& channel : channels) {
      referencedChannels.push_back(channel);
    }
  }

  for (const auto& spec : passThroughSpecifications_) {
    referencedChannels.push_back(spec.inputChannel());
  }

  if (referencedChannels.empty()) {
    // No required or pass-through channels
    return;
  }

  int maxInputChannel =
      *std::max_element(referencedChannels.begin(), referencedChannels.end());
  nullPositions_.resize(maxInputChannel + 1);

  if (markerChannels_.empty()) {
    // No marker channels, set end-of-data to partitionEnd for all referenced
    // channels
    for (int channel : referencedChannels) {
      nullPositions_[channel] = numRows();
    }
    return;
  }

  // Marker channels are present
  std::vector<vector_size_t> markerChannelNullPositions;
  markerChannelNullPositions.resize(markerChannels_.size());
  for (const auto& [_, markerChannel] : markerChannels_) {
    markerChannelNullPositions[markerChannel] =
        findFirstNull(data_->columnAt(inputMapping_[markerChannel]));
  }

  for (int channel : referencedChannels) {
    auto it = markerChannels_.find(channel);
    if (it != markerChannels_.end()) {
      int markerChannel = it->second;
      nullPositions_[channel] = markerChannelNullPositions[markerChannel];
    }
  }
}

void TableFunctionPartition::extractPartitionColumn(
    int32_t columnIndex,
    const VectorPtr& result) const {
  auto numRows = result->size();
  std::vector<vector_size_t> rowNumbers;
  rowNumbers.reserve(numRows);
  for (vector_size_t i = 0; i < numRows; ++i) {
    rowNumbers.push_back(0);
  }

  auto rowNumbersRange = folly::Range(rowNumbers.data(), numRows);
  extractColumn(columnIndex, rowNumbersRange, 0, result);
}

void TableFunctionPartition::extractColumn(
    int32_t columnIndex,
    folly::Range<const vector_size_t*> rowNumbers,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  RowContainer::extractColumn(
      partition_.data(),
      rowNumbers,
      data_->columnAt(inputMapping_[columnIndex]),
      data_->columnHasNulls(inputMapping_[columnIndex]),
      resultOffset,
      result);
}

void TableFunctionPartition::extractPassThroughIndexColumn(
    int32_t columnIndex,
    int32_t passThroughIndex,
    const velox::RowVectorPtr& functionOutput,
    const velox::VectorPtr& result) const {
  auto numRows = functionOutput->size();
  std::vector<vector_size_t> rowNumbers;
  rowNumbers.reserve(numRows);
  FlatVector<int32_t>* passThroughIndexVector =
      functionOutput->childAt(passThroughIndex)->as<FlatVector<int32_t>>();
  for (vector_size_t i = 0; i < functionOutput->size(); ++i) {
    rowNumbers.push_back(passThroughIndexVector->valueAt(i));
  }

  auto rowNumbersRange = folly::Range(rowNumbers.data(), numRows);
  extractColumn(columnIndex, rowNumbersRange, 0, result);
}

void TableFunctionPartition::extractColumn(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    vector_size_t resultOffset,
    const VectorPtr& result) const {
  RowContainer::extractColumn(
      partition_.data() + partitionOffset,
      numRows,
      data_->columnAt(inputMapping_[columnIndex]),
      data_->columnHasNulls(inputMapping_[columnIndex]),
      resultOffset,
      result);
}

void TableFunctionPartition::extractNulls(
    int32_t columnIndex,
    vector_size_t partitionOffset,
    vector_size_t numRows,
    const BufferPtr& nullsBuffer) const {
  RowContainer::extractNulls(
      partition_.data() + partitionOffset,
      numRows,
      data_->columnAt(inputMapping_[columnIndex]),
      nullsBuffer);
}

vector_size_t TableFunctionPartition::findFirstNull(RowColumn column) {
  auto nullMask = column.nullMask();
  const vector_size_t numRows = partition_.size();
  if (!nullMask) {
    return numRows;
  }

  const char* const* rows = partition_.data();
  auto nullByte = column.nullByte();
  for (auto i = 0; i < numRows; ++i) {
    const char* row = rows[i];
    if (row == nullptr || data_->isNullAt(row, nullByte, nullMask)) {
      return i;
    }
  }

  return numRows;
}

std::vector<velox::RowVectorPtr> TableFunctionPartition::assembleInput(
    velox::vector_size_t numRowsPerOutput,
    velox::vector_size_t numPartitionProcessedRows) const {
  std::vector<velox::RowVectorPtr> result;
  result.reserve(requiredColumns_.size());
  if (numRows() == 0) {
    for (int i = 0; i < requiredColumns_.size(); i++) {
      result.push_back(nullptr);
    }
    return result;
  }

  const auto numRowsLeft = numRows() - numPartitionProcessedRows;
  VELOX_CHECK_GT(numRowsLeft, 0);

  for (int i = 0; i < requiredColumns_.size(); i++) {
    auto tableArgType = requiredColumnTypes_[i];
    auto numNonNullRows = std::max(
        nullPositions_[requiredColumns_[i][0]] - numPartitionProcessedRows, 0);
    const auto numNonNullOutputRows =
        std::min(numRowsPerOutput, numNonNullRows);

    auto input = BaseVector::create<RowVector>(
        tableArgType, numNonNullOutputRows, pool_);
    for (int j = 0; j < tableArgType->children().size(); j++) {
      auto numNonNullRowsCol = std::max(
          nullPositions_[requiredColumns_[i][j]] - numPartitionProcessedRows,
          0);
      VELOX_CHECK_EQ(numNonNullRows, numNonNullRowsCol);

      auto columnVector = BaseVector::create(
          tableArgType->childAt(j), numNonNullOutputRows, pool_);
      extractColumn(
          requiredColumns_[i][j],
          numPartitionProcessedRows,
          numNonNullOutputRows,
          0,
          columnVector);
      input->childAt(j) = columnVector;
    }
    result.push_back(input);
  }

  return std::move(result);
}

RowVectorPtr TableFunctionPartition::appendPassThroughColumns(
    const velox::RowVectorPtr& functionOutput) const {
  if (passThroughSpecifications_.empty()) {
    return functionOutput;
  }

  VELOX_CHECK_EQ(
      functionOutput->children().size() + passThroughSpecifications_.size(),
      outputType_->size());
  auto numOutputRows = functionOutput->size();
  auto result =
      BaseVector::create<RowVector>(outputType_, numOutputRows, pool_);
  // Copy function output columns.
  for (int i = 0; i < functionOutput->children().size(); i++) {
    result->childAt(i) = functionOutput->childAt(i);
  }

  // Copy passthrough columns.
  for (const auto& spec : passThroughSpecifications_) {
    auto passThroughColumn = BaseVector::create(
        outputType_->childAt(spec.inputChannel()), numOutputRows, pool_);
    if (spec.isPartitioningColumn()) {
      extractPartitionColumn(spec.inputChannel(), passThroughColumn);
    } else {
      extractPassThroughIndexColumn(
          spec.inputChannel(),
          spec.indexChannel(),
          functionOutput,
          passThroughColumn);
    }
    result->childAt(spec.inputChannel()) = passThroughColumn;
  }
  return result;
}
} // namespace facebook::presto::tvf
