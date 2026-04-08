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
    const std::vector<std::pair<velox::column_index_t, velox::column_index_t>>&
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
  // Collect all channels that need null position tracking (as input channels).
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

  // nullPositions_ is indexed by TableFunctionPartitions column index,
  // not input column index. Find the max partition column index we need.
  int maxReferencedColumn = 0;
  for (int inputChannel : referencedChannels) {
    int actualColumn = inputMapping_[inputChannel];
    maxReferencedColumn = std::max(maxReferencedColumn, actualColumn);
  }
  nullPositions_.resize(maxReferencedColumn + 1);

  if (markerChannels_.empty()) {
    // No marker channels, set end-of-data to partitionEnd for all referenced
    // channels.
    for (int inputChannel : referencedChannels) {
      int actualColumn = inputMapping_[inputChannel];
      nullPositions_[actualColumn] = numRows();
    }
    return;
  }

  // Marker channels are present, so compute null positions based on marker
  // channels for all referenced channels.
  std::unordered_map<int32_t, vector_size_t> markerChannelNullPositions;
  for (const auto& [_, markerInputChannel] : markerChannels_) {
    int actualColumn = inputMapping_[markerInputChannel];
    markerChannelNullPositions[markerInputChannel] =
        findFirstNull(data_->columnAt(actualColumn));
  }

  for (int inputChannel : referencedChannels) {
    int actualColumn = inputMapping_[inputChannel];
    // Find inputChannel in markerChannels_ vector
    auto it = std::find_if(
        markerChannels_.begin(),
        markerChannels_.end(),
        [inputChannel](const auto& pair) {
          return pair.first == inputChannel;
        });
    if (it != markerChannels_.end()) {
      int markerInputChannel = it->second;
      nullPositions_[actualColumn] =
          markerChannelNullPositions[markerInputChannel];
    } else {
      // Channel has no marker, so all rows are valid (set to partition end)
      nullPositions_[actualColumn] = numRows();
    }
  }
}

void TableFunctionPartition::extractPartitionColumn(
    int32_t columnIndex,
    const VectorPtr& result) const {
  // Partitioning columns have the same value for all rows in the partition
  // Extract row 0 (partition start) and repeat it for all output rows.
  auto numRows = result->size();
  std::vector<vector_size_t> rowNumbers(numRows, 0);
  extractColumn(
      columnIndex, folly::Range(rowNumbers.data(), numRows), 0, result);
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

  // Index columns are INTEGER (int32_t) in Velox.
  FlatVector<int32_t>* passThroughIndexVector =
      functionOutput->childAt(passThroughIndex)->as<FlatVector<int32_t>>();
  VELOX_CHECK_NOT_NULL(
      passThroughIndexVector,
      "Pass-through index column at position {} must be INTEGER type",
      passThroughIndex);
  bool hasNonNullIndices = false;
  for (vector_size_t i = 0; i < numRows; ++i) {
    if (passThroughIndexVector->isNullAt(i)) {
      // For NULL index values, we still need to add a placeholder row number.
      // The actual NULL will be set by checking the index vector's null flags.
      rowNumbers.push_back(0);
      result->setNull(i, true);
    } else {
      rowNumbers.push_back(
          static_cast<vector_size_t>(passThroughIndexVector->valueAt(i)));
      hasNonNullIndices = true;
    }
  }

  // Only extract from partition if partition has rows AND we have non-NULL
  // indices
  if (partition_.size() > 0 && hasNonNullIndices) {
    extractColumn(
        columnIndex, folly::Range(rowNumbers.data(), numRows), 0, result);

    // Re-apply NULL flags for rows where the index was NULL.
    for (vector_size_t i = 0; i < numRows; ++i) {
      if (passThroughIndexVector->isNullAt(i)) {
        result->setNull(i, true);
      }
    }
  } else {
    // When partition is empty or all indices are NULL,
    // ensure all rows are marked as NULL.
    for (vector_size_t i = 0; i < numRows; ++i) {
      result->setNull(i, true);
    }
  }
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
    velox::vector_size_t numPartitionProcessedRows) {
  std::vector<velox::RowVectorPtr> result;
  result.reserve(requiredColumns_.size());

  // Empty partitions with !pruneWhenEmpty should produce output row
  // with NULL values for the function.`
  if (numRows() == 0) {
    result.resize(requiredColumns_.size(), nullptr);
    return result;
  }

  // The partition has sent the function all its input rows, but the
  // function has not returned FinishedResult. This can happen for functions
  // that need multiple calls to apply() to return all results for a partition.
  // So the apply() is called with a single row of nullptrs.
  const auto numRowsLeft = numRows() - numPartitionProcessedRows;
  if (numRowsLeft == 0) {
    result.resize(requiredColumns_.size(), nullptr);
    return result;
  }

  VELOX_CHECK_GT(numRowsLeft, 0);
  std::vector<int> inputNonNullRows;
  inputNonNullRows.reserve(requiredColumns_.size());
  int maxNonNullRows = 0;

  for (int i = 0; i < requiredColumns_.size(); i++) {
    auto actualColumnIndex = inputMapping_[requiredColumns_[i][0]];
    auto numNonNullRows = std::max(
        nullPositions_[actualColumnIndex] - numPartitionProcessedRows, 0);
    inputNonNullRows.push_back(numNonNullRows);
    maxNonNullRows = std::max(maxNonNullRows, numNonNullRows);
  }

  if (maxNonNullRows == 0 && requiredColumns_.size() > 1) {
    result.resize(requiredColumns_.size(), nullptr);
    return result;
  }

  for (int i = 0; i < requiredColumns_.size(); i++) {
    auto tableArgType = requiredColumnTypes_[i];
    auto actualColumnIndex = inputMapping_[requiredColumns_[i][0]];
    auto nullPosition = nullPositions_[actualColumnIndex];
    auto inputRows = inputNonNullRows[i];

    const auto numOutputRows =
        (inputRows == 0) ? 0 : std::min(numRowsPerOutput, inputRows);

    auto input =
        BaseVector::create<RowVector>(tableArgType, numOutputRows, pool_);
    if (numOutputRows > 0) {
      for (int j = 0; j < tableArgType->children().size(); j++) {
        auto columnVector =
            BaseVector::create(tableArgType->childAt(j), numOutputRows, pool_);
        extractColumn(
            requiredColumns_[i][j], // Pass INPUT column index, extractColumn
                                    // will map it.
            numPartitionProcessedRows,
            numOutputRows,
            0,
            columnVector);
        // For rows beyond the nullPosition marker, set them to null
        // This handles the case where shorter inputs are padded in the
        // cross-product.
        // TODO(Aditi) : Is this really needed ?
        for (int rowIdx = 0; rowIdx < numOutputRows; rowIdx++) {
          int absoluteRowIdx = numPartitionProcessedRows + rowIdx;
          if (absoluteRowIdx >= nullPosition) {
            columnVector->setNull(rowIdx, true);
          }
        }
        input->childAt(j) = columnVector;
      }
      result.push_back(input);
    } else {
      // Set nullptr for this input table argument if there were no non-null
      // rows for the argument. This can happen when the function has multiple
      // table arguments and the all the rows for one argument are null,
      // but the other argument has non-null rows.
      result.push_back(nullptr);
    }
  }

  return std::move(result);
}

size_t TableFunctionPartition::getNumProperColumns(
    const velox::RowVectorPtr& functionOutput) const {
  // For functions with pass-through columns, the function output contains:
  // - Proper columns (declared in return type)
  // - Index columns (one or more, depending on number of pass-through sources)
  // The outputType_ contains:
  // - Proper columns + pass-through columns

  // The number of proper columns in the output = outputType_->size() -
  // passThroughSpecifications_.size() This is because outputType contains
  // proper columns + pass-through columns
  size_t numProperOutputColumns =
      outputType_->size() - passThroughSpecifications_.size();

  // The function output should have: proper columns + index columns
  // We validate that the function output has at least the proper columns
  VELOX_CHECK_GE(
      functionOutput->children().size(),
      numProperOutputColumns,
      "Function output must have at least {} proper columns, but has only {} total columns",
      numProperOutputColumns,
      functionOutput->children().size());
  return numProperOutputColumns;
}

RowVectorPtr TableFunctionPartition::appendPassThroughColumns(
    const velox::RowVectorPtr& functionOutput) const {
  VELOX_CHECK_NOT_NULL(functionOutput);
  if (passThroughSpecifications_.empty()) {
    return functionOutput;
  }

  auto numOutputRows = functionOutput->size();

  // Handle the case where all columns are pruned (outputType_ has 0 children)
  // In this case, we just return an empty RowVector with the correct row count
  // This can happen with ONLY_PASS_THROUGH functions when all pass-through
  // columns are pruned
  if (outputType_->size() == 0) {
    return BaseVector::create<RowVector>(outputType_, numOutputRows, pool_);
  }

  // For ONLY_PASS_THROUGH functions, all output columns come from pass-through
  // The function output only contains index columns for non-partitioning
  // pass-through
  bool isOnlyPassThrough =
      (passThroughSpecifications_.size() == outputType_->size());
  size_t numProperColumns = 0;
  if (!isOnlyPassThrough) {
    numProperColumns = getNumProperColumns(functionOutput);
  }

  auto result =
      BaseVector::create<RowVector>(outputType_, numOutputRows, pool_);
  // Copy function output columns when non-passthrough.
  if (!isOnlyPassThrough) {
    for (int i = 0; i < numProperColumns; i++) {
      result->childAt(i) = functionOutput->childAt(i);
    }
  }

  // Copy passthrough columns.
  for (const auto& spec : passThroughSpecifications_) {
    auto passThroughColumn = BaseVector::create(
        outputType_->childAt(spec.outputChannel()), numOutputRows, pool_);
    if (numOutputRows > 0) {
      if (spec.isPartitioningColumn()) {
        extractPartitionColumn(spec.inputChannel(), passThroughColumn);
      } else {
        // Only extract using index column if the function output has children.
        // When all columns are pruned, functionOutput may have 0 children.
        if (spec.indexChannel() < functionOutput->childrenSize()) {
          extractPassThroughIndexColumn(
              spec.inputChannel(),
              spec.indexChannel(),
              functionOutput,
              passThroughColumn);
        }
      }
    }
    result->childAt(spec.outputChannel()) = passThroughColumn;
  }
  return result;
}
} // namespace facebook::presto::tvf
