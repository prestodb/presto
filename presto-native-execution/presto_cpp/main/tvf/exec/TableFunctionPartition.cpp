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
    const velox::RowTypePtr& requiredColumnType,
    velox::memory::MemoryPool* pool)
    : data_(data), partition_(rows), inputMapping_(inputMapping), requiredColumnType_(requiredColumnType), pool_(pool) {}

TableFunctionPartition::~TableFunctionPartition() {
  partition_.clear();
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

velox::RowVectorPtr TableFunctionPartition::assembleInput(
    velox::vector_size_t numRowsPerOutput,
    velox::vector_size_t numPartitionProcessedRows,
    const std::vector<velox::column_index_t>& requiredColumns) const {

  if (numRows() == 0) {
    return nullptr;
  }

  const auto numRowsLeft = numRows() - numPartitionProcessedRows;
  VELOX_CHECK_GT(numRowsLeft, 0);
  const auto numOutputRows = std::min(numRowsPerOutput, numRowsLeft);
  auto input =
      BaseVector::create<RowVector>(requiredColumnType_, numOutputRows, pool_);

  for (int i = 0; i < requiredColumns.size(); i++) {
    input->childAt(i)->resize(numOutputRows);
    extractColumn(
        requiredColumns[i],
        numPartitionProcessedRows,
        numOutputRows,
        0,
        input->childAt(i));
  }
  return std::move(input);
}

} // namespace facebook::presto::tvf
