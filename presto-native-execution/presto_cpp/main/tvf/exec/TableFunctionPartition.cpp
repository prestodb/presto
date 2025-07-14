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
#include "presto_cpp/main/tvf/exec/TableFunctionPartition.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;
using namespace facebook::velox::exec;

TableFunctionPartition::TableFunctionPartition(RowContainer* data)
    : data_(data), rows_(), partition_() {}

TableFunctionPartition::~TableFunctionPartition() {
  data_->clear();
  data_->pool()->release();
  rows_.clear();
  partition_.clear();
}

void TableFunctionPartition::addRows(const std::vector<char*>& rows) {
  rows_.insert(rows_.end(), rows.begin(), rows.end());
  partition_ = folly::Range(rows_.data(), rows_.size());
}

void TableFunctionPartition::clear() {
  rows_.clear();
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
      data_->columnAt(columnIndex),
      data_->columnHasNulls(columnIndex),
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
      data_->columnAt(columnIndex),
      data_->columnHasNulls(columnIndex),
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
      data_->columnAt(columnIndex),
      nullsBuffer);
}

} // namespace facebook::presto::tvf
