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

#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::velox::connector::tpch {

using facebook::velox::tpch::Table;

namespace {

RowVectorPtr getTpchData(
    Table table,
    size_t maxRows,
    size_t offset,
    size_t scaleFactor,
    memory::MemoryPool* pool) {
  switch (table) {
    case Table::TBL_PART:
      return velox::tpch::genTpchPart(maxRows, offset, scaleFactor, pool);
    case Table::TBL_SUPPLIER:
      return velox::tpch::genTpchSupplier(maxRows, offset, scaleFactor, pool);
    case Table::TBL_PARTSUPP:
      return velox::tpch::genTpchPartSupp(maxRows, offset, scaleFactor, pool);
    case Table::TBL_CUSTOMER:
      return velox::tpch::genTpchCustomer(maxRows, offset, scaleFactor, pool);
    case Table::TBL_ORDERS:
      return velox::tpch::genTpchOrders(maxRows, offset, scaleFactor, pool);
    case Table::TBL_LINEITEM:
      return velox::tpch::genTpchLineItem(maxRows, offset, scaleFactor, pool);
    case Table::TBL_NATION:
      return velox::tpch::genTpchNation(maxRows, offset, scaleFactor, pool);
    case Table::TBL_REGION:
      return velox::tpch::genTpchRegion(maxRows, offset, scaleFactor, pool);
  }
  return nullptr;
}

} // namespace

std::string TpchTableHandle::toString() const {
  return fmt::format(
      "table: {}, scale factor: {}", toTableName(table_), scaleFactor_);
}

TpchDataSource::TpchDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : pool_(pool) {
  auto tpchTableHandle =
      std::dynamic_pointer_cast<TpchTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      tpchTableHandle, "TableHandle must be an instance of TpchTableHandle");
  tpchTable_ = tpchTableHandle->getTable();
  scaleFactor_ = tpchTableHandle->getScaleFactor();
  tpchTableRowCount_ = getRowCount(tpchTable_, scaleFactor_);

  auto tpchTableSchema = getTableSchema(tpchTableHandle->getTable());
  VELOX_CHECK_NOT_NULL(tpchTableSchema, "TpchSchema can't be null.");

  outputColumnMappings_.reserve(outputType->size());

  for (const auto& outputName : outputType->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column '{}' on table '{}'",
        outputName,
        toTableName(tpchTable_));

    auto handle = std::dynamic_pointer_cast<TpchColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of TpchColumnHandle "
        "for '{}' on table '{}'",
        handle->name(),
        toTableName(tpchTable_));

    auto idx = tpchTableSchema->getChildIdxIfExists(handle->name());
    VELOX_CHECK(
        idx != std::nullopt,
        "Column '{}' not found on TPC-H table '{}'.",
        handle->name(),
        toTableName(tpchTable_));
    outputColumnMappings_.emplace_back(*idx);
  }
  outputType_ = outputType;
}

RowVectorPtr TpchDataSource::projectOutputColumns(RowVectorPtr inputVector) {
  std::vector<VectorPtr> children;
  children.reserve(outputColumnMappings_.size());

  for (const auto channel : outputColumnMappings_) {
    children.emplace_back(inputVector->childAt(channel));
  }

  return std::make_shared<RowVector>(
      pool_,
      outputType_,
      BufferPtr(),
      inputVector->size(),
      std::move(children));
}

void TpchDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_EQ(
      currentSplit_,
      nullptr,
      "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<TpchConnectorSplit>(split);
  VELOX_CHECK(currentSplit_, "Wrong type of split for TpchDataSource.");

  size_t partSize =
      std::ceil((double)tpchTableRowCount_ / (double)currentSplit_->totalParts);

  splitOffset_ = partSize * currentSplit_->partNumber;
  splitEnd_ = splitOffset_ + partSize;
}

std::optional<RowVectorPtr> TpchDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(
      currentSplit_, "No split to process. Call addSplit() first.");

  size_t maxRows = std::min(size, (splitEnd_ - splitOffset_));
  auto outputVector =
      getTpchData(tpchTable_, maxRows, splitOffset_, scaleFactor_, pool_);

  // If the split is exhausted.
  if (!outputVector || outputVector->size() == 0) {
    currentSplit_ = nullptr;
    return nullptr;
  }

  // splitOffset needs to advance based on maxRows passed to getTpchData(), and
  // not the actual number of returned rows in the output vector, as they are
  // not the same for lineitem.
  splitOffset_ += maxRows;
  completedRows_ += outputVector->size();
  completedBytes_ += outputVector->retainedSize();

  return projectOutputColumns(outputVector);
}

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<TpchConnectorFactory>())

} // namespace facebook::velox::connector::tpch
