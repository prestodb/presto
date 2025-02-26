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

#include "presto_cpp/main/connectors/tpcds/TpcdsConnector.h"
#include "presto_cpp/main/connectors/tpcds/DSDGenIterator.h"
#include "presto_cpp/presto_protocol/connector/tpcds/TpcdsConnectorProtocol.h"

using namespace ::facebook::velox;
namespace facebook::presto::connector::tpcds {

using facebook::presto::connector::tpcds::Table;

std::string TpcdsTableHandle::toString() const {
  return fmt::format(
      "table: {}, scale factor: {}", toTableName(table_), scaleFactor_);
}

TpcdsDataSource::TpcdsDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<velox::connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::ColumnHandle>>& columnHandles,
    velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : pool_(pool) {
  auto tpcdsTableHandle =
      std::dynamic_pointer_cast<TpcdsTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      tpcdsTableHandle, "TableHandle must be an instance of TpcdsTableHandle");
  table_ = tpcdsTableHandle->getTpcdsTable();
  scaleFactor_ = tpcdsTableHandle->getScaleFactor();
  DSDGenIterator dsdGenIterator(scaleFactor_, 1, 1);
  rowCount_ = dsdGenIterator.getRowCount(static_cast<int>(table_));

  auto tpcdsTableSchema = getTableSchema(tpcdsTableHandle->getTpcdsTable());
  VELOX_CHECK_NOT_NULL(tpcdsTableSchema, "TpcdsSchema can't be null.");

  outputColumnMappings_.reserve(outputType->size());

  for (const auto& outputName : outputType->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column '{}' on table '{}'",
        outputName,
        toTableName(table_));

    auto handle = std::dynamic_pointer_cast<TpcdsColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of TpcdsColumnHandle "
        "for '{}' on table '{}'",
        handle->name(),
        toTableName(table_));

    auto idx = tpcdsTableSchema->getChildIdxIfExists(handle->name());
    VELOX_CHECK(
        idx != std::nullopt,
        "Column '{}' not found on TPC-DS table '{}'.",
        handle->name(),
        toTableName(table_));
    outputColumnMappings_.emplace_back(*idx);
  }
  outputType_ = outputType;
}

RowVectorPtr TpcdsDataSource::projectOutputColumns(RowVectorPtr inputVector) {
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

void TpcdsDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_EQ(
      currentSplit_,
      nullptr,
      "Previous split has not been processed yet. Call next() to process the split.");
  currentSplit_ = std::dynamic_pointer_cast<TpcdsConnectorSplit>(split);
  VELOX_CHECK(currentSplit_, "Wrong type of split for TpcdsDataSource.");

  size_t partSize =
      std::ceil((double)rowCount_ / (double)currentSplit_->totalParts_);

  splitOffset_ = partSize * currentSplit_->partNumber_;
  splitEnd_ = splitOffset_ + partSize;
}

std::optional<RowVectorPtr> TpcdsDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(
      currentSplit_, "No split to process. Call addSplit() first.");

  size_t maxRows = std::min(size, (splitEnd_ - splitOffset_));
  vector_size_t parallel = currentSplit_->totalParts_;
  vector_size_t child = currentSplit_->partNumber_;
  auto outputVector = genTpcdsData(
      table_, maxRows, splitOffset_, pool_, scaleFactor_, parallel, child);

  // If the split is exhausted.
  if (!outputVector || outputVector->size() == 0) {
    currentSplit_ = nullptr;
    return nullptr;
  }

  // splitOffset needs to advance based on maxRows passed to getTpcdsData(), and
  // not the actual number of returned rows in the output vector, as they are
  // not the same for lineitem.
  splitOffset_ += maxRows;
  completedRows_ += outputVector->size();
  completedBytes_ += outputVector->retainedSize();

  return projectOutputColumns(outputVector);
}

std::unique_ptr<velox::connector::ConnectorSplit>
TpcdsPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* connectorSplit,
    const protocol::SplitContext* splitContext) const {
  auto tpcdsSplit =
      dynamic_cast<const protocol::tpcds::TpcdsSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      tpcdsSplit, "Unexpected split type {}", connectorSplit->_type);
  return std::make_unique<presto::connector::tpcds::TpcdsConnectorSplit>(
      catalogId,
      splitContext->cacheable,
      tpcdsSplit->totalParts,
      tpcdsSplit->partNumber);
}

std::unique_ptr<velox::connector::ColumnHandle>
TpcdsPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto tpcdsColumn =
      dynamic_cast<const protocol::tpcds::TpcdsColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      tpcdsColumn, "Unexpected column handle type {}", column->_type);
  return std::make_unique<presto::connector::tpcds::TpcdsColumnHandle>(
      tpcdsColumn->columnName);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
TpcdsPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser,
    std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::ColumnHandle>>& assignments) const {
  auto tpcdsLayout =
      std::dynamic_pointer_cast<const protocol::tpcds::TpcdsTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      tpcdsLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);
  return std::make_unique<presto::connector::tpcds::TpcdsTableHandle>(
      tableHandle.connectorId,
      presto::connector::tpcds::fromTableName(tpcdsLayout->table.tableName),
      tpcdsLayout->table.scaleFactor);
}

std::unique_ptr<protocol::ConnectorProtocol>
TpcdsPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::tpcds::TpcdsConnectorProtocol>();
}
} // namespace facebook::presto::connector::tpcds
