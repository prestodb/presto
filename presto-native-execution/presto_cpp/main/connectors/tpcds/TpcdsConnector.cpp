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

#include "presto_cpp/main/connectors/tpcds/TpcdsConnector.h"
#include "presto_cpp/main/connectors/tpcds/DSDGenIterator.h"

using namespace ::facebook::velox::tpcds;
namespace facebook::velox::connector::tpcds {

using facebook::velox::tpcds::Table;

namespace {

RowVectorPtr getTpcdsData(
    Table table,
    size_t maxRows,
    size_t offset,
    memory::MemoryPool* pool,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child) {
  switch (table) {
    case Table::TBL_CALL_CENTER:
      return genTpcdsCallCenter(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_CATALOG_PAGE:
      return genTpcdsCatalogPage(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_CATALOG_RETURNS:
      return genTpcdsCatalogReturns(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_CATALOG_SALES:
      return genTpcdsCatalogSales(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_CUSTOMER:
      return genTpcdsCustomer(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_CUSTOMER_ADDRESS:
      return genTpcdsCustomerAddress(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_CUSTOMER_DEMOGRAPHICS:
      return genTpcdsCustomerDemographics(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_DATE_DIM:
      return genTpcdsDateDim(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_HOUSEHOLD_DEMOGRAHICS:
      return genTpcdsHouseholdDemographics(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_INCOME_BAND:
      return genTpcdsIncomeBand(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_INVENTORY:
      return genTpcdsInventory(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_ITEM:
      return genTpcdsItem(pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_PROMOTION:
      return genTpcdsPromotion(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_REASON:
      return genTpcdsReason(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_SHIP_MODE:
      return genTpcdsShipMode(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_STORE:
      return genTpcdsStore(pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_STORE_RETURNS:
      return genTpcdsStoreReturns(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_STORE_SALES:
      return genTpcdsStoreSales(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_TIME:
      return genTpcdsTime(pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_WAREHOUSE:
      return genTpcdsWarehouse(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_WEB_PAGE:
      return genTpcdsWebpage(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_WEB_RETURNS:
      return genTpcdsWebReturns(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_WEB_SALES:
      return genTpcdsWebSales(
          pool, maxRows, offset, scaleFactor, parallel, child);
    case Table::TBL_WEBSITE:
      return genTpcdsWebsite(
          pool, maxRows, offset, scaleFactor, parallel, child);
    default:
      return nullptr;
  }
  return nullptr; // make gcc happy
}

} // namespace

std::string TpcdsTableHandle::toString() const {
  return fmt::format(
      "table: {}, scale factor: {}", toTableName(table_), scaleFactor_);
}

TpcdsDataSource::TpcdsDataSource(
    const std::shared_ptr<const RowType>& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    velox::memory::MemoryPool* FOLLY_NONNULL pool)
    : pool_(pool) {
  auto tpcdsTableHandle =
      std::dynamic_pointer_cast<TpcdsTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      tpcdsTableHandle, "TableHandle must be an instance of TpcdsTableHandle");
  tpcdsTable_ = tpcdsTableHandle->getTable();
  scaleFactor_ = tpcdsTableHandle->getScaleFactor();
  DSDGenIterator dsdGenIterator(scaleFactor_, 1, 1);
  tpcdsTableRowCount_ =
      dsdGenIterator.getRowCount(static_cast<int>(tpcdsTable_));

  auto tpcdsTableSchema = getTableSchema(tpcdsTableHandle->getTable());
  VELOX_CHECK_NOT_NULL(tpcdsTableSchema, "TpcdsSchema can't be null.");

  outputColumnMappings_.reserve(outputType->size());

  for (const auto& outputName : outputType->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column '{}' on table '{}'",
        outputName,
        toTableName(tpcdsTable_));

    auto handle = std::dynamic_pointer_cast<TpcdsColumnHandle>(it->second);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of TpcdsColumnHandle "
        "for '{}' on table '{}'",
        handle->name(),
        toTableName(tpcdsTable_));

    auto idx = tpcdsTableSchema->getChildIdxIfExists(handle->name());
    VELOX_CHECK(
        idx != std::nullopt,
        "Column '{}' not found on TPC-DS table '{}'.",
        handle->name(),
        toTableName(tpcdsTable_));
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

  size_t partSize = std::ceil(
      (double)tpcdsTableRowCount_ / (double)currentSplit_->totalParts);

  splitOffset_ = partSize * currentSplit_->partNumber;
  splitEnd_ = splitOffset_ + partSize;
}

std::optional<RowVectorPtr> TpcdsDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK_NOT_NULL(
      currentSplit_, "No split to process. Call addSplit() first.");

  size_t maxRows = std::min(size, (splitEnd_ - splitOffset_));
  vector_size_t parallel = currentSplit_->totalParts;
  vector_size_t child = currentSplit_->partNumber;
  auto outputVector = getTpcdsData(
      tpcdsTable_, maxRows, splitOffset_, pool_, scaleFactor_, parallel, child);

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

VELOX_REGISTER_CONNECTOR_FACTORY(std::make_shared<TpcdsConnectorFactory>())

} // namespace facebook::velox::connector::tpcds