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

#pragma once

#include "velox/common/memory/Memory.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::tpcds {

/// This file uses TPC-DS DSDGEN to generate data encoded using Velox Vectors.

enum class Table : uint8_t {
  TBL_CALL_CENTER,
  TBL_CATALOG_PAGE,
  TBL_CATALOG_RETURNS,
  TBL_CATALOG_SALES,
  TBL_CUSTOMER,
  TBL_CUSTOMER_ADDRESS,
  TBL_CUSTOMER_DEMOGRAPHICS,
  TBL_DATE_DIM,
  TBL_HOUSEHOLD_DEMOGRAHICS,
  TBL_INCOME_BAND,
  TBL_INVENTORY,
  TBL_ITEM,
  TBL_PROMOTION,
  TBL_REASON,
  TBL_SHIP_MODE,
  TBL_STORE,
  TBL_STORE_RETURNS,
  TBL_STORE_SALES,
  TBL_TIME,
  TBL_WAREHOUSE,
  TBL_WEB_PAGE,
  TBL_WEB_RETURNS,
  TBL_WEB_SALES,
  TBL_WEBSITE
};

static constexpr auto tables = {
    tpcds::Table::TBL_CALL_CENTER,
    tpcds::Table::TBL_CATALOG_PAGE,
    tpcds::Table::TBL_CATALOG_RETURNS,
    tpcds::Table::TBL_CATALOG_SALES,
    tpcds::Table::TBL_CUSTOMER,
    tpcds::Table::TBL_CUSTOMER_ADDRESS,
    tpcds::Table::TBL_CUSTOMER_DEMOGRAPHICS,
    tpcds::Table::TBL_DATE_DIM,
    tpcds::Table::TBL_HOUSEHOLD_DEMOGRAHICS,
    tpcds::Table::TBL_INCOME_BAND,
    tpcds::Table::TBL_INVENTORY,
    tpcds::Table::TBL_ITEM,
    tpcds::Table::TBL_PROMOTION,
    tpcds::Table::TBL_REASON,
    tpcds::Table::TBL_SHIP_MODE,
    tpcds::Table::TBL_STORE,
    tpcds::Table::TBL_STORE_RETURNS,
    tpcds::Table::TBL_STORE_SALES,
    tpcds::Table::TBL_TIME,
    tpcds::Table::TBL_WAREHOUSE,
    tpcds::Table::TBL_WEB_PAGE,
    tpcds::Table::TBL_WEB_RETURNS,
    tpcds::Table::TBL_WEB_SALES,
    tpcds::Table::TBL_WEBSITE};

// Returns table name as a string.
std::string toTableName(Table table);

/// Returns the schema (RowType) for a particular TPC-DS table.
RowTypePtr getTableSchema(Table table);

/// Returns the type of a particular table:column pair. Throws if `columnName`
/// does not exist in `table`.
TypePtr resolveTpcdsColumn(Table table, const std::string& columnName);

Table fromTableName(std::string_view tableName);

RowVectorPtr genTpcdsCallCenter(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsCatalogPage(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsCatalogReturns(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsCatalogSales(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsCustomer(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsCustomerAddress(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsCustomerDemographics(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsDateDim(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsHouseholdDemographics(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsIncomeBand(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsInventory(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsItem(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsPromotion(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsReason(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsShipMode(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsStore(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsStoreReturns(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsStoreSales(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsTime(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsWarehouse(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsWebpage(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsWebReturns(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsWebSales(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);

RowVectorPtr genTpcdsWebsite(
    memory::MemoryPool* pool,
    size_t maxRows,
    size_t offset,
    double scaleFactor,
    vector_size_t parallel,
    vector_size_t child);
} // namespace facebook::velox::tpcds
