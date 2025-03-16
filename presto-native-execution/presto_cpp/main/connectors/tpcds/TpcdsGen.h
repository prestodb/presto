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

#pragma once

#include "velox/common/memory/Memory.h"

namespace facebook::velox {
class RowVector;
using RowVectorPtr = std::shared_ptr<RowVector>;
class RowType;
using RowTypePtr = std::shared_ptr<const RowType>;
} // namespace facebook::velox

namespace facebook::presto::connector::tpcds {

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
  TBL_HOUSEHOLD_DEMOGRAPHICS,
  TBL_INCOME_BAND,
  TBL_INVENTORY,
  TBL_ITEM,
  TBL_PROMOTION,
  TBL_REASON,
  TBL_SHIP_MODE,
  TBL_STORE,
  TBL_STORE_RETURNS,
  TBL_STORE_SALES,
  TBL_TIME_DIM,
  TBL_WAREHOUSE,
  TBL_WEB_PAGE,
  TBL_WEB_RETURNS,
  TBL_WEB_SALES,
  TBL_WEB_SITE
};

static const auto tables = {
    tpcds::Table::TBL_CALL_CENTER,
    tpcds::Table::TBL_CATALOG_PAGE,
    tpcds::Table::TBL_CATALOG_RETURNS,
    tpcds::Table::TBL_CATALOG_SALES,
    tpcds::Table::TBL_CUSTOMER,
    tpcds::Table::TBL_CUSTOMER_ADDRESS,
    tpcds::Table::TBL_CUSTOMER_DEMOGRAPHICS,
    tpcds::Table::TBL_DATE_DIM,
    tpcds::Table::TBL_HOUSEHOLD_DEMOGRAPHICS,
    tpcds::Table::TBL_INCOME_BAND,
    tpcds::Table::TBL_INVENTORY,
    tpcds::Table::TBL_ITEM,
    tpcds::Table::TBL_PROMOTION,
    tpcds::Table::TBL_REASON,
    tpcds::Table::TBL_SHIP_MODE,
    tpcds::Table::TBL_STORE,
    tpcds::Table::TBL_STORE_RETURNS,
    tpcds::Table::TBL_STORE_SALES,
    tpcds::Table::TBL_TIME_DIM,
    tpcds::Table::TBL_WAREHOUSE,
    tpcds::Table::TBL_WEB_PAGE,
    tpcds::Table::TBL_WEB_RETURNS,
    tpcds::Table::TBL_WEB_SALES,
    tpcds::Table::TBL_WEB_SITE};

/// Returns table name as a string.
std::string_view toTableName(Table table);

Table fromTableName(const std::string_view& tableName);

/// Returns the schema (RowType) for a particular TPC-DS table.
const velox::RowTypePtr getTableSchema(Table table);

velox::RowVectorPtr genTpcdsData(
    Table table,
    size_t maxRows,
    size_t offset,
    velox::memory::MemoryPool* pool,
    double scaleFactor,
    int32_t parallel,
    int32_t child);
} // namespace facebook::presto::connector::tpcds
