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
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/type/tests/SubfieldFiltersBuilder.h"

namespace facebook::velox::exec::test {

static int64_t toDate(std::string_view stringDate) {
  Date date;
  parseTo(stringDate, date);
  return date.days();
}

void TpchQueryBuilder::initialize(const std::string& dataPath) {
  for (const auto& [tableName, columns] : kTables_) {
    const fs::path tablePath{dataPath + "/" + tableName};
    for (auto const& dirEntry : fs::directory_iterator{tablePath}) {
      if (!dirEntry.is_regular_file()) {
        continue;
      }
      // Ignore hidden files.
      if (dirEntry.path().filename().c_str()[0] == '.') {
        continue;
      }
      if (tableMetadata_[tableName].dataFiles.empty()) {
        dwio::common::ReaderOptions readerOptions;
        readerOptions.setFileFormat(format_);
        std::unique_ptr<dwio::common::Reader> reader =
            dwio::common::getReaderFactory(readerOptions.getFileFormat())
                ->createReader(
                    std::make_unique<dwio::common::FileInputStream>(
                        dirEntry.path()),
                    readerOptions);
        tableMetadata_[tableName].type = reader->rowType();
        const auto aliases = reader->rowType()->names();
        // There can be extra columns in the file towards the end.
        VELOX_CHECK_GE(aliases.size(), columns.size());
        std::unordered_map<std::string, std::string> aliasMap(columns.size());
        std::transform(
            columns.begin(),
            columns.end(),
            aliases.begin(),
            std::inserter(aliasMap, aliasMap.begin()),
            [](std::string a, std::string b) { return std::make_pair(a, b); });
        tableMetadata_[tableName].columnAliases = std::move(aliasMap);
      }
      tableMetadata_[tableName].dataFiles.push_back(dirEntry.path());
    }
  }
}

const std::vector<std::string>& TpchQueryBuilder::getTableNames() {
  return kTableNames_;
}

TpchPlan TpchQueryBuilder::getQueryPlan(int queryId) const {
  switch (queryId) {
    case 1:
      return getQ1Plan();
    case 6:
      return getQ6Plan();
    default:
      VELOX_NYI("TPC-H query {} is not supported yet", queryId);
  }
}

namespace {
using ColumnHandleMap =
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>;

std::shared_ptr<connector::hive::HiveTableHandle> makeTableHandle(
    common::test::SubfieldFilters subfieldFilters,
    const std::shared_ptr<const core::ITypedExpr>& remainingFilter = nullptr) {
  return std::make_shared<connector::hive::HiveTableHandle>(
      true, std::move(subfieldFilters), remainingFilter);
}

std::shared_ptr<connector::hive::HiveColumnHandle> regularColumn(
    const std::string& name,
    const TypePtr& type) {
  return std::make_shared<connector::hive::HiveColumnHandle>(
      name, connector::hive::HiveColumnHandle::ColumnType::kRegular, type);
}

ColumnHandleMap allRegularColumns(
    const std::shared_ptr<const RowType>& rowType) {
  ColumnHandleMap assignments;
  assignments.reserve(rowType->size());
  for (uint32_t i = 0; i < rowType->size(); ++i) {
    const auto& name = rowType->nameOf(i);
    assignments[name] = regularColumn(name, rowType->childAt(i));
  }
  return assignments;
}
} // namespace

TpchPlan TpchQueryBuilder::getQ1Plan() const {
  static const std::string kTableName = "lineitem";
  std::vector<std::string> selectedColumns = {
      "l_returnflag",
      "l_linestatus",
      "l_quantity",
      "l_extendedprice",
      "l_discount",
      "l_tax",
      "l_shipdate"};

  // Get the corresponding selected column names in the file.
  const auto aliasSelectedColumns =
      getColumnAliases(kTableName, selectedColumns);

  auto columnSelector =
      std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
          tableMetadata_.at(kTableName).type, aliasSelectedColumns);
  auto selectedRowType = columnSelector->buildSelectedReordered();
  // Add an extra project step to map file columns to standard
  // column names.
  const auto projectAlias =
      getProjectColumnAliases(kTableName, selectedColumns);

  // shipdate <= '1998-09-02'
  auto shipDate = getColumnAlias(kTableName, "l_shipdate");
  common::test::SubfieldFiltersBuilder filtersBuilder;
  // DWRF does not support Date type. Use Varchar instead.
  if (selectedRowType->findChild(shipDate)->isVarchar()) {
    filtersBuilder.add(shipDate, common::test::lessThanOrEqual("1998-09-02"));
  } else {
    filtersBuilder.add(
        shipDate, common::test::lessThanOrEqual(toDate("1998-09-02")));
  }
  auto filters = filtersBuilder.build();
  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId lineitemPlanNodeId;

  const auto partialAggStage =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              selectedRowType,
              makeTableHandle(std::move(filters)),
              allRegularColumns(selectedRowType))
          .capturePlanNodeId(lineitemPlanNodeId)
          .project(projectAlias)
          .project(
              {"l_returnflag",
               "l_linestatus",
               "l_quantity",
               "l_extendedprice",
               "l_extendedprice * (1.0 - l_discount) AS l_sum_disc_price",
               "l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax) AS l_sum_charge",
               "l_discount"})
          .partialAggregation(
              {0, 1},
              {"sum(l_quantity)",
               "sum(l_extendedprice)",
               "sum(l_sum_disc_price)",
               "sum(l_sum_charge)",
               "avg(l_quantity)",
               "avg(l_extendedprice)",
               "avg(l_discount)",
               "count(0)"})
          .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator)
                  .localPartition({}, {partialAggStage})
                  .finalAggregation(
                      {0, 1},
                      {"sum(a0)",
                       "sum(a1)",
                       "sum(a2)",
                       "sum(a3)",
                       "avg(a4)",
                       "avg(a5)",
                       "avg(a6)",
                       "count(a7)"},
                      {DOUBLE(),
                       DOUBLE(),
                       DOUBLE(),
                       DOUBLE(),
                       DOUBLE(),
                       DOUBLE(),
                       DOUBLE(),
                       BIGINT()})
                  .orderBy({"l_returnflag", "l_linestatus"}, false)
                  .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kTableName);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ6Plan() const {
  static const std::string kTableName = "lineitem";
  std::vector<std::string> selectedColumns = {
      "l_shipdate", "l_extendedprice", "l_quantity", "l_discount"};

  // Get the corresponding selected column names in the file.
  const auto aliasSelectedColumns =
      getColumnAliases(kTableName, selectedColumns);

  auto columnSelector =
      std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
          tableMetadata_.at(kTableName).type, aliasSelectedColumns);
  auto selectedRowType = columnSelector->buildSelectedReordered();
  // Add an extra project step to map file columns to standard
  // column names.
  const auto projectAlias =
      getProjectColumnAliases(kTableName, selectedColumns);

  auto shipDate = getColumnAlias(kTableName, "l_shipdate");
  common::test::SubfieldFiltersBuilder filtersBuilder;
  // DWRF does not support Date type. Use Varchar instead.
  if (selectedRowType->findChild(shipDate)->isVarchar()) {
    filtersBuilder.add(
        shipDate, common::test::between("1994-01-01", "1994-12-31"));
  } else {
    filtersBuilder.add(
        shipDate,
        common::test::between(toDate("1994-01-01"), toDate("1994-12-31")));
  }
  auto filters = filtersBuilder
                     .add(
                         getColumnAlias(kTableName, "l_discount"),
                         common::test::betweenDouble(0.05, 0.07))
                     .add(
                         getColumnAlias(kTableName, "l_quantity"),
                         common::test::lessThanDouble(24.0))
                     .build();

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId lineitemPlanNodeId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .localPartition(
                      {},
                      {PlanBuilder(planNodeIdGenerator)
                           .tableScan(
                               selectedRowType,
                               makeTableHandle(std::move(filters)),
                               allRegularColumns(selectedRowType))
                           .capturePlanNodeId(lineitemPlanNodeId)
                           .project(projectAlias)
                           .project({"l_extendedprice * l_discount"})
                           .partialAggregation({}, {"sum(p0)"})
                           .planNode()})
                  .finalAggregation({}, {"sum(a0)"}, {DOUBLE()})
                  .planNode();
  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kTableName);
  context.dataFileFormat = format_;
  return context;
}

const std::vector<std::string> TpchQueryBuilder::kTableNames_ = {"lineitem"};

const std::unordered_map<std::string, std::vector<std::string>>
    TpchQueryBuilder::kTables_ = {std::make_pair(
        "lineitem",
        std::vector<std::string>{
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
            "l_commitdate",
            "l_receiptdate",
            "l_shipinstruct",
            "l_shipmode",
            "l_comment"})};

} // namespace facebook::velox::exec::test
