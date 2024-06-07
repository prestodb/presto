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

#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/tpch/gen/TpchGen.h"

#include <fstream>

namespace facebook::velox::exec::test {

namespace {

/// DWRF does not support Date type and Varchar is used.
/// Return the Date filter expression as per data format.
std::string formatDateFilter(
    const std::string& stringDate,
    const RowTypePtr& rowType,
    const std::string& lowerBound,
    const std::string& upperBound) {
  bool isDwrf = rowType->findChild(stringDate)->isVarchar();
  auto suffix = isDwrf ? "" : "::DATE";

  if (!lowerBound.empty() && !upperBound.empty()) {
    return fmt::format(
        "{} between {}{} and {}{}",
        stringDate,
        lowerBound,
        suffix,
        upperBound,
        suffix);
  } else if (!lowerBound.empty()) {
    return fmt::format("{} > {}{}", stringDate, lowerBound, suffix);
  } else if (!upperBound.empty()) {
    return fmt::format("{} < {}{}", stringDate, upperBound, suffix);
  }

  VELOX_FAIL(
      "Date range check expression must have either a lower or an upper bound");
}

std::vector<std::string> mergeColumnNames(
    const std::vector<std::string>& firstColumnVector,
    const std::vector<std::string>& secondColumnVector) {
  std::vector<std::string> mergedColumnVector = std::move(firstColumnVector);
  mergedColumnVector.insert(
      mergedColumnVector.end(),
      secondColumnVector.begin(),
      secondColumnVector.end());
  return mergedColumnVector;
};
} // namespace

void TpchQueryBuilder::readFileSchema(
    const std::string& tableName,
    const std::string& filePath,
    const std::vector<std::string>& columns) {
  dwio::common::ReaderOptions readerOptions{pool_.get()};
  readerOptions.setFileFormat(format_);
  auto uniqueReadFile =
      filesystems::getFileSystem(filePath, nullptr)->openFileForRead(filePath);
  std::shared_ptr<ReadFile> readFile;
  readFile.reset(uniqueReadFile.release());
  auto input = std::make_unique<dwio::common::BufferedInput>(
      readFile, readerOptions.memoryPool());
  std::unique_ptr<dwio::common::Reader> reader =
      dwio::common::getReaderFactory(readerOptions.fileFormat())
          ->createReader(std::move(input), readerOptions);
  const auto fileType = reader->rowType();
  const auto fileColumnNames = fileType->names();
  // There can be extra columns in the file towards the end.
  VELOX_CHECK_GE(fileColumnNames.size(), columns.size());
  std::unordered_map<std::string, std::string> fileColumnNamesMap(
      columns.size());
  std::transform(
      columns.begin(),
      columns.end(),
      fileColumnNames.begin(),
      std::inserter(fileColumnNamesMap, fileColumnNamesMap.begin()),
      [](std::string a, std::string b) { return std::make_pair(a, b); });
  auto columnNames = columns;
  auto types = fileType->children();
  types.resize(columnNames.size());
  tableMetadata_[tableName].type =
      std::make_shared<RowType>(std::move(columnNames), std::move(types));
  tableMetadata_[tableName].fileColumnNames = std::move(fileColumnNamesMap);
}

void TpchQueryBuilder::initialize(const std::string& dataPath) {
  for (const auto& [tableName, columns] : kTables_) {
    const fs::path tablePath{dataPath + "/" + tableName};
    std::error_code error;
    bool anyFound = false;
    for (auto const& dirEntry : fs::directory_iterator{
             tablePath, std::filesystem::directory_options(), error}) {
      if (!dirEntry.is_regular_file()) {
        continue;
      }
      // Ignore hidden files.
      if (dirEntry.path().filename().c_str()[0] == '.') {
        continue;
      }
      if (tableMetadata_[tableName].dataFiles.empty()) {
        anyFound = true;
        readFileSchema(tableName, dirEntry.path().string(), columns);
      }
      tableMetadata_[tableName].dataFiles.push_back(dirEntry.path());
    }
    if (!anyFound && error) {
      std::ifstream file(tablePath);
      std::string line;
      while (std::getline(file, line)) {
        if (tableMetadata_[tableName].dataFiles.empty()) {
          readFileSchema(tableName, line, columns);
        }
        tableMetadata_[tableName].dataFiles.push_back(line);
      }
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
    case 2:
      return getQ2Plan();
    case 3:
      return getQ3Plan();
    case 5:
      return getQ5Plan();
    case 6:
      return getQ6Plan();
    case 7:
      return getQ7Plan();
    case 8:
      return getQ8Plan();
    case 9:
      return getQ9Plan();
    case 10:
      return getQ10Plan();
    case 11:
      return getQ11Plan();
    case 12:
      return getQ12Plan();
    case 13:
      return getQ13Plan();
    case 14:
      return getQ14Plan();
    case 15:
      return getQ15Plan();
    case 16:
      return getQ16Plan();
    case 17:
      return getQ17Plan();
    case 18:
      return getQ18Plan();
    case 19:
      return getQ19Plan();
    case 20:
      return getQ20Plan();
    case 21:
      return getQ21Plan();
    case 22:
      return getQ22Plan();
    default:
      VELOX_NYI("TPC-H query {} is not supported yet", queryId);
  }
}

TpchPlan TpchQueryBuilder::getQ1Plan() const {
  std::vector<std::string> selectedColumns = {
      "l_returnflag",
      "l_linestatus",
      "l_quantity",
      "l_extendedprice",
      "l_discount",
      "l_tax",
      "l_shipdate"};

  const auto selectedRowType = getRowType(kLineitem, selectedColumns);
  const auto& fileColumnNames = getFileColumnNames(kLineitem);

  // shipdate <= '1998-09-02'
  const auto shipDate = "l_shipdate";
  auto filter = formatDateFilter(shipDate, selectedRowType, "", "'1998-09-03'");

  core::PlanNodeId lineitemPlanNodeId;

  auto plan =
      PlanBuilder(pool_.get())
          .tableScan(kLineitem, selectedRowType, fileColumnNames, {filter})
          .capturePlanNodeId(lineitemPlanNodeId)
          .project(
              {"l_returnflag",
               "l_linestatus",
               "l_quantity",
               "l_extendedprice",
               "l_extendedprice * (1.0 - l_discount) AS l_sum_disc_price",
               "l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax) AS l_sum_charge",
               "l_discount"})
          .partialAggregation(
              {"l_returnflag", "l_linestatus"},
              {"sum(l_quantity)",
               "sum(l_extendedprice)",
               "sum(l_sum_disc_price)",
               "sum(l_sum_charge)",
               "avg(l_quantity)",
               "avg(l_extendedprice)",
               "avg(l_discount)",
               "count(0)"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"l_returnflag", "l_linestatus"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ2Plan() const {
  std::vector<std::string> supplierColumnsSubQuery = {
      "s_suppkey", "s_nationkey"};
  std::vector<std::string> nationColumnsSubQuery = {
      "n_nationkey", "n_regionkey"};
  std::vector<std::string> supplierColumns = {
      "s_acctbal",
      "s_name",
      "s_address",
      "s_phone",
      "s_comment",
      "s_suppkey",
      "s_nationkey"};
  std::vector<std::string> partColumns = {
      "p_partkey", "p_mfgr", "p_size", "p_type"};
  std::vector<std::string> partsuppColumns = {
      "ps_partkey", "ps_suppkey", "ps_supplycost"};
  std::vector<std::string> nationColumns = {
      "n_nationkey", "n_name", "n_regionkey"};
  std::vector<std::string> regionColumns = {"r_regionkey", "r_name"};

  auto supplierSelectedRowTypeSubQuery =
      getRowType(kSupplier, supplierColumnsSubQuery);
  const auto& supplierFileColumnsSubQuery = getFileColumnNames(kSupplier);
  auto nationSelectedRowTypeSubQuery =
      getRowType(kNation, nationColumnsSubQuery);
  const auto& nationFileColumnsSubQuery = getFileColumnNames(kNation);
  auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);
  auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  auto partsuppSelectedRowType = getRowType(kPartsupp, partsuppColumns);
  const auto& partsuppFileColumns = getFileColumnNames(kPartsupp);
  auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);
  auto regionSelectedRowType = getRowType(kRegion, regionColumns);
  const auto& regionFileColumns = getFileColumnNames(kRegion);

  const std::string regionNameFilter = "r_name = 'EUROPE'";

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId supplierScanIdSubQuery;
  core::PlanNodeId partsuppScanIdSubQuery;
  core::PlanNodeId nationScanIdSubQuery;
  core::PlanNodeId regionScanIdSubQuery;
  core::PlanNodeId partScanId;
  core::PlanNodeId supplierScanId;
  core::PlanNodeId partsuppScanId;
  core::PlanNodeId nationScanId;
  core::PlanNodeId regionScanId;

  auto regionSubQuery = PlanBuilder(planNodeIdGenerator)
                            .tableScan(
                                kRegion,
                                regionSelectedRowType,
                                regionFileColumns,
                                {regionNameFilter})
                            .capturePlanNodeId(regionScanIdSubQuery)
                            .planNode();

  auto nationJoinRegionSubQuery =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kNation, nationSelectedRowTypeSubQuery, nationFileColumnsSubQuery)
          .capturePlanNodeId(nationScanIdSubQuery)
          .hashJoin(
              {"n_regionkey"},
              {"r_regionkey"},
              regionSubQuery,
              "",
              {"n_nationkey"})
          .planNode();

  auto supplierJoinNationJoinRegionSubQuery =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kSupplier,
              supplierSelectedRowTypeSubQuery,
              supplierFileColumnsSubQuery)
          .capturePlanNodeId(supplierScanIdSubQuery)
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nationJoinRegionSubQuery,
              "",
              {"s_suppkey"})
          .planNode();

  auto part = PlanBuilder(planNodeIdGenerator)
                  .tableScan(
                      kPart,
                      partSelectedRowType,
                      partFileColumns,
                      {},
                      "p_type like '%BRASS'")
                  .capturePlanNodeId(partScanId)
                  .filter("p_size = 15")
                  .planNode();

  auto region = PlanBuilder(planNodeIdGenerator)
                    .tableScan(
                        kRegion,
                        regionSelectedRowType,
                        regionFileColumns,
                        {regionNameFilter})
                    .capturePlanNodeId(regionScanId)
                    .planNode();

  auto nationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .capturePlanNodeId(nationScanId)
          .hashJoin(
              {"n_regionkey"},
              {"r_regionkey"},
              region,
              "",
              {"n_nationkey", "n_name"})
          .planNode();

  auto supplierJoinNationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanId)
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nationJoinRegion,
              "",
              mergeColumnNames(supplierColumns, {"s_suppkey", "n_name"}))
          .planNode();

  auto partsuppJoinPartJoinSupplierJoinNationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kPartsupp, partsuppSelectedRowType, partsuppFileColumns)
          .capturePlanNodeId(partsuppScanId)
          .hashJoin(
              {"ps_partkey"},
              {"p_partkey"},
              part,
              "",
              {"ps_suppkey", "ps_supplycost", "p_partkey", "p_mfgr"})
          .hashJoin(
              {"ps_suppkey"},
              {"s_suppkey"},
              supplierJoinNationJoinRegion,
              "",
              mergeColumnNames(
                  supplierColumns,
                  {"ps_supplycost", "p_partkey", "p_mfgr", "n_name"}))
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kPartsupp, partsuppSelectedRowType, partsuppFileColumns)
          .capturePlanNodeId(partsuppScanIdSubQuery)
          .hashJoin(
              {"ps_suppkey"},
              {"s_suppkey"},
              supplierJoinNationJoinRegionSubQuery,
              "",
              {"ps_supplycost", "ps_partkey"})
          .partialAggregation(
              {"ps_partkey"}, {"min(ps_supplycost) AS min_supplycost"})
          .localPartition({"ps_partkey"})
          .finalAggregation()
          .hashJoin(
              {"ps_partkey"},
              {"p_partkey"},
              partsuppJoinPartJoinSupplierJoinNationJoinRegion,
              "ps_supplycost = min_supplycost",
              mergeColumnNames(
                  supplierColumns, {"p_partkey", "p_mfgr", "n_name"}))
          .orderBy({"s_acctbal DESC", "n_name", "s_name", "p_partkey"}, false)
          .project(
              {"s_acctbal",
               "s_name",
               "n_name",
               "p_partkey",
               "p_mfgr",
               "s_address",
               "s_phone",
               "s_comment"})
          .limit(0, 100, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[supplierScanIdSubQuery] = getTableFilePaths(kSupplier);
  context.dataFiles[partsuppScanIdSubQuery] = getTableFilePaths(kPartsupp);
  context.dataFiles[nationScanIdSubQuery] = getTableFilePaths(kNation);
  context.dataFiles[regionScanIdSubQuery] = getTableFilePaths(kRegion);
  context.dataFiles[partScanId] = getTableFilePaths(kPart);
  context.dataFiles[supplierScanId] = getTableFilePaths(kSupplier);
  context.dataFiles[partsuppScanId] = getTableFilePaths(kPartsupp);
  context.dataFiles[nationScanId] = getTableFilePaths(kNation);
  context.dataFiles[regionScanId] = getTableFilePaths(kRegion);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ3Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_shipdate", "l_orderkey", "l_extendedprice", "l_discount"};
  std::vector<std::string> ordersColumns = {
      "o_orderdate", "o_shippriority", "o_custkey", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey", "c_mktsegment"};

  const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);

  const auto orderDate = "o_orderdate";
  const auto shipDate = "l_shipdate";
  auto orderDateFilter =
      formatDateFilter(orderDate, ordersSelectedRowType, "", "'1995-03-15'");
  auto shipDateFilter =
      formatDateFilter(shipDate, lineitemSelectedRowType, "'1995-03-15'", "");
  auto customerFilter = "c_mktsegment = 'BUILDING'";

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemPlanNodeId;
  core::PlanNodeId ordersPlanNodeId;
  core::PlanNodeId customerPlanNodeId;

  auto customers = PlanBuilder(planNodeIdGenerator, pool_.get())
                       .tableScan(
                           kCustomer,
                           customerSelectedRowType,
                           customerFileColumns,
                           {customerFilter})
                       .capturePlanNodeId(customerPlanNodeId)
                       .planNode();

  auto custkeyJoinNode =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kOrders,
              ordersSelectedRowType,
              ordersFileColumns,
              {orderDateFilter})
          .capturePlanNodeId(ordersPlanNodeId)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              customers,
              "",
              {"o_orderdate", "o_shippriority", "o_orderkey"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kLineitem,
              lineitemSelectedRowType,
              lineitemFileColumns,
              {shipDateFilter})
          .capturePlanNodeId(lineitemPlanNodeId)
          .project(
              {"l_extendedprice * (1.0 - l_discount) AS part_revenue",
               "l_orderkey"})
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              custkeyJoinNode,
              "",
              {"l_orderkey", "o_orderdate", "o_shippriority", "part_revenue"})
          .partialAggregation(
              {"l_orderkey", "o_orderdate", "o_shippriority"},
              {"sum(part_revenue) as revenue"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .project({"l_orderkey", "revenue", "o_orderdate", "o_shippriority"})
          .orderBy({"revenue DESC", "o_orderdate"}, false)
          .limit(0, 10, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[ordersPlanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerPlanNodeId] = getTableFilePaths(kCustomer);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ5Plan() const {
  std::vector<std::string> customerColumns = {"c_custkey", "c_nationkey"};
  std::vector<std::string> ordersColumns = {
      "o_orderdate", "o_custkey", "o_orderkey"};
  std::vector<std::string> lineitemColumns = {
      "l_suppkey", "l_orderkey", "l_discount", "l_extendedprice"};
  std::vector<std::string> supplierColumns = {"s_nationkey", "s_suppkey"};
  std::vector<std::string> nationColumns = {
      "n_nationkey", "n_name", "n_regionkey"};
  std::vector<std::string> regionColumns = {"r_regionkey", "r_name"};

  auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);
  auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);
  auto regionSelectedRowType = getRowType(kRegion, regionColumns);
  const auto& regionFileColumns = getFileColumnNames(kRegion);

  std::string regionNameFilter = "r_name = 'ASIA'";
  const auto orderDate = "o_orderdate";
  std::string orderDateFilter = formatDateFilter(
      orderDate, ordersSelectedRowType, "'1994-01-01'", "'1994-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId regionScanNodeId;

  auto region = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(
                        kRegion,
                        regionSelectedRowType,
                        regionFileColumns,
                        {regionNameFilter})
                    .capturePlanNodeId(regionScanNodeId)
                    .planNode();

  auto orders = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(
                        kOrders,
                        ordersSelectedRowType,
                        ordersFileColumns,
                        {orderDateFilter})
                    .capturePlanNodeId(ordersScanNodeId)
                    .planNode();

  auto customer =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .planNode();

  auto nationJoinRegion =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .capturePlanNodeId(nationScanNodeId)
          .hashJoin(
              {"n_regionkey"},
              {"r_regionkey"},
              region,
              "",
              {"n_nationkey", "n_name"})
          .planNode();

  auto supplierJoinNationRegion =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nationJoinRegion,
              "",
              {"s_suppkey", "n_name", "s_nationkey"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemScanNodeId)
          .project(
              {"l_extendedprice * (1.0 - l_discount) AS part_revenue",
               "l_orderkey",
               "l_suppkey"})
          .hashJoin(
              {"l_suppkey"},
              {"s_suppkey"},
              supplierJoinNationRegion,
              "",
              {"n_name", "part_revenue", "s_nationkey", "l_orderkey"})
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              orders,
              "",
              {"n_name", "part_revenue", "s_nationkey", "o_custkey"})
          .hashJoin(
              {"s_nationkey", "o_custkey"},
              {"c_nationkey", "c_custkey"},
              customer,
              "",
              {"n_name", "part_revenue"})
          .partialAggregation({"n_name"}, {"sum(part_revenue) as revenue"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"revenue DESC"}, false)
          .project({"n_name", "revenue"})
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[nationScanNodeId] = getTableFilePaths(kNation);
  context.dataFiles[regionScanNodeId] = getTableFilePaths(kRegion);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ6Plan() const {
  std::vector<std::string> selectedColumns = {
      "l_shipdate", "l_extendedprice", "l_quantity", "l_discount"};

  const auto selectedRowType = getRowType(kLineitem, selectedColumns);
  const auto& fileColumnNames = getFileColumnNames(kLineitem);

  const auto shipDate = "l_shipdate";
  auto shipDateFilter = formatDateFilter(
      shipDate, selectedRowType, "'1994-01-01'", "'1994-12-31'");

  core::PlanNodeId lineitemPlanNodeId;
  auto plan = PlanBuilder(pool_.get())
                  .tableScan(
                      kLineitem,
                      selectedRowType,
                      fileColumnNames,
                      {shipDateFilter,
                       "l_discount between 0.05 and 0.07",
                       "l_quantity < 24.0"})
                  .capturePlanNodeId(lineitemPlanNodeId)
                  .project({"l_extendedprice * l_discount"})
                  .partialAggregation({}, {"sum(p0)"})
                  .localPartition(std::vector<std::string>{})
                  .finalAggregation()
                  .planNode();
  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ7Plan() const {
  std::vector<std::string> supplierColumns = {"s_nationkey", "s_suppkey"};
  std::vector<std::string> lineitemColumns = {
      "l_shipdate", "l_suppkey", "l_orderkey", "l_discount", "l_extendedprice"};
  std::vector<std::string> ordersColumns = {"o_custkey", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey", "c_nationkey"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};

  auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);
  auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);

  const std::string nationFilter = "n_name IN ('FRANCE', 'GERMANY')";
  auto shipDateFilter = formatDateFilter(
      "l_shipdate", lineitemSelectedRowType, "'1995-01-01'", "'1996-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId suppNationScanNodeId;
  core::PlanNodeId custNationScanNodeId;

  auto custNation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kNation, nationSelectedRowType, nationFileColumns, {nationFilter})
          .capturePlanNodeId(custNationScanNodeId)
          .planNode();

  auto customerJoinNation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .hashJoin(
              {"c_nationkey"},
              {"n_nationkey"},
              custNation,
              "",
              {"n_name", "c_custkey"})
          .project({"n_name as cust_nation", "c_custkey"})
          .planNode();

  auto ordersJoinCustomer =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              customerJoinNation,
              "",
              {"cust_nation", "o_orderkey"})
          .planNode();

  auto suppNation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kNation, nationSelectedRowType, nationFileColumns, {nationFilter})
          .capturePlanNodeId(suppNationScanNodeId)
          .planNode();

  auto supplierJoinNation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              suppNation,
              "",
              {"n_name", "s_suppkey"})
          .project({"n_name as supp_nation", "s_suppkey"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kLineitem,
              lineitemSelectedRowType,
              lineitemFileColumns,
              {shipDateFilter})
          .capturePlanNodeId(lineitemScanNodeId)
          .hashJoin(
              {"l_suppkey"},
              {"s_suppkey"},
              supplierJoinNation,
              "",
              {"supp_nation",
               "l_extendedprice",
               "l_discount",
               "l_shipdate",
               "l_orderkey"})
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              ordersJoinCustomer,
              "(((cust_nation = 'FRANCE') AND (supp_nation = 'GERMANY')) OR "
              "((cust_nation = 'GERMANY') AND (supp_nation = 'FRANCE')))",
              {"supp_nation",
               "cust_nation",
               "l_extendedprice",
               "l_discount",
               "l_shipdate"})
          .project(
              {"cust_nation",
               "supp_nation",
               "l_extendedprice * (1.0 - l_discount) as part_revenue",
               "year(l_shipdate) as l_year"})
          .partialAggregation(
              {"supp_nation", "cust_nation", "l_year"},
              {"sum(part_revenue) as revenue"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"supp_nation", "cust_nation", "l_year"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[suppNationScanNodeId] = getTableFilePaths(kNation);
  context.dataFiles[custNationScanNodeId] = getTableFilePaths(kNation);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ8Plan() const {
  std::vector<std::string> partColumns = {"p_partkey", "p_type"};
  std::vector<std::string> supplierColumns = {"s_suppkey", "s_nationkey"};
  std::vector<std::string> lineitemColumns = {
      "l_suppkey", "l_orderkey", "l_partkey", "l_extendedprice", "l_discount"};
  std::vector<std::string> ordersColumns = {
      "o_orderdate", "o_orderkey", "o_custkey"};
  std::vector<std::string> customerColumns = {"c_nationkey", "c_custkey"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_regionkey"};
  std::vector<std::string> nationColumnsWithName = {
      "n_name", "n_nationkey", "n_regionkey"};
  std::vector<std::string> regionColumns = {"r_name", "r_regionkey"};

  auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);
  auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);
  const auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);
  const auto nationSelectedRowTypeWithName =
      getRowType(kNation, nationColumnsWithName);
  const auto& nationFileColumnsWithName = getFileColumnNames(kNation);
  const auto regionSelectedRowType = getRowType(kRegion, regionColumns);
  const auto& regionFileColumns = getFileColumnNames(kRegion);

  const auto orderDateFilter = formatDateFilter(
      "o_orderdate", ordersSelectedRowType, "'1995-01-01'", "'1996-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId partScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId nationScanNodeIdWithName;
  core::PlanNodeId regionScanNodeId;

  auto nationWithName =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kNation, nationSelectedRowTypeWithName, nationFileColumnsWithName)
          .capturePlanNodeId(nationScanNodeIdWithName)
          .planNode();

  auto region = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(
                        kRegion,
                        regionSelectedRowType,
                        regionFileColumns,
                        {"r_name = 'AMERICA'"})
                    .capturePlanNodeId(regionScanNodeId)
                    .planNode();

  auto part = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(
                      kPart,
                      partSelectedRowType,
                      partFileColumns,
                      {"p_type = 'ECONOMY ANODIZED STEEL'"})
                  .capturePlanNodeId(partScanNodeId)
                  .planNode();

  auto nationJoinRegion =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .capturePlanNodeId(nationScanNodeId)
          .hashJoin(
              {"n_regionkey"}, {"r_regionkey"}, region, "", {"n_nationkey"})
          .planNode();

  auto customerJoinNationJoinRegion =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .hashJoin(
              {"c_nationkey"},
              {"n_nationkey"},
              nationJoinRegion,
              "",
              {"c_custkey"})
          .planNode();

  auto ordersJoinCustomerJoinNationJoinRegion =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kOrders,
              ordersSelectedRowType,
              ordersFileColumns,
              {orderDateFilter})
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              customerJoinNationJoinRegion,
              "",
              {"o_orderkey", "o_orderdate"})
          .planNode();

  auto supplierJoinNation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nationWithName,
              "",
              {"s_suppkey", "n_name"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemScanNodeId)
          .hashJoin(
              {"l_orderkey"},
              {"o_orderkey"},
              ordersJoinCustomerJoinNationJoinRegion,
              "",
              {"l_partkey",
               "l_suppkey",
               "o_orderdate",
               "l_extendedprice",
               "l_discount"})
          .hashJoin(
              {"l_suppkey"},
              {"s_suppkey"},
              supplierJoinNation,
              "",
              {"n_name",
               "o_orderdate",
               "l_partkey",
               "l_extendedprice",
               "l_discount"})
          .hashJoin(
              {"l_partkey"},
              {"p_partkey"},
              part,
              "",
              {"n_name", "o_orderdate", "l_extendedprice", "l_discount"})
          .project(
              {"l_extendedprice * (1.0 - l_discount) as volume",
               "n_name",
               "o_orderdate"})
          .project(
              {"volume",
               "(CASE WHEN n_name = 'BRAZIL' THEN volume ELSE 0.0 END) as brazil_volume",
               "year(o_orderdate) AS o_year"})
          .partialAggregation(
              {"o_year"},
              {"sum(brazil_volume) as volume_brazil",
               "sum(volume) as volume_all"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"o_year"}, false)
          .project({"o_year", "(volume_brazil / volume_all) as mkt_share"})
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFiles[nationScanNodeId] = getTableFilePaths(kNation);
  context.dataFiles[nationScanNodeIdWithName] = getTableFilePaths(kNation);
  context.dataFiles[regionScanNodeId] = getTableFilePaths(kRegion);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ9Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_suppkey",
      "l_partkey",
      "l_discount",
      "l_extendedprice",
      "l_orderkey",
      "l_quantity"};
  std::vector<std::string> partColumns = {"p_name", "p_partkey"};
  std::vector<std::string> supplierColumns = {"s_suppkey", "s_nationkey"};
  std::vector<std::string> partsuppColumns = {
      "ps_partkey", "ps_suppkey", "ps_supplycost"};
  std::vector<std::string> ordersColumns = {"o_orderkey", "o_orderdate"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};

  auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);
  auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto partsuppSelectedRowType = getRowType(kPartsupp, partsuppColumns);
  const auto& partsuppFileColumns = getFileColumnNames(kPartsupp);
  auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId partScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId partsuppScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId nationScanNodeId;

  const std::vector<std::string> lineitemCommonColumns = {
      "l_extendedprice", "l_discount", "l_quantity"};

  auto part = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(
                      kPart,
                      partSelectedRowType,
                      partFileColumns,
                      {},
                      "p_name like '%green%'")
                  .capturePlanNodeId(partScanNodeId)
                  .planNode();

  auto supplier =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanNodeId)
          .planNode();

  auto nation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .capturePlanNodeId(nationScanNodeId)
          .planNode();

  auto lineitemJoinPartJoinSupplier =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemScanNodeId)
          .hashJoin({"l_partkey"}, {"p_partkey"}, part, "", lineitemColumns)
          .hashJoin(
              {"l_suppkey"},
              {"s_suppkey"},
              supplier,
              "",
              mergeColumnNames(lineitemColumns, {"s_nationkey"}))
          .planNode();

  auto partsuppJoinLineitemJoinPartJoinSupplier =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kPartsupp, partsuppSelectedRowType, partsuppFileColumns)
          .capturePlanNodeId(partsuppScanNodeId)
          .hashJoin(
              {"ps_partkey", "ps_suppkey"},
              {"l_partkey", "l_suppkey"},
              lineitemJoinPartJoinSupplier,
              "",
              mergeColumnNames(
                  lineitemCommonColumns,
                  {"l_orderkey", "s_nationkey", "ps_supplycost"}))
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_orderkey"},
              {"l_orderkey"},
              partsuppJoinLineitemJoinPartJoinSupplier,
              "",
              mergeColumnNames(
                  lineitemCommonColumns,
                  {"s_nationkey", "ps_supplycost", "o_orderdate"}))
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nation,
              "",
              mergeColumnNames(
                  lineitemCommonColumns,
                  {"ps_supplycost", "o_orderdate", "n_name"}))
          .project(
              {"n_name AS nation",
               "year(o_orderdate) AS o_year",
               "l_extendedprice * (1.0 - l_discount) - ps_supplycost * l_quantity AS amount"})
          .partialAggregation(
              {"nation", "o_year"}, {"sum(amount) AS sum_profit"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"nation", "o_year DESC"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[partsuppScanNodeId] = getTableFilePaths(kPartsupp);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[nationScanNodeId] = getTableFilePaths(kNation);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ10Plan() const {
  std::vector<std::string> customerColumns = {
      "c_nationkey",
      "c_custkey",
      "c_acctbal",
      "c_name",
      "c_address",
      "c_phone",
      "c_comment"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};
  std::vector<std::string> lineitemColumns = {
      "l_orderkey", "l_returnflag", "l_extendedprice", "l_discount"};
  std::vector<std::string> ordersColumns = {
      "o_orderdate", "o_orderkey", "o_custkey"};

  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);
  const auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);
  const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);

  const auto lineitemReturnFlagFilter = "l_returnflag = 'R'";
  const auto orderDate = "o_orderdate";
  auto orderDateFilter = formatDateFilter(
      orderDate, ordersSelectedRowType, "'1993-10-01'", "'1993-12-31'");

  const std::vector<std::string> customerOutputColumns = {
      "c_name", "c_acctbal", "c_phone", "c_address", "c_custkey", "c_comment"};

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId ordersScanNodeId;

  auto nation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .capturePlanNodeId(nationScanNodeId)
          .planNode();

  auto orders = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(
                        kOrders,
                        ordersSelectedRowType,
                        ordersFileColumns,
                        {orderDateFilter})
                    .capturePlanNodeId(ordersScanNodeId)
                    .planNode();

  auto partialPlan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .hashJoin(
              {"c_custkey"},
              {"o_custkey"},
              orders,
              "",
              mergeColumnNames(
                  customerOutputColumns, {"c_nationkey", "o_orderkey"}))
          .hashJoin(
              {"c_nationkey"},
              {"n_nationkey"},
              nation,
              "",
              mergeColumnNames(customerOutputColumns, {"n_name", "o_orderkey"}))
          .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(
                      kLineitem,
                      lineitemSelectedRowType,
                      lineitemFileColumns,
                      {lineitemReturnFlagFilter})
                  .capturePlanNodeId(lineitemScanNodeId)
                  .project(
                      {"l_extendedprice * (1.0 - l_discount) AS part_revenue",
                       "l_orderkey"})
                  .hashJoin(
                      {"l_orderkey"},
                      {"o_orderkey"},
                      partialPlan,
                      "",
                      mergeColumnNames(
                          customerOutputColumns, {"part_revenue", "n_name"}))
                  .partialAggregation(
                      {"c_custkey",
                       "c_name",
                       "c_acctbal",
                       "n_name",
                       "c_address",
                       "c_phone",
                       "c_comment"},
                      {"sum(part_revenue) as revenue"})
                  .localPartition(std::vector<std::string>{})
                  .finalAggregation()
                  .orderBy({"revenue DESC"}, false)
                  .project(
                      {"c_custkey",
                       "c_name",
                       "revenue",
                       "c_acctbal",
                       "n_name",
                       "c_address",
                       "c_phone",
                       "c_comment"})
                  .limit(0, 20, false)
                  .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFiles[nationScanNodeId] = getTableFilePaths(kNation);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ11Plan() const {
  // Column definitions.
  std::vector<std::string> partsuppColumns = {
      "ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost"};
  std::vector<std::string> partsuppColumnsSubQuery = {
      "ps_availqty", "ps_suppkey", "ps_supplycost"};
  std::vector<std::string> supplierColumns = {"s_suppkey", "s_nationkey"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};

  // Row type definitions.
  auto partsuppRowType = getRowType(kPartsupp, partsuppColumns);
  auto partsuppRowTypeSubQuery = getRowType(kPartsupp, partsuppColumnsSubQuery);
  auto supplierRowType = getRowType(kSupplier, supplierColumns);
  auto nationRowType = getRowType(kNation, nationColumns);

  // File columns.
  const auto& partsuppFileColumns = getFileColumnNames(kPartsupp);
  const auto& partsuppFileColumnsSubQuery = getFileColumnNames(kPartsupp);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  const auto& nationFileColumns = getFileColumnNames(kNation);

  // Filter for nation name.
  const std::string nationNameFilter = "n_name = 'GERMANY'";

  // Plan node ID generator.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  // Subquery plan nodes.
  core::PlanNodeId partsuppScanIdSubQuery;
  core::PlanNodeId supplierScanIdSubQuery;
  core::PlanNodeId nationScanIdSubQuery;

  // Main query plan nodes.
  core::PlanNodeId partsuppScanId;
  core::PlanNodeId supplierScanId;
  core::PlanNodeId nationScanId;

  // Subquery plan to calculate filter cost.
  auto nationSubQuery =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kNation, nationRowType, nationFileColumns, {nationNameFilter})
          .capturePlanNodeId(nationScanIdSubQuery)
          .planNode();

  auto supplierJoinNationSubQuery =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kSupplier, supplierRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanIdSubQuery)
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nationSubQuery,
              "",
              {"s_suppkey"})
          .planNode();

  auto subQueryPlan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kPartsupp, partsuppRowTypeSubQuery, partsuppFileColumnsSubQuery)
          .capturePlanNodeId(partsuppScanIdSubQuery)
          .hashJoin(
              {"ps_suppkey"},
              {"s_suppkey"},
              supplierJoinNationSubQuery,
              "",
              {"ps_availqty", "ps_suppkey", "ps_supplycost"})
          .project({"ps_supplycost * ps_availqty AS product_cost_qty"})
          .partialAggregation({}, {"sum(product_cost_qty) AS sum_cost_qty"})
          .localPartition({})
          .finalAggregation()
          .project({"sum_cost_qty * 0.0001 AS filter_cost_qty"})
          .planNode();

  // Query plan.
  auto nation =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(
              kNation, nationRowType, nationFileColumns, {nationNameFilter})
          .capturePlanNodeId(nationScanId)
          .planNode();

  auto supplierJoinNation =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kSupplier, supplierRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanId)
          .hashJoin({"s_nationkey"}, {"n_nationkey"}, nation, "", {"s_suppkey"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kPartsupp, partsuppRowType, partsuppFileColumns)
          .capturePlanNodeId(partsuppScanId)
          .hashJoin(
              {"ps_suppkey"},
              {"s_suppkey"},
              supplierJoinNation,
              "",
              {"ps_partkey", "ps_availqty", "ps_supplycost"})
          .project(
              {"ps_supplycost * ps_availqty AS product_cost_qty", "ps_partkey"})
          .partialAggregation(
              {"ps_partkey"}, {"sum(product_cost_qty) AS value"})
          .localPartition({"ps_partkey"})
          .finalAggregation()
          .nestedLoopJoin(
              subQueryPlan, {"ps_partkey", "value", "filter_cost_qty"})
          .filter("value > filter_cost_qty")
          .project({"ps_partkey", "value"})
          .orderBy({"value DESC"}, false)
          .planNode();

  // Create context and return.
  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[partsuppScanIdSubQuery] = getTableFilePaths(kPartsupp);
  context.dataFiles[supplierScanIdSubQuery] = getTableFilePaths(kSupplier);
  context.dataFiles[nationScanIdSubQuery] = getTableFilePaths(kNation);
  context.dataFiles[partsuppScanId] = getTableFilePaths(kPartsupp);
  context.dataFiles[supplierScanId] = getTableFilePaths(kSupplier);
  context.dataFiles[nationScanId] = getTableFilePaths(kNation);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ12Plan() const {
  std::vector<std::string> ordersColumns = {"o_orderkey", "o_orderpriority"};
  std::vector<std::string> lineitemColumns = {
      "l_receiptdate",
      "l_orderkey",
      "l_commitdate",
      "l_shipmode",
      "l_shipdate"};

  auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;

  const std::string receiptDateFilter = formatDateFilter(
      "l_receiptdate", lineitemSelectedRowType, "'1994-01-01'", "'1994-12-31'");
  const std::string shipDateFilter = formatDateFilter(
      "l_shipdate", lineitemSelectedRowType, "", "'1995-01-01'");
  const std::string commitDateFilter = formatDateFilter(
      "l_commitdate", lineitemSelectedRowType, "", "'1995-01-01'");

  auto lineitem = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(
                          kLineitem,
                          lineitemSelectedRowType,
                          lineitemFileColumns,
                          {receiptDateFilter,
                           "l_shipmode IN ('MAIL', 'SHIP')",
                           shipDateFilter,
                           commitDateFilter},
                          "l_commitdate < l_receiptdate")
                      .capturePlanNodeId(lineitemScanNodeId)
                      .filter("l_shipdate < l_commitdate")
                      .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns, {})
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_orderkey"},
              {"l_orderkey"},
              lineitem,
              "",
              {"l_shipmode", "o_orderpriority"})
          .project(
              {"l_shipmode",
               "(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count_partial",
               "(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count_partial"})
          .partialAggregation(
              {"l_shipmode"},
              {"sum(high_line_count_partial) as high_line_count",
               "sum(low_line_count_partial) as low_line_count"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"l_shipmode"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ13Plan() const {
  std::vector<std::string> ordersColumns = {
      "o_custkey", "o_comment", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey"};

  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);

  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;

  auto customers =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kOrders,
              ordersSelectedRowType,
              ordersFileColumns,
              {},
              "o_comment not like '%special%requests%'")
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              customers,
              "",
              {"c_custkey", "o_orderkey"},
              core::JoinType::kRight)
          .partialAggregation({"c_custkey"}, {"count(o_orderkey) as c_count"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .singleAggregation({"c_count"}, {"count(0) as custdist"})
          .orderBy({"custdist DESC", "c_count DESC"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ14Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_partkey", "l_extendedprice", "l_discount", "l_shipdate"};
  std::vector<std::string> partColumns = {"p_partkey", "p_type"};

  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);

  const std::string shipDate = "l_shipdate";
  const std::string shipDateFilter = formatDateFilter(
      shipDate, lineitemSelectedRowType, "'1995-09-01'", "'1995-09-30'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId partScanNodeId;

  auto part = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(kPart, partSelectedRowType, partFileColumns)
                  .capturePlanNodeId(partScanNodeId)
                  .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kLineitem,
              lineitemSelectedRowType,
              lineitemFileColumns,
              {},
              shipDateFilter)
          .capturePlanNodeId(lineitemScanNodeId)
          .project(
              {"l_extendedprice * (1.0 - l_discount) as part_revenue",
               "l_shipdate",
               "l_partkey"})
          .hashJoin(
              {"l_partkey"},
              {"p_partkey"},
              part,
              "",
              {"part_revenue", "p_type"})
          .project(
              {"(CASE WHEN (p_type LIKE 'PROMO%') THEN part_revenue ELSE 0.0 END) as filter_revenue",
               "part_revenue"})
          .partialAggregation(
              {},
              {"sum(part_revenue) as total_revenue",
               "sum(filter_revenue) as total_promo_revenue"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .project(
              {"100.00 * total_promo_revenue/total_revenue as promo_revenue"})
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ15Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_suppkey", "l_shipdate", "l_extendedprice", "l_discount"};
  std::vector<std::string> supplierColumns = {
      "s_suppkey", "s_name", "s_address", "s_phone"};

  const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  const auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);

  const std::string shipDateFilter = formatDateFilter(
      "l_shipdate", lineitemSelectedRowType, "'1996-01-01'", "'1996-03-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanNodeIdSubQuery;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId supplierScanNodeId;

  auto maxRevenue =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kLineitem,
              lineitemSelectedRowType,
              lineitemFileColumns,
              {shipDateFilter})
          .capturePlanNodeId(lineitemScanNodeId)
          .project(
              {"l_suppkey",
               "l_extendedprice * (1.0 - l_discount) as part_revenue"})
          .partialAggregation(
              {"l_suppkey"}, {"sum(part_revenue) as total_revenue"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .singleAggregation({}, {"max(total_revenue) as max_revenue"})
          .planNode();

  auto supplierWithMaxRevenue =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kLineitem,
              lineitemSelectedRowType,
              lineitemFileColumns,
              {shipDateFilter})
          .capturePlanNodeId(lineitemScanNodeIdSubQuery)
          .project(
              {"l_suppkey as supplier_no",
               "l_extendedprice * (1.0 - l_discount) as part_revenue"})
          .partialAggregation(
              {"supplier_no"}, {"sum(part_revenue) as total_revenue"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .hashJoin(
              {"total_revenue"},
              {"max_revenue"},
              maxRevenue,
              "",
              {"supplier_no", "total_revenue"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin(
              {"s_suppkey"},
              {"supplier_no"},
              supplierWithMaxRevenue,
              "",
              {"s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"})
          .orderBy({"s_suppkey"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanNodeIdSubQuery] = getTableFilePaths(kLineitem);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ16Plan() const {
  std::vector<std::string> partColumns = {
      "p_brand", "p_type", "p_size", "p_partkey"};
  std::vector<std::string> supplierColumns = {"s_suppkey", "s_comment"};
  std::vector<std::string> partsuppColumns = {"ps_partkey", "ps_suppkey"};

  const auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);
  const auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  const auto partsuppSelectedRowType = getRowType(kPartsupp, partsuppColumns);
  const auto& partsuppFileColumns = getFileColumnNames(kPartsupp);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId partScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId partsuppScanNodeId;

  auto part = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(
                      kPart,
                      partSelectedRowType,
                      partFileColumns,
                      {"p_size in (49, 14, 23, 45, 19, 3, 36, 9)"},
                      "p_type NOT LIKE 'MEDIUM POLISHED%'")
                  .capturePlanNodeId(partScanNodeId)
                  // Neq is unsupported as a tableScan subfield filter for
                  // Parquet source.
                  .filter("p_brand <> 'Brand#45'")
                  .planNode();

  auto supplier = PlanBuilder(planNodeIdGenerator, pool_.get())
                      .tableScan(
                          kSupplier,
                          supplierSelectedRowType,
                          supplierFileColumns,
                          {},
                          "s_comment LIKE '%Customer%Complaints%'")
                      .capturePlanNodeId(supplierScanNodeId)
                      .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kPartsupp, partsuppSelectedRowType, partsuppFileColumns)
          .capturePlanNodeId(partsuppScanNodeId)
          .hashJoin(
              {"ps_partkey"},
              {"p_partkey"},
              part,
              "",
              {"ps_suppkey", "p_brand", "p_type", "p_size"})
          .hashJoin(
              {"ps_suppkey"},
              {"s_suppkey"},
              supplier,
              "",
              {"ps_suppkey", "p_brand", "p_type", "p_size"},
              core::JoinType::kAnti,
              true /*nullAware*/)
          // Empty aggregate is used here to get the distinct count of
          // ps_suppkey.
          // approx_distinct could be used instead for getting the count of
          // distinct ps_suppkey but since approx_distinct is non deterministic
          // and the standard error can not be set to 0, it is not used here.
          .partialAggregation({"p_brand", "p_type", "p_size", "ps_suppkey"}, {})
          .localPartition({"p_brand", "p_type", "p_size", "ps_suppkey"})
          .finalAggregation()
          .partialAggregation(
              {"p_brand", "p_type", "p_size"},
              {"count(ps_suppkey) as supplier_cnt"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"supplier_cnt DESC", "p_brand", "p_type", "p_size"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[partsuppScanNodeId] = getTableFilePaths(kPartsupp);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ17Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_partkey", "l_extendedprice", "l_quantity"};
  std::vector<std::string> partColumns = {
      "p_partkey", "p_brand", "p_container"};

  auto lineitemRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto partRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanId;
  core::PlanNodeId lineitemAggScanId;
  core::PlanNodeId partScanId;
  core::PlanNodeId partAggScanId;

  auto part = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(
                      kPart,
                      partRowType,
                      partFileColumns,
                      {"p_brand = 'Brand#23'", "p_container = 'MED BOX'"})
                  .capturePlanNodeId(partScanId)
                  .planNode();

  auto partAgg = PlanBuilder(planNodeIdGenerator, pool_.get())
                     .tableScan(kPart, partRowType, partFileColumns, {})
                     .capturePlanNodeId(partAggScanId)
                     .planNode();

  auto lineitemJoinPart =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kLineitem, lineitemRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemScanId)
          .hashJoin(
              {"l_partkey"},
              {"p_partkey"},
              part,
              "",
              {"l_quantity", "p_partkey", "l_extendedprice"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kLineitem, lineitemRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemAggScanId)
          .hashJoin(
              {"l_partkey"},
              {"p_partkey"},
              partAgg,
              "",
              {"l_partkey", "l_quantity"})
          .partialAggregation({"l_partkey"}, {"avg(l_quantity) as avg_"})
          .localPartition({"l_partkey"})
          .finalAggregation()
          .hashJoin(
              {"l_partkey"},
              {"p_partkey"},
              lineitemJoinPart,
              "l_quantity < 0.2 * avg_",
              {"l_extendedprice"})
          .partialAggregation({}, {"sum(l_extendedprice) as partial_sum"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .project({"(partial_sum / 7.0) as avg_yearly"})
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanId] = getTableFilePaths(kLineitem);
  context.dataFiles[lineitemAggScanId] = getTableFilePaths(kLineitem);
  context.dataFiles[partScanId] = getTableFilePaths(kPart);
  context.dataFiles[partAggScanId] = getTableFilePaths(kPart);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ18Plan() const {
  std::vector<std::string> lineitemColumns = {"l_orderkey", "l_quantity"};
  std::vector<std::string> ordersColumns = {
      "o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"};
  std::vector<std::string> customerColumns = {"c_name", "c_custkey"};

  const auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);

  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);

  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;

  auto bigOrders =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemScanNodeId)
          .partialAggregation({"l_orderkey"}, {"sum(l_quantity) AS quantity"})
          .localPartition({"l_orderkey"})
          .finalAggregation()
          .filter("quantity > 300.0")
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_orderkey"},
              {"l_orderkey"},
              bigOrders,
              "",
              {"o_orderkey",
               "o_custkey",
               "o_orderdate",
               "o_totalprice",
               "l_orderkey",
               "quantity"})
          .hashJoin(
              {"o_custkey"},
              {"c_custkey"},
              PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(
                      kCustomer, customerSelectedRowType, customerFileColumns)
                  .capturePlanNodeId(customerScanNodeId)
                  .planNode(),
              "",
              {"c_name",
               "c_custkey",
               "o_orderkey",
               "o_orderdate",
               "o_totalprice",
               "quantity"})
          .localPartition(std::vector<std::string>{})
          .orderBy({"o_totalprice DESC", "o_orderdate"}, false)
          .limit(0, 100, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ19Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_partkey",
      "l_shipmode",
      "l_shipinstruct",
      "l_extendedprice",
      "l_discount",
      "l_quantity"};
  std::vector<std::string> partColumns = {
      "p_partkey", "p_brand", "p_container", "p_size"};

  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId partScanNodeId;

  const std::string shipModeFilter = "l_shipmode IN ('AIR', 'AIR REG')";
  const std::string shipInstructFilter =
      "(l_shipinstruct = 'DELIVER IN PERSON')";
  const std::string joinFilterExpr =
      "     ((p_brand = 'Brand#12')"
      "     AND (l_quantity between 1.0 and 11.0)"
      "     AND (p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'))"
      "     AND (p_size BETWEEN 1 AND 5))"
      " OR  ((p_brand ='Brand#23')"
      "     AND (p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'))"
      "     AND (l_quantity between 10.0 and 20.0)"
      "     AND (p_size BETWEEN 1 AND 10))"
      " OR  ((p_brand = 'Brand#34')"
      "     AND (p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'))"
      "     AND (l_quantity between 20.0 and 30.0)"
      "     AND (p_size BETWEEN 1 AND 15))";

  auto part = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(kPart, partSelectedRowType, partFileColumns)
                  .capturePlanNodeId(partScanNodeId)
                  .planNode();

  auto plan = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(
                      kLineitem,
                      lineitemSelectedRowType,
                      lineitemFileColumns,
                      {shipModeFilter, shipInstructFilter})
                  .capturePlanNodeId(lineitemScanNodeId)
                  .project(
                      {"l_extendedprice * (1.0 - l_discount) as part_revenue",
                       "l_shipmode",
                       "l_shipinstruct",
                       "l_partkey",
                       "l_quantity"})
                  .hashJoin(
                      {"l_partkey"},
                      {"p_partkey"},
                      part,
                      joinFilterExpr,
                      {"part_revenue"})
                  .partialAggregation({}, {"sum(part_revenue) as revenue"})
                  .localPartition(std::vector<std::string>{})
                  .finalAggregation()
                  .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ20Plan() const {
  std::vector<std::string> lineitemColumns = {
      "l_shipdate", "l_suppkey", "l_partkey", "l_quantity"};
  std::vector<std::string> partColumns = {"p_partkey", "p_name"};
  std::vector<std::string> supplierColumns = {
      "s_nationkey", "s_address", "s_name", "s_suppkey"};
  std::vector<std::string> partsuppColumns = {
      "ps_availqty", "ps_partkey", "ps_suppkey"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};

  auto lineitemSelectedRowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitemFileColumns = getFileColumnNames(kLineitem);
  auto partSelectedRowType = getRowType(kPart, partColumns);
  const auto& partFileColumns = getFileColumnNames(kPart);
  auto supplierSelectedRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  auto partsuppSelectedRowType = getRowType(kPartsupp, partsuppColumns);
  const auto& partsuppFileColumns = getFileColumnNames(kPartsupp);
  auto nationSelectedRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);

  const std::string shipDateFilter = formatDateFilter(
      "l_shipdate", lineitemSelectedRowType, "'1994-01-01'", "'1994-12-31'");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanId;
  core::PlanNodeId partScanId;
  core::PlanNodeId partAggScanId;
  core::PlanNodeId supplierScanId;
  core::PlanNodeId partsuppScanId;
  core::PlanNodeId nationScanId;

  auto part = PlanBuilder(planNodeIdGenerator, pool_.get())
                  .tableScan(
                      kPart,
                      partSelectedRowType,
                      partFileColumns,
                      {},
                      "p_name like 'forest%'")
                  .capturePlanNodeId(partScanId)
                  .planNode();

  auto partAgg = PlanBuilder(planNodeIdGenerator, pool_.get())
                     .tableScan(
                         kPart,
                         partSelectedRowType,
                         partFileColumns,
                         {},
                         "p_name like 'forest%'")
                     .capturePlanNodeId(partAggScanId)
                     .planNode();

  auto nation = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(
                        kNation,
                        nationSelectedRowType,
                        nationFileColumns,
                        {"n_name = 'CANADA'"})
                    .capturePlanNodeId(nationScanId)
                    .planNode();

  auto partsuppJoinPart =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kPartsupp, partsuppSelectedRowType, partsuppFileColumns)
          .capturePlanNodeId(partsuppScanId)
          .hashJoin(
              {"ps_partkey"},
              {"p_partkey"},
              part,
              "",
              {"ps_partkey", "ps_suppkey", "ps_availqty"},
              core::JoinType::kLeftSemiFilter)
          .planNode();

  auto supplierJoinNation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kSupplier, supplierSelectedRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanId)
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nation,
              "",
              {"s_name", "s_address", "s_suppkey"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kLineitem,
              lineitemSelectedRowType,
              lineitemFileColumns,
              {shipDateFilter})
          .capturePlanNodeId(lineitemScanId)
          .hashJoin(
              {"l_partkey"},
              {"p_partkey"},
              partAgg,
              "",
              {"l_partkey", "l_suppkey", "l_quantity"})
          .partialAggregation(
              {"l_partkey", "l_suppkey"}, {"sum(l_quantity) AS sum_qty"})
          .localPartition({"l_partkey", "l_suppkey"})
          .finalAggregation()
          .project({"l_partkey", "l_suppkey", "0.5 * sum_qty AS filter_qty"})
          .hashJoin(
              {"l_partkey", "l_suppkey"},
              {"ps_partkey", "ps_suppkey"},
              partsuppJoinPart,
              "ps_availqty > filter_qty",
              {"ps_suppkey"})
          .hashJoin(
              {"ps_suppkey"},
              {"s_suppkey"},
              supplierJoinNation,
              "",
              {"s_name", "s_address"},
              core::JoinType::kRightSemiFilter)
          .orderBy({"s_name"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanId] = getTableFilePaths(kLineitem);
  context.dataFiles[partScanId] = getTableFilePaths(kPart);
  context.dataFiles[partAggScanId] = getTableFilePaths(kPart);
  context.dataFiles[supplierScanId] = getTableFilePaths(kSupplier);
  context.dataFiles[partsuppScanId] = getTableFilePaths(kPartsupp);
  context.dataFiles[nationScanId] = getTableFilePaths(kNation);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ21Plan() const {
  std::vector<std::string> supplierColumns = {
      "s_nationkey", "s_name", "s_suppkey"};
  std::vector<std::string> lineitemColumnsWithDates = {
      "l_suppkey", "l_commitdate", "l_orderkey", "l_receiptdate"};
  std::vector<std::string> lineitemColumns = {"l_suppkey", "l_orderkey"};
  std::vector<std::string> ordersColumns = {"o_orderkey", "o_orderstatus"};
  std::vector<std::string> nationColumns = {"n_nationkey", "n_name"};

  auto supplierRowType = getRowType(kSupplier, supplierColumns);
  const auto& supplierFileColumns = getFileColumnNames(kSupplier);
  auto lineitem1RowType = getRowType(kLineitem, lineitemColumnsWithDates);
  const auto& lineitem1FileColumns = getFileColumnNames(kLineitem);
  auto lineitem2RowType = getRowType(kLineitem, lineitemColumns);
  const auto& lineitem2FileColumns = getFileColumnNames(kLineitem);
  auto lineitem3RowType = getRowType(kLineitem, lineitemColumnsWithDates);
  const auto& lineitem3FileColumns = getFileColumnNames(kLineitem);
  auto ordersRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  auto nationRowType = getRowType(kNation, nationColumns);
  const auto& nationFileColumns = getFileColumnNames(kNation);

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId lineitem1ScanNodeId;
  core::PlanNodeId lineitem2ScanNodeId;
  core::PlanNodeId lineitem3ScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId nationScanNodeId;
  const std::string receiptCommitFilter = "l_receiptdate > l_commitdate";

  auto lineitem3 =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kLineitem,
              lineitem3RowType,
              lineitem3FileColumns,
              {},
              receiptCommitFilter)
          .capturePlanNodeId(lineitem3ScanNodeId)
          .project({"l_orderkey as l_orderkey_3", "l_suppkey as l_suppkey_3"})
          .planNode();

  auto nation = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .tableScan(
                        kNation,
                        nationRowType,
                        nationFileColumns,
                        {"n_name = 'SAUDI ARABIA'"})
                    .capturePlanNodeId(nationScanNodeId)
                    .planNode();

  auto supplierJoinNation =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kSupplier, supplierRowType, supplierFileColumns)
          .capturePlanNodeId(supplierScanNodeId)
          .hashJoin(
              {"s_nationkey"},
              {"n_nationkey"},
              nation,
              "",
              {"s_suppkey", "s_name", "s_nationkey"})
          .planNode();

  auto lineitemJoinSupplier =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kLineitem,
              lineitem1RowType,
              lineitem1FileColumns,
              {},
              receiptCommitFilter)
          .capturePlanNodeId(lineitem1ScanNodeId)
          .project({"l_orderkey as l_orderkey_1", "l_suppkey as l_suppkey_1"})
          .hashJoin(
              {"l_suppkey_1"},
              {"s_suppkey"},
              supplierJoinNation,
              "",
              {"l_orderkey_1", "s_nationkey", "l_suppkey_1", "s_name"})
          .planNode();

  auto ordersJoinLineitem1 =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kOrders,
              ordersRowType,
              ordersFileColumns,
              {"o_orderstatus = 'F'"})
          .capturePlanNodeId(ordersScanNodeId)
          .hashJoin(
              {"o_orderkey"},
              {"l_orderkey_1"},
              lineitemJoinSupplier,
              "",
              {"s_nationkey", "l_orderkey_1", "l_suppkey_1", "s_name"})
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kLineitem, lineitem2RowType, lineitem2FileColumns)
          .capturePlanNodeId(lineitem2ScanNodeId)
          .project({"l_orderkey as l_orderkey_2", "l_suppkey as l_suppkey_2"})
          .hashJoin(
              {"l_orderkey_2"},
              {"l_orderkey_1"},
              ordersJoinLineitem1,
              "l_suppkey_2 <> l_suppkey_1",
              {"l_orderkey_1", "l_suppkey_1", "s_name"},
              core::JoinType::kRightSemiFilter)
          .hashJoin(
              {"l_orderkey_1"},
              {"l_orderkey_3"},
              lineitem3,
              "l_suppkey_3 <> l_suppkey_1",
              {"s_name"},
              core::JoinType::kAnti,
              false /*nullAware*/)
          .partialAggregation({"s_name"}, {"count(1) as numwait"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"numwait DESC", "s_name"}, false)
          .limit(0, 100, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[supplierScanNodeId] = getTableFilePaths(kSupplier);
  context.dataFiles[lineitem1ScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[lineitem2ScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[lineitem3ScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[nationScanNodeId] = getTableFilePaths(kNation);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getQ22Plan() const {
  std::vector<std::string> ordersColumns = {"o_custkey"};
  std::vector<std::string> customerColumns = {"c_acctbal", "c_phone"};
  std::vector<std::string> customerColumnsWithKey = {
      "c_custkey", "c_acctbal", "c_phone"};

  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);
  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);
  const auto customerSelectedRowTypeWithKey =
      getRowType(kCustomer, customerColumnsWithKey);
  const auto& customerFileColumnsWithKey = getFileColumnNames(kCustomer);

  const std::string phoneFilter =
      "substr(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')";

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId customerScanNodeIdWithKey;
  core::PlanNodeId ordersScanNodeId;

  auto orders =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(kOrders, ordersSelectedRowType, ordersFileColumns)
          .capturePlanNodeId(ordersScanNodeId)
          .planNode();

  auto customerAvgAccountBalance =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kCustomer,
              customerSelectedRowType,
              customerFileColumns,
              {"c_acctbal > 0.0"},
              phoneFilter)
          .capturePlanNodeId(customerScanNodeId)
          .partialAggregation({}, {"avg(c_acctbal) as avg_acctbal"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator, pool_.get())
          .tableScan(
              kCustomer,
              customerSelectedRowTypeWithKey,
              customerFileColumnsWithKey,
              {},
              phoneFilter)
          .capturePlanNodeId(customerScanNodeIdWithKey)
          .nestedLoopJoin(
              customerAvgAccountBalance,
              {"c_acctbal", "avg_acctbal", "c_custkey", "c_phone"})
          .filter("c_acctbal > avg_acctbal")
          .hashJoin(
              {"c_custkey"},
              {"o_custkey"},
              orders,
              "",
              {"c_acctbal", "c_phone"},
              core::JoinType::kAnti,
              true /*nullAware*/)
          .project({"substr(c_phone, 1, 2) AS country_code", "c_acctbal"})
          .partialAggregation(
              {"country_code"},
              {"count(0) AS numcust", "sum(c_acctbal) AS totacctbal"})
          .localPartition(std::vector<std::string>{})
          .finalAggregation()
          .orderBy({"country_code"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[ordersScanNodeId] = getTableFilePaths(kOrders);
  context.dataFiles[customerScanNodeId] = getTableFilePaths(kCustomer);
  context.dataFiles[customerScanNodeIdWithKey] = getTableFilePaths(kCustomer);
  context.dataFileFormat = format_;
  return context;
}

TpchPlan TpchQueryBuilder::getIoMeterPlan(int columnPct) const {
  VELOX_CHECK(columnPct > 0 && columnPct <= 100);
  auto columns = getFileColumnNames(kLineitem);
  std::vector<std::string> names;
  for (auto& pair : columns) {
    names.push_back(pair.first);
  }
  std::sort(names.begin(), names.end());
  names.resize(names.size() * columnPct / 100);
  if (std::find(names.begin(), names.end(), "l_partkey") == names.end()) {
    names.push_back("l_partkey");
  }

  const auto selectedRowType = getRowType(kLineitem, names);
  std::vector<std::string> aggregates;
  std::vector<std::string> projectExprs;

  for (auto i = 0; i < selectedRowType->size(); ++i) {
    if (selectedRowType->childAt(i)->kind() == TypeKind::VARCHAR) {
      projectExprs.push_back(
          fmt::format("length({}) as l{}", selectedRowType->nameOf(i), i));
      aggregates.push_back(fmt::format("max(l{})", i));
    } else {
      projectExprs.push_back(selectedRowType->nameOf(i));
      aggregates.push_back(fmt::format("max({})", selectedRowType->nameOf(i)));
    }
  }

  std::string filter = "l_partkey between 2000000 and 2500000";

  core::PlanNodeId lineitemPlanNodeId;
  std::unordered_map<std::string, std::string> aliases;
  for (auto& name : names) {
    aliases[name] = name;
  }
  auto plan = PlanBuilder(pool_.get())
                  .tableScan(kLineitem, selectedRowType, aliases, {filter})
                  .capturePlanNodeId(lineitemPlanNodeId)
                  .project(projectExprs)
                  .partialAggregation({}, aggregates)
                  .localPartition(std::vector<std::string>{})
                  .finalAggregation()
                  .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
  context.dataFileFormat = format_;
  return context;
}

const std::vector<std::string> TpchQueryBuilder::kTableNames_ = {
    kLineitem,
    kOrders,
    kCustomer,
    kNation,
    kRegion,
    kPart,
    kSupplier,
    kPartsupp};

const std::unordered_map<std::string, std::vector<std::string>>
    TpchQueryBuilder::kTables_ = {
        std::make_pair(
            "lineitem",
            tpch::getTableSchema(tpch::Table::TBL_LINEITEM)->names()),
        std::make_pair(
            "orders",
            tpch::getTableSchema(tpch::Table::TBL_ORDERS)->names()),
        std::make_pair(
            "customer",
            tpch::getTableSchema(tpch::Table::TBL_CUSTOMER)->names()),
        std::make_pair(
            "nation",
            tpch::getTableSchema(tpch::Table::TBL_NATION)->names()),
        std::make_pair(
            "region",
            tpch::getTableSchema(tpch::Table::TBL_REGION)->names()),
        std::make_pair(
            "part",
            tpch::getTableSchema(tpch::Table::TBL_PART)->names()),
        std::make_pair(
            "supplier",
            tpch::getTableSchema(tpch::Table::TBL_SUPPLIER)->names()),
        std::make_pair(
            "partsupp",
            tpch::getTableSchema(tpch::Table::TBL_PARTSUPP)->names())};

} // namespace facebook::velox::exec::test
