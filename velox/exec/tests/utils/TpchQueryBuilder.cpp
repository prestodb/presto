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
#include "velox/common/base/tests/Fs.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/tpch/gen/TpchGen.h"

namespace facebook::velox::exec::test {

namespace {
int64_t toDate(std::string_view stringDate) {
  Date date;
  parseTo(stringDate, date);
  return date.days();
}

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
} // namespace

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
        tableMetadata_[tableName].fileColumnNames =
            std::move(fileColumnNamesMap);
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
    case 3:
      return getQ3Plan();
    case 5:
      return getQ5Plan();
    case 6:
      return getQ6Plan();
    case 10:
      return getQ10Plan();
    case 13:
      return getQ13Plan();
    case 14:
      return getQ14Plan();
    case 18:
      return getQ18Plan();
    case 19:
      return getQ19Plan();
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
      PlanBuilder()
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
          .localPartition({})
          .finalAggregation()
          .orderBy({"l_returnflag", "l_linestatus"}, false)
          .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
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

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId lineitemPlanNodeId;
  core::PlanNodeId ordersPlanNodeId;
  core::PlanNodeId customerPlanNodeId;

  auto customers = PlanBuilder(planNodeIdGenerator)
                       .tableScan(
                           kCustomer,
                           customerSelectedRowType,
                           customerFileColumns,
                           {customerFilter})
                       .capturePlanNodeId(customerPlanNodeId)
                       .planNode();

  auto custkeyJoinNode =
      PlanBuilder(planNodeIdGenerator)
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
      PlanBuilder(planNodeIdGenerator)
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
          .localPartition({})
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

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId supplierScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId regionScanNodeId;

  auto region = PlanBuilder(planNodeIdGenerator)
                    .tableScan(
                        kRegion,
                        regionSelectedRowType,
                        regionFileColumns,
                        {regionNameFilter})
                    .capturePlanNodeId(regionScanNodeId)
                    .planNode();

  auto orders = PlanBuilder(planNodeIdGenerator)
                    .tableScan(
                        kOrders,
                        ordersSelectedRowType,
                        ordersFileColumns,
                        {orderDateFilter})
                    .capturePlanNodeId(ordersScanNodeId)
                    .planNode();

  auto customer =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .planNode();

  auto nationJoinRegion =
      PlanBuilder(planNodeIdGenerator)
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
      PlanBuilder(planNodeIdGenerator)
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
      PlanBuilder(planNodeIdGenerator)
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
          .localPartition({})
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
  auto plan = PlanBuilder()
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
                  .localPartition({})
                  .finalAggregation()
                  .planNode();
  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemPlanNodeId] = getTableFilePaths(kLineitem);
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

  std::vector<std::string> customerOutputColumns = {
      "c_name", "c_acctbal", "c_phone", "c_address", "c_custkey", "c_comment"};

  auto mergeColumnNames = [](std::vector<std::string>& v1,
                             const std::vector<std::string>& v2) {
    v1.insert(v1.end(), v2.begin(), v2.end());
    return v1;
  };

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId nationScanNodeId;
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId ordersScanNodeId;

  auto nation =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kNation, nationSelectedRowType, nationFileColumns)
          .capturePlanNodeId(nationScanNodeId)
          .planNode();

  auto orders = PlanBuilder(planNodeIdGenerator)
                    .tableScan(
                        kOrders,
                        ordersSelectedRowType,
                        ordersFileColumns,
                        {orderDateFilter})
                    .capturePlanNodeId(ordersScanNodeId)
                    .planNode();

  auto partialPlan =
      PlanBuilder(planNodeIdGenerator)
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

  auto plan = PlanBuilder(planNodeIdGenerator)
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
                  .localPartition({})
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

TpchPlan TpchQueryBuilder::getQ13Plan() const {
  std::vector<std::string> ordersColumns = {
      "o_custkey", "o_comment", "o_orderkey"};
  std::vector<std::string> customerColumns = {"c_custkey"};

  const auto ordersSelectedRowType = getRowType(kOrders, ordersColumns);
  const auto& ordersFileColumns = getFileColumnNames(kOrders);

  const auto customerSelectedRowType = getRowType(kCustomer, customerColumns);
  const auto& customerFileColumns = getFileColumnNames(kCustomer);

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;

  auto customers =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kCustomer, customerSelectedRowType, customerFileColumns)
          .capturePlanNodeId(customerScanNodeId)
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
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
          .partialAggregation({"c_custkey"}, {"count(o_orderkey) as pc_count"})
          .localPartition({})
          .finalAggregation(
              {"c_custkey"}, {"count(pc_count) as c_count"}, {BIGINT()})
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

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId lineitemScanNodeId;
  core::PlanNodeId partScanNodeId;

  auto part = PlanBuilder(planNodeIdGenerator)
                  .tableScan(kPart, partSelectedRowType, partFileColumns)
                  .capturePlanNodeId(partScanNodeId)
                  .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
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
          .localPartition({})
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

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  core::PlanNodeId customerScanNodeId;
  core::PlanNodeId ordersScanNodeId;
  core::PlanNodeId lineitemScanNodeId;

  auto bigOrders =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(kLineitem, lineitemSelectedRowType, lineitemFileColumns)
          .capturePlanNodeId(lineitemScanNodeId)
          .partialAggregation(
              {"l_orderkey"}, {"sum(l_quantity) AS partial_sum"})
          .localPartition({"l_orderkey"})
          .finalAggregation(
              {"l_orderkey"}, {"sum(partial_sum) AS quantity"}, {DOUBLE()})
          .filter("quantity > 300.0")
          .planNode();

  auto plan =
      PlanBuilder(planNodeIdGenerator)
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
              PlanBuilder(planNodeIdGenerator)
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
          .localPartition({})
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

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
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

  auto part = PlanBuilder(planNodeIdGenerator)
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
                  .localPartition({})
                  .finalAggregation()
                  .planNode();

  TpchPlan context;
  context.plan = std::move(plan);
  context.dataFiles[lineitemScanNodeId] = getTableFilePaths(kLineitem);
  context.dataFiles[partScanNodeId] = getTableFilePaths(kPart);
  context.dataFileFormat = format_;
  return context;
}

const std::vector<std::string> TpchQueryBuilder::kTableNames_ =
    {kLineitem, kOrders, kCustomer, kNation, kRegion, kPart, kSupplier};

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
            tpch::getTableSchema(tpch::Table::TBL_SUPPLIER)->names())};

} // namespace facebook::velox::exec::test
