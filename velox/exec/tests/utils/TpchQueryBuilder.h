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

#include "velox/dwio/common/Options.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec::test {

/// Contains the query plan and input data files keyed on source plan node ID.
/// All data files use the same file format specified in 'dataFileFormat'.
struct TpchPlan {
  core::PlanNodePtr plan;
  std::unordered_map<core::PlanNodeId, std::vector<std::string>> dataFiles;
  dwio::common::FileFormat dataFileFormat;
};

/// Contains type information, data files, and file column names for a table.
/// This information is inferred from the input data files.
/// The type names are mapped to the standard names.
/// Example: If the file has a 'returnflag' column, the corresponding type name
/// will be 'l_returnflag'. fileColumnNames store the mapping between standard
/// names and the corresponding name in the file.
struct TpchTableMetadata {
  RowTypePtr type;
  std::vector<std::string> dataFiles;
  std::unordered_map<std::string, std::string> fileColumnNames;
};

/// Builds TPC-H queries using TPC-H data files located in the specified
/// directory. Each table data must be placed in hive-style partitioning. That
/// is, the top-level directory is expected to contain a sub-directory per table
/// name and the name of the sub-directory must match the table name. Example:
/// ls -R data/
///  customer   lineitem
///
///  data/customer:
///  customer1.parquet  customer2.parquet
///
///  data/lineitem:
///  lineitem1.parquet  lineitem2.parquet  lineitem3.parquet

/// The column names can vary. Additional columns may exist towards the end.
/// The class uses standard names (example: l_returnflag) to build TPC-H plans.
/// Since the column names in the file can vary, they are mapped to the standard
/// names. Therefore, the order of the columns in the file is important and
/// should be in the same order as in the TPC-H standard.
class TpchQueryBuilder {
 public:
  explicit TpchQueryBuilder(dwio::common::FileFormat format)
      : format_(format) {}

  /// Read each data file, initialize row types, and determine data paths for
  /// each table.
  /// @param dataPath path to the data files
  void initialize(const std::string& dataPath);

  /// Get the query plan for a given TPC-H query number.
  /// @param queryId TPC-H query number
  TpchPlan getQueryPlan(int queryId) const;

  /// Get the TPC-H table names present.
  static const std::vector<std::string>& getTableNames();

 private:
  TpchPlan getQ1Plan() const;
  TpchPlan getQ3Plan() const;
  TpchPlan getQ5Plan() const;
  TpchPlan getQ6Plan() const;
  TpchPlan getQ9Plan() const;
  TpchPlan getQ10Plan() const;
  TpchPlan getQ12Plan() const;
  TpchPlan getQ13Plan() const;
  TpchPlan getQ14Plan() const;
  TpchPlan getQ18Plan() const;
  TpchPlan getQ19Plan() const;
  TpchPlan getQ22Plan() const;

  const std::vector<std::string>& getTableFilePaths(
      const std::string& tableName) const {
    return tableMetadata_.at(tableName).dataFiles;
  }

  std::shared_ptr<const RowType> getRowType(
      const std::string& tableName,
      const std::vector<std::string>& columnNames) const {
    auto columnSelector = std::make_shared<dwio::common::ColumnSelector>(
        tableMetadata_.at(tableName).type, columnNames);
    return columnSelector->buildSelectedReordered();
  }

  const std::unordered_map<std::string, std::string>& getFileColumnNames(
      const std::string& tableName) const {
    return tableMetadata_.at(tableName).fileColumnNames;
  }

  std::unordered_map<std::string, TpchTableMetadata> tableMetadata_;
  const dwio::common::FileFormat format_;
  static const std::unordered_map<std::string, std::vector<std::string>>
      kTables_;
  static const std::vector<std::string> kTableNames_;

  static constexpr const char* kLineitem = "lineitem";
  static constexpr const char* kCustomer = "customer";
  static constexpr const char* kOrders = "orders";
  static constexpr const char* kNation = "nation";
  static constexpr const char* kRegion = "region";
  static constexpr const char* kPart = "part";
  static constexpr const char* kSupplier = "supplier";
  static constexpr const char* kPartsupp = "partsupp";
  std::unique_ptr<memory::ScopedMemoryPool> pool_ =
      memory::getDefaultScopedMemoryPool();
};

} // namespace facebook::velox::exec::test
