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

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

namespace facebook::velox::exec::test {

/// Contains the query plan and input data files keyed on source plan node ID.
/// All data files use the same file format specified in 'dataFileFormat'.
struct TpchPlan {
  std::shared_ptr<core::PlanNode> plan;
  std::unordered_map<core::PlanNodeId, std::vector<std::string>> dataFiles;
  dwio::common::FileFormat dataFileFormat;
};

/// Contains type information, data files, and column aliases for a table.
/// This information is inferred from the input data files.
struct TpchTableMetadata {
  RowTypePtr type;
  std::vector<std::string> dataFiles;
  std::unordered_map<std::string, std::string> columnAliases;
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
  TpchPlan getQ6Plan() const;
  TpchPlan getQ18Plan() const;

  const std::vector<std::string>& getTableFilePaths(
      const std::string& tableName) const {
    return tableMetadata_.at(tableName).dataFiles;
  }

  const std::string& getColumnAlias(
      const std::string& tableName,
      const std::string& columnName) const {
    return tableMetadata_.at(tableName).columnAliases.at(columnName);
  }

  std::vector<std::string> getColumnAliases(
      const std::string& tableName,
      const std::vector<std::string>& columnNames) const {
    std::vector<std::string> aliases;
    for (const auto& name : columnNames) {
      aliases.push_back(getColumnAlias(tableName, name));
    }
    return aliases;
  }

  std::shared_ptr<const RowType> getRowType(
      const std::string& tableName,
      const std::vector<std::string>& columnNames) const {
    const auto aliases = getColumnAliases(tableName, columnNames);
    auto columnSelector = std::make_shared<dwio::common::ColumnSelector>(
        tableMetadata_.at(tableName).type, aliases);
    return columnSelector->buildSelectedReordered();
  }

  std::vector<std::string> getProjectColumnAliases(
      const std::string& tableName,
      const std::vector<std::string>& columnNames) const {
    std::vector<std::string> aliases;
    for (const auto& name : columnNames) {
      aliases.push_back(getColumnAlias(tableName, name) + " AS " + name);
    }
    return aliases;
  }

  std::unordered_map<std::string, TpchTableMetadata> tableMetadata_;
  const dwio::common::FileFormat format_;
  static const std::unordered_map<std::string, std::vector<std::string>>
      kTables_;
  static const std::vector<std::string> kTableNames_;
};

} // namespace facebook::velox::exec::test
