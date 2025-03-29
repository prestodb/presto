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

#include <velox/core/Expressions.h>
#include <velox/core/ITypedExpr.h>
#include <velox/core/PlanFragment.h>
#include <velox/core/PlanNode.h>
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/IExpr.h"
#include "velox/parse/PlanNodeIdGenerator.h"

namespace facebook::velox::tpch {
enum class Table : uint8_t;
}

namespace facebook::velox::exec::test {

/// A builder class with fluent API for building query plans. Plans are built
/// bottom up starting with the source node (table scan or similar). Expressions
/// and orders can be specified using SQL. See filter, project and orderBy
/// methods for details.
///
/// For example, to build a query plan for a leaf fragment of a simple query
///     SELECT a, sum(b) FROM t GROUP BY 1
///
///     auto plan = PlanBuilder()
///         .tableScan(ROW({"a", "b"}, {INTEGER(), DOUBLE()}))
///         .partialAggregation({"a"}, {"sum(b)"})
///         .planNode();
///
/// Here, we use default PlanNodeIdGenerator that starts from zero, hence, table
/// scan node ID will be "0". You'll need to use this ID when adding splits.
///
/// A join query plan would be a bit more complex:
///     SELECT t.a, u.b FROM t, u WHERE t.key = u.key
///
///     auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
///     core::PlanNodeId tScanId; // ID of the table scan node for 't'.
///     core::PlanNodeId uScanId; // ID of the table scan node for 'u'.
///     auto plan = PlanBuilder(planNodeIdGenerator)
///         .tableScan(ROW({"key", "a"}, {INTEGER(), BIGINT()}))
///         .capturePlanNodeId(tScanId)
///         .hashJoin(
///             {"key"},
///             {"key"},
///             PlanBuilder(planNodeIdGenerator)
///                 .tableScan(ROW({"key", "b"}, {INTEGER(), DOUBLE()})))
///                 .capturePlanNodeId(uScanId)
///                 .planNode(),
///             "", // no extra join filter
///             {"a", "b"})
///         .planNode();
///
/// We use two builders, one for the right-side and another for the left-side
/// of the join. To ensure plan node IDs are unique in the final plan, we use
/// the same instance of PlanNodeIdGenerator with both builders. We also use
/// capturePlanNodeId method to capture the IDs of the table scan nodes for
/// 't' and 'u'. We need these to add splits.
class PlanBuilder {
 public:
  /// Constructor taking an instance of PlanNodeIdGenerator and a memory pool.
  ///
  /// The memory pool is used when parsing expressions containing complex-type
  /// literals, e.g. arrays, maps or structs. The memory pool can be empty if
  /// such expressions are not used in the plan.
  ///
  /// When creating tree-shaped plans, e.g. join queries, use the same instance
  /// of PlanNodeIdGenerator for all builders to ensure unique plan node IDs
  /// across the plan.
  explicit PlanBuilder(
      std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator,
      memory::MemoryPool* pool = nullptr)
      : planNodeIdGenerator_{std::move(planNodeIdGenerator)}, pool_{pool} {}

  /// Constructor with no required parameters suitable for creating
  /// straight-line (e.g. no joins) query plans.
  explicit PlanBuilder(memory::MemoryPool* pool = nullptr)
      : PlanBuilder(std::make_shared<core::PlanNodeIdGenerator>(), pool) {}

  /// Constructor that allows an initial plane node to be specified for testing
  /// this is useful when testing additional connectors that do not rely on the
  /// table scan node supported below.
  PlanBuilder(
      core::PlanNodePtr initialPlanNode,
      std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator,
      memory::MemoryPool* pool = nullptr)
      : planNode_(std::move(initialPlanNode)),
        planNodeIdGenerator_{std::move(planNodeIdGenerator)},
        pool_{pool} {}

  virtual ~PlanBuilder() = default;

  static constexpr const std::string_view kHiveDefaultConnectorId{"test-hive"};
  static constexpr const std::string_view kTpchDefaultConnectorId{"test-tpch"};

  ///
  /// TableScan
  ///

  /// Add a TableScanNode to scan a Hive table.
  ///
  /// @param outputType List of column names and types to read from the table.
  /// @param subfieldFilters A list of SQL expressions for the range filters to
  /// apply to individual columns. Supported filters are: column <= value,
  /// column < value, column >= value, column > value, column = value, column IN
  /// (v1, v2,.. vN), column < v1 OR column >= v2.
  /// @param remainingFilter SQL expression for the additional conjunct. May
  /// include multiple columns and SQL functions. The remainingFilter is AND'ed
  /// with all the subfieldFilters.
  /// @param dataColumns can be different from 'outputType' for the purposes
  /// of testing queries using missing columns. It is used, if specified, for
  /// parseExpr call and as 'dataColumns' for the TableHandle. You supply more
  /// types (for all columns) in this argument as opposed to 'outputType', where
  /// you define the output types only. See 'missingColumns' test in
  /// 'TableScanTest'.
  /// @param assignments Optional ColumnHandles.
  PlanBuilder& tableScan(
      const RowTypePtr& outputType,
      const std::vector<std::string>& subfieldFilters = {},
      const std::string& remainingFilter = "",
      const RowTypePtr& dataColumns = nullptr,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& assignments = {});

  /// Add a TableScanNode to scan a Hive table.
  ///
  /// @param tableName The name of the table to scan.
  /// @param outputType List of column names and types to read from the table.
  /// @param columnAliases Optional aliases for the column names. The key is the
  /// alias (name in 'outputType'), value is the name in the files.
  /// @param subfieldFilters A list of SQL expressions for the range filters to
  /// apply to individual columns. Should use column name aliases, not column
  /// names in the files. Supported filters are: column <= value, column <
  /// value, column >= value, column > value, column = value, column IN (v1,
  /// v2,.. vN), column < v1 OR column >= v2.
  /// @param remainingFilter SQL expression for the additional conjunct. May
  /// include multiple columns and SQL functions. Should use column name
  /// aliases, not column names in the files. The remainingFilter is AND'ed
  /// with all the subfieldFilters.
  /// @param dataColumns can be different from 'outputType' for the purposes
  /// of testing queries using missing columns. It is used, if specified, for
  /// parseExpr call and as 'dataColumns' for the TableHandle. You supply more
  /// types (for all columns) in this argument as opposed to 'outputType', where
  /// you define the output types only. See 'missingColumns' test in
  /// 'TableScanTest'.
  PlanBuilder& tableScan(
      const std::string& tableName,
      const RowTypePtr& outputType,
      const std::unordered_map<std::string, std::string>& columnAliases = {},
      const std::vector<std::string>& subfieldFilters = {},
      const std::string& remainingFilter = "",
      const RowTypePtr& dataColumns = nullptr,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& assignments = {});

  /// Add a TableScanNode to scan a TPC-H table.
  ///
  /// @param tpchTableHandle The handle that specifies the target TPC-H table
  /// and scale factor.
  /// @param columnNames The columns to be returned from that table.
  /// @param scaleFactor The TPC-H scale factor.
  /// @param connectorId The TPC-H connector id.
  PlanBuilder& tpchTableScan(
      tpch::Table table,
      std::vector<std::string> columnNames,
      double scaleFactor = 1,
      std::string_view connectorId = kTpchDefaultConnectorId);

  /// Helper class to build a custom TableScanNode.
  /// Uses a planBuilder instance to get the next plan id, memory pool, and
  /// parse options.
  ///
  /// Uses the hive connector by default. Specify outputType, tableHandle, and
  /// assignments for other connectors. If these three are specified, all other
  /// builder arguments will be ignored.
  class TableScanBuilder {
   public:
    TableScanBuilder(PlanBuilder& builder) : planBuilder_(builder) {}

    /// @param tableName The name of the table to scan.
    TableScanBuilder& tableName(std::string tableName) {
      tableName_ = std::move(tableName);
      return *this;
    }

    /// @param connectorId The id of the connector to scan.
    TableScanBuilder& connectorId(std::string connectorId) {
      connectorId_ = std::move(connectorId);
      return *this;
    }

    /// @param outputType List of column names and types to read from the table.
    /// This property is required.
    TableScanBuilder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    /// @param subfieldFilters A list of SQL expressions to apply to individual
    /// columns. These are range filters that can be efficiently applied as data
    /// is read/decoded. Supported filters are:
    ///
    /// >  column <= value
    /// >  column < value
    /// >  column >= value
    /// >  column > value
    /// >  column = value
    /// >  column IN (v1, v2,.. vN)
    /// >  column < v1
    /// >  column >= v2
    TableScanBuilder& subfieldFilters(std::vector<std::string> subfieldFilters);

    /// @param subfieldFilter A single SQL expression to be applied to an
    /// individual column.
    TableScanBuilder& subfieldFilter(std::string subfieldFilter) {
      return subfieldFilters({std::move(subfieldFilter)});
    }

    /// @param remainingFilter SQL expression for the additional conjunct. May
    /// include multiple columns and SQL functions. The remainingFilter is
    /// AND'ed with all the subfieldFilters.
    TableScanBuilder& remainingFilter(std::string remainingFilter);

    /// @param dataColumns can be different from 'outputType' for the purposes
    /// of testing queries using missing columns. It is used, if specified, for
    /// parseExpr call and as 'dataColumns' for the TableHandle. You supply more
    /// types (for all columns) in this argument as opposed to 'outputType',
    /// where you define the output types only. See 'missingColumns' test in
    /// 'TableScanTest'.
    TableScanBuilder& dataColumns(RowTypePtr dataColumns) {
      dataColumns_ = std::move(dataColumns);
      return *this;
    }

    /// @param columnAliases Optional aliases for the column names. The key is
    /// the alias (name in 'outputType'), value is the name in the files.
    TableScanBuilder& columnAliases(
        std::unordered_map<std::string, std::string> columnAliases) {
      columnAliases_ = std::move(columnAliases);
      return *this;
    }

    /// @param tableHandle Optional tableHandle. Other builder arguments such as
    /// the `subfieldFilters` and `remainingFilter` will be ignored.
    TableScanBuilder& tableHandle(
        std::shared_ptr<connector::ConnectorTableHandle> tableHandle) {
      tableHandle_ = std::move(tableHandle);
      return *this;
    }

    /// @param assignments Optional ColumnHandles.
    /// outputType names should match the keys in the 'assignments' map. The
    /// 'assignments' map may contain more columns than 'outputType' if some
    /// columns are only used by pushed-down filters.
    TableScanBuilder& assignments(
        std::unordered_map<
            std::string,
            std::shared_ptr<connector::ColumnHandle>> assignments) {
      assignments_ = std::move(assignments);
      return *this;
    }

    /// Stop the TableScanBuilder.
    PlanBuilder& endTableScan() {
      planBuilder_.planNode_ = build(planBuilder_.nextPlanNodeId());
      return planBuilder_;
    }

   private:
    /// Build the plan node TableScanNode.
    core::PlanNodePtr build(core::PlanNodeId id);

    PlanBuilder& planBuilder_;
    std::string tableName_{"hive_table"};
    std::string connectorId_{kHiveDefaultConnectorId};
    RowTypePtr outputType_;
    std::vector<core::ExprPtr> subfieldFilters_;
    core::ExprPtr remainingFilter_;
    RowTypePtr dataColumns_;
    std::unordered_map<std::string, std::string> columnAliases_;
    std::shared_ptr<connector::ConnectorTableHandle> tableHandle_;
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        assignments_;
  };

  /// Start a TableScanBuilder.
  TableScanBuilder& startTableScan() {
    tableScanBuilder_.reset(new TableScanBuilder(*this));
    return *tableScanBuilder_;
  }

  ///
  /// TableWriter
  ///

  /// Helper class to build a custom TableWriteNode.
  /// Uses a planBuilder instance to get the next plan id, memory pool, and
  /// upstream node (the node that will produce the data).
  ///
  /// Uses the Hive connector by default.
  class TableWriterBuilder {
   public:
    explicit TableWriterBuilder(PlanBuilder& builder) : planBuilder_(builder) {}

    /// @param outputType The schema that will be written to the output file. It
    /// may reference a subset or change the order of columns from the input
    /// (upstream operator output).
    TableWriterBuilder& outputType(RowTypePtr outputType) {
      outputType_ = std::move(outputType);
      return *this;
    }

    /// @param outputDirectoryPath Path in which output files will be created.
    TableWriterBuilder& outputDirectoryPath(std::string outputDirectoryPath) {
      outputDirectoryPath_ = std::move(outputDirectoryPath);
      return *this;
    }

    /// @param outputFileName File name of the output (optional). If specified
    /// (non-empty), use it instead of generating the file name in Velox. Should
    /// only be specified in non-bucketing write.
    TableWriterBuilder& outputFileName(std::string outputFileName) {
      outputFileName_ = std::move(outputFileName);
      return *this;
    }

    /// @param connectorId The id of the connector to write to.
    TableWriterBuilder& connectorId(std::string_view connectorId) {
      connectorId_ = connectorId;
      return *this;
    }

    /// @param insertHandle TableInsertHandle (optional). Other builder
    /// arguments such as the `connectorId`, `outputDirectoryPath`, `fileFormat`
    /// and so on will be ignored.
    TableWriterBuilder& insertHandle(
        std::shared_ptr<core::InsertTableHandle> insertHandle) {
      insertHandle_ = std::move(insertHandle);
      return *this;
    }

    /// @param partitionBy Specifies the partition key columns.
    TableWriterBuilder& partitionBy(std::vector<std::string> partitionBy) {
      partitionBy_ = std::move(partitionBy);
      return *this;
    }

    /// @param bucketCount Specifies the bucket count.
    TableWriterBuilder& bucketCount(int32_t count) {
      bucketCount_ = count;
      return *this;
    }

    /// @param bucketedBy Specifies the bucket by columns.
    TableWriterBuilder& bucketedBy(std::vector<std::string> bucketedBy) {
      bucketedBy_ = std::move(bucketedBy);
      return *this;
    }

    /// @param aggregates Aggregations for column statistics collection during
    /// write.
    TableWriterBuilder& aggregates(std::vector<std::string> aggregates) {
      aggregates_ = std::move(aggregates);
      return *this;
    }

    /// @param sortBy Specifies the sort by columns.
    TableWriterBuilder& sortBy(
        std::vector<std::shared_ptr<const connector::hive::HiveSortingColumn>>
            sortBy) {
      sortBy_ = std::move(sortBy);
      return *this;
    }

    /// @param serdeParameters Additional parameters passed to the writer.
    TableWriterBuilder& serdeParameters(
        std::unordered_map<std::string, std::string> serdeParameters) {
      serdeParameters_ = std::move(serdeParameters);
      return *this;
    }

    /// @param Option objects passed to the writer.
    TableWriterBuilder& options(
        std::shared_ptr<dwio::common::WriterOptions> options) {
      options_ = std::move(options);
      return *this;
    }

    /// @param fileFormat File format to use for the written data.
    TableWriterBuilder& fileFormat(dwio::common::FileFormat fileFormat) {
      fileFormat_ = fileFormat;
      return *this;
    }

    /// @param compressionKind Compression scheme to use for writing the
    /// output data files.
    TableWriterBuilder& compressionKind(
        common::CompressionKind compressionKind) {
      compressionKind_ = compressionKind;
      return *this;
    }

    /// @param ensureFiles When set the Task will always output a file, even if
    /// it's empty.
    TableWriterBuilder& ensureFiles(const bool ensureFiles) {
      ensureFiles_ = ensureFiles;
      return *this;
    }

    /// Stop the TableWriterBuilder.
    PlanBuilder& endTableWriter() {
      planBuilder_.planNode_ = build(planBuilder_.nextPlanNodeId());
      return planBuilder_;
    }

   private:
    /// Build the plan node TableWriteNode.
    core::PlanNodePtr build(core::PlanNodeId id);

    PlanBuilder& planBuilder_;
    RowTypePtr outputType_;
    std::string outputDirectoryPath_;
    std::string outputFileName_;
    std::string connectorId_{kHiveDefaultConnectorId};
    std::shared_ptr<core::InsertTableHandle> insertHandle_;

    std::vector<std::string> partitionBy_;
    int32_t bucketCount_{0};
    std::vector<std::string> bucketedBy_;
    std::vector<std::string> aggregates_;
    std::vector<std::shared_ptr<const connector::hive::HiveSortingColumn>>
        sortBy_;

    std::unordered_map<std::string, std::string> serdeParameters_;
    std::shared_ptr<dwio::common::WriterOptions> options_;

    dwio::common::FileFormat fileFormat_{dwio::common::FileFormat::DWRF};
    common::CompressionKind compressionKind_{common::CompressionKind_NONE};

    bool ensureFiles_{false};
  };

  /// Start a TableWriterBuilder.
  TableWriterBuilder& startTableWriter() {
    tableWriterBuilder_.reset(new TableWriterBuilder(*this));
    return *tableWriterBuilder_;
  }

  /// Add a ValuesNode using specified data.
  ///
  /// @param values The data to use.
  /// @param parallelizable If true, ValuesNode can run multi-threaded, in which
  /// case it will produce duplicate data from each thread, e.g. each thread
  /// will return all the data in 'values'. Useful for testing.
  /// @param repeatTimes The number of times data is produced as input. If
  /// greater than one, each RowVector will produce data as input `repeatTimes`.
  /// For example, in case `values` has 3 vectors {v1, v2, v3} and repeatTimes
  /// is 2, the input produced will be {v1, v2, v3, v1, v2, v3}. Useful for
  /// testing.
  PlanBuilder& values(
      const std::vector<RowVectorPtr>& values,
      bool parallelizable = false,
      size_t repeatTimes = 1);

  /// Adds a QueryReplayNode for query tracing.
  ///
  /// @param traceNodeDir The trace directory for a given plan node.
  /// @param pipelineId The pipeline id for the traced operator instantiated
  /// from the given plan node.
  /// @param driverIds The target driver ID list for replay. The replaying
  /// operator uses its driver instance id as the list index to get the traced
  /// driver id for replay.
  /// @param outputType The type of the tracing data.
  PlanBuilder& traceScan(
      const std::string& traceNodeDir,
      uint32_t pipelineId,
      std::vector<uint32_t> driverIds,
      const RowTypePtr& outputType);

  /// Add an ExchangeNode.
  ///
  /// Use capturePlanNodeId method to capture the node ID needed for adding
  /// splits.
  ///
  /// @param outputType The type of the data coming in and out of the exchange.
  /// @param serdekind The kind of seralized data format.
  PlanBuilder& exchange(
      const RowTypePtr& outputType,
      VectorSerde::Kind serdekind);

  /// Add a MergeExchangeNode using specified ORDER BY clauses.
  ///
  /// For example,
  ///
  ///     .mergeExchange(outputRowType, {"a", "b DESC", "c ASC NULLS FIRST"})
  ///
  /// By default, uses ASC NULLS LAST sort order, e.g. column "a" above will use
  /// ASC NULLS LAST and column "b" will use DESC NULLS LAST.
  PlanBuilder& mergeExchange(
      const RowTypePtr& outputType,
      const std::vector<std::string>& keys,
      VectorSerde::Kind serdekind);

  /// Add a ProjectNode using specified SQL expressions.
  ///
  /// For example,
  ///
  ///     .project({"a + b", "c * 3"})
  ///
  /// The names of the projections can be specified using SQL statement AS:
  ///
  ///     .project({"a + b AS sum_ab", "c * 3 AS triple_c"})
  ///
  /// If AS statement is not used, the names of the projections will be
  /// generated as p0, p1, p2, etc. Names of columns projected as is will be
  /// preserved.
  ///
  /// For example,
  ///
  ///     project({"a + b AS sum_ab", "c", "d * 7")
  ///
  /// will produce projected columns named sum_ab, c and p2.
  PlanBuilder& project(const std::vector<std::string>& projections);

  /// Add a ProjectNode to keep all existing columns and append more columns
  /// using specified expressions.
  /// @param newColumns A list of one or more expressions to use for computing
  /// additional columns.
  PlanBuilder& appendColumns(const std::vector<std::string>& newColumns);

  /// Variation of project that takes untyped expressions.  Used for access
  /// deeply nested types, in which case Duck DB often fails to parse or infer
  /// the type.
  PlanBuilder& projectExpressions(
      const std::vector<core::ExprPtr>& projections);

  /// Similar to project() except 'optionalProjections' could be empty and the
  /// function will skip creating a ProjectNode in that case.
  PlanBuilder& optionalProject(
      const std::vector<std::string>& optionalProjections);

  /// Add a FilterNode using specified SQL expression.
  ///
  /// @param filter SQL expression of type boolean.
  PlanBuilder& filter(const std::string& filter);

  /// Similar to filter() except 'optionalFilter' could be empty and the
  /// function will skip creating a FilterNode in that case.
  PlanBuilder& optionalFilter(const std::string& optionalFilter);

  /// Adds a TableWriteNode to write all input columns into an un-partitioned
  /// un-bucketed Hive table without compression.
  ///
  /// @param outputDirectoryPath Path to a directory to write data to.
  /// @param fileFormat File format to use for the written data.
  /// @param aggregates Aggregations for column statistics collection during
  /// @param polymorphic options object to be passed to the writer.
  /// write, supported aggregation types vary for different column types.
  /// @param outputFileName Optional file name of the output. If specified
  /// (non-empty), use it instead of generating the file name in Velox. Should
  /// only be specified in non-bucketing write.
  /// For example:
  /// Boolean: count, countIf.
  /// NumericType/Date/Timestamp: min, max, approx_distinct, count.
  /// Varchar: count, approx_distinct, sum_data_size_for_stats,
  /// max_data_size_for_stats.
  PlanBuilder& tableWrite(
      const std::string& outputDirectoryPath,
      const dwio::common::FileFormat fileFormat =
          dwio::common::FileFormat::DWRF,
      const std::vector<std::string>& aggregates = {},
      const std::shared_ptr<dwio::common::WriterOptions>& options = nullptr,
      const std::string& outputFileName = "");

  /// Adds a TableWriteNode to write all input columns into a partitioned Hive
  /// table without compression.
  ///
  /// @param outputDirectoryPath Path to a directory to write data to.
  /// @param partitionBy Specifies the partition key columns.
  /// @param fileFormat File format to use for the written data.
  /// @param aggregates Aggregations for column statistics collection during
  /// write.
  /// @param polymorphic options object to be passed to the writer.
  PlanBuilder& tableWrite(
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionBy,
      const dwio::common::FileFormat fileFormat =
          dwio::common::FileFormat::DWRF,
      const std::vector<std::string>& aggregates = {},
      const std::shared_ptr<dwio::common::WriterOptions>& options = nullptr);

  /// Adds a TableWriteNode to write all input columns into a non-sorted
  /// bucketed Hive table without compression.
  ///
  /// @param outputDirectoryPath Path to a directory to write data to.
  /// @param partitionBy Specifies the partition key columns.
  /// @param bucketCount Specifies the bucket count.
  /// @param bucketedBy Specifies the bucket by columns.
  /// @param fileFormat File format to use for the written data.
  /// @param aggregates Aggregations for column statistics collection during
  /// write.
  /// @param polymorphic options object to be passed to the writer.
  PlanBuilder& tableWrite(
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionBy,
      int32_t bucketCount,
      const std::vector<std::string>& bucketedBy,
      const dwio::common::FileFormat fileFormat =
          dwio::common::FileFormat::DWRF,
      const std::vector<std::string>& aggregates = {},
      const std::shared_ptr<dwio::common::WriterOptions>& options = nullptr);

  /// Adds a TableWriteNode to write all input columns into a sorted bucket Hive
  /// table without compression.
  ///
  /// @param outputDirectoryPath Path to a directory to write data to.
  /// @param partitionBy Specifies the partition key columns.
  /// @param bucketCount Specifies the bucket count.
  /// @param bucketedBy Specifies the bucket by columns.
  /// @param sortBy Specifies the sort by columns.
  /// @param fileFormat File format to use for the written data.
  /// @param aggregates Aggregations for column statistics collection during
  /// write.
  /// @param connectorId Name used to register the connector.
  /// @param serdeParameters Additional parameters passed to the writer.
  /// @param Option objects passed to the writer.
  /// @param outputFileName Optional file name of the output. If specified
  /// (non-empty), use it instead of generating the file name in Velox. Should
  /// only be specified in non-bucketing write.
  /// @param compressionKind Compression scheme to use for writing the
  /// output data files.
  /// @param schema Output schema to be passed to the writer. By default use the
  /// output of the previous operator.
  /// @param ensureFiles When this option is set the HiveDataSink will always
  /// create a file even if there is no data.
  PlanBuilder& tableWrite(
      const std::string& outputDirectoryPath,
      const std::vector<std::string>& partitionBy,
      int32_t bucketCount,
      const std::vector<std::string>& bucketedBy,
      const std::vector<
          std::shared_ptr<const connector::hive::HiveSortingColumn>>& sortBy,
      const dwio::common::FileFormat fileFormat =
          dwio::common::FileFormat::DWRF,
      const std::vector<std::string>& aggregates = {},
      const std::string_view& connectorId = kHiveDefaultConnectorId,
      const std::unordered_map<std::string, std::string>& serdeParameters = {},
      const std::shared_ptr<dwio::common::WriterOptions>& options = nullptr,
      const std::string& outputFileName = "",
      const common::CompressionKind = common::CompressionKind_NONE,
      const RowTypePtr& schema = nullptr,
      const bool ensureFiles = false);

  /// Add a TableWriteMergeNode.
  PlanBuilder& tableWriteMerge(
      const std::shared_ptr<core::AggregationNode>& aggregationNode = nullptr);

  /// Add an AggregationNode representing partial aggregation with the
  /// specified grouping keys, aggregates and optional masks.
  ///
  /// Aggregates are specified as function calls over unmodified input columns,
  /// e.g. sum(a), avg(b), min(c). SQL statement AS can be used to specify names
  /// for the aggregation result columns. In the absence of AS statement, result
  /// columns are named a0, a1, a2, etc.
  ///
  /// For example,
  ///
  ///     partialAggregation({}, {"min(a) AS min_a", "max(b)"})
  ///
  /// will produce output columns min_a and a1, while
  ///
  ///     partialAggregation({"k1", "k2"}, {"min(a) AS min_a", "max(b)"})
  ///
  /// will produce output columns k1, k2, min_a and a1, assuming the names of
  /// the first two input columns are k1 and k2.
  PlanBuilder& partialAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks = {}) {
    return aggregation(
        groupingKeys,
        {},
        aggregates,
        masks,
        core::AggregationNode::Step::kPartial,
        false);
  }

  /// Add final aggregation plan node to match the current partial aggregation
  /// node. Should be called directly after partialAggregation() method or
  /// directly after intermediateAggregation() that follows
  /// partialAggregation(). Can be called also if there is a local exchange
  /// after partial or intermediate aggregation.
  PlanBuilder& finalAggregation();

  /// Add final aggregation plan node using specified grouping keys, aggregate
  /// expressions and their types.
  ///
  /// @param rawInputTypes Raw input types for the aggregate functions.
  PlanBuilder& finalAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::vector<TypePtr>>& rawInputTypes) {
    return aggregation(
        groupingKeys,
        {},
        aggregates,
        {},
        core::AggregationNode::Step::kFinal,
        false,
        rawInputTypes);
  }

  /// Add intermediate aggregation plan node to match the current partial
  /// aggregation node. Should be called directly after partialAggregation()
  /// method. Can be called also if there is a local exchange after partial
  /// aggregation.
  PlanBuilder& intermediateAggregation();

  /// Add intermediate aggregation plan node using specified grouping keys,
  /// aggregate expressions and their types.
  PlanBuilder& intermediateAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates) {
    return aggregation(
        groupingKeys,
        {},
        aggregates,
        {},
        core::AggregationNode::Step::kIntermediate,
        false);
  }

  /// Add a single aggregation plan node using specified grouping keys and
  /// aggregate expressions. See 'partialAggregation' method for the supported
  /// types of aggregate expressions.
  PlanBuilder& singleAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks = {}) {
    return aggregation(
        groupingKeys,
        {},
        aggregates,
        masks,
        core::AggregationNode::Step::kSingle,
        false);
  }

  /// Add an AggregationNode using specified grouping keys,
  /// aggregate expressions and masks. See 'partialAggregation' method for the
  /// supported types of aggregate expressions.
  ///
  /// @param groupingKeys A list of grouping keys. Can be empty for global
  /// aggregations.
  /// @param aggregates A list of aggregate expressions. Must contain at least
  /// one expression.
  /// @param masks An optional list of boolean input columns to use as masks for
  /// the aggregates. Can be empty or have fewer elements than 'aggregates' or
  /// have some elements being empty strings. Non-empty elements must refer to a
  /// boolean input column, which will be used to mask a corresponding
  /// aggregate, e.g. aggregate will skip rows where 'mask' column is false.
  /// @param step Aggregation step: partial, final, intermediate or single.
  /// @param ignoreNullKeys Boolean indicating whether to skip input rows where
  /// one of the grouping keys is null.
  PlanBuilder& aggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      core::AggregationNode::Step step,
      bool ignoreNullKeys) {
    return aggregation(
        groupingKeys, {}, aggregates, masks, step, ignoreNullKeys);
  }

  /// Same as above, but also allows to specify a subset of grouping keys on
  /// which the input is pre-grouped or clustered. Pre-grouped keys enable
  /// streaming or partially streaming aggregation algorithms which use less
  /// memory and CPU then hash aggregation. The caller is responsible
  /// that input data is indeed clustered on the specified keys. If that's not
  /// the case, the query may return incorrect results.
  PlanBuilder& aggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& preGroupedKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      core::AggregationNode::Step step,
      bool ignoreNullKeys) {
    return aggregation(
        groupingKeys,
        preGroupedKeys,
        aggregates,
        masks,
        step,
        ignoreNullKeys,
        {});
  }

  /// A convenience method to create partial aggregation plan node for the case
  /// where input is clustered on all grouping keys.
  PlanBuilder& partialStreamingAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks = {}) {
    return streamingAggregation(
        groupingKeys,
        aggregates,
        masks,
        core::AggregationNode::Step::kPartial,
        false);
  }

  /// A convenience method to create final aggregation plan node for the case
  /// where input is clustered on all grouping keys.
  PlanBuilder& finalStreamingAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates) {
    return streamingAggregation(
        groupingKeys,
        aggregates,
        {},
        core::AggregationNode::Step::kFinal,
        false);
  }

  /// Add an AggregationNode assuming input is clustered on all grouping keys.
  PlanBuilder& streamingAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      core::AggregationNode::Step step,
      bool ignoreNullKeys);

  /// Add a GroupIdNode using the specified grouping keys, grouping sets,
  /// aggregation inputs and a groupId column name.
  /// The grouping keys can specify aliases if an input column is mapped
  /// to an output column with a different name.
  /// e.g. Grouping keys {"k1", "k1 as k2"} means there are 2 grouping keys:
  /// the input column k1 and output column k2 which is an alias of column k1.
  /// Grouping sets using above grouping keys use the output column aliases.
  /// e.g. Grouping sets in the above case could be {{"k1"}, {"k2"}, {}}
  /// The GroupIdNode output columns have grouping keys in the order specified
  /// in groupingKeys variable.
  PlanBuilder& groupId(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::vector<std::string>>& groupingSets,
      const std::vector<std::string>& aggregationInputs,
      std::string groupIdName = "group_id");

  /// Add an ExpandNode using specified projections. See comments for
  /// ExpandNode class for description of this plan node.
  ///
  /// @param projections A list of projection expressions. Each expression is
  /// either a column name, null or non-null constant.
  ///
  /// For example,
  ///
  ///     .expand(
  ///            {{"k1", "null:: bigint k2", "a", "b", "0 as gid"}, //
  ///            Column name will be extracted from the first projection. If the
  ///            column is null, it is also necessary to specify the column
  ///            type.
  ///             {"k1", "null", "a", "b", "1"},
  ///             {"null", "null", "a", "b", "2"}})
  ///
  ///
  PlanBuilder& expand(const std::vector<std::vector<std::string>>& projections);

  /// Add a LocalMergeNode using specified ORDER BY clauses.
  ///
  /// For example,
  ///
  ///     .localMerge({"a", "b DESC", "c ASC NULLS FIRST"})
  ///
  /// By default, uses ASC NULLS LAST sort order, e.g. column "a" above will use
  /// ASC NULLS LAST and column "b" will use DESC NULLS LAST.
  PlanBuilder& localMerge(
      const std::vector<std::string>& keys,
      std::vector<core::PlanNodePtr> sources);

  /// A convenience method to add a LocalMergeNode with a single source (the
  /// current plan node).
  PlanBuilder& localMerge(const std::vector<std::string>& keys);

  /// Adds an OrderByNode using specified ORDER BY clauses.
  ///
  /// For example,
  ///
  ///     .orderBy({"a", "b DESC", "c ASC NULLS FIRST"})
  ///
  /// By default, uses ASC NULLS LAST sort order, e.g. column "a" above will use
  /// ASC NULLS LAST and column "b" will use DESC NULLS LAST.
  PlanBuilder& orderBy(const std::vector<std::string>& keys, bool isPartial);

  /// Add a TopNNode using specified N and ORDER BY clauses.
  ///
  /// For example,
  ///
  ///     .topN({"a", "b DESC", "c ASC NULLS FIRST"}, 10, true)
  ///
  /// By default, uses ASC NULLS LAST sort order, e.g. column "a" above will use
  /// ASC NULLS LAST and column "b" will use DESC NULLS LAST.
  PlanBuilder&
  topN(const std::vector<std::string>& keys, int32_t count, bool isPartial);

  /// Add a LimitNode.
  ///
  /// @param offset Offset, i.e. number of rows of input to skip.
  /// @param count Maximum number of rows to produce after skipping 'offset'
  /// rows.
  /// @param isPartial Boolean indicating whether the limit node is partial or
  /// final. Partial limit can run multi-threaded. Final limit must run
  /// single-threaded.
  PlanBuilder& limit(int64_t offset, int64_t count, bool isPartial);

  /// Add an EnforceSingleRowNode to ensure input has at most one row at
  /// runtime.
  PlanBuilder& enforceSingleRow();

  /// Add an AssignUniqueIdNode to add a column with query-scoped unique value
  /// per row.
  ///
  /// @param idName The name of output column that contains the unique ID.
  /// Column type is assumed as BIGINT.
  /// @param taskUniqueId ID of the Task that will be used to run the query
  /// plan. The ID must be unique across all the tasks of a single query. Tasks
  /// may possibly run on different machines.
  PlanBuilder& assignUniqueId(
      const std::string& idName = "unique",
      const int32_t taskUniqueId = 1);

  /// Add a PartitionedOutputNode to hash-partition the input on the specified
  /// keys using exec::HashPartitionFunction.
  ///
  /// @param keys Partitioning keys. May be empty, in which case all input will
  /// be places in a single partition.
  /// @param numPartitions Number of partitions. Must be greater than or equal
  /// to 1. Keys must not be empty if greater than 1.
  /// @param replicateNullsAndAny Boolean indicating whether to replicate one
  /// arbitrary entry and all entries with null keys to all partitions. Used to
  /// implement proper ANTI join semantics in a distributed execution
  /// environment.
  /// @param outputLayout Optional output layout in case it is different then
  /// the input. Output columns may appear in different order from the input,
  /// some input columns may be missing in the output, some columns may be
  /// duplicated in the output.
  PlanBuilder& partitionedOutput(
      const std::vector<std::string>& keys,
      int numPartitions,
      bool replicateNullsAndAny,
      const std::vector<std::string>& outputLayout = {},
      VectorSerde::Kind serdeKind = VectorSerde::Kind::kPresto);

  /// Same as above, but assumes 'replicateNullsAndAny' is false.
  PlanBuilder& partitionedOutput(
      const std::vector<std::string>& keys,
      int numPartitions,
      const std::vector<std::string>& outputLayout = {},
      VectorSerde::Kind serdeKind = VectorSerde::Kind::kPresto);

  /// Same as above, but allows to provide custom partition function.
  PlanBuilder& partitionedOutput(
      const std::vector<std::string>& keys,
      int numPartitions,
      bool replicateNullsAndAny,
      core::PartitionFunctionSpecPtr partitionFunctionSpec,
      const std::vector<std::string>& outputLayout = {},
      VectorSerde::Kind serdeKind = VectorSerde::Kind::kPresto);

  /// Adds a PartitionedOutputNode to broadcast the input data.
  ///
  /// @param outputLayout Optional output layout in case it is different then
  /// the input. Output columns may appear in different order from the input,
  /// some input columns may be missing in the output, some columns may be
  /// duplicated in the output.
  PlanBuilder& partitionedOutputBroadcast(
      const std::vector<std::string>& outputLayout = {},
      VectorSerde::Kind serdeKind = VectorSerde::Kind::kPresto);

  /// Adds a PartitionedOutputNode to put data into arbitrary buffer.
  PlanBuilder& partitionedOutputArbitrary(
      const std::vector<std::string>& outputLayout = {},
      VectorSerde::Kind serdeKind = VectorSerde::Kind::kPresto);

  /// Adds a LocalPartitionNode to hash-partition the input on the specified
  /// keys using exec::HashPartitionFunction. Number of partitions is determined
  /// at runtime based on parallelism of the downstream pipeline.
  ///
  /// @param keys Partitioning keys. May be empty, in which case all input will
  /// be places in a single partition.
  /// @param sources One or more plan nodes that produce input data.
  PlanBuilder& localPartition(
      const std::vector<std::string>& keys,
      const std::vector<core::PlanNodePtr>& sources);

  /// A convenience method to add a LocalPartitionNode with a single source (the
  /// current plan node).
  PlanBuilder& localPartition(const std::vector<std::string>& keys);

  /// A convenience method to add a LocalPartitionNode with hive partition
  /// function.
  PlanBuilder& localPartition(
      int numBuckets,
      const std::vector<column_index_t>& channels,
      const std::vector<VectorPtr>& constValues);

  /// A convenience method to add a LocalPartitionNode with a single source (the
  /// current plan node) and hive bucket property.
  PlanBuilder& localPartitionByBucket(
      const std::shared_ptr<connector::hive::HiveBucketProperty>&
          bucketProperty);

  /// Add a LocalPartitionNode to partition the input using batch-level
  /// round-robin. Number of partitions is determined at runtime based on
  /// parallelism of the downstream pipeline.
  ///
  /// @param sources One or more plan nodes that produce input data.
  PlanBuilder& localPartitionRoundRobin(
      const std::vector<core::PlanNodePtr>& sources);

  /// A convenience method to add a LocalPartitionNode with a single source (the
  /// current plan node).
  PlanBuilder& localPartitionRoundRobin();

  /// A convenience method to add a LocalPartitionNode for scale writer with
  /// hash partitioning.
  PlanBuilder& scaleWriterlocalPartition(const std::vector<std::string>& keys);

  /// A convenience method to add a LocalPartitionNode for scale writer with
  /// round-robin partitioning.
  PlanBuilder& scaleWriterlocalPartitionRoundRobin();

  /// Add a LocalPartitionNode to partition the input using row-wise
  /// round-robin. Number of partitions is determined at runtime based on
  /// parallelism of the downstream pipeline.
  PlanBuilder& localPartitionRoundRobinRow();

  /// Add a HashJoinNode to join two inputs using one or more join keys and an
  /// optional filter.
  ///
  /// @param leftKeys Join keys from the probe side, the preceding plan node.
  /// Cannot be empty.
  /// @param rightKeys Join keys from the build side, the plan node specified in
  /// 'build' parameter. The number and types of left and right keys must be the
  /// same.
  /// @param build Plan node for the build side. Typically, to reduce memory
  /// usage, the smaller input is placed on the build-side.
  /// @param filter Optional SQL expression for the additional join filter. Can
  /// use columns from both probe and build sides of the join.
  /// @param outputLayout Output layout consisting of columns from probe and
  /// build sides.
  /// @param joinType Type of the join: inner, left, right, full, semi, or anti.
  /// @param nullAware Applies to semi and anti joins. Indicates whether the
  /// join follows IN (null-aware) or EXISTS (regular) semantic.
  PlanBuilder& hashJoin(
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const core::PlanNodePtr& build,
      const std::string& filter,
      const std::vector<std::string>& outputLayout,
      core::JoinType joinType = core::JoinType::kInner,
      bool nullAware = false);

  /// Add a MergeJoinNode to join two inputs using one or more join keys and an
  /// optional filter. The caller is responsible to ensure that inputs are
  /// sorted in ascending order on the join keys. If that's not the case, the
  /// query may produce incorrect results.
  ///
  /// See hashJoin method for the description of the parameters.
  PlanBuilder& mergeJoin(
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const core::PlanNodePtr& build,
      const std::string& filter,
      const std::vector<std::string>& outputLayout,
      core::JoinType joinType = core::JoinType::kInner);

  /// Add a NestedLoopJoinNode to join two inputs using filter as join
  /// condition to perform equal/non-equal join. Only supports inner/outer
  /// joins.
  ///
  /// @param right Right-side input. Typically, to reduce memory usage, the
  /// smaller input is placed on the right-side.
  /// @param joinCondition SQL expression as the join condition. Can
  /// use columns from both probe and build sides of the join.
  /// @param outputLayout Output layout consisting of columns from probe and
  /// build sides.
  /// @param joinType Type of the join: inner, left, right, full.
  PlanBuilder& nestedLoopJoin(
      const core::PlanNodePtr& right,
      const std::string& joinCondition,
      const std::vector<std::string>& outputLayout,
      core::JoinType joinType = core::JoinType::kInner);

  /// Add a NestedLoopJoinNode to produce a cross product of the inputs. First
  /// input comes from the preceding plan node. Second input is specified in
  /// 'right' parameter.
  ///
  /// @param right Right-side input. Typically, to reduce memory usage, the
  /// smaller input is placed on the right-side.
  /// @param outputLayout Output layout consisting of columns from left and
  /// right sides.
  PlanBuilder& nestedLoopJoin(
      const core::PlanNodePtr& right,
      const std::vector<std::string>& outputLayout,
      core::JoinType joinType = core::JoinType::kInner);

  static core::IndexLookupConditionPtr parseIndexJoinCondition(
      const std::string& joinCondition,
      const RowTypePtr& rowType,
      memory::MemoryPool* pool);

  /// Add an IndexLoopJoinNode to join two inputs using one or more join keys
  /// plus optional join conditions. First input comes from the preceding plan
  /// node. Second input is specified in 'right' parameter and must be a
  /// table source with the connector table handle with index lookup support.
  ///
  /// @param right The right input source with index lookup support.
  /// @param joinConditions SQL expressions as the join conditions. Each join
  /// condition must use columns from both sides. For the right side, it can
  /// only use one index column. Currently we support "in" and "between" join
  /// conditions:
  /// "in" condition is written as SQL expression as "contains(a, b)" where "b"
  /// is the index column from right side and "a" is the condition column from
  /// left side. "b" has type T and "a" has type ARRAT(T).
  /// "between" condition is written as SQL expression as "a between b and c"
  /// where "a" is the index column from right side and "b", "c" are either
  /// condition column from left side or a constant but at least one of them
  /// must not be constant. They all have the same type.
  /// @param joinType Type of the join supported: inner, left.
  ///
  /// See hashJoin method for the description of the other parameters.
  PlanBuilder& indexLookupJoin(
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const core::TableScanNodePtr& right,
      const std::vector<std::string>& joinConditions,
      const std::vector<std::string>& outputLayout,
      core::JoinType joinType = core::JoinType::kInner);

  /// Add an UnnestNode to unnest one or more columns of type array or map.
  ///
  /// The output will contain 'replicatedColumns' followed by unnested columns,
  /// followed by an optional ordinality column.
  ///
  /// Array columns are unnested into a single column whose name is generated by
  /// appending '_e' suffix to the array column name.
  ///
  /// Map columns are unnested into two columns whoes names are generated by
  /// appending '_k' and '_v' suffixes to the map column name.
  ///
  /// @param replicateColumns A subset of input columns to include in the output
  /// unmodified.
  /// @param unnestColumns A subset of input columns to unnest. These columns
  /// must be of type array or map.
  /// @param ordinalColumn An optional name for the 'ordinal' column to produce.
  /// This column contains the index of the element of the unnested array or
  /// map. If not specified, the output will not contain this column.
  PlanBuilder& unnest(
      const std::vector<std::string>& replicateColumns,
      const std::vector<std::string>& unnestColumns,
      const std::optional<std::string>& ordinalColumn = std::nullopt);

  /// Add a WindowNode to compute one or more windowFunctions.
  /// @param windowFunctions A list of one or more window function SQL like
  /// strings to be computed by this windowNode.
  /// A window function SQL string looks like :
  /// "name(parameters) OVER (PARTITION BY partition_keys ORDER BY
  /// sorting_keys [ROWS|RANGE BETWEEN [UNBOUNDED PRECEDING | x PRECEDING |
  /// CURRENT ROW] AND [UNBOUNDED FOLLOWING | x FOLLOWING | CURRENT ROW]] AS
  /// columnName"
  /// The PARTITION BY and ORDER BY clauses are optional. An empty PARTITION
  /// list means all the table rows are in a single partition.
  /// An empty ORDER BY list means the window functions will be computed over
  /// all the rows in the partition in a random order. Also, the default frame
  /// if unspecified is RANGE OVER UNBOUNDED PRECEDING AND CURRENT ROW.
  /// Some examples of window function strings are as follows:
  /// "first_value(c) over (partition by a order by b) as d"
  /// "first_value(c) over (partition by a) as d"
  /// "first_value(c) over ()"
  /// "row_number() over (order by b) as a"
  /// "row_number() over (partition by a order by b
  ///  rows between a + 10 preceding and 10 following)"
  PlanBuilder& window(const std::vector<std::string>& windowFunctions);

  /// Adds WindowNode to compute window functions over pre-sorted inputs.
  /// All functions must use same partition by and sorting keys and input must
  /// be already sorted on these.
  PlanBuilder& streamingWindow(const std::vector<std::string>& windowFunctions);

  /// Add a RowNumberNode to compute single row_number window function with an
  /// optional limit and no sorting.
  PlanBuilder& rowNumber(
      const std::vector<std::string>& partitionKeys,
      std::optional<int32_t> limit = std::nullopt,
      bool generateRowNumber = true);

  /// Add a TopNRowNumberNode to compute single row_number window function with
  /// a limit applied to sorted partitions.
  PlanBuilder& topNRowNumber(
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& sortingKeys,
      int32_t limit,
      bool generateRowNumber);

  /// Add a MarkDistinctNode to compute aggregate mask channel
  /// @param markerKey Name of output mask channel
  /// @param distinctKeys List of columns to be marked distinct.
  PlanBuilder& markDistinct(
      std::string markerKey,
      const std::vector<std::string>& distinctKeys);

  /// Stores the latest plan node ID into the specified variable. Useful for
  /// capturing IDs of the leaf plan nodes (table scans, exchanges, etc.) to use
  /// when adding splits at runtime.
  PlanBuilder& capturePlanNodeId(core::PlanNodeId& id) {
    VELOX_CHECK_NOT_NULL(planNode_);
    id = planNode_->id();
    return *this;
  }

  /// Stores the latest plan node into the specified variable. Useful for
  /// capturing intermediate plan nodes without interrupting the build flow.
  template <typename T = core::PlanNode>
  PlanBuilder& capturePlanNode(std::shared_ptr<const T>& planNode) {
    VELOX_CHECK_NOT_NULL(planNode_);
    planNode = std::dynamic_pointer_cast<const T>(planNode_);
    VELOX_CHECK_NOT_NULL(planNode);
    return *this;
  }

  /// Return the latest plan node, e.g. the root node of the plan
  /// tree. The DistributedPlanBuilder override additionally moves stage
  /// information to a parent PlanBuilder.
  const core::PlanNodePtr& planNode() const {
    return planNode_;
  }

  /// Return tha latest plan node wrapped in core::PlanFragment struct.
  core::PlanFragment planFragment() const {
    return core::PlanFragment{planNode_};
  }

  /// Add a user-defined PlanNode as the root of the plan. 'func' takes
  /// the current root of the plan and returns the new root.
  PlanBuilder& addNode(
      std::function<core::PlanNodePtr(std::string nodeId, core::PlanNodePtr)>
          func) {
    planNode_ = func(nextPlanNodeId(), planNode_);
    return *this;
  }

  /// Set parsing options
  PlanBuilder& setParseOptions(const parse::ParseOptions& options) {
    options_ = options;
    return *this;
  }

  /// In a DistributedPlanBuilder, introduces a shuffle boundary. The plan so
  /// far is shuffled and subsequent nodes consume the shuffle. Arguments are as
  /// in partitionedOutput().
  virtual PlanBuilder& shufflePartitioned(
      const std::vector<std::string>& keys,
      int numPartitions,
      bool replicateNullsAndAny,
      const std::vector<std::string>& outputLayout = {}) {
    VELOX_UNSUPPORTED("Needs DistributedPlanBuilder");
  }

  /// In a DistributedPlanBuilder, returns an Exchange on top of the plan built
  /// so far and couples it to the current stage in the enclosing builder.
  /// Arguments are as in shuffle().
  virtual core::PlanNodePtr shufflePartitionedResult(
      const std::vector<std::string>& keys,
      int numPartitions,
      bool replicateNullsAndAny,
      const std::vector<std::string>& outputLayout = {}) {
    VELOX_UNSUPPORTED("Needs DistributedPlanBuilder");
  }

  /// In a DistributedPlanBuilder, returns an Exchange on top of the plan built
  /// so far that ends with a broadcast PartitionedOutput node, and couples the
  /// Exchange to the current stage in the enclosing builder.
  virtual core::PlanNodePtr shuffleBroadcastResult() {
    VELOX_UNSUPPORTED("Needs DistributedPlanBuilder");
  }

 protected:
  // Users who create custom operators might want to extend the PlanBuilder to
  // customize extended plan builders. Those functions are needed in such
  // extensions.
  core::PlanNodeId nextPlanNodeId();

  std::shared_ptr<const core::ITypedExpr> inferTypes(
      const core::ExprPtr& untypedExpr);

  std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator() const {
    return planNodeIdGenerator_;
  }

  memory::MemoryPool* pool() const {
    return pool_;
  }

 private:
  std::shared_ptr<const core::FieldAccessTypedExpr> field(column_index_t index);

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const std::vector<column_index_t>& indices);

  std::shared_ptr<const core::FieldAccessTypedExpr> field(
      const std::string& name);

  std::vector<core::TypedExprPtr> exprs(
      const std::vector<std::string>& expressions,
      const RowTypePtr& inputType);

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const std::vector<std::string>& names);

  static std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const RowTypePtr& inputType,
      const std::vector<std::string>& names);

  static std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const RowTypePtr& inputType,
      const std::vector<column_index_t>& indices);

  static std::shared_ptr<const core::FieldAccessTypedExpr> field(
      const RowTypePtr& inputType,
      column_index_t index);

  static std::shared_ptr<const core::FieldAccessTypedExpr> field(
      const RowTypePtr& inputType,
      const std::string& name);

  core::PlanNodePtr createIntermediateOrFinalAggregation(
      core::AggregationNode::Step step,
      const core::AggregationNode* partialAggNode);

  struct AggregatesAndNames {
    std::vector<core::AggregationNode::Aggregate> aggregates;
    std::vector<std::string> names;
  };

  AggregatesAndNames createAggregateExpressionsAndNames(
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      core::AggregationNode::Step step,
      const std::vector<std::vector<TypePtr>>& rawInputTypes = {});

  PlanBuilder& aggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& preGroupedKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      core::AggregationNode::Step step,
      bool ignoreNullKeys,
      const std::vector<std::vector<TypePtr>>& rawInputTypes);

  /// Create WindowNode based on whether input is sorted and then compute the
  /// window functions.
  PlanBuilder& window(
      const std::vector<std::string>& windowFunctions,
      bool inputSorted);

 protected:
  core::PlanNodePtr planNode_;
  parse::ParseOptions options_;
  std::shared_ptr<TableScanBuilder> tableScanBuilder_;
  std::shared_ptr<TableWriterBuilder> tableWriterBuilder_;

 private:
  std::shared_ptr<core::PlanNodeIdGenerator> planNodeIdGenerator_;
  memory::MemoryPool* pool_;
};
} // namespace facebook::velox::exec::test
