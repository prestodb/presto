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

namespace facebook::velox::core {
class IExpr;
}

namespace facebook::velox::tpch {
enum class Table : uint8_t;
}

namespace facebook::velox::exec::test {

/// Generates unique sequential plan node IDs starting with zero or specified
/// value.
class PlanNodeIdGenerator {
 public:
  explicit PlanNodeIdGenerator(int startId = 0) : nextId_{startId} {}

  int next() {
    return nextId_++;
  }

  void reset(int startId = 0) {
    nextId_ = startId;
  }

 private:
  int nextId_;
};

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
      std::shared_ptr<PlanNodeIdGenerator> planNodeIdGenerator,
      memory::MemoryPool* pool = nullptr)
      : planNodeIdGenerator_{std::move(planNodeIdGenerator)}, pool_{pool} {}

  /// Constructor with no required parameters suitable for creating
  /// straight-line (e.g. no joins) query plans.
  explicit PlanBuilder(memory::MemoryPool* pool = nullptr)
      : PlanBuilder(std::make_shared<PlanNodeIdGenerator>(), pool) {}

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
  PlanBuilder& tableScan(
      const RowTypePtr& outputType,
      const std::vector<std::string>& subfieldFilters = {},
      const std::string& remainingFilter = "");

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
  PlanBuilder& tableScan(
      const std::string& tableName,
      const RowTypePtr& outputType,
      const std::unordered_map<std::string, std::string>& columnAliases = {},
      const std::vector<std::string>& subfieldFilters = {},
      const std::string& remainingFilter = "");

  /// Add a TableScanNode using a connector-specific table handle and
  /// assignments. Supports any connector, not just Hive connector.
  ///
  /// @param outputType List of column names and types to project out. Column
  /// names should match the keys in the 'assignments' map. The 'assignments'
  /// map may contain more columns then 'outputType' if some columns are only
  /// used by pushed-down filters.
  PlanBuilder& tableScan(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& assignments);

  /// Add a TableScanNode to scan a TPC-H table.
  ///
  /// @param tpchTableHandle The handle that specifies the target TPC-H table
  /// and scale factor.
  /// @param columnNames The columns to be returned from that table.
  /// @param scaleFactor The TPC-H scale factor.
  PlanBuilder& tableScan(
      tpch::Table table,
      std::vector<std::string>&& columnNames,
      size_t scaleFactor = 1);

  /// Add a ValuesNode using specified data.
  ///
  /// @param values The data to use.
  /// @param parallelizable If true, ValuesNode can run multi-threaded, in which
  /// case it will produce duplicate data from each thread, e.g. each thread
  /// will return all the data in 'values'. Useful for testing.
  PlanBuilder& values(
      const std::vector<RowVectorPtr>& values,
      bool parallelizable = false);

  /// Add an ExchangeNode.
  ///
  /// Use capturePlanNodeId method to capture the node ID needed for adding
  /// splits.
  ///
  /// @param outputType The type of the data coming in and out of the exchange.
  PlanBuilder& exchange(const RowTypePtr& outputType);

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
      const std::vector<std::string>& keys);

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

  /// Add a FilterNode using specified SQL expression.
  ///
  /// @param filter SQL expression of type boolean.
  PlanBuilder& filter(const std::string& filter);

  /// Adds a TableWriteNode.
  ///
  /// @param inputColumns A subset of input columns to write.
  /// @param tableColumnNames Column names in the target table corresponding to
  /// inputColumns. The names may or may not match. tableColumnNames[i]
  /// corresponds to inputColumns[i].
  /// @param insertHandle Connector-specific table handle.
  /// @param rowCountColumnName The name of the output column containing the
  /// number of rows written.
  PlanBuilder& tableWrite(
      const RowTypePtr& inputColumns,
      const std::vector<std::string>& tableColumnNames,
      const std::shared_ptr<core::InsertTableHandle>& insertHandle,
      const std::string& rowCountColumnName = "rowCount");

  /// Add a TableWriteNode assuming that input columns names match column names
  /// in the target table.
  ///
  /// @param columnNames A subset of input columns to write.
  /// @param insertHandle Connector-specific table handle.
  /// @param rowCountColumnName The name of the output column containing the
  /// number of rows written.
  PlanBuilder& tableWrite(
      const std::vector<std::string>& columnNames,
      const std::shared_ptr<core::InsertTableHandle>& insertHandle,
      const std::string& rowCountColumnName = "rowCount");

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
  /// @param resultTypes Optional list of result types for the aggregates. Use
  /// it to specify the result types for aggregates which cannot infer result
  /// type solely from the types of the intermediate results. 'resultTypes' can
  /// be empty or have fewer elements than 'aggregates'. Elements that are
  /// present must be aligned with 'aggregates' though, e.g. resultTypes[i]
  /// specifies the result type for aggregates[i].
  PlanBuilder& finalAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<TypePtr>& resultTypes) {
    return aggregation(
        groupingKeys,
        {},
        aggregates,
        {},
        core::AggregationNode::Step::kFinal,
        false,
        resultTypes);
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
      const std::vector<std::string>& aggregates,
      const std::vector<TypePtr>& resultTypes) {
    return aggregation(
        groupingKeys,
        {},
        aggregates,
        {},
        core::AggregationNode::Step::kIntermediate,
        false,
        resultTypes);
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
  /// @param resultTypes Optional list of aggregate result types. Must be
  /// specified for intermediate and final aggregations where it is not possible
  /// to infer the result types based on input types. Not needed for partial and
  /// single aggregations.
  PlanBuilder& aggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      core::AggregationNode::Step step,
      bool ignoreNullKeys,
      const std::vector<TypePtr>& resultTypes = {}) {
    return aggregation(
        groupingKeys, {}, aggregates, masks, step, ignoreNullKeys, resultTypes);
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
      bool ignoreNullKeys,
      const std::vector<TypePtr>& resultTypes = {});

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
      const std::vector<std::string>& aggregates,
      const std::vector<TypePtr>& resultTypes = {}) {
    return streamingAggregation(
        groupingKeys,
        aggregates,
        {},
        core::AggregationNode::Step::kFinal,
        false,
        resultTypes);
  }

  /// Add an AggregationNode assuming input is clustered on all grouping keys.
  PlanBuilder& streamingAggregation(
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& masks,
      core::AggregationNode::Step step,
      bool ignoreNullKeys,
      const std::vector<TypePtr>& resultTypes = {});

  /// Add a GroupIdNode using the specified grouping sets, aggregation inputs
  /// and a groupId column name.
  PlanBuilder& groupId(
      const std::vector<std::vector<std::string>>& groupingSets,
      const std::vector<std::string>& aggregationInputs,
      std::string groupIdName = "group_id");

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
      std::vector<std::shared_ptr<const core::PlanNode>> sources);

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
  PlanBuilder& limit(int32_t offset, int32_t count, bool isPartial);

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
      const std::vector<std::string>& outputLayout = {});

  /// Same as above, but assumes 'replicateNullsAndAny' is false.
  PlanBuilder& partitionedOutput(
      const std::vector<std::string>& keys,
      int numPartitions,
      const std::vector<std::string>& outputLayout = {});

  /// Add a PartitionedOutputNode to broadcast the input data.
  ///
  /// @param outputLayout Optional output layout in case it is different then
  /// the input. Output columns may appear in different order from the input,
  /// some input columns may be missing in the output, some columns may be
  /// duplicated in the output.
  PlanBuilder& partitionedOutputBroadcast(
      const std::vector<std::string>& outputLayout = {});

  /// Add a LocalPartitionNode to hash-partition the input on the specified
  /// keys using exec::HashPartitionFunction. Number of partitions is determined
  /// at runtime based on parallelism of the downstream pipeline.
  ///
  /// @param keys Partitioning keys. May be empty, in which case all input will
  /// be places in a single partition.
  /// @param sources One or more plan nodes that produce input data.
  /// @param outputLayout Optional output layout in case it is different then
  /// the input. Output columns may appear in different order from the input,
  /// some input columns may be missing in the output, some columns may be
  /// duplicated in the output. Supports "col AS alias" syntax to change the
  /// names for the input columns.
  PlanBuilder& localPartition(
      const std::vector<std::string>& keys,
      const std::vector<std::shared_ptr<const core::PlanNode>>& sources,
      const std::vector<std::string>& outputLayout = {});

  /// Add a LocalPartitionNode to partition the input using row-wise
  /// round-robin. Number of partitions is determined at runtime based on
  /// parallelism of the downstream pipeline.
  ///
  /// @param sources One or more plan nodes that produce input data.
  /// @param outputLayout Optional output layout in case it is different then
  /// the input. Output columns may appear in different order from the input,
  /// some input columns may be missing in the output, some columns may be
  /// duplicated in the output. Supports "col AS alias" syntax to change the
  /// names for the input columns.
  PlanBuilder& localPartitionRoundRobin(
      const std::vector<std::shared_ptr<const core::PlanNode>>& sources,
      const std::vector<std::string>& outputLayout = {});

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
  PlanBuilder& hashJoin(
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const std::shared_ptr<core::PlanNode>& build,
      const std::string& filter,
      const std::vector<std::string>& outputLayout,
      core::JoinType joinType = core::JoinType::kInner);

  /// Add a MergeJoinNode to join two inputs using one or more join keys and an
  /// optional filter. The caller is responsible to ensure that inputs are
  /// sorted in ascending order on the join keys. If that's not the case, the
  /// query may produce incorrect results.
  ///
  /// See hashJoin method for the description of the parameters.
  PlanBuilder& mergeJoin(
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const std::shared_ptr<core::PlanNode>& build,
      const std::string& filter,
      const std::vector<std::string>& outputLayout,
      core::JoinType joinType = core::JoinType::kInner);

  /// Add a CrossJoinNode to produce a cross product of the inputs. First input
  /// comes from the preceding plan node. Second input is specified in 'right'
  /// parameter.
  ///
  /// @param right Right-side input. Typically, to reduce memory usage, the
  /// smaller input is placed on the right-side.
  /// @param outputLayout Output layout consisting of columns from left and
  /// right sides.
  PlanBuilder& crossJoin(
      const std::shared_ptr<core::PlanNode>& right,
      const std::vector<std::string>& outputLayout);

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

  /// Stores the latest plan node ID into the specified variable. Useful for
  /// capturing IDs of the leaf plan nodes (table scans, exchanges, etc.) to use
  /// when adding splits at runtime.
  PlanBuilder& capturePlanNodeId(core::PlanNodeId& id) {
    VELOX_CHECK_NOT_NULL(planNode_);
    id = planNode_->id();
    return *this;
  }

  /// Return the latest plan node, e.g. the root node of the plan tree.
  const std::shared_ptr<core::PlanNode>& planNode() const {
    return planNode_;
  }

  /// Return tha latest plan node wrapped in core::PlanFragment struct.
  core::PlanFragment planFragment() const {
    return core::PlanFragment{planNode_};
  }

  /// Add a user-defined PlanNode as the root of the plan. 'func' takes
  /// the current root of the plan and returns the new root.
  PlanBuilder& addNode(std::function<std::shared_ptr<core::PlanNode>(
                           std::string nodeId,
                           std::shared_ptr<const core::PlanNode>)> func) {
    planNode_ = func(nextPlanNodeId(), planNode_);
    return *this;
  }

 private:
  std::string nextPlanNodeId();

  std::shared_ptr<const core::FieldAccessTypedExpr> field(ChannelIndex index);

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const std::vector<ChannelIndex>& indices);

  std::shared_ptr<const core::FieldAccessTypedExpr> field(
      const std::string& name);

  std::vector<std::shared_ptr<const core::ITypedExpr>> exprs(
      const std::vector<std::string>& names);

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const std::vector<std::string>& names);

  static std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const RowTypePtr& inputType,
      const std::vector<std::string>& names);

  static std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields(
      const RowTypePtr& inputType,
      const std::vector<ChannelIndex>& indices);

  static std::shared_ptr<const core::FieldAccessTypedExpr> field(
      const RowTypePtr& inputType,
      ChannelIndex index);

  static std::shared_ptr<const core::FieldAccessTypedExpr> field(
      const RowTypePtr& inputType,
      const std::string& name);

  std::shared_ptr<core::PlanNode> createIntermediateOrFinalAggregation(
      core::AggregationNode::Step step,
      const core::AggregationNode* partialAggNode);

  std::shared_ptr<const core::ITypedExpr> inferTypes(
      const std::shared_ptr<const core::IExpr>& untypedExpr);

  struct AggregateExpressionsAndNames {
    std::vector<std::shared_ptr<const core::CallTypedExpr>> aggregates;
    std::vector<std::string> names;
  };

  AggregateExpressionsAndNames createAggregateExpressionsAndNames(
      const std::vector<std::string>& aggregates,
      core::AggregationNode::Step step,
      const std::vector<TypePtr>& resultTypes);

  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
  createAggregateMasks(
      size_t numAggregates,
      const std::vector<std::string>& masks);

  std::shared_ptr<PlanNodeIdGenerator> planNodeIdGenerator_;
  std::shared_ptr<core::PlanNode> planNode_;
  memory::MemoryPool* pool_;
};
} // namespace facebook::velox::exec::test
