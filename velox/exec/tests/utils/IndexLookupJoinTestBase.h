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
#include "velox/connectors/Connector.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/parse/PlanNodeIdGenerator.h"

namespace fecebook::velox::exec::test {

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class IndexLookupJoinTestBase : public HiveConnectorTestBase {
 protected:
  IndexLookupJoinTestBase() = default;

  struct SequenceTableData {
    RowVectorPtr keyData;
    RowVectorPtr valueData;
    RowVectorPtr tableData;
    std::vector<int64_t> minKeys;
    std::vector<int64_t> maxKeys;
  };

  static RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b);

  bool isFilter(const std::string& conditionSql) const;

  int getNumRows(const std::vector<int>& cardinalities);

  /// Generate probe input for lookup join.
  /// @param numBatches: number of probe batches.
  /// @param batchSize: number of rows in each probe batch.
  /// @param numDuplicateProbeRows: number of duplicates for each probe row so
  /// the actual batch size is batchSize * numDuplicatesProbeRows.
  /// @param tableData: contains the sequence table data including key vectors
  /// and min/max key values.
  /// @param probeJoinKeys: the prefix key colums used for equality joins.
  /// @param hasNullKeys: whether the probe input has null keys.
  /// @param inColumns: the ordered list of in conditions.
  /// @param betweenColumns: the ordered list of between conditions.
  /// @param equalMatchPct: percentage of rows in the probe input that matches
  /// with the rows in index table.
  /// @param betweenMatchPct: percentage of rows in the probe input that matches
  /// the rows in index table with between conditions.
  /// @param inMatchPct: percentage of rows in the probe input that matches the
  /// rows in index table with in conditions.
  std::vector<RowVectorPtr> generateProbeInput(
      size_t numBatches,
      size_t batchSize,
      size_t numDuplicateProbeRows,
      SequenceTableData& tableData,
      std::shared_ptr<memory::MemoryPool>& pool,
      const std::vector<std::string>& probeJoinKeys,
      bool hasNullKeys = false,
      const std::vector<std::string>& inColumns = {},
      const std::vector<std::pair<std::string, std::string>>& betweenColumns =
          {},
      std::optional<int> equalMatchPct = std::nullopt,
      std::optional<int> inMatchPct = std::nullopt,
      std::optional<int> betweenMatchPct = std::nullopt);

  /// Makes lookup join plan with the following parameters:
  /// @param indexScanNode: the index table scan node.
  /// @param probeVectors: the probe input vectors.
  /// @param leftKeys: the left join keys of index lookup join.
  /// @param rightKeys: the right join keys of index lookup join.
  /// @param includeMatchColumn: whether the index join output includes a match
  /// column at the end.
  /// @param joinType: the join type of index lookup join.
  /// @param outputColumns: the output column names of index lookup join.
  /// @param joinNodeId: returns the plan node id of the index lookup join
  /// node.
  PlanNodePtr makeLookupPlan(
      const std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
      TableScanNodePtr indexScanNode,
      const std::vector<RowVectorPtr>& probeVectors,
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const std::vector<std::string>& joinConditions,
      bool includeMatchColumn,
      core::JoinType joinType,
      const std::vector<std::string>& outputColumns,
      core::PlanNodeId& joinNodeId);

  /// Makes lookup join plan with the following parameters:
  /// @param indexScanNode: the index table scan node.
  /// @param probeVectors: the probe input vectors.
  /// @param leftKeys: the left join keys of index lookup join.
  /// @param rightKeys: the right join keys of index lookup join.
  /// @param includeMatchColumn: whether the index join output includes a match
  /// column at the end.
  /// @param joinType: the join type of index lookup join.
  /// @param outputColumns: the output column names of index lookup join.
  /// @param joinNodeId: returns the plan node id of the index lookup join
  /// node.
  /// @param probeScanNodeId: returns the plan node id of the probe table scan
  PlanNodePtr makeLookupPlan(
      const std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
      TableScanNodePtr indexScanNode,
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const std::vector<std::string>& joinConditions,
      bool includeMatchColumn,
      JoinType joinType,
      const std::vector<std::string>& outputColumns);

  void createDuckDbTable(
      const std::string& tableName,
      const std::vector<RowVectorPtr>& data);

  /// Makes index table scan node with the specified index table handle.
  /// @param outputType: the output schema of the index table scan node.
  TableScanNodePtr makeIndexScanNode(
      const std::shared_ptr<PlanNodeIdGenerator>& planNodeIdGenerator,
      const connector::ConnectorTableHandlePtr& indexTableHandle,
      const RowTypePtr& outputType,
      const connector::ColumnHandleMap& assignments);

  /// Generate sequence storage table which will be persisted by mock zippydb
  /// client for testing.
  /// @param keyCardinalities: specifies the number of unique keys per each
  /// index column, which also determines the total number of rows stored in the
  /// sequence storage table.
  /// @param tableData: returns the sequence table data stats including the key
  /// vector, value vector, table vector, and the min and max key values for
  /// each index column.
  void generateIndexTableData(
      const std::vector<int>& keyCardinalities,
      SequenceTableData& tableData,
      std::shared_ptr<memory::MemoryPool>& pool);

  /// Write 'probeVectors' to a number of files with one per each file.
  std::vector<std::shared_ptr<TempFilePath>> createProbeFiles(
      const std::vector<RowVectorPtr>& probeVectors);

  /// Makes output schema from the index table scan node with the specified
  /// column names.
  RowTypePtr makeScanOutputType(std::vector<std::string> outputNames);

  std::shared_ptr<Task> runLookupQuery(
      const PlanNodePtr& plan,
      int numPrefetchBatches,
      const std::string& duckDbVefifySql);

  std::shared_ptr<Task> runLookupQuery(
      const PlanNodePtr& plan,
      const std::vector<std::shared_ptr<TempFilePath>>& probeFiles,
      bool serialExecution,
      bool barrierExecution,
      int maxBatchRows,
      int numPrefetchBatches,
      const std::string& duckDbVefifySql);

  /// Verifies the results of the index lookup join query with and without match
  /// column.
  void verifyResultWithMatchColumn(
      const PlanNodePtr& planWithoutMatchColumn,
      const PlanNodeId& probeScanNodeIdWithoutMatchColumn,
      const PlanNodePtr& planWithMatchColumn,
      const PlanNodeId& probeScanNodeIdWithMatchColumn,
      const std::vector<std::shared_ptr<TempFilePath>>& probeFiles);

  RowTypePtr keyType_;
  std::optional<RowTypePtr> partitionType_;
  RowTypePtr valueType_;
  RowTypePtr tableType_;
  RowTypePtr probeType_;
  PlanNodeId joinNodeId_;
  PlanNodeId indexScanNodeId_;
  PlanNodeId probeScanNodeId_;
};
} // namespace fecebook::velox::exec::test
