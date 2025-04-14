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

#include "velox/connectors/Connector.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/parse/PlanNodeIdGenerator.h"

namespace fecebook::velox::exec::test {
class IndexLookupJoinTestBase
    : public facebook::velox::exec::test::HiveConnectorTestBase {
 protected:
  IndexLookupJoinTestBase() = default;

  struct SequenceTableData {
    facebook::velox::RowVectorPtr keyData;
    facebook::velox::RowVectorPtr valueData;
    facebook::velox::RowVectorPtr tableData;
    std::vector<int64_t> minKeys;
    std::vector<int64_t> maxKeys;
  };

  static facebook::velox::RowTypePtr concat(
      const facebook::velox::RowTypePtr& a,
      const facebook::velox::RowTypePtr& b);

  bool isFilter(const std::string& conditionSql) const;

  int getNumRows(const std::vector<int>& cardinalities);

  // Generate probe input for lookup join.
  // @param numBatches: number of probe batches.
  // @param batchSize: number of rows in each probe batch.
  // @param numDuplicateProbeRows: number of duplicates for each probe row so
  // the actual batch size is batchSize * numDuplicatesProbeRows.
  // @param tableData: contains the sequence table data including key vectors
  // and min/max key values.
  // @param probeJoinKeys: the prefix key colums used for equality joins.
  // @param inColumns: the ordered list of in conditions.
  // @param betweenColumns: the ordered list of between conditions.
  // @param equalMatchPct: percentage of rows in the probe input that matches
  // with the rows in index table.
  // @param betweenMatchPct: percentage of rows in the probe input that matches
  // the rows in index table with between conditions.
  // @param inMatchPct: percentage of rows in the probe input that matches the
  // rows in index table with in conditions.
  std::vector<facebook::velox::RowVectorPtr> generateProbeInput(
      size_t numBatches,
      size_t batchSize,
      size_t numDuplicateProbeRows,
      SequenceTableData& tableData,
      std::shared_ptr<facebook::velox::memory::MemoryPool>& pool,
      const std::vector<std::string>& probeJoinKeys,
      const std::vector<std::string> inColumns = {},
      const std::vector<std::pair<std::string, std::string>>& betweenColumns =
          {},
      std::optional<int> equalMatchPct = std::nullopt,
      std::optional<int> inMatchPct = std::nullopt,
      std::optional<int> betweenMatchPct = std::nullopt);

  // Makes lookup join plan with the following parameters:
  // @param indexScanNode: the index table scan node.
  // @param probeVectors: the probe input vectors.
  // @param leftKeys: the left join keys of index lookup join.
  // @param rightKeys: the right join keys of index lookup join.
  // @param joinType: the join type of index lookup join.
  // @param outputColumns: the output column names of index lookup join.
  // @param joinNodeId: returns the plan node id of the index lookup join
  // node.
  facebook::velox::core::PlanNodePtr makeLookupPlan(
      const std::shared_ptr<facebook::velox::core::PlanNodeIdGenerator>&
          planNodeIdGenerator,
      facebook::velox::core::TableScanNodePtr indexScanNode,
      const std::vector<facebook::velox::RowVectorPtr>& probeVectors,
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const std::vector<std::string>& joinConditions,
      facebook::velox::core::JoinType joinType,
      const std::vector<std::string>& outputColumns,
      facebook::velox::core::PlanNodeId& joinNodeId);

  void createDuckDbTable(
      const std::string& tableName,
      const std::vector<facebook::velox::RowVectorPtr>& data);

  // Makes index table scan node with the specified index table handle.
  // @param outputType: the output schema of the index table scan node.
  // @param scanNodeId: returns the plan node id of the index table scan
  // node.
  facebook::velox::core::TableScanNodePtr makeIndexScanNode(
      const std::shared_ptr<facebook::velox::core::PlanNodeIdGenerator>&
          planNodeIdGenerator,
      const std::shared_ptr<facebook::velox::connector::ConnectorTableHandle>
          indexTableHandle,
      const facebook::velox::RowTypePtr& outputType,
      facebook::velox::core::PlanNodeId& scanNodeId,
      std::unordered_map<
          std::string,
          std::shared_ptr<facebook::velox::connector::ColumnHandle>>&
          assignments);

  // Generate sequence storage table which will be persisted by mock zippydb
  // client for testing.
  // @param keyCardinalities: specifies the number of unique keys per each index
  // column, which also determines the total number of rows stored in the
  // sequence storage table.
  // @param tableData: returns the sequence table data stats including the key
  // vector, value vector, table vector, and the min and max key values for each
  // index column.
  void generateIndexTableData(
      const std::vector<int>& keyCardinalities,
      SequenceTableData& tableData,
      std::shared_ptr<facebook::velox::memory::MemoryPool>& pool);

  // Makes output schema from the index table scan node with the specified
  // column names.
  facebook::velox::RowTypePtr makeScanOutputType(
      std::vector<std::string> outputNames);

  std::shared_ptr<facebook::velox::exec::Task> runLookupQuery(
      const facebook::velox::core::PlanNodePtr& plan,
      int numPrefetchBatches,
      const std::string& duckDbVefifySql);

  facebook::velox::RowTypePtr keyType_;
  std::optional<facebook::velox::RowTypePtr> partitionType_;
  facebook::velox::RowTypePtr valueType_;
  facebook::velox::RowTypePtr tableType_;
  facebook::velox::RowTypePtr probeType_;
};
} // namespace fecebook::velox::exec::test
