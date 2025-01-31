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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/connectors/Connector.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TestIndexStorageConnector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::testutil;

namespace fecebook::velox::exec::test {
namespace {
class IndexLookupJoinTest : public HiveConnectorTestBase,
                            public testing::WithParamInterface<bool> {
 protected:
  IndexLookupJoinTest() = default;

  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    core::PlanNode::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    Type::registerSerDe();
    core::ITypedExpr::registerSerDe();
    connector::registerConnectorFactory(
        std::make_shared<TestIndexConnectorFactory>());
    std::shared_ptr<connector::Connector> connector =
        connector::getConnectorFactory(kTestIndexConnectorName)
            ->newConnector(
                kTestIndexConnectorName,
                {},
                nullptr,
                connectorCpuExecutor_.get());
    connector::registerConnector(connector);

    // TODO: extend to support multiple key columns.
    keyType_ = ROW({"u0"}, {BIGINT()});
    valueType_ = ROW({"u1", "u2", "u3"}, {BIGINT(), BIGINT(), VARCHAR()});
    tableType_ = ROW(
        {"u0", "u1", "u2", "u3"}, {BIGINT(), BIGINT(), BIGINT(), VARCHAR()});
    probeType_ = ROW(
        {"t0", "t1", "t2", "t3"}, {BIGINT(), BIGINT(), VARCHAR(), BIGINT()});

    TestIndexTableHandle::registerSerDe();
  }

  void TearDown() override {
    connector::unregisterConnectorFactory(kTestIndexConnectorName);
    HiveConnectorTestBase::TearDown();
  }

  void testSerde(const core::PlanNodePtr& plan) {
    auto serialized = plan->serialize();

    auto copy = ISerializable::deserialize<core::PlanNode>(serialized, pool());

    ASSERT_EQ(plan->toString(true, true), copy->toString(true, true));
  }

  // Generate index lookup table data.
  // @param numKeys: number of unique keys.
  // @param numDuplicatePerKey: number of duplicate rows per unique key so the
  //                            total number of rows in the table is numKeys *
  //                            numDuplicatePerKey.
  // @param keyData: output key column vectors. This is used to populate the
  //                 hash table in the index table.
  // @param valueData: output value column vectors. This is used to populate the
  //                   hash table in the index table.
  // @param tableData: output table column vectors including key and value. This
  //                   is used to populate duckdb table.
  void generateIndexTableData(
      size_t numKeys,
      size_t numDuplicatePerKey,
      RowVectorPtr& keyData,
      RowVectorPtr& valueData,
      RowVectorPtr& tableData) {
    const int numRows = numKeys * numDuplicatePerKey;
    VectorFuzzer::Options opts;
    opts.vectorSize = numRows;
    opts.nullRatio = 0.0;
    VectorFuzzer fuzzer(opts, pool_.get());

    keyData = fuzzer.fuzzInputFlatRow(keyType_);
    valueData = fuzzer.fuzzInputFlatRow(valueType_);
    keyData->childAt(0) = makeFlatVector<int64_t>(
        keyData->size(),
        [numDuplicatePerKey](auto row) { return row / numDuplicatePerKey; });
    std::vector<VectorPtr> tableColumns;
    VELOX_CHECK_EQ(tableType_->size(), keyType_->size() + valueType_->size());
    tableColumns.reserve(tableType_->size());
    for (auto i = 0; i < keyType_->size(); ++i) {
      tableColumns.push_back(keyData->childAt(i));
    }
    for (auto i = 0; i < valueType_->size(); ++i) {
      tableColumns.push_back(valueData->childAt(i));
    }
    tableData = makeRowVector(tableType_->names(), tableColumns);
  }

  // Generate probe input for lookup join.
  // @param numBatches: number of probe batches.
  // @param batchSize: number of rows in each probe batch.
  // @param tableKey: key column vectors of the index table. This is used to set
  //                  the key range for the probe input with specified lookup
  //                  match percentage for convenience.
  // @param matchPct: percentage of rows in the probe input that matches with
  // the rows in index table.
  std::vector<RowVectorPtr> generateProbeTableInput(
      size_t numBatches,
      size_t batchSize,
      const RowVectorPtr& tableKey,
      size_t matchPct) {
    VELOX_CHECK_LE(matchPct, 100);
    std::vector<RowVectorPtr> probeInputs;
    probeInputs.reserve(numBatches);
    VectorFuzzer::Options opts;
    opts.vectorSize = batchSize;
    // TODO: add nullable handling later.
    opts.nullRatio = 0.0;
    VectorFuzzer fuzzer(opts, pool_.get());
    for (int i = 0; i < numBatches; ++i) {
      probeInputs.push_back(fuzzer.fuzzInputRow(probeType_));
    }

    if (tableKey->size() == 0) {
      return probeInputs;
    }

    // Set the key range for the probe input either within or outside the table
    // key range based on the specified match percentage.
    auto* flatKeyVector = tableKey->childAt(0)->asFlatVector<int64_t>();
    const auto minKey = flatKeyVector->valueAt(0);
    const auto maxKey = flatKeyVector->valueAt(flatKeyVector->size() - 1);
    for (int i = 0, row = 0; i < numBatches; ++i) {
      probeInputs[i]->childAt(0)->loadedVector();
      BaseVector::flattenVector(probeInputs[i]->childAt(0));
      auto* flatProbeKeyVector =
          probeInputs[i]->childAt(0)->asFlatVector<int64_t>();
      VELOX_CHECK_NOT_NULL(flatProbeKeyVector);
      for (int j = 0; j < flatProbeKeyVector->size(); ++j, ++row) {
        if (row % 100 < matchPct) {
          flatProbeKeyVector->set(j, folly::Random::rand64(minKey, maxKey + 1));
        } else {
          flatProbeKeyVector->set(j, maxKey + 1 + folly::Random::rand32());
        }
      }
    }
    return probeInputs;
  }

  // Create index table with the given key and value inputs.
  std::shared_ptr<TestIndexTable> createIndexTable(
      const RowVectorPtr& keyData,
      const RowVectorPtr& valueData) {
    auto keyType = std::dynamic_pointer_cast<const RowType>(keyData->type());
    VELOX_CHECK_GE(keyType->size(), 1);
    auto valueType =
        std::dynamic_pointer_cast<const RowType>(valueData->type());
    VELOX_CHECK_GE(valueType->size(), 1);
    const auto numRows = keyData->size();
    VELOX_CHECK_EQ(numRows, valueData->size());

    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.reserve(keyType->size());
    for (auto i = 0; i < keyType->size(); ++i) {
      hashers.push_back(std::make_unique<VectorHasher>(keyType->childAt(i), i));
    }

    // Create the table.
    auto table = HashTable<false>::createForJoin(
        std::move(hashers),
        /*dependentTypes=*/valueType->children(),
        /*allowDuplicates=*/true,
        /*hasProbedFlag=*/false,
        /*minTableSizeForParallelJoinBuild=*/1,
        pool_.get());

    // Insert data into the row container.
    auto rowContainer = table->rows();
    std::vector<DecodedVector> decodedVectors;
    for (auto& vector : keyData->children()) {
      decodedVectors.emplace_back(*vector);
    }
    for (auto& vector : valueData->children()) {
      decodedVectors.emplace_back(*vector);
    }

    std::vector<char*> rows;
    for (auto row = 0; row < numRows; ++row) {
      auto* newRow = rowContainer->newRow();

      for (auto col = 0; col < decodedVectors.size(); ++col) {
        rowContainer->store(decodedVectors[col], row, newRow, col);
      }
    }

    // Build the table index.
    table->prepareJoinTable({}, BaseHashTable::kNoSpillInputStartPartitionBit);
    return std::make_shared<TestIndexTable>(
        std::move(keyType), std::move(valueType), std::move(table));
  }

  void createDuckDbTable(
      const std::string& tableName,
      const std::vector<RowVectorPtr>& data) {
    // Change each column with prefix 'c' to simplify the duckdb table column
    // naming.
    std::vector<std::string> columnNames;
    columnNames.reserve(data[0]->type()->size());
    for (int i = 0; i < data[0]->type()->size(); ++i) {
      columnNames.push_back(fmt::format("c{}", i));
    }
    std::vector<RowVectorPtr> duckDbInputs;
    duckDbInputs.reserve(data.size());
    for (const auto& dataVector : data) {
      duckDbInputs.emplace_back(
          makeRowVector(columnNames, dataVector->children()));
    }
    duckDbQueryRunner_.createTable(tableName, duckDbInputs);
  }

  // Makes index table handle with the specified index table and async lookup
  // flag.
  std::shared_ptr<TestIndexTableHandle> makeIndexTableHandle(
      const std::shared_ptr<TestIndexTable>& indexTable,
      bool asyncLookup) {
    return std::make_shared<TestIndexTableHandle>(
        kTestIndexConnectorName, indexTable, asyncLookup);
  }

  // Makes index table scan node with the specified index table handle.
  // @param outputType: the output schema of the index table scan node.
  // @param scanNodeId: returns the plan node id of the index table scan node.
  core::TableScanNodePtr makeIndexScanNode(
      const std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator,
      const std::shared_ptr<TestIndexTableHandle> indexTableHandle,
      const RowTypePtr& outputType,
      core::PlanNodeId& scanNodeId) {
    auto planBuilder = PlanBuilder(planNodeIdGenerator);
    auto indexTableScan = std::dynamic_pointer_cast<const core::TableScanNode>(
        PlanBuilder::TableScanBuilder(planBuilder)
            .tableHandle(indexTableHandle)
            .outputType(outputType)
            .endTableScan()
            .capturePlanNodeId(scanNodeId)
            .planNode());
    VELOX_CHECK_NOT_NULL(indexTableScan);
    return indexTableScan;
  }

  // Makes output schema from the index table scan node with the specified
  // column names.
  RowTypePtr makeScanOutputType(std::vector<std::string> outputNames) {
    std::vector<TypePtr> types;
    for (int i = 0; i < outputNames.size(); ++i) {
      if (valueType_->getChildIdxIfExists(outputNames[i]).has_value()) {
        types.push_back(valueType_->findChild(outputNames[i]));
        continue;
      }
      types.push_back(keyType_->findChild(outputNames[i]));
    }
    return ROW(std::move(outputNames), std::move(types));
  }

  // Makes lookup join plan with the following parameters:
  // @param indexScanNode: the index table scan node.
  // @param probeVectors: the probe input vectors.
  // @param outputColumns: the output column names of index lookup join.
  // @param joinType: the join type of index lookup join.
  // @param joinNodeId: returns the plan node id of the index lookup join node.
  core::PlanNodePtr makeLookupPlan(
      const std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator,
      core::TableScanNodePtr indexScanNode,
      const std::vector<RowVectorPtr> probeVectors,
      const std::vector<std::string>& outputColumns,
      core::JoinType joinType,
      core::PlanNodeId& joinNodeId) {
    VELOX_CHECK_EQ(keyType_->size(), 1);
    return PlanBuilder(planNodeIdGenerator)
        .values(probeVectors)
        .indexLookupJoin(
            {{probeType_->nameOf(0)}},
            {"u0"},
            indexScanNode,
            {},
            outputColumns,
            joinType)
        .capturePlanNodeId(joinNodeId)
        .planNode();
  }

  const std::unique_ptr<folly::CPUThreadPoolExecutor> connectorCpuExecutor_{
      std::make_unique<folly::CPUThreadPoolExecutor>(128)};
  RowTypePtr keyType_;
  RowTypePtr valueType_;
  RowTypePtr tableType_;
  RowTypePtr probeType_;
};

TEST_P(IndexLookupJoinTest, planNodeAndSerde) {
  TestIndexTableHandle::registerSerDe();

  auto indexConnectorHandle = std::make_shared<TestIndexTableHandle>(
      kTestIndexConnectorName, nullptr, true);

  auto left = makeRowVector(
      {"t0", "t1", "t2"},
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeFlatVector<int64_t>({10, 20, 30}),
       makeFlatVector<int32_t>({10, 30, 20})});

  auto right = makeRowVector(
      {"u0", "u1", "u2"},
      {makeFlatVector<int32_t>({1, 2, 3}),
       makeFlatVector<int64_t>({10, 20, 30}),
       makeFlatVector<int32_t>({10, 30, 20})});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto planBuilder = PlanBuilder();
  auto nonIndexTableScan = std::dynamic_pointer_cast<const core::TableScanNode>(
      PlanBuilder::TableScanBuilder(planBuilder)
          .outputType(std::dynamic_pointer_cast<const RowType>(right->type()))
          .endTableScan()
          .planNode());
  VELOX_CHECK_NOT_NULL(nonIndexTableScan);

  auto indexTableScan = std::dynamic_pointer_cast<const core::TableScanNode>(
      PlanBuilder::TableScanBuilder(planBuilder)
          .tableHandle(indexConnectorHandle)
          .outputType(std::dynamic_pointer_cast<const RowType>(right->type()))
          .endTableScan()
          .planNode());
  VELOX_CHECK_NOT_NULL(indexTableScan);

  for (const auto joinType : {core::JoinType::kLeft, core::JoinType::kInner}) {
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({left})
                    .indexLookupJoin(
                        {"t0"},
                        {"u0"},
                        indexTableScan,
                        {},
                        {"t0", "u1", "t2", "t1"},
                        joinType)
                    .planNode();
    auto indexLookupJoinNode =
        std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(plan);
    ASSERT_TRUE(indexLookupJoinNode->joinConditions().empty());
    ASSERT_EQ(
        indexLookupJoinNode->lookupSource()->tableHandle()->connectorId(),
        kTestIndexConnectorName);
    testSerde(plan);
  }

  // with join conditions.
  for (const auto joinType : {core::JoinType::kLeft, core::JoinType::kInner}) {
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({left})
                    .indexLookupJoin(
                        {"t0"},
                        {"u0"},
                        indexTableScan,
                        {"u1 > t2"},
                        {"t0", "u1", "t2", "t1"},
                        joinType)
                    .planNode();
    auto indexLookupJoinNode =
        std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(plan);
    ASSERT_EQ(indexLookupJoinNode->joinConditions().size(), 1);
    ASSERT_EQ(
        indexLookupJoinNode->lookupSource()->tableHandle()->connectorId(),
        kTestIndexConnectorName);
    testSerde(plan);
  }

  // bad join type.
  {
    VELOX_ASSERT_USER_THROW(
        PlanBuilder(planNodeIdGenerator)
            .values({left})
            .indexLookupJoin(
                {"t0"},
                {"u0"},
                indexTableScan,
                {},
                {"t0", "u1", "t2", "t1"},
                core::JoinType::kFull)
            .planNode(),
        "Unsupported index lookup join type FULL");
  }

  // bad table handle.
  {
    VELOX_ASSERT_USER_THROW(
        PlanBuilder(planNodeIdGenerator)
            .values({left})
            .indexLookupJoin(
                {"t0"}, {"u0"}, nonIndexTableScan, {}, {"t0", "u1", "t2", "t1"})
            .planNode(),
        "The lookup table handle hive_table from connector test-hive doesn't support index lookup");
  }

  // Non-matched join keys.
  {
    VELOX_ASSERT_THROW(
        PlanBuilder(planNodeIdGenerator)
            .values({left})
            .indexLookupJoin(
                {"t0", "t1"},
                {"u0"},
                indexTableScan,
                {"u1 > t2"},
                {"t0", "u1", "t2", "t1"})
            .planNode(),
        "JoinNode requires same number of join keys on left and right sides");
  }

  // No join keys.
  {
    VELOX_ASSERT_THROW(
        PlanBuilder(planNodeIdGenerator)
            .values({left})
            .indexLookupJoin(
                {}, {}, indexTableScan, {"u1 > t2"}, {"t0", "u1", "t2", "t1"})
            .planNode(),
        "JoinNode requires at least one join key");
  }
}

TEST_P(IndexLookupJoinTest, basic) {
  struct {
    int numKeys;
    int numDuplicatePerKey;
    int numBatches;
    int numProbeRowsPerBatch;
    int matchPct;
    std::vector<std::string> scanOutputColumns;
    std::vector<std::string> outputColumns;
    core::JoinType joinType;
    std::string duckDbVerifySql;

    std::string debugString() const {
      return fmt::format(
          "numKeys: {}, numDuplicatePerKey: {}, numBatches: {}, numProbeRowsPerBatch: {}, matchPct: {}, scanOutputColumns: {}, outputColumns: {}, joinType: {}, duckDbVerifySql: {}",
          numKeys,
          numDuplicatePerKey,
          numBatches,
          numProbeRowsPerBatch,
          matchPct,
          folly::join(",", scanOutputColumns),
          folly::join(",", outputColumns),
          core::joinTypeName(joinType),
          duckDbVerifySql);
    }
  } testSettings[] = {
      // Inner join.
      // 10% match.
      {100,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // 10% match with duplicates.
      {100,
       4,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // 10% match with larger lookup table.
      {500,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // 10% match + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // Empty lookup table.
      {0,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // No match.
      {500,
       4,
       10,
       100,
       0,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // 10% match with larger lookup table.
      {500,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // very few (2%) match with larger lookup table.
      {500,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // very few (2%) match + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // All matches with larger lookup table.
      {500,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // All matches + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // No probe projection.
      {500,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // No probe projection + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // Probe column reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"t2", "t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c2, t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      // Lookup column reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2"},
       {"t1", "u2", "u1", "t2"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c2, u.c1, t.c2 FROM t, u WHERE t.c0 = u.c0"},
      // Both sides reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3"},
       {"t2", "u2", "u3", "t1", "u1"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1 FROM t, u WHERE t.c0 = u.c0"},
      // With probe key colums.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3"},
       {"t2", "u2", "u3", "t1", "u1", "t0"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, t.c0 FROM t, u WHERE t.c0 = u.c0"},

      // Left join.
      // 10% match.
      {100,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // 10% match with duplicates.
      {100,
       4,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // 10% match with larger lookup table.
      {500,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // 10% match + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},

      // Empty lookup table.
      {0,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // No match.
      {500,
       4,
       10,
       100,
       0,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // 10% match with larger lookup table.
      {500,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // very few (2%) match with larger lookup table.
      {500,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // very few (2%) match + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // All matches with larger lookup table.
      {500,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // All matches + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // Probe column reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3"},
       {"t2", "t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c2, t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // Lookup column reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2"},
       {"t1", "u2", "u1", "t2"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c2, u.c1, t.c2 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // Both sides reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3"},
       {"t2", "u2", "u3", "t1", "u1"},
       core::JoinType::kLeft,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // With probe key colums.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3"},
       {"t2", "u2", "u3", "t1", "u1", "t0"},
       core::JoinType::kLeft,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, t.c0 FROM t LEFT JOIN u ON t.c0 = u.c0"},
  };
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    RowVectorPtr keyData;
    RowVectorPtr valueData;
    RowVectorPtr tableData;
    generateIndexTableData(
        testData.numKeys,
        testData.numDuplicatePerKey,
        keyData,
        valueData,
        tableData);
    const std::vector<RowVectorPtr> probeVectors = generateProbeTableInput(
        testData.numBatches,
        testData.numProbeRowsPerBatch,
        keyData,
        testData.matchPct);

    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", {tableData});

    const auto indexTable = createIndexTable(keyData, valueData);
    const auto indexTableHandle = makeIndexTableHandle(indexTable, GetParam());
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId indexScanNodeId;
    const auto indexScanNode = makeIndexScanNode(
        planNodeIdGenerator,
        indexTableHandle,
        makeScanOutputType(testData.scanOutputColumns),
        indexScanNodeId);

    core::PlanNodeId joinNodeId;
    auto plan = makeLookupPlan(
        planNodeIdGenerator,
        indexScanNode,
        probeVectors,
        testData.outputColumns,
        testData.joinType,
        joinNodeId);
    AssertQueryBuilder(duckDbQueryRunner_)
        .plan(plan)
        .assertResults(testData.duckDbVerifySql);
  }
}
} // namespace

VELOX_INSTANTIATE_TEST_SUITE_P(
    IndexLookupJoinTest,
    IndexLookupJoinTest,
    testing::ValuesIn({false, true}));
} // namespace fecebook::velox::exec::test
