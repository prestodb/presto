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
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/Connector.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/PlanNodeStats.h"
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
    keyType_ = ROW({"u0", "u1", "u2"}, {BIGINT(), BIGINT(), BIGINT()});
    valueType_ = ROW({"u3", "u4", "u5"}, {BIGINT(), BIGINT(), VARCHAR()});
    tableType_ = concat(keyType_, valueType_);
    probeType_ = ROW(
        {"t0", "t1", "t2", "t3", "t4", "t5"},
        {BIGINT(), BIGINT(), BIGINT(), BIGINT(), ARRAY(BIGINT()), VARCHAR()});

    TestIndexTableHandle::registerSerDe();
  }

  void TearDown() override {
    connector::unregisterConnectorFactory(kTestIndexConnectorName);
    connector::unregisterConnector(kTestIndexConnectorName);
    HiveConnectorTestBase::TearDown();
  }

  RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b) {
    std::vector<std::string> names = a->names();
    std::vector<TypePtr> types = a->children();
    names.insert(names.end(), b->names().begin(), b->names().end());
    types.insert(types.end(), b->children().begin(), b->children().end());
    return ROW(std::move(names), std::move(types));
  }

  void testSerde(const core::PlanNodePtr& plan) {
    auto serialized = plan->serialize();

    auto copy = ISerializable::deserialize<core::PlanNode>(serialized, pool());

    ASSERT_EQ(plan->toString(true, true), copy->toString(true, true));
  }

  template <typename T>
  void getVectorMinMax(FlatVector<T>* vector, T& min, T& max) {
    min = std::numeric_limits<T>::max();
    max = std::numeric_limits<T>::min();
    for (auto i = 0; i < vector->size(); ++i) {
      min = std::min(min, vector->valueAt(i));
      max = std::max(max, vector->valueAt(i));
    }
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
    opts.allowSlice = false;
    VectorFuzzer fuzzer(opts, pool_.get());

    keyData = fuzzer.fuzzInputFlatRow(keyType_);
    valueData = fuzzer.fuzzInputFlatRow(valueType_);
    // Set the key column vector to the same value to easy testing with
    // specified match ratio.
    for (int i = 0; i < keyType_->size(); ++i) {
      keyData->childAt(i) = makeFlatVector<int64_t>(
          keyData->size(),
          [numDuplicatePerKey](auto row) { return row / numDuplicatePerKey; });
    }
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
  std::vector<RowVectorPtr> generateProbeInput(
      size_t numBatches,
      size_t batchSize,
      const RowVectorPtr& tableKey,
      const std::vector<std::string>& probeJoinKeys,
      std::optional<int> equalMatchPct = std::nullopt) {
    std::vector<RowVectorPtr> probeInputs;
    probeInputs.reserve(numBatches);
    VectorFuzzer::Options opts;
    opts.vectorSize = batchSize;
    opts.allowSlice = false;
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
    int64_t minKey;
    int64_t maxKey;
    getVectorMinMax(
        tableKey->childAt(0)->asFlatVector<int64_t>(), minKey, maxKey);
    if (!equalMatchPct.has_value()) {
      return probeInputs;
    }

    // Set the same value for the probe key to easy the match pct control.
    for (int i = 0; i < probeJoinKeys.size(); ++i) {
      const auto& probeKey = probeJoinKeys[i];
      if (i == 0) {
        for (int i = 0, row = 0; i < numBatches; ++i) {
          const auto probeChannel = probeType_->getChildIdx(probeKey);
          auto probeVector = probeInputs[i]->childAt(probeChannel);
          probeVector->loadedVector();
          BaseVector::flattenVector(probeVector);
          auto* flatProbeVector = probeVector->asFlatVector<int64_t>();
          VELOX_CHECK_NOT_NULL(flatProbeVector);
          for (int j = 0; j < flatProbeVector->size(); ++j, ++row) {
            if (row % 100 < equalMatchPct.value()) {
              flatProbeVector->set(
                  j, folly::Random::rand64(minKey, maxKey + 1));
            } else {
              flatProbeVector->set(j, maxKey + 1 + folly::Random::rand32());
            }
          }
          probeInputs[i]->childAt(probeChannel) = probeVector;
        }
      } else {
        for (int i = 0; i < numBatches; ++i) {
          const auto probeChannel = probeType_->getChildIdx(probeKey);
          probeInputs[i]->childAt(probeChannel) = probeInputs[i]->childAt(
              probeType_->getChildIdx(probeJoinKeys[0]));
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
  // @param leftKeys: the left join keys of index lookup join.
  // @param rightKeys: the right join keys of index lookup join.
  // @param joinType: the join type of index lookup join.
  // @param joinNodeId: returns the plan node id of the index lookup join node.
  core::PlanNodePtr makeLookupPlan(
      const std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator,
      core::TableScanNodePtr indexScanNode,
      const std::vector<RowVectorPtr>& probeVectors,
      const std::vector<std::string>& outputColumns,
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      core::JoinType joinType,
      core::PlanNodeId& joinNodeId) {
    VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());
    VELOX_CHECK_LE(leftKeys.size(), keyType_->size());
    return PlanBuilder(planNodeIdGenerator)
        .values(probeVectors)
        .indexLookupJoin(
            leftKeys, rightKeys, indexScanNode, {}, outputColumns, joinType)
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

TEST_P(IndexLookupJoinTest, equalJoin) {
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
       {"u0", "u1", "u2", "u3", "u4"},
       {"t1", "u1", "u2", "u3", "u4"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c4 FROM t, u WHERE t.c0 = u.c0"},
      // 10% match with duplicates.
      {100,
       4,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u4"},
       {"t1", "u1", "u2", "u3", "u4"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c4 FROM t, u WHERE t.c0 = u.c0"},
      // 10% match with larger lookup table.
      {500,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      {2048,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // 10% match + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      {2048,
       4,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      {2048,
       4,
       10,
       2'048,
       100,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // Empty lookup table.
      {0,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // No match.
      {500,
       4,
       10,
       100,
       0,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // 10% match with larger lookup table.
      {500,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // very few (2%) match with larger lookup table.
      {500,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // very few (2%) match + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // All matches with larger lookup table.
      {500,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      {2048,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // All matches + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      {2048,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // No probe projection.
      {500,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // No probe projection + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // Probe column reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t2", "t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // Lookup column reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2", "u5"},
       {"t1", "u2", "u1", "t2", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c2, u.c1, t.c2, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // Both sides reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u2", "u3", "t1", "u1", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // With probe key colums.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u2", "u3", "t1", "u1", "t0", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, t.c0, u.c5 FROM t, u WHERE t.c0 = u.c0"},

      // Left join.
      // 10% match.
      {100,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // 10% match with duplicates.
      {100,
       4,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // 10% match with larger lookup table.
      {500,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // 10% match + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},

      // Empty lookup table.
      {0,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // No match.
      {500,
       4,
       10,
       100,
       0,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // 10% match with larger lookup table.
      {500,
       1,
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // very few (2%) match with larger lookup table.
      {500,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      {2048,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // very few (2%) match + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // All matches with larger lookup table.
      {500,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      {2048,
       1,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // All matches + duplicate with larger lookup table.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      {2048,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      {2'048,
       4,
       10,
       2'048,
       100,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // Probe column reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t2", "t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c2, t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // Lookup column reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2", "u5"},
       {"t1", "u2", "u1", "t2", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c2, u.c1, t.c2, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // Both sides reorder in output.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u2", "u3", "t1", "u1", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // With probe key colums.
      {500,
       4,
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u2", "u3", "t1", "u1", "t0", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, t.c0, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"}};
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
    const std::vector<RowVectorPtr> probeVectors = generateProbeInput(
        testData.numBatches,
        testData.numProbeRowsPerBatch,
        keyData,
        {"t0", "t1", "t2"},
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
        {"t0", "t1", "t2"},
        {"u0", "u1", "u2"},
        testData.joinType,
        joinNodeId);
    AssertQueryBuilder(duckDbQueryRunner_)
        .plan(plan)
        .assertResults(testData.duckDbVerifySql);
  }
}

DEBUG_ONLY_TEST_P(IndexLookupJoinTest, connectorError) {
  RowVectorPtr keyData;
  RowVectorPtr valueData;
  RowVectorPtr tableData;
  generateIndexTableData(1'000, 100, keyData, valueData, tableData);
  const std::vector<RowVectorPtr> probeVectors =
      generateProbeInput(100, 100, keyData, {"t0", "t1", "t2"}, 100);

  const std::string errorMsg{"injectedError"};
  std::atomic_int lookupCount{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::test::TestIndexSource::ResultIterator::syncLookup",
      std::function<void(void*)>([&](void*) {
        // Triggers error in the middle.
        if (lookupCount++ == 10) {
          VELOX_FAIL(errorMsg);
        }
      }));

  const auto indexTable = createIndexTable(keyData, valueData);
  const auto indexTableHandle = makeIndexTableHandle(indexTable, GetParam());
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId indexScanNodeId;
  const auto indexScanNode = makeIndexScanNode(
      planNodeIdGenerator,
      indexTableHandle,
      makeScanOutputType({"u0", "u1", "u2", "u5"}),
      indexScanNodeId);

  core::PlanNodeId joinNodeId;
  auto plan = makeLookupPlan(
      planNodeIdGenerator,
      indexScanNode,
      probeVectors,
      {"u0", "u1", "u2", "t5"},
      {"t0", "t1", "t2"},
      {"u0", "u1", "u2"},
      core::JoinType::kInner,
      joinNodeId);
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool_.get()), errorMsg);
}

TEST_P(IndexLookupJoinTest, outputBatchSize) {
  RowVectorPtr keyData;
  RowVectorPtr valueData;
  RowVectorPtr tableData;
  generateIndexTableData(
      3'000,
      /*numDuplicatePerKey=*/1,
      keyData,
      valueData,
      tableData);

  struct {
    int numProbeBatches;
    int numRowsPerProbeBatch;
    int maxBatchRows;
    int numExpectedOutputBatch;

    std::string debugString() const {
      return fmt::format(
          "numProbeBatches: {}, numRowsPerProbeBatch: {}, maxBatchRows: {}, numExpectedOutputBatch: {}",
          numProbeBatches,
          numRowsPerProbeBatch,
          maxBatchRows,
          numExpectedOutputBatch);
    }
  } testSettings[] = {
      {10, 100, 10, 100},
      {10, 500, 10, 500},
      {10, 1, 200, 10},
      {1, 500, 10, 50},
      {1, 300, 10, 30},
      {1, 500, 200, 3},
      {10, 200, 200, 10},
      {10, 500, 300, 20},
      {10, 50, 1, 500}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    const std::vector<RowVectorPtr> probeVectors = generateProbeInput(
        testData.numProbeBatches,
        testData.numRowsPerProbeBatch,
        keyData,
        {"t0", "t1", "t2"},
        /*equalMatchPct=*/100);

    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", {tableData});

    const auto indexTable = createIndexTable(keyData, valueData);
    const auto indexTableHandle = makeIndexTableHandle(indexTable, GetParam());
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId indexScanNodeId;
    const auto indexScanNode = makeIndexScanNode(
        planNodeIdGenerator,
        indexTableHandle,
        makeScanOutputType({"u0", "u1", "u2", "u5"}),
        indexScanNodeId);

    core::PlanNodeId joinNodeId;
    auto plan = makeLookupPlan(
        planNodeIdGenerator,
        indexScanNode,
        probeVectors,
        {"t4", "u5"},
        {"t0", "t1", "t2"},
        {"u0", "u1", "u2"},
        core::JoinType::kInner,
        joinNodeId);
    const auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .plan(plan)
            .config(
                core::QueryConfig::kPreferredOutputBatchRows,
                std::to_string(testData.maxBatchRows))
            .config(
                core::QueryConfig::kPreferredOutputBatchBytes,
                std::to_string(1ULL << 30))
            .assertResults(
                "SELECT t.c4, u.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND t.c2 = u.c2");
    ASSERT_EQ(
        toPlanStats(task->taskStats()).at(joinNodeId).outputVectors,
        testData.numExpectedOutputBatch);
  }
}

TEST_P(IndexLookupJoinTest, equalJoinFuzzer) {
  RowVectorPtr keyData;
  RowVectorPtr valueData;
  RowVectorPtr tableData;
  generateIndexTableData(4096, 10, keyData, valueData, tableData);
  const std::vector<RowVectorPtr> probeVectors =
      generateProbeInput(50, 256, keyData, {"t0", "t1", "t2"});

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", {tableData});

  const auto indexTable = createIndexTable(keyData, valueData);
  const auto indexTableHandle = makeIndexTableHandle(indexTable, GetParam());
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId indexScanNodeId;
  auto scanOutput = tableType_->names();
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(scanOutput.begin(), scanOutput.end(), g);
  const auto indexScanNode = makeIndexScanNode(
      planNodeIdGenerator,
      indexTableHandle,
      makeScanOutputType(scanOutput),
      indexScanNodeId);

  auto joinOutput = concat(tableType_, probeType_)->names();
  core::PlanNodeId joinNodeId;
  auto plan = makeLookupPlan(
      planNodeIdGenerator,
      indexScanNode,
      probeVectors,
      joinOutput,
      {"t0", "t1", "t2"},
      {"u0", "u1", "u2"},
      core::JoinType::kInner,
      joinNodeId);
  AssertQueryBuilder(duckDbQueryRunner_)
      .plan(plan)
      .assertResults(
          "SELECT u.c0, u.c1, u.c2, u.c3, u.c4, u.c5, t.c0, t.c1, t.c2, t.c3, t.c4, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND t.c2 = u.c2");
}
} // namespace

VELOX_INSTANTIATE_TEST_SUITE_P(
    IndexLookupJoinTest,
    IndexLookupJoinTest,
    testing::ValuesIn({false, true}));
} // namespace fecebook::velox::exec::test
