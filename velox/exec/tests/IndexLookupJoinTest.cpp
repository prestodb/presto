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

#include "velox/exec/IndexLookupJoin.h"
#include "folly/experimental/EventCount.h"
#include "gmock/gmock.h"
#include "gtest/gtest-matchers.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/Connector.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/IndexLookupJoinTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TestIndexStorageConnector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::testutil;

namespace fecebook::velox::exec::test {
namespace {
struct TestParam {
  bool asyncLookup;
  int32_t numPrefetches;

  TestParam(bool _asyncLookup, int32_t _numPrefetches)
      : asyncLookup(_asyncLookup), numPrefetches(_numPrefetches) {}

  std::string toString() const {
    return fmt::format(
        "asyncLookup={}, numPrefetches={}", asyncLookup, numPrefetches);
  }
};

class IndexLookupJoinTest : public IndexLookupJoinTestBase,
                            public testing::WithParamInterface<TestParam> {
 public:
  static std::vector<TestParam> getTestParams() {
    std::vector<TestParam> testParams;
    testParams.emplace_back(true, 0);
    testParams.emplace_back(false, 0);
    testParams.emplace_back(true, 3);
    testParams.emplace_back(false, 3);
    return testParams;
  }

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

  void testSerde(const core::PlanNodePtr& plan) {
    auto serialized = plan->serialize();
    auto copy = ISerializable::deserialize<core::PlanNode>(serialized, pool());
    ASSERT_EQ(plan->toString(true, true), copy->toString(true, true));
  }

  // Create index table with the given key and value inputs.
  std::shared_ptr<TestIndexTable> createIndexTable(
      int numEqualJoinKeys,
      const RowVectorPtr& keyData,
      const RowVectorPtr& valueData) {
    const auto keyType =
        std::dynamic_pointer_cast<const RowType>(keyData->type());
    VELOX_CHECK_GE(keyType->size(), 1);
    VELOX_CHECK_GE(keyType->size(), numEqualJoinKeys);
    auto valueType =
        std::dynamic_pointer_cast<const RowType>(valueData->type());
    VELOX_CHECK_GE(valueType->size(), 1);
    const auto numRows = keyData->size();
    VELOX_CHECK_EQ(numRows, valueData->size());

    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.reserve(numEqualJoinKeys);
    std::vector<VectorPtr> keyVectors;
    keyVectors.reserve(numEqualJoinKeys);
    for (auto i = 0; i < numEqualJoinKeys; ++i) {
      hashers.push_back(std::make_unique<VectorHasher>(keyType->childAt(i), i));
      keyVectors.push_back(keyData->childAt(i));
    }

    std::vector<TypePtr> dependentTypes;
    std::vector<VectorPtr> dependentVectors;
    for (int i = numEqualJoinKeys; i < keyType->size(); ++i) {
      dependentTypes.push_back(keyType->childAt(i));
      dependentVectors.push_back(keyData->childAt(i));
    }
    for (int i = 0; i < valueType->size(); ++i) {
      dependentTypes.push_back(valueType->childAt(i));
      dependentVectors.push_back(valueData->childAt(i));
    }

    // Create the table.
    auto table = HashTable<false>::createForJoin(
        std::move(hashers),
        /*dependentTypes=*/dependentTypes,
        /*allowDuplicates=*/true,
        /*hasProbedFlag=*/false,
        /*minTableSizeForParallelJoinBuild=*/1,
        pool_.get());

    // Insert data into the row container.
    auto* rowContainer = table->rows();
    std::vector<DecodedVector> decodedVectors;
    for (auto& vector : keyData->children()) {
      decodedVectors.emplace_back(*vector);
    }
    for (auto& vector : valueData->children()) {
      decodedVectors.emplace_back(*vector);
    }

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

  // Makes index table handle with the specified index table and async lookup
  // flag.
  std::shared_ptr<TestIndexTableHandle> makeIndexTableHandle(
      const std::shared_ptr<TestIndexTable>& indexTable,
      bool asyncLookup) {
    return std::make_shared<TestIndexTableHandle>(
        kTestIndexConnectorName, indexTable, asyncLookup);
  }

  const std::unique_ptr<folly::CPUThreadPoolExecutor> connectorCpuExecutor_{
      std::make_unique<folly::CPUThreadPoolExecutor>(128)};
};

TEST_P(IndexLookupJoinTest, joinCondition) {
  const auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4"},
          {BIGINT(), BIGINT(), BIGINT(), ARRAY(BIGINT()), BIGINT()});

  auto inJoinCondition = PlanBuilder::parseIndexJoinCondition(
      "contains(c3, c2)", rowType, pool_.get());
  ASSERT_FALSE(inJoinCondition->isFilter());
  ASSERT_EQ(inJoinCondition->toString(), "ROW[\"c2\"] IN ROW[\"c3\"]");

  auto inFilterCondition = PlanBuilder::parseIndexJoinCondition(
      "contains(ARRAY[1,2], c2)", rowType, pool_.get());
  ASSERT_TRUE(inFilterCondition->isFilter());
  ASSERT_EQ(
      inFilterCondition->toString(),
      "ROW[\"c2\"] IN 2 elements starting at 0 {1, 2}");

  auto betweenFilterCondition = PlanBuilder::parseIndexJoinCondition(
      "c0 between 0 AND 1", rowType, pool_.get());
  ASSERT_TRUE(betweenFilterCondition->isFilter());
  ASSERT_EQ(betweenFilterCondition->toString(), "ROW[\"c0\"] BETWEEN 0 AND 1");

  auto betweenJoinCondition1 = PlanBuilder::parseIndexJoinCondition(
      "c0 between c1 AND c4", rowType, pool_.get());
  ASSERT_FALSE(betweenJoinCondition1->isFilter());
  ASSERT_EQ(
      betweenJoinCondition1->toString(),
      "ROW[\"c0\"] BETWEEN ROW[\"c1\"] AND ROW[\"c4\"]");

  auto betweenJoinCondition2 = PlanBuilder::parseIndexJoinCondition(
      "c0 between 0 AND c1", rowType, pool_.get());
  ASSERT_FALSE(betweenJoinCondition2->isFilter());
  ASSERT_EQ(
      betweenJoinCondition2->toString(),
      "ROW[\"c0\"] BETWEEN 0 AND ROW[\"c1\"]");

  auto betweenJoinCondition3 = PlanBuilder::parseIndexJoinCondition(
      "c0 between c1 AND 0", rowType, pool_.get());
  ASSERT_FALSE(betweenJoinCondition3->isFilter());
  ASSERT_EQ(
      betweenJoinCondition3->toString(),
      "ROW[\"c0\"] BETWEEN ROW[\"c1\"] AND 0");
}

TEST_P(IndexLookupJoinTest, planNodeAndSerde) {
  TestIndexTableHandle::registerSerDe();

  auto indexConnectorHandle = std::make_shared<TestIndexTableHandle>(
      kTestIndexConnectorName, nullptr, true);

  auto left = makeRowVector(
      {"t0", "t1", "t2", "t3", "t4"},
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<int64_t>({10, 20, 30}),
       makeFlatVector<int64_t>({10, 30, 20}),
       makeArrayVector<int64_t>(
           3,
           [](auto row) { return row; },
           [](auto /*unused*/, auto index) { return index; }),
       makeArrayVector<int64_t>(
           3,
           [](auto row) { return row; },
           [](auto /*unused*/, auto index) { return index; })});

  auto right = makeRowVector(
      {"u0", "u1", "u2"},
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<int64_t>({10, 20, 30}),
       makeFlatVector<int64_t>({10, 30, 20})});

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

  // with in join conditions.
  for (const auto joinType : {core::JoinType::kLeft, core::JoinType::kInner}) {
    auto plan = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .values({left})
                    .indexLookupJoin(
                        {"t0"},
                        {"u0"},
                        indexTableScan,
                        {"contains(t3, u0)", "contains(t4, u1)"},
                        {"t0", "u1", "t2", "t1"},
                        joinType)
                    .planNode();
    auto indexLookupJoinNode =
        std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(plan);
    ASSERT_EQ(indexLookupJoinNode->joinConditions().size(), 2);
    ASSERT_EQ(
        indexLookupJoinNode->lookupSource()->tableHandle()->connectorId(),
        kTestIndexConnectorName);
    testSerde(plan);
  }

  // with between join conditions.
  for (const auto joinType : {core::JoinType::kLeft, core::JoinType::kInner}) {
    auto plan = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .values({left})
                    .indexLookupJoin(
                        {"t0"},
                        {"u0"},
                        indexTableScan,
                        {"u0 between t0 AND t1",
                         "u1 between t1 AND 10",
                         "u1 between 10 AND t1"},
                        {"t0", "u1", "t2", "t1"},
                        joinType)
                    .planNode();
    auto indexLookupJoinNode =
        std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(plan);
    ASSERT_EQ(indexLookupJoinNode->joinConditions().size(), 3);
    ASSERT_EQ(
        indexLookupJoinNode->lookupSource()->tableHandle()->connectorId(),
        kTestIndexConnectorName);
    testSerde(plan);
  }

  // with mix join conditions.
  for (const auto joinType : {core::JoinType::kLeft, core::JoinType::kInner}) {
    auto plan = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .values({left})
                    .indexLookupJoin(
                        {"t0"},
                        {"u0"},
                        indexTableScan,
                        {"contains(t3, u0)", "u1 between 10 AND t1"},
                        {"t0", "u1", "t2", "t1"},
                        joinType)
                    .planNode();
    auto indexLookupJoinNode =
        std::dynamic_pointer_cast<const core::IndexLookupJoinNode>(plan);
    ASSERT_EQ(indexLookupJoinNode->joinConditions().size(), 2);
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
                {"contains(t4, u0)"},
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
                {},
                {},
                indexTableScan,
                {"contains(t4, u0)"},
                {"t0", "u1", "t2", "t1"})
            .planNode(),
        "JoinNode requires at least one join key");
  }
}

TEST_P(IndexLookupJoinTest, equalJoin) {
  struct {
    std::vector<int> keyCardinalities;
    int numProbeBatches;
    int numRowsPerProbeBatch;
    int matchPct;
    std::vector<std::string> scanOutputColumns;
    std::vector<std::string> outputColumns;
    core::JoinType joinType;
    std::string duckDbVerifySql;

    std::string debugString() const {
      return fmt::format(
          "keyCardinalities: {}, numProbeBatches: {}, numRowsPerProbeBatch: {}, matchPct: {}, "
          "scanOutputColumns: {}, outputColumns: {}, joinType: {},"
          " duckDbVerifySql: {}",
          folly::join(",", keyCardinalities),
          numProbeBatches,
          numRowsPerProbeBatch,
          matchPct,
          folly::join(",", scanOutputColumns),
          folly::join(",", outputColumns),
          core::joinTypeName(joinType),
          duckDbVerifySql);
    }
  } testSettings[] = {
      // Inner join.
      // 10% match.
      {{100, 1, 1},
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // 10% match with larger lookup table.
      {{500, 1, 1},
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      {{2048, 1, 1},
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // Empty lookup table.
      {{0, 1, 1},
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // No match.
      {{500, 1, 1},
       10,
       100,
       0,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // 10% matcvelox::h with larger lookup table.
      {{500, 1, 1},
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},
      {{2048, 1, 1},
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // very few (2%) match with larger lookup table.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      {{2048, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // All matches with larger lookup table.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      {{2048, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // No probe projection.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // Probe column reorder in output.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t2", "t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      {{2048, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t2", "t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, t.c1, u.c1, u.c2, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // Both sides reorder in output.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u2", "u3", "t1", "u1", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      {{2048, 1, 1},
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u2", "u3", "t1", "u1", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // With probe key colums.
      {{500, 1, 1},
       2,
       100,
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u2", "u3", "t1", "u1", "t0", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, t.c0, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      {{2048, 1, 1},
       2,
       100,
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u2", "u3", "t1", "u1", "t0", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, t.c0, u.c5 FROM t, u WHERE t.c0 = u.c0"},
      // Project key columns from lookup table.
      {{2048, 1, 1},
       10,
       100,
       50,
       {"u1", "u0", "u2", "u3"},
       {"t2", "u2", "u3", "t1", "u1", "u0"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, u.c0 FROM t, u WHERE t.c0 = u.c0"},
      {{100, 1, 1},
       10,
       100,
       50,
       {"u1", "u0", "u2", "u3"},
       {"t2", "u2", "u3", "t1", "u1", "u0"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, u.c0 FROM t, u WHERE t.c0 = u.c0"},
      {{2048, 1, 1},
       10,
       2048,
       100,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kInner,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t, u WHERE t.c0 = u.c0"},

      // Left join.
      // 10% match.
      {{100, 1, 1},
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},

      // 10% match with larger lookup table.
      {{500, 1, 1},
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      {{2048, 1, 1},
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // Empty lookup table.
      {{0, 1, 1},
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // No match.
      {{500, 1, 1},
       10,
       100,
       0,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // 10% match with larger lookup table.
      {{500, 1, 1},
       10,
       100,
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // very few (2%) match with larger lookup table.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      {{2048, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // All matches with larger lookup table.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      {{2048, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // Probe column reorder in output.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t2", "t1", "u1", "u2", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c2, t.c1, u.c1, u.c2, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // Lookup column reorder in output.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u1", "u0", "u2", "u5"},
       {"t1", "u2", "u1", "t2", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c2, u.c1, t.c2, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // Both sides reorder in output.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u2", "u3", "t1", "u1", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // With probe key colums.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u2", "u3", "t1", "u1", "t0", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, t.c0, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      // With lookup key colums.
      {{500, 1, 1},
       10,
       100,
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u2", "u3", "t1", "u1", "u0"},
       core::JoinType::kLeft,
       "SELECT t.c2, u.c2, u.c3, t.c1, u.c1, u.c0 FROM t LEFT JOIN u ON t.c0 = u.c0"},
      {{2048, 1, 1},
       10,
       2048,
       100,
       {"u0", "u1", "u2", "u3"},
       {"t1", "u1", "u2", "u3"},
       core::JoinType::kLeft,
       "SELECT t.c1, u.c1, u.c2, u.c3 FROM t LEFT JOIN u ON t.c0 = u.c0"}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    SequenceTableData tableData;
    generateIndexTableData(testData.keyCardinalities, tableData, pool_);
    auto probeVectors = generateProbeInput(
        testData.numProbeBatches,
        testData.numRowsPerProbeBatch,
        1,
        tableData,
        pool_,
        {"t0", "t1", "t2"},
        {},
        {},
        testData.matchPct);

    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", {tableData.tableData});

    const auto indexTable = createIndexTable(
        /*numEqualJoinKeys=*/3, tableData.keyData, tableData.valueData);
    const auto indexTableHandle =
        makeIndexTableHandle(indexTable, GetParam().asyncLookup);
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId indexScanNodeId;
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        columnHandles;
    const auto indexScanNode = makeIndexScanNode(
        planNodeIdGenerator,
        indexTableHandle,
        makeScanOutputType(testData.scanOutputColumns),
        indexScanNodeId,
        columnHandles);

    core::PlanNodeId joinNodeId;
    auto plan = makeLookupPlan(
        planNodeIdGenerator,
        indexScanNode,
        probeVectors,
        {"t0", "t1", "t2"},
        {"u0", "u1", "u2"},
        {},
        testData.joinType,
        testData.outputColumns,
        joinNodeId);
    runLookupQuery(plan, GetParam().numPrefetches, testData.duckDbVerifySql);
  }
}

TEST_P(IndexLookupJoinTest, betweenJoinCondition) {
  struct {
    std::vector<int> keyCardinalities;
    int numProbeBatches;
    int numProbeRowsPerBatch;
    std::string betweenCondition;
    int betweenMatchPct;
    std::vector<std::string> lookupOutputColumns;
    std::vector<std::string> outputColumns;
    core::JoinType joinType;
    std::string duckDbVerifySql;

    std::string debugString() const {
      return fmt::format(
          "keyCardinalities: {}, numProbeBatches: {}, numProbeRowsPerBatch: {}, betweenCondition: {}, betweenMatchPct: {}, lookupOutputColumns: {}, outputColumns: {}, joinType: {}, duckDbVerifySql: {}",
          folly::join(",", keyCardinalities),
          numProbeBatches,
          numProbeRowsPerBatch,
          betweenCondition,
          betweenMatchPct,
          folly::join(",", lookupOutputColumns),
          folly::join(",", outputColumns),
          core::joinTypeName(joinType),
          duckDbVerifySql);
    }
  } testSettings[] = {// Inner join.
                      // 10% match.
                      {{50, 1, 10},
                       1,
                       100,
                       "u2 between t2 and t3",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      {{50, 1, 10},
                       1,
                       100,
                       "u2 between 0 and t3",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN 0 AND t.c3"},
                      {{50, 1, 10},
                       1,
                       100,
                       "u2 between t2 and 1",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND 1"},
                      // 10% match with larger lookup table.
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between 0 and t3",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN 0 AND t.c3"},
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between t2 and 1",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND 1"},
                      // Empty lookup table.
                      {{0, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      // No match.
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       0,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between 0 and t3",
                       0,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN 0 AND t.c3"},
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and 0",
                       0,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND 0"},
                      // very few (2%) match with larger lookup table.
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       2,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       2,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      // All matches
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       100,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      // All matches with larger lookup table.
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       100,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between 0 and t3",
                       100,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN 0 AND t.c3"},
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between t2 and 10",
                       100,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kInner,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND 10"},
                      // No probe projection.
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       10,
                       {"u0", "u1", "u2", "u3", "u5"},
                       {"u1", "u0", "u3", "u5"},
                       core::JoinType::kInner,
                       "SELECT u.c1, u.c0, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      // Probe column reorder in output.
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       10,
                       {"u0", "u1", "u2", "u3", "u5"},
                       {"t2", "t1", "u1", "u3", "u5"},
                       core::JoinType::kInner,
                       "SELECT t.c2, t.c1, u.c1, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      // Both sides reorder in output.
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       10,
                       {"u1", "u0", "u2", "u3", "u5"},
                       {"t2", "u3", "t1", "u1", "u5"},
                       core::JoinType::kInner,
                       "SELECT t.c2, u.c3, t.c1, u.c1, u.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      // With probe key colums.
                      {{50, 1, 10},
                       2,
                       100,
                       "u2 between t2 and t3",
                       10,
                       {"u1", "u0", "u2", "u3", "u5"},
                       {"t2", "u3", "t1", "u1", "u0", "u5"},
                       core::JoinType::kInner,
                       "SELECT t.c2, u.c3, t.c1, u.c1, u.c0, u.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},

                      // Left join.
                      // 10% match.
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between 0 and t3",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN 0 AND t.c3"},
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and 1",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND 1"},
                      // 10% match with larger lookup table.
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between 0 and t3",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN 0 AND t.c3"},
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between t2 and 1",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND 1"},
                      // Empty lookup table.
                      {{0, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       10,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      // No match.
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       0,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between 0 and t3",
                       0,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN 0 AND t.c3"},
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and 0",
                       0,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND 0"},
                      // very few (2%) match with larger lookup table.
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       2,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       2,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      // All matches.
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       2,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      // All matches with larger lookup table.
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       2,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between 0 and t3",
                       2,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN 0 AND t.c3"},
                      {{256, 1, 10},
                       10,
                       100,
                       "u2 between t2 and 10",
                       2,
                       {"u0", "u1", "u2", "u3"},
                       {"t0", "t1", "t2", "t3", "u3", "t5"},
                       core::JoinType::kLeft,
                       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND 10"},
                      // Probe column reorder in output.
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       2,
                       {"u0", "u1", "u2", "u3", "u5"},
                       {"t2", "t1", "u1", "u3", "u5"},
                       core::JoinType::kLeft,
                       "SELECT t.c2, t.c1, u.c1, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      // Both sides reorder in output.
                      {{50, 1, 10},
                       10,
                       100,
                       "u2 between t2 and t3",
                       2,
                       {"u1", "u0", "u2", "u3", "u5"},
                       {"t2", "u3", "t1", "u1", "u5"},
                       core::JoinType::kLeft,
                       "SELECT t.c2, u.c3, t.c1, u.c1, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"},
                      // With probe key colums.
                      {{50, 1, 10},
                       2,
                       100,
                       "u2 between t2 and t3",
                       2,
                       {"u1", "u0", "u2", "u3", "u5"},
                       {"t2", "u3", "t1", "u1", "u0", "u5"},
                       core::JoinType::kLeft,
                       "SELECT t.c2, u.c3, t.c1, u.c1, u.c0, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND u.c2 BETWEEN t.c2 AND t.c3"}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    SequenceTableData tableData;
    generateIndexTableData(testData.keyCardinalities, tableData, pool_);
    auto probeVectors = generateProbeInput(
        testData.numProbeBatches,
        testData.numProbeRowsPerBatch,
        1,
        tableData,
        pool_,
        {"t0", "t1"},
        {},
        {{"t2", "t3"}},
        /*eqaulityMatchPct=*/80,
        /*inColumns=*/std::nullopt,
        testData.betweenMatchPct);

    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", {tableData.tableData});

    const auto indexTable = createIndexTable(
        /*numEqualJoinKeys=*/2, tableData.keyData, tableData.valueData);
    const auto indexTableHandle =
        makeIndexTableHandle(indexTable, GetParam().asyncLookup);
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId indexScanNodeId;
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        columnHandles;
    const auto indexScanNode = makeIndexScanNode(
        planNodeIdGenerator,
        indexTableHandle,
        makeScanOutputType(testData.lookupOutputColumns),
        indexScanNodeId,
        columnHandles);

    core::PlanNodeId joinNodeId;
    auto plan = makeLookupPlan(
        planNodeIdGenerator,
        indexScanNode,
        probeVectors,
        {"t0", "t1"},
        {"u0", "u1"},
        {testData.betweenCondition},
        testData.joinType,
        testData.outputColumns,
        joinNodeId);
    runLookupQuery(plan, GetParam().numPrefetches, testData.duckDbVerifySql);
  }
}

TEST_P(IndexLookupJoinTest, inJoinCondition) {
  struct {
    std::vector<int> keyCardinalities;
    int numProbeBatches;
    int numProbeRowsPerBatch;
    std::string inCondition;
    int inMatchPct;
    std::vector<std::string> lookupOutputColumns;
    std::vector<std::string> outputColumns;
    core::JoinType joinType;
    std::string duckDbVerifySql;

    std::string debugString() const {
      return fmt::format(
          "keyCardinalities: {}: numProbeBatches: {}, numProbeRowsPerBatch: {}, inCondition: {}, inMatchPct: {}, lookupOutputColumns: {}, outputColumns: {}, joinType: {}, duckDbVerifySql: {}",
          folly::join(",", keyCardinalities),
          numProbeBatches,
          numProbeRowsPerBatch,
          inCondition,
          inMatchPct,
          folly::join(",", lookupOutputColumns),
          folly::join(",", outputColumns),
          core::joinTypeName(joinType),
          duckDbVerifySql);
    }
  } testSettings[] = {
      // Inner join.
      // 10% match.
      {{50, 1, 10},
       1,
       100,
       "contains(t4, u2)",
       10,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kInner,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // 10% match with larger lookup table.
      {{256, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       10,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kInner,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // Empty lookup table.
      {{0, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       10,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kInner,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // No match.
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       0,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kInner,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // very few (2%) match with larger lookup table.
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       2,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kInner,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      {{256, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       2,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kInner,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // All matches
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       100,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kInner,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // All matches with larger lookup table.
      {{256, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       100,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kInner,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // No probe projection.
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"u1", "u0", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT u.c1, u.c0, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // Probe column reorder in output.
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       10,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t2", "t1", "u1", "u3", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, t.c1, u.c1, u.c3, u.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // Both sides reorder in output.
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       10,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u3", "t1", "u1", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c3, t.c1, u.c1, u.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // With probe key colums.
      {{50, 1, 10},
       2,
       100,
       "contains(t4, u2)",
       10,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u3", "t1", "u1", "u0", "u5"},
       core::JoinType::kInner,
       "SELECT t.c2, u.c3, t.c1, u.c1, u.c0, u.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},

      // Left join.
      // 10% match.
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       10,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kLeft,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // 10% match with larger lookup table.
      {{256, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       10,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kLeft,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      {{256, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       10,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kLeft,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // Empty lookup table.
      {{0, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       10,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kLeft,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // No match.
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       0,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kLeft,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // very few (2%) match with larger lookup table.
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       2,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kLeft,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      {{256, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       2,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kLeft,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // All matches.
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       2,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kLeft,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // All matches with larger lookup table.
      {{256, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       2,
       {"u0", "u1", "u2", "u3"},
       {"t0", "t1", "t2", "t3", "u3", "t5"},
       core::JoinType::kLeft,
       "SELECT t.c0, t.c1, t.c2, t.c3, u.c3, t.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // Probe column reorder in output.
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       2,
       {"u0", "u1", "u2", "u3", "u5"},
       {"t2", "t1", "u1", "u3", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c2, t.c1, u.c1, u.c3, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // Both sides reorder in output.
      {{50, 1, 10},
       10,
       100,
       "contains(t4, u2)",
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u3", "t1", "u1", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c2, u.c3, t.c1, u.c1, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"},
      // With probe key colums.
      {{50, 1, 10},
       2,
       100,
       "contains(t4, u2)",
       2,
       {"u1", "u0", "u2", "u3", "u5"},
       {"t2", "u3", "t1", "u1", "u0", "u5"},
       core::JoinType::kLeft,
       "SELECT t.c2, u.c3, t.c1, u.c1, u.c0, u.c5 FROM t LEFT JOIN u ON t.c0 = u.c0 AND t.c1 = u.c1 AND array_contains(t.c4, u.c2)"}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    SequenceTableData tableData;
    generateIndexTableData(testData.keyCardinalities, tableData, pool_);
    auto probeVectors = generateProbeInput(
        testData.numProbeBatches,
        testData.numProbeRowsPerBatch,
        1,
        tableData,
        pool_,
        {"t0", "t1"},
        {{"t4"}},
        {},
        /*eqaulityMatchPct=*/80,
        testData.inMatchPct);

    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", {tableData.tableData});

    const auto indexTable = createIndexTable(
        /*numEqualJoinKeys=*/2, tableData.keyData, tableData.valueData);
    const auto indexTableHandle =
        makeIndexTableHandle(indexTable, GetParam().asyncLookup);
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId indexScanNodeId;
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        columnHandles;
    const auto indexScanNode = makeIndexScanNode(
        planNodeIdGenerator,
        indexTableHandle,
        makeScanOutputType(testData.lookupOutputColumns),
        indexScanNodeId,
        columnHandles);

    core::PlanNodeId joinNodeId;
    auto plan = makeLookupPlan(
        planNodeIdGenerator,
        indexScanNode,
        probeVectors,
        {"t0", "t1"},
        {"u0", "u1"},
        {testData.inCondition},
        testData.joinType,
        testData.outputColumns,
        joinNodeId);
    runLookupQuery(plan, GetParam().numPrefetches, testData.duckDbVerifySql);
  }
}

DEBUG_ONLY_TEST_P(IndexLookupJoinTest, connectorError) {
  SequenceTableData tableData;
  generateIndexTableData({100, 1, 1}, tableData, pool_);
  const std::vector<RowVectorPtr> probeVectors = generateProbeInput(
      20, 100, 1, tableData, pool_, {"t0", "t1", "t2"}, {}, {}, 100);

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

  const auto indexTable = createIndexTable(
      /*numEqualJoinKeys=*/3, tableData.keyData, tableData.valueData);
  const auto indexTableHandle =
      makeIndexTableHandle(indexTable, GetParam().asyncLookup);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId indexScanNodeId;
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  const auto indexScanNode = makeIndexScanNode(
      planNodeIdGenerator,
      indexTableHandle,
      makeScanOutputType({"u0", "u1", "u2", "u5"}),
      indexScanNodeId,
      columnHandles);

  core::PlanNodeId joinNodeId;
  auto plan = makeLookupPlan(
      planNodeIdGenerator,
      indexScanNode,
      probeVectors,
      {"t0", "t1", "t2"},
      {"u0", "u1", "u2"},
      {},
      core::JoinType::kInner,
      {"u0", "u1", "u2", "t5"},
      joinNodeId);
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool_.get()), errorMsg);
}

DEBUG_ONLY_TEST_P(IndexLookupJoinTest, prefetch) {
  if (!GetParam().asyncLookup) {
    // This test only works for async lookup.
    return;
  }
  SequenceTableData tableData;
  generateIndexTableData({100, 1, 1}, tableData, pool_);
  const int numProbeBatches{20};
  ASSERT_GT(numProbeBatches, GetParam().numPrefetches);
  const std::vector<RowVectorPtr> probeVectors = generateProbeInput(
      numProbeBatches,
      100,
      1,
      tableData,
      pool_,
      {"t0", "t1", "t2"},
      {},
      {},
      100);
  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", {tableData.tableData});

  std::atomic_int lookupCount{0};
  folly::EventCount asyncLookupWait;
  std::atomic_bool asyncLookupWaitFlag{true};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::test::TestIndexSource::ResultIterator::asyncLookup",
      std::function<void(void*)>([&](void*) {
        // Triggers error in the middle.
        if (++lookupCount > 1 + GetParam().numPrefetches) {
          return;
        }
        asyncLookupWait.await([&] { return !asyncLookupWaitFlag.load(); });
      }));

  const auto indexTable = createIndexTable(
      /*numEqualJoinKeys=*/3, tableData.keyData, tableData.valueData);
  const auto indexTableHandle =
      makeIndexTableHandle(indexTable, GetParam().asyncLookup);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId indexScanNodeId;
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  const auto indexScanNode = makeIndexScanNode(
      planNodeIdGenerator,
      indexTableHandle,
      makeScanOutputType({"u0", "u1", "u2", "u3", "u5"}),
      indexScanNodeId,
      columnHandles);

  core::PlanNodeId joinNodeId;
  auto plan = makeLookupPlan(
      planNodeIdGenerator,
      indexScanNode,
      probeVectors,
      {"t0", "t1", "t2"},
      {"u0", "u1", "u2"},
      {},
      core::JoinType::kInner,
      {"u3", "t5"},
      joinNodeId);
  std::thread queryThread([&] {
    runLookupQuery(
        plan,
        GetParam().numPrefetches,
        "SELECT u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND t.c2 = u.c2");
  });
  while (lookupCount < 1 + GetParam().numPrefetches) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
  }
  std::this_thread::sleep_for(std::chrono::seconds(1)); // NOLINT
  ASSERT_EQ(lookupCount, 1 + GetParam().numPrefetches);
  asyncLookupWaitFlag = false;
  asyncLookupWait.notifyAll();
  queryThread.join();
}

TEST_P(IndexLookupJoinTest, outputBatchSize) {
  SequenceTableData tableData;
  generateIndexTableData({3'000, 1, 1}, tableData, pool_);

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

    const auto probeVectors = generateProbeInput(
        testData.numProbeBatches,
        testData.numRowsPerProbeBatch,
        1,
        tableData,
        pool_,
        {"t0", "t1", "t2"},
        {},
        {},
        /*equalMatchPct=*/100);

    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", {tableData.tableData});

    const auto indexTable = createIndexTable(
        /*numEqualJoinKeys=*/3, tableData.keyData, tableData.valueData);
    const auto indexTableHandle =
        makeIndexTableHandle(indexTable, GetParam().asyncLookup);
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId indexScanNodeId;
    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        columnHandles;
    const auto indexScanNode = makeIndexScanNode(
        planNodeIdGenerator,
        indexTableHandle,
        makeScanOutputType({"u0", "u1", "u2", "u5"}),
        indexScanNodeId,
        columnHandles);

    core::PlanNodeId joinNodeId;
    auto plan = makeLookupPlan(
        planNodeIdGenerator,
        indexScanNode,
        probeVectors,
        {"t0", "t1", "t2"},
        {"u0", "u1", "u2"},
        {},
        core::JoinType::kInner,
        {"t4", "u5"},
        joinNodeId);
    const auto task =
        AssertQueryBuilder(duckDbQueryRunner_)
            .plan(plan)
            .config(
                core::QueryConfig::kIndexLookupJoinMaxPrefetchBatches,
                std::to_string(GetParam().numPrefetches))
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

DEBUG_ONLY_TEST_P(IndexLookupJoinTest, runtimeStats) {
  SequenceTableData tableData;
  generateIndexTableData({100, 1, 1}, tableData, pool_);
  const int numProbeBatches{2};
  const std::vector<RowVectorPtr> probeVectors = generateProbeInput(
      numProbeBatches,
      100,
      1,
      tableData,
      pool_,
      {"t0", "t1", "t2"},
      {},
      {},
      100);
  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", {tableData.tableData});

  SCOPED_TESTVALUE_SET(
      "facebook::velox::exec::test::TestIndexSource::ResultIterator::asyncLookup",
      std::function<void(void*)>([&](void*) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); // NOLINT
      }));

  const auto indexTable = createIndexTable(
      /*numEqualJoinKeys=*/3, tableData.keyData, tableData.valueData);
  const auto indexTableHandle =
      makeIndexTableHandle(indexTable, GetParam().asyncLookup);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId indexScanNodeId;
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  const auto indexScanNode = makeIndexScanNode(
      planNodeIdGenerator,
      indexTableHandle,
      makeScanOutputType({"u0", "u1", "u2", "u3", "u5"}),
      indexScanNodeId,
      columnHandles);

  core::PlanNodeId joinNodeId;
  auto plan = makeLookupPlan(
      planNodeIdGenerator,
      indexScanNode,
      probeVectors,
      {"t0", "t1", "t2"},
      {"u0", "u1", "u2"},
      {},
      core::JoinType::kInner,
      {"u3", "t5"},
      joinNodeId);
  auto task = runLookupQuery(
      plan,
      0,
      "SELECT u.c3, t.c5 FROM t, u WHERE t.c0 = u.c0 AND t.c1 = u.c1 AND t.c2 = u.c2");

  auto taskStats = toPlanStats(task->taskStats());
  auto& operatorStats = taskStats.at(joinNodeId);
  ASSERT_EQ(operatorStats.backgroundTiming.count, numProbeBatches);
  ASSERT_GT(operatorStats.backgroundTiming.cpuNanos, 0);
  ASSERT_GT(operatorStats.backgroundTiming.wallNanos, 0);
  auto runtimeStats = operatorStats.customStats;
  ASSERT_EQ(
      runtimeStats.at(IndexLookupJoin::kConnectorLookupWallTime).count,
      numProbeBatches);
  ASSERT_GT(runtimeStats.at(IndexLookupJoin::kConnectorLookupWallTime).sum, 0);
  ASSERT_EQ(
      runtimeStats.at(IndexLookupJoin::kClientLookupWaitWallTime).count,
      numProbeBatches);
  ASSERT_GT(runtimeStats.at(IndexLookupJoin::kClientLookupWaitWallTime).sum, 0);
  ASSERT_EQ(
      runtimeStats.at(IndexLookupJoin::kConnectorResultPrepareTime).count,
      numProbeBatches);
  ASSERT_GT(
      runtimeStats.at(IndexLookupJoin::kConnectorResultPrepareTime).sum, 0);
  ASSERT_EQ(runtimeStats.count(IndexLookupJoin::kClientRequestProcessTime), 0);
  ASSERT_EQ(runtimeStats.count(IndexLookupJoin::kClientResultProcessTime), 0);
  ASSERT_EQ(runtimeStats.count(IndexLookupJoin::kClientLookupResultSize), 0);
  ASSERT_EQ(runtimeStats.count(IndexLookupJoin::kClientLookupResultRawSize), 0);
  ASSERT_THAT(
      operatorStats.toString(true, true),
      testing::MatchesRegex(
          ".*Runtime stats.*connectorLookupWallNanos:.*clientlookupWaitWallNanos.*connectorResultPrepareCpuNanos.*"));
}

TEST_P(IndexLookupJoinTest, joinFuzzer) {
  SequenceTableData tableData;
  generateIndexTableData({1024, 1, 1}, tableData, pool_);
  const auto probeVectors =
      generateProbeInput(50, 256, 1, tableData, pool_, {"t0", "t1", "t2"});

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", {tableData.tableData});

  const auto indexTable = createIndexTable(
      /*numEqualJoinKeys=*/1, tableData.keyData, tableData.valueData);
  const auto indexTableHandle =
      makeIndexTableHandle(indexTable, GetParam().asyncLookup);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId indexScanNodeId;
  auto scanOutput = tableType_->names();
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(scanOutput.begin(), scanOutput.end(), g);
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  const auto indexScanNode = makeIndexScanNode(
      planNodeIdGenerator,
      indexTableHandle,
      makeScanOutputType(scanOutput),
      indexScanNodeId,
      columnHandles);

  core::PlanNodeId joinNodeId;
  auto plan = makeLookupPlan(
      planNodeIdGenerator,
      indexScanNode,
      probeVectors,
      {"t0"},
      {"u0"},
      {"contains(t4, u1)", "u2 between t1 and t2"},
      core::JoinType::kInner,
      {"u0", "u4", "t0", "t1", "t4"},
      joinNodeId);
  runLookupQuery(
      plan,
      GetParam().numPrefetches,
      "SELECT u.c0, u.c1, u.c2, u.c3, u.c4, u.c5, t.c0, t.c1, t.c2, t.c3, t.c4, t.c5 FROM t, u WHERE t.c0 = u.c0 AND array_contains(t.c4, u.c1) AND u.c2 BETWEEN t.c1 AND t.c2");
}
} // namespace

VELOX_INSTANTIATE_TEST_SUITE_P(
    IndexLookupJoinTest,
    IndexLookupJoinTest,
    testing::ValuesIn(IndexLookupJoinTest::getTestParams()),
    [](const testing::TestParamInfo<TestParam>& info) {
      return fmt::format(
          "{}_{}prefetches",
          info.param.asyncLookup ? "async" : "sync",
          info.param.numPrefetches);
    });
} // namespace fecebook::velox::exec::test
