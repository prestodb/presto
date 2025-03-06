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

  struct SequenceTableData {
    RowVectorPtr keyData;
    RowVectorPtr valueData;
    RowVectorPtr tableData;
    std::vector<int64_t> minKeys;
    std::vector<int64_t> maxKeys;
  };

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

  int getNumRows(const std::vector<int>& cardinalities) {
    int numRows{1};
    for (const auto& cardinality : cardinalities) {
      numRows *= cardinality;
    }
    return numRows;
  }

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
      SequenceTableData& tableData) {
    VELOX_CHECK_EQ(keyCardinalities.size(), keyType_->size());
    const auto numRows = getNumRows(keyCardinalities);
    VectorFuzzer::Options opts;
    opts.vectorSize = numRows;
    opts.nullRatio = 0.0;
    opts.allowSlice = false;
    VectorFuzzer fuzzer(opts, pool_.get());

    tableData.keyData = fuzzer.fuzzInputFlatRow(keyType_);
    tableData.valueData = fuzzer.fuzzInputFlatRow(valueType_);

    VELOX_CHECK_EQ(numRows, tableData.keyData->size());
    tableData.maxKeys.resize(keyType_->size());
    tableData.minKeys.resize(keyType_->size());
    // Set the key column vector to the same value to easy testing with
    // specified match ratio.
    for (int i = keyType_->size() - 1, numRepeats = 1; i >= 0;
         numRepeats *= keyCardinalities[i--]) {
      int64_t minKey = std::numeric_limits<int64_t>::max();
      int64_t maxKey = std::numeric_limits<int64_t>::min();
      int numKeys = keyCardinalities[i];
      tableData.keyData->childAt(i) =
          makeFlatVector<int64_t>(tableData.keyData->size(), [&](auto row) {
            const int64_t keyValue = 1 + (row / numRepeats) % numKeys;
            minKey = std::min(minKey, keyValue);
            maxKey = std::max(maxKey, keyValue);
            return keyValue;
          });
      tableData.minKeys[i] = minKey;
      tableData.maxKeys[i] = maxKey;
    }

    std::vector<VectorPtr> tableColumns;
    VELOX_CHECK_EQ(tableType_->size(), keyType_->size() + valueType_->size());
    tableColumns.reserve(tableType_->size());
    for (auto i = 0; i < keyType_->size(); ++i) {
      tableColumns.push_back(tableData.keyData->childAt(i));
    }
    for (auto i = 0; i < valueType_->size(); ++i) {
      tableColumns.push_back(tableData.valueData->childAt(i));
    }
    tableData.tableData = makeRowVector(tableType_->names(), tableColumns);
  }

  // Generate probe input for lookup join.
  // @param numBatches: number of probe batches.
  // @param batchSize: number of rows in each probe batch.
  // @param tableData: contains the sequence table data including key vectors
  // and min/max key values.
  // @param probeJoinKeys: the prefix key colums used for equality joins.
  // @param inColumns: the ordered list of in conditions.
  // @param betweenColumns: the ordered list of between conditions.
  // @param equalMatchPct: percentage of rows in the probe input that
  // matches
  //                       with the rows in index table.
  // @param betweenMatchPct: percentage of rows in the probe input that
  // matches
  //                         the rows in index table with between
  //                         conditions.
  // @param inMatchPct: percentage of rows in the probe input that matches
  // the
  //                    rows in index table with in conditions.
  std::vector<RowVectorPtr> generateProbeInput(
      size_t numBatches,
      size_t batchSize,
      SequenceTableData& tableData,
      const std::vector<std::string>& probeJoinKeys,
      const std::vector<std::string> inColumns = {},
      const std::vector<std::pair<std::string, std::string>>& betweenColumns =
          {},
      std::optional<int> equalMatchPct = std::nullopt,
      std::optional<int> inMatchPct = std::nullopt,
      std::optional<int> betweenMatchPct = std::nullopt) {
    VELOX_CHECK_EQ(
        probeJoinKeys.size() + betweenColumns.size() + inColumns.size(), 3);
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

    if (tableData.keyData->size() == 0) {
      return probeInputs;
    }

    const auto numTableRows = tableData.keyData->size();
    std::vector<FlatVectorPtr<int64_t>> tableKeyVectors;
    for (int i = 0; i < probeJoinKeys.size(); ++i) {
      auto keyVector = tableData.keyData->childAt(i);
      keyVector->loadedVector();
      BaseVector::flattenVector(keyVector);
      tableKeyVectors.push_back(
          std::dynamic_pointer_cast<FlatVector<int64_t>>(keyVector));
    }

    if (equalMatchPct.has_value()) {
      VELOX_CHECK_GE(equalMatchPct.value(), 0);
      VELOX_CHECK_LE(equalMatchPct.value(), 100);
      for (int i = 0, totalRows = 0; i < numBatches; ++i) {
        std::vector<FlatVectorPtr<int64_t>> probeKeyVectors;
        for (int j = 0; j < probeJoinKeys.size(); ++j) {
          probeKeyVectors.push_back(BaseVector::create<FlatVector<int64_t>>(
              probeType_->findChild(probeJoinKeys[j]),
              probeInputs[i]->size(),
              pool_.get()));
        }
        for (int row = 0; row < probeInputs[i]->size(); ++row, ++totalRows) {
          if (totalRows % 100 < equalMatchPct.value()) {
            const auto matchKeyRow = folly::Random::rand64(numTableRows);
            for (int j = 0; j < probeJoinKeys.size(); ++j) {
              probeKeyVectors[j]->set(
                  row, tableKeyVectors[j]->valueAt(matchKeyRow));
            }
          } else {
            for (int j = 0; j < probeJoinKeys.size(); ++j) {
              probeKeyVectors[j]->set(
                  row, tableData.maxKeys[j] + 1 + folly::Random::rand32());
            }
          }
        }
        for (int j = 0; j < probeJoinKeys.size(); ++j) {
          probeInputs[i]->childAt(j) = probeKeyVectors[j];
        }
      }
    }

    if (inMatchPct.has_value()) {
      VELOX_CHECK(!inColumns.empty());
      VELOX_CHECK_GE(inMatchPct.value(), 0);
      VELOX_CHECK_LE(inMatchPct.value(), 100);
      for (int i = 0; i < inColumns.size(); ++i) {
        const auto inColumnName = inColumns[i];
        const auto inColumnChannel = probeType_->getChildIdx(inColumnName);
        auto inColumnType = std::dynamic_pointer_cast<const ArrayType>(
            probeType_->childAt(inColumnChannel));
        VELOX_CHECK_NOT_NULL(inColumnType);
        const auto tableKeyChannel = probeJoinKeys.size() + i;
        VELOX_CHECK(keyType_->childAt(tableKeyChannel)
                        ->equivalent(*inColumnType->elementType()));
        const auto minValue = !inMatchPct.has_value()
            ? tableData.minKeys[tableKeyChannel] - 1
            : tableData.minKeys[tableKeyChannel];
        const auto maxValue = !inMatchPct.has_value()
            ? minValue
            : tableData.minKeys[tableKeyChannel] +
                (tableData.maxKeys[tableKeyChannel] -
                 tableData.minKeys[tableKeyChannel]) *
                    inMatchPct.value() / 100;
        for (int i = 0; i < numBatches; ++i) {
          probeInputs[i]->childAt(inColumnChannel) = makeArrayVector<int64_t>(
              probeInputs[i]->size(),
              [&](auto row) -> vector_size_t {
                return maxValue - minValue + 1;
              },
              [&](auto /*unused*/, auto index) { return minValue + index; });
        }
      }
    }

    if (betweenMatchPct.has_value()) {
      VELOX_CHECK(!betweenColumns.empty());
      VELOX_CHECK_GE(betweenMatchPct.value(), 0);
      VELOX_CHECK_LE(betweenMatchPct.value(), 100);
      for (int i = 0; i < betweenColumns.size(); ++i) {
        const auto tableKeyChannel = probeJoinKeys.size() + i;
        const auto betweenColumn = betweenColumns[i];
        const auto lowerBoundColumn = betweenColumn.first;
        std::optional<int32_t> lowerBoundChannel;
        if (!lowerBoundColumn.empty()) {
          lowerBoundChannel = probeType_->getChildIdx(lowerBoundColumn);
          VELOX_CHECK(probeType_->childAt(lowerBoundChannel.value())
                          ->equivalent(*keyType_->childAt(tableKeyChannel)));
        }
        const auto upperBoundColumn = betweenColumn.first;
        std::optional<int32_t> upperBoundChannel;
        if (!upperBoundColumn.empty()) {
          upperBoundChannel = probeType_->getChildIdx(upperBoundColumn);
          VELOX_CHECK(probeType_->childAt(upperBoundChannel.value())
                          ->equivalent(*keyType_->childAt(tableKeyChannel)));
        }
        for (int i = 0; i < numBatches; ++i) {
          if (lowerBoundChannel.has_value()) {
            probeInputs[i]->childAt(lowerBoundChannel.value()) =
                makeFlatVector<int64_t>(
                    probeInputs[i]->size(), [&](auto /*unused*/) {
                      return tableData.minKeys[tableKeyChannel];
                    });
          }
          const auto upperBoundColumn = betweenColumn.second;
          if (upperBoundChannel.has_value()) {
            probeInputs[i]->childAt(upperBoundChannel.value()) =
                makeFlatVector<int64_t>(
                    probeInputs[i]->size(), [&](auto /*unused*/) -> int64_t {
                      if (betweenMatchPct.value() == 0) {
                        return tableData.minKeys[tableKeyChannel] - 1;
                      } else {
                        return tableData.minKeys[tableKeyChannel] +
                            (tableData.maxKeys[tableKeyChannel] -
                             tableData.minKeys[tableKeyChannel]) *
                            betweenMatchPct.value() / 100;
                      }
                    });
          }
        }
      }
    }
    return probeInputs;
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
    // Change each column with prefix 'c' to simplify the duckdb table
    // column naming.
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
  // @param scanNodeId: returns the plan node id of the index table scan
  // node.
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
  // @param leftKeys: the left join keys of index lookup join.
  // @param rightKeys: the right join keys of index lookup join.
  // @param joinType: the join type of index lookup join.
  // @param outputColumns: the output column names of index lookup join.
  // @param joinNodeId: returns the plan node id of the index lookup join
  // node.
  core::PlanNodePtr makeLookupPlan(
      const std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator,
      core::TableScanNodePtr indexScanNode,
      const std::vector<RowVectorPtr>& probeVectors,
      const std::vector<std::string>& leftKeys,
      const std::vector<std::string>& rightKeys,
      const std::vector<std::string>& joinConditions,
      core::JoinType joinType,
      const std::vector<std::string>& outputColumns,
      core::PlanNodeId& joinNodeId) {
    VELOX_CHECK_EQ(leftKeys.size(), rightKeys.size());
    VELOX_CHECK_LE(leftKeys.size(), keyType_->size());
    return PlanBuilder(planNodeIdGenerator)
        .values(probeVectors)
        .indexLookupJoin(
            leftKeys,
            rightKeys,
            indexScanNode,
            joinConditions,
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
    generateIndexTableData(testData.keyCardinalities, tableData);
    auto probeVectors = generateProbeInput(
        testData.numProbeBatches,
        testData.numRowsPerProbeBatch,
        tableData,
        {"t0", "t1", "t2"},
        {},
        {},
        testData.matchPct);

    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", {tableData.tableData});

    const auto indexTable = createIndexTable(
        /*numEqualJoinKeys=*/3, tableData.keyData, tableData.valueData);
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
        {"t0", "t1", "t2"},
        {"u0", "u1", "u2"},
        {},
        testData.joinType,
        testData.outputColumns,
        joinNodeId);
    AssertQueryBuilder(duckDbQueryRunner_)
        .plan(plan)
        .assertResults(testData.duckDbVerifySql);
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
    generateIndexTableData(testData.keyCardinalities, tableData);
    auto probeVectors = generateProbeInput(
        testData.numProbeBatches,
        testData.numProbeRowsPerBatch,
        tableData,
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
    const auto indexTableHandle = makeIndexTableHandle(indexTable, GetParam());
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId indexScanNodeId;
    const auto indexScanNode = makeIndexScanNode(
        planNodeIdGenerator,
        indexTableHandle,
        makeScanOutputType(testData.lookupOutputColumns),
        indexScanNodeId);

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
    AssertQueryBuilder(duckDbQueryRunner_)
        .plan(plan)
        .assertResults(testData.duckDbVerifySql);
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
    generateIndexTableData(testData.keyCardinalities, tableData);
    auto probeVectors = generateProbeInput(
        testData.numProbeBatches,
        testData.numProbeRowsPerBatch,
        tableData,
        {"t0", "t1"},
        {{"t4"}},
        {},
        /*eqaulityMatchPct=*/80,
        testData.inMatchPct);

    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", {tableData.tableData});

    const auto indexTable = createIndexTable(
        /*numEqualJoinKeys=*/2, tableData.keyData, tableData.valueData);
    const auto indexTableHandle = makeIndexTableHandle(indexTable, GetParam());
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId indexScanNodeId;
    const auto indexScanNode = makeIndexScanNode(
        planNodeIdGenerator,
        indexTableHandle,
        makeScanOutputType(testData.lookupOutputColumns),
        indexScanNodeId);

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
    AssertQueryBuilder(duckDbQueryRunner_)
        .plan(plan)
        .assertResults(testData.duckDbVerifySql);
  }
}

DEBUG_ONLY_TEST_P(IndexLookupJoinTest, connectorError) {
  SequenceTableData tableData;
  generateIndexTableData({100, 1, 1}, tableData);
  const std::vector<RowVectorPtr> probeVectors =
      generateProbeInput(20, 100, tableData, {"t0", "t1", "t2"}, {}, {}, 100);

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
      {"t0", "t1", "t2"},
      {"u0", "u1", "u2"},
      {},
      core::JoinType::kInner,
      {"u0", "u1", "u2", "t5"},
      joinNodeId);
  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool_.get()), errorMsg);
}

TEST_P(IndexLookupJoinTest, outputBatchSize) {
  SequenceTableData tableData;
  generateIndexTableData({3'000, 1, 1}, tableData);

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
        tableData,
        {"t0", "t1", "t2"},
        {},
        {},
        /*equalMatchPct=*/100);

    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", {tableData.tableData});

    const auto indexTable = createIndexTable(
        /*numEqualJoinKeys=*/3, tableData.keyData, tableData.valueData);
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

TEST_P(IndexLookupJoinTest, joinFuzzer) {
  SequenceTableData tableData;
  generateIndexTableData({1024, 1, 1}, tableData);
  const auto probeVectors =
      generateProbeInput(50, 256, tableData, {"t0", "t1", "t2"});

  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", {tableData.tableData});

  const auto indexTable = createIndexTable(
      /*numEqualJoinKeys=*/1, tableData.keyData, tableData.valueData);
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
  AssertQueryBuilder(duckDbQueryRunner_)
      .plan(plan)
      .assertResults(
          "SELECT u.c0, u.c1, u.c2, u.c3, u.c4, u.c5, t.c0, t.c1, t.c2, t.c3, t.c4, t.c5 FROM t, u WHERE t.c0 = u.c0 AND array_contains(t.c4, u.c1) AND u.c2 BETWEEN t.c1 AND t.c2");
}
} // namespace

VELOX_INSTANTIATE_TEST_SUITE_P(
    IndexLookupJoinTest,
    IndexLookupJoinTest,
    testing::ValuesIn({false, true}));
} // namespace fecebook::velox::exec::test
