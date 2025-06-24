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

#include <boost/random/uniform_int_distribution.hpp>
#include <gtest/gtest.h>

#include <string>

#include <folly/experimental/EventCount.h>

#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/TestIndexStorageConnector.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/tool/trace/IndexLookupJoinReplayer.h"

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::common;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::connector;
using namespace facebook::velox::connector::hive;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::common::hll;
using namespace facebook::velox::tests::utils;

namespace facebook::velox::tool::trace::test {
class IndexLookupJoinReplayerTest : public HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    HiveConnectorTestBase::SetUpTestCase();
    registerFaultyFileSystem();
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    Type::registerSerDe();
    common::Filter::registerSerDe();
    connector::hive::HiveTableHandle::registerSerDe();
    connector::hive::LocationHandle::registerSerDe();
    connector::hive::HiveColumnHandle::registerSerDe();
    connector::hive::HiveInsertTableHandle::registerSerDe();
    connector::hive::HiveInsertFileNameGenerator::registerSerDe();
    connector::hive::HiveConnectorSplit::registerSerDe();
    core::PlanNode::registerSerDe();
    velox::exec::trace::registerDummySourceSerDe();
    core::ITypedExpr::registerSerDe();
    registerPartitionFunctionSerDe();
    auto connectorCpuExecutor =
        std::make_unique<folly::CPUThreadPoolExecutor>(128);
    TestIndexConnectorFactory::registerConnector(connectorCpuExecutor.get());
    TestIndexTableHandle::registerSerDe();
    TestIndexColumnHandle::registerSerDe();
  }

  void TearDown() override {
    probeInput_.clear();
    indexInput_.clear();
    HiveConnectorTestBase::TearDown();
    connector::unregisterConnectorFactory(kTestIndexConnectorName);
    connector::unregisterConnector(kTestIndexConnectorName);
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
        keyType, std::move(valueType), std::move(table));
  }

  // Makes index table handle with the specified index table and async lookup
  // flag.
  static std::shared_ptr<TestIndexTableHandle> makeIndexTableHandle(
      const std::shared_ptr<TestIndexTable>& indexTable) {
    return std::make_shared<TestIndexTableHandle>(
        kTestIndexConnectorName, indexTable, /*asyncLookup*/ false);
  }

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    core::PlanNodeId probeScanId;
    core::PlanNodeId indexScanId;
    std::unordered_map<core::PlanNodeId, std::vector<exec::Split>> splits;

    explicit PlanWithSplits(
        const core::PlanNodePtr& _plan,
        const core::PlanNodeId& _probeScanId = "",
        const core::PlanNodeId& _indexScanId = "",
        const std::unordered_map<
            core::PlanNodeId,
            std::vector<velox::exec::Split>>& _splits = {})
        : plan(_plan),
          probeScanId(_probeScanId),
          indexScanId(_indexScanId),
          splits(_splits) {}
  };

  RowTypePtr concat(const RowTypePtr& a, const RowTypePtr& b) {
    std::vector<std::string> names = a->names();
    std::vector<TypePtr> types = a->children();

    for (auto i = 0; i < b->size(); ++i) {
      names.push_back(b->nameOf(i));
      types.push_back(b->childAt(i));
    }

    return ROW(std::move(names), std::move(types));
  }

  std::vector<RowVectorPtr>
  makeVectors(int32_t count, int32_t rowsPerVector, const RowTypePtr& rowType) {
    return HiveConnectorTestBase::makeVectors(rowType, count, rowsPerVector);
  }

  std::vector<Split> makeSplits(
      const std::vector<RowVectorPtr>& inputs,
      const std::string& path,
      memory::MemoryPool* writerPool) {
    std::vector<Split> splits;
    for (auto i = 0; i < 4; ++i) {
      const std::string filePath = fmt::format("{}/{}", path, i);
      writeToFile(filePath, inputs);
      splits.emplace_back(makeHiveConnectorSplit(filePath));
    }

    return splits;
  }

  core::PlanNodeId traceNodeId_;
  RowTypePtr probeType_{
      ROW({"t0", "t1", "t2", "t3"}, {BIGINT(), VARCHAR(), SMALLINT(), REAL()})};

  RowTypePtr indexType_{
      ROW({"u0", "u1", "u2", "u3"},
          {BIGINT(), INTEGER(), SMALLINT(), VARCHAR()})};
  std::vector<RowVectorPtr> probeInput_ = makeVectors(5, 100, probeType_);
  std::vector<RowVectorPtr> indexInput_ = makeVectors(3, 100, indexType_);

  const std::vector<std::string> leftKeys{"t0"};
  const std::vector<std::string> rightKeys{"u0"};
  const std::shared_ptr<TempDirectoryPath> testDir_ =
      TempDirectoryPath::create();
  const std::string tableDir_ =
      fmt::format("{}/{}", testDir_->getPath(), "table");
  const std::unique_ptr<folly::CPUThreadPoolExecutor> connectorCpuExecutor_{
      std::make_unique<folly::CPUThreadPoolExecutor>(128)};
};

TEST_F(IndexLookupJoinReplayerTest, test) {
  // Create a plan with a table scan and an index lookup join
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId probeScanId;

  // Create a TestIndexTable for the index side
  const auto indexTable = createIndexTable(
      /*numEqualJoinKeys=*/1,
      makeRowVector(
          indexType_->names(),
          {makeFlatVector<int64_t>({1, 2, 3}),
           makeFlatVector<int32_t>({10, 20, 30}),
           makeFlatVector<int16_t>({100, 200, 300}),
           makeFlatVector<StringView>({"a", "b", "c"})}),
      makeRowVector(
          {"u4", "u5"},
          {makeFlatVector<int64_t>({1000, 2000, 3000}),
           makeFlatVector<StringView>({"x", "y", "z"})}));

  // Create a TestIndexTableHandle with the TestIndexTable
  auto indexTableHandle = makeIndexTableHandle(indexTable);

  // Create a table scan node with the TestIndexTableHandle
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      columnHandles;
  for (const auto& name : indexType_->names()) {
    columnHandles[name] = std::make_shared<TestIndexColumnHandle>(name);
  }

  auto indexScan = std::make_shared<core::TableScanNode>(
      planNodeIdGenerator->next(), indexType_, indexTableHandle, columnHandles);

  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .tableScan(probeType_)
          .capturePlanNodeId(probeScanId)
          .indexLookupJoin(
              leftKeys,
              rightKeys,
              std::dynamic_pointer_cast<const core::TableScanNode>(indexScan),
              /*joinConditions=*/{},
              concat(probeType_, indexType_)->names(),
              core::JoinType::kInner)
          .capturePlanNodeId(traceNodeId_)
          .planNode();

  // Create a trace directory
  const auto testDir = TempDirectoryPath::create();
  const auto traceRoot = fmt::format("{}/{}", testDir->getPath(), "traceRoot");
  std::shared_ptr<Task> task;

  // Create a file for the probe data
  const auto sourceFilePath = TempFilePath::create();
  writeToFile(sourceFilePath->getPath(), probeInput_);

  // Run the query with tracing enabled
  auto results =
      AssertQueryBuilder(plan)
          .config(core::QueryConfig::kQueryTraceEnabled, true)
          .config(core::QueryConfig::kQueryTraceDir, traceRoot)
          .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
          .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
          .config(core::QueryConfig::kQueryTraceNodeId, traceNodeId_)
          .splits(
              probeScanId,
              {Split(makeHiveConnectorSplit(sourceFilePath->getPath()))})
          .copyResults(pool(), task);

  // Run the replayer
  const auto replayingResult = IndexLookupJoinReplayer(
                                   traceRoot,
                                   task->queryCtx()->queryId(),
                                   task->taskId(),
                                   traceNodeId_,
                                   "IndexLookupJoin",
                                   "",
                                   0,
                                   executor_.get())
                                   .run();
  assertEqualResults({results}, {replayingResult});
}

} // namespace facebook::velox::tool::trace::test
