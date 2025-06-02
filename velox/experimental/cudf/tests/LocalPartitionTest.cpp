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
#include "velox/experimental/cudf/exec/ToCudf.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec::test {
namespace {

class LocalPartitionTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    cudf_velox::registerCudf();
  }

  template <typename T>
  FlatVectorPtr<T> makeFlatSequence(T start, vector_size_t size) {
    return makeFlatVector<T>(size, [start](auto row) { return start + row; });
  }

  template <typename T>
  FlatVectorPtr<T> makeFlatSequence(T start, T max, vector_size_t size) {
    return makeFlatVector<T>(
        size, [start, max](auto row) { return (start + row) % max; });
  }

  std::vector<std::shared_ptr<TempFilePath>> writeToFiles(
      const std::vector<RowVectorPtr>& vectors) {
    auto filePaths = makeFilePaths(vectors.size());
    for (auto i = 0; i < vectors.size(); i++) {
      writeToFile(filePaths[i]->getPath(), vectors[i]);
    }
    return filePaths;
  }
};

TEST_F(LocalPartitionTest, gather) {
  std::vector<RowVectorPtr> vectors = {
      makeRowVector({makeFlatSequence<int32_t>(0, 100)}),
      makeRowVector({makeFlatSequence<int32_t>(53, 100)}),
      makeRowVector({makeFlatSequence<int32_t>(-71, 100)}),
  };

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  auto valuesNode = [&](int index) {
    return PlanBuilder(planNodeIdGenerator).values({vectors[index]}).planNode();
  };

  auto op = PlanBuilder(planNodeIdGenerator)
                .localPartition(
                    {},
                    {
                        valuesNode(0),
                        valuesNode(1),
                        valuesNode(2),
                    })
                .singleAggregation({}, {"min(c0)", "max(c0)"})
                .planNode();

  auto task = assertQuery(op, "SELECT -71, 152");

  auto filePaths = writeToFiles(vectors);

  auto rowType = asRowType(vectors[0]->type());

  std::vector<core::PlanNodeId> scanNodeIds;

  auto tableScanNode = [&]() {
    auto node = PlanBuilder(planNodeIdGenerator).tableScan(rowType).planNode();
    scanNodeIds.push_back(node->id());
    return node;
  };

  op = PlanBuilder(planNodeIdGenerator)
           .localPartition(
               {},
               {
                   tableScanNode(),
                   tableScanNode(),
                   tableScanNode(),
               })
           .singleAggregation({}, {"min(c0)", "max(c0)"})
           .planNode();

  AssertQueryBuilder queryBuilder(op, duckDbQueryRunner_);
  for (auto i = 0; i < filePaths.size(); ++i) {
    queryBuilder.split(
        scanNodeIds[i], makeHiveConnectorSplit(filePaths[i]->getPath()));
  }

  task = queryBuilder.assertResults("SELECT -71, 152");
}

TEST_F(LocalPartitionTest, partition) {
  std::vector<RowVectorPtr> vectors = {
      makeRowVector({makeFlatSequence<int32_t>(0, 100)}),
      makeRowVector({makeFlatSequence<int32_t>(53, 100)}),
      makeRowVector({makeFlatSequence<int32_t>(-71, 100)}),
  };

  auto filePaths = writeToFiles(vectors);

  auto rowType = asRowType(vectors[0]->type());

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

  std::vector<core::PlanNodeId> scanNodeIds;

  auto scanAggNode = [&]() {
    auto builder = PlanBuilder(planNodeIdGenerator);
    auto scanNode = builder.tableScan(rowType).planNode();
    scanNodeIds.push_back(scanNode->id());
    return builder.partialAggregation({"c0"}, {"max(c0)"}).planNode();
  };

  auto op = PlanBuilder(planNodeIdGenerator)
                .localPartition(
                    {"c0"},
                    {
                        scanAggNode(),
                        scanAggNode(),
                        scanAggNode(),
                    })
                .finalAggregation()
                .planNode();

  createDuckDbTable(vectors);

  AssertQueryBuilder queryBuilder(op, duckDbQueryRunner_);
  queryBuilder.maxDrivers(2);
  queryBuilder.config(core::QueryConfig::kMaxLocalExchangePartitionCount, "2");

  for (auto i = 0; i < filePaths.size(); ++i) {
    queryBuilder.split(
        scanNodeIds[i], makeHiveConnectorSplit(filePaths[i]->getPath()));
  }

  auto task =
      queryBuilder.assertResults("SELECT c0, max(c0) FROM tmp GROUP BY 1");
}

} // namespace
} // namespace facebook::velox::exec::test
