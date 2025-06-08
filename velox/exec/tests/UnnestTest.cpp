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
#include "velox/common/testutil/OptionalEmpty.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempFilePath.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class UnnestTest : public HiveConnectorTestBase,
                   public testing::WithParamInterface<vector_size_t> {
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
  }

  void TearDown() override {
    HiveConnectorTestBase::TearDown();
  }

 protected:
  CursorParameters makeCursorParameters(
      const core::PlanNodePtr& planNode) const {
    CursorParameters params;
    params.planNode = planNode;
    params.queryConfigs[core::QueryConfig::kPreferredOutputBatchRows] =
        std::to_string(batchSize_);
    return params;
  }

  const vector_size_t batchSize_{GetParam()};
};

TEST_P(UnnestTest, basicArray) {
  auto vector = makeRowVector({
      makeFlatVector<int64_t>(100, [](auto row) { return row; }),
      makeArrayVector<int32_t>(
          100,
          [](auto row) { return row % 5 + 1; },
          [](auto row, auto index) { return index * (row % 3); },
          nullEvery(7)),
  });

  createDuckDbTable({vector});

  // TODO Add tests with empty arrays. This requires better support in DuckDB.

  auto op = PlanBuilder().values({vector}).unnest({"c0"}, {"c1"}).planNode();
  auto params = makeCursorParameters(op);
  assertQuery(params, "SELECT c0, UNNEST(c1) FROM tmp WHERE c0 % 7 > 0");
}

TEST_P(UnnestTest, arrayWithIdentityMap) {
  struct {
    int32_t vectorSize;
    int32_t arraySize;
    int32_t outputBatchSize;
    int32_t expectedOutputBatches;

    std::string debugString() const {
      return fmt::format(
          "vectorSize: {}, arraySize: {}, outputBatchSize: {}, expectedOutputBatches: {}",
          vectorSize,
          arraySize,
          outputBatchSize,
          expectedOutputBatches);
    }
  } testSettings[] = {
      {100, 1, 1, 100},
      {100, 1, 100, 1},
      {100, 4, 1, 400},
      {100, 4, 100, 4},
      {100, 4, 400, 1},
      {1024, 4, 256, 16},
      {1024, 4, 1024, 4},
      {1024, 4, 4096, 1},
      {1024, 1, 256, 4},
      {1024, 4, 7, 586},
      {1024, 1, 7, 147}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto vector = makeRowVector(
        {makeFlatVector<int64_t>(
             testData.vectorSize, [](auto row) { return row; }),
         makeArrayVector<int32_t>(
             testData.vectorSize,
             [&](auto /*unused*/) { return testData.arraySize; },
             [](auto row, auto index) { return index * (row % 3); })});
    createDuckDbTable({vector});

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId unnestPlanNodeId;
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({vector})
                    .unnest({"c0"}, {"c1"})
                    .capturePlanNodeId(unnestPlanNodeId)
                    .planNode();

    auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                    .config(
                        core::QueryConfig::kPreferredOutputBatchRows,
                        std::to_string(testData.outputBatchSize))
                    .assertResults({"SELECT c0, UNNEST(c1) FROM tmp"});
    const auto taskStats = task->taskStats();
    ASSERT_EQ(
        exec::toPlanStats(taskStats).at(unnestPlanNodeId).outputVectors,
        testData.expectedOutputBatches);
  }
}

TEST_P(UnnestTest, arrayWithoutIdentityMap) {
  struct {
    int32_t vectorSize;
    int32_t arraySize1;
    int32_t arraySize2;
    int32_t outputBatchSize;
    int32_t expectedOutputBatches;

    std::string debugString() const {
      return fmt::format(
          "vectorSize: {}, arraySize1: {}, arraySize2: {}, outputBatchSize: {}, expectedOutputBatches: {}",
          vectorSize,
          arraySize1,
          arraySize2,
          outputBatchSize,
          expectedOutputBatches);
    }
  } testSettings[] = {
      {100, 1, 2, 100, 2},
      {100, 1, 2, 200, 1},
      {1024, 1, 4, 256, 16},
      {1024, 3, 4, 256, 16},
      {1024, 1, 4, 4096, 1},
      {1024, 3, 4, 4096, 1},
      {1024, 1, 4, 7, 586},
      {1024, 3, 4, 7, 586}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto vector = makeRowVector(
        {makeFlatVector<int64_t>(
             testData.vectorSize, [](auto row) { return row; }),
         makeArrayVector<int32_t>(
             testData.vectorSize,
             [&](auto /*unused*/) { return testData.arraySize1; },
             [](auto row, auto index) { return index * (row % 3); }),
         makeArrayVector<int32_t>(
             testData.vectorSize,
             [&](auto /*unused*/) { return testData.arraySize2; },
             [](auto row, auto index) { return index * (row % 3); })});
    createDuckDbTable({vector});

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId unnestPlanNodeId;
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values({vector})
                    .unnest({"c0"}, {"c1", "c2"})
                    .capturePlanNodeId(unnestPlanNodeId)
                    .planNode();

    auto task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(
                core::QueryConfig::kPreferredOutputBatchRows,
                std::to_string(testData.outputBatchSize))
            .assertResults({"SELECT c0, UNNEST(c1), UNNEST(c2) FROM tmp"});
    const auto taskStats = task->taskStats();
    ASSERT_EQ(
        exec::toPlanStats(taskStats).at(unnestPlanNodeId).outputVectors,
        testData.expectedOutputBatches);
  }
}

TEST_P(UnnestTest, arrayWithNull) {
  const auto vector = makeRowVector(
      {makeFlatVector<int64_t>(1024, [](auto row) { return row; }),
       makeArrayVector<int32_t>(
           1024,
           [&](auto /*unused*/) { return 3; },
           [](auto row, auto index) { return index * (row % 3); },
           nullEvery(6)),
       makeArrayVector<int32_t>(
           1024,
           [&](auto /*unused*/) { return 4; },
           [](auto row, auto index) { return index * (row % 3); })});
  createDuckDbTable({vector});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId unnestPlanNodeId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values({vector})
                  .unnest({"c0"}, {"c1", "c2"})
                  .capturePlanNodeId(unnestPlanNodeId)
                  .planNode();

  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(
              core::QueryConfig::kPreferredOutputBatchRows, std::to_string(25))
          .assertResults({"SELECT c0, UNNEST(c1), UNNEST(c2) FROM tmp"});
  const auto taskStats = task->taskStats();
  ASSERT_EQ(
      exec::toPlanStats(taskStats).at(unnestPlanNodeId).outputVectors, 164);
}

TEST_P(UnnestTest, arrayWithOrdinality) {
  auto array = vectorMaker_.arrayVectorNullable<int32_t>(
      {{{1, 2, std::nullopt, 4}},
       std::nullopt,
       {{5, 6}},
       common::testutil::optionalEmpty,
       {{{{std::nullopt}}}},
       {{7, 8, 9}}});
  auto vector = makeRowVector(
      {makeNullableFlatVector<double>({1.1, 2.2, 3.3, 4.4, 5.5, std::nullopt}),
       array});

  auto op = PlanBuilder()
                .values({vector})
                .unnest({"c0"}, {"c1"}, "ordinal")
                .planNode();

  auto expected = makeRowVector(
      {makeNullableFlatVector<double>(
           {1.1,
            1.1,
            1.1,
            1.1,
            3.3,
            3.3,
            5.5,
            std::nullopt,
            std::nullopt,
            std::nullopt}),
       makeNullableFlatVector<int32_t>(
           {1, 2, std::nullopt, 4, 5, 6, std::nullopt, 7, 8, 9}),
       makeNullableFlatVector<int64_t>({1, 2, 3, 4, 1, 2, 1, 1, 2, 3})});
  auto params = makeCursorParameters(op);
  assertQuery(params, expected);

  // Test with array wrapped in dictionary.
  auto reversedIndices = makeIndicesInReverse(6);
  auto vectorInDict = makeRowVector(
      {makeNullableFlatVector<double>({1.1, 2.2, 3.3, 4.4, 5.5, std::nullopt}),
       wrapInDictionary(reversedIndices, 6, array)});
  op = PlanBuilder()
           .values({vectorInDict})
           .unnest({"c0"}, {"c1"}, "ordinal")
           .planNode();

  auto expectedInDict = makeRowVector(
      {makeNullableFlatVector<double>(
           {1.1,
            1.1,
            1.1,
            2.2,
            4.4,
            4.4,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            std::nullopt}),
       makeNullableFlatVector<int32_t>(
           {7, 8, 9, std::nullopt, 5, 6, 1, 2, std::nullopt, 4}),
       makeNullableFlatVector<int64_t>({1, 2, 3, 1, 1, 2, 1, 2, 3, 4})});
  params = makeCursorParameters(op);
  assertQuery(params, expectedInDict);
}

TEST_P(UnnestTest, basicMap) {
  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeMapVector<int64_t, double>(
           100,
           [](auto /* row */) { return 2; },
           [](auto row) { return row % 2; },
           [](auto row) { return row % 2 + 1; })});
  auto op = PlanBuilder().values({vector}).unnest({"c0"}, {"c1"}).planNode();
  // DuckDB doesn't support Unnest from MAP column. Hence,using 2 separate array
  // columns with the keys and values part of the MAP to validate.
  auto duckDbVector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeArrayVector<int32_t>(
           100,
           [](auto /* row */) { return 2; },
           [](auto /* row */, auto index) { return index; }),
       makeArrayVector<int32_t>(
           100,
           [](auto /* row */) { return 2; },
           [](auto /* row */, auto index) { return index + 1; })});
  createDuckDbTable({duckDbVector});
  auto params = makeCursorParameters(op);
  assertQuery(params, "SELECT c0, UNNEST(c1), UNNEST(c2) FROM tmp");
}

TEST_P(UnnestTest, mapWithOrdinality) {
  auto map = makeMapVector<int32_t, double>(
      {{{1, 1.1}, {2, std::nullopt}},
       {{3, 3.3}, {4, 4.4}, {5, 5.5}},
       {{6, std::nullopt}}});
  auto vector =
      makeRowVector({makeNullableFlatVector<int32_t>({1, 2, 3}), map});

  auto op = PlanBuilder()
                .values({vector})
                .unnest({"c0"}, {"c1"}, "ordinal")
                .planNode();

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 1, 2, 2, 2, 3}),
       makeNullableFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
       makeNullableFlatVector<double>(
           {1.1, std::nullopt, 3.3, 4.4, 5.5, std::nullopt}),
       makeNullableFlatVector<int64_t>({1, 2, 1, 2, 3, 1})});
  auto params = makeCursorParameters(op);
  assertQuery(params, expected);

  // Test with map wrapped in dictionary.
  auto reversedIndices = makeIndicesInReverse(3);
  auto vectorInDict = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 2, 3}),
       wrapInDictionary(reversedIndices, 3, map)});
  op = PlanBuilder()
           .values({vectorInDict})
           .unnest({"c0"}, {"c1"}, "ordinal")
           .planNode();

  auto expectedInDict = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 2, 2, 2, 3, 3}),
       makeNullableFlatVector<int32_t>({6, 3, 4, 5, 1, 2}),
       makeNullableFlatVector<double>(
           {std::nullopt, 3.3, 4.4, 5.5, 1.1, std::nullopt}),
       makeNullableFlatVector<int64_t>({1, 1, 2, 3, 1, 2})});
  params = makeCursorParameters(op);
  assertQuery(params, expectedInDict);
}

TEST_P(UnnestTest, multipleColumns) {
  std::vector<vector_size_t> offsets(100, 0);
  for (int i = 1; i < 100; ++i) {
    offsets[i] = offsets[i - 1] + i % 11 + 1;
  }

  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       vectorMaker_.mapVector<int64_t, double>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4); },
           [](auto row, auto index) { return index * (row % 4) + 1; },
           nullEvery(6)),
       makeArrayVector<int32_t>(
           100,
           [](auto row) { return row % 5 + 1; },
           [](auto row, auto index) { return index * (row % 3); },
           nullEvery(7)),
       makeArrayVector(offsets, makeConstant<int32_t>(7, 700))});

  auto op = PlanBuilder()
                .values({vector})
                .unnest({"c0"}, {"c1", "c2", "c3"})
                .planNode();

  // DuckDB doesn't support Unnest from MAP column. Hence,using 2 separate array
  // columns with the keys and values part of the MAP to validate.
  auto duckDbVector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeArrayVector<int64_t>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4); },
           nullEvery(6)),
       makeArrayVector<double>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4) + 1; },
           nullEvery(6)),
       makeArrayVector<int32_t>(
           100,
           [](auto row) { return row % 5 + 1; },
           [](auto row, auto index) { return index * (row % 3); },
           nullEvery(7)),
       makeArrayVector(offsets, makeConstant<int32_t>(7, 700))});
  createDuckDbTable({duckDbVector});
  auto params = makeCursorParameters(op);
  assertQuery(
      params,
      "SELECT c0, UNNEST(c1), UNNEST(c2), UNNEST(c3), UNNEST(c4) FROM tmp");
}

TEST_P(UnnestTest, multipleColumnsWithOrdinality) {
  std::vector<vector_size_t> offsets(100, 0);
  for (int i = 1; i < 100; ++i) {
    offsets[i] = offsets[i - 1] + i % 11 + 1;
  }

  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       vectorMaker_.mapVector<int64_t, double>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4); },
           [](auto row, auto index) { return index * (row % 4) + 1; },
           nullEvery(6)),
       makeArrayVector<int32_t>(
           100,
           [](auto row) { return row % 5 + 1; },
           [](auto row, auto index) { return index * (row % 3); },
           nullEvery(7)),
       makeArrayVector(offsets, makeConstant<int32_t>(7, 700))});

  auto op = PlanBuilder()
                .values({vector})
                .unnest({"c0"}, {"c1", "c2", "c3"}, "ordinal")
                .planNode();

  // DuckDB doesn't support Unnest from MAP column. Hence,using 2 separate array
  // columns with the keys and values part of the MAP to validate.
  auto ordinalitySize = [&](auto row) {
    if (row % 42 == 0) {
      return offsets[row + 1] - offsets[row];
    } else if (row % 7 == 0) {
      return std::max(row % 7 + 1, offsets[row + 1] - offsets[row]);
    } else if (row % 6 == 0) {
      return std::max(row % 5 + 1, offsets[row + 1] - offsets[row]);
    } else {
      return std::max(
          std::max(row % 5, row % 7) + 1,
          (row == 99 ? 700 : offsets[row + 1]) - offsets[row]);
    }
  };

  auto duckDbVector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeArrayVector<int64_t>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4); },
           nullEvery(6)),
       makeArrayVector<double>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4) + 1; },
           nullEvery(6)),
       makeArrayVector<int32_t>(
           100,
           [](auto row) { return row % 5 + 1; },
           [](auto row, auto index) { return index * (row % 3); },
           nullEvery(7)),
       makeArrayVector(offsets, makeConstant<int32_t>(7, 700)),
       makeArrayVector<int64_t>(
           100, ordinalitySize, [](auto /* row */, auto index) {
             return index + 1;
           })});
  createDuckDbTable({duckDbVector});
  auto params = makeCursorParameters(op);
  assertQuery(
      params,
      "SELECT c0, UNNEST(c1), UNNEST(c2), UNNEST(c3), UNNEST(c4), UNNEST(c5) FROM tmp");

  // Test with empty arrays and maps.
  vector = makeRowVector(
      {makeNullableFlatVector<double>({1.1, 2.2, std::nullopt, 4.4, 5.5}),
       vectorMaker_.arrayVectorNullable<int32_t>(
           {{{1, 2, std::nullopt, 4}}, std::nullopt, {{5, 6}}, {}, {{7}}}),
       makeMapVector<int32_t, double>(
           {{{1, 1.1}, {2, std::nullopt}},
            {{3, 3.3}, {4, 4.4}, {5, 5.5}},
            {{6, std::nullopt}},
            {},
            {}})});

  op = PlanBuilder()
           .values({vector})
           .unnest({"c0"}, {"c1", "c2"}, "ordinal")
           .planNode();

  auto expected = makeRowVector(
      {makeNullableFlatVector<double>(
           {1.1,
            1.1,
            1.1,
            1.1,
            2.2,
            2.2,
            2.2,
            std::nullopt,
            std::nullopt,
            5.5}),
       makeNullableFlatVector<int32_t>(
           {1,
            2,
            std::nullopt,
            4,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            5,
            6,
            7}),
       makeNullableFlatVector<int32_t>(
           {1,
            2,
            std::nullopt,
            std::nullopt,
            3,
            4,
            5,
            6,
            std::nullopt,
            std::nullopt}),
       makeNullableFlatVector<double>(
           {1.1,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            3.3,
            4.4,
            5.5,
            std::nullopt,
            std::nullopt,
            std::nullopt}),
       makeNullableFlatVector<int64_t>({1, 2, 3, 4, 1, 2, 3, 1, 2, 1})});
  params = makeCursorParameters(op);
  assertQuery(params, expected);
}

TEST_P(UnnestTest, allEmptyOrNullArrays) {
  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeArrayVector<int32_t>(
           100,
           [](auto /* row */) { return 0; },
           [](auto /* row */, auto index) { return index; },
           nullEvery(5)),
       makeArrayVector<int32_t>(
           100,
           [](auto /* row */) { return 0; },
           [](auto /* row */, auto index) { return index; },
           nullEvery(7))});

  auto op =
      PlanBuilder().values({vector}).unnest({"c0"}, {"c1", "c2"}).planNode();
  auto params = makeCursorParameters(op);
  assertQueryReturnsEmptyResult(params);

  op = PlanBuilder()
           .values({vector})
           .unnest({"c0"}, {"c1", "c2"}, "ordinal")
           .planNode();
  assertQueryReturnsEmptyResult(params);
}

TEST_P(UnnestTest, allEmptyOrNullMaps) {
  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeMapVector<int64_t, double>(
           100,
           [](auto /* row */) { return 0; },
           [](auto /* row */) { return 0; },
           [](auto /* row */) { return 0; },
           nullEvery(5)),
       makeMapVector<int64_t, double>(
           100,
           [](auto /* row */) { return 0; },
           [](auto /* row */) { return 0; },
           [](auto /* row */) { return 0; },
           nullEvery(7))});

  auto op =
      PlanBuilder().values({vector}).unnest({"c0"}, {"c1", "c2"}).planNode();
  auto params = makeCursorParameters(op);
  assertQueryReturnsEmptyResult(params);

  op = PlanBuilder()
           .values({vector})
           .unnest({"c0"}, {"c1", "c2"}, "ordinal")
           .planNode();
  params = makeCursorParameters(op);
  assertQueryReturnsEmptyResult(params);
}

TEST_P(UnnestTest, batchSize) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
  });

  // Unnest 1K rows into 3K rows.
  core::PlanNodeId unnestId;
  auto plan = PlanBuilder()
                  .values({data})
                  .project({"sequence(1, 3) as s"})
                  .unnest({}, {"s"})
                  .capturePlanNodeId(unnestId)
                  .planNode();

  auto expected = makeRowVector({
      makeFlatVector<int64_t>(1'000 * 3, [](auto row) { return 1 + row % 3; }),
  });

  auto task = AssertQueryBuilder(plan)
                  .config(
                      core::QueryConfig::kPreferredOutputBatchRows,
                      std::to_string(batchSize_))
                  .assertResults({expected});
  auto stats = exec::toPlanStats(task->taskStats());

  ASSERT_EQ(3'000, stats.at(unnestId).outputRows);
  int32_t expectedNumVectors = 3'000 / batchSize_;
  if (3'000 % batchSize_ != 0) {
    expectedNumVectors++;
  }
  ASSERT_EQ(expectedNumVectors, stats.at(unnestId).outputVectors);
}

TEST_P(UnnestTest, barrier) {
  std::vector<RowVectorPtr> vectors;
  std::vector<std::shared_ptr<TempFilePath>> tempFiles;
  const int numSplits{5};
  const int numRowsPerSplit{1'00};
  for (int32_t i = 0; i < 5; ++i) {
    auto vector = makeRowVector({
        makeFlatVector<int64_t>(numRowsPerSplit, [](auto row) { return row; }),
    });
    vectors.push_back(vector);
    tempFiles.push_back(TempFilePath::create());
  }
  writeToFiles(toFilePaths(tempFiles), vectors);
  createDuckDbTable(vectors);

  // Unnest 1K rows into 3K rows.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId unnestPlanNodeId;
  const auto plan = PlanBuilder(planNodeIdGenerator)
                        .startTableScan()
                        .outputType(std::dynamic_pointer_cast<const RowType>(
                            vectors[0]->type()))
                        .endTableScan()
                        .project({"sequence(1, 3) as s"})
                        .unnest({}, {"s"})
                        .capturePlanNodeId(unnestPlanNodeId)
                        .planNode();

  const auto expectedResult = makeRowVector({
      makeFlatVector<int64_t>(
          numRowsPerSplit * 3 * numSplits,
          [](auto row) { return 1 + row % 3; }),
  });

  struct {
    bool barrierExecution;
    int numOutputRows;

    std::string toString() const {
      return fmt::format(
          "barrierExecution {}, numOutputRows {}",
          barrierExecution,
          numOutputRows);
    }
  } testSettings[] = {{true, 23}, {false, 23}, {true, 200}, {false, 200}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.toString());
    const int numExpectedOutputVectors =
        bits::divRoundUp(numRowsPerSplit * 3, testData.numOutputRows) *
        numSplits;
    auto task = AssertQueryBuilder(plan)
                    .config(core::QueryConfig::kSparkPartitionId, "0")
                    .config(
                        core::QueryConfig::kMaxSplitPreloadPerDriver,
                        std::to_string(tempFiles.size()))
                    .splits(makeHiveConnectorSplits(tempFiles))
                    .serialExecution(true)
                    .barrierExecution(testData.barrierExecution)
                    .config(
                        core::QueryConfig::kPreferredOutputBatchRows,
                        std::to_string(testData.numOutputRows))
                    .assertResults(expectedResult);
    const auto taskStats = task->taskStats();
    ASSERT_EQ(taskStats.numBarriers, testData.barrierExecution ? numSplits : 0);
    ASSERT_EQ(taskStats.numFinishedSplits, numSplits);
    ASSERT_EQ(
        exec::toPlanStats(taskStats).at(unnestPlanNodeId).outputRows,
        numSplits * numRowsPerSplit * 3);
    // NOTE: unnest operator produce the same number of output batches no matter
    // it is under barrier execution mode or not.
    ASSERT_EQ(
        exec::toPlanStats(taskStats).at(unnestPlanNodeId).outputVectors,
        numExpectedOutputVectors);
  }
}

TEST_P(UnnestTest, spiltOutput) {
  std::vector<RowVectorPtr> vectors;
  const auto numBatches = 5;
  const auto inputBatchSize = 2048;
  for (int32_t i = 0; i < 5; ++i) {
    auto vector = makeRowVector({
        makeFlatVector<int64_t>(inputBatchSize, [](auto row) { return row; }),
    });
    vectors.push_back(vector);
  }
  createDuckDbTable(vectors);

  // Unnest 1K rows into 3K rows.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId unnestPlanNodeId;
  const auto plan = PlanBuilder(planNodeIdGenerator)
                        .values(vectors)
                        .project({"sequence(1, 3) as s"})
                        .unnest({}, {"s"})
                        .capturePlanNodeId(unnestPlanNodeId)
                        .planNode();

  const auto expectedResult = makeRowVector({
      makeFlatVector<int64_t>(
          numBatches * 3 * inputBatchSize,
          [](auto row) { return 1 + row % 3; }),
  });

  struct {
    bool produceSingleOutput;
    int expectedNumOutputExectors;

    std::string toString() const {
      return fmt::format(
          "produceSingleOutput {}, expectedNumOutputExectors {}",
          produceSingleOutput,
          expectedNumOutputExectors);
    }
  } testSettings[] = {
      {true, numBatches},
      {false, bits::divRoundUp(inputBatchSize * 3, GetParam()) * numBatches}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.toString());
    auto task = AssertQueryBuilder(plan)
                    .config(
                        core::QueryConfig::kPreferredOutputBatchRows,
                        std::to_string(GetParam()))
                    .config(
                        core::QueryConfig::kUnnestSplitOutput,
                        testData.produceSingleOutput ? "false" : "true")
                    .assertResults(expectedResult);
    const auto taskStats = task->taskStats();
    ASSERT_EQ(
        exec::toPlanStats(taskStats).at(unnestPlanNodeId).outputVectors,
        testData.expectedNumOutputExectors);
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    UnnestTest,
    UnnestTest,
    testing::ValuesIn(/*batchSize*/ {2, 17, 33, 1024}),
    [](const testing::TestParamInfo<int32_t>& info) {
      return fmt::format("outputBatchSize_{}", info.param);
    });
