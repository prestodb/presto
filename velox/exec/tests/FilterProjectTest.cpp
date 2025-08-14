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
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec {
namespace {

class FilterProjectTest : public test::HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
  }

  void assertFilter(
      const std::vector<RowVectorPtr>& vectors,
      const std::string& filter = "c1 % 10  > 0") {
    core::PlanNodePtr filterNode;
    auto plan = test::PlanBuilder()
                    .values(vectors)
                    .filter(filter)
                    .capturePlanNode(filterNode)
                    .planNode();
    ASSERT_TRUE(filterNode->supportsBarrier());

    assertQuery(plan, "SELECT * FROM tmp WHERE " + filter);
  }

  void assertProject(const std::vector<RowVectorPtr>& vectors) {
    core::PlanNodePtr projectNode;
    auto plan = test::PlanBuilder()
                    .values(vectors)
                    .project({"c0", "c1", "c0 + c1"})
                    .capturePlanNode(projectNode)
                    .planNode();
    ASSERT_TRUE(projectNode->supportsBarrier());

    auto task = assertQuery(plan, "SELECT c0, c1, c0 + c1 FROM tmp");

    // A quick sanity check for memory usage reporting. Check that peak total
    // memory usage for the project node is > 0.
    auto planStats = toPlanStats(task->taskStats());
    auto projectNodeId = plan->id();
    auto it = planStats.find(projectNodeId);
    ASSERT_TRUE(it != planStats.end());
    ASSERT_GT(it->second.peakMemoryBytes, 0);
  }

  RowVectorPtr makeTestVector(vector_size_t size = 100) const {
    auto rowType = ROW(
        {"c0", "c1", "c2", "c3"}, {BIGINT(), INTEGER(), SMALLINT(), DOUBLE()});

    return std::dynamic_pointer_cast<RowVector>(
        velox::test::BatchMaker::createBatch(rowType, size, *pool_));
  }

  std::vector<RowVectorPtr> makeTestVectors() const {
    std::vector<RowVectorPtr> vectors;
    vectors.reserve(10);
    for (int32_t i = 0; i < 10; ++i) {
      vectors.push_back(makeTestVector());
    }
    return vectors;
  }
};

TEST_F(FilterProjectTest, filter) {
  auto vectors = makeTestVectors();
  createDuckDbTable(vectors);
  assertFilter(vectors);
}

TEST_F(FilterProjectTest, filterOverDictionary) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = makeTestVector();

    auto indices = allocateIndices(2 * vector->size(), pool_.get());
    auto rawIndices = indices->asMutable<int32_t>();
    for (int32_t j = 0; j < vector->size(); j += 2) {
      rawIndices[j] = j / 2;
      rawIndices[j + 1] = j / 2;
    }

    auto newChildren = vector->children();
    newChildren[1] =
        wrapInDictionary(indices, vector->size(), vector->childAt(1));

    vectors.push_back(makeRowVector(newChildren));
  }
  createDuckDbTable(vectors);

  assertFilter(vectors);
}

TEST_F(FilterProjectTest, filterOverConstant) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = makeTestVector();

    std::vector<VectorPtr> newChildren = vector->children();
    newChildren[1] =
        BaseVector::wrapInConstant(vector->size(), 7, vector->childAt(1));

    vectors.push_back(makeRowVector(newChildren));
  }
  createDuckDbTable(vectors);

  assertFilter(vectors);
}

TEST_F(FilterProjectTest, project) {
  auto vectors = makeTestVectors();
  createDuckDbTable(vectors);
  assertProject(vectors);
}

TEST_F(FilterProjectTest, projectOverDictionary) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = makeTestVector();

    auto indices = allocateIndices(2 * vector->size(), pool_.get());
    auto rawIndices = indices->asMutable<int32_t>();
    for (int32_t j = 0; j < vector->size(); j += 2) {
      rawIndices[j] = j / 2;
      rawIndices[j + 1] = j / 2;
    }

    std::vector<VectorPtr> newChildren = vector->children();
    newChildren[1] =
        wrapInDictionary(indices, vector->size(), vector->childAt(1));
    vectors.push_back(makeRowVector(newChildren));
  }
  createDuckDbTable(vectors);

  assertProject(vectors);
}

TEST_F(FilterProjectTest, projectOverConstant) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    auto vector = makeTestVector();

    std::vector<VectorPtr> newChildren = vector->children();
    newChildren[1] =
        BaseVector::wrapInConstant(vector->size(), 7, vector->childAt(1));
    vectors.push_back(makeRowVector(newChildren));
  }
  createDuckDbTable(vectors);

  assertProject(vectors);
}

TEST_F(FilterProjectTest, projectOverLazy) {
  vector_size_t size = 1'000;
  auto valueAtC0 = [](auto row) -> int32_t {
    return row % 2 == 0 ? row : -row;
  };
  auto valueAtC1 = [](auto row) -> double {
    return row % 3 == 0 ? row * 0.1 : -row * 0.1;
  };
  auto lazyVectors = makeRowVector({
      vectorMaker_.lazyFlatVector<int32_t>(size, valueAtC0),
      vectorMaker_.lazyFlatVector<double>(size, valueAtC1),
  });

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, valueAtC0),
      makeFlatVector<double>(size, valueAtC1),
  });

  createDuckDbTable({vectors});

  auto plan = test::PlanBuilder()
                  .values({lazyVectors})
                  .project({"c0 > 0 AND c1 > 0.0", "c1 + 5.2"})
                  .planNode();
  assertQuery(plan, "SELECT c0 > 0 AND c1 > 0, c1 + 5.2 FROM tmp");
}

TEST_F(FilterProjectTest, filterProject) {
  auto vectors = makeTestVectors();
  createDuckDbTable(vectors);

  auto plan = test::PlanBuilder()
                  .values(vectors)
                  .filter("c1 % 10  > 0")
                  .project({"c0", "c1", "c0 + c1"})
                  .planNode();

  assertQuery(plan, "SELECT c0, c1, c0 + c1 FROM tmp WHERE c1 % 10 > 0");
}

TEST_F(FilterProjectTest, dereference) {
  auto vectors = makeTestVectors();
  createDuckDbTable(vectors);

  auto plan = test::PlanBuilder()
                  .values(vectors)
                  .project({"row_constructor(c1, c2) AS c1_c2"})
                  .project({"c1_c2.c1", "c1_c2.c2"})
                  .planNode();
  assertQuery(plan, "SELECT c1, c2 FROM tmp");

  plan = test::PlanBuilder()
             .values(vectors)
             .project({"row_constructor(c1, c2) AS c1_c2"})
             .filter("c1_c2.c1 % 10 = 5")
             .project({"c1_c2.c1", "c1_c2.c2"})
             .planNode();
  assertQuery(plan, "SELECT c1, c2 FROM tmp WHERE c1 % 10 = 5");
}

TEST_F(FilterProjectTest, allFailedOrPassed) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 10; ++i) {
    // We alternate between a batch where all pass and a batch where
    // no row passes. c0 is flat vector. c1 is constant vector.
    const int32_t value = i % 2 == 0 ? 0 : 1;

    vectors.push_back(makeRowVector({
        makeFlatVector<int32_t>(100, [&](auto row) { return value; }),
        makeConstant(value, 100),
    }));
  }
  createDuckDbTable(vectors);

  // filter over flat vector
  assertFilter(vectors, "c0 = 0");

  // filter over constant vector
  assertFilter(vectors, "c1 = 0");
}

// Tests fusing of consecutive filters and projects.
TEST_F(FilterProjectTest, filterProjectFused) {
  auto vectors = makeTestVectors();
  createDuckDbTable(vectors);

  auto plan = test::PlanBuilder()
                  .values(vectors)
                  .filter("c0 % 10 < 9")
                  .project({"c0", "c1", "c0 % 100 + c1 % 50 AS e1"})
                  .filter("c0 % 10 < 8")
                  .project({"c0", "c1", "e1", "c0 % 100 AS e2"})
                  .filter("c0 % 10 < 5")
                  .project({"c0", "c1", "e1", "e2"})
                  .planNode();

  assertQuery(
      plan,
      "SELECT c0, c1, c0 %100 + c1 % 50, c0 % 100 FROM tmp WHERE c0 % 10 < 5");
}

TEST_F(FilterProjectTest, projectAndIdentityOverLazy) {
  // Verify that a lazy column which is a part of both an identity projection
  // and a regular projection is loaded correctly. This is done by running a
  // projection that only operates over a subset of the rows of the lazy vector.
  vector_size_t size = 20;
  auto valueAt = [](auto row) -> int32_t { return row; };
  auto lazyVectors = makeRowVector({
      makeFlatVector<int32_t>(size, valueAt),
      vectorMaker_.lazyFlatVector<int32_t>(size, valueAt),
  });

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, valueAt),
      makeFlatVector<int32_t>(size, valueAt),
  });

  createDuckDbTable({vectors});

  auto plan = test::PlanBuilder()
                  .values({lazyVectors})
                  .project({"c0 < 10 AND c1 < 10", "c1"})
                  .planNode();
  assertQuery(plan, "SELECT c0 < 10 AND c1 < 10, c1 FROM tmp");
}

// Verify the optimization of avoiding copy in null propagation does not break
// the case when the field is shared between multiple parents.
TEST_F(FilterProjectTest, nestedFieldReferenceSharedChild) {
  auto shared = makeFlatVector<int64_t>(10, folly::identity);
  auto vector = makeRowVector({
      makeRowVector({
          makeRowVector({shared}, nullEvery(2)),
          makeRowVector({shared}, nullEvery(3)),
      }),
  });
  auto plan =
      test::PlanBuilder()
          .values({vector})
          .project({"coalesce((c0).c0.c0, 0) + coalesce((c0).c1.c0, 0)"})
          .planNode();
  auto expected = makeFlatVector<int64_t>(10);
  for (int i = 0; i < 10; ++i) {
    expected->set(i, (i % 2 == 0 ? 0 : i) + (i % 3 == 0 ? 0 : i));
  }
  test::AssertQueryBuilder(plan).assertResults(makeRowVector({expected}));
}

TEST_F(FilterProjectTest, numSilentThrow) {
  auto row = makeRowVector({makeFlatVector<int32_t>(100, folly::identity)});

  core::PlanNodeId filterId;
  // Change the plan when /0 error is fixed not to throw.
  auto plan = test::PlanBuilder()
                  .values({row})
                  .filter("try (c0 / 0) = 1")
                  .capturePlanNodeId(filterId)
                  .planNode();

  auto task = test::AssertQueryBuilder(plan).assertEmptyResults();
  auto planStats = toPlanStats(task->taskStats());
  ASSERT_EQ(100, planStats.at(filterId).customStats.at("numSilentThrow").sum);
}

TEST_F(FilterProjectTest, statsSplitter) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(100, folly::identity),
  });

  // Make 5 batches, 20 rows each: 0...19, 20...39, 40...59, 60...79, 80...99.
  // Filter out firs 25 rows (1 full batch plus some more), then keep every 3rd
  // row.
  core::PlanNodeId filterId;
  core::PlanNodeId projectId;
  auto plan = test::PlanBuilder()
                  .values(split(data, 5))
                  .filter("if (c0 < 25, false, c0 % 3 = 0)")
                  .capturePlanNodeId(filterId)
                  .project({"c0", "c0 + 1"})
                  .capturePlanNodeId(projectId)
                  .planNode();

  std::shared_ptr<Task> task;
  test::AssertQueryBuilder(plan).runWithoutResults(task);

  auto planStats = toPlanStats(task->taskStats());

  const auto& filterStats = planStats.at(filterId);
  const auto& projectStats = planStats.at(projectId);

  EXPECT_EQ(filterStats.inputRows, 100);
  EXPECT_EQ(filterStats.inputVectors, 5);

  EXPECT_EQ(filterStats.outputRows, 25);
  EXPECT_EQ(filterStats.outputVectors, 4);
  EXPECT_LT(filterStats.outputBytes * 2, filterStats.inputBytes);

  EXPECT_EQ(projectStats.inputRows, 25);
  EXPECT_EQ(projectStats.inputVectors, 4);

  EXPECT_EQ(projectStats.inputBytes, filterStats.outputBytes);
  EXPECT_LT(projectStats.inputBytes, projectStats.outputBytes);

  EXPECT_EQ(projectStats.outputRows, 25);
  EXPECT_EQ(projectStats.outputVectors, 4);
}

TEST_F(FilterProjectTest, barrier) {
  std::vector<RowVectorPtr> vectors;
  std::vector<std::shared_ptr<test::TempFilePath>> tempFiles;
  const int numSplits{5};
  for (int32_t i = 0; i < numSplits; ++i) {
    vectors.push_back(makeTestVector());
    tempFiles.push_back(test::TempFilePath::create());
  }
  writeToFiles(toFilePaths(tempFiles), vectors);
  createDuckDbTable(vectors);

  core::PlanNodeId projectPlanNodeId;
  auto plan = test::PlanBuilder()
                  .tableScan(vectors.front()->rowType())
                  .filter("c1 % 10  > 0")
                  .project({"c0", "c1", "c0 + c1"})
                  .capturePlanNodeId(projectPlanNodeId)
                  .planNode();
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
    auto task =
        test::AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(
                core::QueryConfig::kMaxSplitPreloadPerDriver,
                std::to_string(tempFiles.size()))
            .config(
                core::QueryConfig::kPreferredOutputBatchRows,
                std::to_string(testData.numOutputRows))
            .splits(makeHiveConnectorSplits(tempFiles))
            .serialExecution(true)
            .barrierExecution(testData.barrierExecution)
            .assertResults("SELECT c0, c1, c0 + c1 FROM tmp WHERE c1 % 10 > 0");
    const auto taskStats = task->taskStats();
    ASSERT_EQ(taskStats.numBarriers, testData.barrierExecution ? numSplits : 0);
    ASSERT_EQ(taskStats.numFinishedSplits, numSplits);
    // NOTE: the projector node doesn't respect output batch size as it does
    // one-to-one mapping and expects the upstream operator respects the output
    // batch size.
    ASSERT_EQ(
        exec::toPlanStats(taskStats).at(projectPlanNodeId).outputVectors,
        numSplits);
  }
}

TEST_F(FilterProjectTest, lazyDereference) {
  constexpr int kSize = 10;
  VectorPtr expected[] = {
      makeFlatVector<int64_t>(kSize, [](auto i) { return i; }),
      makeFlatVector<int64_t>(kSize, [](auto i) { return i + kSize; }),
      makeFlatVector<int64_t>(kSize, [](auto i) { return i + kSize * 2; }),
  };
  auto vector = makeRowVector({
      makeRowVector({expected[0], expected[1]}),
      makeRowVector({makeRowVector({expected[2]})}),
  });
  auto file = test::TempFilePath::create();
  writeToFile(file->getPath(), vector);
  CursorParameters params;
  params.copyResult = false;
  params.serialExecution = true;
  params.planNode = test::PlanBuilder()
                        .tableScan(vector->rowType())
                        .lazyDereference({"c0.c0", "c0.c1", "(c1).c0.c0"})
                        .planNode();
  auto cursor = TaskCursor::create(params);
  cursor->task()->addSplit(
      "0", exec::Split(makeHiveConnectorSplit(file->getPath())));
  cursor->task()->noMoreSplits("0");
  ASSERT_TRUE(cursor->moveNext());
  auto* result = cursor->current()->asChecked<RowVector>();
  ASSERT_EQ(result->size(), kSize);
  ASSERT_EQ(result->childrenSize(), 3);
  for (int i = 0; i < result->childrenSize(); ++i) {
    auto* lazy = result->childAt(i)->asChecked<LazyVector>();
    ASSERT_FALSE(lazy->isLoaded());
    ASSERT_EQ(lazy->size(), kSize);
    velox::test::assertEqualVectors(expected[i], lazy->loadedVectorShared());
  }
}

} // namespace
} // namespace facebook::velox::exec
