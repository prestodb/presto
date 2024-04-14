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

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class MergeJoinTest : public HiveConnectorTestBase {
 protected:
  using OperatorTestBase::assertQuery;

  CursorParameters makeCursorParameters(
      const std::shared_ptr<const core::PlanNode>& planNode,
      uint32_t preferredOutputBatchSize) {
    auto queryCtx = std::make_shared<core::QueryCtx>(executor_.get());

    CursorParameters params;
    params.planNode = planNode;
    params.queryCtx = queryCtx;
    params.queryCtx->testingOverrideConfigUnsafe(
        {{core::QueryConfig::kPreferredOutputBatchRows,
          std::to_string(preferredOutputBatchSize)}});
    return params;
  }

  template <typename T>
  void testJoin(
      std::function<T(vector_size_t /*row*/)> leftKeyAt,
      std::function<T(vector_size_t /*row*/)> rightKeyAt) {
    // Single batch on the left and right sides of the join.
    {
      auto leftKeys = makeFlatVector<T>(1'234, leftKeyAt);
      auto rightKeys = makeFlatVector<T>(1'234, rightKeyAt);

      testJoin({leftKeys}, {rightKeys});
    }

    // Multiple batches on one side. Single batch on the other side.
    {
      std::vector<VectorPtr> leftKeys = {
          makeFlatVector<T>(1024, leftKeyAt),
          makeFlatVector<T>(
              1024, [&](auto row) { return leftKeyAt(1024 + row); }),
      };
      std::vector<VectorPtr> rightKeys = {makeFlatVector<T>(2048, rightKeyAt)};

      testJoin(leftKeys, rightKeys);

      // Swap left and right side keys.
      testJoin(rightKeys, leftKeys);
    }

    // Multiple batches on each side.
    {
      std::vector<VectorPtr> leftKeys = {
          makeFlatVector<T>(512, leftKeyAt),
          makeFlatVector<T>(
              1024, [&](auto row) { return leftKeyAt(512 + row); }),
          makeFlatVector<T>(
              16, [&](auto row) { return leftKeyAt(512 + 1024 + row); }),
      };
      std::vector<VectorPtr> rightKeys = {
          makeFlatVector<T>(123, rightKeyAt),
          makeFlatVector<T>(
              1024, [&](auto row) { return rightKeyAt(123 + row); }),
          makeFlatVector<T>(
              1234, [&](auto row) { return rightKeyAt(123 + 1024 + row); }),
      };

      testJoin(leftKeys, rightKeys);

      // Swap left and right side keys.
      testJoin(rightKeys, leftKeys);
    }
  }

  void testJoin(
      const std::vector<VectorPtr>& leftKeys,
      const std::vector<VectorPtr>& rightKeys) {
    std::vector<RowVectorPtr> left;
    left.reserve(leftKeys.size());
    vector_size_t startRow = 0;
    for (const auto& key : leftKeys) {
      auto payload = makeFlatVector<int32_t>(
          key->size(), [startRow](auto row) { return (startRow + row) * 10; });
      left.push_back(makeRowVector({key, payload}));
      startRow += key->size();
    }

    std::vector<RowVectorPtr> right;
    right.reserve(rightKeys.size());
    startRow = 0;
    for (const auto& key : rightKeys) {
      auto payload = makeFlatVector<int32_t>(
          key->size(), [startRow](auto row) { return (startRow + row) * 20; });
      right.push_back(makeRowVector({key, payload}));
      startRow += key->size();
    }

    createDuckDbTable("t", left);
    createDuckDbTable("u", right);

    // Test INNER join.
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(left)
                    .mergeJoin(
                        {"c0"},
                        {"u_c0"},
                        PlanBuilder(planNodeIdGenerator)
                            .values(right)
                            .project({"c1 AS u_c1", "c0 AS u_c0"})
                            .planNode(),
                        "",
                        {"c0", "c1", "u_c1"},
                        core::JoinType::kInner)
                    .planNode();

    // Use very small output batch size.
    assertQuery(
        makeCursorParameters(plan, 16),
        "SELECT t.c0, t.c1, u.c1 FROM t, u WHERE t.c0 = u.c0");

    // Use regular output batch size.
    assertQuery(
        makeCursorParameters(plan, 1024),
        "SELECT t.c0, t.c1, u.c1 FROM t, u WHERE t.c0 = u.c0");

    // Use very large output batch size.
    assertQuery(
        makeCursorParameters(plan, 10'000),
        "SELECT t.c0, t.c1, u.c1 FROM t, u WHERE t.c0 = u.c0");

    // Test LEFT join.
    planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    plan = PlanBuilder(planNodeIdGenerator)
               .values(left)
               .mergeJoin(
                   {"c0"},
                   {"u_c0"},
                   PlanBuilder(planNodeIdGenerator)
                       .values(right)
                       .project({"c1 as u_c1", "c0 as u_c0"})
                       .planNode(),
                   "",
                   {"c0", "c1", "u_c1"},
                   core::JoinType::kLeft)
               .planNode();

    // Use very small output batch size.
    assertQuery(
        makeCursorParameters(plan, 16),
        "SELECT t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0");

    // Use regular output batch size.
    assertQuery(
        makeCursorParameters(plan, 1024),
        "SELECT t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0");

    // Use very large output batch size.
    assertQuery(
        makeCursorParameters(plan, 10'000),
        "SELECT t.c0, t.c1, u.c1 FROM t LEFT JOIN u ON t.c0 = u.c0");
  }
};

TEST_F(MergeJoinTest, oneToOneAllMatch) {
  testJoin<int32_t>([](auto row) { return row; }, [](auto row) { return row; });
}

TEST_F(MergeJoinTest, someDontMatch) {
  testJoin<int32_t>(
      [](auto row) { return row % 5 == 0 ? row - 1 : row; },
      [](auto row) { return row % 7 == 0 ? row - 1 : row; });
}

TEST_F(MergeJoinTest, fewMatch) {
  testJoin<int32_t>(
      [](auto row) { return row * 5; }, [](auto row) { return row * 7; });
}

TEST_F(MergeJoinTest, duplicateMatch) {
  testJoin<int32_t>(
      [](auto row) { return row / 2; }, [](auto row) { return row / 3; });
}

TEST_F(MergeJoinTest, allRowsMatch) {
  std::vector<VectorPtr> leftKeys = {
      makeFlatVector<int32_t>(2, [](auto /* row */) { return 5; }),
      makeFlatVector<int32_t>(3, [](auto /* row */) { return 5; }),
      makeFlatVector<int32_t>(4, [](auto /* row */) { return 5; }),
  };
  std::vector<VectorPtr> rightKeys = {
      makeFlatVector<int32_t>(7, [](auto /* row */) { return 5; })};

  testJoin(leftKeys, rightKeys);

  testJoin(rightKeys, leftKeys);
}

TEST_F(MergeJoinTest, aggregationOverJoin) {
  auto left =
      makeRowVector({"t_c0"}, {makeFlatVector<int32_t>({1, 2, 3, 4, 5})});
  auto right = makeRowVector({"u_c0"}, {makeFlatVector<int32_t>({2, 4, 6})});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values({left})
          .mergeJoin(
              {"t_c0"},
              {"u_c0"},
              PlanBuilder(planNodeIdGenerator).values({right}).planNode(),
              "",
              {"t_c0", "u_c0"},
              core::JoinType::kInner)
          .singleAggregation({}, {"count(1)"})
          .planNode();

  auto result = readSingleValue(plan);
  ASSERT_FALSE(result.isNull());
  ASSERT_EQ(2, result.value<int64_t>());
}

TEST_F(MergeJoinTest, nonFirstJoinKeys) {
  auto left = makeRowVector(
      {"t_data", "t_key"},
      {
          makeFlatVector<int32_t>({50, 40, 30, 20, 10}),
          makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
      });
  auto right = makeRowVector(
      {"u_data", "u_key"},
      {
          makeFlatVector<int32_t>({23, 22, 21}),
          makeFlatVector<int32_t>({2, 4, 6}),
      });

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values({left})
          .mergeJoin(
              {"t_key"},
              {"u_key"},
              PlanBuilder(planNodeIdGenerator).values({right}).planNode(),
              "",
              {"t_key", "t_data", "u_data"},
              core::JoinType::kInner)
          .planNode();

  assertQuery(plan, "VALUES (2, 40, 23), (4, 20, 22)");
}

TEST_F(MergeJoinTest, innerJoinFilter) {
  vector_size_t size = 1'000;
  // Join keys on the left side: 0, 10, 20,..
  // Payload on the left side: 0, 1, 2, 3,..
  auto left = makeRowVector(
      {"t_c0", "t_c1"},
      {
          makeFlatVector<int32_t>(size, [](auto row) { return row * 10; }),
          makeFlatVector<int64_t>(
              size, [](auto row) { return row; }, nullEvery(13)),
      });

  // Join keys on the right side: 0, 5, 10, 15, 20,..
  // Payload on the right side: 0, 1, 2, 3, 4, 5, 6, 0, 1, 2,..
  auto right = makeRowVector(
      {"u_c0", "u_c1"},
      {
          makeFlatVector<int32_t>(size, [](auto row) { return row * 5; }),
          makeFlatVector<int64_t>(
              size, [](auto row) { return row % 7; }, nullEvery(17)),
      });

  createDuckDbTable("t", {left});
  createDuckDbTable("u", {right});

  auto plan = [&](const std::string& filter) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    return PlanBuilder(planNodeIdGenerator)
        .values({left})
        .mergeJoin(
            {"t_c0"},
            {"u_c0"},
            PlanBuilder(planNodeIdGenerator).values({right}).planNode(),
            filter,
            {"t_c0", "u_c0", "u_c1"},
            core::JoinType::kInner)
        .planNode();
  };

  assertQuery(
      plan("(t_c1 + u_c1) % 2 = 0"),
      "SELECT t_c0, u_c0, u_c1 FROM t, u WHERE t_c0 = u_c0 AND (t_c1 + u_c1) % 2 = 0");

  assertQuery(
      plan("(t_c1 + u_c1) % 2 = 1"),
      "SELECT t_c0, u_c0, u_c1 FROM t, u WHERE t_c0 = u_c0 AND (t_c1 + u_c1) % 2 = 1");

  // No rows pass filter.
  assertQuery(
      plan("(t_c1 + u_c1) % 2 < 0"),
      "SELECT t_c0, u_c0, u_c1 FROM t, u WHERE t_c0 = u_c0 AND (t_c1 + u_c1) % 2 < 0");

  // All rows pass filter.
  assertQuery(
      plan("(t_c1 + u_c1) % 2 >= 0"),
      "SELECT t_c0, u_c0, u_c1 FROM t, u WHERE t_c0 = u_c0 AND (t_c1 + u_c1) % 2 >= 0");

  // Filter expressions over join keys.
  assertQuery(
      plan("(t_c0 + u_c1) % 2 = 0"),
      "SELECT t_c0, u_c0, u_c1 FROM t, u WHERE t_c0 = u_c0 AND (t_c0 + u_c1) % 2 = 0");

  assertQuery(
      plan("(t_c1 + u_c0) % 2 = 0"),
      "SELECT t_c0, u_c0, u_c1 FROM t, u WHERE t_c0 = u_c0 AND (t_c1 + u_c0) % 2 = 0");

  // Very small output batch size.
  assertQuery(
      makeCursorParameters(plan("(t_c1 + u_c1) % 2 = 0"), 16),
      "SELECT t_c0, u_c0, u_c1 FROM t, u WHERE t_c0 = u_c0 AND (t_c1 + u_c1) % 2 = 0");
}

TEST_F(MergeJoinTest, leftJoinFilter) {
  // Each row on the left side has at most one match on the right side.
  auto left = makeRowVector(
      {"t_c0", "t_c1"},
      {
          makeFlatVector<int32_t>({0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50}),
          makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
      });

  auto right = makeRowVector(
      {"u_c0", "u_c1"},
      {
          makeFlatVector<int32_t>({0, 10, 20, 30, 40, 50}),
          makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5}),
      });

  createDuckDbTable("t", {left});
  createDuckDbTable("u", {right});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = [&](const std::string& filter) {
    return PlanBuilder(planNodeIdGenerator)
        .values({left})
        .mergeJoin(
            {"t_c0"},
            {"u_c0"},
            PlanBuilder(planNodeIdGenerator).values({right}).planNode(),
            filter,
            {"t_c0", "t_c1", "u_c1"},
            core::JoinType::kLeft)
        .planNode();
  };

  // Test with different output batch sizes.
  for (auto batchSize : {1, 3, 16}) {
    assertQuery(
        makeCursorParameters(plan("(t_c1 + u_c1) % 2 = 0"), batchSize),
        "SELECT t_c0, t_c1, u_c1 FROM t LEFT JOIN u ON t_c0 = u_c0 AND (t_c1 + u_c1) % 2 = 0");
  }

  // A left-side row with multiple matches on the right side.
  left = makeRowVector(
      {"t_c0", "t_c1"},
      {
          makeFlatVector<int32_t>({5, 10}),
          makeFlatVector<int32_t>({0, 0}),
      });

  right = makeRowVector(
      {"u_c0", "u_c1"},
      {
          makeFlatVector<int32_t>({10, 10, 10, 10, 10, 10}),
          makeFlatVector<int32_t>({0, 1, 2, 3, 4, 5}),
      });

  createDuckDbTable("t", {left});
  createDuckDbTable("u", {right});

  // Test with different filters and output batch sizes.
  for (auto batchSize : {1, 3, 16}) {
    for (auto filter :
         {"t_c1 + u_c1 > 3",
          "t_c1 + u_c1 < 3",
          "t_c1 + u_c1 > 100",
          "t_c1 + u_c1 < 100"}) {
      assertQuery(
          makeCursorParameters(plan(filter), batchSize),
          fmt::format(
              "SELECT t_c0, t_c1, u_c1 FROM t LEFT JOIN u ON t_c0 = u_c0 AND {}",
              filter));
    }
  }
}

// Verify that both left-side and right-side pipelines feeding the merge join
// always run single-threaded.
TEST_F(MergeJoinTest, numDrivers) {
  auto left = makeRowVector({"t_c0"}, {makeFlatVector<int32_t>({1, 2, 3})});
  auto right = makeRowVector({"u_c0"}, {makeFlatVector<int32_t>({0, 2, 5})});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values({left}, true)
          .mergeJoin(
              {"t_c0"},
              {"u_c0"},
              PlanBuilder(planNodeIdGenerator).values({right}, true).planNode(),
              "",
              {"t_c0", "u_c0"},
              core::JoinType::kInner)
          .planNode();

  auto task = AssertQueryBuilder(plan, duckDbQueryRunner_)
                  .maxDrivers(5)
                  .assertResults("SELECT 2, 2");

  // We have two pipelines in the task and each must have 1 driver.
  EXPECT_EQ(2, task->numTotalDrivers());
  EXPECT_EQ(2, task->numFinishedDrivers());
}

TEST_F(MergeJoinTest, lazyVectors) {
  // a dataset of multiple row groups with multiple columns. We create
  // different dictionary wrappings for different columns and load the
  // rows in scope at different times.  We make 11000 repeats of 300
  // followed by ascending rows. These will hits one 300 from the
  // right side and cover more than one batch, so that we test lazy
  // loading where we buffer multiple batches of input.
  auto leftVectors = makeRowVector(
      {makeFlatVector<int32_t>(
           30'000, [](auto row) { return row < 11000 ? 300 : row; }),
       makeFlatVector<int64_t>(30'000, [](auto row) { return row % 23; }),
       makeFlatVector<int32_t>(30'000, [](auto row) { return row % 31; }),
       makeFlatVector<StringView>(30'000, [](auto row) {
         return StringView::makeInline(fmt::format("{}   string", row % 43));
       })});

  auto rightVectors = makeRowVector(
      {"rc0", "rc1"},
      {makeFlatVector<int32_t>(10'000, [](auto row) { return row * 3; }),
       makeFlatVector<int64_t>(10'000, [](auto row) { return row % 31; })});

  auto leftFile = TempFilePath::create();
  writeToFile(leftFile->getPath(), leftVectors);
  createDuckDbTable("t", {leftVectors});

  auto rightFile = TempFilePath::create();
  writeToFile(rightFile->getPath(), rightVectors);
  createDuckDbTable("u", {rightVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId leftScanId;
  core::PlanNodeId rightScanId;
  auto op = PlanBuilder(planNodeIdGenerator)
                .tableScan(
                    ROW({"c0", "c1", "c2", "c3"},
                        {INTEGER(), BIGINT(), INTEGER(), VARCHAR()}))
                .capturePlanNodeId(leftScanId)
                .mergeJoin(
                    {"c0"},
                    {"rc0"},
                    PlanBuilder(planNodeIdGenerator)
                        .tableScan(ROW({"rc0", "rc1"}, {INTEGER(), BIGINT()}))
                        .capturePlanNodeId(rightScanId)
                        .planNode(),
                    "c1 + rc1 < 30",
                    {"c0", "rc0", "c1", "rc1", "c2", "c3"})
                .planNode();

  AssertQueryBuilder(op, duckDbQueryRunner_)
      .split(rightScanId, makeHiveConnectorSplit(rightFile->getPath()))
      .split(leftScanId, makeHiveConnectorSplit(leftFile->getPath()))
      .assertResults(
          "SELECT c0, rc0, c1, rc1, c2, c3  FROM t, u WHERE t.c0 = u.rc0 and c1 + rc1 < 30");
}

TEST_F(MergeJoinTest, nullKeys) {
  auto left = makeRowVector(
      {"t0"}, {makeNullableFlatVector<int64_t>({1, 2, 5, std::nullopt})});

  auto right = makeRowVector(
      {"u0"},
      {makeNullableFlatVector<int64_t>({1, 5, std::nullopt, std::nullopt})});

  createDuckDbTable("t", {left});
  createDuckDbTable("u", {right});

  // Inner join.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values({left})
          .mergeJoin(
              {"t0"},
              {"u0"},
              PlanBuilder(planNodeIdGenerator).values({right}).planNode(),
              "",
              {"t0", "u0"},
              core::JoinType::kInner)
          .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults("SELECT * FROM t, u WHERE t.t0 = u.u0");

  // Left join.
  plan = PlanBuilder(planNodeIdGenerator)
             .values({left})
             .mergeJoin(
                 {"t0"},
                 {"u0"},
                 PlanBuilder(planNodeIdGenerator).values({right}).planNode(),
                 "",
                 {"t0", "u0"},
                 core::JoinType::kLeft)
             .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults("SELECT * FROM t LEFT JOIN u ON t.t0 = u.u0");
}

TEST_F(MergeJoinTest, complexTypedFilter) {
  constexpr vector_size_t size{1000};

  auto right = makeRowVector(
      {"u_c0"},
      {makeFlatVector<int32_t>(size, [](auto row) { return row * 2; })});

  auto testComplexTypedFilter =
      [&](const std::vector<RowVectorPtr>& left,
          const std::string& filter,
          const std::string& queryFilter,
          const std::vector<std::string>& outputLayout) {
        createDuckDbTable("t", left);
        createDuckDbTable("u", {right});
        auto planNodeIdGenerator =
            std::make_shared<core::PlanNodeIdGenerator>();
        auto plan =
            PlanBuilder(planNodeIdGenerator)
                .values(left)
                .mergeJoin(
                    {"t_c0"},
                    {"u_c0"},
                    PlanBuilder(planNodeIdGenerator).values({right}).planNode(),
                    filter,
                    outputLayout,
                    core::JoinType::kLeft)
                .planNode();

        std::string outputs;
        for (auto i = 0; i < outputLayout.size(); ++i) {
          outputs += std::move(outputLayout[i]);
          if (i + 1 < outputLayout.size()) {
            outputs += ", ";
          }
        }

        assertQuery(
            plan,
            fmt::format(
                "SELECT {} FROM t LEFT JOIN u ON t_c0 = u_c0 AND {}",
                outputs,
                queryFilter));
      };

  std::vector<std::vector<std::string>> outputLayouts{
      {"t_c0", "u_c0"}, {"t_c0", "u_c0", "t_c1"}};

  {
    const std::vector<std::vector<int32_t>> pattern{
        {1},
        {1, 2},
        {1, 2, 4},
        {1, 2, 4, 8},
        {1, 2, 4, 8, 16},
    };
    std::vector<std::vector<int32_t>> arrayVector;
    arrayVector.reserve(size);
    for (auto i = 0; i < size / pattern.size(); ++i) {
      arrayVector.insert(arrayVector.end(), pattern.begin(), pattern.end());
    }
    auto left = {
        makeRowVector(
            {"t_c0", "t_c1"},
            {makeFlatVector<int32_t>(size, [](auto row) { return row; }),
             makeArrayVector<int32_t>(arrayVector)}),
        makeRowVector(
            {"t_c0", "t_c1"},
            {makeFlatVector<int32_t>(
                 size, [size](auto row) { return size + row * 2; }),
             makeArrayVector<int32_t>(arrayVector)})};

    for (const auto& outputLayout : outputLayouts) {
      testComplexTypedFilter(
          left, "array_max(t_c1) >= 8", "list_max(t_c1) >= 8", outputLayout);
    }
  }

  {
    auto sizeAt = [](vector_size_t row) { return row % 5; };
    auto keyAt = [](vector_size_t row) { return row % 11; };
    auto valueAt = [](vector_size_t row) { return row % 13; };
    auto keys = makeArrayVector<int64_t>(size, sizeAt, keyAt);
    auto values = makeArrayVector<int32_t>(size, sizeAt, valueAt);

    auto mapVector =
        makeMapVector<int64_t, int32_t>(size, sizeAt, keyAt, valueAt);

    auto left = {
        makeRowVector(
            {"t_c0", "t_c1"},
            {makeFlatVector<int32_t>(size, [](auto row) { return row; }),
             mapVector}),
        makeRowVector(
            {"t_c0", "t_c1"},
            {makeFlatVector<int32_t>(
                 size, [size](auto row) { return size + row * 2; }),
             mapVector})};

    for (const auto& outputLayout : outputLayouts) {
      testComplexTypedFilter(
          left, "cardinality(t_c1) > 4", "cardinality(t_c1) > 4", outputLayout);
    }
  }
};
