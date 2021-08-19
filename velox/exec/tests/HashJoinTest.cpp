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

#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/tests/Cursor.h"
#include "velox/exec/tests/HiveConnectorTestBase.h"
#include "velox/exec/tests/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

using facebook::velox::test::BatchMaker;

static const std::string kWriter = "HashJoinTest.Writer";

class HashJoinTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
  }

  void testJoin(
      const std::vector<TypePtr>& keyTypes,
      int32_t numThreads,
      int32_t leftSize,
      int32_t rightSize,
      const std::string& referenceQuery,
      const std::string& filter = "") {
    auto leftType = makeRowType(keyTypes, "t_");
    auto rightType = makeRowType(keyTypes, "u_");

    auto leftBatch = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(leftType, leftSize, *pool_));
    auto rightBatch = std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rightType, rightSize, *pool_));

    CursorParameters params;
    params.planNode =
        PlanBuilder()
            .values({leftBatch}, true)
            .hashJoin(
                allChannels(keyTypes.size()),
                allChannels(keyTypes.size()),
                PlanBuilder().values({rightBatch}, true).planNode(),
                filter,
                allChannels(2 * (1 + keyTypes.size())))
            .planNode();
    params.numThreads = numThreads;

    createDuckDbTable("t", {leftBatch});
    createDuckDbTable("u", {rightBatch});
    ::assertQuery(
        params, [](auto*) {}, referenceQuery, duckDbQueryRunner_);
  }

  static RowTypePtr makeRowType(
      const std::vector<TypePtr>& keyTypes,
      const std::string& namePrefix) {
    std::vector<std::string> names;
    for (int i = 0; i < keyTypes.size(); ++i) {
      names.push_back(fmt::format("{}k{}", namePrefix, i));
    }
    names.push_back(fmt::format("{}data", namePrefix));

    std::vector<TypePtr> types = keyTypes;
    types.push_back(VARCHAR());

    return ROW(std::move(names), std::move(types));
  }

  static std::vector<ChannelIndex> allChannels(int32_t numChannels) {
    std::vector<ChannelIndex> channels(numChannels);
    std::iota(channels.begin(), channels.end(), 0);
    return channels;
  }
};

TEST_F(HashJoinTest, bigintArray) {
  testJoin(
      {BIGINT()},
      1,
      16000,
      15000,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0");
}

TEST_F(HashJoinTest, bigintArrayParallel) {
  testJoin(
      {BIGINT()},
      2,
      16000,
      15000,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 "
      "UNION ALL SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 "
      "UNION ALL SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 "
      "UNION ALL SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0");
}

TEST_F(HashJoinTest, emptyBuild) {
  testJoin(
      {BIGINT()},
      1,
      16000,
      0,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0");
}

TEST_F(HashJoinTest, normalizedKey) {
  testJoin(
      {BIGINT(), VARCHAR()},
      1,
      16000,
      15000,
      "SELECT t_k0, t_k1, t_data, u_k0, u_k1, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND t_k1 = u_k1");
}

TEST_F(HashJoinTest, normalizedKeyOverflow) {
  testJoin(
      {BIGINT(), VARCHAR(), BIGINT(), BIGINT(), BIGINT(), BIGINT()},
      1,
      16000,
      15000,
      "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5  ");
}

TEST_F(HashJoinTest, allTypes) {
  testJoin(
      {BIGINT(), VARCHAR(), REAL(), DOUBLE(), INTEGER(), SMALLINT(), TINYINT()},
      1,
      16000,
      15000,
      "SELECT t_k0, t_k1, t_k2, t_k3, t_k4, t_k5, t_k6, t_data, u_k0, u_k1, u_k2, u_k3, u_k4, u_k5, u_k6, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND t_k1 = u_k1 AND t_k2 = u_k2 AND t_k3 = u_k3 AND t_k4 = u_k4 AND t_k5 = u_k5 AND t_k6 = u_k6 ");
}

TEST_F(HashJoinTest, filter) {
  testJoin(
      {BIGINT()},
      1,
      16000,
      15000,
      "SELECT t_k0, t_data, u_k0, u_data FROM "
      "  t, u "
      "  WHERE t_k0 = u_k0 AND ((t_k0 % 100) + (u_k0 % 100)) % 40 < 20",
      "((t_k0 % 100) + (u_k0 % 100)) % 40 < 20");
}

TEST_F(HashJoinTest, memory) {
  // Measures memory allocation in a 1:n hash join followed by
  // projection and aggregation. We expect vectors to be mostly
  // reused, except for t_k0 + 1, which is a dictionary after the
  // join.
  std::vector<TypePtr> keyTypes = {BIGINT()};
  auto leftType = makeRowType(keyTypes, "t_");
  auto rightType = makeRowType(keyTypes, "u_");

  std::vector<RowVectorPtr> leftBatches;
  std::vector<RowVectorPtr> rightBatches;
  for (auto i = 0; i < 100; ++i) {
    leftBatches.push_back(std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(leftType, 1000, *pool_)));
  }
  for (auto i = 0; i < 10; ++i) {
    rightBatches.push_back(std::dynamic_pointer_cast<RowVector>(
        BatchMaker::createBatch(rightType, 800, *pool_)));
  }
  CursorParameters params;
  params.planNode = PlanBuilder()
                        .values(leftBatches, true)
                        .hashJoin(
                            allChannels(keyTypes.size()),
                            allChannels(keyTypes.size()),
                            PlanBuilder().values(rightBatches, true).planNode(),
                            "",
                            allChannels(2 * (1 + keyTypes.size())))
                        .project({"t_k0 % 1000", "u_k0 % 1000"}, {"k1", "k2"})
                        .finalAggregation({}, {"sum(k1)", "sum(k2)"})
                        .planNode();
  params.queryCtx = core::QueryCtx::create();
  auto tracker = memory::MemoryUsageTracker::create();
  params.queryCtx->pool()->setMemoryUsageTracker(tracker);
  auto [taskCursor, rows] = readCursor(params, [](Task*) {});
  EXPECT_GT(2500, tracker->getNumAllocs());
  EXPECT_GT(7'500'000, tracker->getCumulativeBytes());
}

TEST_F(HashJoinTest, lazyVectors) {
  auto leftVectors = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row % 23; }),
      makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
  });

  auto rightVectors = makeRowVector(
      {makeFlatVector<int32_t>(1'000, [](auto row) { return row % 31; })});

  auto leftFile = TempFilePath::create();
  writeToFile(leftFile->path, kWriter, leftVectors);
  createDuckDbTable("t", {leftVectors});

  auto rightFile = TempFilePath::create();
  writeToFile(rightFile->path, kWriter, rightVectors);
  createDuckDbTable("u", {rightVectors});

  auto op =
      PlanBuilder(10)
          .tableScan(ROW({"c0", "c1"}, {INTEGER(), BIGINT()}))
          .hashJoin(
              {0},
              {0},
              PlanBuilder(0).tableScan(ROW({"c0"}, {INTEGER()})).planNode(),
              "",
              {1})
          .project({"c1 + 1"})
          .planNode();

  assertQuery(
      op,
      {{0, rightFile}, {10, leftFile}},
      "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0");
}

/// Test hash join where build-side keys come from a small range and allow for
/// array-based lookup instead of a hash table.
TEST_F(HashJoinTest, arrayBasedLookup) {
  auto oddIndices = makeIndices(500, [](auto i) { return 2 * i + 1; });

  auto leftVectors = {
      // Join key vector is flat.
      makeRowVector({
          makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
          makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
      }),
      // Join key vector is constant. There is a match in the build side.
      makeRowVector({
          BaseVector::createConstant(4, 2'000, pool_.get()),
          makeFlatVector<int64_t>(2'000, [](auto row) { return row; }),
      }),
      // Join key vector is constant. There is no match.
      makeRowVector({
          BaseVector::createConstant(5, 2'000, pool_.get()),
          makeFlatVector<int64_t>(2'000, [](auto row) { return row; }),
      }),
      // Join key vector is a dictionary.
      makeRowVector({
          wrapInDictionary(
              oddIndices,
              500,
              makeFlatVector<int32_t>(1'000, [](auto row) { return row * 4; })),
          makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
      })};

  // 100 key values in [0, 198] range.
  auto rightVectors = {makeRowVector(
      {makeFlatVector<int32_t>(100, [](auto row) { return row * 2; })})};

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto op =
      PlanBuilder(10)
          .values(leftVectors)
          .hashJoin(
              {0}, {0}, PlanBuilder(0).values(rightVectors).planNode(), "", {1})
          .project({"c1 + 1"})
          .planNode();

  assertQuery(op, "SELECT t.c1 + 1 FROM t, u WHERE t.c0 = u.c0");
}
