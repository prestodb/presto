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
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class CrossJoinTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
  }

  template <typename T>
  VectorPtr sequence(vector_size_t size, T start = 0) {
    return makeFlatVector<int32_t>(
        size, [start](auto row) { return start + row; });
  }

  template <typename T>
  VectorPtr lazySequence(vector_size_t size, T start = 0) {
    return vectorMaker_.lazyFlatVector<int32_t>(
        size, [start](auto row) { return start + row; });
  }
};

TEST_F(CrossJoinTest, basic) {
  auto leftVectors = {
      makeRowVector({sequence<int32_t>(10)}),
      makeRowVector({sequence<int32_t>(100, 10)}),
      makeRowVector({sequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({sequence<int32_t>(7, 10 + 100 + 1'000)}),
  };

  auto rightVectors = {
      makeRowVector({sequence<int32_t>(10)}),
      makeRowVector({sequence<int32_t>(100, 10)}),
      makeRowVector({sequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({sequence<int32_t>(11, 10 + 100 + 1'000)}),
  };

  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  // All x 13. Join output vectors contains multiple probe rows each.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .crossJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .filter("c0 < 13")
                        .project({"c0 AS u_c0"})
                        .planNode(),
                    {"c0", "u_c0"})
                .planNode();

  assertQuery(op, "SELECT * FROM t, u WHERE u.c0 < 13");

  // 13 x all. Join output vectors contains single probe row each.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .filter("c0 < 13")
           .crossJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .project({"c0 AS u_c0"})
                   .planNode(),
               {"c0", "u_c0"})
           .planNode();

  assertQuery(op, "SELECT * FROM t, u WHERE t.c0 < 13");

  // All x 13. No columns on the build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .crossJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({vectorMaker_.rowVector(ROW({}, {}), 13)})
                   .planNode(),
               {"c0"})
           .planNode();

  assertQuery(op, "SELECT t.* FROM t, (SELECT * FROM u LIMIT 13) u");

  // 13 x All. No columns on the build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .filter("c0 < 13")
           .crossJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({vectorMaker_.rowVector(ROW({}, {}), 1121)})
                   .planNode(),
               {"c0"})
           .planNode();

  assertQuery(
      op,
      "SELECT t.* FROM (SELECT * FROM t WHERE c0 < 13) t, (SELECT * FROM u LIMIT 1121) u");

  // Empty build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .crossJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .filter("c0 < 0")
                   .project({"c0 AS u_c0"})
                   .planNode(),
               {"c0", "u_c0"})
           .planNode();

  assertQueryReturnsEmptyResult(op);

  // Multi-threaded build side.
  planNodeIdGenerator->reset();
  CursorParameters params;
  params.maxDrivers = 4;
  params.planNode = PlanBuilder(planNodeIdGenerator)
                        .values({leftVectors})
                        .crossJoin(
                            PlanBuilder(planNodeIdGenerator, pool_.get())
                                .values({rightVectors}, true)
                                .filter("c0 in (10, 17)")
                                .project({"c0 AS u_c0"})
                                .planNode(),
                            {"c0", "u_c0"})
                        .limit(0, 100'000, false)
                        .planNode();

  OperatorTestBase::assertQuery(
      params,
      "SELECT * FROM t, (SELECT * FROM UNNEST (ARRAY[10, 17, 10, 17, 10, 17, 10, 17])) u");
}

TEST_F(CrossJoinTest, lazyVectors) {
  auto leftVectors = {
      makeRowVector({lazySequence<int32_t>(10)}),
      makeRowVector({lazySequence<int32_t>(100, 10)}),
      makeRowVector({lazySequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({lazySequence<int32_t>(7, 10 + 100 + 1'000)}),
  };

  auto rightVectors = {
      makeRowVector({lazySequence<int32_t>(10)}),
      makeRowVector({lazySequence<int32_t>(100, 10)}),
      makeRowVector({lazySequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({lazySequence<int32_t>(11, 10 + 100 + 1'000)}),
  };

  createDuckDbTable("t", {makeRowVector({sequence<int32_t>(1117)})});
  createDuckDbTable("u", {makeRowVector({sequence<int32_t>(1121)})});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .crossJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .project({"c0 AS u_c0"})
                        .planNode(),
                    {"c0", "u_c0"})
                .filter("c0 + u_c0 < 100")
                .planNode();

  assertQuery(op, "SELECT * FROM t, u WHERE t.c0 + u.c0 < 100");
}

// Test cross join with a build side that has rows, but no columns.
TEST_F(CrossJoinTest, zeroColumnBuild) {
  auto leftVectors = {
      makeRowVector({sequence<int32_t>(10)}),
      makeRowVector({sequence<int32_t>(100, 10)}),
      makeRowVector({sequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({sequence<int32_t>(7, 10 + 100 + 1'000)}),
  };

  auto rightVectors = {
      makeRowVector({sequence<int32_t>(5)}),
      //      vectorMaker_.rowVector(ROW({}, {}), 5),
  };

  createDuckDbTable("t", {leftVectors});

  // Build side has > 1 row.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({leftVectors})
                .crossJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values({rightVectors})
                        .project({})
                        .planNode(),
                    {"c0"})
                .planNode();

  assertQuery(
      op, "SELECT t.* FROM t, (SELECT * FROM UNNEST (ARRAY[0, 1, 2, 3, 4])) u");

  // Build side has exactly 1 row.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({leftVectors})
           .crossJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({rightVectors})
                   .filter("c0 = 1")
                   .project({})
                   .planNode(),
               {"c0"})
           .planNode();

  assertQuery(op, "SELECT * FROM t");
}

// Test multi-threaded build and probe sides.
TEST_F(CrossJoinTest, parallelism) {
  // Setup 5 threads for build and probe. Each build thread gets 3 identical
  // rows of input from the Values operator. The build thread that finishes last
  // combines data from all other threads making it 3x5=15 rows and puts them
  // into the CrossJoinBridge. All probe threads get 2 identical ros of input
  // from the Values operator and join them with 15 rows of build side data from
  // the bridge. Each probe thread is expected to produce 30 rows.

  auto left = {makeRowVector({sequence<int32_t>(2)})};
  auto right = {makeRowVector({"u_c0"}, {sequence<int32_t>(3)})};

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  CursorParameters params;
  params.maxDrivers = 5;
  params.planNode =
      PlanBuilder(planNodeIdGenerator)
          .values({left}, true)
          .crossJoin(
              PlanBuilder(planNodeIdGenerator).values({right}, true).planNode(),
              {"c0", "u_c0"})
          .partialAggregation({}, {"count(1)"})
          .planNode();

  OperatorTestBase::assertQuery(params, "VALUES (30), (30), (30), (30), (30)");
}
