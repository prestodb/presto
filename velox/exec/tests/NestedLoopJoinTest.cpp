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
#include "velox/exec/tests/utils/VectorTestUtil.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class NestedLoopJoinTest : public HiveConnectorTestBase {
 protected:
  void setProbeType(const RowTypePtr& probeType) {
    probeType_ = probeType;
  }

  void setBuildType(const RowTypePtr& buildType) {
    buildType_ = buildType;
  }

  void setComparisons(std::vector<std::string> comparisons) {
    comparisons_ = std::move(comparisons);
  }

  void setOutputLayout(std::vector<std::string> outputLayout) {
    outputLayout_ = std::move(outputLayout);
  }

  void setJoinConditionStr(std::string joinConditionStr) {
    joinConditionStr_ = std::move(joinConditionStr);
  }

  void setQueryStr(std::string queryStr) {
    queryStr_ = std::move(queryStr);
  }

  void setJoinTypes(std::vector<core::JoinType> joinTypes) {
    joinTypes_ = std::move(joinTypes);
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

  void runSingleAndMultiDriverTest(
      const std::vector<RowVectorPtr>& probeVectors,
      const std::vector<RowVectorPtr>& buildVectors) {
    runTest(probeVectors, buildVectors, 1);
    runTest(probeVectors, buildVectors, 4);
  }

  void runTest(
      const std::vector<RowVectorPtr>& probeVectors,
      const std::vector<RowVectorPtr>& buildVectors,
      int32_t numDrivers) {
    createDuckDbTable("t", probeVectors);
    createDuckDbTable("u", buildVectors);

    CursorParameters params;
    params.maxDrivers = numDrivers;
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    for (const auto joinType : joinTypes_) {
      for (const auto& comparison : comparisons_) {
        SCOPED_TRACE(fmt::format(
            "maxDrivers:{} joinType:{} comparison:{}",
            std::to_string(numDrivers),
            joinTypeName(joinType),
            comparison));

        params.planNode =
            PlanBuilder(planNodeIdGenerator)
                .values(probeVectors)
                .localPartition({probeKeyName_})
                .nestedLoopJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values(buildVectors)
                        .localPartition({buildKeyName_})
                        .planNode(),
                    fmt::format(fmt::runtime(joinConditionStr_), comparison),
                    outputLayout_,
                    joinType)
                .planNode();

        assertQuery(
            params,
            fmt::format(
                fmt::runtime(queryStr_), joinTypeName(joinType), comparison));
      }
    }
  }

 protected:
  const std::string probeKeyName_{"t0"};
  const std::string buildKeyName_{"u0"};
  RowTypePtr probeType_{ROW({{probeKeyName_, BIGINT()}})};
  RowTypePtr buildType_{ROW({{buildKeyName_, BIGINT()}})};
  std::vector<std::string> comparisons_{"=", "<", "<=", "<>"};
  std::vector<core::JoinType> joinTypes_{
      core::JoinType::kInner,
      core::JoinType::kLeft,
      core::JoinType::kRight,
      core::JoinType::kFull};
  std::vector<std::string> outputLayout_{probeKeyName_, buildKeyName_};
  std::string joinConditionStr_{probeKeyName_ + " {} " + buildKeyName_};
  std::string queryStr_{fmt::format(
      "SELECT {0}, {1} FROM t {{}} JOIN u ON t.{0} {{}} u.{1}",
      probeKeyName_,
      buildKeyName_)};
};

TEST_F(NestedLoopJoinTest, emptyBuildOrProbeWithoutFilter) {
  auto empty = makeRowVector({"u0"}, {makeFlatVector<StringView>({})});
  auto nonEmpty = makeRowVector({"t0"}, {makeFlatVector<StringView>({"foo"})});
  auto expected = makeRowVector({makeFlatVector<StringView>({"foo", "foo"})});

  auto testJoin = [&](const std::vector<RowVectorPtr>& leftVectors,
                      const std::vector<RowVectorPtr>& rightVectors,
                      core::JoinType joinType,
                      const std::vector<std::string>& outputLayout,
                      const VectorPtr& expected) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .values(leftVectors)
                    .localPartitionRoundRobinRow()
                    .nestedLoopJoin(
                        PlanBuilder(planNodeIdGenerator)
                            .values(rightVectors)
                            .localPartition({})
                            .planNode(),
                        "",
                        outputLayout,
                        joinType)
                    .planNode();
    AssertQueryBuilder builder{plan};
    auto result = builder.copyResults(pool());
    facebook::velox::test::assertEqualVectors(expected, result);
  };

  testJoin({nonEmpty}, {empty}, core::JoinType::kLeft, {"t0"}, nonEmpty);
  testJoin(
      {nonEmpty, nonEmpty},
      {empty, empty},
      core::JoinType::kLeft,
      {"t0"},
      expected);
  testJoin({empty}, {nonEmpty}, core::JoinType::kLeft, {"u0"}, empty);

  testJoin({empty}, {nonEmpty}, core::JoinType::kRight, {"t0"}, nonEmpty);
  testJoin(
      {empty, empty},
      {nonEmpty, nonEmpty},
      core::JoinType::kRight,
      {"t0"},
      expected);
  testJoin({nonEmpty}, {empty}, core::JoinType::kRight, {"u0"}, empty);

  testJoin(
      {nonEmpty, nonEmpty},
      {empty, empty},
      core::JoinType::kFull,
      {"t0", "u0"},
      makeRowVector({
          makeFlatVector<StringView>({"foo", "foo"}),
          makeNullableFlatVector<StringView>({std::nullopt, std::nullopt}),
      }));
  testJoin(
      {empty, empty},
      {nonEmpty, nonEmpty},
      core::JoinType::kFull,
      {"u0", "t0"},
      makeRowVector({
          makeNullableFlatVector<StringView>({std::nullopt, std::nullopt}),
          makeFlatVector<StringView>({"foo", "foo"}),
      }));
}

TEST_F(NestedLoopJoinTest, basic) {
  auto probeVectors = makeBatches(20, 5, probeType_, pool_.get());
  auto buildVectors = makeBatches(18, 5, buildType_, pool_.get());
  runSingleAndMultiDriverTest(probeVectors, buildVectors);
}

TEST_F(NestedLoopJoinTest, emptyProbe) {
  auto probeVectors = makeBatches(0, 5, probeType_, pool_.get());
  auto buildVectors = makeBatches(18, 5, buildType_, pool_.get());
  runSingleAndMultiDriverTest(probeVectors, buildVectors);
}

TEST_F(NestedLoopJoinTest, emptyBuild) {
  auto probeVectors = makeBatches(20, 5, probeType_, pool_.get());
  auto buildVectors = makeBatches(0, 5, buildType_, pool_.get());
  runSingleAndMultiDriverTest(probeVectors, buildVectors);
}

TEST_F(NestedLoopJoinTest, basicCrossJoin) {
  auto probeVectors = {
      makeRowVector({sequence<int32_t>(10)}),
      makeRowVector({sequence<int32_t>(100, 10)}),
      makeRowVector({sequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({sequence<int32_t>(7, 10 + 100 + 1'000)}),
  };

  auto buildVectors = {
      makeRowVector({sequence<int32_t>(10)}),
      makeRowVector({sequence<int32_t>(100, 10)}),
      makeRowVector({sequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({sequence<int32_t>(11, 10 + 100 + 1'000)}),
  };

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  // All x 13. Join output vectors contains multiple probe rows each.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({probeVectors})
                .nestedLoopJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values({buildVectors})
                        .filter("c0 < 13")
                        .project({"c0 AS u_c0"})
                        .planNode(),
                    {"c0", "u_c0"})
                .planNode();

  assertQuery(op, "SELECT * FROM t, u WHERE u.c0 < 13");

  // 13 x all. Join output vectors contains single probe row each.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({probeVectors})
           .filter("c0 < 13")
           .nestedLoopJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({buildVectors})
                   .project({"c0 AS u_c0"})
                   .planNode(),
               {"c0", "u_c0"})
           .planNode();

  assertQuery(op, "SELECT * FROM t, u WHERE t.c0 < 13");

  // All x 13. No columns on the build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({probeVectors})
           .nestedLoopJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({vectorMaker_.rowVector(ROW({}, {}), 13)})
                   .planNode(),
               {"c0"})
           .planNode();

  assertQuery(op, "SELECT t.* FROM t, (SELECT * FROM u LIMIT 13) u");

  // 13 x All. No columns on the build side.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({probeVectors})
           .filter("c0 < 13")
           .nestedLoopJoin(
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
           .values({probeVectors})
           .nestedLoopJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({buildVectors})
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
                        .values({probeVectors})
                        .nestedLoopJoin(
                            PlanBuilder(planNodeIdGenerator, pool_.get())
                                .values({buildVectors}, true)
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

TEST_F(NestedLoopJoinTest, outerJoinWithoutCondition) {
  auto probeVectors = {
      makeRowVector({sequence<int32_t>(10)}),
      makeRowVector({sequence<int32_t>(100, 10)}),
  };

  auto buildVectors = {
      makeRowVector({sequence<int32_t>(1, 111)}),
  };

  createDuckDbTable("t", {probeVectors});
  createDuckDbTable("u", {buildVectors});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto testOuterJoin = [&](core::JoinType joinType) {
    auto op = PlanBuilder(planNodeIdGenerator)
                  .values({probeVectors})
                  .nestedLoopJoin(
                      PlanBuilder(planNodeIdGenerator)
                          .values({buildVectors})
                          .project({"c0 AS u_c0"})
                          .planNode(),
                      {"c0", "u_c0"},
                      joinType)
                  .singleAggregation({}, {"count(*)"})
                  .planNode();

    assertQuery(
        op,
        fmt::format(
            "SELECT count(*) FROM t {} join u on 1",
            core::joinTypeName(joinType)));
  };
  testOuterJoin(core::JoinType::kLeft);
  testOuterJoin(core::JoinType::kRight);
  testOuterJoin(core::JoinType::kFull);
}

TEST_F(NestedLoopJoinTest, lazyVectors) {
  auto probeVectors = {
      makeRowVector({lazySequence<int32_t>(10)}),
      makeRowVector({lazySequence<int32_t>(100, 10)}),
      makeRowVector({lazySequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({lazySequence<int32_t>(7, 10 + 100 + 1'000)}),
  };

  auto buildVectors = {
      makeRowVector({lazySequence<int32_t>(10)}),
      makeRowVector({lazySequence<int32_t>(100, 10)}),
      makeRowVector({lazySequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({lazySequence<int32_t>(11, 10 + 100 + 1'000)}),
  };

  createDuckDbTable("t", {makeRowVector({sequence<int32_t>(1117)})});
  createDuckDbTable("u", {makeRowVector({sequence<int32_t>(1121)})});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({probeVectors})
                .nestedLoopJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values({buildVectors})
                        .project({"c0 AS u_c0"})
                        .planNode(),
                    "c0+u_c0<100",
                    {"c0", "u_c0"},
                    core::JoinType::kFull)
                .planNode();

  assertQuery(op, "SELECT * FROM t FULL JOIN u ON t.c0 + u.c0 < 100");
}

// Test cross join with a build side that has rows, but no columns.
TEST_F(NestedLoopJoinTest, zeroColumnBuild) {
  auto probeVectors = {
      makeRowVector({sequence<int32_t>(10)}),
      makeRowVector({sequence<int32_t>(100, 10)}),
      makeRowVector({sequence<int32_t>(1'000, 10 + 100)}),
      makeRowVector({sequence<int32_t>(7, 10 + 100 + 1'000)}),
  };

  auto buildVectors = {
      makeRowVector({sequence<int32_t>(1)}),
      makeRowVector({sequence<int32_t>(4, 1)})};

  createDuckDbTable("t", {probeVectors});

  // Build side has > 1 row.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto op = PlanBuilder(planNodeIdGenerator)
                .values({probeVectors})
                .nestedLoopJoin(
                    PlanBuilder(planNodeIdGenerator)
                        .values({buildVectors})
                        .project({})
                        .planNode(),
                    {"c0"})
                .planNode();

  assertQuery(
      op, "SELECT t.* FROM t, (SELECT * FROM UNNEST (ARRAY[0, 1, 2, 3, 4])) u");

  // Build side has exactly 1 row.
  planNodeIdGenerator->reset();
  op = PlanBuilder(planNodeIdGenerator)
           .values({probeVectors})
           .nestedLoopJoin(
               PlanBuilder(planNodeIdGenerator)
                   .values({buildVectors})
                   .filter("c0 = 1")
                   .project({})
                   .planNode(),
               {"c0"})
           .planNode();

  assertQuery(op, "SELECT * FROM t");
}

TEST_F(NestedLoopJoinTest, bigintArray) {
  auto probeVectors = makeBatches(1000, 5, probeType_, pool_.get());
  auto buildVectors = makeBatches(900, 5, buildType_, pool_.get());
  setComparisons({"="});
  setJoinTypes({core::JoinType::kFull});
  runSingleAndMultiDriverTest(probeVectors, buildVectors);
}

TEST_F(NestedLoopJoinTest, allTypes) {
  RowTypePtr probeType = ROW(
      {{"t0", BIGINT()},
       {"t1", VARCHAR()},
       {"t2", REAL()},
       {"t3", DOUBLE()},
       {"t4", INTEGER()},
       {"t5", SMALLINT()},
       {"t6", TINYINT()}});

  RowTypePtr buildType = ROW(
      {{"u0", BIGINT()},
       {"u1", VARCHAR()},
       {"u2", REAL()},
       {"u3", DOUBLE()},
       {"u4", INTEGER()},
       {"u5", SMALLINT()},
       {"u6", TINYINT()}});

  auto probeVectors = makeBatches(60, 5, probeType, pool_.get());
  auto buildVectors = makeBatches(50, 5, buildType, pool_.get());
  createDuckDbTable("t", probeVectors);
  createDuckDbTable("u", buildVectors);

  setProbeType(probeType);
  setBuildType(buildType);
  setComparisons({"="});
  setJoinConditionStr(
      "t0 {0} u0 AND t1 {0} u1 AND t2 {0} u2 AND t3 {0} u3 AND t4 {0} u4 AND t5 {0} u5 AND t6 {0} u6");
  setQueryStr(
      "SELECT t0, u0 FROM t {0} JOIN u ON t.t0 {1} u0 AND t1 {1} u1 AND t2 {1} u2 AND t3 {1} u3 AND t4 {1} u4 AND t5 {1} u5 AND t6 {1} u6");
  runSingleAndMultiDriverTest(probeVectors, buildVectors);
}
