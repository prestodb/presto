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
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::exec::test {

class SpatialJoinTest : public OperatorTestBase {
 public:
  /* The polygons below have the following relations:
   * [Legend: = equals, v overlap, / disjoint]
   *
   *   A  B  C  D
   * A =  v  /  /
   * B v  =  /  /
   * C /  /  =  /
   * D /  /  /  =
   *
   * Overlap means geometries share interior points (for full definition see
   * (DE-9IM)[https://en.wikipedia.org/wiki/DE-9IM]), but neither contains the
   * other.
   */
  static constexpr std::string_view kPolygonA =
      "POLYGON ((0 0, -0.5 2.5, 0 5, 2.5 5.5, 5 5, 5.5 2.5, 5 0, 2.5 -0.5, 0 0))";
  static constexpr std::string_view kPolygonB =
      "POLYGON ((4 4, 3.5 7, 4 10, 7 10.5, 10 10, 10.5 7, 10 4, 7 3.5, 4 4))";
  static constexpr std::string_view kPolygonC =
      "POLYGON ((15 15, 15 14, 14 14, 14 15, 15 15))";
  static constexpr std::string_view kPolygonD =
      "POLYGON ((18 18, 18 19, 19 19, 19 18, 18 18))";

  // A set of points: X in A, Y in A and B, Z in B, W outside of A and B
  static constexpr std::string_view kPointX = "POINT (1 1)";
  static constexpr std::string_view kPointY = "POINT (4.5 4.5)";
  static constexpr std::string_view kPointZ = "POINT (6 6)";
  static constexpr std::string_view kPointW = "POINT (20 20)";
  static constexpr std::string_view kPointV = "POINT (15 15)";
  static constexpr std::string_view kPointS = "POINT (18 18)";
  static constexpr std::string_view kPointQ = "POINT (28 28)";
  static constexpr std::string_view kMultipointU = "MULTIPOINT (15 15)";
  static constexpr std::string_view kMultipointT =
      "MULTIPOINT (14.5 14.5, 16 16)";
  static constexpr std::string_view kMultipointR = "MULTIPOINT (15 15, 19 19)";

 protected:
  void runTest(
      const std::vector<std::optional<std::string_view>>& probeWkts,
      const std::vector<std::optional<std::string_view>>& buildWkts,
      const std::string& predicate,
      core::JoinType joinType,
      const std::vector<std::optional<std::string_view>>& expectedLeftWkts,
      const std::vector<std::optional<std::string_view>>& expectedRightWkts) {
    runTestWithDrivers(
        probeWkts,
        buildWkts,
        predicate,
        joinType,
        expectedLeftWkts,
        expectedRightWkts,
        1,
        false);
    runTestWithDrivers(
        probeWkts,
        buildWkts,
        predicate,
        joinType,
        expectedLeftWkts,
        expectedRightWkts,
        1,
        true);
    runTestWithDrivers(
        probeWkts,
        buildWkts,
        predicate,
        joinType,
        expectedLeftWkts,
        expectedRightWkts,
        4,
        false);
    runTestWithDrivers(
        probeWkts,
        buildWkts,
        predicate,
        joinType,
        expectedLeftWkts,
        expectedRightWkts,
        4,
        true);
  }

  void runTestWithDrivers(
      const std::vector<std::optional<std::string_view>>& probeWkts,
      const std::vector<std::optional<std::string_view>>& buildWkts,
      const std::string& predicate,
      core::JoinType joinType,
      const std::vector<std::optional<std::string_view>>& expectedLeftWkts,
      const std::vector<std::optional<std::string_view>>& expectedRightWkts,
      int32_t maxDrivers,
      bool separateBatches) {
    std::vector<std::optional<std::string>> probeWktsStr(
        probeWkts.begin(), probeWkts.end());
    std::vector<std::optional<std::string>> buildWktsStr(
        buildWkts.begin(), buildWkts.end());
    std::vector<std::optional<std::string>> expectedLeftWktsStr(
        expectedLeftWkts.begin(), expectedLeftWkts.end());
    std::vector<std::optional<std::string>> expectedRightWktsStr(
        expectedRightWkts.begin(), expectedRightWkts.end());

    std::vector<RowVectorPtr> probeBatches;
    std::vector<RowVectorPtr> buildBatches;
    if (separateBatches) {
      for (const auto& wkt : probeWktsStr) {
        probeBatches.push_back(makeRowVector(
            {"left_g"}, {makeNullableFlatVector<std::string>({wkt})}));
      }
      for (const auto& wkt : buildWktsStr) {
        buildBatches.push_back(makeRowVector(
            {"right_g"}, {makeNullableFlatVector<std::string>({wkt})}));
      }
    } else {
      probeBatches.push_back(makeRowVector(
          {"left_g"}, {makeNullableFlatVector<std::string>(probeWktsStr)}));
      buildBatches.push_back(makeRowVector(
          {"right_g"}, {makeNullableFlatVector<std::string>(buildWktsStr)}));
    }
    auto expectedRows = makeRowVector(
        {"left_g", "right_g"},
        {makeNullableFlatVector<std::string>(expectedLeftWktsStr),
         makeNullableFlatVector<std::string>(expectedRightWktsStr)});

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan =
        PlanBuilder(planNodeIdGenerator)
            .values(probeBatches)
            .project({"ST_GeometryFromText(left_g) AS left_g"})
            .localPartitionRoundRobinRow()
            .spatialJoin(
                PlanBuilder(planNodeIdGenerator)
                    .values(buildBatches)
                    .project({"ST_GeometryFromText(right_g) AS right_g"})
                    .localPartition({})
                    .planNode(),
                predicate,
                {"left_g", "right_g"},
                joinType)
            .project(
                {"ST_AsText(left_g) AS left_g",
                 "ST_AsText(right_g) AS right_g"})
            .planNode();
    AssertQueryBuilder builder{plan};
    builder.maxDrivers(maxDrivers).assertResults({expectedRows});
  }
};
TEST_F(SpatialJoinTest, simpleSpatialJoin) {
  runTest(
      {"POINT (1 1)", "POINT (1 2)"},
      {"POINT (1 1)", "POINT (2 1)"},
      "ST_Intersects(left_g, right_g)",
      core::JoinType::kInner,
      {"POINT (1 1)"},
      {"POINT (1 1)"});
  runTest(
      {"POINT (1 1)", "POINT (1 2)"},
      {"POINT (1 1)", "POINT (2 1)"},
      "ST_Intersects(left_g, right_g)",
      core::JoinType::kLeft,
      {"POINT (1 1)", "POINT (1 2)"},
      {"POINT (1 1)", std::nullopt});
}

TEST_F(SpatialJoinTest, selfSpatialJoin) {
  std::vector<std::optional<std::string_view>> inputWkts = {
      kPolygonA, kPolygonB, kPolygonC, kPolygonD};
  std::vector<std::optional<std::string_view>> leftOutputWkts = {
      kPolygonA, kPolygonA, kPolygonB, kPolygonB, kPolygonC, kPolygonD};
  std::vector<std::optional<std::string_view>> rightOutputWkts = {
      kPolygonA, kPolygonB, kPolygonA, kPolygonB, kPolygonC, kPolygonD};

  runTest(
      inputWkts,
      inputWkts,
      "ST_Intersects(left_g, right_g)",
      core::JoinType::kInner,
      leftOutputWkts,
      rightOutputWkts);

  runTest(
      inputWkts,
      inputWkts,
      "ST_Intersects(left_g, right_g)",
      core::JoinType::kLeft,
      leftOutputWkts,
      rightOutputWkts);

  runTest(
      inputWkts,
      inputWkts,
      "ST_Overlaps(left_g, right_g)",
      core::JoinType::kInner,
      {kPolygonA, kPolygonB},
      {kPolygonB, kPolygonA});

  runTest(
      inputWkts,
      inputWkts,
      "ST_Intersects(left_g, right_g) AND ST_Overlaps(left_g, right_g)",
      core::JoinType::kInner,
      {kPolygonA, kPolygonB},
      {kPolygonB, kPolygonA});

  runTest(
      inputWkts,
      inputWkts,
      "ST_Overlaps(left_g, right_g)",
      core::JoinType::kLeft,
      {kPolygonA, kPolygonB, kPolygonC, kPolygonD},
      {kPolygonB, kPolygonA, std::nullopt, std::nullopt});

  runTest(
      inputWkts,
      inputWkts,
      "ST_Equals(left_g, right_g)",
      core::JoinType::kInner,
      inputWkts,
      inputWkts);

  runTest(
      inputWkts,
      inputWkts,
      "ST_Equals(left_g, right_g)",
      core::JoinType::kLeft,
      inputWkts,
      inputWkts);
}

TEST_F(SpatialJoinTest, pointPolygonSpatialJoin) {
  std::vector<std::optional<std::string_view>> polygonWkts = {
      kPolygonA, kPolygonB, kPolygonC, kPolygonD};
  std::vector<std::optional<std::string_view>> pointWkts = {
      kPointX,
      kPointY,
      kPointZ,
      kPointW,
      kPointV,
      kPointS,
      kPointQ,
      kMultipointU,
      kMultipointT,
      kMultipointR};

  std::vector<std::optional<std::string_view>> pointOutputWkts = {
      kPointX,
      kPointY,
      kPointY,
      kPointZ,
      kPointV,
      kPointS,
      kMultipointU,
      kMultipointR,
      kMultipointR,
      kMultipointT};
  std::vector<std::optional<std::string_view>> polygonOutputWkts = {
      kPolygonA,
      kPolygonA,
      kPolygonB,
      kPolygonB,
      kPolygonC,
      kPolygonD,
      kPolygonC,
      kPolygonC,
      kPolygonD,
      kPolygonC};
  runTest(
      pointWkts,
      polygonWkts,
      "ST_Intersects(left_g, right_g)",
      core::JoinType::kInner,
      pointOutputWkts,
      polygonOutputWkts);
}

TEST_F(SpatialJoinTest, failOnGroupedExecution) {
  std::vector<RowVectorPtr> batches{
      makeRowVector({"wkt"}, {makeFlatVector<std::string>({"POINT(0 0)"})})};
  core::PlanNodeId groupedScanNodeId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto planFragment =
      PlanBuilder(planNodeIdGenerator)
          .values(batches)
          .capturePlanNodeId(groupedScanNodeId)
          .project({"ST_GeometryFromText(wkt) AS left_g"})
          .spatialJoin(
              PlanBuilder(planNodeIdGenerator)
                  .values(batches)
                  .project({"ST_GeometryFromText(wkt) AS right_g"})
                  .localPartition({})
                  .planNode(),
              "ST_Intersects(left_g, right_g)",
              {"left_g", "right_g"},
              core::JoinType::kInner)
          .project(
              {"ST_AsText(left_g) AS left_g", "ST_AsText(right_g) AS right_g"})
          .planFragment();
  planFragment.executionStrategy = core::ExecutionStrategy::kGrouped;
  planFragment.groupedExecutionLeafNodeIds.emplace(groupedScanNodeId);
  auto task = Task::create(
      "task-grouped-join",
      std::move(planFragment),
      0,
      core::QueryCtx::create(driverExecutor_.get()),
      Task::ExecutionMode::kParallel);

  VELOX_ASSERT_THROW(
      task->start(1), "Spatial joins do not support grouped execution.");
}

} // namespace facebook::velox::exec::test
