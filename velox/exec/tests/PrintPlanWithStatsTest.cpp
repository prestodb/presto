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

#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

using facebook::velox::exec::test::PlanBuilder;

class PrintPlanWithStatsTest : public HiveConnectorTestBase {};

void compareOutputs(
    const std::string& testName,
    const std::string& result,
    const std::string& expectedRegex) {
  std::string line;
  std::string eline;
  std::istringstream iss(result);
  std::istringstream ess(expectedRegex);
  int lineCount = 0;
  for (; std::getline(iss, line);) {
    lineCount++;
    std::getline(ess, eline);
    ASSERT_THAT(line, ::testing::MatchesRegex(eline))
        << "Source:" << testName << ", Line:" << lineCount;
  }
  ASSERT_FALSE(std::getline(ess, line));
}

void ensureTaskCompletion(exec::Task* task) {
  // ASSERT_TRUE requires a function with return type void.
  ASSERT_TRUE(waitForTaskCompletion(task));
}

// The test outputs of "innerJoinWithTableScan" and
// "partialAggregateWithTableScan" reflect the documentation for
// printPlanWithStats. A failure likely means that the documentation needs an
// update as well.

TEST_F(PrintPlanWithStatsTest, innerJoinWithTableScan) {
  const int32_t numSplits = 20;
  const int32_t numRowsProbe = 1024;
  const int32_t numRowsBuild = 100;
  std::vector<RowVectorPtr> leftVectors;
  leftVectors.reserve(numSplits);
  auto leftFiles = makeFilePaths(numSplits);

  for (int i = 0; i < numSplits; i++) {
    auto rowVector = makeRowVector({
        makeFlatVector<int32_t>(
            numRowsProbe, [&](auto row) { return row - i * 10; }),
        makeFlatVector<int64_t>(numRowsProbe, [](auto row) { return row; }),
    });
    leftVectors.push_back(rowVector);
    writeToFile(leftFiles[i]->path, rowVector);
  }
  auto probeType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
  // 100 key values in [35, 233] range.
  auto rightKey = makeFlatVector<int32_t>(
      numRowsBuild, [](auto row) { return 35 + row * 2; });
  auto rightVectors = {makeRowVector({
      rightKey,
      makeFlatVector<int64_t>(numRowsBuild, [](auto row) { return row; }),
  })};
  createDuckDbTable("t", {leftVectors});
  createDuckDbTable("u", {rightVectors});

  auto buildSide = PlanBuilder(planNodeIdGenerator)
                       .values(rightVectors)
                       .project({"c0 AS u_c0", "c1 AS u_c1"})
                       .planNode();
  core::PlanNodeId leftScanId;
  auto op = PlanBuilder(planNodeIdGenerator)
                .tableScan(probeType)
                .capturePlanNodeId(leftScanId)
                .hashJoin(
                    {"c0"},
                    {"u_c0"},
                    buildSide,
                    "",
                    {"c0", "c1", "u_c1"},
                    core::JoinType::kInner)
                .project({"c0", "c1 + 1", "c1 + u_c1"})
                .planNode();

  auto task =
      AssertQueryBuilder(op, duckDbQueryRunner_)
          .splits(leftScanId, makeHiveConnectorSplits(leftFiles))
          .assertResults(
              "SELECT t.c0, t.c1 + 1, t.c1 + u.c1 FROM t, u WHERE t.c0 = u.c0");

  ensureTaskCompletion(task.get());
  compareOutputs(
      ::testing::UnitTest::GetInstance()->current_test_info()->name(),
      printPlanWithStats(*op, task->taskStats()),
      "-- Project\\[expressions: \\(c0:INTEGER, ROW\\[\"c0\"\\]\\), \\(p1:BIGINT, plus\\(ROW\\[\"c1\"\\],1\\)\\), \\(p2:BIGINT, plus\\(ROW\\[\"c1\"\\],ROW\\[\"u_c1\"\\]\\)\\)\\] -> c0:INTEGER, p1:BIGINT, p2:BIGINT\n"
      "   Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1\n"
      "  -- HashJoin\\[INNER c0=u_c0\\] -> c0:INTEGER, c1:BIGINT, u_c1:BIGINT\n"
      "     Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 2\\.00MB, Memory allocations: .+\n"
      "     HashBuild: Input: 100 rows \\(.+\\), Output: 0 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1\n"
      "     HashProbe: Input: 2000 rows \\(.+\\), Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1\n"
      "    -- TableScan\\[table: hive_table\\] -> c0:INTEGER, c1:BIGINT\n"
      "       Input: 2000 rows \\(.+\\), Raw Input: 20480 rows \\(.+\\), Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1, Splits: 20\n"
      "    -- Project\\[expressions: \\(u_c0:INTEGER, ROW\\[\"c0\"\\]\\), \\(u_c1:BIGINT, ROW\\[\"c1\"\\]\\)\\] -> u_c0:INTEGER, u_c1:BIGINT\n"
      "       Output: 100 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 0B, Memory allocations: .+, Threads: 1\n"
      "      -- Values\\[100 rows in 1 vectors\\] -> c0:INTEGER, c1:BIGINT\n"
      "         Input: 0 rows \\(.+\\), Output: 100 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 0B, Memory allocations: .+, Threads: 1\n");

  // with custom stats
  compareOutputs(
      ::testing::UnitTest::GetInstance()->current_test_info()->name(),
      printPlanWithStats(*op, task->taskStats(), true),
      "-- Project\\[expressions: \\(c0:INTEGER, ROW\\[\"c0\"\\]\\), \\(p1:BIGINT, plus\\(ROW\\[\"c1\"\\],1\\)\\), \\(p2:BIGINT, plus\\(ROW\\[\"c1\"\\],ROW\\[\"u_c1\"\\]\\)\\)\\] -> c0:INTEGER, p1:BIGINT, p2:BIGINT\n"
      "   Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1\n"
      "      dataSourceLazyWallNanos    sum: .+, count: 20, min: .+, max: .+\n"
      "  -- HashJoin\\[INNER c0=u_c0\\] -> c0:INTEGER, c1:BIGINT, u_c1:BIGINT\n"
      "     Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 2\\.00MB, Memory allocations: .+\n"
      "     HashBuild: Input: 100 rows \\(.+\\), Output: 0 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1\n"
      "        distinctKey0       sum: 101, count: 1, min: 101, max: 101\n"
      "        queuedWallNanos    sum: .+, count: 1, min: .+, max: .+\n"
      "        rangeKey0          sum: 200, count: 1, min: 200, max: 200\n"
      "     HashProbe: Input: 2000 rows \\(.+\\), Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1\n"
      "        dynamicFiltersProduced    sum: 1, count: 1, min: 1, max: 1\n"
      "        queuedWallNanos           sum: .+, count: 1, min: .+, max: .+\n"
      "    -- TableScan\\[table: hive_table\\] -> c0:INTEGER, c1:BIGINT\n"
      "       Input: 2000 rows \\(.+\\), Raw Input: 20480 rows \\(.+\\), Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1, Splits: 20\n"
      "          dataSourceWallNanos       sum: .+, count: 40, min: .+, max: .+\n"
      "          dynamicFiltersAccepted    sum: 1, count: 1, min: 1, max: 1\n"
      "          localReadBytes            sum: 0B, count: 1, min: 0B, max: 0B\n"
      "          numLocalRead              sum: 0, count: 1, min: 0, max: 0\n"
      "          numPrefetch               sum: .+, count: 1, min: .+, max: .+\n"
      "          numRamRead                sum: 0, count: 1, min: 0, max: 0\n"
      "          numStorageRead            sum: .+, count: 1, min: .+, max: .+\n"
      "          prefetchBytes             sum: .+, count: 1, min: .+, max: .+\n"
      "          queuedWallNanos           sum: .+, count: 1, min: .+, max: .+\n"
      "          ramReadBytes              sum: 0B, count: 1, min: 0B, max: 0B\n"
      "          skippedSplitBytes         sum: 0B, count: 1, min: 0B, max: 0B\n"
      "          skippedSplits             sum: 0, count: 1, min: 0, max: 0\n"
      "          skippedStrides            sum: 0, count: 1, min: 0, max: 0\n"
      "          storageReadBytes          sum: .+, count: 1, min: .+, max: .+\n"
      "    -- Project\\[expressions: \\(u_c0:INTEGER, ROW\\[\"c0\"\\]\\), \\(u_c1:BIGINT, ROW\\[\"c1\"\\]\\)\\] -> u_c0:INTEGER, u_c1:BIGINT\n"
      "       Output: 100 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 0B, Memory allocations: .+, Threads: 1\n"
      "      -- Values\\[100 rows in 1 vectors\\] -> c0:INTEGER, c1:BIGINT\n"
      "         Input: 0 rows \\(.+\\), Output: 100 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 0B, Memory allocations: .+, Threads: 1\n");
}

TEST_F(PrintPlanWithStatsTest, partialAggregateWithTableScan) {
  RowTypePtr rowType{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
  auto vectors = makeVectors(rowType, 10, 1'000);
  auto filePath = TempFilePath::create();
  writeToFile(filePath->path, vectors);
  createDuckDbTable(vectors);

  auto op =
      PlanBuilder()
          .tableScan(rowType)
          .partialAggregation(
              {"c5"}, {"max(c0)", "sum(c1)", "sum(c2)", "sum(c3)", "sum(c4)"})
          .planNode();

  auto task = assertQuery(
      op,
      {filePath},
      "SELECT c5, max(c0), sum(c1), sum(c2), sum(c3), sum(c4) FROM tmp group by c5");

  ensureTaskCompletion(task.get());
  compareOutputs(
      ::testing::UnitTest::GetInstance()->current_test_info()->name(),
      printPlanWithStats(*op, task->taskStats()),
      "-- Aggregation\\[PARTIAL \\[c5\\] a0 := max\\(ROW\\[\"c0\"\\]\\), a1 := sum\\(ROW\\[\"c1\"\\]\\), a2 := sum\\(ROW\\[\"c2\"\\]\\), a3 := sum\\(ROW\\[\"c3\"\\]\\), a4 := sum\\(ROW\\[\"c4\"\\]\\)\\] -> c5:VARCHAR, a0:BIGINT, a1:BIGINT, a2:BIGINT, a3:DOUBLE, a4:DOUBLE\n"
      "   Output: .+, Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1\n"
      "  -- TableScan\\[table: hive_table\\] -> c0:BIGINT, c1:INTEGER, c2:SMALLINT, c3:REAL, c4:DOUBLE, c5:VARCHAR\n"
      "     Input: 10000 rows \\(.+\\), Output: 10000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1, Splits: 1\n");

  compareOutputs(
      ::testing::UnitTest::GetInstance()->current_test_info()->name(),
      printPlanWithStats(*op, task->taskStats(), true),
      "-- Aggregation\\[PARTIAL \\[c5\\] a0 := max\\(ROW\\[\"c0\"\\]\\), a1 := sum\\(ROW\\[\"c1\"\\]\\), a2 := sum\\(ROW\\[\"c2\"\\]\\), a3 := sum\\(ROW\\[\"c3\"\\]\\), a4 := sum\\(ROW\\[\"c4\"\\]\\)\\] -> c5:VARCHAR, a0:BIGINT, a1:BIGINT, a2:BIGINT, a3:DOUBLE, a4:DOUBLE\n"
      "   Output: .+, Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1\n"
      "  -- TableScan\\[table: hive_table\\] -> c0:BIGINT, c1:INTEGER, c2:SMALLINT, c3:REAL, c4:DOUBLE, c5:VARCHAR\n"
      "     Input: 10000 rows \\(.+\\), Output: 10000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 1\\.00MB, Memory allocations: .+, Threads: 1, Splits: 1\n"
      "        dataSourceLazyWallNanos    sum: .+, count: 7, min: .+, max: .+\n"
      "        dataSourceWallNanos        sum: .+, count: 2, min: .+, max: .+\n"
      "        loadedToValueHook          sum: 50000, count: 5, min: 10000, max: 10000\n"
      "        localReadBytes             sum: 0B, count: 1, min: 0B, max: 0B\n"
      "        numLocalRead               sum: 0, count: 1, min: 0, max: 0\n"
      "        numPrefetch                sum: .+, count: .+, min: .+, max: .+\n"
      "        numRamRead                 sum: 0, count: 1, min: 0, max: 0\n"
      "        numStorageRead             sum: .+, count: 1, min: .+, max: .+\n"
      "        prefetchBytes              sum: .+, count: 1, min: .+, max: .+\n"
      "        queuedWallNanos            sum: .+, count: 1, min: .+, max: .+\n"
      "        ramReadBytes               sum: 0B, count: 1, min: 0B, max: 0B\n"
      "        skippedSplitBytes          sum: 0B, count: 1, min: 0B, max: 0B\n"
      "        skippedSplits              sum: 0, count: 1, min: 0, max: 0\n"
      "        skippedStrides             sum: 0, count: 1, min: 0, max: 0\n"
      "        storageReadBytes           sum: .+, count: 1, min: .+, max: .+\n");
}
