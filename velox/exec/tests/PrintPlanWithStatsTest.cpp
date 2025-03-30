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
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include <gtest/gtest.h>
#include <re2/re2.h>

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

using facebook::velox::exec::test::PlanBuilder;

class PrintPlanWithStatsTest : public HiveConnectorTestBase {};

struct ExpectedLine {
  std::string line;
  bool optional = false;
};

void compareOutputs(
    const std::string& testName,
    const std::string& result,
    const std::vector<ExpectedLine>& expectedRegex) {
  std::string line;
  std::string eline;
  std::istringstream iss(result);
  int lineCount = 0;
  int expectedLineIndex = 0;
  for (; std::getline(iss, line);) {
    lineCount++;
    std::vector<std::string> potentialLines;
    auto expectedLine = expectedRegex.at(expectedLineIndex++);
    while (!RE2::FullMatch(line, expectedLine.line)) {
      potentialLines.push_back(expectedLine.line);
      if (!expectedLine.optional) {
        ASSERT_FALSE(true) << "Output did not match."
                           << "\n  Source: " << testName
                           << "\n  Line number: " << lineCount
                           << "\n  Line: " << line
                           << "\n  Expected Line one of: "
                           << folly::join(",", potentialLines);
      }
      expectedLine = expectedRegex.at(expectedLineIndex++);
    }
  }
  for (int i = expectedLineIndex; i < expectedRegex.size(); i++) {
    ASSERT_TRUE(expectedRegex[expectedLineIndex].optional);
  }
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
    writeToFile(leftFiles[i]->getPath(), rowVector);
  }
  auto probeType = ROW({"c0", "c1"}, {INTEGER(), BIGINT()});

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
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
      task->printPlanWithStats(),
      {{"-- Project\\[4\\]\\[expressions: \\(c0:INTEGER, ROW\\[\"c0\"\\]\\), \\(p1:BIGINT, plus\\(ROW\\[\"c1\"\\],1\\)\\), \\(p2:BIGINT, plus\\(ROW\\[\"c1\"\\],ROW\\[\"u_c1\"\\]\\)\\)\\] -> c0:INTEGER, p1:BIGINT, p2:BIGINT"},
       {"   Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"  -- HashJoin\\[3\\]\\[INNER c0=u_c0\\] -> c0:INTEGER, c1:BIGINT, u_c1:BIGINT"},
       {"     Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"     HashBuild: Input: 100 rows \\(.+\\), Output: 0 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+ Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"     HashProbe: Input: 2000 rows \\(.+\\), Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"    -- TableScan\\[2\\]\\[table: hive_table\\] -> c0:INTEGER, c1:BIGINT"},
       {"       Input: 2000 rows \\(.+\\), Raw Input: 20480 rows \\(.+\\), Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, Splits: 20, DynamicFilter producer plan nodes: 3, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"    -- Project\\[1\\]\\[expressions: \\(u_c0:INTEGER, ROW\\[\"c0\"\\]\\), \\(u_c1:BIGINT, ROW\\[\"c1\"\\]\\)\\] -> u_c0:INTEGER, u_c1:BIGINT"},
       {"       Output: 100 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 0B, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"      -- Values\\[0\\]\\[100 rows in 1 vectors\\] -> c0:INTEGER, c1:BIGINT"},
       {"         Input: 0 rows \\(.+\\), Output: 100 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 0B, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"}});

  // with custom stats
  compareOutputs(
      ::testing::UnitTest::GetInstance()->current_test_info()->name(),
      printPlanWithStats(*op, task->taskStats(), true),
      {{"-- Project\\[4\\]\\[expressions: \\(c0:INTEGER, ROW\\[\"c0\"\\]\\), \\(p1:BIGINT, plus\\(ROW\\[\"c1\"\\],1\\)\\), \\(p2:BIGINT, plus\\(ROW\\[\"c1\"\\],ROW\\[\"u_c1\"\\]\\)\\)\\] -> c0:INTEGER, p1:BIGINT, p2:BIGINT"},
       {"   Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"      dataSourceLazyCpuNanos[ ]* sum: .+, count: .+, min: .+, max: .+"},
       {"      dataSourceLazyInputBytes[ ]* sum: .+, count: .+, min: .+, max: .+"},
       {"      dataSourceLazyWallNanos[ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"      runningAddInputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"      runningFinishWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"      runningGetOutputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"  -- HashJoin\\[3\\]\\[INNER c0=u_c0\\] -> c0:INTEGER, c1:BIGINT, u_c1:BIGINT"},
       {"     Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"     HashBuild: Input: 100 rows \\(.+\\), Output: 0 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"        distinctKey0\\s+sum: 101, count: 1, min: 101, max: 101, avg: 101"},
       {"        hashtable.buildWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"        hashtable.capacity\\s+sum: 200, count: 1, min: 200, max: 200, avg: 200"},
       {"        hashtable.numDistinct\\s+sum: 100, count: 1, min: 100, max: 100, avg: 100"},
       {"        hashtable.numRehashes\\s+sum: 1, count: 1, min: 1, max: 1, avg: 1"},
       {"        queuedWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"        rangeKey0\\s+sum: 200, count: 1, min: 200, max: 200, avg: 200"},
       {"        runningAddInputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"        runningFinishWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"        runningGetOutputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"     HashProbe: Input: 2000 rows \\(.+\\), Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       // These lines may or may not appear depending on whether the operator
       // gets blocked during a run.
       {"        blockedWaitForJoinBuildTimes\\s+sum: 1, count: 1, min: 1, max: 1, avg: 1",
        true},
       {"        blockedWaitForJoinBuildWallNanos\\s+sum: .+, count: 1, min: .+, max: .+",
        true},
       {"        dynamicFiltersProduced\\s+sum: 1, count: 1, min: 1, max: 1, avg: 1",
        true},
       {"        queuedWallNanos\\s+sum: .+, count: 1, min: .+, max: .+", true},
       // This line may or may not appear depending on how the threads
       // running the Drivers are executed, this only appears if the
       // HashProbe has to wait for the HashBuild construction.
       {"        runningAddInputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"        runningFinishWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"        runningGetOutputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"    -- TableScan\\[2\\]\\[table: hive_table\\] -> c0:INTEGER, c1:BIGINT"},
       {"       Input: 2000 rows \\(.+\\), Raw Input: 20480 rows \\(.+\\), Output: 2000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, Splits: 20, DynamicFilter producer plan nodes: 3, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"          dataSourceAddSplitWallNanos[ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"          dataSourceReadWallNanos[ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"          dynamicFiltersAccepted[ ]* sum: 1, count: 1, min: 1, max: 1, avg: 1"},
       {"          footerBufferOverread[ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"          ioWaitWallNanos      [ ]* sum: .+, count: .+ min: .+, max: .+"},
       {"          maxSingleIoWaitWallNanos[ ]*sum: .+, count: 1, min: .+, max: .+"},
       {"          numPrefetch         [ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"          numRamRead          [ ]* sum: 60, count: 1, min: 60, max: 60, avg: 60"},
       {"          numStorageRead      [ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"          numStripes[ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"          overreadBytes[ ]* sum: 0B, count: 1, min: 0B, max: 0B, avg: 0B"},
       {"          prefetchBytes       [ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"          preloadedSplits[ ]+sum: .+, count: .+, min: .+, max: .+",
        true},
       {"          processedSplits[ ]+sum: 20, count: 1, min: 20, max: 20, avg: 20"},
       {"          processedStrides[ ]+sum: 20, count: 1, min: 20, max: 20, avg: 20"},
       {"          ramReadBytes        [ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"          readyPreloadedSplits[ ]+sum: .+, count: .+, min: .+, max: .+",
        true},
       {"          runningAddInputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"          runningFinishWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"          runningGetOutputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"          storageReadBytes    [ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"          totalRemainingFilterTime\\s+sum: .+, count: .+, min: .+, max: .+"},
       {"          totalScanTime       [ ]* sum: .+, count: .+, min: .+, max: .+"},
       {"    -- Project\\[1\\]\\[expressions: \\(u_c0:INTEGER, ROW\\[\"c0\"\\]\\), \\(u_c1:BIGINT, ROW\\[\"c1\"\\]\\)\\] -> u_c0:INTEGER, u_c1:BIGINT"},
       {"       Output: 100 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 0B, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"          runningAddInputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"          runningFinishWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"          runningGetOutputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"      -- Values\\[0\\]\\[100 rows in 1 vectors\\] -> c0:INTEGER, c1:BIGINT"},
       {"         Input: 0 rows \\(.+\\), Output: 100 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: 0B, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"            runningAddInputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"            runningFinishWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"            runningGetOutputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"}});
}

TEST_F(PrintPlanWithStatsTest, partialAggregateWithTableScan) {
  RowTypePtr rowType{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
  auto vectors = makeVectors(rowType, 10, 1'000);
  createDuckDbTable(vectors);

  const std::vector<int32_t> numPrefetchSplits = {0, 2};
  for (const auto& numPrefetchSplit : numPrefetchSplits) {
    SCOPED_TRACE(fmt::format("numPrefetchSplit {}", numPrefetchSplit));
    asyncDataCache_->clear();
    auto filePath = TempFilePath::create();
    writeToFile(filePath->getPath(), vectors);

    auto op =
        PlanBuilder()
            .tableScan(rowType)
            .partialAggregation(
                {"c5"}, {"max(c0)", "sum(c1)", "sum(c2)", "sum(c3)", "sum(c4)"})
            .planNode();

    auto task =
        AssertQueryBuilder(op, duckDbQueryRunner_)
            .config(
                core::QueryConfig::kMaxSplitPreloadPerDriver,
                std::to_string(numPrefetchSplit))
            .splits(makeHiveConnectorSplits({filePath}))
            .assertResults(
                "SELECT c5, max(c0), sum(c1), sum(c2), sum(c3), sum(c4) FROM tmp group by c5");
    ensureTaskCompletion(task.get());
    compareOutputs(
        ::testing::UnitTest::GetInstance()->current_test_info()->name(),
        printPlanWithStats(*op, task->taskStats()),
        {{"-- Aggregation\\[1\\]\\[PARTIAL \\[c5\\] a0 := max\\(ROW\\[\"c0\"\\]\\), a1 := sum\\(ROW\\[\"c1\"\\]\\), a2 := sum\\(ROW\\[\"c2\"\\]\\), a3 := sum\\(ROW\\[\"c3\"\\]\\), a4 := sum\\(ROW\\[\"c4\"\\]\\)\\] -> c5:VARCHAR, a0:BIGINT, a1:BIGINT, a2:BIGINT, a3:DOUBLE, a4:DOUBLE"},
         {"   Output: .+, Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
         {"  -- TableScan\\[0\\]\\[table: hive_table\\] -> c0:BIGINT, c1:INTEGER, c2:SMALLINT, c3:REAL, c4:DOUBLE, c5:VARCHAR"},
         {"     Input: 10000 rows \\(.+\\), Output: 10000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, Splits: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"}});

    compareOutputs(
        ::testing::UnitTest::GetInstance()->current_test_info()->name(),
        printPlanWithStats(*op, task->taskStats(), true),
        {{"-- Aggregation\\[1\\]\\[PARTIAL \\[c5\\] a0 := max\\(ROW\\[\"c0\"\\]\\), a1 := sum\\(ROW\\[\"c1\"\\]\\), a2 := sum\\(ROW\\[\"c2\"\\]\\), a3 := sum\\(ROW\\[\"c3\"\\]\\), a4 := sum\\(ROW\\[\"c4\"\\]\\)\\] -> c5:VARCHAR, a0:BIGINT, a1:BIGINT, a2:BIGINT, a3:DOUBLE, a4:DOUBLE"},
         {"   Output: .+, Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
         {"      dataSourceLazyCpuNanos\\s+sum: .+, count: .+, min: .+, max: .+"},
         {"      dataSourceLazyInputBytes\\s+sum: .+, count: .+, min: .+, max: .+"},
         {"      dataSourceLazyWallNanos\\s+sum: .+, count: .+, min: .+, max: .+"},
         {"      distinctKey0\\s+sum: .+, count: 1, min: .+, max: .+"},
         {"      hashtable.capacity\\s+sum: (?:1273|1252), count: 1, min: (?:1273|1252), max: (?:1273|1252), avg: (?:1273|1252)"},
         {"      hashtable.numDistinct\\s+sum: (?:849|835), count: 1, min: (?:849|835), max: (?:849|835), avg: (?:849|835)"},
         {"      hashtable.numRehashes\\s+sum: 1, count: 1, min: 1, max: 1, avg: 1"},
         {"      hashtable.numTombstones\\s+sum: 0, count: 1, min: 0, max: 0, avg: 0"},
         {"      loadedToValueHook\\s+sum: 50000, count: 5, min: 10000, max: 10000, avg: 10000"},
         {"      runningAddInputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
         {"      runningFinishWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
         {"      runningGetOutputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
         {"  -- TableScan\\[0\\]\\[table: hive_table\\] -> c0:BIGINT, c1:INTEGER, c2:SMALLINT, c3:REAL, c4:DOUBLE, c5:VARCHAR"},
         {"     Input: 10000 rows \\(.+\\), Output: 10000 rows \\(.+\\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, Splits: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
         {"        dataSourceAddSplitWallNanos[ ]* sum: .+, count: 1, min: .+, max: .+"},
         {"        dataSourceReadWallNanos[ ]* sum: .+, count: 1, min: .+, max: .+"},
         {"        footerBufferOverread[ ]* sum: .+, count: 1, min: .+, max: .+"},
         {"        ioWaitWallNanos      [ ]* sum: .+, count: .+ min: .+, max: .+"},
         {"        maxSingleIoWaitWallNanos[ ]*sum: .+, count: 1, min: .+, max: .+"},
         {"        numPrefetch      [ ]* sum: .+, count: .+, min: .+, max: .+"},
         {"        numRamRead       [ ]* sum: 7, count: 1, min: 7, max: 7, avg: 7"},
         {"        numStorageRead   [ ]* sum: .+, count: 1, min: .+, max: .+"},
         {"        numStripes[ ]* sum: .+, count: 1, min: .+, max: .+"},
         {"        overreadBytes[ ]* sum: 0B, count: 1, min: 0B, max: 0B, avg: 0B"},

         {"        prefetchBytes    [ ]* sum: .+, count: 1, min: .+, max: .+"},
         {"        processedSplits  [ ]* sum: 1, count: 1, min: 1, max: 1, avg: 1"},
         {"        processedStrides [ ]* sum: 1, count: 1, min: 1, max: 1, avg: 1"},
         {"        preloadedSplits[ ]+sum: .+, count: .+, min: .+, max: .+",
          true},
         {"        ramReadBytes     [ ]* sum: .+, count: 1, min: .+, max: .+"},
         {"        readyPreloadedSplits[ ]+sum: .+, count: .+, min: .+, max: .+",
          true},
         {"        runningAddInputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
         {"        runningFinishWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
         {"        runningGetOutputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
         {"        storageReadBytes [ ]* sum: .+, count: 1, min: .+, max: .+"},
         {"        totalRemainingFilterTime\\s+sum: .+, count: .+, min: .+, max: .+"},
         {"        totalScanTime    [ ]* sum: .+, count: .+, min: .+, max: .+"}});
  }
}

TEST_F(PrintPlanWithStatsTest, tableWriterWithTableScan) {
  RowTypePtr rowType{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), INTEGER(), SMALLINT(), REAL(), DOUBLE(), VARCHAR()})};
  auto vectors = makeVectors(rowType, 10, 10);

  const auto filePath = TempFilePath::create();
  writeToFile(filePath->getPath(), vectors);
  const auto writeDir = TempDirectoryPath::create();

  auto writePlan = PlanBuilder()
                       .tableScan(rowType)
                       .tableWrite(writeDir->getPath())
                       .planNode();

  std::shared_ptr<exec::Task> task;
  AssertQueryBuilder(writePlan)
      .splits(makeHiveConnectorSplits({filePath}))
      .copyResults(pool(), task);
  ensureTaskCompletion(task.get());
  compareOutputs(
      ::testing::UnitTest::GetInstance()->current_test_info()->name(),
      printPlanWithStats(*writePlan, task->taskStats()),
      {{R"(-- TableWrite\[1\]\[.+InsertTableHandle .+)"},
       {"   Output: .+, Physical written output: .+, Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {R"(  -- TableScan\[0\]\[table: hive_table\] -> c0:BIGINT, c1:INTEGER, c2:SMALLINT, c3:REAL, c4:DOUBLE, c5:VARCHAR)"},
       {R"(     Input: 100 rows \(.+\), Output: 100 rows \(.+\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, Splits: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+))"}});

  compareOutputs(
      ::testing::UnitTest::GetInstance()->current_test_info()->name(),
      printPlanWithStats(*writePlan, task->taskStats(), true),
      {{R"(-- TableWrite\[1\]\[.+InsertTableHandle .+)"},
       {"   Output: .+, Physical written output: .+, Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
       {"      dataSourceLazyCpuNanos\\s+sum: .+, count: .+, min: .+, max: .+"},
       {"      dataSourceLazyInputBytes\\s+sum: .+, count: .+, min: .+, max: .+"},
       {"      dataSourceLazyWallNanos\\s+sum: .+, count: .+, min: .+, max: .+"},
       {"      numWrittenFiles\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"      runningAddInputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"      runningFinishWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"      runningGetOutputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"      runningWallNanos\\s+sum: .+, count: 1, min: .+, max: .+, avg: .+"},
       {"      stripeSize\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"      writeIOWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {R"(  -- TableScan\[0\]\[table: hive_table\] -> c0:BIGINT, c1:INTEGER, c2:SMALLINT, c3:REAL, c4:DOUBLE, c5:VARCHAR)"},
       {R"(     Input: 100 rows \(.+\), Output: 100 rows \(.+\), Cpu time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, Threads: 1, Splits: 1, CPU breakdown: B/I/O/F (.+/.+/.+/.+))"},
       {"        dataSourceAddSplitWallNanos[ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"        dataSourceReadWallNanos[ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"        footerBufferOverread[ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"        ioWaitWallNanos      [ ]* sum: .+, count: .+ min: .+, max: .+"},
       {"        maxSingleIoWaitWallNanos[ ]*sum: .+, count: 1, min: .+, max: .+"},
       {"        numPrefetch      [ ]* sum: .+, count: .+, min: .+, max: .+"},
       {"        numRamRead       [ ]* sum: 7, count: 1, min: 7, max: 7, avg: 7"},
       {"        numStorageRead   [ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"        numStripes[ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"        overreadBytes[ ]* sum: 0B, count: 1, min: 0B, max: 0B, avg: 0B"},

       {"        prefetchBytes    [ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"        processedSplits  [ ]* sum: 1, count: 1, min: 1, max: 1, avg: 1"},
       {"        processedStrides [ ]* sum: 1, count: 1, min: 1, max: 1, avg: 1"},
       {"        preloadedSplits[ ]+sum: .+, count: .+, min: .+, max: .+",
        true},
       {"        ramReadBytes     [ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"        readyPreloadedSplits[ ]+sum: .+, count: .+, min: .+, max: .+",
        true},
       {"        runningAddInputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"        runningFinishWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"        runningGetOutputWallNanos\\s+sum: .+, count: 1, min: .+, max: .+"},
       {"        storageReadBytes [ ]* sum: .+, count: 1, min: .+, max: .+"},
       {"        totalRemainingFilterTime\\s+sum: .+, count: .+, min: .+, max: .+"},
       {"        totalScanTime    [ ]* sum: .+, count: .+, min: .+, max: .+"}});
}

TEST_F(PrintPlanWithStatsTest, taskAPI) {
  // Test various task states.
  auto checkOutput = [](exec::Task* task) {
    compareOutputs(
        ::testing::UnitTest::GetInstance()->current_test_info()->name(),
        task->printPlanWithStats(),
        {{"-- Aggregation\\[1\\]\\[SINGLE \\[c0\\] a0 := sum\\(ROW\\[\"c1\"\\]\\)\\] -> c0:BIGINT, a0:BIGINT"},
         {"   Output: .+, Cpu time: .+, Wall time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"},
         {"  -- TableScan\\[0\\]\\[table: hive_table\\] -> c0:BIGINT, c1:BIGINT"},
         {"     Input: .+, Output: .+, Cpu time: .+, Wall time: .+, Blocked wall time: .+, Peak memory: .+, Memory allocations: .+, CPU breakdown: B/I/O/F (.+/.+/.+/.+)"}});
  };

  const auto data = makeRowVector({
      makeFlatVector<int64_t>(50, folly::identity),
      makeFlatVector<int64_t>(50, folly::identity),
  });

  const auto plan = PlanBuilder()
                        .tableScan(asRowType(data->type()))
                        .singleAggregation({"c0"}, {"sum(c1)"}, {})
                        .planFragment();

  auto task = exec::Task::create(
      "task",
      std::move(plan),
      0,
      core::QueryCtx::create(driverExecutor_.get()),
      exec::Task::ExecutionMode::kParallel);
  checkOutput(task.get());

  task->start(4, 1);
  checkOutput(task.get());

  task->requestAbort();
  ASSERT_TRUE(waitForTaskAborted(task.get()));
  checkOutput(task.get());
}
