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
#include "velox/common/file/FileSystems.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::exec {

namespace {

class TopNRowNumberTest : public OperatorTestBase {
 protected:
  TopNRowNumberTest() {
    filesystems::registerLocalFileSystem();
  }
};

TEST_F(TopNRowNumberTest, basic) {
  auto data = makeRowVector({
      // Partitioning key.
      makeFlatVector<int64_t>({1, 1, 2, 2, 1, 2, 1}),
      // Sorting key.
      makeFlatVector<int64_t>({77, 66, 55, 44, 33, 22, 11}),
      // Data.
      makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60, 70}),
  });

  createDuckDbTable({data});

  auto testLimit = [&](auto limit) {
    // Emit row numbers.
    auto plan = PlanBuilder()
                    .values({data})
                    .topNRowNumber({"c0"}, {"c1"}, limit, true)
                    .planNode();
    assertQuery(
        plan,
        fmt::format(
            "SELECT * FROM (SELECT *, row_number() over (partition by c0 order by c1) as rn FROM tmp) "
            " WHERE rn <= {}",
            limit));

    // Do not emit row numbers.
    plan = PlanBuilder()
               .values({data})
               .topNRowNumber({"c0"}, {"c1"}, limit, false)
               .planNode();

    assertQuery(
        plan,
        fmt::format(
            "SELECT c0, c1, c2 FROM (SELECT *, row_number() over (partition by c0 order by c1) as rn FROM tmp) "
            " WHERE rn <= {}",
            limit));

    // No partitioning keys.
    plan = PlanBuilder()
               .values({data})
               .topNRowNumber({}, {"c1"}, limit, true)
               .planNode();
    assertQuery(
        plan,
        fmt::format(
            "SELECT * FROM (SELECT *, row_number() over (order by c1) as rn FROM tmp) "
            " WHERE rn <= {}",
            limit));
  };

  testLimit(1);
  testLimit(2);
  testLimit(3);
  testLimit(5);
}

TEST_F(TopNRowNumberTest, largeOutput) {
  // Make 10 vectors. Use different types for partitioning key, sorting key and
  // data. Use order of columns different from partitioning keys, followed by
  // sorting keys, followed by data.
  const vector_size_t size = 10'000;
  auto data = split(
      makeRowVector(
          {"d", "p", "s"},
          {
              // Data.
              makeFlatVector<float>(size, [](auto row) { return row; }),
              // Partitioning key.
              makeFlatVector<int16_t>(size, [](auto row) { return row % 7; }),
              // Sorting key.
              makeFlatVector<int32_t>(
                  size, [](auto row) { return (size - row) * 10; }),
          }),
      10);

  createDuckDbTable(data);

  auto spillDirectory = exec::test::TempDirectoryPath::create();

  auto testLimit = [&](auto limit) {
    SCOPED_TRACE(fmt::format("Limit: {}", limit));
    core::PlanNodeId topNRowNumberId;
    auto plan = PlanBuilder()
                    .values(data)
                    .topNRowNumber({"p"}, {"s"}, limit, true)
                    .capturePlanNodeId(topNRowNumberId)
                    .planNode();

    auto sql = fmt::format(
        "SELECT * FROM (SELECT *, row_number() over (partition by p order by s) as rn FROM tmp) "
        " WHERE rn <= {}",
        limit);
    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(sql);

    // Spilling.
    {
      TestScopedSpillInjection scopedSpillInjection(100);
      auto task =
          AssertQueryBuilder(plan, duckDbQueryRunner_)
              .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
              .config(core::QueryConfig::kSpillEnabled, "true")
              .config(core::QueryConfig::kTopNRowNumberSpillEnabled, "true")
              .spillDirectory(spillDirectory->getPath())
              .assertResults(sql);

      auto taskStats = exec::toPlanStats(task->taskStats());
      const auto& stats = taskStats.at(topNRowNumberId);

      ASSERT_GT(stats.spilledBytes, 0);
      ASSERT_GT(stats.spilledRows, 0);
      ASSERT_GT(stats.spilledFiles, 0);
      ASSERT_GT(stats.spilledPartitions, 0);
    }

    // No partitioning keys.
    plan = PlanBuilder()
               .values(data)
               .topNRowNumber({}, {"s"}, limit, true)
               .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(fmt::format(
            "SELECT * FROM (SELECT *, row_number() over (order by s) as rn FROM tmp) "
            " WHERE rn <= {}",
            limit));
  };

  testLimit(1);
  testLimit(5);
  testLimit(100);
  testLimit(1000);
  testLimit(2000);
}

TEST_F(TopNRowNumberTest, manyPartitions) {
  const vector_size_t size = 10'000;
  auto data = split(
      makeRowVector(
          {"d", "s", "p"},
          {
              // Data.
              makeFlatVector<int64_t>(
                  size, [](auto row) { return row; }, nullEvery(11)),
              // Sorting key.
              makeFlatVector<int64_t>(
                  size,
                  [](auto row) { return (size - row) * 10; },
                  [](auto row) { return row == 123; }),
              // Partitioning key. Make sure to spread rows from the same
              // partition across multiple batches to trigger de-dup logic when
              // reading back spilled data.
              makeFlatVector<int64_t>(
                  size, [](auto row) { return row % 5'000; }, nullEvery(7)),
          }),
      10);

  createDuckDbTable(data);

  auto spillDirectory = exec::test::TempDirectoryPath::create();

  auto testLimit = [&](auto limit, size_t outputBatchBytes = 1024) {
    SCOPED_TRACE(fmt::format("Limit: {}", limit));
    core::PlanNodeId topNRowNumberId;
    auto plan = PlanBuilder()
                    .values(data)
                    .topNRowNumber({"p"}, {"s"}, limit, true)
                    .capturePlanNodeId(topNRowNumberId)
                    .planNode();

    auto sql = fmt::format(
        "SELECT * FROM (SELECT *, row_number() over (partition by p order by s) as rn FROM tmp) "
        " WHERE rn <= {}",
        limit);
    assertQuery(plan, sql);

    // Spilling.
    {
      TestScopedSpillInjection scopedSpillInjection(100);
      auto task =
          AssertQueryBuilder(plan, duckDbQueryRunner_)
              .config(
                  core::QueryConfig::kPreferredOutputBatchBytes,
                  fmt::format("{}", outputBatchBytes))
              .config(core::QueryConfig::kSpillEnabled, "true")
              .config(core::QueryConfig::kTopNRowNumberSpillEnabled, "true")
              .spillDirectory(spillDirectory->getPath())
              .assertResults(sql);

      auto taskStats = exec::toPlanStats(task->taskStats());
      const auto& stats = taskStats.at(topNRowNumberId);

      ASSERT_GT(stats.spilledBytes, 0);
      ASSERT_GT(stats.spilledRows, 0);
      ASSERT_GT(stats.spilledFiles, 0);
      ASSERT_GT(stats.spilledPartitions, 0);
    }
  };

  testLimit(1);
  testLimit(2);
  testLimit(100);

  testLimit(1, 1);
}

TEST_F(TopNRowNumberTest, abandonPartialEarly) {
  auto data = makeRowVector(
      {"p", "s"},
      {
          makeFlatVector<int64_t>(1'000, [](auto row) { return row % 10; }),
          makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
      });

  createDuckDbTable({data});

  core::PlanNodeId topNRowNumberId;
  auto runPlan = [&](int32_t minRows) {
    auto plan = PlanBuilder()
                    .values(split(data, 10))
                    .topNRowNumber({"p"}, {"s"}, 99, false)
                    .capturePlanNodeId(topNRowNumberId)
                    .topNRowNumber({"p"}, {"s"}, 99, true)
                    .planNode();
    auto task =
        AssertQueryBuilder(plan, duckDbQueryRunner_)
            .config(
                core::QueryConfig::kAbandonPartialTopNRowNumberMinRows,
                fmt::format("{}", minRows))
            .config(core::QueryConfig::kAbandonPartialTopNRowNumberMinPct, "80")
            .assertResults(
                "SELECT * FROM (SELECT *, row_number() over (partition by p order by s) as rn FROM tmp) "
                "WHERE rn <= 99");

    return exec::toPlanStats(task->taskStats());
  };

  // Partial operator is abandoned after 2 input batches.
  {
    auto taskStats = runPlan(100);
    const auto& stats = taskStats.at(topNRowNumberId);
    ASSERT_EQ(stats.outputRows, 1'000);
    ASSERT_EQ(stats.customStats.at("abandonedPartial").sum, 1);
  }

  // Partial operator continues for all of input.
  {
    auto taskStats = runPlan(100'000);
    const auto& stats = taskStats.at(topNRowNumberId);
    ASSERT_EQ(stats.outputRows, 990);
    ASSERT_EQ(stats.customStats.count("abandonedPartial"), 0);
  }
}

TEST_F(TopNRowNumberTest, planNodeValidation) {
  auto data = makeRowVector(
      ROW({"a", "b", "c", "d", "e"},
          {
              BIGINT(),
              BIGINT(),
              BIGINT(),
              BIGINT(),
              BIGINT(),
          }),
      10);

  auto plan = [&](const std::vector<std::string>& partitionKeys,
                  const std::vector<std::string>& sortingKeys,
                  int32_t limit = 10) {
    PlanBuilder()
        .values({data})
        .topNRowNumber(partitionKeys, sortingKeys, limit, true)
        .planNode();
  };

  VELOX_ASSERT_THROW(
      plan({"a", "a"}, {"b"}),
      "Partitioning keys must be unique. Found duplicate key: a");

  VELOX_ASSERT_THROW(
      plan({"a", "b"}, {"c", "d", "c"}),
      "Sorting keys must be unique and not overlap with partitioning keys. Found duplicate key: c");

  VELOX_ASSERT_THROW(
      plan({"a", "b"}, {"c", "b"}),
      "Sorting keys must be unique and not overlap with partitioning keys. Found duplicate key: b");

  VELOX_ASSERT_THROW(
      plan({"a", "b"}, {}), "Number of sorting keys must be greater than zero");

  VELOX_ASSERT_THROW(
      plan({"a", "b"}, {"c"}, -5), "Limit must be greater than zero");

  VELOX_ASSERT_THROW(
      plan({"a", "b"}, {"c"}, 0), "Limit must be greater than zero");
}

TEST_F(TopNRowNumberTest, maxSpillBytes) {
  const auto rowType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), INTEGER(), VARCHAR()});
  const auto vectors = createVectors(rowType, 1024, 15 << 20);
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .values(vectors)
                  .topNRowNumber({"c0"}, {"c1"}, 100, true)
                  .planNode();
  struct {
    int32_t maxSpilledBytes;
    bool expectedExceedLimit;
    std::string debugString() const {
      return fmt::format("maxSpilledBytes {}", maxSpilledBytes);
    }
  } testSettings[] = {{1 << 30, false}, {13 << 20, true}, {0, false}};

  auto spillDirectory = exec::test::TempDirectoryPath::create();
  auto queryCtx = core::QueryCtx::create(executor_.get());

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    try {
      TestScopedSpillInjection scopedSpillInjection(100);
      AssertQueryBuilder(plan)
          .spillDirectory(spillDirectory->getPath())
          .queryCtx(queryCtx)
          .config(core::QueryConfig::kSpillEnabled, "true")
          .config(core::QueryConfig::kTopNRowNumberSpillEnabled, "true")
          .config(
              core::QueryConfig::kMaxSpillBytes,
              std::to_string(testData.maxSpilledBytes))
          .copyResults(pool_.get());
      ASSERT_FALSE(testData.expectedExceedLimit);
    } catch (const VeloxRuntimeError& e) {
      ASSERT_TRUE(testData.expectedExceedLimit);
      ASSERT_NE(
          e.message().find(
              "Query exceeded per-query local spill limit of 13.00MB"),
          std::string::npos);
      ASSERT_EQ(
          e.errorCode(), facebook::velox::error_code::kSpillLimitExceeded);
    }
  }
}

} // namespace
} // namespace facebook::velox::exec
