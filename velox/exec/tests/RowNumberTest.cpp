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
#include "velox/common/file/FileSystems.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace facebook::velox::exec::test {

class RowNumberTest : public OperatorTestBase {
 protected:
  RowNumberTest() {
    filesystems::registerLocalFileSystem();
  }
};

TEST_F(RowNumberTest, spill) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>(1'000, [](auto row) { return row; }),
  });

  core::PlanNodeId rowNumberId;
  auto plan = PlanBuilder()
                  .values({data, data, data})
                  .rowNumber({"c0"})
                  .capturePlanNodeId(rowNumberId)
                  .singleAggregation({"row_number"}, {"count(1)"})
                  .planNode();

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<int64_t>({1'000, 1'000, 1'000}),
  });

  auto spillDirectory = exec::test::TempDirectoryPath::create();

  auto task = AssertQueryBuilder(plan)
                  .config(core::QueryConfig::kTestingSpillPct, "100")
                  .config(core::QueryConfig::kSpillEnabled, "true")
                  .config(core::QueryConfig::kRowNumberSpillEnabled, "true")
                  .spillDirectory(spillDirectory->path)
                  .assertResults({expected});

  auto taskStats = exec::toPlanStats(task->taskStats());
  const auto& stats = taskStats.at(rowNumberId);

  ASSERT_GT(stats.spilledBytes, 0);
  ASSERT_GT(stats.spilledRows, 0);
  ASSERT_GT(stats.spilledFiles, 0);
  ASSERT_GT(stats.spilledPartitions, 0);
} // namespace facebook::velox::exec::test

TEST_F(RowNumberTest, basic) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7}),
  });

  createDuckDbTable({data});

  // No limit, emit row numbers.
  auto plan = PlanBuilder().values({data}).rowNumber({"c0"}).planNode();
  assertQuery(plan, "SELECT *, row_number() over (partition by c0) FROM tmp");

  // No limit, don't emit row numbers.
  plan = PlanBuilder()
             .values({data})
             .rowNumber({"c0"}, std::nullopt, false)
             .planNode();
  assertQuery(
      plan,
      "SELECT c0, c1 FROM (SELECT *, row_number() over (partition by c0) as rn FROM tmp)");

  auto testLimit = [&](int32_t limit) {
    // Limit, emit row numbers.
    auto plan =
        PlanBuilder().values({data}).rowNumber({"c0"}, limit).planNode();
    assertQuery(
        plan,
        fmt::format(
            "SELECT * FROM (SELECT *, row_number() over (partition by c0) as rn FROM tmp) "
            "WHERE rn <= {}",
            limit));

    // Limit, don't emit row numbers.
    plan =
        PlanBuilder().values({data}).rowNumber({"c0"}, limit, false).planNode();
    assertQuery(
        plan,
        fmt::format(
            "SELECT c0, c1 FROM (SELECT *, row_number() over (partition by c0) as rn FROM tmp) "
            "WHERE rn <= {}",
            limit));
  };

  testLimit(1);
  testLimit(2);
  testLimit(5);
}

TEST_F(RowNumberTest, noPartitionKeys) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(1'000, [](auto row) { return row; }),
  });

  createDuckDbTable({data, data});

  // No limit, emit row numbers.
  auto plan = PlanBuilder().values({data, data}).rowNumber({}).planNode();
  assertQuery(plan, "SELECT *, row_number() over () FROM tmp");

  // No limit, don't emit row numbers.
  plan = PlanBuilder()
             .values({data, data})
             .rowNumber({}, std::nullopt, false)
             .planNode();
  assertQuery(
      plan, "SELECT c0 FROM (SELECT *, row_number() over () as rn FROM tmp)");

  auto testLimit = [&](int32_t limit) {
    // Emit row numbers.
    auto plan =
        PlanBuilder().values({data, data}).rowNumber({}, limit).planNode();
    assertQuery(
        plan,
        fmt::format(
            "SELECT * FROM (SELECT *, row_number() over () as rn FROM tmp) "
            "WHERE rn <= {}",
            limit));

    // Don't emit row numbers.
    plan = PlanBuilder()
               .values({data, data})
               .rowNumber({}, limit, false)
               .planNode();
    assertQuery(
        plan,
        fmt::format(
            "SELECT c0 FROM (SELECT *, row_number() over () as rn FROM tmp) "
            "WHERE rn <= {}",
            limit));
  };

  testLimit(1);
  testLimit(50);
}

TEST_F(RowNumberTest, largeInput) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>(10'000, [](auto row) { return row % 7; }),
      makeFlatVector<int64_t>(10'000, [](auto row) { return row; }),
  });

  createDuckDbTable({data, data});

  // No limit, emit row numbers.
  auto plan = PlanBuilder().values({data, data}).rowNumber({"c0"}).planNode();
  assertQuery(plan, "SELECT *, row_number() over (partition by c0) FROM tmp");

  // No limit, don't emit row numbers.
  plan = PlanBuilder()
             .values({data, data})
             .rowNumber({"c0"}, std::nullopt, false)
             .planNode();
  assertQuery(
      plan,
      "SELECT c0, c1 FROM (SELECT *, row_number() over (partition by c0) as rn FROM tmp)");

  auto testLimit = [&](int32_t limit) {
    // Emit row numbers.
    auto plan =
        PlanBuilder().values({data, data}).rowNumber({"c0"}, limit).planNode();
    assertQuery(
        plan,
        fmt::format(
            "SELECT * FROM (SELECT *, row_number() over (partition by c0) as rn FROM tmp) "
            "WHERE rn <= {}",
            limit));

    // Don't emit row numbers.
    plan = PlanBuilder()
               .values({data, data})
               .rowNumber({"c0"}, limit, false)
               .planNode();
    assertQuery(
        plan,
        fmt::format(
            "SELECT c0, c1 FROM (SELECT *, row_number() over (partition by c0) as rn FROM tmp) "
            "WHERE rn <= {}",
            limit));
  };

  testLimit(1);
  testLimit(100);
  testLimit(2'000);
  testLimit(5'000);
}

} // namespace facebook::velox::exec::test
