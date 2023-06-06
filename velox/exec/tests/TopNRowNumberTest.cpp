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
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::exec {

namespace {

class TopNRowNumberTest : public OperatorTestBase {};

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
  const vector_size_t size = 10'000;
  auto data = makeRowVector({
      // Partitioning key.
      makeFlatVector<int64_t>(size, [](auto row) { return row % 7; }),
      // Sorting key.
      makeFlatVector<int64_t>(size, [](auto row) { return (size - row) * 10; }),
      // Data.
      makeFlatVector<int64_t>(size, [](auto row) { return row; }),
  });

  createDuckDbTable({data});

  auto testLimit = [&](auto limit) {
    auto plan = PlanBuilder()
                    .values({data})
                    .topNRowNumber({"c0"}, {"c1"}, limit, true)
                    .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(fmt::format(
            "SELECT * FROM (SELECT *, row_number() over (partition by c0 order by c1) as rn FROM tmp) "
            " WHERE rn <= {}",
            limit));

    // No partitioning keys.
    plan = PlanBuilder()
               .values({data})
               .topNRowNumber({}, {"c1"}, limit, true)
               .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(core::QueryConfig::kPreferredOutputBatchBytes, "1024")
        .assertResults(fmt::format(
            "SELECT * FROM (SELECT *, row_number() over (order by c1) as rn FROM tmp) "
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
  auto data = makeRowVector({
      // Partitioning key.
      makeFlatVector<int64_t>(
          size, [](auto row) { return row / 2; }, nullEvery(7)),
      // Sorting key.
      makeFlatVector<int64_t>(
          size,
          [](auto row) { return (size - row) * 10; },
          [](auto row) { return row == 123; }),
      // Data.
      makeFlatVector<int64_t>(
          size, [](auto row) { return row; }, nullEvery(11)),
  });

  createDuckDbTable({data});

  auto testLimit = [&](auto limit) {
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
  };

  testLimit(1);
  testLimit(2);
  testLimit(100);
}

} // namespace
} // namespace facebook::velox::exec
