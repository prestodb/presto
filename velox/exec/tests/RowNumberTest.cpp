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
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec::test {

class RowNumberTest : public OperatorTestBase {};

TEST_F(RowNumberTest, basic) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 1, 2, 1, 2, 1}),
      makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7}),
  });

  createDuckDbTable({data});

  // No limit.
  auto plan = PlanBuilder().values({data}).rowNumber({"c0"}).planNode();
  assertQuery(plan, "SELECT *, row_number() over (partition by c0) FROM tmp");

  auto testLimit = [&](int32_t limit) {
    auto plan =
        PlanBuilder().values({data}).rowNumber({"c0"}, limit).planNode();
    assertQuery(
        plan,
        fmt::format(
            "SELECT * FROM (SELECT *, row_number() over (partition by c0) as rn FROM tmp) "
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

  // No limit.
  auto plan = PlanBuilder().values({data, data}).rowNumber({}).planNode();
  assertQuery(plan, "SELECT *, row_number() over () FROM tmp");

  auto testLimit = [&](int32_t limit) {
    auto plan =
        PlanBuilder().values({data, data}).rowNumber({}, limit).planNode();
    assertQuery(
        plan,
        fmt::format(
            "SELECT * FROM (SELECT *, row_number() over () as rn FROM tmp) "
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

  // No limit.
  auto plan = PlanBuilder().values({data, data}).rowNumber({"c0"}).planNode();
  assertQuery(plan, "SELECT *, row_number() over (partition by c0) FROM tmp");

  auto testLimit = [&](int32_t limit) {
    auto plan =
        PlanBuilder().values({data, data}).rowNumber({"c0"}, limit).planNode();
    assertQuery(
        plan,
        fmt::format(
            "SELECT * FROM (SELECT *, row_number() over (partition by c0) as rn FROM tmp) "
            "WHERE rn <= {}",
            limit));
  };

  testLimit(1);
  testLimit(100);
  testLimit(2'000);
  testLimit(5'000);
}

} // namespace facebook::velox::exec::test
