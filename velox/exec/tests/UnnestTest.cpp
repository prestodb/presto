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

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class UnnestTest : public OperatorTestBase {};

TEST_F(UnnestTest, basicArray) {
  auto vector = makeRowVector({
      makeFlatVector<int64_t>(100, [](auto row) { return row; }),
      makeArrayVector<int32_t>(
          100,
          [](auto row) { return row % 5 + 1; },
          [](auto row, auto index) { return index * (row % 3); },
          nullEvery(7)),
  });

  createDuckDbTable({vector});

  // TODO Add tests with empty arrays. This requires better support in DuckDB.

  auto op = PlanBuilder().values({vector}).unnest({"c0"}, {"c1"}).planNode();
  assertQuery(op, "SELECT c0, UNNEST(c1) FROM tmp WHERE c0 % 7 > 0");
}

TEST_F(UnnestTest, basicMap) {
  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeMapVector<int64_t, double>(
           100,
           [](auto /* row */) { return 2; },
           [](auto row) { return row % 2; },
           [](auto row) { return row % 2 + 1; })});
  auto op = PlanBuilder().values({vector}).unnest({"c0"}, {"c1"}).planNode();
  // DuckDB doesn't support Unnest from MAP column. Hence,using 2 separate array
  // columns with the keys and values part of the MAP to validate.
  auto duckDbVector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeArrayVector<int32_t>(
           100,
           [](auto /* row */) { return 2; },
           [](auto /* row */, auto index) { return index; }),
       makeArrayVector<int32_t>(
           100,
           [](auto /* row */) { return 2; },
           [](auto /* row */, auto index) { return index + 1; })});
  createDuckDbTable({duckDbVector});
  assertQuery(op, "SELECT c0, UNNEST(c1), UNNEST(c2) FROM tmp");
}

TEST_F(UnnestTest, allEmptyOrNullArrays) {
  auto vector = makeRowVector({
      makeFlatVector<int64_t>(100, [](auto row) { return row; }),
      makeArrayVector<int32_t>(
          100,
          [](auto /* row */) { return 0; },
          [](auto /* row */, auto index) { return index; },
          nullEvery(5)),
  });

  auto op = PlanBuilder().values({vector}).unnest({"c0"}, {"c1"}).planNode();
  assertQueryReturnsEmptyResult(op);
}

TEST_F(UnnestTest, allEmptyOrNullMaps) {
  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeMapVector<int64_t, double>(
           100,
           [](auto /* row */) { return 0; },
           [](auto /* row */) { return 0; },
           [](auto /* row */) { return 0; },
           nullEvery(5))});
  auto op = PlanBuilder().values({vector}).unnest({"c0"}, {"c1"}).planNode();
  assertQueryReturnsEmptyResult(op);
}
