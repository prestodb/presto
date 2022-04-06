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

TEST_F(UnnestTest, arrayWithOrdinality) {
  auto array = vectorMaker_.arrayVectorNullable<int32_t>(
      {{{1, 2, std::nullopt, 4}},
       std::nullopt,
       {{5, 6}},
       {{}},
       {{{{std::nullopt}}}},
       {{7, 8, 9}}});
  auto vector = makeRowVector(
      {makeNullableFlatVector<double>({1.1, 2.2, 3.3, 4.4, 5.5, std::nullopt}),
       array});

  auto op = PlanBuilder()
                .values({vector})
                .unnest({"c0"}, {"c1"}, "ordinal")
                .planNode();

  auto expected = makeRowVector(
      {makeNullableFlatVector<double>(
           {1.1,
            1.1,
            1.1,
            1.1,
            3.3,
            3.3,
            5.5,
            std::nullopt,
            std::nullopt,
            std::nullopt}),
       makeNullableFlatVector<int32_t>(
           {1, 2, std::nullopt, 4, 5, 6, std::nullopt, 7, 8, 9}),
       makeNullableFlatVector<int64_t>({1, 2, 3, 4, 1, 2, 1, 1, 2, 3})});
  assertQuery(op, expected);

  // Test with array wrapped in dictionary.
  auto reversedIndices = makeIndicesInReverse(6);
  auto vectorInDict = makeRowVector(
      {makeNullableFlatVector<double>({1.1, 2.2, 3.3, 4.4, 5.5, std::nullopt}),
       wrapInDictionary(reversedIndices, 6, array)});
  op = PlanBuilder()
           .values({vectorInDict})
           .unnest({"c0"}, {"c1"}, "ordinal")
           .planNode();

  auto expectedInDict = makeRowVector(
      {makeNullableFlatVector<double>(
           {1.1,
            1.1,
            1.1,
            2.2,
            4.4,
            4.4,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            std::nullopt}),
       makeNullableFlatVector<int32_t>(
           {7, 8, 9, std::nullopt, 5, 6, 1, 2, std::nullopt, 4}),
       makeNullableFlatVector<int64_t>({1, 2, 3, 1, 1, 2, 1, 2, 3, 4})});
  assertQuery(op, expectedInDict);
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

TEST_F(UnnestTest, mapWithOrdinality) {
  auto map = makeMapVector<int32_t, double>(
      {{{1, 1.1}, {2, std::nullopt}},
       {{3, 3.3}, {4, 4.4}, {5, 5.5}},
       {{6, std::nullopt}}});
  auto vector =
      makeRowVector({makeNullableFlatVector<int32_t>({1, 2, 3}), map});

  auto op = PlanBuilder()
                .values({vector})
                .unnest({"c0"}, {"c1"}, "ordinal")
                .planNode();

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 1, 2, 2, 2, 3}),
       makeNullableFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
       makeNullableFlatVector<double>(
           {1.1, std::nullopt, 3.3, 4.4, 5.5, std::nullopt}),
       makeNullableFlatVector<int64_t>({1, 2, 1, 2, 3, 1})});
  assertQuery(op, expected);

  // Test with map wrapped in dictionary.
  auto reversedIndices = makeIndicesInReverse(3);
  auto vectorInDict = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 2, 3}),
       wrapInDictionary(reversedIndices, 3, map)});
  op = PlanBuilder()
           .values({vectorInDict})
           .unnest({"c0"}, {"c1"}, "ordinal")
           .planNode();

  auto expectedInDict = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 2, 2, 2, 3, 3}),
       makeNullableFlatVector<int32_t>({6, 3, 4, 5, 1, 2}),
       makeNullableFlatVector<double>(
           {std::nullopt, 3.3, 4.4, 5.5, 1.1, std::nullopt}),
       makeNullableFlatVector<int64_t>({1, 1, 2, 3, 1, 2})});
  assertQuery(op, expectedInDict);
}

TEST_F(UnnestTest, multipleColumns) {
  std::vector<vector_size_t> offsets(100, 0);
  for (int i = 1; i < 100; ++i) {
    offsets[i] = offsets[i - 1] + i % 11 + 1;
  }

  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       vectorMaker_.mapVector<int64_t, double>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4); },
           [](auto row, auto index) { return index * (row % 4) + 1; },
           nullEvery(6)),
       makeArrayVector<int32_t>(
           100,
           [](auto row) { return row % 5 + 1; },
           [](auto row, auto index) { return index * (row % 3); },
           nullEvery(7)),
       makeArrayVector(offsets, makeConstant<int32_t>(7, 700))});

  auto op = PlanBuilder()
                .values({vector})
                .unnest({"c0"}, {"c1", "c2", "c3"})
                .planNode();

  // DuckDB doesn't support Unnest from MAP column. Hence,using 2 separate array
  // columns with the keys and values part of the MAP to validate.
  auto duckDbVector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeArrayVector<int64_t>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4); },
           nullEvery(6)),
       makeArrayVector<double>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4) + 1; },
           nullEvery(6)),
       makeArrayVector<int32_t>(
           100,
           [](auto row) { return row % 5 + 1; },
           [](auto row, auto index) { return index * (row % 3); },
           nullEvery(7)),
       makeArrayVector(offsets, makeConstant<int32_t>(7, 700))});
  createDuckDbTable({duckDbVector});
  assertQuery(
      op, "SELECT c0, UNNEST(c1), UNNEST(c2), UNNEST(c3), UNNEST(c4) FROM tmp");
}

TEST_F(UnnestTest, multipleColumnsWithOrdinality) {
  std::vector<vector_size_t> offsets(100, 0);
  for (int i = 1; i < 100; ++i) {
    offsets[i] = offsets[i - 1] + i % 11 + 1;
  }

  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       vectorMaker_.mapVector<int64_t, double>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4); },
           [](auto row, auto index) { return index * (row % 4) + 1; },
           nullEvery(6)),
       makeArrayVector<int32_t>(
           100,
           [](auto row) { return row % 5 + 1; },
           [](auto row, auto index) { return index * (row % 3); },
           nullEvery(7)),
       makeArrayVector(offsets, makeConstant<int32_t>(7, 700))});

  auto op = PlanBuilder()
                .values({vector})
                .unnest({"c0"}, {"c1", "c2", "c3"}, "ordinal")
                .planNode();

  // DuckDB doesn't support Unnest from MAP column. Hence,using 2 separate array
  // columns with the keys and values part of the MAP to validate.
  auto ordinalitySize = [&](auto row) {
    if (row % 42 == 0) {
      return offsets[row + 1] - offsets[row];
    } else if (row % 7 == 0) {
      return std::max(row % 7 + 1, offsets[row + 1] - offsets[row]);
    } else if (row % 6 == 0) {
      return std::max(row % 5 + 1, offsets[row + 1] - offsets[row]);
    } else {
      return std::max(
          std::max(row % 5, row % 7) + 1,
          (row == 99 ? 700 : offsets[row + 1]) - offsets[row]);
    }
  };

  auto duckDbVector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeArrayVector<int64_t>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4); },
           nullEvery(6)),
       makeArrayVector<double>(
           100,
           [](auto row) { return row % 7 + 1; },
           [](auto row, auto index) { return index * (row % 4) + 1; },
           nullEvery(6)),
       makeArrayVector<int32_t>(
           100,
           [](auto row) { return row % 5 + 1; },
           [](auto row, auto index) { return index * (row % 3); },
           nullEvery(7)),
       makeArrayVector(offsets, makeConstant<int32_t>(7, 700)),
       makeArrayVector<int64_t>(
           100, ordinalitySize, [](auto /* row */, auto index) {
             return index + 1;
           })});
  createDuckDbTable({duckDbVector});
  assertQuery(
      op,
      "SELECT c0, UNNEST(c1), UNNEST(c2), UNNEST(c3), UNNEST(c4), UNNEST(c5) FROM tmp");

  // Test with empty arrays and maps.
  vector = makeRowVector(
      {makeNullableFlatVector<double>({1.1, 2.2, std::nullopt, 4.4, 5.5}),
       vectorMaker_.arrayVectorNullable<int32_t>(
           {{{1, 2, std::nullopt, 4}}, std::nullopt, {{5, 6}}, {}, {{7}}}),
       makeMapVector<int32_t, double>(
           {{{1, 1.1}, {2, std::nullopt}},
            {{3, 3.3}, {4, 4.4}, {5, 5.5}},
            {{6, std::nullopt}},
            {},
            {}})});

  op = PlanBuilder()
           .values({vector})
           .unnest({"c0"}, {"c1", "c2"}, "ordinal")
           .planNode();

  auto expected = makeRowVector(
      {makeNullableFlatVector<double>(
           {1.1,
            1.1,
            1.1,
            1.1,
            2.2,
            2.2,
            2.2,
            std::nullopt,
            std::nullopt,
            5.5}),
       makeNullableFlatVector<int32_t>(
           {1,
            2,
            std::nullopt,
            4,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            5,
            6,
            7}),
       makeNullableFlatVector<int32_t>(
           {1,
            2,
            std::nullopt,
            std::nullopt,
            3,
            4,
            5,
            6,
            std::nullopt,
            std::nullopt}),
       makeNullableFlatVector<double>(
           {1.1,
            std::nullopt,
            std::nullopt,
            std::nullopt,
            3.3,
            4.4,
            5.5,
            std::nullopt,
            std::nullopt,
            std::nullopt}),
       makeNullableFlatVector<int64_t>({1, 2, 3, 4, 1, 2, 3, 1, 2, 1})});
  assertQuery(op, expected);
}

TEST_F(UnnestTest, allEmptyOrNullArrays) {
  auto vector = makeRowVector(
      {makeFlatVector<int64_t>(100, [](auto row) { return row; }),
       makeArrayVector<int32_t>(
           100,
           [](auto /* row */) { return 0; },
           [](auto /* row */, auto index) { return index; },
           nullEvery(5)),
       makeArrayVector<int32_t>(
           100,
           [](auto /* row */) { return 0; },
           [](auto /* row */, auto index) { return index; },
           nullEvery(7))});

  auto op =
      PlanBuilder().values({vector}).unnest({"c0"}, {"c1", "c2"}).planNode();
  assertQueryReturnsEmptyResult(op);

  op = PlanBuilder()
           .values({vector})
           .unnest({"c0"}, {"c1", "c2"}, "ordinal")
           .planNode();
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
           nullEvery(5)),
       makeMapVector<int64_t, double>(
           100,
           [](auto /* row */) { return 0; },
           [](auto /* row */) { return 0; },
           [](auto /* row */) { return 0; },
           nullEvery(7))});

  auto op =
      PlanBuilder().values({vector}).unnest({"c0"}, {"c1", "c2"}).planNode();
  assertQueryReturnsEmptyResult(op);

  op = PlanBuilder()
           .values({vector})
           .unnest({"c0"}, {"c1", "c2"}, "ordinal")
           .planNode();
  assertQueryReturnsEmptyResult(op);
}
