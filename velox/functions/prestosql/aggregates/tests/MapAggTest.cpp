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
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

class MapAggTest : public AggregationTestBase {};

TEST_F(MapAggTest, groupBy) {
  vector_size_t num = 10;

  auto vectors = {makeRowVector(
      {makeFlatVector<int32_t>(num, [](vector_size_t row) { return row / 3; }),
       makeFlatVector<int32_t>(num, [](vector_size_t row) { return row; }),
       makeFlatVector<double>(
           num, [](vector_size_t row) { return row + 0.05; })})};

  static std::array<int32_t, 10> keys{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  vector_size_t keyIndex{0};
  vector_size_t valIndex{0};
  auto expectedResult = {makeRowVector(
      {makeFlatVector<int32_t>({0, 1, 2, 3}),
       makeMapVector<int32_t, double>(
           4,
           [&](vector_size_t row) { return (row == 3) ? 1 : 3; },
           [&](vector_size_t row) { return keys[keyIndex++]; },
           [&](vector_size_t row) { return keys[valIndex++] + 0.05; })})};

  testAggregations(vectors, {"c0"}, {"map_agg(c1, c2)"}, expectedResult);
}

TEST_F(MapAggTest, groupByWithNulls) {
  vector_size_t size = 90;

  auto vectors = {makeRowVector(
      {makeFlatVector<int32_t>(size, [](vector_size_t row) { return row / 3; }),
       makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; }),
       makeFlatVector<double>(
           size, [](vector_size_t row) { return row + 0.05; }, nullEvery(7))})};

  auto expectedResult = {makeRowVector(
      {makeFlatVector<int32_t>(30, [](vector_size_t row) { return row; }),
       makeMapVector<int32_t, double>(
           30,
           [](vector_size_t /*row*/) { return 3; },
           [](vector_size_t row) { return row; },
           [](vector_size_t row) { return row + 0.05; },
           nullptr,
           nullEvery(7))})};

  testAggregations(vectors, {"c0"}, {"map_agg(c1, c2)"}, expectedResult);
}

TEST_F(MapAggTest, groupByWithDuplicates) {
  vector_size_t num = 10;
  auto vectors = {makeRowVector(
      {makeFlatVector<int32_t>(num, [](vector_size_t row) { return row / 2; }),
       makeFlatVector<int32_t>(num, [](vector_size_t row) { return row / 2; }),
       makeFlatVector<double>(
           num, [](vector_size_t row) { return row + 0.05; })})};

  auto expectedResult = {makeRowVector(
      {makeFlatVector<int32_t>({0, 1, 2, 3, 4}),
       makeMapVector<int32_t, double>(
           5,
           [&](vector_size_t /*row*/) { return 1; },
           [&](vector_size_t row) { return row; },
           [&](vector_size_t row) { return 2 * row + 0.05; })})};

  testAggregations(vectors, {"c0"}, {"map_agg(c1, c2)"}, expectedResult);
}

TEST_F(MapAggTest, groupByNoData) {
  auto vectors = {makeRowVector(
      {makeFlatVector<int32_t>({}),
       makeFlatVector<int32_t>({}),
       makeFlatVector<int32_t>({})})};

  auto expectedResult = {makeRowVector(
      {makeFlatVector<int32_t>({}), makeMapVector<int32_t, double>({})})};

  testAggregations(vectors, {"c0"}, {"map_agg(c1, c2)"}, expectedResult);
}

TEST_F(MapAggTest, global) {
  vector_size_t num = 10;

  auto vectors = {makeRowVector(
      {makeFlatVector<int32_t>(num, [](vector_size_t row) { return row; }),
       makeFlatVector<double>(
           num, [](vector_size_t row) { return row + 0.05; })})};

  auto expectedResult = {makeRowVector({makeMapVector<int32_t, double>(
      1,
      [&](vector_size_t /*row*/) { return num; },
      [&](vector_size_t row) { return row; },
      [&](vector_size_t row) { return row + 0.05; })})};

  testAggregations(vectors, {}, {"map_agg(c0, c1)"}, expectedResult);
}

TEST_F(MapAggTest, globalWithNulls) {
  vector_size_t size = 10;

  std::vector<RowVectorPtr> vectors = {makeRowVector(
      {makeFlatVector<int32_t>(size, [](vector_size_t row) { return row; }),
       makeFlatVector<double>(
           size, [](vector_size_t row) { return row + 0.05; }, nullEvery(7))})};

  auto expectedResult = {makeRowVector({makeMapVector<int32_t, double>(
      1,
      [&](vector_size_t /*row*/) { return size; },
      [&](vector_size_t row) { return row; },
      [&](vector_size_t row) { return row + 0.05; },
      nullptr,
      nullEvery(7))})};

  testAggregations(vectors, {}, {"map_agg(c0, c1)"}, expectedResult);
}

TEST_F(MapAggTest, globalNoData) {
  auto vectors = {makeRowVector(
      {makeFlatVector<int32_t>({}), makeFlatVector<int32_t>({})})};
  auto expectedResult = {makeRowVector({makeMapVector<int32_t, double>(
      1,
      [&](vector_size_t /*row*/) { return 0; },
      [&](vector_size_t /*row*/) { return 0; },
      [&](vector_size_t /*row*/) { return 0; })})};

  testAggregations(vectors, {}, {"map_agg(c0, c1)"}, expectedResult);
}

TEST_F(MapAggTest, globalDuplicateKeys) {
  vector_size_t size = 10;

  std::vector<RowVectorPtr> vectors = {makeRowVector(
      {makeFlatVector<int32_t>(size, [](vector_size_t row) { return row / 2; }),
       makeFlatVector<double>(
           size, [](vector_size_t row) { return row + 0.05; }, nullEvery(7))})};

  auto expectedResult = {makeRowVector({makeMapVector<int32_t, double>(
      1,
      [&](vector_size_t /*row*/) { return 5; },
      [&](vector_size_t row) { return row; },
      [&](vector_size_t row) { return 2 * row + 0.05; },
      nullptr,
      nullEvery(7))})};

  testAggregations(vectors, {}, {"map_agg(c0, c1)"}, expectedResult);
}

} // namespace
} // namespace facebook::velox::aggregate::test
