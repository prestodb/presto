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

class MapUnionTest : public AggregationTestBase {};

/**
 * This test checks single, partial, intermediate, final aggregates
 * with and without local exchange when
 * there are no duplicates in the keys of the map.
 *
 * Takes as input a table that contains 2 columns.
 * First column: five 0's followed by five 1's.
 * Second column: A map of size one which contains
 * consecutive numbers, where the key is NULL for
 * every 4th entry (num % 4 == 0) and the value is NULL
 * for every 7th entry (num % 7 == 0).
 *
 * The expected output is to GROUP BY the first column
 * and the map size is to be of size 3 for the first row
 * while of size 4 for the second row, where each map
 * has a list of consecutive numbers and the value of the
 * map is NULL for every 7th entry (num % 7 == 0).
 */
TEST_F(MapUnionTest, groupByWithoutDuplicates) {
  auto inputVectors = {makeRowVector(
      {makeFlatVector<int32_t>(10, [](vector_size_t row) { return row / 5; }),
       makeMapVector<int32_t, double>(
           10,
           [&](vector_size_t /*row*/) { return 1; },
           [&](vector_size_t row) { return row; },
           [&](vector_size_t row) { return row + 0.05; },
           nullEvery(4),
           nullEvery(7))})};

  auto expectedResult = {makeRowVector(
      {makeFlatVector<int32_t>({0, 1}),
       makeMapVector<int32_t, double>(
           2,
           [&](vector_size_t row) { return row == 0 ? 3 : 4; },
           [&](vector_size_t row) { return row; },
           [&](vector_size_t row) { return row + 0.05; },
           nullptr,
           nullEvery(7))})};

  testAggregations(inputVectors, {"c0"}, {"map_union(c1)"}, expectedResult);
}

/**
 * This test checks single, partial, intermediate, final aggregates
 * with and without local exchange when
 * there are duplicates in the keys of the map.
 *
 * Takes as input a table that contains 2 columns.
 * First column: five 0's followed by five 1's
 * Second column: A map of size one which contains
 * (Key, Value) as (1, 1.05).
 *
 * The expected output is to GROUP BY the first column
 * and for each row, the map size is to be of size 1 which
 * contains (Key, Value) as (1, 1.05).
 */
TEST_F(MapUnionTest, groupByWithDuplicates) {
  auto inputVectors = {makeRowVector(
      {makeFlatVector<int32_t>(10, [](vector_size_t row) { return row / 5; }),
       makeMapVector<int32_t, double>(
           10,
           [&](vector_size_t /*row*/) { return 1; },
           [&](vector_size_t /*row*/) { return 1; },
           [&](vector_size_t /*row*/) { return 1.05; })})};
  auto expectedResult = {makeRowVector(
      {makeFlatVector<int32_t>({0, 1}),
       makeMapVector<int32_t, double>(
           2,
           [&](vector_size_t /*row*/) { return 1; },
           [&](vector_size_t /*row*/) { return 1; },
           [&](vector_size_t /*row*/) { return 1.05; })})};

  testAggregations(inputVectors, {"c0"}, {"map_union(c1)"}, expectedResult);
}

/**
 * This test checks single, partial, intermediate, final aggregates
 * with and without local exchange when input is empty.
 */
TEST_F(MapUnionTest, groupByNoData) {
  auto inputVectors = {makeRowVector(
      {makeFlatVector<int32_t>({}), makeMapVector<int32_t, double>({})})};
  auto expectedResult = inputVectors;

  testAggregations(inputVectors, {"c0"}, {"map_union(c1)"}, expectedResult);
}

/**
 * This test checks global aggregate when
 * with and without local exchange when
 * there are no duplicates in the keys of the map.
 *
 * Takes as input a table that contains 1 column i.e.
 * a map of size one which contains consecutive numbers,
 * where the key is NULL for every 4th entry
 * (num % 4 == 0) and the value is NULL
 * for every 7th entry (num % 7 == 0).
 *
 * The expected output is a map of all the non-NULL keys.
 */
TEST_F(MapUnionTest, globalWithoutDuplicates) {
  auto inputVectors = {makeRowVector({makeMapVector<int32_t, double>(
      10,
      [&](vector_size_t /*row*/) { return 1; },
      [&](vector_size_t row) { return row; },
      [&](vector_size_t row) { return row + 0.05; },
      nullEvery(4),
      nullEvery(7))})};
  auto expectedResult = {makeRowVector({makeMapVector<int32_t, double>(
      1,
      [&](vector_size_t /*row*/) { return 7; },
      [&](vector_size_t row) { return row; },
      [&](vector_size_t row) { return row + 0.05; },
      nullptr,
      nullEvery(7))})};

  testAggregations(inputVectors, {}, {"map_union(c0)"}, expectedResult);
}

/**
 * This test checks global aggregate when
 * with and without local exchange when
 * there are duplicates in the keys of the map.
 *
 * Takes as input a table that contains 1 column i.e.
 * a map of size one which contains
 * (Key, Value) as (1, 1.05).
 *
 * The expected output a map which
 * contains (Key, Value) as (1, 1.05).
 */
TEST_F(MapUnionTest, globalWithDuplicates) {
  auto inputVectors = {makeRowVector({makeMapVector<int32_t, double>(
      10,
      [&](vector_size_t /*row*/) { return 1; },
      [&](vector_size_t /*row*/) { return 1; },
      [&](vector_size_t /*row*/) { return 1.05; })})};
  auto expectedResult = {makeRowVector({makeMapVector<int32_t, double>(
      1,
      [&](vector_size_t /*row*/) { return 1; },
      [&](vector_size_t /*row*/) { return 1; },
      [&](vector_size_t /*row*/) { return 1.05; })})};

  testAggregations(inputVectors, {}, {"map_union(c0)"}, expectedResult);
}

/**
 * This test checks global aggregate when
 * the input is empty.
 */
TEST_F(MapUnionTest, globalNoData) {
  auto inputVectors = {makeRowVector({makeMapVector<int32_t, double>(
      1,
      [&](vector_size_t /*row*/) { return 0; },
      [&](vector_size_t /*row*/) { return 0; },
      [&](vector_size_t /*row*/) { return 0; })})};
  auto expectedResult = inputVectors;

  testAggregations(inputVectors, {}, {"map_union(c0)"}, expectedResult);
}
} // namespace
} // namespace facebook::velox::aggregate::test
