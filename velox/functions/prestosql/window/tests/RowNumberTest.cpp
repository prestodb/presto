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
#include "velox/functions/prestosql/window/tests/WindowTestBase.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

class RowNumberTest : public WindowTestBase {};

TEST_F(RowNumberTest, basic) {
  testWindowFunction({makeSimpleVector(50)}, "row_number()", kBasicOverClauses);
}

TEST_F(RowNumberTest, basicWithSortOrder) {
  testWindowFunction(
      {makeSimpleVector(50)}, "row_number()", kSortOrderBasedOverClauses);
}

TEST_F(RowNumberTest, singlePartition) {
  // Test all input rows in a single partition.
  testWindowFunction(
      {makeSinglePartitionVector(1000)}, "row_number()", kBasicOverClauses);
}

TEST_F(RowNumberTest, singlePartitionWithSortOrder) {
  // Test all input rows in a single partition.
  testWindowFunction(
      {makeSinglePartitionVector(500)},
      "row_number()",
      kSortOrderBasedOverClauses);
}

TEST_F(RowNumberTest, multiInput) {
  // Double the input rows so that partitioning and ordering over multiple
  // input groups are exercised.
  testWindowFunction(
      {makeSinglePartitionVector(250), makeSinglePartitionVector(250)},
      "row_number()",
      kBasicOverClauses);
}

TEST_F(RowNumberTest, multiInputWithSortOrder) {
  // Double the input rows so that partitioning and ordering over multiple
  // input groups are exercised.
  testWindowFunction(
      {makeSimpleVector(500), makeSimpleVector(500)},
      "row_number()",
      kSortOrderBasedOverClauses);
}

TEST_F(RowNumberTest, randomInput) {
  auto vectors = makeFuzzVectors(
      ROW({"c0", "c1", "c2", "c3"}, {BIGINT(), VARCHAR(), INTEGER(), BIGINT()}),
      10,
      2,
      0.3);
  createDuckDbTable(vectors);

  std::vector<std::string> overClauses = {
      "partition by c0 order by c1, c2, c3",
      "partition by c1 order by c0, c2, c3",
      "partition by c0 order by c1 desc, c2, c3",
      "partition by c1 order by c0 desc, c2, c3",
      "partition by c0 order by c1 desc, c2 nulls first, c3",
      "partition by c1 order by c0 desc, c2 nulls first, c3",
      "partition by c0 order by c1 desc nulls first, c2 nulls first, c3",
      "partition by c1 order by c0 desc nulls first, c2 nulls first, c3",
      "order by c0, c1, c2, c3",
      "order by c0, c1 nulls first, c2, c3",
      "partition by c0, c1, c2, c3",
  };

  testWindowFunction(vectors, "row_number()", overClauses);
}

}; // namespace
}; // namespace facebook::velox::window::test
