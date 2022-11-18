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

class NtileTest : public WindowTestBase {
 protected:
  void testWindowFunction(
      const std::vector<RowVectorPtr>& vectors,
      const std::vector<std::string>& overClauses) {
    // These invocations of ntile check the following cases :
    // i) Constant buckets
    // ii) Number of buckets <, =, > number of rows in the partition.
    // iii) Number of buckets evenly divide (value 10) or not (other values).
    // TODO: Add null value testing also pending issues with DuckDB.
    for (auto i = 1; i < 20; i += 3) {
      WindowTestBase::testWindowFunction(
          vectors, fmt::format("ntile({})", i), overClauses);
    }
  }

  RowVectorPtr makeSimpleVector(vector_size_t size) {
    return makeRowVector({
        makeFlatVector<int64_t>(size, [](auto row) { return row % 5 + 1; }),
        makeFlatVector<int64_t>(
            size, [](auto row) { return row % 7 + 1; }, nullEvery(15)),
    });
  }
};

TEST_F(NtileTest, basic) {
  NtileTest::testWindowFunction({makeSimpleVector(50)}, kBasicOverClauses);
}

TEST_F(NtileTest, basicWithSortOrders) {
  NtileTest::testWindowFunction(
      {makeSimpleVector(50)}, kSortOrderBasedOverClauses);
}

TEST_F(NtileTest, columnBasic) {
  auto vector = NtileTest::makeSimpleVector(100);
  WindowTestBase::testWindowFunction({vector}, "ntile(c0)", kBasicOverClauses);
}

TEST_F(NtileTest, columnBasicWithSortOrders) {
  auto vector = NtileTest::makeSimpleVector(100);
  WindowTestBase::testWindowFunction(
      {vector}, "ntile(c0)", kSortOrderBasedOverClauses);
}

TEST_F(NtileTest, singlePartition) {
  NtileTest::testWindowFunction(
      {makeSinglePartitionVector(75)}, kBasicOverClauses);
}

TEST_F(NtileTest, singlePartitionWithSortOrders) {
  NtileTest::testWindowFunction(
      {makeSinglePartitionVector(50)}, kSortOrderBasedOverClauses);
}

TEST_F(NtileTest, multiInput) {
  NtileTest::testWindowFunction(
      {makeSinglePartitionVector(75), makeSinglePartitionVector(50)},
      kBasicOverClauses);
}

TEST_F(NtileTest, multiInputWithSortOrders) {
  NtileTest::testWindowFunction(
      {makeSinglePartitionVector(75), makeSinglePartitionVector(50)},
      kSortOrderBasedOverClauses);
}

TEST_F(NtileTest, errorCases) {
  auto vectors = makeSimpleVector(5);

  std::string overClause = "partition by c0 order by c1";
  std::string bucketError = "Buckets must be greater than 0";
  assertWindowFunctionError({vectors}, "ntile(0)", overClause, bucketError);
  assertWindowFunctionError({vectors}, "ntile(-1)", overClause, bucketError);

  vector_size_t size = 10;
  RowVectorPtr columnVector = makeRowVector({
      makeFlatVector<int64_t>(size, [](auto row) { return row % 5; }),
      makeFlatVector<int64_t>(
          size, [](auto row) { return row % 7 + 1; }, nullEvery(15)),
  });
  assertWindowFunctionError(
      {columnVector}, "ntile(c0)", overClause, bucketError);
}

}; // namespace
}; // namespace facebook::velox::window::test
