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

#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

class NthValueTest : public WindowTestBase {
 protected:
  void testPrimitiveType(const TypePtr& type) {
    vector_size_t size = 25;
    auto vectors = makeRowVector({
        makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
        typeValues(type, size),
    });
    testWindowFunction({vectors}, "nth_value(c3, c2)", kBasicOverClauses);
    testWindowFunction(
        {vectors}, "nth_value(c3, c2)", kSortOrderBasedOverClauses);
    testWindowFunction({vectors}, "nth_value(c3, 1)", kBasicOverClauses);
    testWindowFunction(
        {vectors}, "nth_value(c3, 5)", kSortOrderBasedOverClauses);
  }

  RowVectorPtr makeBasicVectors(vector_size_t size) {
    return makeRowVector({
        makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
    });
  }

  RowVectorPtr makeSinglePartitionVectors(vector_size_t size) {
    return makeRowVector({
        makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 5 + 1; }),
    });
  }

 private:
  VectorPtr typeValues(const TypePtr& type, vector_size_t size) {
    VectorFuzzer::Options options;
    options.vectorSize = size;
    options.nullRatio = 0.2;
    options.useMicrosecondPrecisionTimestamp = true;
    VectorFuzzer fuzzer(options, pool_.get(), 0);
    return fuzzer.fuzz(type);
  }
};

TEST_F(NthValueTest, basic) {
  auto vectors = makeBasicVectors(50);

  testWindowFunction({vectors}, "nth_value(c0, c2)", kBasicOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 1)", kBasicOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 5)", kBasicOverClauses);
}

TEST_F(NthValueTest, basicWithSortOrder) {
  auto vectors = makeBasicVectors(50);

  testWindowFunction(
      {vectors}, "nth_value(c0, c2)", kSortOrderBasedOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 1)", kSortOrderBasedOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 5)", kSortOrderBasedOverClauses);
}

TEST_F(NthValueTest, singlePartition) {
  auto vectors = makeSinglePartitionVectors(500);

  testWindowFunction({vectors}, "nth_value(c0, c2)", kBasicOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 1)", kBasicOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 25)", kBasicOverClauses);
}

TEST_F(NthValueTest, singlePartitionWithSortOrder) {
  auto vectors = makeSinglePartitionVectors(500);

  testWindowFunction(
      {vectors}, "nth_value(c0, c2)", kSortOrderBasedOverClauses);
  testWindowFunction({vectors}, "nth_value(c0, 1)", kSortOrderBasedOverClauses);
  testWindowFunction(
      {vectors}, "nth_value(c0, 25)", kSortOrderBasedOverClauses);
}

TEST_F(NthValueTest, multiInput) {
  auto vectors = makeSinglePartitionVectors(250);
  auto doubleVectors = {vectors, vectors};

  testWindowFunction(doubleVectors, "nth_value(c0, c2)", kBasicOverClauses);
  testWindowFunction(doubleVectors, "nth_value(c0, 1)", kBasicOverClauses);
  testWindowFunction(doubleVectors, "nth_value(c0, 25)", kBasicOverClauses);
}

TEST_F(NthValueTest, multiInputWithSortOrder) {
  auto vectors = makeSinglePartitionVectors(250);
  auto doubleVectors = {vectors, vectors};

  testWindowFunction(
      doubleVectors, "nth_value(c0, c2)", kSortOrderBasedOverClauses);
  testWindowFunction(
      doubleVectors, "nth_value(c0, 1)", kSortOrderBasedOverClauses);
  testWindowFunction(
      doubleVectors, "nth_value(c0, 25)", kSortOrderBasedOverClauses);
}

TEST_F(NthValueTest, integerValues) {
  testPrimitiveType(INTEGER());
}

TEST_F(NthValueTest, tinyintValues) {
  testPrimitiveType(TINYINT());
}

TEST_F(NthValueTest, smallintValues) {
  testPrimitiveType(SMALLINT());
}

TEST_F(NthValueTest, bigintValues) {
  testPrimitiveType(BIGINT());
}

TEST_F(NthValueTest, realValues) {
  testPrimitiveType(REAL());
}

TEST_F(NthValueTest, doubleValues) {
  testPrimitiveType(DOUBLE());
}

TEST_F(NthValueTest, varcharValues) {
  testPrimitiveType(VARCHAR());
}

TEST_F(NthValueTest, varbinaryValues) {
  testPrimitiveType(VARBINARY());
}

TEST_F(NthValueTest, timestampValues) {
  testPrimitiveType(TIMESTAMP());
}

TEST_F(NthValueTest, dateValues) {
  testPrimitiveType(DATE());
}

TEST_F(NthValueTest, nullOffsets) {
  // Test that nth_value with null offset returns rows with null value.
  vector_size_t size = 100;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
      makeFlatVector<int64_t>(
          size, [](auto row) { return row % 3 + 1; }, nullEvery(5)),
  });

  testWindowFunction({vectors}, "nth_value(c0, c2)", kBasicOverClauses);
  testWindowFunction(
      {vectors}, "nth_value(c0, c2)", kSortOrderBasedOverClauses);
}

TEST_F(NthValueTest, offsetValues) {
  // Test values for offset < 1.
  vector_size_t size = 20;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 5; }),
  });

  std::string overClause = "partition by c0 order by c1";
  std::string offsetError = "Offset must be at least 1";
  assertWindowFunctionError(
      {vectors}, "nth_value(c0, 0)", overClause, offsetError);
  assertWindowFunctionError(
      {vectors}, "nth_value(c0, -1)", overClause, offsetError);
  assertWindowFunctionError(
      {vectors}, "nth_value(c0, c2)", overClause, offsetError);
}

TEST_F(NthValueTest, basicRangeFrames) {
  auto vectors = makeBasicVectors(50);

  testWindowFunction(
      {vectors}, "nth_value(c0, c2)", kFrameOverClauses, kRangeFrameClauses);
  testWindowFunction(
      {vectors}, "nth_value(c0, 1)", kFrameOverClauses, kRangeFrameClauses);
  testWindowFunction(
      {vectors}, "nth_value(c0, 5)", kFrameOverClauses, kRangeFrameClauses);
}

TEST_F(NthValueTest, singlePartitionRangeFrames) {
  auto vectors = makeSinglePartitionVectors(400);

  testWindowFunction(
      {vectors}, "nth_value(c0, c2)", kFrameOverClauses, kRangeFrameClauses);
  testWindowFunction(
      {vectors}, "nth_value(c0, 5)", kFrameOverClauses, kRangeFrameClauses);
}

TEST_F(NthValueTest, multiInputRangeFrames) {
  auto vectors = makeSinglePartitionVectors(200);
  auto doubleVectors = {vectors, vectors};

  testWindowFunction(
      doubleVectors,
      "nth_value(c0, c2)",
      kFrameOverClauses,
      kRangeFrameClauses);
  testWindowFunction(
      {vectors}, "nth_value(c0, 5)", kFrameOverClauses, kRangeFrameClauses);
}

}; // namespace
}; // namespace facebook::velox::window::test
