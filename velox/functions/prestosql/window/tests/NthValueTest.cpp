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
    vector_size_t size = 100;
    auto vectors = makeRowVector({
        makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
        makeFlatVector<int32_t>(size, [](auto row) { return row; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
        typeValues(type, size),
    });
    testTwoColumnOverClauses({vectors}, "nth_value(c3, c2)");
    testTwoColumnOverClauses({vectors}, "nth_value(c3, 1)");
    testTwoColumnOverClauses({vectors}, "nth_value(c3, 5)");
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
  vector_size_t size = 100;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 7; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 3 + 1; }),
  });

  testTwoColumnOverClauses({vectors}, "nth_value(c0, c2)");
  testTwoColumnOverClauses({vectors}, "nth_value(c0, 1)");
  testTwoColumnOverClauses({vectors}, "nth_value(c0, 5)");
}

TEST_F(NthValueTest, singlePartition) {
  // Test all input rows in a single partition.
  vector_size_t size = 1'000;

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, [](auto /* row */) { return 1; }),
      makeFlatVector<int32_t>(size, [](auto row) { return row % 50; }),
      makeFlatVector<int64_t>(size, [](auto row) { return row % 5 + 1; }),
  });

  testTwoColumnOverClauses({vectors}, "nth_value(c0, c2)");
  testTwoColumnOverClauses({vectors}, "nth_value(c0, 1)");
  testTwoColumnOverClauses({vectors}, "nth_value(c0, 25)");
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

  testTwoColumnOverClauses({vectors}, "nth_value(c0, c2)");
}

TEST_F(NthValueTest, offsetValues) {
  // Test values for offset < 1.
  vector_size_t size = 100;

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

}; // namespace
}; // namespace facebook::velox::window::test
