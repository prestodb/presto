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
#include "velox/experimental/cudf/exec/CudfFilterProject.h"
#include "velox/experimental/cudf/exec/ToCudf.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::testutil;

namespace {

template <typename T>
T getColValue(const std::vector<RowVectorPtr>& input, int col, int32_t index) {
  return input[0]->as<RowVector>()->childAt(col)->as<FlatVector<T>>()->valueAt(
      index);
}

class CudfFilterProjectTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    filesystems::registerLocalFileSystem();
    cudf_velox::registerCudf();
    rng_.seed(123);

    rowType_ = ROW({{"c0", INTEGER()}, {"c1", DOUBLE()}, {"c2", VARCHAR()}});
  }

  void TearDown() override {
    cudf_velox::unregisterCudf();
    OperatorTestBase::TearDown();
  }

  void testMultiplyOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a multiply operation
    auto plan =
        PlanBuilder().values(input).project({"1.0 * c1 AS result"}).planNode();

    // Run the test
    runTest(plan, "SELECT 1.0 * c1 AS result FROM tmp");
  }

  void testDivideOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a divide operation
    auto plan =
        PlanBuilder().values(input).project({"c0 / c1 AS result"}).planNode();

    // Run the test
    runTest(plan, "SELECT c0 / c1 AS result FROM tmp");
  }

  void testMultiplyAndMinusOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a multiply and minus operation
    auto plan = PlanBuilder()
                    .values(input)
                    .project({"c0 * (1.0 - c1) AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT c0 * (1.0 - c1) AS result FROM tmp");
  }

  void testStringEqualOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a string equal operation
    auto c2Value = input[0]
                       ->as<RowVector>()
                       ->childAt(2)
                       ->as<FlatVector<StringView>>()
                       ->valueAt(1)
                       .str();
    auto plan = PlanBuilder()
                    .values(input)
                    .project({"c2 = '" + c2Value + "' AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT c2 = '" + c2Value + "' AS result FROM tmp");
  }

  void testStringNotEqualOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a string not equal operation
    auto c2Value = input[0]
                       ->as<RowVector>()
                       ->childAt(2)
                       ->as<FlatVector<StringView>>()
                       ->valueAt(1)
                       .str();
    auto plan = PlanBuilder()
                    .values(input)
                    .project({"c2 <> '" + c2Value + "' AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT c2 <> '" + c2Value + "' AS result FROM tmp");
  }

  void testAndOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with AND operation
    auto plan = PlanBuilder()
                    .values(input)
                    .project({"c0 = 1 AND c1 = 2.0 AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT c0 = 1 AND c1 = 2.0 AS result FROM tmp");
  }

  void testOrOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with OR operation
    auto plan = PlanBuilder()
                    .values(input)
                    .project({"c0 = 1 OR c1 = 2.0 AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT c0 = 1 OR c1 = 2.0 AS result FROM tmp");
  }

  void testYearFunction(const std::vector<RowVectorPtr>& input) {
    // Create a plan with YEAR function
    auto plan =
        PlanBuilder().values(input).project({"YEAR(c2) AS result"}).planNode();

    // Run the test
    runTest(plan, "SELECT YEAR(c2) AS result FROM tmp");
  }

  void testLengthFunction(const std::vector<RowVectorPtr>& input) {
    // Create a plan with LENGTH function
    auto plan = PlanBuilder()
                    .values(input)
                    .project({"LENGTH(c2) AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT LENGTH(c2) AS result FROM tmp");
  }

  void testCaseWhenOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a CASE WHEN operation
    auto plan =
        PlanBuilder()
            .values(input)
            .project({"CASE WHEN c0 = 0 THEN 1.0 ELSE 0.0 END AS result"})
            .planNode();

    // Run the test
    runTest(
        plan,
        "SELECT CASE WHEN c0 = 0 THEN 1.0 ELSE 0.0 END AS result FROM tmp");
  }

  void testSubstrOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a substr operation
    auto plan = PlanBuilder()
                    .values(input)
                    .project({"substr(c2, 1, 3) AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT substr(c2, 1, 3) AS result FROM tmp");
  }

  void testLikeOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a like operation
    auto plan = PlanBuilder()
                    .values(input)
                    .project({"c2 LIKE '%test%' AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT c2 LIKE '%test%' AS result FROM tmp");
  }

  void testLessThanOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a less than operation
    auto plan =
        PlanBuilder().values(input).project({"c0 < c1 AS result"}).planNode();

    // Run the test
    runTest(plan, "SELECT c0 < c1 AS result FROM tmp");

    // compare against literals
    plan = PlanBuilder().values(input).project({"c0 < 1 AS result"}).planNode();

    // Run the test
    runTest(plan, "SELECT c0 < 1 AS result FROM tmp");
  }

  void testGreaterThanOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a greater than operation
    auto plan =
        PlanBuilder().values(input).project({"c0 > c1 AS result"}).planNode();

    // Run the test
    runTest(plan, "SELECT c0 > c1 AS result FROM tmp");

    // compare against literals
    plan = PlanBuilder().values(input).project({"c0 > 1 AS result"}).planNode();

    // Run the test
    runTest(plan, "SELECT c0 > 1 AS result FROM tmp");
  }

  void testLessThanEqualOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a less than equal operation
    auto plan =
        PlanBuilder().values(input).project({"c0 <= c1 AS result"}).planNode();

    // Run the test
    runTest(plan, "SELECT c0 <= c1 AS result FROM tmp");
  }

  void testGreaterThanEqualOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a greater than equal operation
    auto plan =
        PlanBuilder().values(input).project({"c0 >= c1 AS result"}).planNode();

    // Run the test
    runTest(plan, "SELECT c0 >= c1 AS result FROM tmp");
  }

  void testNotOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a NOT operation
    auto plan = PlanBuilder()
                    .values(input)
                    .project({"NOT (c0 = 1) AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT NOT (c0 = 1) AS result FROM tmp");
  }

  void testBetweenOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with a BETWEEN operation
    auto plan = PlanBuilder()
                    .values(input)
                    .project({"c0 BETWEEN 1 AND 100 AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT c0 BETWEEN 1 AND 100 AS result FROM tmp");
  }

  void testMultiInputAndOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with multiple AND operations
    auto c2Value = getColValue<StringView>(input, 2, 1).str();
    auto plan = PlanBuilder()
                    .values(input)
                    .project(
                        {"c0 > 1000 AND c0 < 20000 AND c2 = '" + c2Value +
                         "' AS result"})
                    .planNode();

    // Run the test
    runTest(
        plan,
        "SELECT c0 > 1000 AND c0 < 20000 AND c2 = '" + c2Value +
            "' AS result FROM tmp");
  }

  void testMultiInputOrOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with multiple OR operations
    auto c2Value = getColValue<StringView>(input, 2, 1).str();
    auto plan = PlanBuilder()
                    .values(input)
                    .project(
                        {"c0 > 16000 OR c0 < 8000 OR c1 = 2.0 OR c2 = '" +
                         c2Value + "' AS result"})
                    .planNode();

    // Run the test
    runTest(
        plan,
        "SELECT c0 > 16000 OR c0 < 8000 OR c1 = 2.0 OR c2 = '" + c2Value +
            "' AS result FROM tmp");
  }

  void testIntegerInOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with an IN operation for integers
    std::vector<int32_t> c0Values;
    for (int32_t i = 0; i < 5; i++) {
      c0Values.push_back(getColValue<int32_t>(input, 0, i));
    }
    std::string c0ValuesStr;
    for (size_t i = 0; i < c0Values.size(); ++i) {
      c0ValuesStr += std::to_string(c0Values[i]) + ",";
    }
    c0ValuesStr.pop_back();
    auto plan = PlanBuilder(pool_.get())
                    .values(input)
                    .project({"c0 IN (" + c0ValuesStr + ") AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT c0 IN (" + c0ValuesStr + ") AS result FROM tmp");
  }

  void testDoubleInOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with an IN operation for doubles
    std::vector<double> c1Values;
    for (int32_t i = 0; i < 4; i++) {
      c1Values.push_back(getColValue<double>(input, 1, i));
    }
    std::string c1ValuesStr;
    for (size_t i = 0; i < c1Values.size(); ++i) {
      c1ValuesStr += std::to_string(c1Values[i]) + ",";
    }
    c1ValuesStr.pop_back();
    auto plan = PlanBuilder(pool_.get())
                    .values(input)
                    .project({"c1 IN (" + c1ValuesStr + ") AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT c1 IN (" + c1ValuesStr + ") AS result FROM tmp");
  }

  void testStringInOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan with an IN operation for strings
    std::vector<StringView> c2Values;
    for (int32_t i = 0; i < 3; i++) {
      c2Values.push_back(getColValue<StringView>(input, 2, i));
    }
    std::string c2ValuesStr;
    for (size_t i = 0; i < c2Values.size(); ++i) {
      c2ValuesStr += "'" + c2Values[i].str() + "',";
    }
    c2ValuesStr.pop_back();
    auto plan = PlanBuilder(pool_.get())
                    .values(input)
                    .project({"c2 IN (" + c2ValuesStr + ") AS result"})
                    .planNode();

    // Run the test
    runTest(plan, "SELECT c2 IN (" + c2ValuesStr + ") AS result FROM tmp");
  }

  void testMixedInOperation(const std::vector<RowVectorPtr>& input) {
    // Create a plan that combines multiple IN operations
    auto plan =
        PlanBuilder(pool_.get())
            .values(input)
            .project(
                {"c0 IN (1, 2, 3) OR c1 IN (1.5, 2.5) OR c2 IN ('test1', 'test2') AS result"})
            .planNode();

    // Run the test
    runTest(
        plan,
        "SELECT c0 IN (1, 2, 3) OR c1 IN (1.5, 2.5) OR c2 IN ('test1', 'test2') AS result FROM tmp");
  }

  void runTest(core::PlanNodePtr planNode, const std::string& duckDbSql) {
    SCOPED_TRACE("run without spilling");
    assertQuery(planNode, duckDbSql);
  }

  std::vector<RowVectorPtr> makeVectors(
      const RowTypePtr& rowType,
      int32_t numVectors,
      int32_t rowsPerVector) {
    std::vector<RowVectorPtr> vectors;
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          facebook::velox::test::BatchMaker::createBatch(
              rowType, rowsPerVector, *pool_));
      vectors.push_back(vector);
    }
    return vectors;
  }

  folly::Random::DefaultGenerator rng_;
  RowTypePtr rowType_;
};

TEST_F(CudfFilterProjectTest, multiplyOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testMultiplyOperation(vectors);
}

TEST_F(CudfFilterProjectTest, divideOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testDivideOperation(vectors);
}

TEST_F(CudfFilterProjectTest, multiplyAndMinusOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testMultiplyAndMinusOperation(vectors);
}

TEST_F(CudfFilterProjectTest, stringEqualOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testStringEqualOperation(vectors);
}

TEST_F(CudfFilterProjectTest, stringNotEqualOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testStringNotEqualOperation(vectors);
}

TEST_F(CudfFilterProjectTest, andOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testAndOperation(vectors);
}

TEST_F(CudfFilterProjectTest, orOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testOrOperation(vectors);
}

TEST_F(CudfFilterProjectTest, lengthFunction) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testLengthFunction(vectors);
}

TEST_F(CudfFilterProjectTest, yearFunction) {
  // Update row type to use TIMESTAMP directly
  auto rowType =
      ROW({{"c0", INTEGER()}, {"c1", DOUBLE()}, {"c2", TIMESTAMP()}});

  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType, 2, batchSize);

  // Set timestamp values directly
  for (auto& vector : vectors) {
    auto timestampVector = vector->childAt(2)->asFlatVector<Timestamp>();
    for (vector_size_t i = 0; i < batchSize; ++i) {
      // Set to 2024-03-14 12:34:56
      Timestamp ts(1710415496, 0); // seconds, nanos
      timestampVector->set(i, ts);
    }
  }

  createDuckDbTable(vectors);
  testYearFunction(vectors);
}

TEST_F(CudfFilterProjectTest, DISABLED_caseWhenOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  // failing because switch copies nulls too.
  createDuckDbTable(vectors);

  testCaseWhenOperation(vectors);
}

TEST_F(CudfFilterProjectTest, substrOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testSubstrOperation(vectors);
}

TEST_F(CudfFilterProjectTest, likeOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testLikeOperation(vectors);
}

TEST_F(CudfFilterProjectTest, lessThanOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testLessThanOperation(vectors);
}

TEST_F(CudfFilterProjectTest, greaterThanOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testGreaterThanOperation(vectors);
}

TEST_F(CudfFilterProjectTest, lessThanEqualOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testLessThanEqualOperation(vectors);
}

TEST_F(CudfFilterProjectTest, greaterThanEqualOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testGreaterThanEqualOperation(vectors);
}

TEST_F(CudfFilterProjectTest, notOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testNotOperation(vectors);
}

TEST_F(CudfFilterProjectTest, betweenOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testBetweenOperation(vectors);
}

TEST_F(CudfFilterProjectTest, multiInputAndOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testMultiInputAndOperation(vectors);
}

TEST_F(CudfFilterProjectTest, multiInputOrOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testMultiInputOrOperation(vectors);
}

TEST_F(CudfFilterProjectTest, integerInOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testIntegerInOperation(vectors);
}

TEST_F(CudfFilterProjectTest, doubleInOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testDoubleInOperation(vectors);
}

TEST_F(CudfFilterProjectTest, stringInOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testStringInOperation(vectors);
}

TEST_F(CudfFilterProjectTest, mixedInOperation) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  testMixedInOperation(vectors);
}

TEST_F(CudfFilterProjectTest, simpleFilter) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  // Create a plan with a simple filter
  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c0 > 500")
                  .project({"c0", "c1", "c2"})
                  .planNode();

  // Run the test
  assertQuery(plan, "SELECT c0, c1, c2 FROM tmp WHERE c0 > 500");
}

TEST_F(CudfFilterProjectTest, filterWithProject) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  // Create a plan with filter and project
  auto plan =
      PlanBuilder()
          .values(vectors)
          .filter("c0 > 500")
          .project({"c0 + 2 as doubled", "c1 + 1.0 as incremented", "c2"})
          .planNode();

  // Run the test
  assertQuery(
      plan,
      "SELECT c0 + 2 as doubled, c1 + 1.0 as incremented, c2 FROM tmp WHERE c0 > 500");
}

TEST_F(CudfFilterProjectTest, complexFilter) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  // Create a plan with a complex filter condition
  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c0 > 500 AND c1 < 0.5 AND c2 LIKE '%test%'")
                  .project({"c0", "c1", "c2"})
                  .planNode();

  // Run the test
  assertQuery(
      plan,
      "SELECT c0, c1, c2 FROM tmp WHERE c0 > 500 AND c1 < 0.5 AND c2 LIKE '%test%'");
}

TEST_F(CudfFilterProjectTest, filterWithNullValues) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);

  // Add some null values to the vectors
  for (auto& vector : vectors) {
    auto c0Vector = vector->childAt(0)->asFlatVector<int32_t>();
    auto c1Vector = vector->childAt(1)->asFlatVector<double>();
    for (vector_size_t i = 0; i < batchSize; i += 10) {
      c0Vector->setNull(i, true);
      c1Vector->setNull(i, true);
    }
  }

  createDuckDbTable(vectors);

  // Create a plan with filter that handles null values
  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c0 IS NOT NULL AND c1 IS NOT NULL")
                  .project({"c0", "c1", "c2"})
                  .planNode();

  // Run the test
  assertQuery(
      plan,
      "SELECT c0, c1, c2 FROM tmp WHERE c0 IS NOT NULL AND c1 IS NOT NULL");
}

TEST_F(CudfFilterProjectTest, filterWithOrCondition) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  // Create a plan with OR condition in filter
  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c0 > 500 OR c1 < 0.5")
                  .project({"c0", "c1", "c2"})
                  .planNode();

  // Run the test
  assertQuery(plan, "SELECT c0, c1, c2 FROM tmp WHERE c0 > 500 OR c1 < 0.5");
}

TEST_F(CudfFilterProjectTest, filterWithInCondition) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  // Create a plan with IN condition in filter
  auto plan = PlanBuilder(pool_.get())
                  .values(vectors)
                  .filter("c0 IN (100, 200, 300, 400, 500)")
                  .project({"c0", "c1", "c2"})
                  .planNode();

  // Run the test
  assertQuery(
      plan, "SELECT c0, c1, c2 FROM tmp WHERE c0 IN (100, 200, 300, 400, 500)");
}

TEST_F(CudfFilterProjectTest, filterWithBetweenCondition) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  // Create a plan with BETWEEN condition in filter
  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c0 BETWEEN 100 AND 500")
                  .project({"c0", "c1", "c2"})
                  .planNode();

  // Run the test
  assertQuery(plan, "SELECT c0, c1, c2 FROM tmp WHERE c0 BETWEEN 100 AND 500");
}

TEST_F(CudfFilterProjectTest, filterWithStringOperations) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  // Create a plan with string operations in filter
  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("LENGTH(c2) > 5")
                  .project({"c0", "c1", "c2"})
                  .planNode();

  // Run the test
  assertQuery(plan, "SELECT c0, c1, c2 FROM tmp WHERE LENGTH(c2) > 5");
}

TEST_F(CudfFilterProjectTest, filterWithoutProject) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  // Create a plan with only filter (no projection)
  auto plan =
      PlanBuilder().values(vectors).filter("c0 > 500 AND c1 < 0.5").planNode();

  // Run the test - should return all columns without modification
  assertQuery(plan, "SELECT c0, c1, c2 FROM tmp WHERE c0 > 500 AND c1 < 0.5");
}

TEST_F(CudfFilterProjectTest, filterWithEmptyResult) {
  vector_size_t batchSize = 1000;
  auto vectors = makeVectors(rowType_, 2, batchSize);
  createDuckDbTable(vectors);

  // Create a plan with a filter that should return no rows
  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c0 < 0 AND c0 > 1000") // Impossible condition
                  .planNode();

  // Run the test - should return empty result
  assertQuery(plan, "SELECT c0, c1, c2 FROM tmp WHERE c0 < 0 AND c0 > 1000");
}

TEST_F(CudfFilterProjectTest, dereference) {
  auto rowType = ROW(
      {"c0", "c1", "c2", "c3"}, {BIGINT(), INTEGER(), SMALLINT(), DOUBLE()});
  auto vectors = makeVectors(rowType, 10, 100);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .project({"row_constructor(c1, c2) AS c1_c2"})
                  .project({"c1_c2.c1", "c1_c2.c2"})
                  .planNode();
  assertQuery(plan, "SELECT c1, c2 FROM tmp");

  plan = PlanBuilder()
             .values(vectors)
             .project({"row_constructor(c1, c2) AS c1_c2"})
             .filter("c1_c2.c1 % 10 = 5")
             .project({"c1_c2.c1", "c1_c2.c2"})
             .planNode();
  assertQuery(plan, "SELECT c1, c2 FROM tmp WHERE c1 % 10 = 5");
}
} // namespace
