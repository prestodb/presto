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
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

namespace facebook::velox::window::test {

/// Common set of window function over clauses using a combination of two
/// columns.
static std::vector<std::string> kBasicOverClauses = {
    "partition by c0 order by c1",
    "partition by c1 order by c0",
    "partition by c0 order by c1 desc",
    "partition by c1 order by c0 desc",
    // No partition by clause.
    "order by c0, c1",
    "order by c1, c0",
    "order by c0 asc, c1 desc",
    "order by c1 asc, c0 desc",
    // No order by clause.
    "partition by c0, c1",
};

/// Common set of window function over clauses with different sort orders
/// using a combination of two columns.
static std::vector<std::string> kSortOrderBasedOverClauses = {
    "partition by c0 order by c1 nulls first",
    "partition by c1 order by c0 nulls first",
    "partition by c0 order by c1 desc nulls first",
    "partition by c1 order by c0 desc nulls first",
    // No partition by clause.
    "order by c0 asc nulls first, c1 desc nulls first",
    "order by c1 asc nulls first, c0 desc nulls first",
    "order by c0 desc nulls first, c1 asc nulls first",
    "order by c1 desc nulls first, c0 asc nulls first",
};

/// Common set of window function over clauses for window frame tests to ensure
/// total ordering for deterministic results in ROW mode.
static std::vector<std::string> kFrameOverClauses = {
    "partition by c0 order by c1, c2",
    "partition by c1 order by c0, c2",
    "partition by c0 order by c2, c1",
    "partition by c1 order by c2, c0",
    // No partition by clause.
    "order by c0 asc nulls first, c1 desc, c2 asc",
    "order by c1 asc, c0 desc nulls last, c2 desc",
    "order by c0 asc, c2 desc, c1 asc nulls last",
    "order by c2 asc, c1 desc nulls first, c0 asc",
    // No order by clause.
    "partition by c0, c1, c2",
};

/// Common set of window function frame clauses in RANGE mode, with current row,
/// unbounded preceding, and unbounded following frame combinations.
static std::vector<std::string> kRangeFrameClauses = {
    "range unbounded preceding",
    "range current row",
    "range between unbounded preceding and current row",
    "range between current row and unbounded following",
    "range between unbounded preceding and unbounded following",
};

class WindowTestBase : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
    velox::window::registerWindowFunctions();
  }

  /// This function generates a simple two integer column RowVector for tests.
  /// The first integer column has row number % 5 values.
  /// The second integer column has row number % 7 values.
  RowVectorPtr makeSimpleVector(vector_size_t size);

  /// This function generates a two integer column RowVector for tests.
  /// The intention here is that the first column has a constant value of 1.
  /// The second column has a value of the row number.
  /// This tests the case where all data is in a single partition.
  RowVectorPtr makeSinglePartitionVector(vector_size_t size);

  /// This function generates a two integer column RowVector for tests.
  /// Both the first and second column of each data row is the row number.
  RowVectorPtr makeSingleRowPartitionsVector(vector_size_t size);

  /// This function generates test data using the VectorFuzzer.
  std::vector<RowVectorPtr> makeFuzzVectors(
      const RowTypePtr& rowType,
      vector_size_t size,
      int numVectors,
      float nullRatio = 0.0);

  /// This function tests SQL queries for the window function and
  /// the specified overClauses with the input RowVectors.
  /// Note : 'function' should be a full window function invocation string
  /// including input parameters and open/close braces. e.g. rank(), ntile(5)
  void testWindowFunction(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::vector<std::string>& overClauses,
      const std::vector<std::string>& frameClauses = {""});

  /// This function tests the SQL query for the window function and overClause
  /// combination with the input RowVectors. It is expected that query execution
  /// will throw an exception with the errorMessage specified.
  void assertWindowFunctionError(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::string& overClause,
      const std::string& errorMessage);

 private:
  void testWindowFunction(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::string& overClause,
      const std::string& frameClause = "");
};
}; // namespace facebook::velox::window::test
