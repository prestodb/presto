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
#pragma once

#include "velox/exec/tests/utils/OperatorTestBase.h"

namespace facebook::velox::window::test {

enum class WindowStyle { kSort, kStreaming, kRandom };

/// Exhaustive set of window function over clauses using a combination of four
/// columns. Columns c0 and c1 have input data meant to test partitioning
/// and sorting behavior of the operator. Columns c2 and c3 are used with
/// frame clauses.
/// Note: c2 and c3 are not used for testing sorting but are added in the over
/// clauses as using these always results in a completely deterministic order
/// of the input rows making all function evaluation and result comparison
/// deterministic.
/// The individual functions might add new columns to these clauses if required.
inline const std::vector<std::string> kOverClauses = {
    "partition by c0 order by c1 desc, c2, c3",
    "partition by c1 order by c0, c2 desc, c3",
    "partition by c0 order by c2, c1, c3",

    // Order by with asc/desc and nulls first/last.
    "partition by c0 order by c1 nulls first, c2, c3",
    "partition by c0, c2 order by c1 nulls first, c3",
    "partition by c0 order by c1 desc nulls first, c2, c3",

    // No partition by clause.
    "order by c0 asc nulls first, c1 desc, c2, c3",
    "order by c1 asc, c0 desc nulls last, c2 desc, c3",
    "order by c0 asc, c2 desc, c1 asc nulls last, c3",
    "order by c2 asc nulls first, c1 desc nulls first, c0, c3",

    // No order by clause.
    "partition by c0, c1, c2, c3",
};

/// Exhaustive set of window function frame combinations.
inline const std::vector<std::string> kFrameClauses = {
    // Frame clauses in RANGE mode, with current row, unbounded preceding, and
    // unbounded following frame combinations.
    "range unbounded preceding",
    "range current row",
    "range between current row and unbounded following",
    "range between unbounded preceding and unbounded following",

    // Frame clauses in ROWS mode, with current row, unbounded preceding, and
    // unbounded following frame combinations.
    "rows unbounded preceding",
    "rows current row",
    "rows between unbounded preceding and unbounded following",

    // Frame clauses in ROWS mode with k preceding and k following frame bounds,
    // where k is a constant integer.
    "rows between 5 preceding and current row",
    "rows between 5 preceding and unbounded following",
    "rows between current row and 5 following",
    "rows between unbounded preceding and 5 following",
    "rows between 5 preceding and 5 following",

    // Frame clauses in ROWS mode with k preceding and k following frame bounds,
    // where k is a column.
    "rows between c2 preceding and current row",
    "rows between current row and c2 following",
    "rows between c2 preceding and c2 following",
    "rows between c3 preceding and c2 following",
};

// These frame clauses could have empty or partial frames.
inline const std::vector<std::string> kEmptyFrameClauses = {
    "rows between unbounded preceding and 1 preceding",
    "rows between 1 preceding and 4 preceding",
    "rows between 1 following and unbounded following",
    "rows between 4 following and 1 following",
    "rows between c2 preceding and c3 preceding",
    "rows between c2 following and c3 following",

};

class WindowTestBase : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override {
    exec::test::OperatorTestBase::SetUp();
  }

  /// The below 4 functions are used to generate input data vectors for window
  /// function testing. The vectors have 4 columns:
  /// The first two columns are used to test different partition and sorting
  /// combinations in the over clause.
  /// The third and fourth columns are used to test frame clauses. So they
  /// contain positive non-zero values.

  /// Generates uniformly distributed partition and sort values.
  RowVectorPtr makeSimpleVector(vector_size_t size);

  /// Generates data where all rows belong to a single partition.
  RowVectorPtr makeSinglePartitionVector(vector_size_t size);

  /// Generates data where all partitions have a single row.
  RowVectorPtr makeSingleRowPartitionsVector(vector_size_t size);

  /// Generates a vector of random input values.
  RowVectorPtr makeRandomInputVector(vector_size_t size);

  VectorPtr makeRandomInputVector(
      const TypePtr& type,
      vector_size_t size,
      float nullRatio);

  struct QueryInfo {
    core::PlanNodePtr planNode;
    std::string functionSql;
    std::string querySql;
  };

  QueryInfo buildWindowQuery(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::string& overClause,
      const std::string& frameClause);

  // This function is used to test the StreamingWindow. It will add the order by
  // action to ensure the data is ordered.
  QueryInfo buildStreamingWindowQuery(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::string& overClause,
      const std::string& frameClause);

  /// Tests SQL queries for the window function and
  /// the specified overClauses and frameClauses with the input RowVectors.
  /// Note : 'function' should be a full window function invocation string
  /// including input parameters and open/close braces. e.g. rank(), ntile(5).
  /// If the frameClauses is not specified, then the default is a single empty
  /// clause that corresponds to the default frame of RANGE UNBOUNDED PRECEDING
  /// AND CURRENT ROW.
  void testWindowFunction(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::vector<std::string>& overClauses,
      const std::vector<std::string>& frameClauses = {""},
      bool createTable = true,
      WindowStyle windowStyle = WindowStyle::kRandom);

  /// Tests the window function with the {overClause, frameClause} pair and
  /// asserts that the result is equal to expectedResult.
  void testWindowFunction(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::string& overClause,
      const std::string& frameClause,
      const RowVectorPtr& expectedResult);

  /// Tests the SQL query for the window function and overClause
  /// combination with the input RowVectors. It is expected that query execution
  /// will throw an exception with the errorMessage specified.
  void assertWindowFunctionError(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::string& overClause,
      const std::string& errorMessage);

  /// Tests the SQL query for the window function, overClause,
  /// and frameClause combination with the input RowVectors. It is expected that
  /// query execution will throw an exception with the errorMessage specified.
  void assertWindowFunctionError(
      const std::vector<RowVectorPtr>& input,
      const std::string& function,
      const std::string& overClause,
      const std::string& frameClause,
      const std::string& errorMessage);

  /// Tests different combinations of k range frame columns.
  /// These are special as they require generating frame bound value columns.
  void testKRangeFrames(const std::string& function);

  /// ParseOptions for the DuckDB Parser. nth_value in Spark expects to parse
  /// integer as int vs bigint in Presto. The default is to parse integer
  /// as bigint (Presto behavior).
  parse::ParseOptions options_;

 private:
  enum class BoundType {
    kPreceding,
    kFollowing,
    kCurrentRow,
  };

  struct RangeFrameBound {
    std::optional<int64_t> value;
    BoundType bound;
  };

  void rangeFrameTest(
      bool ascending,
      const std::string& function,
      const RangeFrameBound& startBound,
      const RangeFrameBound& endBound);

  void rangeFrameTestImpl(
      bool ascending,
      const std::string& function,
      const RangeFrameBound& startBound,
      const RangeFrameBound& endBound,
      bool unorderedColumns);
};
} // namespace facebook::velox::window::test
