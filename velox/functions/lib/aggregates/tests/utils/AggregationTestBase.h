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

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::exec::test {
class AssertQueryBuilder;
}

namespace facebook::velox::functions::aggregate::test {

class AggregationTestBase : public exec::test::OperatorTestBase {
 protected:
  void SetUp() override;
  void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();

  std::vector<RowVectorPtr>
  makeVectors(const RowTypePtr& rowType, vector_size_t size, int numVectors);

  /// Generates a variety of logically equivalent plans to compute aggregations
  /// using combinations of partial, final, single, and intermediate
  /// aggregations with and without local exchanges. Runs all these plans and
  /// verifies results against DuckDB.
  ///
  /// The following plans are generated:
  ///     - partial -> final
  ///     - single
  ///     - partial -> intermediate -> final
  ///     - partial -> local exchange -> final
  ///     - partial -> local exchange -> intermediate -> local exchange -> final
  ///
  /// Example:
  ///     testAggregations(
  ///         [&](PlanBuilder& builder) {
  ///             builder
  ///                 .values(vectors)
  ///                 .project({"c0 % 10 AS c0_mod_10", "c1"});
  ///         },
  ///      {"c0_mod_10"},
  ///      {"sum(c1)"},
  ///      "SELECT c0 % 10, sum(c1) FROM tmp GROUP BY 1");
  ///
  /// @param makeSource A function taking a reference to PlanBuilder and
  /// building up the plan all the way up to the aggregation.
  /// @param groupingKeys A list of grouping keys. May be empty.
  /// @param aggregates A list of aggregates to compute.
  /// @param duckDbSql An equivalent DuckDB SQL query.
  /// @param config Optional configuration properties to use when evaluating
  /// aggregations.
  void testAggregations(
      std::function<void(exec::test::PlanBuilder&)> makeSource,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::string& duckDbSql,
      const std::unordered_map<std::string, std::string>& config = {});

  /// Same as above, but allows to specify a set of projections to apply after
  /// the aggregation.
  void testAggregations(
      std::function<void(exec::test::PlanBuilder&)> makeSource,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& postAggregationProjections,
      std::function<std::shared_ptr<exec::Task>(
          exec::test::AssertQueryBuilder& builder)> assertResults,
      const std::unordered_map<std::string, std::string>& config = {});

  /// Convenience version that allows to specify input data instead of a
  /// function to build Values plan node.
  void testAggregations(
      const std::vector<RowVectorPtr>& data,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::string& duckDbSql,
      const std::unordered_map<std::string, std::string>& config = {});

  /// Convenience version that allows to specify input data instead of a
  /// function to build Values plan node.
  void testAggregations(
      const std::vector<RowVectorPtr>& data,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& postAggregationProjections,
      const std::string& duckDbSql,
      const std::unordered_map<std::string, std::string>& config = {});

  /// Convenience version that allows to specify input data instead of a
  /// function to build Values plan node, and the expected result instead of a
  /// DuckDB SQL query to validate the result.
  void testAggregations(
      const std::vector<RowVectorPtr>& data,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<RowVectorPtr>& expectedResult,
      const std::unordered_map<std::string, std::string>& config = {});

  /// Convenience version that allows to specify input data instead of a
  /// function to build Values plan node, and the expected result instead of a
  /// DuckDB SQL query to validate the result.
  void testAggregations(
      const std::vector<RowVectorPtr>& data,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& postAggregationProjections,
      const std::vector<RowVectorPtr>& expectedResult,
      const std::unordered_map<std::string, std::string>& config = {});

  /// Ensure the function is working in streaming use case.  Create a first
  /// aggregation function, add the rawInput1, then extract the accumulator,
  /// read the accumulator to second function, and continue to add rawInput2 to
  /// second function.
  VectorPtr testStreaming(
      const std::string& functionName,
      bool testGlobal,
      const std::vector<VectorPtr>& rawInput1,
      const std::vector<VectorPtr>& rawInput2,
      const std::unordered_map<std::string, std::string>& config = {});

  // Given a list of aggregation expressions, test their equivalent plans using
  // companion functions. For example, suppose we have the following arguments
  // where c0 and c1 are double colummns.
  //   aggregates = {"avg(c0), avg(c1)"}
  //   groupingKeys = {"g0"},
  //   postAggregationProjections = {},
  // construct and test aggregation plans for a query consisting the following
  // operations:
  //   a0, a1 = avg_partial(c0), avg_partial(c1) group by g0, k1
  //   a0, a1 = avg_merge(a0), avg_merge(a1) group by g0
  //   g0, c0, c1 = g0, avg_extract_double(a0), avg_extract_double(a1)
  // Note that we intentionally add an additional grouping key k1 at the
  // avg_partial operation so that the following avg_merge operation will have
  // multiple values to merge for each g0 group.
  // This config is used in Aggregate parameter core::QueryConfig.
  void testAggregationsWithCompanion(
      const std::vector<RowVectorPtr>& data,
      const std::function<void(exec::test::PlanBuilder&)>&
          preAggregationProcessing,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::vector<TypePtr>>& aggregatesArgTypes,
      const std::vector<std::string>& postAggregationProjections,
      const std::string& duckDbSql,
      const std::unordered_map<std::string, std::string>& config = {});

  void testAggregationsWithCompanion(
      const std::vector<RowVectorPtr>& data,
      const std::function<void(exec::test::PlanBuilder&)>&
          preAggregationProcessing,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::vector<TypePtr>>& aggregatesArgTypes,
      const std::vector<std::string>& postAggregationProjections,
      const std::vector<RowVectorPtr>& expectedResult,
      const std::unordered_map<std::string, std::string>& config = {});

  void testAggregationsWithCompanion(
      const std::vector<RowVectorPtr>& data,
      const std::function<void(exec::test::PlanBuilder&)>&
          preAggregationProcessing,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::vector<TypePtr>>& aggregatesArgTypes,
      const std::vector<std::string>& postAggregationProjections,
      std::function<std::shared_ptr<exec::Task>(
          exec::test::AssertQueryBuilder&)> assertResults,
      const std::unordered_map<std::string, std::string>& config = {});

  // Split the input into 2 files then read the input from files.  Can reveal
  // bugs in string life cycle management.
  void testReadFromFiles(
      std::function<void(exec::test::PlanBuilder&)> makeSource,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& postAggregationProjections,
      std::function<std::shared_ptr<exec::Task>(
          exec::test::AssertQueryBuilder&)> assertResults,
      const std::unordered_map<std::string, std::string>& config = {});

  void testReadFromFiles(
      const std::vector<RowVectorPtr>& data,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<RowVectorPtr>& expectedResult,
      const std::unordered_map<std::string, std::string>& config = {}) {
    testReadFromFiles(
        [&](auto& planBuilder) { planBuilder.values(data); },
        groupingKeys,
        aggregates,
        {},
        [&](auto& assertBuilder) {
          return assertBuilder.assertResults({expectedResult});
        },
        config);
  }

  /// Generates a variety of logically equivalent plans to compute aggregations
  /// using combinations of partial, final, single, and intermediate
  /// aggregations with and without local exchanges. Runs all these plans and
  /// verifies they all fail with the specified error message.
  void testFailingAggregations(
      const std::vector<RowVectorPtr>& data,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::string& expectedMessage,
      const std::unordered_map<std::string, std::string>& config = {});

  /// Specifies that aggregate functions used in this test are not sensitive
  /// to the order of inputs.
  void allowInputShuffle() {
    allowInputShuffle_ = true;
  }
  /// Specifies that aggregate functions used in this test are sensitive
  /// to the order of inputs.
  void disallowInputShuffle() {
    allowInputShuffle_ = false;
  }

  void disableTestStreaming() {
    testStreaming_ = false;
  }

  void enableTestStreaming() {
    testStreaming_ = true;
  }

  void disableTestIncremental() {
    testIncremental_ = false;
  }

  void enableTestIncremental() {
    testIncremental_ = true;
  }

  /// Whether testStreaming should be called in testAggregations.
  bool testStreaming_{true};

  /// Whether testIncrementalAggregation should be called in testAggregations.
  bool testIncremental_{true};

 private:
  // Test streaming use case where raw inputs are added after intermediate
  // results. Return the result of aggregates if successful.
  RowVectorPtr validateStreamingInTestAggregations(
      const std::function<void(exec::test::PlanBuilder&)>& makeSource,
      const std::vector<std::string>& aggregates,
      const std::unordered_map<std::string, std::string>& config);

  VectorPtr testStreaming(
      const std::string& functionName,
      bool testGlobal,
      const std::vector<VectorPtr>& rawInput1,
      vector_size_t rawInput1Size,
      const std::vector<VectorPtr>& rawInput2,
      vector_size_t rawInput2Size,
      const std::unordered_map<std::string, std::string>& config = {});

  // Test to ensure that when extractValues() or extractAccumulators() is called
  // twice on the same accumulator, the extracted results are the same. This
  // ensures that extractValues() and extractAccumulators() are free of side
  // effects.
  void testIncrementalAggregation(
      const std::function<void(exec::test::PlanBuilder&)>& makeSource,
      const std::vector<std::string>& aggregates,
      const std::unordered_map<std::string, std::string>& config = {});

  void testAggregationsImpl(
      std::function<void(exec::test::PlanBuilder&)> makeSource,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& postAggregationProjections,
      std::function<std::shared_ptr<exec::Task>(
          exec::test::AssertQueryBuilder&)> assertResults,
      const std::unordered_map<std::string, std::string>& config);

  void testStreamingAggregationsImpl(
      std::function<void(exec::test::PlanBuilder&)> makeSource,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& postAggregationProjections,
      std::function<std::shared_ptr<exec::Task>(
          exec::test::AssertQueryBuilder&)> assertResults,
      const std::unordered_map<std::string, std::string>& config);

  bool allowInputShuffle_{false};
};

} // namespace facebook::velox::functions::aggregate::test
