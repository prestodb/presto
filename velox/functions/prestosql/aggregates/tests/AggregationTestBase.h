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
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace facebook::velox::aggregate::test {

class AggregationTestBase : public exec::test::OperatorTestBase {
 protected:
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
  void testAggregations(
      std::function<void(exec::test::PlanBuilder&)> makeSource,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::string& duckDbSql);

  /// Same as above, but allows to specify a set of projections to apply after
  /// the aggregation.
  template <bool UseDuckDB = true>
  void testAggregations(
      std::function<void(exec::test::PlanBuilder&)> makeSource,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& postAggregationProjections,
      const std::string& duckDbSql,
      const std::vector<RowVectorPtr>& expectedResult = {});

  /// Convenience version that allows to specify input data instead of a
  /// function to build Values plan node.
  void testAggregations(
      const std::vector<RowVectorPtr>& data,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::string& duckDbSql);

  /// Convenience version that allows to specify input data instead of a
  /// function to build Values plan node.
  void testAggregations(
      const std::vector<RowVectorPtr>& data,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& postAggregationProjections,
      const std::string& duckDbSql);

  /// Convenience version that allows to specify input data instead of a
  /// function to build Values plan node, and the expected result instead of a
  /// DuckDB SQL query to validate the result.
  void testAggregations(
      const std::vector<RowVectorPtr>& data,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<RowVectorPtr>& expectedResult);

  // Turns off spill test. Use if the test has only one batch of input.
  void disableSpill() {
    noSpill_ = true;
  }

  /// Convenience version that allows to specify input data instead of a
  /// function to build Values plan node, and the expected result instead of a
  /// DuckDB SQL query to validate the result.
  void testAggregations(
      const std::vector<RowVectorPtr>& data,
      const std::vector<std::string>& groupingKeys,
      const std::vector<std::string>& aggregates,
      const std::vector<std::string>& postAggregationProjections,
      const std::vector<RowVectorPtr>& expectedResult);

  // Set to true if the case does not have enough data to spill even
  // if spill forced (only one vector of input).
  bool noSpill_{false};
};

} // namespace facebook::velox::aggregate::test
