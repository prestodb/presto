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

#include <string>

#include "velox/core/PlanNode.h"
#include "velox/exec/fuzzer/ResultVerifier.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::exec::test {

class ArbitraryResultVerifier : public ResultVerifier {
 public:
  bool supportsCompare() override {
    return false;
  }

  bool supportsVerify() override {
    return true;
  }

  void initialize(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& aggregate,
      const std::string& aggregateName) override {
    // Create an "oracle" with an array column of all elements in each group.
    // The result of arbitrary will be verified by checking that it is an
    // element in the array of the corresponding group.
    std::stringstream ss;
    toTypeSql(aggregate.call->type(), ss);
    aggregateTypeSql_ = ss.str();
    std::vector<std::string> projectColumns = groupingKeys;
    // Add a column of aggregateTypeSql_ of all nulls so that we can union the
    // oracle result with the actual result.
    projectColumns.push_back(fmt::format(
        "cast(NULL as {}) as {}", aggregateTypeSql_, aggregateName));
    projectColumns.push_back("expected");

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto plan =
        PlanBuilder(planNodeIdGenerator, input[0]->pool())
            .values(input)
            .singleAggregation(groupingKeys, {makeArrayAggCall(aggregate)})
            .project(projectColumns)
            .planNode();

    expected_ = AssertQueryBuilder(plan).copyResults(input[0]->pool());
    groupingKeys_ = groupingKeys;
    name_ = aggregateName;
  }

  bool compare(
      const RowVectorPtr& /*result*/,
      const RowVectorPtr& /*altResult*/) override {
    VELOX_UNSUPPORTED();
  }

  bool verify(const RowVectorPtr& result) override {
    // Union 'result' with 'expected_', group by on 'groupingKeys_' and produce
    // pairs of actual and expected values per group. We cannot use join because
    // grouping keys may have nulls because rows with some grouping keys being
    // nulls are ignored during inner joins and considered unmatchable during
    // left, right, and outer joins.
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto expectedSource =
        PlanBuilder(planNodeIdGenerator).values({expected_}).planNode();

    // Append a column of array of arbitrary result type so that we can union
    // 'result' with 'expected_'.
    auto actualSource =
        PlanBuilder(planNodeIdGenerator, result->pool())
            .values({result})
            .appendColumns({fmt::format(
                "cast(NULL as {}[]) as expected", aggregateTypeSql_)})
            .planNode();

    // In the union of 'expected_' and 'result', there are two rows for each
    // group, one row with a non-null array of all elements, and the other row
    // with the result of arbitrary(). Combine them by array_agg to make one row
    // of each group with two arrays and then use remove_nulls to remove the
    // nulls in these arrays. After remove_nulls, we'll check 'a[1]' is always
    // an element in 'e[1]'. An exception is that if the original arbitrary()
    // result is null, 'a' after remove_nulls is an empty array. In this
    // situation, either 'e' is empty too, or e[1] is an array that only
    // contains null.
    auto plan =
        PlanBuilder(planNodeIdGenerator)
            .localPartition({}, {expectedSource, actualSource})
            .singleAggregation(
                groupingKeys_,
                {fmt::format("array_agg({}) as a", name_),
                 "array_agg(expected) as e"})
            .project({"remove_nulls(a) as a", "remove_nulls(e) as e"})
            .project(
                {"switch(cardinality(a) > 0, cardinality(e) > 0 and \"$internal$contains\"(e[1], a[1]), switch(cardinality(e) > 0, cardinality(remove_nulls(e[1])) = 0, true))"})
            .planNode();
    auto contains = AssertQueryBuilder(plan).copyResults(result->pool());

    const auto numGroups = result->size();
    VELOX_CHECK_EQ(numGroups, contains->size());

    VectorPtr expected =
        BaseVector::createConstant(BOOLEAN(), true, numGroups, result->pool());
    velox::test::VectorMaker vectorMaker(result->pool());
    auto expectedRow = vectorMaker.rowVector({expected});

    return assertEqualResults({expectedRow}, {contains});
  }

  void reset() override {
    expected_.reset();
  }

 private:
  std::string makeArrayAggCall(
      const core::AggregationNode::Aggregate& aggregate) {
    const auto& args = aggregate.call->inputs();
    VELOX_CHECK_GE(args.size(), 1)

    auto inputField = core::TypedExprs::asFieldAccess(args[0]);
    VELOX_CHECK_NOT_NULL(inputField)

    std::string arrayAggCall = fmt::format("array_agg({})", inputField->name());

    if (aggregate.mask != nullptr) {
      arrayAggCall += fmt::format(" filter (where {})", aggregate.mask->name());
    }
    arrayAggCall += " as expected";

    return arrayAggCall;
  }

  RowVectorPtr expected_;
  std::vector<std::string> groupingKeys_;
  std::string name_;
  std::string aggregateTypeSql_;
};

} // namespace facebook::velox::exec::test
