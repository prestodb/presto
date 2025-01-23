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

namespace facebook::velox::exec::test {

// The result of the avg(interval) -> row(double, bigint) -> interval function
// may have a precision error depending on the ordering of inputs due to the
// accumulation in the double intermediate state. This verifier convert the
// interval aggregaiton result to double before comparing the actual and
// expected results, so that the epsilon comparison for floating-point types in
// assertEqualResults() can kick in.
class AverageResultVerifier : public ResultVerifier {
 public:
  bool supportsCompare() override {
    return true;
  }

  bool supportsVerify() override {
    return false;
  }

  void initialize(
      const std::vector<RowVectorPtr>& /*input*/,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& aggregate,
      const std::string& aggregateName) override {
    if (aggregate.call->type()->isIntervalDayTime()) {
      projections_ = groupingKeys;
      projections_.push_back(
          fmt::format("cast(to_milliseconds({}) as double)", aggregateName));
    }
  }

  void initializeWindow(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& /*partitionByKeys*/,
      const std::vector<SortingKeyAndOrder>& /*sortingKeysAndOrders*/,
      const core::WindowNode::Function& function,
      const std::string& /*frame*/,
      const std::string& windowName) override {
    if (function.functionCall->type()->isIntervalDayTime()) {
      projections_ = asRowType(input[0]->type())->names();
      projections_.push_back(
          fmt::format("cast(to_milliseconds({}) as double)", windowName));
    }
  }

  bool compare(const RowVectorPtr& result, const RowVectorPtr& altResult)
      override {
    if (projections_.empty()) {
      return assertEqualResults({result}, {altResult});
    } else {
      return assertEqualResults({transform(result)}, {transform(altResult)});
    }
  }

  bool verify(const RowVectorPtr& /*result*/) override {
    VELOX_UNSUPPORTED();
  }

  void reset() override {
    projections_.clear();
  }

 private:
  RowVectorPtr transform(const RowVectorPtr& data) {
    auto plan = PlanBuilder().values({data}).project(projections_).planNode();
    return AssertQueryBuilder(plan).copyResults(data->pool());
  }

  std::vector<std::string> projections_;
};

} // namespace facebook::velox::exec::test
