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

#include "velox/functions/prestosql/fuzzer/NoisySumResultVerifier.h"

namespace facebook::velox::exec::test {
class NoisyAvgResultVerifier : public NoisySumResultVerifier {
 public:
  void initialize(
      const std::vector<RowVectorPtr>& input,
      const std::vector<core::ExprPtr>& projections,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& aggregate,
      const std::string& aggregateName) override {
    groupingKeys_ = groupingKeys;
    name_ = aggregateName;
    // Extract the noise scale from the function call.
    const auto& args = aggregate.call->inputs();
    extractNoiseScaleAndBound(input[0], args);

    // Extract aggregate column name before deduplication
    auto field = core::TypedExprs::asFieldAccess(args[0]);
    VELOX_CHECK_NOT_NULL(field);
    aggregateColumn_ = field->name();

    // When distinct is true, we should deduplicate the input before clipping.
    // if mask is provided, mask should apply to the input before
    // deduplication.
    auto deduplicatedInput = input;
    if (aggregate.distinct) {
      deduplicatedInput = deduplicateInput(input, aggregate.mask);
    }
    // Clip the input to the specified bounds and convert to double. This is
    // needed because the noisy_avg_gaussian function only return double
    // outputs.
    clipInput(deduplicatedInput);

    auto sumCall = fmt::format("avg({})", aggregateColumn_);

    // If distinct is false, mask has not been applied yet.
    if (aggregate.mask != nullptr && !aggregate.distinct) {
      sumCall += fmt::format(" filter (where {})", aggregate.mask->name());
    }

    core::PlanNodePtr plan = PlanBuilder()
                                 .values(clippedInput_)
                                 .projectExpressions(projections)
                                 .singleAggregation(groupingKeys, {sumCall})
                                 .planNode();

    expectedNoNoise_ = AssertQueryBuilder(plan).copyResults(input[0]->pool());
  }
};
} // namespace facebook::velox::exec::test
