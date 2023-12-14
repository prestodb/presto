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

namespace facebook::velox::exec::test {

// Applies specified SQL transformation to the results before comparing. For
// example, sorts an array before comparing results of array_agg.
//
// Supports 'compare' API.
class TransformResultVerifier : public ResultVerifier {
 public:
  // @param transform fmt::format-compatible SQL expression to use to transform
  // aggregation results before comparison. The string must have a single
  // placeholder for the column name that contains aggregation results. For
  // example, "array_sort({})".
  explicit TransformResultVerifier(const std::string& transform)
      : transform_{transform} {}

  static std::shared_ptr<ResultVerifier> create(const std::string& transform) {
    return std::make_shared<TransformResultVerifier>(transform);
  }

  bool supportsCompare() override {
    return true;
  }

  bool supportsVerify() override {
    return false;
  }

  void initialize(
      const std::vector<RowVectorPtr>& /*input*/,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& /*aggregate*/,
      const std::string& aggregateName) override {
    projections_ = groupingKeys;
    projections_.push_back(
        fmt::format(fmt::runtime(transform_), aggregateName));
  }

  bool compare(const RowVectorPtr& result, const RowVectorPtr& altResult)
      override {
    return assertEqualResults({transform(result)}, {transform(altResult)});
  }

  bool verify(const RowVectorPtr& /*result*/) override {
    VELOX_UNSUPPORTED();
  }

  void reset() override {
    projections_.clear();
  }

 private:
  RowVectorPtr transform(const RowVectorPtr& data) {
    VELOX_CHECK(!projections_.empty());
    auto plan = PlanBuilder().values({data}).project(projections_).planNode();
    return AssertQueryBuilder(plan).copyResults(data->pool());
  }

  const std::string transform_;

  std::vector<std::string> projections_;
};

} // namespace facebook::velox::exec::test
