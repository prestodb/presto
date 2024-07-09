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

#include "velox/common/hyperloglog/HllUtils.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/fuzzer/ResultVerifier.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec::test {

// Compares results of approx_distinct(x[, e]) with count(distinct x).
// For each group calculates the difference between 2 values and counts number
// of groups where difference is > 2e. If total number of groups is >= 50,
// allows 2 groups > 2e. If number of groups is small (< 50),
// expects all groups to be under 2e.
class ApproxDistinctResultVerifier : public ResultVerifier {
 public:
  bool supportsCompare() override {
    return false;
  }

  bool supportsVerify() override {
    return true;
  }

  // Compute count(distinct x) over 'input'.
  void initialize(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& aggregate,
      const std::string& aggregateName) override {
    auto plan =
        PlanBuilder()
            .values(input)
            .singleAggregation(groupingKeys, {makeCountDistinctCall(aggregate)})
            .planNode();

    expected_ = AssertQueryBuilder(plan).copyResults(input[0]->pool());
    groupingKeys_ = groupingKeys;
    name_ = aggregateName;
    error_ = extractError(aggregate.call, input[0]);
    verifyWindow_ = false;
  }

  // Compute count_distinct(x) over 'input' over 'frame'.
  void initializeWindow(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& partitionByKeys,
      const std::vector<SortingKeyAndOrder>& /*sortingKeysAndOrders*/,
      const core::WindowNode::Function& function,
      const std::string& frame,
      const std::string& windowName) override {
    auto plan = PlanBuilder()
                    .values(input)
                    .window({makeCountDistinctWindowCall(function, frame)})
                    .planNode();

    expected_ = AssertQueryBuilder(plan).copyResults(input[0]->pool());
    groupingKeys_ = partitionByKeys;
    name_ = windowName;
    error_ = extractError(function.functionCall, input[0]);
    verifyWindow_ = true;
  }

  bool compare(
      const RowVectorPtr& /*result*/,
      const RowVectorPtr& /*altResult*/) override {
    VELOX_UNSUPPORTED();
  }

  bool verify(const RowVectorPtr& result) override {
    // Union 'result' with 'expected_', group by on 'groupingKeys_' and produce
    // pairs of actual and expected values per group. We cannot use join because
    // grouping keys may have nulls.
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto expectedSource = PlanBuilder(planNodeIdGenerator)
                              .values({expected_})
                              .appendColumns({"'expected' as label"})
                              .planNode();

    auto actualSource = PlanBuilder(planNodeIdGenerator)
                            .values({result})
                            .appendColumns({"'actual' as label"})
                            .planNode();

    if (verifyWindow_) {
      return verifyWindow(
          expectedSource, actualSource, planNodeIdGenerator, result->pool());
    }

    auto mapAgg = fmt::format("map_agg(label, {}) as m", name_);
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .localPartition({}, {expectedSource, actualSource})
                    .singleAggregation(groupingKeys_, {mapAgg})
                    .project({"m['actual'] as a", "m['expected'] as e"})
                    .planNode();
    auto combined = AssertQueryBuilder(plan).copyResults(result->pool());

    auto* actual = combined->childAt(0)->as<SimpleVector<int64_t>>();
    auto* expected = combined->childAt(1)->as<SimpleVector<int64_t>>();

    const auto numGroups = result->size();
    VELOX_CHECK_EQ(numGroups, combined->size());

    std::vector<double> largeGaps;
    for (auto i = 0; i < numGroups; ++i) {
      VELOX_CHECK(!actual->isNullAt(i))
      VELOX_CHECK(!expected->isNullAt(i))

      const auto actualCnt = actual->valueAt(i);
      const auto expectedCnt = expected->valueAt(i);
      if (actualCnt != expectedCnt) {
        if (expectedCnt > 0) {
          const auto gap =
              std::abs(actualCnt - expectedCnt) * 1.0 / expectedCnt;
          if (gap > 2 * error_) {
            largeGaps.push_back(gap);
            LOG(ERROR) << fmt::format(
                "approx_distinct(x, {}) is more than 2 stddev away from "
                "count(distinct x). Difference: {}, approx_distinct: {}, "
                "count(distinct): {}. This is unusual, but doesn't necessarily "
                "indicate a bug.",
                error_,
                gap,
                actualCnt,
                expectedCnt);
          }
        } else {
          LOG(ERROR) << fmt::format(
              "count(distinct x) returned 0, but approx_distinct(x, {}) is {}",
              error_,
              actualCnt);
          return false;
        }
      }
    }

    // We expect large deviations (>2 stddev) in < 5% of values.
    if (numGroups >= 50) {
      return largeGaps.size() <= 3;
    }

    return largeGaps.empty();
  }

  // For approx_distinct in window operations, input sets for rows in the same
  // partition are correlated. Since the error bound of approx_distinct only
  // applies to independent input sets, we only take one max gap in each
  // partition when checking the error bound.
  bool verifyWindow(
      core::PlanNodePtr& expectedSource,
      core::PlanNodePtr& actualSource,
      std::shared_ptr<core::PlanNodeIdGenerator>& planNodeIdGenerator,
      memory::MemoryPool* pool) {
    auto mapAgg = fmt::format("map_agg(label, {}) as m", name_);

    auto keys = groupingKeys_;
    keys.push_back("row_number");

    // Calculate the gap between each actual and the corresponding expected
    // counts.
    auto projections = keys;
    projections.push_back(
        "cast(abs(m['actual'] - m['expected']) as double) / cast(m['expected'] as double) as gap");

    auto plan =
        PlanBuilder(planNodeIdGenerator)
            .localPartition({}, {expectedSource, actualSource})
            .singleAggregation(keys, {mapAgg})
            .project(projections)
            // groupingKeys_ are the partition-by keys for window operations.
            // The error bound of approx_distinct is for independent input sets,
            // while input sets in the same partition are correlated. So we only
            // take one max gap in each partition.
            .singleAggregation(groupingKeys_, {"max(gap)"})
            .planNode();
    auto combined = AssertQueryBuilder(plan).copyResults(pool);
    const auto numGroups = combined->size();

    std::vector<double> largeGaps;
    // Standard error would likely exceed the error bound when the number of
    // groups is small and the error bound is large. So we only check the
    // standard error when the numGroups >= 50 and the error bound is smaller
    // than or equan to the default error bound.
    bool checkError =
        (error_ <= common::hll::kDefaultApproxDistinctStandardError ||
         numGroups >= 50);
    for (auto i = 0; i < numGroups; ++i) {
      const auto gap =
          combined->children().back()->as<SimpleVector<double>>()->valueAt(i);
      if (gap > 2 * error_) {
        largeGaps.push_back(gap);
        LOG(ERROR) << fmt::format(
            "approx_distinct(x, {}) is more than 2 stddev away from "
            "count(distinct x) at {}. Difference: {}. This is unusual, but doesn't necessarily "
            "indicate a bug.",
            error_,
            combined->toString(i),
            gap);
      }
    }
    if (!checkError) {
      LOG(WARNING) << fmt::format(
          "{} groups have large errors that exceed the error bound, but we don't fail the verification because current numGroups = {} and error bound is {}.",
          largeGaps.size(),
          numGroups,
          error_);
      return true;
    }

    // We expect large deviations (>2 stddev) in < 5% of values.
    if (numGroups >= 50) {
      return largeGaps.size() <= 0.05 * numGroups;
    }

    return largeGaps.empty();
  }

  void reset() override {
    expected_.reset();
  }

 private:
  static constexpr double kDefaultError = 0.023;

  static double extractError(
      const core::CallTypedExprPtr& call,
      const RowVectorPtr& input) {
    const auto& args = call->inputs();

    if (args.size() == 1) {
      return kDefaultError;
    }

    auto field = core::TypedExprs::asFieldAccess(args[1]);
    VELOX_CHECK_NOT_NULL(field);
    auto errorVector =
        input->childAt(field->name())->as<SimpleVector<double>>();
    return errorVector->valueAt(0);
  }

  static std::string makeCountDistinctCall(
      const core::AggregationNode::Aggregate& aggregate) {
    const auto& args = aggregate.call->inputs();
    VELOX_CHECK_GE(args.size(), 1)

    auto inputField = core::TypedExprs::asFieldAccess(args[0]);
    VELOX_CHECK_NOT_NULL(inputField)

    std::string countDistinctCall =
        fmt::format("count(distinct {})", inputField->name());

    if (aggregate.mask != nullptr) {
      countDistinctCall +=
          fmt::format(" filter (where {})", aggregate.mask->name());
    }

    return countDistinctCall;
  }

  static std::string makeCountDistinctWindowCall(
      const core::WindowNode::Function& function,
      const std::string& frame) {
    const auto& args = function.functionCall->inputs();
    VELOX_CHECK_GE(args.size(), 1)

    auto inputField = core::TypedExprs::asFieldAccess(args[0]);
    VELOX_CHECK_NOT_NULL(inputField)

    std::string countDistinctCall =
        fmt::format("\"$internal$count_distinct\"({})", inputField->name());

    if (function.ignoreNulls) {
      countDistinctCall += " ignore nulls";
    }
    countDistinctCall += " over(" + frame + ")";

    return countDistinctCall;
  }

  RowVectorPtr expected_;
  std::vector<std::string> groupingKeys_;
  std::string name_;
  double error_;
  bool verifyWindow_{false};
};

} // namespace facebook::velox::exec::test
