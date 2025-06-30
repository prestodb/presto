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

#include <cstdint>
#include <string>
#include "velox/core/PlanNode.h"
#include "velox/exec/fuzzer/ResultVerifier.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec::test {

/// Verifier for the noisy_sum_gaussian function.
/// The function takes 2-5 arguments:
/// If the signature takes 5 arguments, the function clips
/// the input to the specified bounds, and then sums the clipped values.
/// At the last step, it adds Gaussian noise to the sum with the specified
/// noise scale. Random seed is used to generate a determined sequence of
/// noises.
/// The idea of this verifier is to create a benchmark plan that simulates the
/// function without adding noise. Then we can compare the result of the actual
/// function with the result of the benchmark plan. If the difference is within
/// the expected range, we consider the result as correct.
/// To create the benchmark, we cannot use noisy_sum_gaussian directly and we
/// need to use a sequence of other functions to replicate the same behavior.
/// Basically we need to:
/// 1. Deduplicate the input with window function if distinct is true.
/// 2. Clip the input to the specified bounds and convert the aggregate column
///    to double to ensure the result is always double, keeping consistent with
///    noisy_sum_gaussian. This process returns a new vector with clipped input.
/// 3. Sum the clipped values.
/// 4. Call noisy_sum_gaussian on the input.
/// 5. Compare the result with the expected result.
class NoisySumResultVerifier : public ResultVerifier {
 public:
  bool supportsCompare() override {
    return false;
  }

  bool supportsVerify() override {
    return true;
  }

  void initialize(
      const std::vector<RowVectorPtr>& input,
      const std::vector<core::ExprPtr>& projections,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& aggregate,
      const std::string& aggregateName) override {
    VELOX_CHECK(!input.empty());

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
    // needed because the noisy_sum_gaussian function only return double
    // outputs.
    clipInput(deduplicatedInput);

    auto sumCall = fmt::format("sum({})", aggregateColumn_);

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

  bool compare(
      [[maybe_unused]] const RowVectorPtr& result,
      [[maybe_unused]] const RowVectorPtr& otherResult) override {
    VELOX_UNSUPPORTED();
  }

  bool verify(const RowVectorPtr& result) override {
    // The expected result and actual result are grouped by the same keys,
    // but the rows may be in different order. So we need to union the results.
    // Create sources for expected and actual results
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    auto expectedSource = PlanBuilder(planNodeIdGenerator)
                              .values({expectedNoNoise_})
                              .appendColumns({"'expected' as label"})
                              .planNode();
    auto actualSource = PlanBuilder(planNodeIdGenerator)
                            .values({result})
                            .appendColumns({"'actual' as label"})
                            .planNode();

    // Combine expected and actual results by grouping keys using map_agg
    auto mapAgg = fmt::format("map_agg(label, {}) as m", name_);
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .localPartition({}, {expectedSource, actualSource})
                    .singleAggregation(groupingKeys_, {mapAgg})
                    .project({"m['actual'] as a", "m['expected'] as e"})
                    .planNode();
    auto combined = AssertQueryBuilder(plan).copyResults(result->pool());

    // Extract actual and expected values, handling type differences
    auto actualColumn = combined->childAt(0);
    auto expectedColumn = combined->childAt(1);

    const auto numGroups = result->size();
    VELOX_CHECK_EQ(numGroups, combined->size());

    // Calculate allowed difference based on noise scale
    const int64_t deviationMultiple = 50;
    const double allowedFailureRate = 0.001;
    // When noise_scale is 0, we expect exact match.
    // But type conversion may lose precision, especially for very large result.
    // so we allow a small percentage of differences.
    const double allowedDifferencePercent = 0.000000001;
    const auto allowedDifference = deviationMultiple * noiseScale_;
    auto lowerBound = -allowedDifference;
    auto upperBound = allowedDifference;

    // Check each group's result
    int failures = 0;
    for (auto i = 0; i < numGroups; ++i) {
      // Skip verification for null rows
      if (expectedColumn->isNullAt(i) || actualColumn->isNullAt(i)) {
        continue;
      }

      const auto actualValue =
          actualColumn->as<SimpleVector<double>>()->valueAt(i);
      const auto expectedValue =
          expectedColumn->as<SimpleVector<double>>()->valueAt(i);
      const auto difference = actualValue - expectedValue;

      // Allow a small percentage of difference for noise_scale = 0
      lowerBound =
          std::min(lowerBound, -allowedDifferencePercent * expectedValue);
      upperBound =
          std::max(upperBound, allowedDifferencePercent * expectedValue);

      // Check if actual value is within expected +/- allowedDifference
      if (difference < lowerBound || difference > upperBound) {
        LOG(ERROR) << fmt::format(
            "noisy_sum_gaussian result is outside the expected range.\n"
            "  Group: {}\n"
            "  Actual: {}\n"
            "  Expected: {}\n"
            "  Difference: {}\n"
            "  Allowed range: [{}, {}] (noise_scale = {})",
            i,
            actualValue,
            expectedValue,
            difference,
            expectedValue + lowerBound,
            expectedValue + upperBound,
            noiseScale_);
        failures++;
      }
    }

    // Allow a very small percentage of failures for large result sets
    if (numGroups >= 50) {
      const auto maxFailures = static_cast<int>(allowedFailureRate * numGroups);
      if (failures > maxFailures) {
        LOG(ERROR) << fmt::format(
            "Too many failures: {} out of {} groups (max allowed: {})",
            failures,
            numGroups,
            maxFailures);
        return false;
      }
      return true;
    }

    // For small result sets, require all groups to pass
    return failures == 0;
  }

  void reset() override {
    noiseScale_ = 0.0;
    lowerBound_.reset();
    upperBound_.reset();
    name_.clear();
    groupingKeys_.clear();
    aggregateColumn_.clear();
    expectedNoNoise_.reset();
    clippedInput_.clear();
  }

 protected:
  std::vector<RowVectorPtr> deduplicateInput(
      const std::vector<RowVectorPtr>& input,
      const core::FieldAccessTypedExprPtr& mask) {
    // Deduplicate the input by using window functions to identify unique
    // combinations of grouping keys and aggregate column, preserving the
    // original row type.
    VELOX_CHECK(!input.empty());
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    // Create partition keys that include both the original grouping keys
    // and the aggregate column to ensure proper deduplication for DISTINCT
    // aggregation
    auto partitionKeys = groupingKeys_;
    partitionKeys.push_back(aggregateColumn_);

    // Build partition by clause for window function
    std::string partitionBy;
    if (!partitionKeys.empty()) {
      partitionBy =
          fmt::format("partition by {}", fmt::join(partitionKeys, ", "));
    }

    // Use row_number() window function to identify first occurrence of each
    // unique combination, then filter to keep only those rows(row number = 1)
    auto windowExpr = fmt::format("row_number() over ({}) as rn", partitionBy);

    auto builder = PlanBuilder(planNodeIdGenerator).values(input);

    // Apply mask filter first if it exists
    if (mask != nullptr) {
      builder = builder.filter(mask->name());
    }

    auto source = builder.window({windowExpr})
                      .filter("rn = 1")
                      .project(asRowType(input[0]->type())->names())
                      .planNode();
    auto deduplicated =
        AssertQueryBuilder(source).copyResults(input[0]->pool());
    return {deduplicated};
  }

  void extractNoiseScaleAndBound(
      const RowVectorPtr& input,
      const std::vector<core::TypedExprPtr>& args) {
    auto secondArg = input->childAt(1);
    if (secondArg->type()->isDouble()) {
      noiseScale_ = secondArg->as<SimpleVector<double>>()->valueAt(0);
    } else if (secondArg->type()->isBigint()) {
      noiseScale_ = static_cast<double>(
          secondArg->as<SimpleVector<int64_t>>()->valueAt(0));
    }

    // Extract lower and upper bound if they exist
    if (args.size() > 3) {
      auto thirdArg = input->childAt(2);
      if (thirdArg->type()->isDouble()) {
        lowerBound_ = thirdArg->as<SimpleVector<double>>()->valueAt(0);
      } else if (thirdArg->type()->isBigint()) {
        lowerBound_ = static_cast<double>(
            thirdArg->as<SimpleVector<int64_t>>()->valueAt(0));
      }

      auto fourthArg = input->childAt(3);
      if (fourthArg->type()->isDouble()) {
        upperBound_ = fourthArg->as<SimpleVector<double>>()->valueAt(0);
      } else if (fourthArg->type()->isBigint()) {
        upperBound_ = static_cast<double>(
            fourthArg->as<SimpleVector<int64_t>>()->valueAt(0));
      }
    }
  }

  void clipInput(const std::vector<RowVectorPtr>& input) {
    auto pool = input[0]->pool();
    for (const auto& rowVector : input) {
      std::vector<VectorPtr> clippedChildren;

      for (auto i = 0; i < rowVector->childrenSize(); ++i) {
        auto column = rowVector->childAt(i);

        if (i == 0) {
          // Clip first column values within bounds and convert to double
          auto size = column->size();
          auto clippedColumn = BaseVector::create(DOUBLE(), size, pool);

          for (auto j = 0; j < size; ++j) {
            if (column->isNullAt(j)) {
              clippedColumn->setNull(j, true);
            } else {
              auto type = column->type()->kind();
              VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
                  updateValueByType, type, column, clippedColumn, j);
            }
          }
          clippedChildren.push_back(clippedColumn);
        } else {
          // For other columns, just copy them as-is
          clippedChildren.push_back(column);
        }
      }

      // Create a new RowType with first column changed to double
      auto originalRowType = asRowType(rowVector->type());
      std::vector<std::string> names = originalRowType->names();
      std::vector<TypePtr> types = originalRowType->children();

      // Change first column type to double
      types[0] = DOUBLE();

      auto newRowType = ROW(std::move(names), std::move(types));

      auto clippedRowVector = std::make_shared<RowVector>(
          pool, newRowType, nullptr, rowVector->size(), clippedChildren);

      clippedInput_.push_back(clippedRowVector);
    }
  }

  // Helper function to update the value in the clipped column based on the
  // data type.
  template <TypeKind TData>
  void updateValueByType(
      VectorPtr& column,
      VectorPtr& clippedColumn,
      vector_size_t i) {
    using T = typename TypeTraits<TData>::NativeType;
    // Skip testing decimal types.
    // Skip non-numeric types.
    if constexpr (
        std::is_same_v<T, TypeTraits<TypeKind::TIMESTAMP>> ||
        std::is_same_v<T, TypeTraits<TypeKind::VARBINARY>> ||
        std::is_same_v<T, TypeTraits<TypeKind::VARCHAR>> ||
        std::is_same_v<T, facebook::velox::StringView> ||
        std::is_same_v<T, facebook::velox::Timestamp>) {
      VELOX_FAIL("NoisySumGaussianAggregate does not support this data type.");
    } else {
      // Convert the value to double and set it in the clipped column
      auto rawValue = column->asFlatVector<T>()->valueAt(i);
      // Skip NaN.
      if (std::isnan(rawValue)) {
        clippedColumn->setNull(i, true);
        return;
      }

      // Apply clipping if bounds are set
      double clippedValue = static_cast<double>(rawValue);
      if (lowerBound_.has_value() && upperBound_.has_value()) {
        clippedValue =
            std::min(*upperBound_, std::max(*lowerBound_, clippedValue));
      }

      clippedColumn->asFlatVector<double>()->set(i, clippedValue);
    }
  }

  double noiseScale_{0.0};
  std::optional<double> lowerBound_;
  std::optional<double> upperBound_;
  std::string name_;
  std::vector<std::string> groupingKeys_;
  std::string aggregateColumn_;
  RowVectorPtr expectedNoNoise_;
  std::vector<RowVectorPtr> clippedInput_;
};

} // namespace facebook::velox::exec::test
