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

// Verifies results of approx_percentile by checking the range of percentiles
// represented by the result value and asserting that the requested percentile
// falls into that range within 'accuracy'.
class ApproxPercentileResultVerifier : public ResultVerifier {
 public:
  bool supportsCompare() override {
    return false;
  }

  bool supportsVerify() override {
    return true;
  }

  // Compute the range of percentiles represented by each of the input values.
  void initialize(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& groupingKeys,
      const core::AggregationNode::Aggregate& aggregate,
      const std::string& aggregateName) override {
    VELOX_CHECK(!input.empty());

    int64_t numInputs = 0;
    for (const auto& v : input) {
      numInputs += v->size();
    }

    const auto& args = aggregate.call->inputs();
    const auto& valueField = fieldName(args[0]);
    std::optional<std::string> weightField;
    if (args.size() >= 3 && args[1]->type()->isBigint()) {
      weightField = fieldName(args[1]);
    }

    groupingKeys_ = groupingKeys;
    name_ = aggregateName;

    percentiles_ = extractPercentiles(input, aggregate);
    VELOX_CHECK(!percentiles_.empty());

    accuracy_ = extractAccuracy(aggregate, input[0]);

    // Compute percentiles for all values.
    allRanges_ =
        computePercentiles(input, valueField, weightField, aggregate.mask);
    VELOX_CHECK_LE(allRanges_->size(), numInputs);
  }

  bool compare(
      const RowVectorPtr& /*result*/,
      const RowVectorPtr& /*altResult*/) override {
    VELOX_UNSUPPORTED();
  }

  bool verify(const RowVectorPtr& result) override {
    // Compute acceptable ranges of percentiles for each value in 'result'.
    auto ranges = getPercentileRanges(result);
    // VELOX_CHECK_EQ(ranges->size(), result->size() * percentiles_.size());

    auto& value = ranges->childAt(name_);
    auto* minPct = ranges->childAt("min_pct")->as<SimpleVector<double>>();
    auto* maxPct = ranges->childAt("max_pct")->as<SimpleVector<double>>();
    auto* pctIndex = ranges->childAt("pct_index")->as<SimpleVector<int64_t>>();

    for (auto i = 0; i < ranges->size(); ++i) {
      if (value->isNullAt(i)) {
        VELOX_CHECK(minPct->isNullAt(i));
        VELOX_CHECK(maxPct->isNullAt(i));
        continue;
      }

      VELOX_CHECK(!minPct->isNullAt(i));
      VELOX_CHECK(!maxPct->isNullAt(i));
      VELOX_CHECK(!pctIndex->isNullAt(i));

      const auto pct = percentiles_[pctIndex->valueAt(i)];

      std::pair<double, double> range{minPct->valueAt(i), maxPct->valueAt(i)};
      if (!checkPercentileGap(pct, range, accuracy_)) {
        return false;
      }
    }

    return true;
  }

  void reset() override {
    allRanges_.reset();
  }

 private:
  static constexpr double kDefaultAccuracy = 0.0133;

  static double extractAccuracy(
      const core::AggregationNode::Aggregate& aggregate,
      const RowVectorPtr& input) {
    const auto& args = aggregate.call->inputs();

    column_index_t accuracyIndex = 2;
    if (args.size() >= 3 && args[1]->type()->isBigint()) {
      // We have a 'w' argument.
      accuracyIndex = 3;
    }

    if (args.size() <= accuracyIndex) {
      return kDefaultAccuracy;
    }

    auto field = core::TypedExprs::asFieldAccess(args[accuracyIndex]);
    VELOX_CHECK_NOT_NULL(field);
    auto accuracyVector =
        input->childAt(field->name())->as<SimpleVector<double>>();
    return accuracyVector->valueAt(0);
  }

  static bool checkPercentileGap(
      double pct,
      const std::pair<double, double>& range,
      double accuracy) {
    double gap = 0.0;
    if (pct < range.first) {
      gap = range.first - pct;
    } else if (pct > range.second) {
      gap = pct - range.second;
    }

    if (gap > accuracy) {
      LOG(ERROR) << "approx_percentile(pct: " << pct
                 << ", accuracy: " << accuracy << ") is more than " << accuracy
                 << " away from acceptable range of [" << range.first << ", "
                 << range.second << "]. Difference: " << gap;
      return false;
    }

    return true;
  }

  static std::vector<std::string> append(
      const std::vector<std::string>& values,
      const std::vector<std::string>& newValues) {
    auto combined = values;
    combined.insert(combined.end(), newValues.begin(), newValues.end());
    return combined;
  }

  // Groups input by 'groupingKeys_'. Within each group, sorts data on
  // 'valueField', duplicates rows according to optional weight, filters out
  // NULLs and rows where mask is not true, then computes ranges of row numbers
  // and turns these into ranges of percentiles.
  //
  // @return A vector of grouping keys, followed by value column named 'name_',
  // followed by min_pct and max_pct columns.
  RowVectorPtr computePercentiles(
      const std::vector<RowVectorPtr>& input,
      const std::string& valueField,
      const std::optional<std::string>& weightField,
      const core::FieldAccessTypedExprPtr& mask) {
    VELOX_CHECK(!input.empty())
    const auto rowType = asRowType(input[0]->type());

    std::vector<std::string> projections = groupingKeys_;
    projections.push_back(fmt::format("{} as x", valueField));
    projections.push_back(fmt::format(
        "{} as w",
        weightField.has_value() ? weightField.value() : "1::bigint"));

    PlanBuilder planBuilder;
    planBuilder.values(input);

    if (mask != nullptr) {
      planBuilder.filter(mask->name());
    }

    planBuilder.project(projections).filter("x IS NOT NULL AND w > 0");

    std::string partitionByClause;
    if (!groupingKeys_.empty()) {
      partitionByClause =
          fmt::format("partition by {}", folly::join(", ", groupingKeys_));
    }

    std::vector<std::string> windowCalls = {
        // Compute the sum of all the weights.
        fmt::format(
            "sum(w) OVER ({} order by x range between unbounded preceding and unbounded following) as total",
            partitionByClause),
        // Compute the sum of the weights in the current frame.
        fmt::format(
            "sum(w) OVER ({} order by x range between current row and current row) "
            "as sum",
            partitionByClause),
        // Compute the sum of the weights of all frames up to and including the
        // current frame.
        fmt::format(
            "sum(w) OVER ({} order by x range between unbounded preceding and current row) "
            "as prefix_sum",
            partitionByClause),
    };

    planBuilder.window(windowCalls)
        .appendColumns({
            // The sum of the weights of all frames before the current frame
            // over the total sum.
            "(prefix_sum::double - sum::double) / total::double as lower",
            // The sum of the weights of all frames up to and including the
            // current frame over the total sum.
            "(prefix_sum::double) / total::double as upper",
        })
        .singleAggregation(
            append(groupingKeys_, {"x"}),
            {"min(lower) as min_pct", "max(upper) as max_pct"})
        .project(append(
            groupingKeys_,
            {fmt::format("x as {}", name_), "min_pct", "max_pct"}));

    auto plan = planBuilder.planNode();
    return AssertQueryBuilder(plan).copyResults(input[0]->pool());
  }

  static const std::string& fieldName(const core::TypedExprPtr& expression) {
    auto field = core::TypedExprs::asFieldAccess(expression);
    VELOX_CHECK_NOT_NULL(field);
    return field->name();
  }

  // Extract 'percentile' argument.
  static std::vector<double> extractPercentiles(
      const std::vector<RowVectorPtr>& input,
      const core::AggregationNode::Aggregate& aggregate) {
    const auto args = aggregate.call->inputs();
    column_index_t percentileIndex = 1;
    if (args.size() >= 3 && args[1]->type()->isBigint()) {
      percentileIndex = 2;
    }

    const auto& percentileExpr = args[percentileIndex];

    if (auto constantExpr = core::TypedExprs::asConstant(percentileExpr)) {
      if (constantExpr->type()->isDouble()) {
        return {constantExpr->value().value<double>()};
      }

      return toList(constantExpr->valueVector());
    }

    const auto& percentileVector = input[0]->childAt(fieldName(percentileExpr));

    if (percentileVector->type()->isDouble()) {
      VELOX_CHECK(!percentileVector->isNullAt(0));
      return {percentileVector->as<SimpleVector<double>>()->valueAt(0)};
    }

    return toList(percentileVector);
  }

  static std::vector<double> toList(const VectorPtr& vector) {
    VELOX_CHECK(vector->type()->equivalent(*ARRAY(DOUBLE())));

    DecodedVector decoded(*vector);
    auto arrayVector = decoded.base()->as<ArrayVector>();

    VELOX_CHECK(!decoded.isNullAt(0));
    const auto offset = arrayVector->offsetAt(decoded.index(0));
    const auto size = arrayVector->sizeAt(decoded.index(0));

    auto* elementsVector = arrayVector->elements()->as<SimpleVector<double>>();

    std::vector<double> percentiles;
    percentiles.reserve(size);
    for (auto i = 0; i < size; ++i) {
      VELOX_CHECK(!elementsVector->isNullAt(offset + i));
      percentiles.push_back(elementsVector->valueAt(offset + i));
    }
    return percentiles;
  }

  // For each row ([k1, k2,] x) in 'result', lookup min_pct and max_pct in
  // 'allRanges_'. Return a vector of ([k1, k2,] x, min_pct, max_pct) rows.
  RowVectorPtr getPercentileRanges(const RowVectorPtr& result) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodePtr expectedSource;
    core::PlanNodePtr actualSource;
    if (result->childAt(name_)->type()->isArray()) {
      expectedSource =
          PlanBuilder(planNodeIdGenerator)
              .values({allRanges_})
              .appendColumns({fmt::format(
                  "sequence(0, {}) as s", percentiles_.size() - 1)})
              .unnest(
                  append(groupingKeys_, {name_, "min_pct", "max_pct"}),
                  {"s"},
                  "pct_index")
              .project(append(
                  groupingKeys_,
                  {name_, "min_pct", "max_pct", "pct_index - 1 as pct_index"}))
              .planNode();

      actualSource = PlanBuilder(planNodeIdGenerator)
                         .values({result})
                         .unnest(groupingKeys_, {name_}, "pct_index")
                         .project(append(
                             groupingKeys_,
                             {
                                 fmt::format("{}_e as {}", name_, name_),
                                 "null::double as min_pct",
                                 "null::double as max_pct",
                                 "pct_index - 1 as pct_index",
                             }))
                         .planNode();
    } else {
      expectedSource = PlanBuilder(planNodeIdGenerator)
                           .values({allRanges_})
                           .appendColumns({"0 as pct_index"})
                           .planNode();

      actualSource = PlanBuilder(planNodeIdGenerator)
                         .values({result})
                         .appendColumns({
                             "null::double as min_pct",
                             "null::double as max_pct",
                             "0 as pct_index",
                         })
                         .planNode();
    }

    auto plan = PlanBuilder(planNodeIdGenerator)
                    .localPartition({}, {expectedSource, actualSource})
                    .singleAggregation(
                        append(groupingKeys_, {name_, "pct_index"}),
                        {
                            "count(1) as cnt",
                            "arbitrary(min_pct) as min_pct",
                            "arbitrary(max_pct) as max_pct",
                        })
                    .filter({"cnt = 2"})
                    .planNode();
    return AssertQueryBuilder(plan).copyResults(result->pool());
  }

  std::vector<std::string> groupingKeys_;
  std::string name_;
  std::vector<double> percentiles_;
  double accuracy_;
  RowVectorPtr allRanges_;
};

} // namespace facebook::velox::exec::test
