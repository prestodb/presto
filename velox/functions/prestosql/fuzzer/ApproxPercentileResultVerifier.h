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
    verifyWindow_ = false;

    int64_t numInputs = 0;
    for (const auto& v : input) {
      numInputs += v->size();
    }

    groupingKeys_ = groupingKeys;
    name_ = aggregateName;

    const auto& [valueField, weightField] =
        extractValueAndWeight(aggregate.call);
    extractPercentileAndAccuracy(aggregate.call, input);

    // Compute percentiles for all values.
    allRanges_ =
        computePercentiles(input, valueField, weightField, aggregate.mask);
    VELOX_CHECK_LE(allRanges_->size(), numInputs);
  }

  void initializeWindow(
      const std::vector<RowVectorPtr>& input,
      const std::vector<std::string>& partitionByKeys,
      const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders,
      const core::WindowNode::Function& function,
      const std::string& /*frame*/,
      const std::string& windowName) override {
    VELOX_CHECK(!input.empty());
    verifyWindow_ = true;

    groupingKeys_ = partitionByKeys;
    name_ = windowName;

    const auto& [valueField, weightField] =
        extractValueAndWeight(function.functionCall);
    bool isArrayPercentile =
        extractPercentileAndAccuracy(function.functionCall, input);

    allRanges_ = computePercentilesForWindow(
        input,
        valueField,
        weightField,
        sortingKeysAndOrders,
        function.frame,
        function.functionCall->type(),
        isArrayPercentile);
  }

  bool compare(
      const RowVectorPtr& /*result*/,
      const RowVectorPtr& /*altResult*/) override {
    VELOX_UNSUPPORTED();
  }

  bool verify(const RowVectorPtr& result) override {
    // Compute acceptable ranges of percentiles for each value in 'result'.
    RowVectorPtr ranges;
    if (verifyWindow_) {
      ranges = getPercentileRangesForWindow(result);
    } else {
      ranges = getPercentileRanges(result);
    }

    auto& value = ranges->childAt(name_);
    auto* minPct = ranges->childAt("min_pct")->as<SimpleVector<double>>();
    auto* maxPct = ranges->childAt("max_pct")->as<SimpleVector<double>>();
    auto* pctIndex = ranges->childAt("pct_index")->as<SimpleVector<int64_t>>();

    // Number of non-null rows in the actual result.
    auto numNonNull = 0;
    for (auto i = 0; i < ranges->size(); ++i) {
      if (value->isNullAt(i)) {
        VELOX_CHECK(minPct->isNullAt(i));
        VELOX_CHECK(maxPct->isNullAt(i));
        continue;
      }
      numNonNull++;
      VELOX_CHECK(!minPct->isNullAt(i));
      VELOX_CHECK(!maxPct->isNullAt(i));
      VELOX_CHECK(!pctIndex->isNullAt(i));

      const auto pct = percentiles_[pctIndex->valueAt(i)];

      std::pair<double, double> range{minPct->valueAt(i), maxPct->valueAt(i)};
      if (!checkPercentileGap(pct, range, accuracy_)) {
        return false;
      }
    }
    if (verifyWindow_ && numNonNull != allRanges_->size()) {
      LOG(ERROR) << fmt::format(
          "Expected result contains {} non-null rows while the actual result contains {}.",
          allRanges_->size(),
          numNonNull);
      return false;
    }

    return true;
  }

  void reset() override {
    allRanges_.reset();
  }

 private:
  static constexpr double kDefaultAccuracy = 0.0133;

  // Extracts a pair of [valueField, weightField] from functionCall. weightField
  // is an optional.
  std::pair<std::string, std::optional<std::string>> extractValueAndWeight(
      const core::CallTypedExprPtr& functionCall) {
    const auto& args = functionCall->inputs();
    const auto& valueField = fieldName(args[0]);
    std::optional<std::string> weightField;
    if (args.size() >= 3 && args[1]->type()->isBigint()) {
      weightField = fieldName(args[1]);
    }
    return std::make_pair(valueField, weightField);
  }

  // Extracts the percentile(s) to percentiles_ and the accuracy to accuracy_.
  // Return a boolean indicating whether the percentile is an array.
  bool extractPercentileAndAccuracy(
      const core::CallTypedExprPtr& functionCall,
      const std::vector<RowVectorPtr>& input) {
    bool isArrayPercentile;
    percentiles_ = extractPercentiles(input, functionCall, isArrayPercentile);
    VELOX_CHECK(!percentiles_.empty());

    accuracy_ = extractAccuracy(functionCall, input[0]);
    return isArrayPercentile;
  }

  static double extractAccuracy(
      const core::CallTypedExprPtr& functionCall,
      const RowVectorPtr& input) {
    const auto& args = functionCall->inputs();

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

  std::string getFrameClause(const core::WindowNode::Frame& frame) {
    std::stringstream ss;
    ss << core::WindowNode::windowTypeName(frame.type) << " between ";
    if (frame.startValue) {
      ss << frame.startValue->toString() << " ";
    }
    ss << core::WindowNode::boundTypeName(frame.startType) << " and ";
    if (frame.endValue) {
      ss << frame.endValue->toString() << " ";
    }
    ss << core::WindowNode::boundTypeName(frame.endType);
    return ss.str();
  }

  std::string getOrderByClause(
      const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders) {
    if (sortingKeysAndOrders.empty()) {
      return "";
    }
    std::stringstream ss;
    ss << "order by ";
    for (auto i = 0; i < sortingKeysAndOrders.size(); ++i) {
      if (i > 0) {
        ss << ", ";
      }
      ss << sortingKeysAndOrders[i].key_ << " "
         << sortingKeysAndOrders[i].sortOrder_.toString();
    }
    return ss.str();
  }

  std::string getPartitionByClause(
      const std::vector<std::string>& partitionByKeys) {
    if (partitionByKeys.empty()) {
      return "";
    }
    return "partition by " + folly::join(", ", partitionByKeys);
  }

  // For each input row, calculates a map of {value : [order_min, order_max]} as
  // 'expected' for every distinct value in the window frame of the current row,
  // and the weighted total number of values in the frame as 'cnt'. 'order_min'
  // is the rank right before the first appearance of 'value' when values in the
  // current frame are sorted by sortingKeysAndOrders and 'order_max' is the
  // rank of the last appearance of 'value'. For example, for a table 't(c0, c1,
  // c2, weight, row_num)' and a window operation 'approx_percentile(c0,
  // percentile) over (partition by c1 order by c2 desc rows between 1 preceding
  // and 1 following)', this method essentially returns the result of the
  // following query:
  //  SELECT
  //      c1,
  //      row_num,
  //      NULL AS actual,
  //      MAP_AGG(bucket_element, order_pair) AS expected,
  //      ARBITRARY(weight_total) AS cnt
  //  FROM (
  //      SELECT
  //          c1,
  //          row_num,
  //          bucket_element,
  //          CAST(
  //              ROW(COALESCE(order_min, 0), order_max) AS ROW(
  //                  order_min BIGINT,
  //                  order_max BIGINT
  //              )
  //          ) AS order_pair,
  //          weight_total
  //      FROM (
  //          SELECT
  //              c1,
  //              row_num,
  //              bucket_element,
  //              weight,
  //              SUM(weight) OVER (
  //                  PARTITION BY
  //                      c1,
  //                      row_num
  //                  ORDER BY
  //                      bucket_element ASC
  //                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  //              ) AS weight_total,
  //              SUM(weight) OVER (
  //                  PARTITION BY
  //                      c1,
  //                      row_num
  //                  ORDER BY
  //                      bucket_element ASC
  //                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  //              ) AS order_min,
  //              SUM(weight) OVER (
  //                  PARTITION BY
  //                      c1,
  //                      row_num
  //                  ORDER BY
  //                      bucket_element ASC
  //                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  //              ) AS order_max
  //          FROM (
  //              SELECT
  //                  c1,
  //                  row_num,
  //                  bucket_element,
  //                  bucket_weight AS weight
  //              FROM (
  //                  SELECT
  //                      c1,
  //                      row_num,
  //                      bucket_element,
  //                      bucket_weight
  //                  FROM (
  //                      SELECT
  //                          c1,
  //                          row_num,
  //                          TRANSFORM_VALUES(bucket, (k, v) -> ARRAY_SUM(v))
  //                          AS bucket
  //                      FROM (
  //                          SELECT
  //                              c1,
  //                              row_num,
  //                              MULTIMAP_AGG(c0, weight) OVER (
  //                                  PARTITION BY
  //                                      c1
  //                                  ORDER BY
  //                                      c2 DESC
  //                                  ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
  //                              ) AS bucket
  //                          FROM (
  //                              VALUES
  //                                  (1, TRUE, 1, 2, 1),
  //                                  (2, TRUE, 3, 1, 2),
  //                                  (1, TRUE, 4, 1, 3),
  //                                  (4, TRUE, 5, 1, 4),
  //                                  (1, FALSE, 1, 2, 5),
  //                                  (2, FALSE, 3, 1, 6),
  //                                  (3, FALSE, 4, 1, 7),
  //                                  (4, FALSE, 5, 1, 8)
  //                          ) t(c0, c1, c2, weight, row_num)
  //                      )
  //                  ) bucketed(c1, row_num, bucket)
  //                  CROSS JOIN UNNEST(bucket) AS tmp(bucket_element,
  //                  bucket_weight)
  //              )
  //          )
  //      )
  //  )
  //  GROUP BY
  //      c1,
  //      row_num
  RowVectorPtr computePercentilesForWindow(
      const std::vector<RowVectorPtr>& input,
      const std::string& valueField,
      const std::optional<std::string>& weightField,
      const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders,
      const core::WindowNode::Frame& frame,
      const TypePtr& resultType,
      bool isArray) {
    VELOX_CHECK(!input.empty())
    const auto rowType = asRowType(input[0]->type());
    const bool weighted = weightField.has_value();

    std::vector<std::string> projections = groupingKeys_;
    for (const auto& sortingKey : sortingKeysAndOrders) {
      if (sortingKey.key_ != "row_number") {
        projections.push_back(sortingKey.key_);
      }
    }
    projections.push_back("row_number");
    projections.push_back(fmt::format("{} as x", valueField));
    projections.push_back(
        fmt::format("{} as w", weighted ? weightField.value() : "1::bigint"));

    PlanBuilder planBuilder;
    planBuilder.values(input).project(projections).filter("w > 0");

    auto partitionByKeysWithRowNumber =
        getPartitionByClause(append(groupingKeys_, {"row_number"}));
    planBuilder
        .window({fmt::format(
            "multimap_agg(x, w) over ({} {} {}) as bucket",
            getPartitionByClause(groupingKeys_),
            getOrderByClause(sortingKeysAndOrders),
            getFrameClause(frame))})
        .project(append(
            groupingKeys_,
            {"row_number",
             "transform_values(bucket, (k, v) -> array_sum(v)) as bucket"}))
        .unnest(append(groupingKeys_, {"row_number"}), {"bucket"})
        .project(append(
            groupingKeys_, {"row_number", "bucket_k", "bucket_v as weight"}))
        .window(
            {fmt::format(
                 "sum(weight) over ({} order by bucket_k asc rows between unbounded preceding and unbounded following) as cnt",
                 partitionByKeysWithRowNumber),
             fmt::format(
                 "sum(weight) over ({} order by bucket_k asc rows between unbounded preceding and 1 preceding) as order_min",
                 partitionByKeysWithRowNumber),
             fmt::format(
                 "sum(weight) over ({} order by bucket_k asc rows between unbounded preceding and current row) as order_max",
                 partitionByKeysWithRowNumber)})
        .project(append(
            groupingKeys_,
            {"row_number",
             "bucket_k as element",
             "cnt",
             "row_constructor(coalesce(order_min, 0), order_max) as order_pair"}))
        .singleAggregation(
            append(groupingKeys_, {"row_number"}),
            {"map_agg(element, order_pair) as expected",
             "arbitrary(cnt) as cnt"});

    if (isArray) {
      std::stringstream ss;
      toTypeSql(resultType->asArray().elementType(), ss);

      planBuilder
          .appendColumns(
              {fmt::format("sequence(1, {}) as seq", percentiles_.size())})
          .unnest(
              append(groupingKeys_, {"row_number", "expected", "cnt"}), {"seq"})
          .project(append(
              groupingKeys_,
              {"row_number",
               fmt::format("cast(null as {}) as actual_e", ss.str()),
               "seq_e as pct_index",
               "expected",
               "cnt"}));
    } else {
      std::stringstream ss;
      toTypeSql(resultType, ss);

      planBuilder.project(append(
          groupingKeys_,
          {"row_number",
           fmt::format("cast(null as {}) as actual", ss.str()),
           "expected",
           "cnt"}));
    }

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
      const core::CallTypedExprPtr& functionCall,
      bool& isArray) {
    const auto args = functionCall->inputs();
    column_index_t percentileIndex = 1;
    if (args.size() >= 3 && args[1]->type()->isBigint()) {
      percentileIndex = 2;
    }

    const auto& percentileExpr = args[percentileIndex];

    if (auto constantExpr = core::TypedExprs::asConstant(percentileExpr)) {
      if (constantExpr->type()->isDouble()) {
        isArray = false;
        return {constantExpr->value().value<double>()};
      }
      isArray = true;
      return toList(constantExpr->valueVector());
    }

    const auto& percentileVector = input[0]->childAt(fieldName(percentileExpr));

    if (percentileVector->type()->isDouble()) {
      VELOX_CHECK(!percentileVector->isNullAt(0));
      isArray = false;
      return {percentileVector->as<SimpleVector<double>>()->valueAt(0)};
    }
    isArray = true;
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

  // For each row ([k1, k2,] x) in 'result', lookup min_pct and max_pct in
  // 'allRanges_'. Return a vector of ([k1, k2,] x, min_pct, max_pct) rows.
  // For example, for an actual result table 't2(c1, row_num, actual)' where c1
  // is the partition-by key, this method essentially returns the result of the
  // following query:
  // SELECT
  //     c1,
  //     actual,
  //     CAST(order_pair.order_min AS DOUBLE) / CAST(cnt AS DOUBLE),
  //     CAST(order_pair.order_max AS DOUBLE) / CAST(cnt AS DOUBLE)
  // FROM (
  //     SELECT
  //         c1,
  //         actual,
  //         expected,
  //         value,
  //         order_pair,
  //         cnt
  //     FROM (
  //         SELECT
  //             c1,
  //             ARBITRARY(actual) AS actual,
  //             ARBITRARY(expected) AS expected,
  //             ARBITRARY(cnt) AS cnt
  //         FROM (
  //             SELECT
  //                 *
  //             FROM (
  //                 VALUES
  //                     (TRUE, 1, 1, NULL, NULL),
  //                     (FALSE, 7, 3, NULL, NULL)
  //             ) t2(c1, row_num, actual, expected, cnt)
  //
  //             UNION ALL
  //
  //             SELECT
  //                 *
  //             FROM allRanges_
  //         )
  //         GROUP BY
  //             c1,
  //             row_num
  //     ) combined(c1, actual, expected, cnt)
  //     CROSS JOIN UNNEST(expected) AS tmp(value, order_pair)
  //     WHERE
  //         value = actual
  // )
  RowVectorPtr getPercentileRangesForWindow(const RowVectorPtr& result) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodePtr expectedSource;
    core::PlanNodePtr actualSource;
    core::PlanNodePtr plan;
    expectedSource =
        PlanBuilder(planNodeIdGenerator).values({allRanges_}).planNode();
    auto expectedType = allRanges_->type()->asRow().findChild("expected");
    std::stringstream ss;
    toTypeSql(expectedType, ss);
    auto expectedTypeSql = ss.str();

    if (result->childAt(name_)->type()->isArray()) {
      actualSource =
          PlanBuilder(planNodeIdGenerator)
              .values({result})
              .project(append(
                  groupingKeys_,
                  {"row_number",
                   fmt::format("{} as actual", name_),
                   fmt::format("cast(null as {}) as expected", expectedTypeSql),
                   "cast(null as bigint) as cnt"}))
              .unnest(
                  append(groupingKeys_, {"row_number", "expected", "cnt"}),
                  {"actual"},
                  "pct_index")
              .project(append(
                  groupingKeys_,
                  {"row_number", "actual_e", "pct_index", "expected", "cnt"}))
              .planNode();

      plan =
          PlanBuilder(planNodeIdGenerator)
              .localPartition({}, {expectedSource, actualSource})
              .singleAggregation(
                  append(groupingKeys_, {"pct_index", "row_number"}),
                  {"arbitrary(actual_e) as actual",
                   "arbitrary(expected) as expected",
                   "arbitrary(cnt) as cnt"})
              .unnest(
                  append(groupingKeys_, {"actual", "pct_index", "cnt"}),
                  {"expected"})
              .filter("actual = expected_k")
              .project(append(
                  groupingKeys_,
                  {fmt::format("actual as {}", name_),
                   "pct_index - 1 as pct_index",
                   "cast(expected_v.c1 as double) / cast(cnt as double) as min_pct",
                   "cast(expected_v.c2 as double) / cast(cnt as double) as max_pct"}))
              .planNode();

    } else {
      actualSource =
          PlanBuilder(planNodeIdGenerator)
              .values({result})
              .project(append(
                  groupingKeys_,
                  {"row_number",
                   fmt::format("{} as actual", name_),
                   fmt::format("cast(null as {}) as expected", expectedTypeSql),
                   "cast(null as bigint) as cnt"}))
              .planNode();

      plan =
          PlanBuilder(planNodeIdGenerator)
              .localPartition({}, {expectedSource, actualSource})
              .singleAggregation(
                  append(groupingKeys_, {"row_number"}),
                  {"arbitrary(actual) as actual",
                   "arbitrary(expected) as expected",
                   "arbitrary(cnt) as cnt"})
              .unnest(append(groupingKeys_, {"actual", "cnt"}), {"expected"})
              .filter("actual = expected_k")
              .project(append(
                  groupingKeys_,
                  {fmt::format("actual as {}", name_),
                   "0 as pct_index",
                   "cast(expected_v.c1 as double) / cast(cnt as double) as min_pct",
                   "cast(expected_v.c2 as double) / cast(cnt as double) as max_pct"}))
              .planNode();
    }
    return AssertQueryBuilder(plan).copyResults(result->pool());
  }

  std::vector<std::string> groupingKeys_;
  std::string name_;
  std::vector<double> percentiles_;
  double accuracy_;
  RowVectorPtr allRanges_;
  TypePtr resultType_;
  bool verifyWindow_;
};

} // namespace facebook::velox::exec::test
