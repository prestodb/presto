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

#include "velox/functions/prestosql/fuzzer/MinMaxByResultVerifier.h"

namespace facebook::velox::exec::test {

void MinMaxByResultVerifier::initialize(
    const std::vector<RowVectorPtr>& input,
    const std::vector<std::string>& groupingKeys,
    const core::AggregationNode::Aggregate& aggregate,
    const std::string& aggregateName) {
  if (aggregate.call->inputs().size() > 2) {
    minMaxByN_ = true;
  }

  std::stringstream ss;
  toTypeSql(aggregate.call->type(), ss);
  aggregateTypeSql_ = ss.str();

  // Suppose the original query is below
  // SELECT
  //     MIN_BY(x, y, 2) FILTER(
  //         WHERE
  //             a
  //     )
  // FROM (
  //     VALUES
  //         (1, 'a', 'g1', TRUE),
  //         (2, 'a', 'g1', FALSE),
  //         (3, 'b', 'g1', TRUE),
  //         (4, 'a', 'g1', TRUE),
  //         (5, NULL, 'g2', TRUE),
  //         (6, 'b', 'g2', TRUE),
  //         (7, 'a', 'g2', TRUE)
  // ) t(x, y, b, a)
  // GROUP BY
  //     b
  // , we construct a expected query as follows:
  // SELECT
  //     ARRAY_AGG(
  //         expected
  //         ORDER BY
  //             y ASC nulls last
  //     )
  // FROM (
  //     SELECT
  //         b,
  //         y,
  //         expected
  //     FROM (
  //         SELECT
  //             b,
  //             y,
  //             IF (y IS NULL, CAST(NULL AS ARRAY(BIGINT)), expected) AS
  //             expected
  //         FROM (
  //             SELECT
  //                 b,
  //                 y,
  //                 ARRAY_AGG(x) AS expected
  //             FROM (
  //                 VALUES
  //                     (1, 'a', 'g1', TRUE),
  //                     (2, 'a', 'g1', FALSE),
  //                     (3, 'b', 'g1', TRUE),
  //                     (4, 'a', 'g1', TRUE),
  //                     (5, NULL, 'g2', TRUE),
  //                     (6, 'a', 'g2', TRUE),
  //                     (7, 'a', 'g2', TRUE)
  //             ) t(x, y, b, a)
  //             WHERE
  //                 a
  //             GROUP BY
  //                 b,
  //                 y
  //         )
  //     )
  //     WHERE
  //         expected IS NOT NULL
  // )
  // GROUP BY
  //     b
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = PlanBuilder(planNodeIdGenerator, input[0]->pool()).values(input);
  // Filter out masked rows first so that groups with all rows filtered out
  // won't take a row_number during the filtering later.
  if (aggregate.mask != nullptr) {
    plan = plan.filter(aggregate.mask->name());
  }

  // First, we compute array_agg grouped by the original grouping keys
  // plus y to get the buckets of x values by the same y. Use
  // $internal$array_agg here to make sure we don't ignore NULLs.
  auto yColumn = extractYColumnName(aggregate);
  auto groupingKeysAndY = combine(groupingKeys, {yColumn});
  plan =
      plan.singleAggregation(groupingKeysAndY, {makeArrayAggCall(aggregate)});

  // When y is NULL, min_by/max_by doesn't return the x values associated with
  // it. So remove the x bucket for NULL y.
  std::string expectColumn;
  if (minMaxByN_) {
    expectColumn = fmt::format(
        "if ({} is null, cast(NULL as {}), expected) as expected",
        yColumn,
        aggregateTypeSql_);
  } else {
    expectColumn = fmt::format(
        "if ({} is null, cast(NULL as {}[]), expected) as expected",
        yColumn,
        aggregateTypeSql_);
  }
  auto projectColumnsForNullY = combine(groupingKeysAndY, {expectColumn});
  plan = plan.project(projectColumnsForNullY).filter("expected is not null");

  // Aggregate buckets in order of y into a list per aggregation group.
  plan = plan.singleAggregation(
      groupingKeys,
      {fmt::format(
          "array_agg(expected order by {} {} nulls last) as expected",
          yColumn,
          minBy_ ? "asc" : "desc")});

  // Add a column of aggregateTypeSql_ of all nulls so that we can union the
  // expected list of buckets with the actual result in the verify method.
  auto nullColumn =
      fmt::format("cast(NULL as {}) as {}", aggregateTypeSql_, aggregateName);
  auto finalProjectColumns = combine(groupingKeys, {nullColumn, "expected"});
  plan = plan.project(finalProjectColumns);

  expected_ = AssertQueryBuilder(plan.planNode()).copyResults(input[0]->pool());
  groupingKeys_ = groupingKeys;
  name_ = aggregateName;
}

bool MinMaxByResultVerifier::verify(const RowVectorPtr& result) {
  // Union 'result' with 'expected_', group by on 'groupingKeys_' and produce
  // pairs of actual and expected values per group. We cannot use join because
  // grouping keys may have nulls.
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto expectedSource =
      PlanBuilder(planNodeIdGenerator).values({expected_}).planNode();

  // Append a NULL column for the expected column to 'result' so that we can
  // union 'result' with 'expected_'.
  std::string nullExpected;
  if (minMaxByN_) {
    nullExpected =
        fmt::format("cast(NULL as {}[]) as expected", aggregateTypeSql_);
  } else {
    nullExpected =
        fmt::format("cast(NULL as {}[][]) as expected", aggregateTypeSql_);
  }
  auto actualSource = PlanBuilder(planNodeIdGenerator, result->pool())
                          .values({result})
                          .appendColumns({nullExpected})
                          .planNode();

  core::PlanNodePtr plan;
  if (minMaxByN_) {
    // Construct a query that checks every element in 'actualSource.a0' is
    // contained in 'expectedSource.expected' of the corresponding group:
    // SELECT
    //     IF (CARDINALITY(a) > 0, a[1], CAST(NULL AS {})) AS a
    //     IF (CARDINALITY(e) > 0, e[1], ARRAY[CAST(NULL AS {})]) AS e
    // FROM (
    //     SELECT
    //         remove_nulls(a) AS a,
    //         remove_nulls(e) AS e
    //     FROM (
    //         SELECT
    //             ARRAY_AGG(a0) AS a,
    //             ARRAY_AGG(expected) AS e
    //         FROM (
    //             SELECT
    //                 *
    //             FROM expected
    //
    //             UNION ALL
    //
    //             SELECT
    //                 *
    //             FROM actual
    //         )
    //         GROUP BY
    //             b
    //     )
    // )
    plan =
        PlanBuilder(planNodeIdGenerator, result->pool())
            .localPartition({}, {expectedSource, actualSource})
            // Bring the actual and corresponding expected to the same row.
            .singleAggregation(
                groupingKeys_,
                {fmt::format("array_agg({}) as a", name_),
                 "array_agg(expected) as e"})
            .project({"remove_nulls(a) as a", "remove_nulls(e) as e"})
            // If a or e becomes empty, it means the original actual or
            // expected was NULL and was removed at the last step. So recover
            // them to be NULL. Otherwise, a and e should have only one
            // element that is the original actual and expected, so extract
            // it.
            .project(
                {fmt::format(
                     "if (cardinality(a) > 0, a[1], cast(NULL as {})) as a",
                     aggregateTypeSql_),
                 fmt::format(
                     "if (cardinality(e) > 0, e[1], array[cast(NULL as {})]) as e",
                     aggregateTypeSql_)})
            .planNode();
  } else {
    // Construct a query that checks 'actualSource.a0' is contained in
    // 'expectedSource.expected'. This is similar to the query above, but has
    // the top-level projection replaced with the last project below.
    plan =
        PlanBuilder(planNodeIdGenerator, result->pool())
            .localPartition({}, {expectedSource, actualSource})
            .singleAggregation(
                groupingKeys_,
                {fmt::format("array_agg({}) as a", name_),
                 "array_agg(expected) as e"})
            .project({"remove_nulls(a) as a", "remove_nulls(e) as e"})
            // Wrap the actual result in an array so that it can use the same
            // check logic for 3-arg min_by/max_by subsequently.
            .project(
                {fmt::format(
                     "array[switch(cardinality(a) > 0, a[1], cast(null as {}))] as a",
                     aggregateTypeSql_),
                 fmt::format(
                     "switch(cardinality(e) > 0, e[1], array[cast(null as {}[])]) as e",
                     aggregateTypeSql_)})
            .planNode();
  }

  // Check that elements in the actual result fall in the buckets in expected
  // in order.
  auto actualAndExpected = AssertQueryBuilder(plan).copyResults(result->pool());
  auto numGroups = result->size();
  VELOX_CHECK_EQ(numGroups, actualAndExpected->size());

  auto actualColumn =
      actualAndExpected->as<RowVector>()->childAt(0)->as<ArrayVector>();
  auto expectedColumn =
      actualAndExpected->as<RowVector>()->childAt(1)->as<ArrayVector>();
  for (auto i = 0; i < numGroups; ++i) {
    if (actualColumn->isNullAt(i)) {
      VELOX_CHECK(containsNull(expectedColumn, i));
    } else {
      VELOX_CHECK(!expectedColumn->isNullAt(i));

      auto actualOffset = actualColumn->offsetAt(i);
      auto actualSize = actualColumn->sizeAt(i);
      auto expectedOffset = expectedColumn->offsetAt(i);
      auto expectedSize = expectedColumn->sizeAt(i);
      VELOX_CHECK_GE(actualSize, 0);
      VELOX_CHECK_GE(expectedSize, 0);

      auto actualElements = actualColumn->elements();
      auto expectedInnerBuckets = expectedColumn->elements()->as<ArrayVector>();
      auto expectedBucketElements = expectedInnerBuckets->elements();
      auto bucketIndex = 0;
      auto remainingSize = actualSize;
      while (remainingSize > 0) {
        VELOX_CHECK_LT(
            bucketIndex,
            expectedSize,
            "Elements in acutal result not found in expected buckets. ActualAndExpected at row {}: {}.\n",
            i,
            actualAndExpected->toString(i));
        if (expectedInnerBuckets->isNullAt(expectedOffset + bucketIndex)) {
          if (!minMaxByN_ && actualElements->isNullAt(actualOffset)) {
            // min_by/max_by(x, y) returns NULL because all rows in the group
            // are masked out.
            remainingSize -= 1;
            actualOffset += 1;
            bucketIndex++;
          } else {
            bucketIndex++;
          }
          continue;
        }
        auto currentBucketSize =
            expectedInnerBuckets->sizeAt(expectedOffset + bucketIndex);
        auto currentBucketOffset =
            expectedInnerBuckets->offsetAt(expectedOffset + bucketIndex);
        auto numElementFromCurrentBucket =
            std::min(currentBucketSize, remainingSize);
        SelectivityVector memo(currentBucketSize, true);

        for (auto j = 0; j < numElementFromCurrentBucket; ++j) {
          if (getElementIndexInBucketWithMemo(
                  actualElements,
                  actualOffset + j,
                  expectedBucketElements,
                  currentBucketOffset,
                  currentBucketSize,
                  memo) == -1) {
            VELOX_FAIL(
                "Element in actual result not found in expected buckets.");
          }
        }

        remainingSize -= numElementFromCurrentBucket;
        actualOffset += numElementFromCurrentBucket;
        bucketIndex++;
      }
    }
  }
  return true;
}

std::vector<std::string> MinMaxByResultVerifier::combine(
    const std::vector<std::string>& op1,
    const std::vector<std::string>& op2) {
  std::vector<std::string> result;
  result.reserve(op1.size() + op2.size());
  result.insert(result.end(), op1.begin(), op1.end());
  result.insert(result.end(), op2.begin(), op2.end());
  return result;
}

bool MinMaxByResultVerifier::containsNull(
    const ArrayVector* vector,
    vector_size_t index) {
  auto offset = vector->offsetAt(index);
  auto size = vector->sizeAt(index);
  for (auto i = 0; i < size; ++i) {
    if (vector->elements()->isNullAt(offset + i)) {
      return true;
    }
  }
  return false;
}

int32_t MinMaxByResultVerifier::getElementIndexInBucketWithMemo(
    const VectorPtr& actualElements,
    vector_size_t actualIndex,
    const VectorPtr& expectedBucketElements,
    vector_size_t currentBucketOffset,
    vector_size_t currentBucketSize,
    SelectivityVector& memo) {
  for (auto i = 0; i < currentBucketSize; ++i) {
    if (actualElements->equalValueAt(
            expectedBucketElements.get(),
            actualIndex,
            currentBucketOffset + i) &&
        memo.isValid(i)) {
      memo.setValid(i, false);
      return i;
    }
  }
  return -1;
}

std::string MinMaxByResultVerifier::extractYColumnName(
    const core::AggregationNode::Aggregate& aggregate) {
  const auto& args = aggregate.call->inputs();
  VELOX_CHECK_GE(args.size(), 2)

  auto inputField = core::TypedExprs::asFieldAccess(args[1]);
  VELOX_CHECK_NOT_NULL(inputField)

  return inputField->name();
}

std::string MinMaxByResultVerifier::makeArrayAggCall(
    const core::AggregationNode::Aggregate& aggregate) {
  const auto& args = aggregate.call->inputs();
  VELOX_CHECK_GE(args.size(), 1)

  auto distinct = aggregate.distinct ? "distinct" : "";
  auto inputField = core::TypedExprs::asFieldAccess(args[0]);
  VELOX_CHECK_NOT_NULL(inputField)

  // Use $internal$array_agg to ensure we don't ignore input nulls since they
  // may affect the result of min_by/max_by.
  std::string arrayAggCall = fmt::format(
      "\"$internal$array_agg\"({} {})", distinct, inputField->name());
  arrayAggCall += " as expected";

  return arrayAggCall;
}

} // namespace facebook::velox::exec::test
