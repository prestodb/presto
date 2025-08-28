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

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/PlanNode.h"
#include "velox/duckdb/conversion/DuckParser.h"
#include "velox/exec/ColumnStatsCollector.h"
#include "velox/exec/tests/utils/AggregationResolver.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/parse/TypeResolver.h"

namespace facebook::velox::exec::test {

class ColumnStatsCollectorTest : public OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  void SetUp() override {
    OperatorTestBase::SetUp();
  }

  core::ColumnStatsSpec createStatsSpec(
      const RowTypePtr& type,
      const std::vector<std::string>& groupingKeys,
      core::AggregationNode::Step step,
      const std::vector<std::string>& aggregates) {
    VELOX_CHECK(
        step == core::AggregationNode::Step::kPartial ||
            step == core::AggregationNode::Step::kSingle,
        "Unsupported aggregation step: {}",
        core::AggregationNode::toName(step));

    std::vector<core::AggregationNode::Aggregate> aggs;
    aggs.reserve(aggregates.size());
    std::vector<std::string> names;
    names.reserve(aggregates.size());

    duckdb::ParseOptions options;
    options.parseIntegerAsBigint = true;
    AggregateTypeResolver resolver(step);

    for (auto i = 0; i < aggregates.size(); ++i) {
      const auto& aggregate = aggregates[i];
      const auto untypedExpr = duckdb::parseAggregateExpr(aggregate, options);

      core::AggregationNode::Aggregate agg;
      agg.call = std::dynamic_pointer_cast<const core::CallTypedExpr>(
          core::Expressions::inferTypes(untypedExpr.expr, type, pool()));

      for (const auto& input : agg.call->inputs()) {
        agg.rawInputTypes.push_back(input->type());
      }

      VELOX_CHECK_NULL(untypedExpr.maskExpr);
      VELOX_CHECK(!untypedExpr.distinct);
      VELOX_CHECK(untypedExpr.orderBy.empty());

      aggs.emplace_back(agg);

      if (untypedExpr.expr->alias().has_value()) {
        names.push_back(untypedExpr.expr->alias().value());
      } else {
        names.push_back(fmt::format("a{}", i));
      }
    }
    VELOX_CHECK_EQ(aggs.size(), names.size());

    std::vector<core::FieldAccessTypedExprPtr> groupingKeyExprs;
    groupingKeyExprs.reserve(groupingKeys.size());
    for (const auto& groupingKey : groupingKeys) {
      auto untypedGroupingKeyExpr = duckdb::parseExpr(groupingKey, options);
      auto groupingKeyExpr =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
              core::Expressions::inferTypes(
                  untypedGroupingKeyExpr, type, pool()));
      VELOX_CHECK_NOT_NULL(
          groupingKeyExpr,
          "Grouping key must use a column name, not an expression: {}",
          groupingKey);
      groupingKeyExprs.emplace_back(std::move(groupingKeyExpr));
    }

    return core::ColumnStatsSpec(
        std::move(groupingKeyExprs), step, std::move(names), std::move(aggs));
  }

  // Helper to create test data
  std::vector<RowVectorPtr> createTestData(
      const RowTypePtr& rowType,
      size_t numBatches,
      size_t batchSize,
      bool withNulls = false) {
    VectorFuzzer fuzzer(
        {.vectorSize = batchSize, .nullRatio = withNulls ? 0.2 : 0.0}, pool());
    std::vector<RowVectorPtr> batches;
    batches.reserve(numBatches);
    for (int i = 0; i < numBatches; ++i) {
      batches.push_back(fuzzer.fuzzInputRow(rowType));
    }
    return batches;
  }

  std::unique_ptr<ColumnStatsCollector> createStatsCollector(
      const core::ColumnStatsSpec& statsSpec,
      const RowTypePtr& inputType) {
    return std::make_unique<ColumnStatsCollector>(
        statsSpec, inputType, &queryConfig_, pool(), &nonReclaimableSection_);
  }

  void verifyResult(
      const std::vector<RowVectorPtr>& inputBatches,
      const std::vector<std::string>& groupingKeys,
      core::AggregationNode::Step step,
      const std::vector<std::string>& aggregates,
      const std::vector<RowVectorPtr>& resultBatches) {
    core::PlanNodePtr plan;
    if (step == core::AggregationNode::Step::kSingle) {
      plan = PlanBuilder()
                 .values(inputBatches)
                 .singleAggregation(groupingKeys, aggregates)
                 .planNode();
    } else {
      plan = PlanBuilder()
                 .values(inputBatches)
                 .partialAggregation(groupingKeys, aggregates)
                 .planNode();
    }

    const auto expectedResult = AssertQueryBuilder(plan).copyResults(pool());
    assertEqualResults(resultBatches, {expectedResult});
  }

  core::QueryConfig queryConfig_{{}};
  tsan_atomic<bool> nonReclaimableSection_{false};
};

TEST_F(ColumnStatsCollectorTest, basic) {
  const auto inputType =
      ROW({"c0", "c1", "c2"}, {INTEGER(), DOUBLE(), INTEGER()});

  struct {
    std::vector<std::string> groupingKeys;
    std::vector<std::string> aggregates;
    core::AggregationNode::Step step;
    bool hasNulls;

    std::string debugString() const {
      return fmt::format(
          "groupingKeys: {}, aggregates: {}, {}, hasNulls: {}",
          folly::join(",", groupingKeys),
          folly::join(",", aggregates),
          core::AggregationNode::toName(step),
          hasNulls);
    }
  } testCases[] = {
      {std::vector<std::string>{},
       std::vector<std::string>{"sum(c0)", "count(c1)", "sum(c2)"},
       core::AggregationNode::Step::kSingle,
       false},
      {std::vector<std::string>{"c0"},
       std::vector<std::string>{"count(c1)", "sum(c2)"},
       core::AggregationNode::Step::kSingle,
       false},
      {std::vector<std::string>{},
       std::vector<std::string>{"sum(c0)", "count(c1)", "sum(c2)"},
       core::AggregationNode::Step::kPartial,
       false},
      {std::vector<std::string>{"c0"},
       std::vector<std::string>{"count(c1)", "sum(c2)"},
       core::AggregationNode::Step::kPartial,
       false},
      {std::vector<std::string>{},
       std::vector<std::string>{"sum(c0)", "count(c1)", "sum(c2)"},
       core::AggregationNode::Step::kSingle,
       true},
      {std::vector<std::string>{"c0"},
       std::vector<std::string>{"count(c1)", "sum(c2)"},
       core::AggregationNode::Step::kSingle,
       true},
      {std::vector<std::string>{},
       std::vector<std::string>{"sum(c0)", "count(c1)", "sum(c2)"},
       core::AggregationNode::Step::kPartial,
       true},
      {std::vector<std::string>{"c0"},
       std::vector<std::string>{"count(c1)", "sum(c2)"},
       core::AggregationNode::Step::kPartial,
       true},
      // Multiple grouping keys.
      {std::vector<std::string>{"c0", "c1"},
       std::vector<std::string>{"sum(c2)"},
       core::AggregationNode::Step::kSingle,
       false},
      {std::vector<std::string>{"c0", "c1"},
       std::vector<std::string>{"sum(c2)"},
       core::AggregationNode::Step::kPartial,
       false},
      {std::vector<std::string>{"c0", "c1"},
       std::vector<std::string>{"sum(c2)"},
       core::AggregationNode::Step::kSingle,
       true},
      {std::vector<std::string>{"c0", "c1"},
       std::vector<std::string>{"sum(c2)"},
       core::AggregationNode::Step::kPartial,
       true}};

  for (const auto& testCase : testCases) {
    SCOPED_TRACE(testCase.debugString());

    // Create stats spec for count and sum
    auto statsSpec = createStatsSpec(
        inputType, testCase.groupingKeys, testCase.step, testCase.aggregates);

    // Create collector.
    auto collector = createStatsCollector(statsSpec, inputType);

    // Create test data.
    auto inputBatches = createTestData(inputType, 10, 100, testCase.hasNulls);
    VELOX_ASSERT_THROW(collector->addInput(inputBatches[0]), "");

    // Initialize.
    collector->initialize();

    // Add input.
    for (const auto& input : inputBatches) {
      collector->addInput(input);
      ASSERT_EQ(collector->getOutput(), nullptr);
    }
    ASSERT_FALSE(collector->finished());
    collector->noMoreInput();
    ASSERT_FALSE(collector->finished());

    // Get output
    std::vector<RowVectorPtr> outputBatches;
    for (;;) {
      auto output = collector->getOutput();
      if (output != nullptr) {
        // Single output row without grouping keys.
        if (testCase.groupingKeys.empty()) {
          ASSERT_EQ(output->size(), 1);
        }
        ASSERT_EQ(
            output->childrenSize(),
            testCase.aggregates.size() + testCase.groupingKeys.size());
        // Verify column names (using asRowType to access field names)
        auto rowType = std::dynamic_pointer_cast<const RowType>(output->type());
        ASSERT_NE(rowType, nullptr);
        auto outputChannel = testCase.groupingKeys.size();
        const int numAggregates = testCase.aggregates.size();
        for (int i = 0; i < numAggregates; ++i) {
          EXPECT_EQ(
              rowType->nameOf(outputChannel++), statsSpec.aggregateNames[i]);
        }
        outputBatches.push_back(output);
        EXPECT_FALSE(collector->finished());
      } else {
        break;
      }
    }
    // Verify finished.
    EXPECT_TRUE(collector->finished());

    verifyResult(
        inputBatches,
        testCase.groupingKeys,
        testCase.step,
        testCase.aggregates,
        outputBatches);
  }
}
} // namespace facebook::velox::exec::test
