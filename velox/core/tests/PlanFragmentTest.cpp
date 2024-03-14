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

#include "velox/core/PlanFragment.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace ::facebook::velox;
using namespace ::facebook::velox::core;
using facebook::velox::exec::test::PlanBuilder;

namespace {
class PlanFragmentTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() override {
    rowType_ = ROW({"c0", "c1", "c2"}, {BIGINT(), BIGINT(), BIGINT()});
    rowTypeWithProjection_ = ROW(
        {"c0", "c1", "c2", "c3"}, {BIGINT(), BIGINT(), BIGINT(), BOOLEAN()});
    emptyVectors_ = {makeEmptyRowVector(rowType_)};
    valueNode_ = std::make_shared<ValuesNode>("valueNode", emptyVectors_);
    probeType_ = ROW({"u0", "u1", "u2"}, {BIGINT(), BIGINT(), BIGINT()});
    probeTypeWithProjection_ = ROW(
        {"u0", "u1", "u2", "u3"}, {BIGINT(), BIGINT(), BIGINT(), BOOLEAN()});
    emptyProbeVectors_ = {makeEmptyRowVector(probeType_)};
    probeValueNode_ =
        std::make_shared<ValuesNode>("valueNode", emptyProbeVectors_);
  }

  RowVectorPtr makeEmptyRowVector(RowTypePtr rowType) {
    return std::make_shared<RowVector>(
        pool_.get(),
        rowType,
        nullptr, // nulls
        0,
        std::vector<VectorPtr>{});
  }

  std::shared_ptr<QueryCtx> getSpillQueryCtx(
      bool spillEnabled,
      bool aggrSpillEnabled,
      bool joinSpillEnabled,
      bool orderBySpillEnabled) {
    std::unordered_map<std::string, std::string> configData({
        {QueryConfig::kSpillEnabled, spillEnabled ? "true" : "false"},
        {QueryConfig::kAggregationSpillEnabled,
         aggrSpillEnabled ? "true" : "false"},
        {QueryConfig::kJoinSpillEnabled, joinSpillEnabled ? "true" : "false"},
        {QueryConfig::kOrderBySpillEnabled,
         orderBySpillEnabled ? "true" : "false"},
    });
    return std::make_shared<QueryCtx>(nullptr, std::move(configData));
  }

  RowTypePtr rowType_;
  RowTypePtr rowTypeWithProjection_;
  std::vector<RowVectorPtr> emptyVectors_;
  std::shared_ptr<PlanNode> valueNode_;
  RowTypePtr probeType_;
  RowTypePtr probeTypeWithProjection_;
  std::vector<RowVectorPtr> emptyProbeVectors_;
  std::shared_ptr<PlanNode> probeValueNode_;
  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};
} // namespace

TEST_F(PlanFragmentTest, orderByCanSpill) {
  struct {
    bool spillingEnabled;
    bool orderByEnabled;
    bool expectedCanSpill;

    std::string debugString() const {
      return fmt::format(
          "spillingEnabled:{} orderByEnabled:{} expectedCanSpill:{}",
          spillingEnabled,
          orderByEnabled,
          expectedCanSpill);
    }
  } testSettings[] = {
      {true, false, false},
      {false, true, false},
      {true, true, true},
      {false, false, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    const std::vector<FieldAccessTypedExprPtr> sortingKeys{nullptr};
    const std::vector<SortOrder> sortingOrders{{true, true}};
    auto orderBy = std::make_shared<OrderByNode>(
        "orderBy", sortingKeys, sortingOrders, false, valueNode_);

    auto queryCtx = getSpillQueryCtx(
        testData.spillingEnabled, true, true, testData.orderByEnabled);
    const PlanFragment planFragment{orderBy};
    ASSERT_EQ(
        planFragment.canSpill(queryCtx->queryConfig()),
        testData.expectedCanSpill);
  }
}

TEST_F(PlanFragmentTest, aggregationCanSpill) {
  const std::vector<FieldAccessTypedExprPtr> groupingKeys{
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0"),
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c1")};
  const std::vector<FieldAccessTypedExprPtr> preGroupingKeys{
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0")};
  const std::vector<FieldAccessTypedExprPtr> emptyPreGroupingKeys;
  std::vector<std::string> aggregateNames{"sum"};
  std::vector<std::string> emptyAggregateNames{};
  std::vector<TypedExprPtr> aggregateInputs{
      std::make_shared<InputTypedExpr>(BIGINT())};
  const std::vector<AggregationNode::Aggregate> aggregates{
      {std::make_shared<core::CallTypedExpr>(BIGINT(), aggregateInputs, "sum"),
       {},
       nullptr,
       {},
       {}}};
  const std::vector<AggregationNode::Aggregate> emptyAggregates{};

  struct {
    AggregationNode::Step aggregationStep;
    bool isSpillEnabled;
    bool isAggregationSpillEnabled;
    bool isDistinct;
    bool hasPreAggregation;
    bool expectedCanSpill;

    std::string debugString() const {
      return fmt::format(
          "aggregationStep:{} isSpillEnabled:{} isAggregationSpillEnabled:{} isDistinct:{} hasPreAggregation:{} expectedCanSpill:{}",
          AggregationNode::stepName(aggregationStep),
          isSpillEnabled,
          isAggregationSpillEnabled,
          isDistinct,
          hasPreAggregation,
          expectedCanSpill);
    }
  } testSettings[] = {
      {AggregationNode::Step::kSingle, false, true, false, false, false},
      {AggregationNode::Step::kSingle, true, false, false, false, false},
      {AggregationNode::Step::kSingle, true, true, true, false, true},
      {AggregationNode::Step::kSingle, true, true, false, true, false},
      {AggregationNode::Step::kSingle, true, true, false, false, true},
      {AggregationNode::Step::kIntermediate, false, true, false, false, false},
      {AggregationNode::Step::kIntermediate, true, false, false, false, false},
      {AggregationNode::Step::kIntermediate, true, true, true, false, false},
      {AggregationNode::Step::kIntermediate, true, true, false, true, false},
      {AggregationNode::Step::kIntermediate, true, true, false, false, false},
      {AggregationNode::Step::kPartial, false, true, false, false, false},
      {AggregationNode::Step::kPartial, true, false, false, false, false},
      {AggregationNode::Step::kPartial, true, true, true, false, false},
      {AggregationNode::Step::kPartial, true, true, false, true, false},
      {AggregationNode::Step::kPartial, true, true, false, false, false},
      {AggregationNode::Step::kFinal, false, true, false, false, false},
      {AggregationNode::Step::kFinal, true, false, false, false, false},
      {AggregationNode::Step::kFinal, true, true, true, false, true},
      {AggregationNode::Step::kFinal, true, true, false, true, false},
      {AggregationNode::Step::kFinal, true, true, false, false, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const auto aggregation = std::make_shared<AggregationNode>(
        "aggregation",
        testData.aggregationStep,
        groupingKeys,
        testData.hasPreAggregation ? preGroupingKeys : emptyPreGroupingKeys,
        testData.isDistinct ? emptyAggregateNames : aggregateNames,
        testData.isDistinct ? emptyAggregates : aggregates,
        false,
        valueNode_);
    auto queryCtx = getSpillQueryCtx(
        testData.isSpillEnabled,
        testData.isAggregationSpillEnabled,
        true,
        true);
    const PlanFragment planFragment{aggregation};
    ASSERT_EQ(
        planFragment.canSpill(queryCtx->queryConfig()),
        testData.expectedCanSpill);
  }
}

TEST_F(PlanFragmentTest, hashJoin) {
  const std::vector<FieldAccessTypedExprPtr> buildKeys{
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "c0")};
  const std::vector<FieldAccessTypedExprPtr> probeKeys{
      std::make_shared<core::FieldAccessTypedExpr>(BIGINT(), "u0")};
  TypedExprPtr filter{
      std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), true)};

  struct {
    JoinType joinType;
    bool isNullAware;
    bool hasFilter;
    RowTypePtr outputType;
    bool isSpillEnabled;
    bool isJoinSpillEnabled;
    bool expectedCanSpill;

    std::string debugString() const {
      return fmt::format(
          "joinType:{} isNullAware:{} hasFilter:{} isSpillEnabled:{} isJoinSpillEnabled:{} expectedCanSpill:{}",
          joinType,
          isNullAware,
          hasFilter,
          isSpillEnabled,
          isJoinSpillEnabled,
          expectedCanSpill);
    }
  } testSettings[] = {
      {JoinType::kInner, false, true, rowType_, false, true, false},
      {JoinType::kInner, false, true, rowType_, true, false, false},
      {JoinType::kInner, false, true, rowType_, true, true, true},
      {JoinType::kRight, false, true, rowType_, false, true, false},
      {JoinType::kRight, false, true, rowType_, true, false, false},
      {JoinType::kRight, false, true, rowType_, true, true, true},
      {JoinType::kLeft, false, true, rowType_, false, true, false},
      {JoinType::kLeft, false, true, rowType_, true, false, false},
      {JoinType::kLeft, false, true, rowType_, true, true, true},
      {JoinType::kFull, false, true, rowType_, false, true, false},
      {JoinType::kFull, false, true, rowType_, true, false, false},
      {JoinType::kFull, false, true, rowType_, true, true, true},
      {JoinType::kLeftSemiFilter, false, true, rowType_, false, true, false},
      {JoinType::kLeftSemiFilter, false, true, rowType_, true, false, false},
      {JoinType::kLeftSemiFilter, false, true, rowType_, true, true, true},
      {JoinType::kLeftSemiProject,
       false,
       true,
       rowTypeWithProjection_,
       false,
       true,
       false},
      {JoinType::kLeftSemiProject,
       false,
       true,
       rowTypeWithProjection_,
       true,
       false,
       false},
      {JoinType::kLeftSemiProject,
       false,
       true,
       rowTypeWithProjection_,
       true,
       true,
       true},
      {JoinType::kRightSemiFilter, false, true, probeType_, false, true, false},
      {JoinType::kRightSemiFilter, false, true, probeType_, true, false, false},
      {JoinType::kRightSemiFilter, false, true, probeType_, true, true, true},
      {JoinType::kRightSemiProject,
       false,
       true,
       probeTypeWithProjection_,
       false,
       true,
       false},
      {JoinType::kRightSemiProject,
       false,
       true,
       probeTypeWithProjection_,
       true,
       false,
       false},
      {JoinType::kRightSemiProject,
       false,
       true,
       probeTypeWithProjection_,
       true,
       true,
       true},
      {JoinType::kAnti, false, true, rowType_, false, true, false},
      {JoinType::kAnti, false, true, rowType_, true, false, false},
      {JoinType::kAnti, false, true, rowType_, true, true, true},
      {JoinType::kAnti, false, false, rowType_, false, true, false},
      {JoinType::kAnti, false, false, rowType_, true, false, false},
      {JoinType::kAnti, false, false, rowType_, true, true, true},
      {JoinType::kAnti, true, true, rowType_, false, true, false},
      {JoinType::kAnti, true, true, rowType_, true, false, false},
      {JoinType::kAnti, true, true, rowType_, true, true, false},
      {JoinType::kAnti, true, false, rowType_, false, true, false},
      {JoinType::kAnti, true, false, rowType_, true, false, false},
      {JoinType::kAnti, true, false, rowType_, true, true, false}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const auto join = std::make_shared<HashJoinNode>(
        "join",
        testData.joinType,
        testData.isNullAware,
        buildKeys,
        probeKeys,
        filter,
        valueNode_,
        probeValueNode_,
        testData.outputType);
    auto queryCtx = getSpillQueryCtx(
        testData.isSpillEnabled, true, testData.isJoinSpillEnabled, true);
    const PlanFragment planFragment{join};
    ASSERT_EQ(
        planFragment.canSpill(queryCtx->queryConfig()),
        testData.expectedCanSpill);
  }
}

TEST_F(PlanFragmentTest, executionStrategyToString) {
  ASSERT_EQ(
      executionStrategyToString(core::ExecutionStrategy::kUngrouped),
      "UNGROUPED");
  ASSERT_EQ(
      executionStrategyToString(core::ExecutionStrategy::kGrouped), "GROUPED");
  ASSERT_EQ(
      executionStrategyToString(static_cast<core::ExecutionStrategy>(999)),
      "UNKNOWN: 999");
}
