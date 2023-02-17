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

#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/expression/SignatureBinder.h"

using facebook::velox::exec::test::AssertQueryBuilder;
using facebook::velox::exec::test::CursorParameters;
using facebook::velox::exec::test::PlanBuilder;

namespace facebook::velox::aggregate::test {

std::vector<RowVectorPtr> AggregationTestBase::makeVectors(
    const RowTypePtr& rowType,
    vector_size_t size,
    int numVectors) {
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < numVectors; ++i) {
    auto vector = std::dynamic_pointer_cast<RowVector>(
        velox::test::BatchMaker::createBatch(rowType, size, *pool_));
    vectors.push_back(vector);
  }
  return vectors;
}

void AggregationTestBase::SetUp() {
  exec::test::OperatorTestBase::SetUp();
  filesystems::registerLocalFileSystem();
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::string& duckDbSql) {
  SCOPED_TRACE(duckDbSql);
  testAggregations(data, groupingKeys, aggregates, {}, duckDbSql);
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<RowVectorPtr>& expectedResult) {
  testAggregations(data, groupingKeys, aggregates, {}, expectedResult);
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    const std::string& duckDbSql) {
  SCOPED_TRACE(duckDbSql);
  testAggregations(
      [&](PlanBuilder& builder) { builder.values(data); },
      groupingKeys,
      aggregates,
      postAggregationProjections,
      [&](auto& builder) { return builder.assertResults(duckDbSql); });
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    const std::vector<RowVectorPtr>& expectedResult) {
  testAggregations(
      [&](PlanBuilder& builder) { builder.values(data); },
      groupingKeys,
      aggregates,
      postAggregationProjections,
      [&](auto& builder) { return builder.assertResults(expectedResult); });
}

void AggregationTestBase::testAggregations(
    std::function<void(PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::string& duckDbSql) {
  testAggregations(
      makeSource, groupingKeys, aggregates, {}, [&](auto& builder) {
        return builder.assertResults(duckDbSql);
      });
}

namespace {
// Returns total spilled bytes inside 'task'.
int64_t spilledBytes(const exec::Task& task) {
  auto stats = task.taskStats();
  int64_t spilledBytes = 0;
  for (auto& pipeline : stats.pipelineStats) {
    for (auto op : pipeline.operatorStats) {
      spilledBytes += op.spilledBytes;
    }
  }

  return spilledBytes;
}
} // namespace

void AggregationTestBase::validateStreamingInTestAggregations(
    const std::function<void(PlanBuilder&)>& makeSource,
    const std::vector<std::string>& aggregates) {
  PlanBuilder builder(pool());
  makeSource(builder);
  auto input = AssertQueryBuilder(builder.planNode()).copyResults(pool());
  if (input->size() < 2) {
    return;
  }
  auto size1 = input->size() / 2;
  auto size2 = input->size() - size1;
  builder.singleAggregation({}, aggregates);
  auto expected = AssertQueryBuilder(builder.planNode()).copyResults(pool());
  ASSERT_EQ(expected->size(), 1);
  auto& aggregationNode =
      static_cast<const core::AggregationNode&>(*builder.planNode());
  ASSERT_EQ(expected->childrenSize(), aggregationNode.aggregates().size());
  for (int i = 0; i < aggregationNode.aggregates().size(); ++i) {
    auto& aggregate = aggregationNode.aggregates()[i];
    SCOPED_TRACE(aggregate->name());
    std::vector<VectorPtr> rawInput1, rawInput2;
    for (auto& arg : aggregate->inputs()) {
      VectorPtr column;
      auto channel = exec::exprToChannel(arg.get(), input->type());
      if (channel == kConstantChannel) {
        auto constant = dynamic_cast<const core::ConstantTypedExpr*>(arg.get());
        column = constant->toConstantVector(pool());
      } else {
        column = input->childAt(channel);
      }
      rawInput1.push_back(column->slice(0, size1));
      rawInput2.push_back(column->slice(size1, size2));
    }
    velox::test::assertEqualVectors(
        expected->childAt(i),
        testStreaming(
            aggregate->name(), true, rawInput1, size1, rawInput2, size2));
    velox::test::assertEqualVectors(
        expected->childAt(i),
        testStreaming(
            aggregate->name(), false, rawInput1, size1, rawInput2, size2));
  }
}

void AggregationTestBase::testAggregations(
    std::function<void(PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    std::function<std::shared_ptr<exec::Task>(AssertQueryBuilder&)>
        assertResults) {
  {
    SCOPED_TRACE("Run partial + final");
    PlanBuilder builder(pool());
    makeSource(builder);
    builder.partialAggregation(groupingKeys, aggregates).finalAggregation();
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    assertResults(queryBuilder);
  }

  if (!groupingKeys.empty() && allowInputShuffle_) {
    SCOPED_TRACE("Run partial + final with spilling");
    PlanBuilder builder(pool());
    makeSource(builder);

    // Spilling needs at least 2 batches of input. Use round-robin
    // repartitioning to split input into multiple batches.
    core::PlanNodeId partialNodeId;
    builder.localPartitionRoundRobin()
        .partialAggregation(groupingKeys, aggregates)
        .capturePlanNodeId(partialNodeId)
        .localPartition(groupingKeys)
        .finalAggregation();

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    auto spillDirectory = exec::test::TempDirectoryPath::create();

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.config(core::QueryConfig::kTestingSpillPct, "100")
        .config(core::QueryConfig::kSpillEnabled, "true")
        .config(core::QueryConfig::kAggregationSpillEnabled, "true")
        .spillDirectory(spillDirectory->path)
        .maxDrivers(4);

    auto task = assertResults(queryBuilder);

    // Expect > 0 spilled bytes unless there was no input.
    auto inputRows = toPlanStats(task->taskStats()).at(partialNodeId).inputRows;
    if (inputRows > 1) {
      EXPECT_LT(0, spilledBytes(*task));
    } else {
      EXPECT_EQ(0, spilledBytes(*task));
    }
  }

  {
    SCOPED_TRACE("Run single");
    PlanBuilder builder(pool());
    makeSource(builder);
    builder.singleAggregation(groupingKeys, aggregates);
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    assertResults(queryBuilder);
  }

  // TODO: turn on this after spilling for VarianceAggregationTest pass.
#if 0
  if (!groupingKeys.empty() && allowInputShuffle_) {
    SCOPED_TRACE("Run single with spilling");
    PlanBuilder builder(pool());
    makeSource(builder);
    core::PlanNodeId aggregationNodeId;
    builder.singleAggregation(groupingKeys, aggregates)
        .capturePlanNodeId(aggregationNodeId);
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    auto spillDirectory = exec::test::TempDirectoryPath::create();

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    queryBuilder.config(core::QueryConfig::kTestingSpillPct, "100")
        .config(core::QueryConfig::kSpillEnabled, "true")
        .config(core::QueryConfig::kAggregationSpillEnabled, "true")
        .spillDirectory(spillDirectory->path);

    auto task = assertResults(queryBuilder);

    // Expect > 0 spilled bytes unless there was no input.
    auto inputRows =
        toPlanStats(task->taskStats()).at(aggregationNodeId).inputRows;
    if (inputRows > 1) {
      EXPECT_LT(0, spilledBytes(*task));
    } else {
      EXPECT_EQ(0, spilledBytes(*task));
    }
  }
#endif

  {
    SCOPED_TRACE("Run partial + intermediate + final");
    PlanBuilder builder(pool());
    makeSource(builder);

    builder.partialAggregation(groupingKeys, aggregates)
        .intermediateAggregation()
        .finalAggregation();

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    assertResults(queryBuilder);
  }

  if (!groupingKeys.empty()) {
    SCOPED_TRACE("Run partial + local exchange + final");
    PlanBuilder builder(pool());
    makeSource(builder);

    builder.partialAggregation(groupingKeys, aggregates)
        .localPartition(groupingKeys)
        .finalAggregation();

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    assertResults(queryBuilder.maxDrivers(4));
  }

  {
    SCOPED_TRACE(
        "Run partial + local exchange + intermediate + local exchange + final");
    PlanBuilder builder(pool());
    makeSource(builder);

    builder.partialAggregation(groupingKeys, aggregates);

    if (groupingKeys.empty()) {
      builder.localPartitionRoundRobin();
    } else {
      builder.localPartition(groupingKeys);
    }

    builder.intermediateAggregation()
        .localPartition(groupingKeys)
        .finalAggregation();

    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    AssertQueryBuilder queryBuilder(builder.planNode(), duckDbQueryRunner_);
    assertResults(queryBuilder.maxDrivers(2));
  }

  if (testStreaming_ && groupingKeys.empty() &&
      postAggregationProjections.empty()) {
    SCOPED_TRACE("Streaming");
    validateStreamingInTestAggregations(makeSource, aggregates);
  }
}

namespace {
std::pair<TypePtr, TypePtr> getResultTypes(
    const std::string& name,
    const std::vector<TypePtr>& rawInputTypes) {
  auto signatures = exec::getAggregateFunctionSignatures(name);
  if (!signatures.has_value()) {
    VELOX_FAIL("Aggregate {} not registered", name);
  }
  for (auto& signature : signatures.value()) {
    exec::SignatureBinder binder(*signature, rawInputTypes);
    if (binder.tryBind()) {
      auto intermediateType =
          binder.tryResolveType(signature->intermediateType());
      VELOX_CHECK(
          intermediateType, "failed to resolve intermediate type for {}", name);
      auto finalType = binder.tryResolveType(signature->returnType());
      VELOX_CHECK(finalType, "failed to resolve final type for {}", name);
      return {intermediateType, finalType};
    }
  }
  VELOX_FAIL("Could not infer intermediate type for aggregate {}", name);
}
} // namespace

VectorPtr AggregationTestBase::testStreaming(
    const std::string& functionName,
    bool testGlobal,
    const std::vector<VectorPtr>& rawInput1,
    const std::vector<VectorPtr>& rawInput2) {
  VELOX_CHECK(!rawInput1.empty());
  VELOX_CHECK(!rawInput2.empty());
  return testStreaming(
      functionName,
      testGlobal,
      rawInput1,
      rawInput1[0]->size(),
      rawInput2,
      rawInput2[0]->size());
}

VectorPtr AggregationTestBase::testStreaming(
    const std::string& functionName,
    bool testGlobal,
    const std::vector<VectorPtr>& rawInput1,
    vector_size_t rawInput1Size,
    const std::vector<VectorPtr>& rawInput2,
    vector_size_t rawInput2Size) {
  constexpr int kRowSizeOffset = 8;
  constexpr int kOffset = kRowSizeOffset + 8;
  HashStringAllocator allocator(pool());
  std::vector<TypePtr> rawInputTypes;
  for (auto& vec : rawInput1) {
    rawInputTypes.push_back(vec->type());
  }
  auto [intermediateType, finalType] =
      getResultTypes(functionName, rawInputTypes);
  auto createFunction = [&, &finalType = finalType] {
    auto func = exec::Aggregate::create(
        functionName,
        core::AggregationNode::Step::kSingle,
        rawInputTypes,
        finalType);
    func->setAllocator(&allocator);
    func->setOffsets(kOffset, 0, 1, kRowSizeOffset);
    return func;
  };
  auto func = createFunction();
  int maxRowCount = std::max(rawInput1Size, rawInput2Size);
  std::vector<char> group(kOffset + func->accumulatorFixedWidthSize());
  std::vector<char*> groups(maxRowCount, group.data());
  std::vector<vector_size_t> indices(maxRowCount, 0);
  func->initializeNewGroups(groups.data(), indices);
  if (testGlobal) {
    func->addSingleGroupRawInput(
        group.data(), SelectivityVector(rawInput1Size), rawInput1, false);
  } else {
    func->addRawInput(
        groups.data(), SelectivityVector(rawInput1Size), rawInput1, false);
  }
  auto intermediate = BaseVector::create(intermediateType, 1, pool());
  func->extractAccumulators(groups.data(), 1, &intermediate);
  // Create a new function picking up the intermediate result.
  auto func2 = createFunction();
  func2->initializeNewGroups(groups.data(), indices);
  if (testGlobal) {
    func2->addSingleGroupIntermediateResults(
        group.data(), SelectivityVector(1), {intermediate}, false);
  } else {
    func2->addIntermediateResults(
        groups.data(), SelectivityVector(1), {intermediate}, false);
  }
  // Add more raw input.
  if (testGlobal) {
    func2->addSingleGroupRawInput(
        group.data(), SelectivityVector(rawInput2Size), rawInput2, false);
  } else {
    func2->addRawInput(
        groups.data(), SelectivityVector(rawInput2Size), rawInput2, false);
  }
  auto result = BaseVector::create(finalType, 1, pool());
  func2->extractValues(groups.data(), 1, &result);
  return result;
}

} // namespace facebook::velox::aggregate::test
