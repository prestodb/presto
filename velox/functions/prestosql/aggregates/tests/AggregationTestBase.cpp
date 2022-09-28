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

void AggregationTestBase::testAggregations(
    std::function<void(PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    std::function<std::shared_ptr<exec::Task>(AssertQueryBuilder& builder)>
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
        .config(core::QueryConfig::kSpillPath, spillDirectory->path)
        .maxDrivers(4);

    auto task = assertResults(queryBuilder);

    // Expect > 0 spilled bytes unless there was no input.
    auto inputRows = toPlanStats(task->taskStats()).at(partialNodeId).inputRows;
    if (inputRows > 0) {
      EXPECT_LT(0, spilledBytes(*task));
    } else {
      EXPECT_EQ(0, spilledBytes(*task));
    }
  }

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
}

} // namespace facebook::velox::aggregate::test
