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

#include "AggregationTestBase.h"
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"

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

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::string& duckDbSql) {
  testAggregations(data, groupingKeys, aggregates, {}, duckDbSql);
}

void AggregationTestBase::testAggregations(
    const std::vector<RowVectorPtr>& data,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    const std::string& duckDbSql) {
  testAggregations(
      [&](PlanBuilder& builder) { builder.values(data); },
      groupingKeys,
      aggregates,
      postAggregationProjections,
      duckDbSql);
}

void AggregationTestBase::testAggregations(
    std::function<void(PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::string& duckDbSql) {
  testAggregations(makeSource, groupingKeys, aggregates, {}, duckDbSql);
}

void AggregationTestBase::testAggregations(
    std::function<void(PlanBuilder&)> makeSource,
    const std::vector<std::string>& groupingKeys,
    const std::vector<std::string>& aggregates,
    const std::vector<std::string>& postAggregationProjections,
    const std::string& duckDbSql) {
  // Run partial + final.
  {
    PlanBuilder builder;
    makeSource(builder);
    builder.partialAggregation(groupingKeys, aggregates).finalAggregation();
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }
    assertQuery(builder.planNode(), duckDbSql);
  }

  // Run single.
  {
    PlanBuilder builder;
    makeSource(builder);
    builder.singleAggregation(groupingKeys, aggregates);
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }
    assertQuery(builder.planNode(), duckDbSql);
  }

  // Run partial + intermediate + final.
  {
    PlanBuilder builder;
    makeSource(builder);
    builder.partialAggregation(groupingKeys, aggregates)
        .intermediateAggregation()
        .finalAggregation();
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }
    assertQuery(builder.planNode(), duckDbSql);
  }

  // Run partial + local exchange + final.
  if (!groupingKeys.empty()) {
    auto planNodeIdGenerator =
        std::make_shared<exec::test::PlanNodeIdGenerator>();
    PlanBuilder sourceBuilder{planNodeIdGenerator};
    makeSource(sourceBuilder);

    auto builder =
        PlanBuilder(planNodeIdGenerator)
            .localPartition(
                groupingKeys,
                {sourceBuilder.partialAggregation(groupingKeys, aggregates)
                     .planNode()})
            .finalAggregation();
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    CursorParameters params;
    params.planNode = builder.planNode();
    params.maxDrivers = 2;
    assertQuery(params, duckDbSql);
  }

  // Run partial + local exchange + intermediate + local exchange + final.
  {
    auto planNodeIdGenerator =
        std::make_shared<exec::test::PlanNodeIdGenerator>();
    PlanBuilder sourceBuilder{planNodeIdGenerator};
    makeSource(sourceBuilder);

    PlanBuilder partialBuilder{planNodeIdGenerator};
    if (groupingKeys.empty()) {
      partialBuilder.localPartitionRoundRobin(
          {sourceBuilder.partialAggregation(groupingKeys, aggregates)
               .planNode()});
    } else {
      partialBuilder.localPartition(
          groupingKeys,
          {sourceBuilder.partialAggregation(groupingKeys, aggregates)
               .planNode()});
    }

    auto builder =
        PlanBuilder(planNodeIdGenerator)
            .localPartition(
                groupingKeys,
                {partialBuilder.intermediateAggregation().planNode()})
            .finalAggregation();
    if (!postAggregationProjections.empty()) {
      builder.project(postAggregationProjections);
    }

    CursorParameters params;
    params.planNode = builder.planNode();
    params.maxDrivers = 2;
    assertQuery(params, duckDbSql);
  }
}

} // namespace facebook::velox::aggregate::test
