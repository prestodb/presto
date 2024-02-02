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

#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/common/memory/Memory.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/TableWriter.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::memory;

namespace facebook::velox::exec::test {

std::shared_ptr<core::QueryCtx> newQueryCtx(
    const std::unique_ptr<MemoryManager>& memoryManager,
    const std::shared_ptr<folly::Executor>& executor,
    int64_t memoryCapacity,
    std::unique_ptr<MemoryReclaimer>&& reclaimer) {
  std::unordered_map<std::string, std::shared_ptr<Config>> configs;
  std::shared_ptr<MemoryPool> pool = memoryManager->addRootPool(
      "",
      memoryCapacity,
      reclaimer != nullptr ? std::move(reclaimer) : MemoryReclaimer::create());
  auto queryCtx = std::make_shared<core::QueryCtx>(
      executor.get(),
      core::QueryConfig({}),
      configs,
      cache::AsyncDataCache::getInstance(),
      std::move(pool));
  return queryCtx;
}

std::unique_ptr<memory::MemoryManager> createMemoryManager(
    int64_t arbitratorCapacity,
    uint64_t memoryPoolInitCapacity,
    uint64_t memoryPoolTransferCapacity,
    uint64_t maxReclaimWaitMs) {
  memory::MemoryManagerOptions options;
  options.arbitratorCapacity = arbitratorCapacity;
  // Avoid allocation failure in unit tests.
  options.allocatorCapacity = arbitratorCapacity * 2;
  options.arbitratorKind = "SHARED";
  options.memoryPoolInitCapacity = memoryPoolInitCapacity;
  options.memoryPoolTransferCapacity = memoryPoolTransferCapacity;
  options.memoryReclaimWaitMs = maxReclaimWaitMs;
  options.checkUsageLeak = true;
  options.arbitrationStateCheckCb = memoryArbitrationStateCheck;
  return std::make_unique<memory::MemoryManager>(options);
}

core::PlanNodePtr hashJoinPlan(
    const std::vector<RowVectorPtr>& vectors,
    core::PlanNodeId& joinNodeId) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  return PlanBuilder(planNodeIdGenerator)
      .values(vectors, true)
      .project({"c0", "c1", "c2"})
      .hashJoin(
          {"c0"},
          {"u1"},
          PlanBuilder(planNodeIdGenerator)
              .values(vectors, true)
              .project({"c0 AS u0", "c1 AS u1", "c2 AS u2"})
              .planNode(),
          "",
          {"c0", "c1", "c2"},
          core::JoinType::kInner)
      .capturePlanNodeId(joinNodeId)
      .planNode();
}

QueryTestResult runHashJoinTask(
    const std::vector<RowVectorPtr>& vectors,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    uint32_t numDrivers,
    memory::MemoryPool* pool,
    bool enableSpilling,
    const RowVectorPtr& expectedResult) {
  QueryTestResult result;
  const auto plan = hashJoinPlan(vectors, result.planNodeId);
  if (enableSpilling) {
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    result.data = AssertQueryBuilder(plan)
                      .spillDirectory(spillDirectory->path)
                      .config(core::QueryConfig::kSpillEnabled, true)
                      .config(core::QueryConfig::kJoinSpillEnabled, true)
                      .queryCtx(queryCtx)
                      .maxDrivers(numDrivers)
                      .copyResults(pool, result.task);
  } else {
    result.data = AssertQueryBuilder(plan)
                      .queryCtx(queryCtx)
                      .maxDrivers(numDrivers)
                      .copyResults(pool, result.task);
  }
  if (expectedResult != nullptr) {
    assertEqualResults({result.data}, {expectedResult});
  }
  return result;
}

core::PlanNodePtr aggregationPlan(
    const std::vector<RowVectorPtr>& vectors,
    core::PlanNodeId& aggregateNodeId) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  return PlanBuilder(planNodeIdGenerator)
      .values(vectors)
      .singleAggregation({"c0", "c1"}, {"array_agg(c2)"})
      .capturePlanNodeId(aggregateNodeId)
      .planNode();
}

QueryTestResult runAggregateTask(
    const std::vector<RowVectorPtr>& vectors,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    bool enableSpilling,
    uint32_t numDrivers,
    memory::MemoryPool* pool,
    const RowVectorPtr& expectedResult) {
  QueryTestResult result;
  const auto plan = aggregationPlan(vectors, result.planNodeId);
  if (enableSpilling) {
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    result.data =
        AssertQueryBuilder(plan)
            .spillDirectory(spillDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kAggregationSpillEnabled, "true")
            .queryCtx(queryCtx)
            .maxDrivers(numDrivers)
            .copyResults(pool, result.task);
  } else {
    result.data = AssertQueryBuilder(plan)
                      .queryCtx(queryCtx)
                      .maxDrivers(numDrivers)
                      .copyResults(pool, result.task);
  }
  if (expectedResult != nullptr) {
    assertEqualResults({result.data}, {expectedResult});
  }
  return result;
}

core::PlanNodePtr orderByPlan(
    const std::vector<RowVectorPtr>& vectors,
    core::PlanNodeId& orderNodeId) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  return PlanBuilder(planNodeIdGenerator)
      .values(vectors)
      .project({"c0", "c1", "c2"})
      .orderBy({"c2 ASC NULLS LAST"}, false)
      .capturePlanNodeId(orderNodeId)
      .planNode();
}

QueryTestResult runOrderByTask(
    const std::vector<RowVectorPtr>& vectors,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    uint32_t numDrivers,
    memory::MemoryPool* pool,
    bool enableSpilling,
    const RowVectorPtr& expectedResult) {
  QueryTestResult result;
  const auto plan = orderByPlan(vectors, result.planNodeId);
  if (enableSpilling) {
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    result.data = AssertQueryBuilder(plan)
                      .spillDirectory(spillDirectory->path)
                      .config(core::QueryConfig::kSpillEnabled, "true")
                      .config(core::QueryConfig::kOrderBySpillEnabled, "true")
                      .queryCtx(queryCtx)
                      .maxDrivers(numDrivers)
                      .copyResults(pool, result.task);
  } else {
    result.data = AssertQueryBuilder(plan)
                      .queryCtx(queryCtx)
                      .maxDrivers(numDrivers)
                      .copyResults(pool, result.task);
  }
  if (expectedResult != nullptr) {
    assertEqualResults({result.data}, {expectedResult});
  }
  return result;
}

core::PlanNodePtr rowNumberPlan(
    const std::vector<RowVectorPtr>& vectors,
    core::PlanNodeId& rowNumberNodeId) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  return PlanBuilder(planNodeIdGenerator)
      .values(vectors)
      .rowNumber({"c0"}, 2, false)
      .project({"c0", "c1"})
      .capturePlanNodeId(rowNumberNodeId)
      .planNode();
}

QueryTestResult runRowNumberTask(
    const std::vector<RowVectorPtr>& vectors,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    uint32_t numDrivers,
    memory::MemoryPool* pool,
    bool enableSpilling,
    const RowVectorPtr& expectedResult) {
  QueryTestResult result;
  const auto plan = rowNumberPlan(vectors, result.planNodeId);
  if (enableSpilling) {
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    result.data = AssertQueryBuilder(plan)
                      .spillDirectory(spillDirectory->path)
                      .config(core::QueryConfig::kSpillEnabled, "true")
                      .config(core::QueryConfig::kRowNumberSpillEnabled, "true")
                      .queryCtx(queryCtx)
                      .maxDrivers(numDrivers)
                      .copyResults(pool, result.task);
  } else {
    result.data = AssertQueryBuilder(plan)
                      .queryCtx(queryCtx)
                      .maxDrivers(numDrivers)
                      .copyResults(pool, result.task);
  }
  if (expectedResult != nullptr) {
    assertEqualResults({result.data}, {expectedResult});
  }
  return result;
}

core::PlanNodePtr topNPlan(
    const std::vector<RowVectorPtr>& vectors,
    core::PlanNodeId& topNodeId) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  return PlanBuilder(planNodeIdGenerator)
      .values(vectors)
      .project({"c1"})
      .topN({"c1 NULLS FIRST"}, 10, false)
      .capturePlanNodeId(topNodeId)
      .planNode();
}

QueryTestResult runTopNTask(
    const std::vector<RowVectorPtr>& vectors,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    uint32_t numDrivers,
    memory::MemoryPool* pool,
    bool enableSpilling,
    const RowVectorPtr& expectedResult) {
  QueryTestResult result;
  const auto plan = topNPlan(vectors, result.planNodeId);
  if (enableSpilling) {
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    result.data =
        AssertQueryBuilder(plan)
            .spillDirectory(spillDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kTopNRowNumberSpillEnabled, "true")
            .queryCtx(queryCtx)
            .maxDrivers(numDrivers)
            .copyResults(pool, result.task);
  } else {
    result.data = AssertQueryBuilder(plan)
                      .queryCtx(queryCtx)
                      .maxDrivers(numDrivers)
                      .copyResults(pool, result.task);
  }
  if (expectedResult != nullptr) {
    assertEqualResults({result.data}, {expectedResult});
  }
  return result;
}

core::PlanNodePtr writePlan(
    const std::vector<RowVectorPtr>& vectors,
    const std::string& outputDirPath,
    core::PlanNodeId& writeNodeId) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  return PlanBuilder(planNodeIdGenerator)
      .values(vectors)
      .tableWrite(outputDirPath)
      .singleAggregation(
          {}, {fmt::format("sum({})", TableWriteTraits::rowCountColumnName())})
      .capturePlanNodeId(writeNodeId)
      .planNode();
}

QueryTestResult runWriteTask(
    const std::vector<RowVectorPtr>& vectors,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    uint32_t numDrivers,
    memory::MemoryPool* pool,
    const std::string& kHiveConnectorId,
    bool enableSpilling,
    const RowVectorPtr& expectedResult) {
  QueryTestResult result;
  const auto outputDirectory = TempDirectoryPath::create();
  auto plan = writePlan(vectors, outputDirectory->path, result.planNodeId);
  if (enableSpilling) {
    const auto spillDirectory = exec::test::TempDirectoryPath::create();
    result.data =
        AssertQueryBuilder(plan)
            .spillDirectory(spillDirectory->path)
            .config(core::QueryConfig::kSpillEnabled, "true")
            .config(core::QueryConfig::kAggregationSpillEnabled, "false")
            .config(core::QueryConfig::kWriterSpillEnabled, "true")
            // Set 0 file writer flush threshold to always trigger flush in
            // test.
            .config(core::QueryConfig::kWriterFlushThresholdBytes, "0")
            // Set stripe size to extreme large to avoid writer internal
            // triggered flush.
            .connectorSessionProperty(
                kHiveConnectorId,
                connector::hive::HiveConfig::kOrcWriterMaxStripeSizeSession,
                "1GB")
            .connectorSessionProperty(
                kHiveConnectorId,
                connector::hive::HiveConfig::
                    kOrcWriterMaxDictionaryMemorySession,
                "1GB")
            .connectorSessionProperty(
                kHiveConnectorId,
                connector::hive::HiveConfig::
                    kOrcWriterMaxDictionaryMemorySession,
                "1GB")
            .queryCtx(queryCtx)
            .maxDrivers(numDrivers)
            .copyResults(pool, result.task);
  } else {
    result.data = AssertQueryBuilder(plan)
                      .queryCtx(queryCtx)
                      .maxDrivers(numDrivers)
                      .copyResults(pool, result.task);
  }
  if (expectedResult != nullptr) {
    assertEqualResults({result.data}, {expectedResult});
  }
  return result;
}

} // namespace facebook::velox::exec::test
