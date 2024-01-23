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
#include "velox/exec/OutputBufferManager.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class LimitTest : public HiveConnectorTestBase {};

TEST_F(LimitTest, basic) {
  vector_size_t batchSize = 1'000;
  std::vector<RowVectorPtr> vectors;
  for (int32_t i = 0; i < 3; ++i) {
    auto c0 = makeFlatVector<int64_t>(
        batchSize, [&](auto row) { return batchSize * i + row; }, nullEvery(5));
    auto c1 = makeFlatVector<int32_t>(
        batchSize, [&](auto row) { return row; }, nullEvery(7));
    auto c2 = makeFlatVector<double>(
        batchSize, [](auto row) { return row * 0.1; }, nullEvery(11));
    vectors.push_back(makeRowVector({c0, c1, c2}));
  }
  createDuckDbTable(vectors);

  auto makePlan = [&](int64_t offset, int64_t limit) {
    return PlanBuilder().values(vectors).limit(offset, limit, true).planNode();
  };

  assertQuery(makePlan(0, 10), "SELECT * FROM tmp LIMIT 10");
  int64_t limit = (int64_t)(std::numeric_limits<int32_t>::max()) + 1000000;
  int64_t offset = (int64_t)(std::numeric_limits<int32_t>::max()) + 1000;
  assertQuery(
      makePlan(0, limit), fmt::format("SELECT * FROM tmp LIMIT {}", limit));
  assertQuery(makePlan(0, 1'234), "SELECT * FROM tmp LIMIT 1234");

  assertQuery(makePlan(17, 10), "SELECT * FROM tmp OFFSET 17 LIMIT 10");
  assertQuery(makePlan(17, 983), "SELECT * FROM tmp OFFSET 17 LIMIT 983");
  assertQuery(makePlan(17, 2'000), "SELECT * FROM tmp OFFSET 17 LIMIT 2000");
  assertQuery(
      makePlan(offset, limit),
      fmt::format("SELECT * FROM tmp OFFSET {} LIMIT {}", offset, limit));

  assertQuery(
      makePlan(offset, 2000),
      fmt::format("SELECT * FROM tmp OFFSET {} LIMIT 2000", offset));

  assertQuery(makePlan(1'000, 145), "SELECT * FROM tmp OFFSET 1000 LIMIT 145");
  assertQuery(
      makePlan(1'000, 1'000), "SELECT * FROM tmp OFFSET 1000 LIMIT 1000");
  assertQuery(
      makePlan(1'000, 1'234), "SELECT * FROM tmp OFFSET 1000 LIMIT 1234");

  assertQuery(makePlan(1'234, 10), "SELECT * FROM tmp OFFSET 1234 LIMIT 10");
  assertQuery(makePlan(1'234, 983), "SELECT * FROM tmp OFFSET 1234 LIMIT 983");
  assertQuery(
      makePlan(1'234, 1'000), "SELECT * FROM tmp OFFSET 1234 LIMIT 1000");
  assertQuery(
      makePlan(1'234, 2'000), "SELECT * FROM tmp OFFSET 1234 LIMIT 2000");

  assertQueryReturnsEmptyResult(makePlan(12'345, 10));
}

TEST_F(LimitTest, limitOverLocalExchange) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>(1'000, [](auto row) { return row; })});

  auto file = TempFilePath::create();
  writeToFile(file->path, {data});

  core::PlanNodeId scanNodeId;

  CursorParameters params;
  params.planNode = PlanBuilder()
                        .tableScan(asRowType(data->type()))
                        .capturePlanNodeId(scanNodeId)
                        .localPartition(std::vector<std::string>{})
                        .limit(0, 20, true)
                        .planNode();

  auto cursor = TaskCursor::create(params);
  cursor->task()->addSplit(
      scanNodeId, exec::Split(makeHiveConnectorSplit(file->path)));

  int32_t numRead = 0;
  while (cursor->moveNext()) {
    auto vector = cursor->current();
    numRead += vector->size();
  }

  // Do not send no-more-splits message. Expect the task to finish without
  // receiving that message.

  ASSERT_EQ(20, numRead);
  ASSERT_TRUE(waitForTaskCompletion(cursor->task().get()));
}

TEST_F(LimitTest, partialLimitEagerFlush) {
  std::vector<RowVectorPtr> batches(
      10, makeRowVector({makeFlatVector(std::vector<int64_t>(1, 0))}));
  auto test = [&](bool projectInBetween) {
    SCOPED_TRACE(fmt::format("projectInBetween={}", projectInBetween));
    CursorParameters params;
    auto builder = PlanBuilder().values(batches).limit(0, 10, true);
    if (projectInBetween) {
      builder = builder.project({"c0 + 1 as c0"});
    }
    params.planNode = builder.partitionedOutput({}, 1).planNode();
    auto cursor = TaskCursor::create(params);
    ASSERT_FALSE(cursor->moveNext());
    auto bufferManager = exec::OutputBufferManager::getInstance().lock();
    auto [numPagesPromise, numPagesFuture] = folly::makePromiseContract<int>();
    ASSERT_TRUE(bufferManager->getData(
        cursor->task()->taskId(),
        0,
        INT32_MAX,
        0,
        [numPagesPromise =
             std::make_shared<folly::Promise<int>>(std::move(numPagesPromise))](
            std::vector<std::unique_ptr<folly::IOBuf>> pages,
            int64_t /*sequence*/) {
          numPagesPromise->setValue(pages.size());
        }));
    ASSERT_GE(std::move(numPagesFuture).get(std::chrono::seconds(1)), 10);
    cursor->task()->requestCancel();
  };
  test(true);
  test(false);
}
