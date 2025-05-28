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

#include "velox/runner/tests/PrestoQueryReplayRunner.h"

#include "velox/exec/PartitionFunction.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::runner {
namespace {
// Presto 'taskId' is in the format of
// queryId.stageId.stageExecutionId.taskId.attemptNumber. return stage id from
// the given taskId.
std::string getTaskPrefix(const std::string& taskId) {
  std::vector<std::string_view> parts;
  folly::split('.', taskId, parts);
  VELOX_CHECK_EQ(parts.size(), 5);
  return std::string(parts[1]);
}

class PrestoQueryReplayRunnerTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();

    Type::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    exec::registerPartitionFunctionSerDe();

    exec::ExchangeSource::registerFactory(
        exec::test::createLocalExchangeSource);
  }
};

TEST_F(PrestoQueryReplayRunnerTest, basicWithoutTableScan) {
  // The serialized plan is equivalent to
  // PlanBuilder()
  //    .values({makeRowVector(
  //        {makeFlatVector<int64_t>(10, [](auto row) { return row + 1; }),
  //         makeFlatVector<int64_t>(10, [](auto row) { return row + 1; })})})
  //    .project({"c0 % 3 as c0", "c1 as c1"})
  //    .partialAggregation({"c0"}, {"sum(c1) as a0"})
  //    .finalAggregation();
  const auto queryId = "abc";
  const std::vector<std::string> serializedPlanFragments = {
      /*partial aggregation*/
      R"({"task_id":"abc.1.0.0.0","remote_task_ids":{},"plan_fragment":{"outputType":{"names":["c0","a0"],"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"BIGINT","name":"Type"}],"type":"ROW","name":"Type"},"serdeKind":"Presto","replicateNullsAndAny":false,"partitionFunctionSpec":{"keyChannels":[0],"constants":[],"inputType":{"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"BIGINT","name":"Type"}],"names":["c0","a0"],"type":"ROW","name":"Type"},"name":"HashPartitionFunctionSpec"},"numPartitions":3,"sources":[{"ignoreNullKeys":false,"globalGroupingSets":[],"aggregates":[{"distinct":false,"sortingOrders":[],"sortingKeys":[],"rawInputTypes":[{"type":"INTEGER","name":"Type"}],"call":{"inputs":[{"inputs":[{"type":{"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"INTEGER","name":"Type"}],"names":["c0","c1"],"type":"ROW","name":"Type"},"name":"InputTypedExpr"}],"fieldName":"c1","type":{"type":"INTEGER","name":"Type"},"name":"FieldAccessTypedExpr"}],"functionName":"sum","type":{"type":"BIGINT","name":"Type"},"name":"CallTypedExpr"}}],"aggregateNames":["a0"],"preGroupedKeys":[],"step":"PARTIAL","groupingKeys":[{"fieldName":"c0","type":{"type":"BIGINT","name":"Type"},"name":"FieldAccessTypedExpr"}],"sources":[{"projections":[{"functionName":"mod","inputs":[{"inputs":[{"type":{"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"INTEGER","name":"Type"}],"names":["c0","c1"],"type":"ROW","name":"Type"},"name":"InputTypedExpr"}],"fieldName":"c0","type":{"type":"BIGINT","name":"Type"},"name":"FieldAccessTypedExpr"},{"value":{"value":3,"type":"BIGINT"},"type":{"type":"BIGINT","name":"Type"},"name":"ConstantTypedExpr"}],"type":{"type":"BIGINT","name":"Type"},"name":"CallTypedExpr"},{"fieldName":"c1","inputs":[{"type":{"names":["c0","c1"],"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"INTEGER","name":"Type"}],"type":"ROW","name":"Type"},"name":"InputTypedExpr"}],"type":{"type":"INTEGER","name":"Type"},"name":"FieldAccessTypedExpr"}],"sources":[{"data":"AAAAAHwAAAB7ImNUeXBlcyI6W3sidHlwZSI6IkJJR0lOVCIsIm5hbWUiOiJUeXBlIn0seyJ0eXBlIjoiSU5URUdFUiIsIm5hbWUiOiJUeXBlIn1dLCJuYW1lcyI6WyJjMCIsImMxIl0sInR5cGUiOiJST1ciLCJuYW1lIjoiVHlwZSJ9CgAAAAACAAAAAQAAAAAfAAAAeyJ0eXBlIjoiQklHSU5UIiwibmFtZSI6IlR5cGUifQoAAAAAAVAAAAABAAAAAAAAAAIAAAAAAAAAAwAAAAAAAAAEAAAAAAAAAAUAAAAAAAAABgAAAAAAAAAHAAAAAAAAAAgAAAAAAAAACQAAAAAAAAAKAAAAAAAAAAEAAAAAIAAAAHsidHlwZSI6IklOVEVHRVIiLCJuYW1lIjoiVHlwZSJ9CgAAAAABKAAAAAEAAAACAAAAAwAAAAQAAAAFAAAABgAAAAcAAAAIAAAACQAAAAoAAAA=","parallelizable":false,"repeatTimes":1,"id":"0","name":"ValuesNode"}],"names":["c0","c1"],"id":"1","name":"ProjectNode"}],"id":"2","name":"AggregationNode"}],"keys":[{"fieldName":"c0","inputs":[{"type":{"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"BIGINT","name":"Type"}],"names":["c0","a0"],"type":"ROW","name":"Type"},"name":"InputTypedExpr"}],"type":{"type":"BIGINT","name":"Type"},"name":"FieldAccessTypedExpr"}],"kind":"PARTITIONED","id":"3","name":"PartitionedOutputNode"}})",
      /*final aggregation*/
      R"({"task_id":"abc.0.0.0.0","remote_task_ids":{"4":["abc.1.0.0.0"]},"plan_fragment":{"serdeKind":"Presto","partitionFunctionSpec":{"name":"GatherPartitionFunctionSpec"},"outputType":{"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"BIGINT","name":"Type"}],"names":["c0","a0"],"type":"ROW","name":"Type"},"replicateNullsAndAny":false,"keys":[],"numPartitions":1,"sources":[{"aggregateNames":["a0"],"aggregates":[{"distinct":false,"sortingKeys":[],"sortingOrders":[],"rawInputTypes":[{"type":"BIGINT","name":"Type"}],"call":{"functionName":"sum","inputs":[{"fieldName":"a0","inputs":[{"type":{"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"BIGINT","name":"Type"}],"names":["c0","a0"],"type":"ROW","name":"Type"},"name":"InputTypedExpr"}],"type":{"type":"BIGINT","name":"Type"},"name":"FieldAccessTypedExpr"}],"type":{"type":"BIGINT","name":"Type"},"name":"CallTypedExpr"}}],"globalGroupingSets":[],"ignoreNullKeys":false,"sources":[{"serdeKind":"Presto","outputType":{"names":["c0","a0"],"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"BIGINT","name":"Type"}],"type":"ROW","name":"Type"},"id":"4","name":"ExchangeNode"}],"step":"FINAL","preGroupedKeys":[],"groupingKeys":[{"fieldName":"c0","type":{"type":"BIGINT","name":"Type"},"name":"FieldAccessTypedExpr"}],"id":"5","name":"AggregationNode"}],"kind":"PARTITIONED","id":"6","name":"PartitionedOutputNode"}})",
      /*gathering 1*/
      R"({"task_id":"abc.3.0.0.0","remote_task_ids":{"13":["abc.0.0.0.0","abc.0.2.0.0","abc.0.1.0.0"]},"plan_fragment":{"replicateNullsAndAny":false,"serdeKind":"Presto","partitionFunctionSpec":{"name":"GatherPartitionFunctionSpec"},"outputType":{"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"BIGINT","name":"Type"}],"names":["c0","a0"],"type":"ROW","name":"Type"},"kind":"PARTITIONED","keys":[],"numPartitions":1,"sources":[{"outputType":{"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"BIGINT","name":"Type"}],"names":["c0","a0"],"type":"ROW","name":"Type"},"serdeKind":"Presto","id":"13","name":"ExchangeNode"}],"id":"14","name":"PartitionedOutputNode"}})",
      /*gathering 2*/
      R"({"task_id":"abc.2.0.0.0","remote_task_ids":{"15":["abc.3.0.0.0"]},"plan_fragment":{"outputType":{"cTypes":[{"type":"BIGINT","name":"Type"},{"type":"BIGINT","name":"Type"}],"names":["c0","a0"],"type":"ROW","name":"Type"},"serdeKind":"Presto","id":"15","name":"ExchangeNode"}})",
  };

  auto rootPool = memory::memoryManager()->addRootPool("testRootPool");
  auto pool = rootPool->addLeafChild("testLeafPool");
  PrestoQueryReplayRunner runner{pool.get(), getTaskPrefix};
  auto result = runner.run(queryId, serializedPlanFragments);

  auto expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2}),
      makeFlatVector<int64_t>({18, 22, 15}),
  });
  EXPECT_EQ(result.size(), 1);
  exec::test::assertEqualResults({expected}, result);
}

} // namespace
} // namespace facebook::velox::runner
