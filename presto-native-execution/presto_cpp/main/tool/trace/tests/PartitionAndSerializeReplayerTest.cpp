/*
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
/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 */
#include <folly/init/Init.h>
#include <folly/system/HardwareConcurrency.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/operators/tests/PlanBuilder.h"
#include "presto_cpp/main/tool/trace/PartitionAndSerializeReplayer.h"
#include "velox/exec/PartitionFunction.h"
#include "velox/exec/TraceUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/common/testutil/TempDirectoryPath.h"

using namespace facebook::velox;
using namespace facebook::velox::common::testutil;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::tool::trace;
using namespace facebook::presto;
using namespace facebook::presto::operators;

namespace {

// Mock operator that passes data through for tracing
class MockPartitionAndSerializeOperator : public Operator {
 public:
  MockPartitionAndSerializeOperator(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const PartitionAndSerializeNode>& node)
      : Operator(
            driverCtx,
            node->outputType(),
            operatorId,
            node->id(),
            "MockPartitionAndSerialize") {}

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    if (!input_) {
      return nullptr;
    }
    auto output = std::move(input_);
    return output;
  }

  bool needsInput() const override {
    return !input_;
  }

  void noMoreInput() override {
    Operator::noMoreInput();
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && !input_;
  }

 private:
  RowVectorPtr input_;
};

class MockPartitionAndSerializeTranslator
    : public Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    if (auto partitionNode =
            std::dynamic_pointer_cast<const PartitionAndSerializeNode>(node)) {
      return std::make_unique<MockPartitionAndSerializeOperator>(
          id, ctx, partitionNode);
    }
    return nullptr;
  }
};

} // namespace

class PartitionAndSerializeReplayerTest : public HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    exec::test::HiveConnectorTestBase::SetUpTestCase();
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    filesystems::registerLocalFileSystem();
    facebook::velox::exec::trace::registerDummySourceSerDe();
    if (!isRegisteredVectorSerde()) {
      serializer::presto::PrestoVectorSerde::registerVectorSerde();
    }
    Type::registerSerDe();
    common::Filter::registerSerDe();
    core::PlanNode::registerSerDe();
    core::ITypedExpr::registerSerDe();
    exec::registerPartitionFunctionSerDe();
    DeserializationWithContextRegistryForSharedPtr().Register(
        "PartitionAndSerializeNode", PartitionAndSerializeNode::create);
    exec::trace::registerTraceNodeFactory(
        "MockPartitionAndSerialize",
        [](const core::PlanNode* traceNode,
           const core::PlanNodeId& nodeId) -> core::PlanNodePtr {
          if (const auto* partitionNode =
                  dynamic_cast<const PartitionAndSerializeNode*>(traceNode)) {
            return std::make_shared<PartitionAndSerializeNode>(
                nodeId,
                partitionNode->keys(),
                partitionNode->numPartitions(),
                partitionNode->serializedRowType(),
                std::make_shared<exec::trace::DummySourceNode>(
                    partitionNode->sources().front()->outputType()),
                partitionNode->isReplicateNullsAndAny(),
                partitionNode->partitionFunctionFactory(),
                partitionNode->sortingOrders(),
                partitionNode->sortingKeys());
          }
          return nullptr;
        });
  }

  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
        folly::hardware_concurrency());
  }

  void TearDown() override {
    HiveConnectorTestBase::TearDown();
  }

  std::unique_ptr<folly::CPUThreadPoolExecutor> executor_;
};

TEST_F(PartitionAndSerializeReplayerTest, basicReplay) {
  auto traceDirPath = exec::test::TempDirectoryPath::create();
  const std::string traceRoot = traceDirPath->getPath();

  // Register mock operator for trace phase
  auto mockTranslator = std::make_unique<MockPartitionAndSerializeTranslator>();
  exec::Operator::registerOperator(std::move(mockTranslator));

  const auto inputData = makeRowVector({
      makeFlatVector<int32_t>({0, 1, 2, 3, 4}),
      makeFlatVector<std::string>(
          {"data-a", "data-b", "data-c", "data-d", "data-e"}),
  });

  // Create partition and serialize plan with mock operator
  const uint32_t numPartitions = 5;
  std::string partitionNodeId;
  auto plan = PlanBuilder()
                  .values({inputData})
                  .addNode(addPartitionAndSerializeNode(numPartitions, false))
                  .capturePlanNodeId(partitionNodeId)
                  .planNode();

  // Run the trace phase with mock operator
  std::shared_ptr<Task> task;
  AssertQueryBuilder(duckDbQueryRunner_)
      .plan(plan)
      .config(core::QueryConfig::kQueryTraceEnabled, true)
      .config(core::QueryConfig::kQueryTraceDir, traceRoot)
      .config(core::QueryConfig::kQueryTraceMaxBytes, 100UL << 30)
      .config(core::QueryConfig::kQueryTraceTaskRegExp, ".*")
      .config(core::QueryConfig::kQueryTraceNodeId, partitionNodeId)
      .copyResults(pool(), task);

  // Clear and register real operators for replay phase
  exec::Operator::unregisterAllOperators();
  exec::Operator::registerOperator(
      std::make_unique<PartitionAndSerializeTranslator>());

  // Test the replayer with the traced data
  const std::string driverIds = "0";
  const uint64_t queryCapacity = 1024 * 1024;

  PartitionAndSerializeReplayer replayer(
      traceRoot,
      task->queryCtx()->queryId(),
      task->taskId(),
      partitionNodeId,
      "PartitionAndSerialize",
      "",
      driverIds,
      queryCapacity,
      executor_.get());

  auto result = replayer.run();
  EXPECT_TRUE(result != nullptr);

  // Verify the output has the expected structure
  // Output should have 3 columns: partition (INTEGER), key (VARBINARY), data
  // (VARBINARY)
  EXPECT_EQ(result->childrenSize(), 3);
  EXPECT_EQ(result->childAt(0)->typeKind(), TypeKind::INTEGER);
  EXPECT_EQ(result->childAt(1)->typeKind(), TypeKind::VARBINARY);
  EXPECT_EQ(result->childAt(2)->typeKind(), TypeKind::VARBINARY);
  EXPECT_EQ(result->size(), inputData->size());
}
