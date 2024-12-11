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

#include "velox/exec/ScaleWriterLocalPartition.h"

#include "velox/core/PlanNode.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace {
using BlockingCallback = std::function<BlockingReason(ContinueFuture*)>;
using FinishCallback = std::function<void(bool)>;

// The object used to coordinate the behavior of the consumer and producer of
// the scale writer exchange for test purpose.
class TestExchangeController {
 public:
  TestExchangeController(
      uint32_t numProducers,
      uint32_t numConsumers,
      uint64_t holdBufferBytes,
      std::optional<double> producerTargetBufferMemoryRatio,
      std::optional<double> consumerTargetBufferMemoryRatio,
      std::optional<int32_t> producerMaxDelayUs,
      std::optional<int32_t> consumerMaxDelayUs,
      const std::vector<RowVectorPtr>& inputVectors,
      bool keepConsumerInput = true)
      : numProducers_(numProducers),
        numConsumers_(numConsumers),
        holdBufferBytes_(holdBufferBytes),
        producerTargetBufferMemoryRatio_(producerTargetBufferMemoryRatio),
        consumerTargetBufferMemoryRatio_(consumerTargetBufferMemoryRatio),
        producerMaxDelayUs_(producerMaxDelayUs),
        consumerMaxDelayUs_(consumerMaxDelayUs),
        keepConsumerInput_(keepConsumerInput),
        producerInputVectors_(inputVectors),
        consumerInputs_(numConsumers_) {}

  ~TestExchangeController() {
    clear();
  }

  // The number of drivers of the producer pipeline.
  uint32_t numProducers() const {
    return numProducers_;
  }

  // The number of drivers of the consumer pipeline.
  uint32_t numConsumers() const {
    return numConsumers_;
  }

  // Specifies the buffer to hold to test the effect of query memory usage
  // ratio for writer scaling control.
  uint64_t holdBufferBytes() const {
    return holdBufferBytes_;
  }

  void maybeHoldBuffer(memory::MemoryPool* pool) {
    std::lock_guard<std::mutex> l(mutex_);
    if (holdBufferBytes_ == 0) {
      return;
    }
    holdPool_ = pool;
    holdBuffer_ = holdPool_->allocate(holdBufferBytes_);
  }

  std::optional<double> producerTargetBufferMemoryRatio() const {
    return producerTargetBufferMemoryRatio_;
  }

  std::optional<double> consumerTargetBufferMemoryRatio() const {
    return consumerTargetBufferMemoryRatio_;
  }

  std::optional<int32_t> producerMaxDelayUs() const {
    return producerMaxDelayUs_;
  }

  std::optional<int32_t> consumerMaxDelayUs() const {
    return consumerMaxDelayUs_;
  }

  RowVectorPtr getInput() {
    std::lock_guard<std::mutex> l(mutex_);
    if (nextInput_ >= producerInputVectors_.size()) {
      return nullptr;
    }
    return producerInputVectors_[nextInput_++];
  }

  core::PlanNodeId exchangeNodeId() const {
    std::lock_guard<std::mutex> l(mutex_);
    return exchnangeNodeId_;
  }

  void setExchangeNodeId(const core::PlanNodeId& nodeId) {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(exchnangeNodeId_.empty());
    exchnangeNodeId_ = nodeId;
  }

  void addConsumerInput(uint32_t consumerId, const RowVectorPtr& input) {
    if (!keepConsumerInput_) {
      return;
    }
    consumerInputs_[consumerId].push_back(input);
  }

  const std::vector<std::vector<RowVectorPtr>> consumerInputs() const {
    std::lock_guard<std::mutex> l(mutex_);
    return consumerInputs_;
  }

  void clear() {
    std::lock_guard<std::mutex> l(mutex_);
    if (holdBuffer_ != nullptr) {
      holdPool_->free(holdBuffer_, holdBufferBytes_);
      holdBuffer_ = nullptr;
    }
    for (auto& input : consumerInputs_) {
      input.clear();
    }
    consumerInputs_.clear();
  }

 private:
  const uint32_t numProducers_;
  const uint32_t numConsumers_;
  const uint64_t holdBufferBytes_;
  const std::optional<double> producerTargetBufferMemoryRatio_;
  const std::optional<double> consumerTargetBufferMemoryRatio_;
  const std::optional<int32_t> producerMaxDelayUs_;
  const std::optional<int32_t> consumerMaxDelayUs_;
  const bool keepConsumerInput_;

  mutable std::mutex mutex_;
  memory::MemoryPool* holdPool_{nullptr};
  void* holdBuffer_{nullptr};
  core::PlanNodeId exchnangeNodeId_;
  uint32_t nextInput_{0};
  std::vector<RowVectorPtr> producerInputVectors_;
  std::vector<std::vector<RowVectorPtr>> consumerInputs_;
};

class FakeSourceNode : public core::PlanNode {
 public:
  FakeSourceNode(const core::PlanNodeId& id, const RowTypePtr& ouputType)
      : PlanNode(id), ouputType_{ouputType} {}

  const RowTypePtr& outputType() const override {
    return ouputType_;
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    static const std::vector<core::PlanNodePtr> kEmptySources;
    return kEmptySources;
  }

  std::string_view name() const override {
    return "FakeSourceNode";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  const RowTypePtr ouputType_;
};

class FakeSourceOperator : public SourceOperator {
 public:
  FakeSourceOperator(
      DriverCtx* ctx,
      int32_t id,
      const std::shared_ptr<const FakeSourceNode>& node,
      const std::shared_ptr<TestExchangeController>& testController)
      : SourceOperator(
            ctx,
            node->outputType(),
            id,
            node->id(),
            "FakeSourceOperator"),
        testController_(testController) {
    VELOX_CHECK_NOT_NULL(testController_);
    rng_.seed(id);
  }

  RowVectorPtr getOutput() override {
    VELOX_CHECK(!finished_);
    auto output = testController_->getInput();
    if (output == nullptr) {
      finished_ = true;
      return nullptr;
    }
    waitForOutput();
    return output;
  }

  bool isFinished() override {
    return finished_;
  }

  BlockingReason isBlocked(ContinueFuture* /*unused*/) override {
    return BlockingReason::kNotBlocked;
  }

 private:
  void initialize() override {
    Operator::initialize();

    if (operatorCtx_->driverCtx()->driverId != 0) {
      return;
    }

    testController_->maybeHoldBuffer(pool());
  }

  void waitForOutput() {
    if (FOLLY_UNLIKELY(memoryManager_ == nullptr)) {
      memoryManager_ =
          operatorCtx_->driver()->task()->getLocalExchangeMemoryManager(
              operatorCtx_->driverCtx()->splitGroupId,
              testController_->exchangeNodeId());
    }

    if (testController_->producerTargetBufferMemoryRatio().has_value()) {
      while (memoryManager_->bufferedBytes() >
             (memoryManager_->maxBufferBytes() *
              testController_->producerTargetBufferMemoryRatio().value())) {
        std::this_thread::sleep_for(std::chrono::microseconds(100)); // NOLINT
      }
    }
    if (testController_->producerMaxDelayUs().has_value()) {
      const auto delayUs = folly::Random::rand32(rng_) %
          testController_->producerMaxDelayUs().value();
      std::this_thread::sleep_for(std::chrono::microseconds(delayUs)); // NOLINT
    }
  }

  const std::shared_ptr<TestExchangeController> testController_;
  folly::Random::DefaultGenerator rng_;

  std::shared_ptr<LocalExchangeMemoryManager> memoryManager_;
  bool finished_{false};
};

class FakeSourceNodeFactory : public Operator::PlanNodeTranslator {
 public:
  explicit FakeSourceNodeFactory(
      const std::shared_ptr<TestExchangeController>& testController)
      : testController_(testController) {}

  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    auto fakeSourceNode = std::dynamic_pointer_cast<const FakeSourceNode>(node);
    if (fakeSourceNode == nullptr) {
      return nullptr;
    }
    return std::make_unique<FakeSourceOperator>(
        ctx,
        id,
        std::dynamic_pointer_cast<const FakeSourceNode>(node),
        testController_);
  }

  std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& node) override {
    auto fakeSourceNode = std::dynamic_pointer_cast<const FakeSourceNode>(node);
    if (fakeSourceNode == nullptr) {
      return std::nullopt;
    }
    return testController_->numProducers();
  }

 private:
  const std::shared_ptr<TestExchangeController> testController_;
};

class FakeWriteNode : public core::PlanNode {
 public:
  FakeWriteNode(const core::PlanNodeId& id, const core::PlanNodePtr& input)
      : PlanNode(id), sources_{input} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<std::shared_ptr<const PlanNode>>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "FakeWriteNode";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}
  std::vector<core::PlanNodePtr> sources_;
};

class FakeWriteOperator : public Operator {
 public:
  FakeWriteOperator(
      DriverCtx* ctx,
      int32_t id,
      const std::shared_ptr<const FakeWriteNode>& node,
      const std::shared_ptr<TestExchangeController>& testController)
      : Operator(ctx, node->outputType(), id, node->id(), "FakeWriteOperator"),
        testController_(testController) {
    VELOX_CHECK_NOT_NULL(testController_);
    rng_.seed(id);
  }

  void initialize() override {
    Operator::initialize();
    VELOX_CHECK(exchangeQueues_.empty());
    exchangeQueues_ = operatorCtx_->driver()->task()->getLocalExchangeQueues(
        operatorCtx_->driverCtx()->splitGroupId,
        testController_->exchangeNodeId());
  }

  bool needsInput() const override {
    return !noMoreInput_ && !input_;
  }

  void addInput(RowVectorPtr input) override {
    waitForConsume();
    testController_->addConsumerInput(
        operatorCtx_->driverCtx()->driverId, input);
    input_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    return std::move(input_);
  }

  bool isFinished() override {
    return noMoreInput_ && input_ == nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* /*unused*/) override {
    return BlockingReason::kNotBlocked;
  }

 private:
  // Returns true if the producers of all the exchange queues are done.
  bool exchangeQueueClosed() const {
    for (const auto& queue : exchangeQueues_) {
      if (!queue->testingProducersDone()) {
        return false;
      }
    }
    return true;
  }

  void waitForConsume() {
    if (FOLLY_UNLIKELY(memoryManager_ == nullptr)) {
      memoryManager_ =
          operatorCtx_->driver()->task()->getLocalExchangeMemoryManager(
              operatorCtx_->driverCtx()->splitGroupId,
              testController_->exchangeNodeId());
    }
    if (testController_->consumerTargetBufferMemoryRatio().has_value()) {
      while (!exchangeQueueClosed() &&
             memoryManager_->bufferedBytes() <
                 (memoryManager_->maxBufferBytes() *
                  testController_->consumerTargetBufferMemoryRatio().value())) {
        std::this_thread::sleep_for(std::chrono::microseconds(100)); // NOLINT
      }
    }
    if (testController_->consumerMaxDelayUs().has_value()) {
      const auto delayUs = folly::Random::rand32(rng_) %
          testController_->consumerMaxDelayUs().value();
      std::this_thread::sleep_for(std::chrono::microseconds(delayUs)); // NOLINT
    }
  }

  const std::shared_ptr<TestExchangeController> testController_;
  folly::Random::DefaultGenerator rng_;

  std::shared_ptr<LocalExchangeMemoryManager> memoryManager_;
  std::vector<std::shared_ptr<LocalExchangeQueue>> exchangeQueues_;
};

class FakeWriteNodeFactory : public Operator::PlanNodeTranslator {
 public:
  explicit FakeWriteNodeFactory(
      const std::shared_ptr<TestExchangeController>& testController)
      : testController_(testController) {}

  std::unique_ptr<Operator> toOperator(
      DriverCtx* ctx,
      int32_t id,
      const core::PlanNodePtr& node) override {
    auto fakeWriteNode = std::dynamic_pointer_cast<const FakeWriteNode>(node);
    if (fakeWriteNode == nullptr) {
      return nullptr;
    }
    return std::make_unique<FakeWriteOperator>(
        ctx,
        id,
        std::dynamic_pointer_cast<const FakeWriteNode>(node),
        testController_);
  }

  std::optional<uint32_t> maxDrivers(const core::PlanNodePtr& node) override {
    auto fakeWriteNode = std::dynamic_pointer_cast<const FakeWriteNode>(node);
    if (fakeWriteNode == nullptr) {
      return std::nullopt;
    }
    return testController_->numConsumers();
  }

 private:
  const std::shared_ptr<TestExchangeController> testController_;
};
} // namespace

class ScaleWriterLocalPartitionTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();

    rng_.seed(123);
    rowType_ = ROW({"c0", "c1", "c3"}, {BIGINT(), INTEGER(), BIGINT()});
  }

  void TearDown() override {
    Operator::unregisterAllOperators();
    HiveConnectorTestBase::TearDown();
  }

  std::vector<RowVectorPtr> makeVectors(
      uint32_t numVectors,
      uint32_t vectorSize,
      const std::vector<int32_t>& partitionKeyValues = {}) {
    VectorFuzzer::Options options;
    options.vectorSize = vectorSize;
    options.allowLazyVector = false;
    // NOTE: order by used to sort data doesn't handle null rows.
    options.nullRatio = 0.0;
    VectorFuzzer fuzzer(options, pool_.get());

    std::vector<RowVectorPtr> vectors;
    for (auto i = 0; i < numVectors; ++i) {
      vectors.push_back(fuzzer.fuzzRow(rowType_));
    }
    if (partitionKeyValues.empty()) {
      return vectors;
    }
    for (auto i = 0; i < numVectors; ++i) {
      auto partitionVector = BaseVector::create(
          vectors[i]->childAt(partitionChannel_)->type(),
          vectors[i]->childAt(partitionChannel_)->size(),
          pool_.get());
      auto* partitionVectorFlat = partitionVector->asFlatVector<int32_t>();
      for (auto j = 0; j < partitionVector->size(); ++j) {
        partitionVectorFlat->set(
            j,
            partitionKeyValues
                [folly::Random::rand32(rng_) % partitionKeyValues.size()]);
      }
      vectors[i]->childAt(partitionChannel_) = partitionVector;
    }
    return vectors;
  }

  std::string partitionColumnName() const {
    return rowType_->nameOf(partitionChannel_);
  }

  RowVectorPtr sortData(const std::vector<RowVectorPtr>& inputVectors) {
    std::vector<std::string> orderByKeys;
    orderByKeys.reserve(rowType_->size());
    for (const auto& name : rowType_->names()) {
      orderByKeys.push_back(fmt::format("{} ASC NULLS FIRST", name));
    }
    AssertQueryBuilder queryBuilder(PlanBuilder()
                                        .values(inputVectors)
                                        .orderBy(orderByKeys, false)
                                        .planNode());
    return queryBuilder.copyResults(pool_.get());
  }

  void verifyResults(
      const std::vector<RowVectorPtr>& actual,
      const std::vector<RowVectorPtr>& expected) {
    const auto actualSorted = sortData(actual);
    const auto expectedSorted = sortData(expected);
    exec::test::assertEqualResults({actualSorted}, {expectedSorted});
  }

  // Returns the partition keys set of the input 'vectors'.
  std::set<int32_t> partitionKeys(const std::vector<RowVectorPtr>& vectors) {
    DecodedVector partitionColumnDecoder;
    std::set<int32_t> keys;
    for (const auto& vector : vectors) {
      partitionColumnDecoder.decode(*vector->childAt(partitionChannel_));
      for (auto i = 0; i < partitionColumnDecoder.size(); ++i) {
        keys.insert(partitionColumnDecoder.valueAt<int32_t>(i));
      }
    }
    return keys;
  }

  // Verifies the partition keys of the consumer inputs from 'controller' are
  // disjoint.
  void verifyDisjointPartitionKeys(TestExchangeController* controller) {
    std::set<int32_t> allPartitionKeys;
    for (const auto& consumerInput : controller->consumerInputs()) {
      std::set<int32_t> consumerPartitionKeys = partitionKeys(consumerInput);
      std::set<int32_t> diffPartitionKeys;
      std::set_difference(
          consumerPartitionKeys.begin(),
          consumerPartitionKeys.end(),
          allPartitionKeys.begin(),
          allPartitionKeys.end(),
          std::inserter(diffPartitionKeys, diffPartitionKeys.end()));
      ASSERT_EQ(diffPartitionKeys.size(), consumerPartitionKeys.size())
          << "diffPartitionKeys: " << folly::join(",", diffPartitionKeys)
          << " consumerPartitionKeys: "
          << folly::join(",", consumerPartitionKeys)
          << ", allPartitionKeys: " << folly::join(",", allPartitionKeys);
      allPartitionKeys = diffPartitionKeys;
    }
  }

  const column_index_t partitionChannel_{1};
  folly::Random::DefaultGenerator rng_;
  RowTypePtr rowType_;
};

TEST_F(ScaleWriterLocalPartitionTest, unpartitionBasic) {
  const std::vector<RowVectorPtr> inputVectors = makeVectors(128, 1024);
  const uint64_t queryCapacity = 256 << 20;
  const uint32_t maxDrivers = 32;
  const uint32_t maxExchanegBufferSize = 2 << 20;

  struct {
    uint32_t numProducers;
    uint32_t numConsumers;
    uint64_t rebalanceProcessBytesThreshold;
    double scaleWriterRebalanceMaxMemoryUsageRatio;
    uint64_t holdBufferBytes;
    double producerBufferedMemoryRatio;
    double consumerBufferedMemoryRatio;
    bool expectedRebalance;

    std::string debugString() const {
      return fmt::format(
          "numProducers {}, numConsumers {}, rebalanceProcessBytesThreshold {}, scaleWriterRebalanceMaxMemoryUsageRatio {}, holdBufferBytes {}, producerBufferedMemoryRatio {}, consumerBufferedMemoryRatio {}, expectedRebalance {}",
          numProducers,
          numConsumers,
          succinctBytes(rebalanceProcessBytesThreshold),
          scaleWriterRebalanceMaxMemoryUsageRatio,
          succinctBytes(holdBufferBytes),
          producerBufferedMemoryRatio,
          consumerBufferedMemoryRatio,
          expectedRebalance);
    }
  } testSettings[] = {
      {1, 1, 0, 1.0, 0, 0.8, 0.6, false},
      {4, 1, 0, 1.0, 0, 0.8, 0.6, false},
      {1, 4, 1ULL << 30, 1.0, 0, 0.8, 0.6, false},
      {4, 4, 1ULL << 30, 1.0, 0, 0.8, 0.6, false},
      {1, 4, 0, 1.0, 0, 0.3, 0.2, false},
      {4, 4, 0, 1.0, 0, 0.3, 0.2, false},
      {1, 4, 0, 0.1, queryCapacity / 2, 0.8, 0.6, false},
      {4, 4, 0, 0.1, queryCapacity / 2, 0.8, 0.6, false},
      {1, 4, 0, 1.0, 0, 0.8, 0.6, true},
      {4, 4, 0, 1.0, 0, 0.8, 0.6, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    ASSERT_LE(testData.numProducers, maxDrivers);
    ASSERT_LE(testData.numConsumers, maxDrivers);

    Operator::unregisterAllOperators();

    auto testController = std::make_shared<TestExchangeController>(
        testData.numProducers,
        testData.numConsumers,
        testData.holdBufferBytes,
        testData.producerBufferedMemoryRatio,
        testData.consumerBufferedMemoryRatio,
        std::nullopt,
        std::nullopt,
        inputVectors);
    Operator::registerOperator(
        std::make_unique<FakeWriteNodeFactory>(testController));
    Operator::registerOperator(
        std::make_unique<FakeSourceNodeFactory>(testController));

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId exchnangeNodeId;
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .addNode([&](const core::PlanNodeId& id,
                                 const core::PlanNodePtr& input) {
                      return std::make_shared<FakeSourceNode>(id, rowType_);
                    })
                    .scaleWriterlocalPartitionRoundRobin()
                    .capturePlanNodeId(exchnangeNodeId)
                    .addNode([](const core::PlanNodeId& id,
                                const core::PlanNodePtr& input) {
                      return std::make_shared<FakeWriteNode>(id, input);
                    })
                    .planNode();
    testController->setExchangeNodeId(exchnangeNodeId);

    AssertQueryBuilder queryBuilder(plan);
    std::shared_ptr<Task> task;
    const auto result =
        // Consumer and producer have overload the max drivers of their
        // associated pipelines. We set max driver for the query to make sure it
        // is larger than the customiized driver count for consumer and
        // producer.
        queryBuilder.maxDrivers(maxDrivers)
            .maxQueryCapacity(queryCapacity)
            .config(
                core::QueryConfig::kMaxLocalExchangeBufferSize,
                std::to_string(maxExchanegBufferSize))
            .config(
                core::QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio,
                std::to_string(
                    testData.scaleWriterRebalanceMaxMemoryUsageRatio))
            .config(
                core::QueryConfig::
                    kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
                std::to_string(testData.rebalanceProcessBytesThreshold))
            .copyResults(pool_.get(), task);
    uint32_t nonEmptyConsumers{0};
    for (const auto& consumerInput : testController->consumerInputs()) {
      if (!consumerInput.empty()) {
        ++nonEmptyConsumers;
      }
    }
    auto planStats = toPlanStats(task->taskStats());
    if (testData.expectedRebalance) {
      ASSERT_GT(
          planStats.at(exchnangeNodeId)
              .customStats.at(ScaleWriterLocalPartition::kScaledWriters)
              .sum,
          0);
      ASSERT_LE(
          planStats.at(exchnangeNodeId)
              .customStats.at(ScaleWriterLocalPartition::kScaledWriters)
              .sum,
          planStats.at(exchnangeNodeId)
                  .customStats.at(ScaleWriterLocalPartition::kScaledWriters)
                  .count *
              (testData.numConsumers - 1));
      ASSERT_GT(nonEmptyConsumers, 1);
    } else {
      ASSERT_EQ(
          planStats.at(exchnangeNodeId)
              .customStats.count(ScaleWriterLocalPartition::kScaledWriters),
          0);
      ASSERT_EQ(nonEmptyConsumers, 1);
    }

    testController->clear();
    task.reset();

    verifyResults(inputVectors, {result});
    waitForAllTasksToBeDeleted();
  }
}

TEST_F(ScaleWriterLocalPartitionTest, unpartitionFuzzer) {
  const std::vector<RowVectorPtr> inputVectors = makeVectors(256, 512);
  const uint64_t queryCapacity = 256 << 20;
  const uint32_t maxExchanegBufferSize = 2 << 20;

  for (bool fastConsumer : {false, true}) {
    SCOPED_TRACE(fmt::format("fastConsumer: {}", fastConsumer));
    Operator::unregisterAllOperators();

    auto testController = std::make_shared<TestExchangeController>(
        fastConsumer ? 1 : 4,
        4,
        0,
        std::nullopt,
        std::nullopt,
        fastConsumer ? 4 : 64,
        fastConsumer ? 64 : 4,
        inputVectors,
        /*keepConsumerInput=*/false);
    Operator::registerOperator(
        std::make_unique<FakeWriteNodeFactory>(testController));
    Operator::registerOperator(
        std::make_unique<FakeSourceNodeFactory>(testController));

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId exchnangeNodeId;
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .addNode([&](const core::PlanNodeId& id,
                                 const core::PlanNodePtr& input) {
                      return std::make_shared<FakeSourceNode>(id, rowType_);
                    })
                    .scaleWriterlocalPartitionRoundRobin()
                    .capturePlanNodeId(exchnangeNodeId)
                    .addNode([](const core::PlanNodeId& id,
                                const core::PlanNodePtr& input) {
                      return std::make_shared<FakeWriteNode>(id, input);
                    })
                    .planNode();
    testController->setExchangeNodeId(exchnangeNodeId);

    AssertQueryBuilder queryBuilder(plan);
    std::shared_ptr<Task> task;
    const auto result =
        // Consumer and producer have overload the max drivers of their
        // associated pipelines. We set max driver for the query to make sure it
        // is larger than the customiized driver count for consumer and
        // producer.
        queryBuilder.maxDrivers(32)
            .maxQueryCapacity(queryCapacity)
            .config(
                core::QueryConfig::kMaxLocalExchangeBufferSize,
                std::to_string(maxExchanegBufferSize))
            .config(
                core::QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio,
                "1.0")
            .config(
                core::QueryConfig::
                    kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
                "256")
            .copyResults(pool_.get());
    verifyResults(inputVectors, {result});
    waitForAllTasksToBeDeleted();
  }
}

TEST_F(ScaleWriterLocalPartitionTest, partitionBasic) {
  const uint64_t queryCapacity = 256 << 20;
  const uint32_t maxDrivers = 32;
  const uint32_t maxExchanegBufferSize = 2 << 20;

  struct {
    uint32_t numProducers;
    uint32_t numConsumers;
    uint32_t numPartitionsPerWriter;
    uint64_t rebalanceProcessBytesThreshold;
    double scaleWriterRebalanceMaxMemoryUsageRatio;
    uint64_t holdBufferBytes;
    std::vector<int32_t> partitionKeys;
    double producerBufferedMemoryRatio;
    double consumerBufferedMemoryRatio;
    bool expectedRebalance;

    std::string debugString() const {
      return fmt::format(
          "numProducers {}, numConsumers {}, numPartitionsPerWriter {}, rebalanceProcessBytesThreshold {}, scaleWriterRebalanceMaxMemoryUsageRatio {}, holdBufferBytes {}, partitionKeys {}, producerBufferedMemoryRatio {}, consumerBufferedMemoryRatio {}, expectedRebalance {}",
          numProducers,
          numConsumers,
          numPartitionsPerWriter,
          succinctBytes(rebalanceProcessBytesThreshold),
          scaleWriterRebalanceMaxMemoryUsageRatio,
          succinctBytes(holdBufferBytes),
          folly::join(":", partitionKeys),
          producerBufferedMemoryRatio,
          consumerBufferedMemoryRatio,
          expectedRebalance);
    }
  } testSettings[] = {
      {1, 1, 4, 0, 1.0, 0, {1, 2}, 0.8, 0.6, false},
      {4, 1, 4, 0, 1.0, 0, {1, 2}, 0.8, 0.6, false},
      {1, 4, 4, 1ULL << 30, 1.0, 0, {1, 2}, 0.8, 0.6, false},
      {4, 4, 4, 1ULL << 30, 1.0, 0, {1, 2}, 0.8, 0.6, false},
      {1, 4, 4, 0, 1.0, 0, {1, 2}, 0.3, 0.2, false},
      {4, 4, 4, 0, 1.0, 0, {1, 2}, 0.3, 0.2, false},
      {1, 4, 4, 0, 0.1, queryCapacity / 2, {1, 2}, 0.8, 0.6, false},
      {4, 4, 4, 0, 0.1, queryCapacity / 2, {1, 2}, 0.8, 0.6, false},
      {1, 32, 128, 0, 1.0, 0, {1, 2, 3, 4, 5, 6, 7, 8}, 0.8, 0.6, true},
      {4, 32, 128, 0, 1.0, 0, {1, 2, 3, 4, 5, 6, 7, 8}, 0.8, 0.6, true},
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    ASSERT_LE(testData.numProducers, maxDrivers);
    ASSERT_LE(testData.numConsumers, maxDrivers);

    Operator::unregisterAllOperators();

    const std::vector<RowVectorPtr> inputVectors =
        makeVectors(32, 2048, testData.partitionKeys);

    auto testController = std::make_shared<TestExchangeController>(
        testData.numProducers,
        testData.numConsumers,
        testData.holdBufferBytes,
        testData.producerBufferedMemoryRatio,
        testData.consumerBufferedMemoryRatio,
        std::nullopt,
        std::nullopt,
        inputVectors);
    Operator::registerOperator(
        std::make_unique<FakeWriteNodeFactory>(testController));
    Operator::registerOperator(
        std::make_unique<FakeSourceNodeFactory>(testController));

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId exchnangeNodeId;
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .addNode([&](const core::PlanNodeId& id,
                                 const core::PlanNodePtr& input) {
                      return std::make_shared<FakeSourceNode>(id, rowType_);
                    })
                    .scaleWriterlocalPartition({partitionColumnName()})
                    .capturePlanNodeId(exchnangeNodeId)
                    .addNode([](const core::PlanNodeId& id,
                                const core::PlanNodePtr& input) {
                      return std::make_shared<FakeWriteNode>(id, input);
                    })
                    .planNode();
    testController->setExchangeNodeId(exchnangeNodeId);

    AssertQueryBuilder queryBuilder(plan);
    std::shared_ptr<Task> task;
    const auto result =
        // Consumer and producer have overload the max drivers of their
        // associated pipelines. We set max driver for the query to make sure it
        // is larger than the customiized driver count for consumer and
        // producer.
        queryBuilder.maxDrivers(maxDrivers)
            .maxQueryCapacity(queryCapacity)
            .config(
                core::QueryConfig::kMaxLocalExchangeBufferSize,
                std::to_string(maxExchanegBufferSize))
            .config(
                core::QueryConfig::kScaleWriterMaxPartitionsPerWriter,
                std::to_string(testData.numPartitionsPerWriter))
            .config(
                core::QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio,
                std::to_string(
                    testData.scaleWriterRebalanceMaxMemoryUsageRatio))
            .config(
                core::QueryConfig::
                    kScaleWriterMinProcessedBytesRebalanceThreshold,
                std::to_string(testData.rebalanceProcessBytesThreshold))
            .config(
                core::QueryConfig::
                    kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
                "0")
            .copyResults(pool_.get(), task);
    auto planStats = toPlanStats(task->taskStats());
    if (testData.expectedRebalance) {
      ASSERT_EQ(
          planStats.at(exchnangeNodeId)
              .customStats.count(
                  ScaleWriterPartitioningLocalPartition::kScaledPartitions),
          1);
      ASSERT_GT(
          planStats.at(exchnangeNodeId)
              .customStats
              .at(ScaleWriterPartitioningLocalPartition::kScaledPartitions)
              .sum,
          0);
      ASSERT_EQ(
          planStats.at(exchnangeNodeId)
              .customStats.count(
                  ScaleWriterPartitioningLocalPartition::kRebalanceTriggers),
          1);
      ASSERT_GT(
          planStats.at(exchnangeNodeId)
              .customStats
              .at(ScaleWriterPartitioningLocalPartition::kRebalanceTriggers)
              .sum,
          0);
    } else {
      ASSERT_EQ(
          planStats.at(exchnangeNodeId)
              .customStats.count(
                  ScaleWriterPartitioningLocalPartition::kScaledPartitions),
          0);
      ASSERT_EQ(
          planStats.at(exchnangeNodeId)
              .customStats.count(
                  ScaleWriterPartitioningLocalPartition::kRebalanceTriggers),
          0);
      verifyDisjointPartitionKeys(testController.get());
    }
    testController->clear();
    task.reset();

    verifyResults(inputVectors, {result});
    waitForAllTasksToBeDeleted();
  }
}

TEST_F(ScaleWriterLocalPartitionTest, partitionFuzzer) {
  const std::vector<RowVectorPtr> inputVectors =
      makeVectors(1024, 256, {1, 2, 3, 4, 5, 6, 7, 8});
  const uint64_t queryCapacity = 256 << 20;
  const uint32_t maxExchanegBufferSize = 2 << 20;

  for (bool fastConsumer : {false, true}) {
    SCOPED_TRACE(fmt::format("fastConsumer: {}", fastConsumer));
    Operator::unregisterAllOperators();

    auto testController = std::make_shared<TestExchangeController>(
        fastConsumer ? 1 : 4,
        4,
        0,
        std::nullopt,
        std::nullopt,
        fastConsumer ? 4 : 64,
        fastConsumer ? 64 : 4,
        inputVectors,
        /*keepConsumerInput=*/false);
    Operator::registerOperator(
        std::make_unique<FakeWriteNodeFactory>(testController));
    Operator::registerOperator(
        std::make_unique<FakeSourceNodeFactory>(testController));

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
    core::PlanNodeId exchnangeNodeId;
    auto plan = PlanBuilder(planNodeIdGenerator)
                    .addNode([&](const core::PlanNodeId& id,
                                 const core::PlanNodePtr& input) {
                      return std::make_shared<FakeSourceNode>(id, rowType_);
                    })
                    .scaleWriterlocalPartition({partitionColumnName()})
                    .capturePlanNodeId(exchnangeNodeId)
                    .addNode([](const core::PlanNodeId& id,
                                const core::PlanNodePtr& input) {
                      return std::make_shared<FakeWriteNode>(id, input);
                    })
                    .planNode();
    testController->setExchangeNodeId(exchnangeNodeId);

    AssertQueryBuilder queryBuilder(plan);
    std::shared_ptr<Task> task;
    const auto result =
        // Consumer and producer have overload the max drivers of their
        // associated pipelines. We set max driver for the query to make sure it
        // is larger than the customiized driver count for consumer and
        // producer.
        queryBuilder.maxDrivers(32)
            .maxQueryCapacity(queryCapacity)
            .config(
                core::QueryConfig::kMaxLocalExchangeBufferSize,
                std::to_string(maxExchanegBufferSize))
            .config(
                core::QueryConfig::kScaleWriterRebalanceMaxMemoryUsageRatio,
                "1.0")
            .config(
                core::QueryConfig::
                    kScaleWriterMinProcessedBytesRebalanceThreshold,
                "256")
            .config(
                core::QueryConfig::
                    kScaleWriterMinPartitionProcessedBytesRebalanceThreshold,
                "32")
            .copyResults(pool_.get());
    verifyResults(inputVectors, {result});
    waitForAllTasksToBeDeleted();
  }
}
