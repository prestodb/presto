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
#include "velox/exec/JoinBridge.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class CustomJoinNode : public core::PlanNode {
 public:
  CustomJoinNode(
      const core::PlanNodeId& id,
      core::PlanNodePtr left,
      core::PlanNodePtr right)
      : PlanNode(id), sources_{std::move(left), std::move(right)} {}

  const RowTypePtr& outputType() const override {
    return sources_[0]->outputType();
  }

  const std::vector<core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "custom join";
  }

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  std::vector<core::PlanNodePtr> sources_;
};

class CustomJoinBridge : public JoinBridge {
 public:
  void setNumRows(std::optional<int32_t> numRows) {
    std::vector<ContinuePromise> promises;
    {
      std::lock_guard<std::mutex> l(mutex_);
      VELOX_CHECK(!numRows_.has_value(), "setNumRows may be called only once");
      numRows_ = numRows;
      promises = std::move(promises_);
    }
    notify(std::move(promises));
  }

  std::optional<int32_t> numRowsOrFuture(ContinueFuture* future) {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(!cancelled_, "Getting data after the build side is aborted");
    if (numRows_.has_value()) {
      return numRows_;
    }
    promises_.emplace_back("CustomJoinBridge::numRowsOrFuture");
    *future = promises_.back().getSemiFuture();
    return std::nullopt;
  }

 private:
  std::optional<int32_t> numRows_;
};

class CustomJoinBuild : public Operator {
 public:
  CustomJoinBuild(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::shared_ptr<const CustomJoinNode> joinNode)
      : Operator(
            driverCtx,
            nullptr,
            operatorId,
            joinNode->id(),
            "CustomJoinBuild") {}

  void addInput(RowVectorPtr input) override {
    auto inputSize = input->size();
    if (inputSize > 0) {
      numRows_ += inputSize;
    }
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  void noMoreInput() override {
    Operator::noMoreInput();
    std::vector<ContinuePromise> promises;
    std::vector<std::shared_ptr<Driver>> peers;
    // The last Driver to hit CustomJoinBuild::finish gathers the data from
    // all build Drivers and hands it over to the probe side. At this
    // point all build Drivers are continued and will free their
    // state. allPeersFinished is true only for the last Driver of the
    // build pipeline.
    if (!operatorCtx_->task()->allPeersFinished(
            planNodeId(), operatorCtx_->driver(), &future_, promises, peers)) {
      return;
    }

    for (auto& peer : peers) {
      auto op = peer->findOperator(planNodeId());
      auto* build = dynamic_cast<CustomJoinBuild*>(op);
      VELOX_CHECK(build);
      numRows_ += build->numRows_;
    }

    // Realize the promises so that the other Drivers (which were not
    // the last to finish) can continue from the barrier and finish.
    peers.clear();
    for (auto& promise : promises) {
      promise.setValue();
    }

    auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId());
    auto customJoinBridge =
        std::dynamic_pointer_cast<CustomJoinBridge>(joinBridge);
    customJoinBridge->setNumRows(std::make_optional(numRows_));
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    if (!future_.valid()) {
      return BlockingReason::kNotBlocked;
    }
    *future = std::move(future_);
    return BlockingReason::kWaitForJoinBuild;
  }

  bool isFinished() override {
    return !future_.valid() && noMoreInput_;
  }

 private:
  int32_t numRows_ = 0;

  ContinueFuture future_{ContinueFuture::makeEmpty()};
};

class CustomJoinProbe : public Operator {
 public:
  CustomJoinProbe(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::shared_ptr<const CustomJoinNode> joinNode)
      : Operator(
            driverCtx,
            nullptr,
            operatorId,
            joinNode->id(),
            "CustomJoinProbe") {}

  bool needsInput() const override {
    return !finished_ && input_ == nullptr;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    if (!input_) {
      return nullptr;
    }

    const auto inputSize = input_->size();
    if (remainingLimit_ <= inputSize) {
      finished_ = true;
    }

    if (remainingLimit_ >= inputSize) {
      remainingLimit_ -= inputSize;
      auto output = input_;
      input_.reset();
      return output;
    }

    // Return nullptr if there is no data to return.
    if (remainingLimit_ == 0) {
      input_.reset();
      return nullptr;
    }

    auto output = std::make_shared<RowVector>(
        input_->pool(),
        input_->type(),
        input_->nulls(),
        remainingLimit_,
        input_->children());
    input_.reset();
    remainingLimit_ = 0;
    return output;
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    if (numRows_.has_value()) {
      return BlockingReason::kNotBlocked;
    }

    auto joinBridge = operatorCtx_->task()->getCustomJoinBridge(
        operatorCtx_->driverCtx()->splitGroupId, planNodeId());
    auto numRows = std::dynamic_pointer_cast<CustomJoinBridge>(joinBridge)
                       ->numRowsOrFuture(future);

    if (!numRows.has_value()) {
      return BlockingReason::kWaitForJoinBuild;
    }
    numRows_ = std::move(numRows);
    remainingLimit_ = numRows_.value();

    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_ || (noMoreInput_ && input_ == nullptr);
  }

 private:
  int32_t remainingLimit_;
  std::optional<int32_t> numRows_;

  bool finished_{false};
};

class CustomJoinBridgeTranslator : public Operator::PlanNodeTranslator {
  std::unique_ptr<Operator>
  toOperator(DriverCtx* ctx, int32_t id, const core::PlanNodePtr& node) {
    if (auto joinNode = std::dynamic_pointer_cast<const CustomJoinNode>(node)) {
      return std::make_unique<CustomJoinProbe>(id, ctx, joinNode);
    }
    return nullptr;
  }

  std::unique_ptr<JoinBridge> toJoinBridge(const core::PlanNodePtr& node) {
    if (auto joinNode = std::dynamic_pointer_cast<const CustomJoinNode>(node)) {
      auto joinBridge = std::make_unique<CustomJoinBridge>();
      return joinBridge;
    }
    return nullptr;
  }

  OperatorSupplier toOperatorSupplier(const core::PlanNodePtr& node) {
    if (auto joinNode = std::dynamic_pointer_cast<const CustomJoinNode>(node)) {
      return [joinNode](int32_t operatorId, DriverCtx* ctx) {
        return std::make_unique<CustomJoinBuild>(operatorId, ctx, joinNode);
      };
    }
    return nullptr;
  }
};

/// This test will show the CustomJoinBuild passing count of input rows to
/// CustomJoinProbe via CustomJoinBridge, then probe will emit corresponding
/// number of rows, like a Limit operator.
class CustomJoinTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    Operator::registerOperator(std::make_unique<CustomJoinBridgeTranslator>());
  }

  RowVectorPtr makeSimpleRowVector(vector_size_t size) {
    return makeRowVector(
        {makeFlatVector<int32_t>(size, [](auto row) { return row; })});
  }

  void testCustomJoin(
      int32_t numThreads,
      const std::vector<RowVectorPtr>& leftBatch,
      const std::vector<RowVectorPtr>& rightBatch,
      const std::string& referenceQuery) {
    createDuckDbTable("t", {leftBatch});

    auto planNodeIdGenerator = std::make_shared<PlanNodeIdGenerator>();
    auto leftNode =
        PlanBuilder(planNodeIdGenerator).values({leftBatch}, true).planNode();
    auto rightNode =
        PlanBuilder(planNodeIdGenerator).values({rightBatch}, true).planNode();

    CursorParameters params;
    params.maxDrivers = numThreads;
    params.planNode =
        PlanBuilder(planNodeIdGenerator)
            .values({leftBatch}, true)
            .addNode([&leftNode, &rightNode](
                         std::string id, core::PlanNodePtr /* input */) {
              return std::make_shared<CustomJoinNode>(
                  id, std::move(leftNode), std::move(rightNode));
            })
            .project({"c0"})
            .planNode();

    OperatorTestBase::assertQuery(params, referenceQuery);
  }
};

TEST_F(CustomJoinTest, basic) {
  auto leftBatch = {makeSimpleRowVector(100)};
  auto rightBatch = {makeSimpleRowVector(10)};
  testCustomJoin(1, leftBatch, rightBatch, "SELECT c0 FROM t LIMIT 10");
}

TEST_F(CustomJoinTest, emptyBuild) {
  auto leftBatch = {makeSimpleRowVector(100)};
  auto rightBatch = {makeSimpleRowVector(0)};
  testCustomJoin(1, leftBatch, rightBatch, "SELECT c0 FROM t LIMIT 0");
}

TEST_F(CustomJoinTest, parallelism) {
  auto leftBatch = {
      makeSimpleRowVector(30),
      makeSimpleRowVector(40),
      makeSimpleRowVector(50)};
  auto rightBatch = {
      makeSimpleRowVector(5), makeSimpleRowVector(10), makeSimpleRowVector(15)};

  testCustomJoin(
      3,
      leftBatch,
      rightBatch,
      "(SELECT c0 FROM t LIMIT 90) "
      "UNION ALL (SELECT c0 FROM t LIMIT 90) "
      "UNION ALL (SELECT c0 FROM t LIMIT 90)");
}
