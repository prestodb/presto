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

namespace {
using ReplacedJoinNode = typename facebook::velox::core::HashJoinNode;

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
      std::shared_ptr<const ReplacedJoinNode> joinNode)
      : Operator(
            driverCtx,
            nullptr,
            operatorId,
            joinNode->id(),
            "CustomJoinBuild") {}
  CustomJoinBuild(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const core::PlanNodeId& joinNodeid)
      : Operator(
            driverCtx,
            nullptr,
            operatorId,
            joinNodeid,
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

    // checks
    VELOX_CHECK_NOT_NULL(
        customJoinBridge,
        "Join bridge for plan node ID is of the wrong type: {}",
        planNodeId());
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
      std::shared_ptr<const ReplacedJoinNode> joinNode)
      : Operator(
            driverCtx,
            nullptr,
            operatorId,
            joinNode->id(),
            "CustomJoinProbe") {}
  CustomJoinProbe(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const core::PlanNodeId& joinNodeid)
      : Operator(
            driverCtx,
            nullptr,
            operatorId,
            joinNodeid,
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
    auto customJoinBridge =
        std::dynamic_pointer_cast<CustomJoinBridge>(joinBridge);

    // checks
    VELOX_CHECK_NOT_NULL(
        customJoinBridge,
        "Join bridge for plan node ID is of the wrong type: {}",
        planNodeId());
    auto numRows = customJoinBridge->numRowsOrFuture(future);

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
    if (auto joinNode =
            std::dynamic_pointer_cast<const ReplacedJoinNode>(node)) {
      return std::make_unique<CustomJoinProbe>(id, ctx, joinNode);
    }
    return nullptr;
  }

  std::unique_ptr<JoinBridge> toJoinBridge(const core::PlanNodePtr& node) {
    if (auto joinNode =
            std::dynamic_pointer_cast<const ReplacedJoinNode>(node)) {
      auto joinBridge = std::make_unique<CustomJoinBridge>();
      return std::move(joinBridge);
    }
    return nullptr;
  }

  OperatorSupplier toOperatorSupplier(const core::PlanNodePtr& node) {
    if (auto joinNode =
            std::dynamic_pointer_cast<const ReplacedJoinNode>(node)) {
      return [joinNode](int32_t operatorId, DriverCtx* ctx) {
        return std::make_unique<CustomJoinBuild>(operatorId, ctx, joinNode);
      };
    }
    return nullptr;
  }
};

bool CustomDriverAdapter(
    const exec::DriverFactory& driverFactory_,
    exec::Driver& driver_) {
  auto operators = driver_.operators();
  // Make sure operator states are initialized.  We will need to inspect some of
  // them during the transformation.
  driver_.initializeOperators();
  auto ctx = driver_.driverCtx();
  // Replace HashBuild and HashProbe operators with CustomHashBuild and
  // CustomHashProbe operators.
  for (int32_t operatorIndex = 0; operatorIndex < operators.size();
       ++operatorIndex) {
    std::vector<std::unique_ptr<exec::Operator>> replace_op;

    facebook::velox::exec::Operator* oper = operators[operatorIndex];
    VELOX_CHECK(oper);
    if (auto joinBuildOp =
            dynamic_cast<facebook::velox::exec::HashBuild*>(oper)) {
      auto planid = joinBuildOp->planNodeId();
      auto id = joinBuildOp->operatorId();
      replace_op.push_back(std::make_unique<CustomJoinBuild>(id, ctx, planid));
      replace_op[0]->initialize();
      auto replaced = driverFactory_.replaceOperators(
          driver_, operatorIndex, operatorIndex + 1, std::move(replace_op));
    } else if (
        auto joinProbeOp =
            dynamic_cast<facebook::velox::exec::HashProbe*>(oper)) {
      auto planid = joinProbeOp->planNodeId();
      auto id = joinProbeOp->operatorId();
      replace_op.push_back(std::make_unique<CustomJoinProbe>(id, ctx, planid));
      replace_op[0]->initialize();
      auto replaced = driverFactory_.replaceOperators(
          driver_, operatorIndex, operatorIndex + 1, std::move(replace_op));
    }
  }
  return true;
}

void registerCustomDriver() {
  exec::DriverAdapter custAdapter{"opRep", {}, CustomDriverAdapter};
  exec::DriverFactory::registerAdapter(custAdapter);
}

void registerOperatorReplacement() {
  // Registering Translator
  exec::Operator::registerOperator(
      std::make_unique<CustomJoinBridgeTranslator>());
  // Registering Custom Driver Adapter
  registerCustomDriver();
}
} // namespace

/// This test will show the operator replacement with other operators using
/// driver adapter and usage of custom join bridge for replaced velox
/// join operators. It uses same custom operators from CustomJoinTest,
/// which will emit number of input rows.
class OperatorReplacementTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    registerOperatorReplacement();
  }

  RowVectorPtr makeSimpleRowVector(vector_size_t size) {
    return makeRowVector(
        {makeFlatVector<int32_t>(size, [](auto row) { return row; })});
  }

  void testOperatorReplacement(
      int32_t numThreads,
      const std::vector<RowVectorPtr>& leftBatch,
      const std::vector<RowVectorPtr>& rightBatch,
      const std::string& referenceQuery) {
    createDuckDbTable("t", {leftBatch});

    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    CursorParameters params;
    params.maxDrivers = numThreads;
    params.planNode = PlanBuilder(planNodeIdGenerator)
                          .values({leftBatch}, false)
                          .hashJoin(
                              {"c0"},
                              {"u1"},
                              PlanBuilder(planNodeIdGenerator)
                                  .values(rightBatch, false)
                                  .project({"c0 AS u1"})
                                  .planNode(),
                              "",
                              {"c0"})
                          .project({"c0"})
                          .planNode();

    OperatorTestBase::assertQuery(params, referenceQuery);
  }
};

TEST_F(OperatorReplacementTest, basic) {
  auto leftBatch = {makeSimpleRowVector(100)};
  auto rightBatch = {makeSimpleRowVector(10)};
  testOperatorReplacement(
      1, leftBatch, rightBatch, "SELECT c0 FROM t LIMIT 10");
}
