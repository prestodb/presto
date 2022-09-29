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
#include "velox/exec/SpillOperatorGroup.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

class SpillOperatorGroupTest : public testing::Test {
 protected:
  SpillOperatorGroupTest(int32_t numOperators = 1)
      : numOperators_(numOperators) {}

  void SetUp() override {
    rng_.seed(1245);
    taskId_ = "SpillTask";
    operatorId_ = 0;
    splitGroupId_ = 0;
    planNodeId_ = "SpillNode";
  }

  void TearDown() override {}

  class MockOperator : public Operator {
   public:
    explicit MockOperator(
        int32_t operatorId,
        SpillOperatorGroup* FOLLY_NONNULL spillGroup)
        : Operator(operatorId, 0, "SpillNode", "SpillOperator"),
          spillGroup_(spillGroup),
          spillFuture_(ContinueFuture::makeEmpty()) {
      spillGroup_->addOperator(*this, [&](const std::vector<Operator*>& ops) {
        std::unordered_set<uint32_t> opIds;
        for (const auto& op : ops) {
          opIds.insert(op->stats().operatorId);
        }
        ASSERT_EQ(opIds.size(), ops.size());
        for (auto& op : ops) {
          MockOperator* spillOp = dynamic_cast<MockOperator*>(op);
          ++spillOp->numSpillRuns_;
        }
      });
    }

    bool needsInput() const override {
      return false;
    }

    void addInput(RowVectorPtr input) override {}

    void noMoreInput() override {
      Operator::noMoreInput();
    }

    RowVectorPtr getOutput() override {
      return nullptr;
    }

    BlockingReason isBlocked(ContinueFuture* future) override {
      return BlockingReason::kNotBlocked;
    }

    bool isFinished() override {
      return false;
    }

    /// Methods used for spill test.
    bool requestSpill() {
      return spillGroup_->requestSpill(*this, spillFuture_);
    }

    bool waitSpill() {
      return spillGroup_->waitSpill(*this, spillFuture_);
    }

    void stopSpill() {
      spillStopped_ = true;
      spillGroup_->operatorStopped(*this);
    }

    bool spillStopped() const {
      return spillStopped_;
    }

    void restartSpill() {
      spillStopped_ = false;
    }

    int32_t numSpillRuns() const {
      return numSpillRuns_;
    }

    ContinueFuture& spillFuture() {
      return spillFuture_;
    }

   private:
    SpillOperatorGroup* const spillGroup_; // Not owned.

    bool spillStopped_{false};
    ContinueFuture spillFuture_;
    int32_t numSpillRuns_{0};
  };

  std::unique_ptr<MockOperator> newSpillOperator(
      SpillOperatorGroup* FOLLY_NONNULL spillGroup) {
    return std::make_unique<MockOperator>(operatorId_++, spillGroup);
  }

  void setupSpillOperators() {
    std::vector<std::unique_ptr<SpillOperatorGroupTest::MockOperator>> ops;
    ops.reserve(numOperators_);
    for (int32_t i = 0; i < numOperators_; ++i) {
      ops.push_back(newSpillOperator(spillGroup_.get()));
    }
    spillOps_ = std::move(ops);
    ASSERT_EQ(spillGroup_->state(), SpillOperatorGroup::State::kInit);
  }

  void setupSpillGroup() {
    spillGroup_ = std::make_unique<SpillOperatorGroup>(
        taskId_, splitGroupId_++, planNodeId_);
    ASSERT_EQ(spillGroup_->state(), SpillOperatorGroup::State::kInit);
    numSpillRuns_ = 0;

    setupSpillOperators();
  }

  void runSpillOperators(
      bool triggerSpill = true,
      bool operatorStopped = false) {
    SCOPED_TRACE(fmt::format(
        "triggerSpill:{}, operatorStopped:{}", triggerSpill, operatorStopped));
    ASSERT_TRUE(hasRunningOperators());

    auto numRunningOpsLeft = numRunningOperators();
    bool spillTriggered = false;
    for (int32_t i = 0; i < spillOps_.size(); ++i) {
      auto* op = spillOps_[i].get();
      if (op->spillStopped()) {
        continue;
      }

      --numRunningOpsLeft;
      bool wait;
      if (operatorStopped && oneIn(2)) {
        op->stopSpill();
        continue;
      }
      if (triggerSpill && (!spillTriggered || oneIn(3))) {
        spillTriggered = true;
        wait = op->requestSpill();
      } else {
        wait = op->waitSpill();
      }
      if (spillTriggered) {
        if (numRunningOpsLeft != 0) {
          ASSERT_TRUE(spillGroup_->needSpill());
          ASSERT_TRUE(wait);
          ASSERT_TRUE(op->spillFuture().valid());
        } else {
          ASSERT_FALSE(spillGroup_->needSpill());
          ASSERT_FALSE(wait);
          ASSERT_FALSE(op->spillFuture().valid());
        }
      } else {
        ASSERT_FALSE(spillGroup_->needSpill());
        ASSERT_FALSE(wait);
        ASSERT_FALSE(op->spillFuture().valid());
      }
    }

    ASSERT_FALSE(spillGroup_->needSpill());
    if (hasRunningOperators()) {
      ASSERT_EQ(spillGroup_->state(), SpillOperatorGroup::State::kRunning);
    } else {
      ASSERT_EQ(spillGroup_->state(), SpillOperatorGroup::State::kStopped);
    }
    if (spillTriggered) {
      ++numSpillRuns_;
      waitSpillOperators();
    }
    checkSpillOperators();
  }

  void checkSpillOperators() {
    for (auto& op : spillOps_) {
      ASSERT_EQ(op->numSpillRuns(), numSpillRuns_);
    }
  }

  void waitSpillOperators() {
    int32_t numNonWaitOperators = 0;
    int32_t numStoppedOperators = 0;
    for (auto& op : spillOps_) {
      if (op->spillStopped()) {
        ++numStoppedOperators;
        ASSERT_FALSE(op->spillFuture().valid());
        continue;
      }
      auto future = std::move(op->spillFuture());
      if (future.valid()) {
        future.wait();
      } else {
        ++numNonWaitOperators;
      }
    }
    // NOTE: at most one spill operator doesn't need to wait.
    if (numNonWaitOperators == 0) {
      ASSERT_GT(numStoppedOperators, 0);
    } else {
      ASSERT_EQ(numNonWaitOperators, 1);
    }
  }

  bool hasRunningOperators() const {
    return numRunningOperators() != 0;
  }

  int32_t numRunningOperators() const {
    int32_t numRunningOps = 0;
    for (auto& op : spillOps_) {
      if (!op->spillStopped()) {
        ++numRunningOps;
      }
    }
    return numRunningOps;
  }

  void startSpillGroup() {
    if (spillGroup_->state() != SpillOperatorGroup::State::kStopped) {
      spillGroup_->start();
      return;
    }
    for (auto& op : spillOps_) {
      ASSERT_TRUE(op->spillStopped());
      op->restartSpill();
    }
    spillGroup_->restart();
    ASSERT_EQ(spillGroup_->state(), SpillOperatorGroup::State::kRunning);
  }

  uint32_t randInt(uint32_t n) {
    std::lock_guard<std::mutex> l(mutex_);
    return folly::Random().rand64(rng_) % (n + 1);
  }

  bool oneIn(uint32_t n) {
    std::lock_guard<std::mutex> l(mutex_);
    return folly::Random().oneIn(n, rng_);
  }

  const int32_t numOperators_;

  std::mutex mutex_;
  folly::Random::DefaultGenerator rng_;

  uint32_t operatorId_;
  std::string taskId_;
  uint32_t splitGroupId_;
  core::PlanNodeId planNodeId_;
  int32_t numSpillRuns_{0};
  std::unique_ptr<SpillOperatorGroup> spillGroup_;
  std::vector<std::unique_ptr<MockOperator>> spillOps_;
};

class MultiSpillOperatorGroupTest
    : public SpillOperatorGroupTest,
      public testing::WithParamInterface<int32_t> {
 public:
  MultiSpillOperatorGroupTest() : SpillOperatorGroupTest(GetParam()) {}
};

TEST_P(MultiSpillOperatorGroupTest, spillRun) {
  setupSpillGroup();

  int32_t numRestarts = 0;
  while (numRestarts++ < 3) {
    SCOPED_TRACE(fmt::format("numRestarts: {}", numRestarts - 1));
    startSpillGroup();

    // The initial spill runs with all the operators involved.
    for (int32_t run = 0; run < 4; ++run) {
      ASSERT_FALSE(spillGroup_->needSpill());

      // No spill triggered.
      runSpillOperators(false);

      // Spill triggered.
      runSpillOperators(true);
    }

    // Continue run spills until all the operators have stopped.
    while (hasRunningOperators()) {
      runSpillOperators(true, true);
    }
  }
}

TEST_P(MultiSpillOperatorGroupTest, noSpillRun) {
  setupSpillGroup();
  startSpillGroup();
  // Run test without triggering spill.
  while (hasRunningOperators()) {
    runSpillOperators(false, true);
  }
}

TEST_P(MultiSpillOperatorGroupTest, error) {
  setupSpillGroup();
  startSpillGroup();

  // Can't start spill group twice.
  ASSERT_ANY_THROW(startSpillGroup());

  // Can't add spill operator after the group started.
  ASSERT_ANY_THROW(setupSpillOperators());

  // Can't restart if the group is not stopped.
  ASSERT_ANY_THROW(spillGroup_->restart());

  for (auto& op : spillOps_) {
    op->stopSpill();
    // Can only stop an operator once.
    ASSERT_ANY_THROW(op->stopSpill());
  }

  // Can't add op after the group has stopped.
  ASSERT_ANY_THROW(newSpillOperator(spillGroup_.get()));

  // Can't request spill or wait after the group has stopped.
  for (auto& op : spillOps_) {
    ASSERT_ANY_THROW(op->requestSpill());
  }
  for (auto& op : spillOps_) {
    ASSERT_ANY_THROW(op->waitSpill());
  }

  // Restart the spill group and everything works fine.
  startSpillGroup();
  for (auto& op : spillOps_) {
    op->waitSpill();
  }
  for (auto& op : spillOps_) {
    op->requestSpill();
  }
}

TEST_P(MultiSpillOperatorGroupTest, multiThreading) {
  setupSpillGroup();
  startSpillGroup();

  std::vector<std::thread> opThreads;
  opThreads.reserve(numOperators_);

  struct BarrierState {
    int32_t numRequested{0};
    std::vector<ContinuePromise> promises;
  };
  const int32_t numIterations = 100;
  std::mutex barrierLock;
  std::vector<BarrierState> barriers(numIterations);

  // Start one thread per each spill operator and then stop and sync at the end
  // of each test iteration by BarrierState. Then restart the spill group to run
  // the next test iteration.
  for (size_t i = 0; i < numOperators_; ++i) {
    auto* op = spillOps_[i].get();
    opThreads.emplace_back([&, op]() {
      for (int32_t iter = 0; iter < numIterations; ++iter) {
        const auto maxNumActions = 1 + randInt(6);
        int32_t action = 0;
        while (++action < maxNumActions) {
          if (oneIn(6)) {
            op->stopSpill();
            break;
          }
          bool wait;
          if (oneIn(3)) {
            wait = op->requestSpill();
          } else {
            wait = op->waitSpill();
          }
          auto future = std::move(op->spillFuture());
          if (wait) {
            ASSERT_TRUE(future.valid());
            future.wait();
          } else {
            ASSERT_FALSE(future.valid());
          }
        }
        if (!op->spillStopped()) {
          op->stopSpill();
        }

        // Wait for peers.
        std::vector<ContinuePromise> promises;
        ContinueFuture future(ContinueFuture::makeEmpty());
        {
          std::lock_guard<std::mutex> l(barrierLock);
          auto& barrier = barriers[iter];
          if (++barrier.numRequested < numOperators_) {
            barrier.promises.emplace_back(
                "SpillOperatorGroupTest::multiThreading");
            future = barrier.promises.back().getSemiFuture();
          } else {
            promises = std::move(barrier.promises);
          }
        }
        if (future.valid()) {
          future.wait();
        } else {
          startSpillGroup();

          for (auto& promise : promises) {
            promise.setValue();
          }
        }
      }
    });
  }

  for (auto& th : opThreads) {
    th.join();
  }
  numSpillRuns_ = spillOps_[0]->numSpillRuns();
  checkSpillOperators();
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    SpillOperatorGroupTest,
    MultiSpillOperatorGroupTest,
    testing::ValuesIn({1, 4, 32}));
