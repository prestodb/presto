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

#include "velox/dwio/common/ResultOrActions.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <list>

using namespace ::testing;
using namespace ::facebook::velox::dwio::common;

namespace {

using ReaderResult = ResultOrActions<uint64_t>;
using SplitResult = ResultOrActions<uint64_t>;
using VoidResult = ResultOrActions<folly::Unit>;

using ActionsMock = std::vector<int>;

auto getAction(ActionsMock& executedActions) {
  executedActions.push_back(0);
  return [&executedActions, i = executedActions.size() - 1]() {
    executedActions.at(i)++;
  };
}

std::vector<std::function<void()>> getActions(
    ActionsMock& executedActions,
    int numActions) {
  std::vector<std::function<void()>> actions;
  actions.reserve(numActions);
  for (int i = 0; i < numActions; i++) {
    actions.push_back(getAction(executedActions));
  }
  return actions;
}

template <typename T>
class ResultOrActionsTypedTest : public testing::Test {};

} // namespace

TEST(ResultOrActionsTest, HasResult) {
  ReaderResult readerResult(10);
  ASSERT_TRUE(readerResult.hasResult());
  EXPECT_EQ(readerResult.result(), 10);
  EXPECT_THAT(
      [&]() { readerResult.actions(); },
      Throws<facebook::velox::VeloxRuntimeError>(Property(
          &facebook::velox::VeloxRuntimeError::message,
          HasSubstr("Can't get actions of class that has a result"))));
}

TEST(ResultOrActionsTest, Void) {
  {
    VoidResult readerResult;
    EXPECT_EQ(readerResult.actions().size(), 0);
  }
  {
    VoidResult readerResult;
    EXPECT_EQ(readerResult.actions().size(), 0);
  }
  ActionsMock executedActions;
  {
    VoidResult readerResult(getAction(executedActions));
    EXPECT_EQ(readerResult.actions().size(), 1);
  }
  {
    VoidResult readerResult(getActions(executedActions, 2));
    EXPECT_EQ(readerResult.actions().size(), 2);
  }
}

TEST(ResultOrActionsTest, MoveBackToResult) {
  SplitResult splitResult(1);
  ActionsMock executedActions;

  ReaderResult readerResult(getAction(executedActions));

  EXPECT_THAT(
      [&]() { splitResult.moveActionsBack(std::move(readerResult.actions())); },
      Throws<facebook::velox::VeloxRuntimeError>(Property(
          &facebook::velox::VeloxRuntimeError::message,
          HasSubstr("Can't move actions to an object that has a result"))));
}

TEST(ResultOrActionsTest, MoveFrontToResult) {
  SplitResult splitResult(1);
  ActionsMock executedActions;

  ReaderResult readerResult(getAction(executedActions));

  EXPECT_THAT(
      [&]() {
        splitResult.moveActionsFront(std::move(readerResult.actions()));
      },
      Throws<facebook::velox::VeloxRuntimeError>(Property(
          &facebook::velox::VeloxRuntimeError::message,
          HasSubstr("Can't move actions to an object that has a result"))));
}

TEST(ResultOrActionsTest, MoveBackFromResult) {
  SplitResult splitResult(1);
  ReaderResult readerResult(1);

  EXPECT_THAT(
      [&]() { splitResult.moveActionsBack(std::move(readerResult.actions())); },
      Throws<facebook::velox::VeloxRuntimeError>(Property(
          &facebook::velox::VeloxRuntimeError::message,
          HasSubstr("Can't get actions of class that has a result"))));
}

TEST(ResultOrActionsTest, MoveFrontFromResult) {
  SplitResult splitResult(1);
  ReaderResult readerResult(1);

  EXPECT_THAT(
      [&]() {
        splitResult.moveActionsFront(std::move(readerResult.actions()));
      },
      Throws<facebook::velox::VeloxRuntimeError>(Property(
          &facebook::velox::VeloxRuntimeError::message,
          HasSubstr("Can't get actions of class that has a result"))));
}

using ReaderResultTypes = ::testing::Types<ReaderResult, VoidResult>;
TYPED_TEST_SUITE(ResultOrActionsTypedTest, ReaderResultTypes);

TYPED_TEST(ResultOrActionsTypedTest, NoResult) {
  ReaderResult readerResult;
  if constexpr (!std::is_same_v<TypeParam, VoidResult>) {
    ASSERT_FALSE(readerResult.hasResult());
    EXPECT_THAT(
        [&]() { readerResult.result(); },
        Throws<facebook::velox::VeloxRuntimeError>(Property(
            &facebook::velox::VeloxRuntimeError::message,
            HasSubstr("Result is not set"))));
  }
  EXPECT_EQ(readerResult.actions().size(), 0);
  EXPECT_EQ(readerResult.runAllActions(), 0);
}

TYPED_TEST(ResultOrActionsTypedTest, ActionNeeded) {
  ActionsMock executedActions;
  ReaderResult readerResult(getAction(executedActions));
  if constexpr (!std::is_same_v<TypeParam, VoidResult>) {
    ASSERT_FALSE(readerResult.hasResult());
    EXPECT_THAT(
        [&]() { readerResult.result(); },
        Throws<facebook::velox::VeloxRuntimeError>(Property(
            &facebook::velox::VeloxRuntimeError::message,
            HasSubstr("Result is not set"))));
  }
  EXPECT_EQ(readerResult.actions().size(), 1);
  EXPECT_EQ(executedActions, ActionsMock({0}));
  readerResult.actions()[0]();
  EXPECT_EQ(executedActions, ActionsMock({1}));
  EXPECT_EQ(readerResult.runAllActions(), 1);
  EXPECT_EQ(executedActions, ActionsMock({2}));
}

TYPED_TEST(ResultOrActionsTypedTest, ActionsNeeded) {
  ActionsMock executedActions;
  ReaderResult readerResult(getActions(executedActions, 2));
  if constexpr (!std::is_same_v<TypeParam, VoidResult>) {
    ASSERT_FALSE(readerResult.hasResult());
    EXPECT_THAT(
        [&]() { readerResult.result(); },
        Throws<facebook::velox::VeloxRuntimeError>(Property(
            &facebook::velox::VeloxRuntimeError::message,
            HasSubstr("Result is not set"))));
  }
  EXPECT_EQ(readerResult.actions().size(), 2);
  EXPECT_EQ(executedActions, ActionsMock({0, 0}));
  readerResult.actions()[0]();
  EXPECT_EQ(executedActions, ActionsMock({1, 0}));
  readerResult.actions()[1]();
  EXPECT_EQ(executedActions, ActionsMock({1, 1}));
  EXPECT_EQ(readerResult.runAllActions(), 2);
  EXPECT_EQ(executedActions, ActionsMock({2, 2}));
}

TYPED_TEST(ResultOrActionsTypedTest, MoveActionsBackIncremental) {
  SplitResult splitResult;
  ActionsMock executedActions;
  {
    ReaderResult readerResult(getAction(executedActions));
    splitResult.moveActionsBack(std::move(readerResult.actions()));
  }
  ASSERT_EQ(splitResult.actions().size(), 1);
  EXPECT_EQ(executedActions, ActionsMock({0}));
  splitResult.actions()[0]();
  EXPECT_EQ(executedActions, ActionsMock({1}));
  {
    ReaderResult readerResult(getAction(executedActions));
    splitResult.moveActionsBack(std::move(readerResult.actions()));
  }
  ASSERT_EQ(splitResult.actions().size(), 2);
  EXPECT_EQ(executedActions, ActionsMock({1, 0}));
  splitResult.actions()[0]();
  EXPECT_EQ(executedActions, ActionsMock({2, 0}));
  splitResult.actions()[1]();
  EXPECT_EQ(executedActions, ActionsMock({2, 1}));
  {
    ReaderResult readerResult(getActions(executedActions, 2));
    splitResult.moveActionsBack(std::move(readerResult.actions()));
  }
  ASSERT_EQ(splitResult.actions().size(), 4);
  EXPECT_EQ(executedActions, ActionsMock({2, 1, 0, 0}));
  splitResult.actions()[2]();
  EXPECT_EQ(executedActions, ActionsMock({2, 1, 1, 0}));
  splitResult.actions()[3]();
  EXPECT_EQ(executedActions, ActionsMock({2, 1, 1, 1}));
  EXPECT_EQ(splitResult.runAllActions(), 4);
  EXPECT_EQ(executedActions, ActionsMock({3, 2, 2, 2}));
}

TYPED_TEST(ResultOrActionsTypedTest, MoveActionsFrontIncremental) {
  SplitResult splitResult;
  ActionsMock executedActions;
  {
    ReaderResult readerResult(getAction(executedActions));
    splitResult.moveActionsFront(std::move(readerResult.actions()));
  }
  ASSERT_EQ(splitResult.actions().size(), 1);
  EXPECT_EQ(executedActions, ActionsMock({0}));
  splitResult.actions()[0]();
  EXPECT_EQ(executedActions, ActionsMock({1}));
  {
    ReaderResult readerResult(getAction(executedActions));
    splitResult.moveActionsFront(std::move(readerResult.actions()));
  }
  ASSERT_EQ(splitResult.actions().size(), 2);
  EXPECT_EQ(executedActions, ActionsMock({1, 0}));
  splitResult.actions()[0]();
  EXPECT_EQ(executedActions, ActionsMock({1, 1}));
  splitResult.actions()[1]();
  EXPECT_EQ(executedActions, ActionsMock({2, 1}));
  {
    ReaderResult readerResult(getActions(executedActions, 2));
    splitResult.moveActionsFront(std::move(readerResult.actions()));
  }
  ASSERT_EQ(splitResult.actions().size(), 4);
  EXPECT_EQ(executedActions, ActionsMock({2, 1, 0, 0}));
  splitResult.actions()[0]();
  EXPECT_EQ(executedActions, ActionsMock({2, 1, 1, 0}));
  splitResult.actions()[1]();
  EXPECT_EQ(executedActions, ActionsMock({2, 1, 1, 1}));
  // Actions 2 and 3 were already rotated in the second moveActionsFront, so
  // they don't map to indices 0 and 1, but to 1 and 0.
  splitResult.actions()[2]();
  EXPECT_EQ(executedActions, ActionsMock({2, 2, 1, 1}));
  splitResult.actions()[3]();
  EXPECT_EQ(executedActions, ActionsMock({3, 2, 1, 1}));
  EXPECT_EQ(splitResult.runAllActions(), 4);
  EXPECT_EQ(executedActions, ActionsMock({4, 3, 2, 2}));
}

TYPED_TEST(ResultOrActionsTypedTest, MergeActions) {
  ActionsMock executedActions;
  ReaderResult readerResult(getActions(executedActions, 2));
  EXPECT_EQ(readerResult.actions().size(), 2);
  EXPECT_EQ(executedActions, ActionsMock({0, 0}));
  readerResult.actions()[0]();
  EXPECT_EQ(executedActions, ActionsMock({1, 0}));
  readerResult.actions()[1]();
  EXPECT_EQ(executedActions, ActionsMock({1, 1}));
  readerResult.mergeActions();
  EXPECT_EQ(readerResult.actions().size(), 1);
  readerResult.actions()[0]();
  EXPECT_EQ(executedActions, ActionsMock({2, 2}));
  EXPECT_EQ(readerResult.runAllActions(), 1);
  EXPECT_EQ(executedActions, ActionsMock({3, 3}));
}

TYPED_TEST(ResultOrActionsTypedTest, MoveConstructor) {
  ActionsMock executedActions;
  ReaderResult readerResult(getActions(executedActions, 2));
  SplitResult splitResult(std::move(readerResult));

  EXPECT_EQ(splitResult.actions().size(), 2);
  EXPECT_EQ(executedActions, ActionsMock({0, 0}));
  splitResult.actions()[0]();
  EXPECT_EQ(executedActions, ActionsMock({1, 0}));
  splitResult.actions()[1]();
  EXPECT_EQ(executedActions, ActionsMock({1, 1}));
  EXPECT_EQ(splitResult.runAllActions(), 2);
  EXPECT_EQ(executedActions, ActionsMock({2, 2}));
}

TYPED_TEST(ResultOrActionsTypedTest, CopyAssignment) {
  ActionsMock executedActions;
  ReaderResult readerResult(getActions(executedActions, 2));
  SplitResult splitResult;
  splitResult = std::move(readerResult);

  EXPECT_EQ(splitResult.actions().size(), 2);
  EXPECT_EQ(executedActions, ActionsMock({0, 0}));
  splitResult.actions()[0]();
  EXPECT_EQ(executedActions, ActionsMock({1, 0}));
  splitResult.actions()[1]();
  EXPECT_EQ(executedActions, ActionsMock({1, 1}));
  EXPECT_EQ(splitResult.runAllActions(), 2);
  EXPECT_EQ(executedActions, ActionsMock({2, 2}));
}
