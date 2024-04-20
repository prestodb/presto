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

#include <gtest/gtest.h>

#include "velox/dwio/common/UnitLoaderTools.h"

using namespace ::testing;
using namespace ::facebook::velox::dwio::common;
using namespace ::facebook::velox::dwio::common::unit_loader_tools;

TEST(UnitLoaderToolsTests, NoCallbacksCreated) {
  std::atomic_size_t callCount = 0;
  {
    CallbackOnLastSignal callback([&callCount]() { ++callCount; });
    EXPECT_EQ(callCount, 0);
  }
  EXPECT_EQ(callCount, 1);
}

TEST(UnitLoaderToolsTests, SupportsNullCallbacks) {
  CallbackOnLastSignal callback(nullptr);
  auto cb = callback.getCallback();
  EXPECT_TRUE(cb == nullptr);
}

TEST(UnitLoaderToolsTests, NoExplicitCalls) {
  std::atomic_size_t callCount = 0;
  {
    CallbackOnLastSignal callback([&callCount]() { ++callCount; });
    EXPECT_EQ(callCount, 0);
    {
      auto c1 = callback.getCallback();
      auto c4 = callback.getCallback();
      EXPECT_EQ(callCount, 0);

      auto c2 = std::move(c1);
      auto c3(c2);
      EXPECT_EQ(callCount, 0);

      auto c5 = std::move(c4);
      auto c6(c5);
      EXPECT_EQ(callCount, 0);
    }
    EXPECT_EQ(callCount, 1);
  }
  EXPECT_EQ(callCount, 1);
}

TEST(UnitLoaderToolsTests, NoExplicitCallsFactoryDeletedFirst) {
  std::atomic_size_t callCount = 0;
  {
    std::function<void()> c1, c2;
    {
      CallbackOnLastSignal callback([&callCount]() { ++callCount; });
      EXPECT_EQ(callCount, 0);

      c1 = callback.getCallback();
      c2 = callback.getCallback();
      EXPECT_EQ(callCount, 0);
    }
    EXPECT_EQ(callCount, 0);
  }
  EXPECT_EQ(callCount, 1);
}

TEST(UnitLoaderToolsTests, ExplicitCalls) {
  std::atomic_size_t callCount = 0;
  {
    CallbackOnLastSignal callback([&callCount]() { ++callCount; });
    EXPECT_EQ(callCount, 0);
    {
      auto c1 = callback.getCallback();
      auto c4 = callback.getCallback();
      EXPECT_EQ(callCount, 0);

      c1();
      auto c2 = std::move(c1);
      c2();
      auto c3(c2);
      c3();
      EXPECT_EQ(callCount, 0);

      c4();
      EXPECT_EQ(callCount, 1);
      auto c5 = std::move(c4);
      c5();
      auto c6(c2);
      c6();
      EXPECT_EQ(callCount, 1);
    }
    EXPECT_EQ(callCount, 1);
  }
  EXPECT_EQ(callCount, 1);
}

TEST(UnitLoaderToolsTests, WillOnlyCallbackOnce) {
  std::atomic_size_t callCount = 0;
  {
    CallbackOnLastSignal callback([&callCount]() { ++callCount; });
    EXPECT_EQ(callCount, 0);
    {
      auto c1 = callback.getCallback();
      auto c4 = callback.getCallback();
      EXPECT_EQ(callCount, 0);

      c1();
      auto c2 = std::move(c1);
      c2();
      auto c3(c2);
      c3();
      EXPECT_EQ(callCount, 0);

      c4();
      EXPECT_EQ(callCount, 1);
      auto c5 = std::move(c4);
      c5();
      auto c6(c2);
      c6();
      EXPECT_EQ(callCount, 1);

      // This won't emit a new call
      auto c7 = callback.getCallback();
      c7();
      EXPECT_EQ(callCount, 1);
    }
    EXPECT_EQ(callCount, 1);
  }
  EXPECT_EQ(callCount, 1);
}
