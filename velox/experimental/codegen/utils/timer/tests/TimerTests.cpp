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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <chrono>
#include "velox/experimental/codegen/utils/timer/NestedScopedTimer.h"

namespace facebook::velox::codegen {
using namespace std::chrono_literals;

class TimerTest : public testing::Test {};

/// Testing clock with constant increments of 10 ns
/// This make testing more predictable.
struct TestClock {
  static auto now() {
    static size_t count = 0; // Assuming single threading here
    count++;
    return std::chrono::steady_clock::time_point::min() + count * 10ns;
  }
  using time_point = std::chrono::steady_clock::time_point;
  using duration = std::chrono::steady_clock::duration;
};

using TestEventSequence = NamedEventSequence<TestClock>;
using TestTimerTree = TimerTree<TestClock>;
using TestNamedNestedScopedTimer = NamedNestedScopedTimer<TestClock>;

TEST_F(TimerTest, improperNesting1) {
  const char* name1 = "Counter A";
  TestEventSequence eventSequence;
  eventSequence.addEndEvent(name1);
  EXPECT_THROW(
      TestTimerTree::fromEventSequence(eventSequence), std::runtime_error);
}

TEST_F(TimerTest, improperNesting2) {
  const char* name1 = "Counter A";
  TestEventSequence eventSequence;
  eventSequence.addStartEvent(name1);
  EXPECT_THROW(
      TestTimerTree::fromEventSequence(eventSequence), std::runtime_error);
}

TEST_F(TimerTest, EventSequence) {
  TestEventSequence eventSequence;
  {
    TestNamedNestedScopedTimer timer1("A", eventSequence);
    TestNamedNestedScopedTimer timer2("B", eventSequence);
    TestNamedNestedScopedTimer timer3("C", eventSequence);
    TestNamedNestedScopedTimer timer4("D", eventSequence);
  }
  ASSERT_EQ(eventSequence.startEvents.size(), 4);
  ASSERT_EQ(eventSequence.endEvents.size(), 4);

  ASSERT_EQ(eventSequence.startEvents[0].info, "A");
  ASSERT_EQ(eventSequence.startEvents[1].info, "B");
  ASSERT_EQ(eventSequence.startEvents[2].info, "C");
  ASSERT_EQ(eventSequence.startEvents[3].info, "D");

  ASSERT_EQ(eventSequence.endEvents[0].info, "D");
  ASSERT_EQ(eventSequence.endEvents[1].info, "C");
  ASSERT_EQ(eventSequence.endEvents[2].info, "B");
  ASSERT_EQ(eventSequence.endEvents[3].info, "A");
}

TEST_F(TimerTest, nestedTimerCount) {
  TestEventSequence eventSequence;

  {
    TestNamedNestedScopedTimer timer1("main", eventSequence);
    for (size_t i = 0; i < 10; ++i) {
      TestNamedNestedScopedTimer timer5("loop", eventSequence);

      if (i % 3 == 0) {
        TestNamedNestedScopedTimer timer2("if", eventSequence);
      } else {
        TestNamedNestedScopedTimer timer3("else1", eventSequence);
        TestNamedNestedScopedTimer timer4("else2", eventSequence);
      }
    }
  }
  auto timerTree = TestTimerTree::fromEventSequence(eventSequence);
  ASSERT_EQ(timerTree->children["main"]->timePoints.size(), 2);
  ASSERT_EQ(
      timerTree->children["main"]->children["loop"]->timePoints.size(), 20);
  ASSERT_EQ(
      timerTree->children["main"]
          ->children["loop"]
          ->children["else1"]
          ->children["else2"]
          ->timePoints.size(),
      timerTree->children["main"]
          ->children["loop"]
          ->children["else1"]
          ->timePoints.size());
  ASSERT_EQ(
      timerTree->children["main"]
              ->children["loop"]
              ->children["if"]
              ->timePoints.size() +
          timerTree->children["main"]
              ->children["loop"]
              ->children["else1"]
              ->timePoints.size(),
      timerTree->children["main"]->children["loop"]->timePoints.size());
}

TEST_F(TimerTest, nestedTimerPrint) {
  TestEventSequence eventSequence;

  {
    TestNamedNestedScopedTimer timer1("main", eventSequence);
    for (size_t i = 0; i < 10; ++i) {
      TestNamedNestedScopedTimer timer5("loop", eventSequence);

      if (i % 2 == 0) {
        TestNamedNestedScopedTimer timer2("even", eventSequence);
      } else {
        TestNamedNestedScopedTimer timer3("odd1", eventSequence);
        TestNamedNestedScopedTimer timer4("odd2", eventSequence);
      }
    }
  }
  auto timerTree = TestTimerTree::fromEventSequence(eventSequence);
  TestTimerTree::computeStatistics(*timerTree);
  const std::string result =
      R"counter([<ROOT>] : count = 1, min = 0, max = 0, avg = 0 , total (ns) = 0 , % parent = 0.00 , % total = 0.00
+[main] : count = 1, min = 510, max = 510, avg = 510 , total (ns) = 510 , % parent = 100.00 , % total = 100.00
++[loop] : count = 10, min = 30, max = 50, avg = 40 , total (ns) = 400 , % parent = 78.43 , % total = 78.43
+++[even] : count = 5, min = 10, max = 10, avg = 10 , total (ns) = 50 , % parent = 12.50 , % total = 9.80
+++[odd1] : count = 5, min = 30, max = 30, avg = 30 , total (ns) = 150 , % parent = 37.50 , % total = 29.41
++++[odd2] : count = 5, min = 10, max = 10, avg = 10 , total (ns) = 50 , % parent = 33.33 , % total = 9.80
)counter";
  std::stringstream output;
  timerTree->print(output, "", "+");
  ASSERT_EQ(result, output.str());
}

} // namespace facebook::velox::codegen
