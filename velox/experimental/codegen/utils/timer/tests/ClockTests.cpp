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
#include <xsimd/xsimd.hpp> // @manual
#include <chrono>
#include "boost/accumulators/accumulators.hpp"
#include "boost/accumulators/statistics.hpp"
#include "velox/experimental/codegen/utils/timer/NestedScopedTimer.h"

namespace facebook::velox::codegen {

class ClockTest : public testing::Test {
 protected:
  const size_t iteration = 100;
};

using namespace boost::accumulators;

/// Compute basic statistics on a vector of time points
/// \tparam Clock
/// \param eventSequence
/// \return
template <typename Clock>
auto timePointStats(
    const std::vector<typename Clock::time_point>& timePoints,
    size_t size) {
  accumulator_set<
      double,
      stats<tag::min, tag::max, tag::mean, tag::lazy_variance>>
      accumulatorSet;
  for (size_t i = 0; i + 1 < size; i++) {
    auto durations = timePoints[i + 1] - timePoints[i];
    accumulatorSet(static_cast<double>(durations.count()));
  }
  return accumulatorSet;
}

using namespace boost::accumulators;
template <typename Clock, bool warmCache, typename Action>
void testClock(
    const std::string& message,
    const Action& action,
    size_t iteration) {
  std::vector<typename Clock::time_point> timePoints;
  timePoints.reserve(iteration);

  if constexpr (warmCache) {
    for (size_t i = 0; i < iteration; i++) {
      action(timePoints, i);
    };
    timePoints.clear();
  }

  for (size_t i = 0; i < iteration; i++) {
    action(timePoints, i);
  };

  auto stats = timePointStats<Clock>(timePoints, iteration);

  std::cout << fmt::format(
      "{} : min = {}, max = {}, mean = {}, variance = {}\n",
      message,
      min(stats),
      max(stats),
      mean(stats),
      variance(stats));
}
template <typename Clock>
void printClockStatistics(const std::string& clockName, size_t iterations) {
  testClock<Clock, true>(
      fmt::format("{} push_back with hot cache", clockName),
      [](auto& timePoints, size_t) -> void {
        timePoints.push_back(Clock::now());
      },
      iterations);
  testClock<Clock, false>(
      fmt::format("{} push_back with cold cache", clockName),
      [](auto& timePoints, size_t) -> void {
        timePoints.push_back(Clock::now());
      },
      iterations);

  testClock<Clock, true>(
      fmt::format("{} [] with hot cache", clockName),
      [](auto& timePoints, size_t i) -> void { timePoints[i] = Clock::now(); },
      iterations);
  testClock<Clock, false>(
      fmt::format("{} [] with cold cache", clockName),
      [](auto& timePoints, size_t i) -> void { timePoints[i] = Clock::now(); },
      iterations);
#if XSIMD_WITH_SSE2
  testClock<Clock, true>(
      fmt::format("{} non temporal with hot cache", clockName),
      [](auto& timePoints, size_t i) -> void {
        auto n = Clock::now();
        _mm_stream_pi(
            reinterpret_cast<__m64*>(&timePoints[i]),
            *reinterpret_cast<__m64*>(&n));
      },
      iterations);
  testClock<Clock, false>(
      fmt::format("{} non temporal with cold cache", clockName),
      [](auto& timePoints, size_t i) -> void {
        auto n = Clock::now();
        _mm_stream_pi(
            reinterpret_cast<__m64*>(&timePoints[i]),
            *reinterpret_cast<__m64*>(&n));
      },
      iterations);
#endif
}

/// Compute basic statistics acros an event sequence
/// \tparam Clock
/// \param eventSequence
/// \return
template <typename Clock>
auto timerEventStats(const NamedEventSequence<Clock>& eventSequence) {
  accumulator_set<
      double,
      stats<tag::min, tag::max, tag::mean, tag::lazy_variance>>
      accumulatorSet;
  for (size_t i = 0; i < eventSequence.endEvents.size(); i++) {
    auto durations = eventSequence.endEvents[i].timePoint -
        eventSequence.startEvents[i].timePoint;
    accumulatorSet(static_cast<double>(durations.count()));
  }
  return accumulatorSet;
}

/// For a given clock, this function returns some statistics on the how precise
/// the ScopedTimer are
/// \tparam Clock
/// \tparam warmCache runtime the test twice to warm the cache
/// \param iterations
template <typename Clock, bool warmCache>
void testNestedTimer(const std::string& message, size_t iterations) {
  NamedEventSequence<Clock> eventSequence;

  if constexpr (warmCache) {
    for (size_t i = 0; i < iterations; i++) {
      NamedNestedScopedTimer<Clock> timer1(
          "NamedNestedScopeTimer", eventSequence);
    };
    eventSequence.startEvents.clear();
    eventSequence.endEvents.clear();
  }

  for (size_t i = 0; i < iterations; i++) {
    NamedNestedScopedTimer<Clock> timer1(
        "NamedNestedScopeTimer", eventSequence);
  };
  auto stats = timerEventStats(eventSequence);
  std::cout << fmt::format(
      "{} : min = {},max = {}, mean = {}, variance = {}\n",
      message,
      min(stats),
      max(stats),
      mean(stats),
      variance(stats));
}

TEST_F(ClockTest, clockPrecision) {
  printClockStatistics<std::chrono::steady_clock>("steady_clock", iteration);
  std::cout << "------" << std::endl;
  printClockStatistics<std::chrono::high_resolution_clock>(
      "high_resolution_clock", iteration);
  ASSERT_TRUE(true);
}

TEST_F(ClockTest, nestTimerPrecision) {
  testNestedTimer<std::chrono::steady_clock, true>(
      "steady_clock hot cache", iteration);
  testNestedTimer<std::chrono::steady_clock, false>(
      "steady_clock cold cache", iteration);

  testNestedTimer<std::chrono::high_resolution_clock, true>(
      "high_resolution_clock hot cache", iteration);
  testNestedTimer<std::chrono::high_resolution_clock, true>(
      "high_resolution_clock cold cache", iteration);
  ASSERT_TRUE(true);
}
} // namespace facebook::velox::codegen
