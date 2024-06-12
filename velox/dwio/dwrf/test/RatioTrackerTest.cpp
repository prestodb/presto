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
#include <memory>

#include "velox/dwio/dwrf/writer/RatioTracker.h"

using namespace ::testing;

namespace facebook::velox::dwrf {

TEST(RatioTrackerTest, BasicTests) {
  struct TestCase {
    explicit TestCase(
        std::shared_ptr<RatioTracker> tracker,
        const std::vector<std::pair<size_t, size_t>>& samples,
        float finalResult)
        : tracker{std::move(tracker)},
          samples{samples},
          finalResult{finalResult} {}

    std::shared_ptr<RatioTracker> tracker;
    const std::vector<std::pair<size_t, size_t>> samples;
    const float finalResult;
  };

  std::vector<TestCase> testCases{
      TestCase{std::make_shared<RatioTracker>(0.1f), {}, 0.1f},
      TestCase{std::make_shared<RatioTracker>(0.3f), {}, 0.3f},
      TestCase{std::make_shared<RatioTracker>(0.3f), {{10, 3}}, 0.3f},
      TestCase{std::make_shared<RatioTracker>(0.3f), {{10, 5}}, 0.5f},
      TestCase{std::make_shared<RatioTracker>(0.3f), {{10, 5}, {0, 10}}, 0.5f},
      TestCase{
          std::make_shared<RatioTracker>(0.3f),
          {{10, 5}, {20, 10}, {10, 10}, {30, 10}},
          0.5f},
      TestCase{
          std::make_shared<RatioTracker>(0.3f),
          {{10, 11}, {20, 1}, {30, 1}, {40, 1}, {50, 1}},
          0.1f},
      TestCase{std::make_shared<CompressionRatioTracker>(), {}, 0.3f},
      TestCase{std::make_shared<FlushOverheadRatioTracker>(), {}, 0.1f},
      TestCase{std::make_shared<AverageRowSizeTracker>(), {}, 0.0f},
  };

  for (auto& testCase : testCases) {
    auto sampleSize = 0;
    for (const auto& sample : testCase.samples) {
      sampleSize += (sample.first ? 1 : 0);
      testCase.tracker->takeSample(sample.first, sample.second);
    }
    // Can consider EXPECT_NEAR if we have a specific bound.
    EXPECT_FLOAT_EQ(
        testCase.finalResult, testCase.tracker->getEstimatedRatio());
    EXPECT_EQ(sampleSize, testCase.tracker->getSampleSize());
  }
}

TEST(RatioTrackerTest, EmptyInputTests) {
  struct TestCase {
    explicit TestCase(
        std::shared_ptr<RatioTracker> tracker,
        const std::pair<size_t, size_t>& input,
        bool isEmptyInput)
        : tracker{std::move(tracker)},
          input{input},
          isEmptyInput{isEmptyInput} {}

    std::shared_ptr<RatioTracker> tracker;
    const std::pair<size_t, size_t> input;
    const bool isEmptyInput;
  };

  std::vector<TestCase> testCases{
      TestCase{std::make_shared<RatioTracker>(0.2f), {6, 3}, false},
      TestCase{std::make_shared<RatioTracker>(0.2f), {6, 0}, false},
      TestCase{std::make_shared<RatioTracker>(0.2f), {0, 0}, true},
      TestCase{std::make_shared<RatioTracker>(0.2f), {0, 3}, true},
      TestCase{std::make_shared<FlushOverheadRatioTracker>(), {6, 3}, false},
      TestCase{std::make_shared<FlushOverheadRatioTracker>(), {6, 0}, false},
      TestCase{std::make_shared<FlushOverheadRatioTracker>(), {0, 0}, true},
      TestCase{std::make_shared<FlushOverheadRatioTracker>(), {0, 3}, true},
      TestCase{std::make_shared<AverageRowSizeTracker>(), {6, 3}, false},
      TestCase{std::make_shared<AverageRowSizeTracker>(), {6, 0}, false},
      TestCase{std::make_shared<AverageRowSizeTracker>(), {0, 0}, true},
      TestCase{std::make_shared<AverageRowSizeTracker>(), {0, 3}, true},
      TestCase{std::make_shared<CompressionRatioTracker>(), {6, 3}, false},
      TestCase{std::make_shared<CompressionRatioTracker>(), {6, 0}, true},
      TestCase{std::make_shared<CompressionRatioTracker>(), {0, 0}, true},
      TestCase{std::make_shared<CompressionRatioTracker>(), {0, 3}, true},
  };

  for (auto& testCase : testCases) {
    EXPECT_EQ(
        testCase.isEmptyInput,
        testCase.tracker->isEmptyInput(
            testCase.input.first, testCase.input.second));
  }
}

} // namespace facebook::velox::dwrf
