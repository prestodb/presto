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

#include "velox/dwio/common/MeasureTime.h"

using namespace ::testing;
using namespace ::facebook::velox::dwio::common;

TEST(MeasureTimeTests, DoesntCreateMeasureIfNoCallback) {
  EXPECT_FALSE(measureTimeIfCallback(nullptr).has_value());
}

TEST(MeasureTimeTests, CreatesMeasureIfCallback) {
  auto callback =
      std::function<void(std::chrono::high_resolution_clock::duration)>(
          [](const auto&) {});
  EXPECT_TRUE(measureTimeIfCallback(callback).has_value());
}

TEST(MeasureTimeTests, MeasuresTime) {
  bool measured{false};
  {
    auto callback =
        std::function<void(std::chrono::high_resolution_clock::duration)>(
            [&measured](const auto&) { measured = true; });
    auto measure = measureTimeIfCallback(callback);
    EXPECT_TRUE(measure.has_value());
    EXPECT_FALSE(measured);
  }
  EXPECT_TRUE(measured);
}
