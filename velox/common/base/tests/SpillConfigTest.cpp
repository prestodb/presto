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

#include "velox/common/base/SpillConfig.h"
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/HashBitRange.h"

using namespace facebook::velox::common;
using namespace facebook::velox::exec;

TEST(SpillConfig, spillLevel) {
  const uint8_t kInitialBitOffset = 16;
  const uint8_t kNumPartitionsBits = 3;
  const SpillConfig config(
      []() -> std::string_view { return ""; },
      [&](uint64_t) {},
      "fakeSpillPath",
      0,
      0,
      nullptr,
      0,
      0,
      kInitialBitOffset,
      kNumPartitionsBits,
      0,
      0,
      0,
      "none");
  struct {
    uint8_t bitOffset;
    // Indicates an invalid if 'expectedLevel' is negative.
    int32_t expectedLevel;

    std::string debugString() const {
      return fmt::format(
          "bitOffset:{}, expectedLevel:{}", bitOffset, expectedLevel);
    }
  } testSettings[] = {
      {0, -1},
      {kInitialBitOffset - 1, -1},
      {kInitialBitOffset - kNumPartitionsBits, -1},
      {kInitialBitOffset, 0},
      {kInitialBitOffset + 1, -1},
      {kInitialBitOffset + kNumPartitionsBits, 1},
      {kInitialBitOffset + 3 * kNumPartitionsBits, 3},
      {kInitialBitOffset + 15 * kNumPartitionsBits, 15},
      {kInitialBitOffset + 16 * kNumPartitionsBits, -1}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    if (testData.expectedLevel == -1) {
      ASSERT_ANY_THROW(config.spillLevel(testData.bitOffset));
    } else {
      ASSERT_EQ(config.spillLevel(testData.bitOffset), testData.expectedLevel);
    }
  }
}

TEST(SpillConfig, spillLevelLimit) {
  struct {
    uint8_t startBitOffset;
    int32_t numBits;
    uint8_t bitOffset;
    int32_t maxSpillLevel;
    bool expectedExceeds;

    std::string debugString() const {
      return fmt::format(
          "startBitOffset:{}, numBits:{}, bitOffset:{}, maxSpillLevel:{}, expectedExceeds:{}",
          startBitOffset,
          numBits,
          bitOffset,
          maxSpillLevel,
          expectedExceeds);
    }
  } testSettings[] = {
      {0, 2, 2, 0, true},
      {0, 2, 2, 1, false},
      {0, 2, 4, 0, true},
      {0, 2, 0, -1, false},
      {0, 2, 62, -1, false},
      {0, 2, 63, -1, true},
      {0, 2, 64, -1, true},
      {0, 2, 65, -1, true},
      {30, 3, 30, 0, false},
      {30, 3, 33, 0, true},
      {30, 3, 30, 1, false},
      {30, 3, 33, 1, false},
      {30, 3, 36, 1, true},
      {30, 3, 0, -1, false},
      {30, 3, 60, -1, false},
      {30, 3, 63, -1, true},
      {30, 3, 66, -1, true}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    const HashBitRange partitionBits(
        testData.startBitOffset, testData.startBitOffset + testData.numBits);
    const SpillConfig config(
        []() -> std::string_view { return ""; },
        [&](uint64_t) {},
        "fakeSpillPath",
        0,
        0,
        nullptr,
        0,
        0,
        testData.startBitOffset,
        testData.numBits,
        testData.maxSpillLevel,
        0,
        0,
        "none");

    ASSERT_EQ(
        testData.expectedExceeds,
        config.exceedSpillLevelLimit(testData.bitOffset));
  }
}

TEST(SpillConfig, spillableReservationPercentages) {
  struct {
    uint32_t growthPct;
    int32_t minPct;
    bool expectedError;

    std::string debugString() const {
      return fmt::format(
          "growthPct:{}, minPct:{}, expectedError:{}",
          growthPct,
          minPct,
          expectedError);
    }
  } testSettings[] = {
      {100, 50, false},
      {50, 50, false},
      {50, 100, true},
      {1, 50, true},
      {1, 1, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    auto createConfigFn = [&]() {
      const SpillConfig config(
          [&]() -> std::string_view { return ""; },
          [&](uint64_t) {},
          "spillableReservationPercentages",
          0,
          0,
          nullptr,
          testData.minPct,
          testData.growthPct,
          0,
          0,
          0,
          1'000'000,
          0,
          "none");
    };

    if (testData.expectedError) {
      VELOX_ASSERT_THROW(
          createConfigFn(),
          "Spillable memory reservation growth pct should not be lower than minimum available pct");
    } else {
      createConfigFn();
    }
  }
}
