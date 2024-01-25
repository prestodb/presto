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

#include "velox/common/base/SpillStats.h"
#include <gtest/gtest.h>
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"

using namespace facebook::velox::common;

TEST(SpillTest, spillStats) {
  SpillStats stats1;
  ASSERT_TRUE(stats1.empty());
  stats1.spillRuns = 100;
  stats1.spilledInputBytes = 2048;
  stats1.spilledBytes = 1024;
  stats1.spilledPartitions = 1024;
  stats1.spilledFiles = 1023;
  stats1.spillWriteTimeUs = 1023;
  stats1.spillFlushTimeUs = 1023;
  stats1.spillWrites = 1023;
  stats1.spillSortTimeUs = 1023;
  stats1.spillFillTimeUs = 1023;
  stats1.spilledRows = 1023;
  stats1.spillSerializationTimeUs = 1023;
  stats1.spillMaxLevelExceededCount = 3;
  ASSERT_FALSE(stats1.empty());
  SpillStats stats2;
  stats2.spillRuns = 100;
  stats2.spilledInputBytes = 2048;
  stats2.spilledBytes = 1024;
  stats2.spilledPartitions = 1025;
  stats2.spilledFiles = 1026;
  stats2.spillWriteTimeUs = 1026;
  stats2.spillFlushTimeUs = 1027;
  stats2.spillWrites = 1028;
  stats2.spillSortTimeUs = 1029;
  stats2.spillFillTimeUs = 1030;
  stats2.spilledRows = 1031;
  stats2.spillSerializationTimeUs = 1032;
  stats2.spillMaxLevelExceededCount = 4;
  ASSERT_TRUE(stats1 < stats2);
  ASSERT_TRUE(stats1 <= stats2);
  ASSERT_FALSE(stats1 > stats2);
  ASSERT_FALSE(stats1 >= stats2);
  ASSERT_TRUE(stats1 != stats2);
  ASSERT_FALSE(stats1 == stats2);

  ASSERT_TRUE(stats1 == stats1);
  ASSERT_FALSE(stats1 != stats1);
  ASSERT_FALSE(stats1 > stats1);
  ASSERT_TRUE(stats1 >= stats1);
  ASSERT_FALSE(stats1 < stats1);
  ASSERT_TRUE(stats1 <= stats1);

  SpillStats delta = stats2 - stats1;
  ASSERT_EQ(delta.spilledInputBytes, 0);
  ASSERT_EQ(delta.spilledBytes, 0);
  ASSERT_EQ(delta.spilledPartitions, 1);
  ASSERT_EQ(delta.spilledFiles, 3);
  ASSERT_EQ(delta.spillWriteTimeUs, 3);
  ASSERT_EQ(delta.spillFlushTimeUs, 4);
  ASSERT_EQ(delta.spillWrites, 5);
  ASSERT_EQ(delta.spillSortTimeUs, 6);
  ASSERT_EQ(delta.spillFillTimeUs, 7);
  ASSERT_EQ(delta.spilledRows, 8);
  ASSERT_EQ(delta.spillSerializationTimeUs, 9);
  delta = stats1 - stats2;
  ASSERT_EQ(delta.spilledInputBytes, 0);
  ASSERT_EQ(delta.spilledBytes, 0);
  ASSERT_EQ(delta.spilledPartitions, -1);
  ASSERT_EQ(delta.spilledFiles, -3);
  ASSERT_EQ(delta.spillWriteTimeUs, -3);
  ASSERT_EQ(delta.spillFlushTimeUs, -4);
  ASSERT_EQ(delta.spillWrites, -5);
  ASSERT_EQ(delta.spillSortTimeUs, -6);
  ASSERT_EQ(delta.spillFillTimeUs, -7);
  ASSERT_EQ(delta.spilledRows, -8);
  ASSERT_EQ(delta.spillSerializationTimeUs, -9);
  ASSERT_EQ(delta.spillMaxLevelExceededCount, -1);
  stats1.spilledInputBytes = 2060;
  stats1.spilledBytes = 1030;
  VELOX_ASSERT_THROW(stats1 < stats2, "");
  VELOX_ASSERT_THROW(stats1 > stats2, "");
  VELOX_ASSERT_THROW(stats1 <= stats2, "");
  VELOX_ASSERT_THROW(stats1 >= stats2, "");
  ASSERT_TRUE(stats1 != stats2);
  ASSERT_FALSE(stats1 == stats2);
  const SpillStats zeroStats;
  stats1.reset();
  ASSERT_EQ(zeroStats, stats1);
  ASSERT_EQ(
      stats2.toString(),
      "spillRuns[100] spilledInputBytes[2.00KB] spilledBytes[1.00KB] spilledRows[1031] spilledPartitions[1025] spilledFiles[1026] spillFillTimeUs[1.03ms] spillSortTime[1.03ms] spillSerializationTime[1.03ms] spillWrites[1028] spillFlushTime[1.03ms] spillWriteTime[1.03ms] maxSpillExceededLimitCount[4]");
}
