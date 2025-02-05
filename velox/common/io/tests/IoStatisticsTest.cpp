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
#include "velox/common/io/IoStatistics.h"
#include <gtest/gtest.h>
#include "velox/common/base/RuntimeMetrics.h"

namespace facebook::velox::io {

TEST(IoStatisticsTest, merge) {
  IoStatistics ioStats;
  ioStats.addStorageStats(
      "remoteRead", RuntimeCounter(100, RuntimeCounter::Unit::kBytes));

  IoStatistics ioStats2;
  ioStats2.addStorageStats(
      "localRead", RuntimeCounter(100, RuntimeCounter::Unit::kBytes));
  ioStats2.addStorageStats(
      "remoteRead", RuntimeCounter(200, RuntimeCounter::Unit::kBytes));

  ioStats.merge(ioStats2);

  RuntimeMetric localReadResult;
  localReadResult.unit = RuntimeCounter::Unit::kBytes;
  localReadResult.sum = 100;
  localReadResult.count = 1;
  localReadResult.min = 100;
  localReadResult.max = 100;

  RuntimeMetric remoteReadResult;
  remoteReadResult.unit = RuntimeCounter::Unit::kBytes;
  remoteReadResult.sum = 300;
  remoteReadResult.count = 2;
  remoteReadResult.min = 100;
  remoteReadResult.max = 200;

  EXPECT_EQ(
      ioStats.storageStats().at("localRead").toString(),
      localReadResult.toString());
  EXPECT_EQ(
      ioStats.storageStats().at("remoteRead").toString(),
      remoteReadResult.toString());
}
} // namespace facebook::velox::io
