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

#include "velox/common/base/SuccinctPrinter.h"

#include <gtest/gtest.h>

namespace facebook::velox {

TEST(SuccinctPrinterTest, testSuccinctNanos) {
  EXPECT_EQ(succinctNanos(123), "123ns");
  EXPECT_EQ(succinctNanos(1'000), "1.00us");
  EXPECT_EQ(succinctNanos(1'234), "1.23us");
  EXPECT_EQ(succinctNanos(123'456), "123.46us");
  EXPECT_EQ(succinctNanos(1'000'000), "1.00ms");
  EXPECT_EQ(succinctNanos(12'345'678), "12.35ms");
  EXPECT_EQ(succinctNanos(12'345'678, 6), "12.345678ms");
  EXPECT_EQ(succinctNanos(1'000'000'000), "1.00s");
  EXPECT_EQ(succinctNanos(1'234'567'890), "1.23s");
  EXPECT_EQ(succinctNanos(60'499'000'000), "1m 0s");
  EXPECT_EQ(succinctNanos(60'555'000'000), "1m 1s");
  EXPECT_EQ(succinctNanos(3'599'499'000'000), "59m 59s");
  EXPECT_EQ(succinctNanos(3'600'000'000'000), "1h 0m 0s");
  EXPECT_EQ(succinctNanos(86'399'499'000'000), "23h 59m 59s");
  EXPECT_EQ(succinctNanos(86'400'123'000'000), "1d 0h 0m 0s");
  EXPECT_EQ(succinctNanos(867'661'789'000'000), "10d 1h 1m 2s");
}

TEST(SuccinctPrinterTest, testSuccinctMillis) {
  EXPECT_EQ(succinctMillis(123), "123ms");
  EXPECT_EQ(succinctMillis(1'000), "1.00s");
  EXPECT_EQ(succinctMillis(1'234), "1.23s");
  EXPECT_EQ(succinctMillis(59'990), "59.99s");
  EXPECT_EQ(succinctMillis(60'499), "1m 0s");
  EXPECT_EQ(succinctMillis(61'000), "1m 1s");
  EXPECT_EQ(succinctMillis(3'599'456), "59m 59s");
  EXPECT_EQ(succinctMillis(3'600'000), "1h 0m 0s");
  EXPECT_EQ(succinctMillis(86'399'498), "23h 59m 59s");
  EXPECT_EQ(succinctMillis(86'400'123), "1d 0h 0m 0s");
  EXPECT_EQ(succinctMillis(867'661'789), "10d 1h 1m 2s");
}

TEST(SuccinctPrinterTest, testSuccinctBytes) {
  EXPECT_EQ(succinctBytes(123), "123B");
  EXPECT_EQ(succinctBytes(1'024), "1.00KB");
  EXPECT_EQ(succinctBytes(123'456), "120.56KB");
  EXPECT_EQ(succinctBytes(1'048'576), "1.00MB");
  EXPECT_EQ(succinctBytes(12'345'678, 4), "11.7738MB");
  EXPECT_EQ(succinctBytes(1'073'741'824), "1.00GB");
  EXPECT_EQ(succinctBytes(1'234'567'890), "1.15GB");
  EXPECT_EQ(succinctBytes(1'099'511'627'776), "1.00TB");
  EXPECT_EQ(succinctBytes(1234'099'511'627'776), "1122.41TB");
}

} // namespace facebook::velox
