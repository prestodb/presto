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
#include <limits>
#include <optional>
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/Timestamp.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class ArraySizeTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  int32_t arraySize(const std::vector<std::optional<T>>& input) {
    auto row = makeRowVector({makeNullableArrayVector(
        std::vector<std::vector<std::optional<T>>>{input})});
    return evaluateOnce<int32_t>("array_size(c0)", row).value();
  }
};

TEST_F(ArraySizeTest, boolean) {
  EXPECT_EQ(arraySize<bool>({true, false}), 2);
  EXPECT_EQ(arraySize<bool>({true}), 1);
  EXPECT_EQ(arraySize<bool>({}), 0);
  EXPECT_EQ(arraySize<bool>({true, false, true, std::nullopt}), 4);
}

TEST_F(ArraySizeTest, integer) {
  EXPECT_EQ(arraySize<int8_t>({}), 0);
  EXPECT_EQ(arraySize<int8_t>({1}), 1);
  EXPECT_EQ(arraySize<int8_t>({std::nullopt}), 1);
  EXPECT_EQ(arraySize<int8_t>({std::nullopt, 1}), 2);
}

TEST_F(ArraySizeTest, float) {
  EXPECT_EQ(arraySize<float>({}), 0);
  EXPECT_EQ(arraySize<float>({1.1}), 1);
  EXPECT_EQ(arraySize<float>({std::nullopt}), 1);
  EXPECT_EQ(arraySize<float>({std::nullopt, 1.1}), 2);
}

TEST_F(ArraySizeTest, varchar) {
  EXPECT_EQ(arraySize<std::string>({"red", "blue"}), 2);
  EXPECT_EQ(
      arraySize<std::string>({std::nullopt, "blue", "yellow", "orange"}), 4);
  EXPECT_EQ(arraySize<std::string>({}), 0);
  EXPECT_EQ(arraySize<std::string>({std::nullopt}), 1);
}

TEST_F(ArraySizeTest, date) {
  auto dt = [](const std::string& dateStr) { return DATE()->toDays(dateStr); };
  EXPECT_EQ(arraySize<int32_t>({dt("1970-01-01"), dt("2023-08-23")}), 2);
}

TEST_F(ArraySizeTest, timestamp) {
  auto ts = [](int64_t micros) { return Timestamp::fromMicros(micros); };
  EXPECT_EQ(arraySize<Timestamp>({}), 0);
  EXPECT_EQ(arraySize<Timestamp>({std::nullopt}), 1);
  EXPECT_EQ(arraySize<Timestamp>({ts(0), ts(1)}), 2);
}
} // namespace
} // namespace facebook::velox::functions::sparksql::test
