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

#include "velox/type/LongDecimal.h"

namespace facebook::velox {
namespace {

TEST(DecimalTest, toString) {
  EXPECT_EQ(std::to_string(buildInt128(0, 0)), "0");
  EXPECT_EQ(std::to_string(buildInt128(0, 1)), "1");
  EXPECT_EQ(
      std::to_string(buildInt128(0xFFFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull)),
      "-1");
  EXPECT_EQ(std::to_string(buildInt128(1, 0)), "18446744073709551616");
  EXPECT_EQ(
      std::to_string(buildInt128(0xFFFFFFFFFFFFFFFFull, 0)),
      "-18446744073709551616");
  constexpr int128_t kMax =
      buildInt128(0x7FFFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFFFFFull);
  EXPECT_EQ(std::to_string(kMax), "170141183460469231731687303715884105727");
  EXPECT_EQ(
      std::to_string(-kMax - 1), "-170141183460469231731687303715884105728");
}

} // namespace
} // namespace facebook::velox
