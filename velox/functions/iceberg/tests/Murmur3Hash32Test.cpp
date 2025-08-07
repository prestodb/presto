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
#include "velox/functions/iceberg/Murmur3Hash32.h"

#include <gtest/gtest.h>

namespace facebook::velox::functions::iceberg {
namespace {

TEST(Murmur3Hash32Test, bigint) {
  Murmur3Hash32 func;
  EXPECT_EQ(func.hashInt64(10), -289985220);
  EXPECT_EQ(func.hashInt64(0), 1669671676);
  EXPECT_EQ(func.hashInt64(-5), 1222806974);
}

TEST(Murmur3Hash32Test, string) {
  Murmur3Hash32 func;

  const auto hash = [&](std::string input) {
    return func.hashBytes(input.c_str(), input.size());
  };

  EXPECT_EQ(hash("abcdefg"), -2009294074);
  EXPECT_EQ(hash("abc"), -1277324294);
  EXPECT_EQ(hash("abcd"), 1139631978);
  EXPECT_EQ(hash("abcde"), -392455434);
  EXPECT_EQ(hash("测试"), -25843656);
  EXPECT_EQ(hash("测试raul试测"), -912788207);
  EXPECT_EQ(hash(""), 0);
  EXPECT_EQ(hash("Товары"), 1817480714);
  EXPECT_EQ(hash("😀"), -1095487750);
}
} // namespace
} // namespace facebook::velox::functions::iceberg
