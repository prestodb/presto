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
#include "velox/functions/iceberg/BucketFunction.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/iceberg/tests/IcebergFunctionBaseTest.h"

#include <gtest/gtest.h>

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::velox::functions::iceberg {
namespace {

class BucketFunctionTest
    : public functions::iceberg::test::IcebergFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<int32_t> bucket(
      std::optional<int32_t> numBuckets,
      std::optional<T> value) {
    return evaluateOnce<int32_t>("bucket(c0, c1)", numBuckets, value);
  }

  template <typename T>
  std::optional<int32_t> bucket(
      const TypePtr& type,
      std::optional<int32_t> numBuckets,
      std::optional<T> value) {
    return evaluateOnce<int32_t>(
        "bucket(c0, c1)", {INTEGER(), type}, numBuckets, value);
  }
};

TEST_F(BucketFunctionTest, integerTypes) {
  EXPECT_EQ(bucket<int32_t>(10, 8), 3);
  EXPECT_EQ(bucket<int32_t>(10, 42), 6);
  EXPECT_EQ(bucket<int32_t>(100, 34), 79);
  EXPECT_EQ(bucket<int32_t>(1000, INT_MAX), 606);
  EXPECT_EQ(bucket<int32_t>(1000, INT_MIN), 856);
  EXPECT_EQ(bucket<int64_t>(10, 8), 3);
  EXPECT_EQ(bucket<int64_t>(10, 42), 6);
  EXPECT_EQ(bucket<int64_t>(100, -34), 97);
  EXPECT_EQ(bucket<int64_t>(2, -1), 0);
  VELOX_ASSERT_THROW(
      bucket<int64_t>(0, 34),
      "Reason: (0 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
  VELOX_ASSERT_THROW(
      bucket<int64_t>(-3, 34),
      "Reason: (-3 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
}

TEST_F(BucketFunctionTest, string) {
  EXPECT_EQ(bucket<std::string>(5, "abcdefg"), 4);
  EXPECT_EQ(bucket<std::string>(128, "abc"), 122);
  EXPECT_EQ(bucket<std::string>(128, "abcd"), 106);
  EXPECT_EQ(bucket<std::string>(64, "abcde"), 54);
  EXPECT_EQ(bucket<std::string>(12, "测试"), 8);
  EXPECT_EQ(bucket<std::string>(16, "测试raul试测"), 1);
  EXPECT_EQ(bucket<std::string>(16, ""), 0);
  EXPECT_EQ(bucket<std::string>(16, "Товары"), 10);
  EXPECT_EQ(bucket<std::string>(120, "😀"), 58);
  VELOX_ASSERT_THROW(
      bucket<std::string>(0, "abc"),
      "Reason: (0 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
  VELOX_ASSERT_THROW(
      bucket<std::string>(-3, "abc"),
      "Reason: (-3 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
}

TEST_F(BucketFunctionTest, binary) {
  EXPECT_EQ(bucket<StringView>(VARBINARY(), 128, "abc"), 122);
  EXPECT_EQ(bucket<StringView>(VARBINARY(), 5, "abcdefg"), 4);
}

TEST_F(BucketFunctionTest, timestamp) {
  EXPECT_EQ(bucket<Timestamp>(20, Timestamp(1633046400, 0)), 17);
  EXPECT_EQ(bucket<Timestamp>(20, Timestamp(0, 123456789)), 0);
  EXPECT_EQ(bucket<Timestamp>(20, Timestamp(-62167219200, 0)), 5);
  VELOX_ASSERT_THROW(
      bucket<Timestamp>(0, Timestamp(-62167219200, 0)),
      "Reason: (0 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
  VELOX_ASSERT_THROW(
      bucket<Timestamp>(-3, Timestamp(-62167219200, 0)),
      "Reason: (-3 vs. 0) Invalid number of buckets.\nExpression: numBuckets <= 0\n");
}

TEST_F(BucketFunctionTest, date) {
  EXPECT_EQ(bucket<int32_t>(DATE(), 10, 8), 3);
  EXPECT_EQ(bucket<int32_t>(DATE(), 10, 42), 6);
}

TEST_F(BucketFunctionTest, decimal) {
  EXPECT_EQ(bucket<int64_t>(DECIMAL(9, 2), 64, 1234), 56);
  EXPECT_EQ(bucket<int64_t>(DECIMAL(9, 2), 18, 1230), 13);
  EXPECT_EQ(bucket<int64_t>(DECIMAL(9, 2), 16, 12999), 2);
  EXPECT_EQ(bucket<int64_t>(DECIMAL(9, 2), 128, 5), 85);
  EXPECT_EQ(bucket<int64_t>(DECIMAL(9, 2), 18, 5), 3);

  EXPECT_EQ(bucket<int128_t>(DECIMAL(38, 2), 10, 12), 7);
  EXPECT_EQ(bucket<int128_t>(DECIMAL(38, 2), 10, 0), 7);
  EXPECT_EQ(bucket<int128_t>(DECIMAL(38, 2), 10, 234), 6);
  EXPECT_EQ(
      bucket<int128_t>(DECIMAL(38, 2), 10, DecimalUtil::kLongDecimalMax), 4);
}

} // namespace
} // namespace facebook::velox::functions::iceberg
