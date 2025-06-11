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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class UnBase64Test : public SparkFunctionBaseTest {
 protected:
  std::optional<std::string> unbase64(const std::optional<std::string>& a) {
    return evaluateOnce<std::string>("unbase64(c0)", a);
  }
};

TEST_F(UnBase64Test, basic) {
  EXPECT_EQ(unbase64(std::nullopt), std::nullopt);
  EXPECT_EQ(unbase64("TWFu"), "Man");
  EXPECT_EQ(unbase64("TWFu\r\nTWFu"), "ManMan");
  EXPECT_EQ(unbase64("aGVsbG8gd29ybGQ="), "hello world");
  EXPECT_EQ(unbase64("U3BhcmsgU1FM"), "Spark SQL");
  EXPECT_EQ(unbase64("#"), "");
  EXPECT_EQ(unbase64("YQ==="), "a");
  EXPECT_EQ(unbase64("aA"), "h");
  EXPECT_EQ(unbase64("c3d"), "sw");
  EXPECT_EQ(unbase64("cd@"), "q");
  EXPECT_EQ(
      unbase64("SGVsbG8gV29ybGQgZnJvbSBWZW@@Xvece"),
      "Hello World from Vee\xEFy\xC7");
  EXPECT_EQ(unbase64("@@dmVsb3g="), "velox");
}

TEST_F(UnBase64Test, error) {
  VELOX_ASSERT_USER_THROW(
      unbase64("aGVsx"), "Last unit does not have enough valid bits");
  VELOX_ASSERT_USER_THROW(
      unbase64("xx=y"), "Input byte array has wrong 4-byte ending unit");
  VELOX_ASSERT_USER_THROW(
      unbase64("xx="), "Input byte array has wrong 4-byte ending unit");
  VELOX_ASSERT_USER_THROW(
      unbase64("aGVs="), "Input byte array has wrong 4-byte ending unit");
  VELOX_ASSERT_USER_THROW(
      unbase64("AQ==y"), "Input byte array has incorrect ending");
  VELOX_ASSERT_USER_THROW(
      unbase64("a"), "Input should at least have 2 bytes for base64 bytes");

  VELOX_ASSERT_USER_THROW(
      unbase64("c@"), "Last unit does not have enough valid bits");
  VELOX_ASSERT_USER_THROW(
      unbase64("=="), "Input byte array has wrong 4-byte ending unit");
  VELOX_ASSERT_USER_THROW(
      unbase64("SGVsbG8gV29ybGQgZnJvbSBWZW===xveCE="),
      "Input byte array has incorrect ending");
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
