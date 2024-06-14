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
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/JsonType.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

namespace facebook::velox::functions {

namespace {

class FailTest : public functions::test::FunctionBaseTest {};

TEST_F(FailTest, basic) {
  auto message = std::optional("test error message");
  VELOX_ASSERT_USER_THROW(
      evaluateOnce<UnknownValue>("fail(c0)", message), message.value());

  EXPECT_EQ(std::nullopt, evaluateOnce<UnknownValue>("try(fail(c0))", message));

  VELOX_ASSERT_USER_THROW(
      evaluateOnce<UnknownValue>("fail(123::integer, c0)", message),
      message.value());

  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<UnknownValue>("try(fail(123::integer, c0))", message));
}

TEST_F(FailTest, json) {
  auto json =
      std::optional(R"({"code": 123, "message": "test error message"})");

  VELOX_ASSERT_USER_THROW(
      evaluateOnce<UnknownValue>("fail(c0)", JSON(), json),
      "test error message");

  EXPECT_EQ(
      std::nullopt, evaluateOnce<UnknownValue>("try(fail(c0))", JSON(), json));

  VELOX_ASSERT_USER_THROW(
      evaluateOnce<UnknownValue>("fail(123::integer, c0)", JSON(), json),
      "test error message");

  EXPECT_EQ(
      std::nullopt,
      evaluateOnce<UnknownValue>("try(fail(123::integer, c0))", JSON(), json));
}

TEST_F(FailTest, invalidJson) {
  // Invalid JSON.
  VELOX_ASSERT_USER_THROW(
      evaluateOnce<UnknownValue>(
          "fail(c0)", JSON(), std::optional("not a valid JSON")),
      "Cannot extract 'message' from the JSON");

  // Valid JSON with missing 'message' field.
  VELOX_ASSERT_USER_THROW(
      evaluateOnce<UnknownValue>("fail(c0)", JSON(), std::optional("{}")),
      "Cannot extract 'message' from the JSON");

  VELOX_ASSERT_USER_THROW(
      evaluateOnce<UnknownValue>(
          "fail(c0)", JSON(), std::optional(R"({"code": 123})")),
      "Cannot extract 'message' from the JSON");

  // Valid JSON with 'message' field that's not string.
  VELOX_ASSERT_USER_THROW(
      evaluateOnce<UnknownValue>(
          "fail(c0)",
          JSON(),
          std::optional(R"({"code": 123, "message": [1, 2, 3]})")),
      "Cannot extract 'message' from the JSON");
}

} // namespace
} // namespace facebook::velox::functions
