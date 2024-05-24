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

#include "velox/common/base/Status.h"
#include <gtest/gtest.h>
#include <sstream>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::test {
namespace {

TEST(StatusTest, testCodeAndMessage) {
  Status ok = Status::OK();
  ASSERT_EQ(StatusCode::kOK, ok.code());
  ASSERT_EQ("", ok.message());
  ASSERT_EQ("OK", ok.codeAsString());
  ASSERT_TRUE(ok.ok());
  ASSERT_FALSE(ok.isIOError());

  Status fileError = Status::IOError("file error");
  ASSERT_EQ(StatusCode::kIOError, fileError.code());
  ASSERT_EQ("file error", fileError.message());
}

TEST(StatusTest, testNoMessage) {
  Status fileError = Status::IOError();
  ASSERT_EQ(StatusCode::kIOError, fileError.code());
  ASSERT_EQ("", fileError.message());
  ASSERT_EQ("IOError: ", fileError.toString());
  ASSERT_EQ("IOError", fileError.codeAsString());
}

TEST(StatusTest, testToString) {
  Status fileError = Status::IOError("file error");
  ASSERT_EQ("IOError: file error", fileError.toString());
  ASSERT_EQ("IOError", fileError.codeAsString());

  std::stringstream ss;
  ss << fileError;
  ASSERT_EQ(fileError.toString(), ss.str());

  // Check that fmt has the right specializations.
  ASSERT_EQ(fileError.toString(), fmt::format("{}", fileError));
  ASSERT_EQ("Unknown error", fmt::format("{}", StatusCode::kUnknownError));
}

TEST(StatusTest, andStatus) {
  Status a = Status::OK();
  Status b = Status::OK();
  Status c = Status::Invalid("invalid value");
  Status d = Status::IOError("file error");

  Status res;
  res = a & b;
  ASSERT_TRUE(res.ok());
  res = a & c;
  ASSERT_TRUE(res.isInvalid());
  res = d & c;
  ASSERT_TRUE(res.isIOError());

  res = Status::OK();
  res &= c;
  ASSERT_TRUE(res.isInvalid());
  res &= d;
  ASSERT_TRUE(res.isInvalid());

  // With rvalues.
  res = Status::OK() & Status::Invalid("foo");
  ASSERT_TRUE(res.isInvalid());
  res = Status::Invalid("foo") & Status::OK();
  ASSERT_TRUE(res.isInvalid());
  res = Status::Invalid("foo") & Status::IOError("bar");
  ASSERT_TRUE(res.isInvalid());

  res = Status::OK();
  res &= Status::OK();
  ASSERT_TRUE(res.ok());
  res &= Status::Invalid("foo");
  ASSERT_TRUE(res.isInvalid());
  res &= Status::IOError("bar");
  ASSERT_TRUE(res.isInvalid());
}

TEST(StatusTest, testEquality) {
  ASSERT_EQ(Status(), Status::OK());
  ASSERT_EQ(Status::Invalid("error"), Status::Invalid("error"));

  ASSERT_NE(Status::Invalid("error"), Status::OK());
  ASSERT_NE(Status::Invalid("error"), Status::Invalid("other error"));
}

TEST(StatusTest, testAbort) {
  Status a = Status::Invalid("will abort process");
  ASSERT_DEATH(a.abort(), "");
}

Status returnIf(bool cond) {
  VELOX_RETURN_IF(cond, Status::Invalid("error"));
  return Status::OK();
}

Status returnNotOk(Status s) {
  VELOX_RETURN_NOT_OK(s);
  return Status::Invalid("invalid");
}

TEST(StatusTest, macros) {
  ASSERT_EQ(returnIf(true), Status::Invalid("error"));
  ASSERT_EQ(returnIf(false), Status::OK());

  ASSERT_EQ(returnNotOk(Status::UserError("user")), Status::UserError("user"));
  ASSERT_EQ(returnNotOk(Status::OK()), Status::Invalid("invalid"));

  VELOX_CHECK_OK(Status::OK()); // does not throw.

  bool didThrow = false;
  try {
    VELOX_CHECK_OK(Status::Invalid("invalid"));
  } catch (const VeloxRuntimeError&) {
    didThrow = true;
  }
  ASSERT_TRUE(didThrow) << "VELOX_CHECK_OK did not throw";
}

Expected<int> modulo(int a, int b) {
  if (b == 0) {
    return folly::makeUnexpected(Status::UserError("division by zero"));
  }

  return a % b;
}

TEST(StatusTest, expected) {
  auto result = modulo(10, 3);
  EXPECT_TRUE(result.hasValue());
  EXPECT_EQ(result.value(), 1);

  result = modulo(10, 0);
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), Status::UserError("division by zero"));
}

} // namespace
} // namespace facebook::velox::test
