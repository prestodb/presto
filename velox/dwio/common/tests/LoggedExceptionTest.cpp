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
#include "velox/dwio/common/exception/Exception.h"

using namespace facebook::dwio::common::exception;
using namespace facebook::velox;

namespace {
struct ExceptionCounter {
  int numExceptions = 0;
  int numWarnings = 0;
};

class TestExceptionLogger : public ExceptionLogger {
 public:
  explicit TestExceptionLogger(std::shared_ptr<ExceptionCounter> counter)
      : counter_(std::move(counter)) {}

  void logException(
      const char* /* unused */,
      size_t /* unused */,
      const char* /* unused */,
      const char* /* unused */,
      const char* /* unused */) override {
    ++counter_->numExceptions;
  }

  void logWarning(
      const char* /* unused */,
      size_t /* unused */,
      const char* /* unused */,
      const char* /* unused */,
      const char* /* unused */) override {
    ++counter_->numWarnings;
  }

 private:
  std::shared_ptr<ExceptionCounter> counter_;
};
} // namespace

TEST(LoggedExceptionTest, basic) {
  // no logger
  ASSERT_ANY_THROW(throw LoggedException("Test error message"));

  auto counter = std::make_shared<ExceptionCounter>();
  registerExceptionLogger(std::make_unique<TestExceptionLogger>(counter));

  ASSERT_ANY_THROW(throw LoggedException("Test error message"));
  ASSERT_EQ(1, counter->numExceptions);
  ASSERT_EQ(0, counter->numWarnings);

  DWIO_WARN("test warning");
  ASSERT_EQ(1, counter->numExceptions);
  ASSERT_EQ(1, counter->numWarnings);

  ASSERT_ANY_THROW(throw LoggedException("Test error message #2"));
  ASSERT_EQ(2, counter->numExceptions);
  ASSERT_EQ(1, counter->numWarnings);

  DWIO_WARN("test warning #2");
  ASSERT_EQ(2, counter->numExceptions);
  ASSERT_EQ(2, counter->numWarnings);

  ASSERT_ANY_THROW(DWIO_ENSURE_EQ(1, 2));
  ASSERT_EQ(3, counter->numExceptions);
  ASSERT_EQ(2, counter->numWarnings);

  // Verifier duplicate registration is rejected
  ASSERT_ANY_THROW(
      registerExceptionLogger(std::make_unique<TestExceptionLogger>(counter)));
}
