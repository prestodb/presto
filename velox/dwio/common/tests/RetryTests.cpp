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
#include "velox/dwio/common/Retry.h"

using namespace std::chrono_literals;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::common::retrypolicy;

class Raise {
 public:
  explicit Raise(uint8_t exceptionCount)
      : exceptionCount_(exceptionCount), count_(0) {}

  template <typename T>
  T call(const T& value) {
    if (count_++ < exceptionCount_) {
      throw retriable_error(std::runtime_error("Bad!!!"));
    }

    return value;
  }

  uint8_t count() const {
    return count_;
  }

  ~Raise() = default;

 private:
  const uint8_t exceptionCount_;
  uint8_t count_;
};

TEST(RetryModuleTests, retryUntilSuccessDefault) {
  Raise raise(4);
  std::function<int8_t()> retriable = [&raise]() {
    return raise.call<int8_t>(10);
  };

  KAttemptsPolicyFactory policyFactory({10ms, 10ms, 10ms, 10ms, 10ms});
  auto start = std::chrono::steady_clock::now();
  int8_t result =
      RetryModule::withRetry(retriable, policyFactory.getRetryPolicy());
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);

  ASSERT_GE(duration.count(), 40);
  ASSERT_EQ(result, 10);
  ASSERT_EQ(raise.count(), 5);
}

TEST(RetryModuleTests, retryUntilSuccessBackoff) {
  Raise raise(4);
  std::function<int8_t()> retriable = [&raise]() {
    return raise.call<int8_t>(10);
  };

  ExponentialBackoffPolicyFactory policyFactory(1ms, 25ms, 5);
  auto start = std::chrono::steady_clock::now();
  int8_t result =
      RetryModule::withRetry(retriable, policyFactory.getRetryPolicy());
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);

  ASSERT_GE(duration.count(), 15);
  ASSERT_EQ(result, 10);
  ASSERT_EQ(raise.count(), 5);
}

TEST(RetryModuleTests, retryCapMaxDelay) {
  Raise raise(4);
  std::function<int8_t()> retriable = [&raise]() {
    return raise.call<int8_t>(10);
  };

  ExponentialBackoffPolicyFactory policyFactory(1ms, 2ms, 5);
  auto start = std::chrono::steady_clock::now();
  int8_t result =
      RetryModule::withRetry(retriable, policyFactory.getRetryPolicy());
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);

  ASSERT_GE(duration.count(), 7);
  ASSERT_EQ(result, 10);
  ASSERT_EQ(raise.count(), 5);
}

TEST(RetryModuleTests, failOnRetriesExceededDefault) {
  Raise raise(6);
  std::function<int8_t()> retriable = [&raise]() {
    return raise.call<int8_t>(10);
  };

  KAttemptsPolicyFactory policyFactory({10ms, 10ms, 10ms, 10ms, 10ms});
  ASSERT_THROW(
      RetryModule::withRetry(retriable, policyFactory.getRetryPolicy()),
      retries_exhausted);
  ASSERT_EQ(raise.count(), 6);
}

TEST(RetryModuleTests, failOnRetriesExceededBackoff) {
  Raise raise(6);
  std::function<int8_t()> retriable = [&raise]() {
    return raise.call<int8_t>(10);
  };

  ExponentialBackoffPolicyFactory policyFactory(1ms, 25ms, 5);
  ASSERT_THROW(
      RetryModule::withRetry(retriable, policyFactory.getRetryPolicy()),
      retries_exhausted);
  ASSERT_EQ(raise.count(), 6);
}

TEST(RetryModuleTests, failOnRetriesExceededTotalBackoff) {
  Raise raise(100);
  std::function<int8_t()> retriable = [&raise]() {
    return raise.call<int8_t>(10);
  };

  ExponentialBackoffPolicyFactory policyFactory(1ms, 25ms, 100, 50ms);
  auto start = std::chrono::steady_clock::now();
  ASSERT_THROW(
      RetryModule::withRetry(retriable, policyFactory.getRetryPolicy()),
      retries_exhausted);
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);
  ASSERT_LT(raise.count(), 50);
  ASSERT_GT(duration.count(), 50);
}

TEST(RetryModuleTests, defineDifferentUnit) {
  Raise raise(1);
  std::function<int8_t()> retriable = [&raise]() {
    return raise.call<int8_t>(11);
  };

  KAttemptsPolicyFactory policyFactory({10000us, 0.001s});
  int8_t result =
      RetryModule::withRetry(retriable, policyFactory.getRetryPolicy());
  ASSERT_EQ(result, 11);
  ASSERT_EQ(raise.count(), 2);
}

TEST(RetryModuleTests, testJitter) {
  ExponentialBackoffPolicyFactory policyFactory(1ms, 25ms, 100);
  auto policy = policyFactory.getRetryPolicy();
  float nextWait = RetryDuration(1ms).count();
  RetryDuration wait = RetryDuration(0ms);
  for (auto i = 0; i < 1000; i++) {
    auto nextWaitTime = policy->nextWaitTime();
    if (!nextWaitTime.has_value()) {
      break;
    }
    wait = wait + nextWaitTime.value();
    nextWait +=
        std::min((float)2.0 * nextWait, (float)RetryDuration(25ms).count());
  }

  ASSERT_GT(wait.count(), nextWait);
}

TEST(RetryModuleTests, exponentialBackOffCountExecutionTime) {
  Raise raise(1);
  auto retriable = [&raise]() {
    std::this_thread::sleep_for(20ms);
    return raise.call<int8_t>(10);
  };
  ExponentialBackoffPolicyFactory policyFactory(1ms, 1ms, 5, 10ms, true);
  ASSERT_THROW(
      RetryModule::withRetry(retriable, policyFactory.getRetryPolicy()),
      retries_exhausted);
}
