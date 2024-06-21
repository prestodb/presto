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

#pragma once

#include <folly/Optional.h>
#include <glog/logging.h>
#include <chrono>
#include <exception>
#include <functional>
#include <stdexcept>
#include <thread>
#include <vector>

#include "velox/dwio/common/RandGen.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

class retriable_error : public std::runtime_error {
 public:
  explicit retriable_error(const std::exception& e)
      : std::runtime_error(e.what()) {}
  explicit retriable_error(const std::string& errorMsg)
      : std::runtime_error(errorMsg) {}
};

class retries_canceled : public std::runtime_error {
 public:
  explicit retries_canceled()
      : std::runtime_error("Retries have been canceled.") {}
};

class retries_exhausted : public std::runtime_error {
 public:
  explicit retries_exhausted(const std::exception& e)
      : std::runtime_error(e.what()) {}
};

using RetryDuration =
    std::chrono::duration<float, std::chrono::milliseconds::period>;

namespace retrypolicy {

class IRetryPolicy {
 public:
  virtual ~IRetryPolicy() = default;
  virtual folly::Optional<RetryDuration> nextWaitTime() = 0;
  virtual void start() {}
};

class KAttempts : public IRetryPolicy {
 public:
  explicit KAttempts(std::vector<RetryDuration> durations)
      : index_(0), durations_(std::move(durations)) {}

  folly::Optional<RetryDuration> nextWaitTime() override {
    if (index_ < durations_.size()) {
      return folly::Optional<RetryDuration>(durations_[index_++]);
    } else {
      return folly::Optional<RetryDuration>();
    }
  }

 private:
  size_t index_;
  const std::vector<RetryDuration> durations_;
};

class ExponentialBackoff : public IRetryPolicy {
 public:
  ExponentialBackoff(
      RetryDuration start,
      RetryDuration max,
      uint64_t maxRetries,
      RetryDuration maxTotal,
      bool countExecutionTime)
      : maxWait_(max),
        maxTotal_(maxTotal),
        countExecutionTime_(countExecutionTime),
        nextWait_(start),
        totalWait_(0),
        retriesLeft_(maxRetries) {
    DWIO_ENSURE_LE(start.count(), max.count());
    DWIO_ENSURE(maxTotal_.count() == 0 || maxTotal_.count() > start.count());
  }

  void start() override {
    startTime_ = std::chrono::system_clock::now();
  }

  folly::Optional<RetryDuration> nextWaitTime() override {
    if (retriesLeft_ == 0 || (maxTotal_.count() > 0 && total() >= maxTotal_)) {
      return folly::Optional<RetryDuration>();
    }

    RetryDuration waitTime = nextWait_ + jitter();
    nextWait_ = std::min(nextWait_ + nextWait_, maxWait_);
    --retriesLeft_;
    totalWait_ += waitTime;
    return folly::Optional<RetryDuration>(waitTime);
  }

 private:
  RetryDuration jitter() {
    return RetryDuration(rand_.gen(folly::to<int64_t>(nextWait_.count()) / 2));
  }

  RetryDuration total() const {
    return countExecutionTime_ ? std::chrono::system_clock::now() - startTime_
                               : totalWait_;
  }

  const RetryDuration maxWait_;
  const RetryDuration maxTotal_;
  const bool countExecutionTime_;
  std::chrono::system_clock::time_point startTime_;
  RetryDuration nextWait_;
  RetryDuration totalWait_;
  uint64_t retriesLeft_;
  RandGen rand_;
};

class IRetryPolicyFactory {
 public:
  virtual std::unique_ptr<IRetryPolicy> getRetryPolicy() const = 0;
  virtual ~IRetryPolicyFactory() = default;
};

class KAttemptsPolicyFactory : public IRetryPolicyFactory {
 private:
  std::vector<RetryDuration> durations_;

 public:
  KAttemptsPolicyFactory(std::vector<RetryDuration> durations)
      : durations_{durations} {}

  std::unique_ptr<IRetryPolicy> getRetryPolicy() const {
    return std::make_unique<KAttempts>(durations_);
  }
};

class ExponentialBackoffPolicyFactory : public IRetryPolicyFactory {
 public:
  ExponentialBackoffPolicyFactory(
      RetryDuration start,
      RetryDuration maxWait,
      uint64_t maxRetries = std::numeric_limits<uint64_t>::max(),
      RetryDuration maxTotal = RetryDuration::zero(),
      bool countExecutionTime = false)
      : start_(start),
        maxWait_(maxWait),
        maxRetries_(maxRetries),
        maxTotal_(maxTotal),
        countExecutionTime_(countExecutionTime) {}

  std::unique_ptr<IRetryPolicy> getRetryPolicy() const {
    return std::make_unique<ExponentialBackoff>(
        start_, maxWait_, maxRetries_, maxTotal_, countExecutionTime_);
  }

 private:
  const RetryDuration start_;
  const RetryDuration maxWait_;
  const uint64_t maxRetries_;
  const RetryDuration maxTotal_;
  const bool countExecutionTime_;
};

} // namespace retrypolicy

class RetryModule {
 public:
  template <typename F>
  static auto withRetry(
      F func,
      std::unique_ptr<retrypolicy::IRetryPolicy> policy,
      std::function<bool()> abortFunc = nullptr) {
    policy->start();
    do {
      try {
        // If abort signal is triggered before func, no ops.
        if (abortFunc && abortFunc()) {
          LOG(WARNING) << "Retries canceled.";
          throw retries_canceled();
        }

        return func();
      } catch (const retriable_error& error) {
        LOG(INFO) << "RetryModule caught retriable exception. " << error.what();
        folly::Optional<RetryDuration> wait = policy->nextWaitTime();
        if (wait.has_value()) {
          auto ms = wait.value().count();
          LOG(INFO) << "RetryModule : Waiting for " << ms
                    << "(ms) before retry";
          while (ms > 0) {
            // check abort signal every second.
            if (abortFunc && abortFunc()) {
              LOG(WARNING) << "Retries canceled.";
              throw retries_canceled();
            }

            const float abortCheckInterval = 1000;
            std::this_thread::sleep_for(
                RetryDuration(std::min(ms, abortCheckInterval)));
            ms -= abortCheckInterval;
          }
        } else {
          LOG(WARNING) << "Retries exhausted!!!";
          throw retries_exhausted(error);
        }
      }
    } while (true);
  }
};

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
