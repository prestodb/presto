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

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace facebook::velox::dwio::common::unit_loader_tools {

class Measure {
 public:
  explicit Measure(const std::function<void(uint64_t)>& blockedOnIoMsCallback)
      : blockedOnIoMsCallback_{blockedOnIoMsCallback},
        startTime_{std::chrono::high_resolution_clock::now()} {}

  Measure(const Measure&) = delete;
  Measure(Measure&&) = delete;
  Measure& operator=(const Measure&) = delete;
  Measure& operator=(Measure&& other) = delete;

  ~Measure() {
    auto timeBlockedOnIo =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - startTime_);
    blockedOnIoMsCallback_(timeBlockedOnIo.count());
  }

 private:
  const std::function<void(uint64_t)>& blockedOnIoMsCallback_;
  const std::chrono::time_point<std::chrono::high_resolution_clock> startTime_;
};

inline std::optional<Measure> measureBlockedOnIo(
    const std::function<void(uint64_t)>& blockedOnIoMsCallback) {
  if (blockedOnIoMsCallback) {
    return std::make_optional<Measure>(blockedOnIoMsCallback);
  }
  return std::nullopt;
}

// This class can create many callbacks that can be distributed to unit loader
// factories. Only when the last created callback is activated, this class will
// emit the original callback.
// If the callbacks created are never activated (because of an exception for
// example), this will still work, since they will emit the signal on the
// destructor.
// The class expects that all callbacks are created before they start emiting
// signals.
class CallbackOnLastSignal {
  class EnsureCallOnlyOnce {
   public:
    EnsureCallOnlyOnce(std::function<void()> cb) : cb_{std::move(cb)} {}

    void callOriginalIfNotCalled() {
      if (cb_) {
        cb_();
        cb_ = nullptr;
      }
    }

    ~EnsureCallOnlyOnce() {
      callOriginalIfNotCalled();
    }

   private:
    std::function<void()> cb_;
  };

  class CallWhenOneInstanceLeft {
   public:
    CallWhenOneInstanceLeft(
        std::shared_ptr<EnsureCallOnlyOnce> callWrapper,
        std::shared_ptr<size_t> factoryExists)
        : factoryExists_{std::move(factoryExists)},
          callOnce_{std::move(callWrapper)} {}

    CallWhenOneInstanceLeft(const CallWhenOneInstanceLeft& other)
        : factoryExists_{other.factoryExists_}, callOnce_{other.callOnce_} {}

    CallWhenOneInstanceLeft(CallWhenOneInstanceLeft&& other) noexcept {
      factoryExists_ = std::move(other.factoryExists_);
      callOnce_ = std::move(other.callOnce_);
    }

    void operator()() {
      if (!callOnce_) {
        return;
      }
      // If this is the last callback created out callOnce_, call the
      // original callback.
      if (callOnce_.use_count() <= (1 + *factoryExists_)) {
        callOnce_->callOriginalIfNotCalled();
      }
      callOnce_ = nullptr;
    }

    ~CallWhenOneInstanceLeft() {
      (*this)();
    }

   private:
    std::shared_ptr<size_t> factoryExists_;
    std::shared_ptr<EnsureCallOnlyOnce> callOnce_;
  };

 public:
  CallbackOnLastSignal(std::function<void()> cb)
      : factoryExists_{std::make_shared<size_t>(1)},
        callWrapper_{std::make_shared<EnsureCallOnlyOnce>(std::move(cb))} {}

  ~CallbackOnLastSignal() {
    *factoryExists_ = 0;
  }

  std::function<void()> getCallback() const {
    return CallWhenOneInstanceLeft{callWrapper_, factoryExists_};
  }

 private:
  std::shared_ptr<size_t> factoryExists_;
  std::shared_ptr<EnsureCallOnlyOnce> callWrapper_;
};

} // namespace facebook::velox::dwio::common::unit_loader_tools
