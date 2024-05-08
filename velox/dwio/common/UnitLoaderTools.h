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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "folly/synchronization/CallOnce.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::dwio::common::unit_loader_tools {

// This class can create many callbacks that can be distributed to unit loader
// factories. Only when the last created callback is activated, this class will
// emit the original callback.
// If the callback objects created are never explicitly called (because of an
// exception for example), the callback object will do the call in the
// destructor, guaranteeting the call.
class CallbackOnLastSignal {
  class Callable {
   public:
    virtual ~Callable() {}
    virtual void call() = 0;
  };

  class CallableFunction : public Callable {
   public:
    explicit CallableFunction(std::function<void()> cb) : cb_{std::move(cb)} {}

    void call() override {
      if (cb_) {
        // Could be null
        cb_();
      }
    }

   private:
    std::function<void()> cb_;
  };

  // This class will ensure that the contained callback is only called once.
  class CallOnce : public Callable {
   public:
    explicit CallOnce(std::shared_ptr<Callable> cb) : cb_{std::move(cb)} {}

    CallOnce(const CallOnce& other) = delete;
    CallOnce(CallOnce&& other) = delete;
    CallOnce& operator=(const CallOnce& other) = delete;
    CallOnce& operator=(CallOnce&& other) noexcept = delete;

    void call() override {
      folly::call_once(called_, [&]() { cb_->call(); });
    }

   private:
    std::shared_ptr<Callable> cb_;
    folly::once_flag called_;
  };

  // This class will ensure that only the call from the last caller will go
  // through.
  class CallOnCountZero : public Callable {
   public:
    CallOnCountZero(
        std::shared_ptr<std::atomic_size_t> callsLeft,
        std::shared_ptr<Callable> cb)
        : callsLeft_{std::move(callsLeft)}, cb_{std::move(cb)} {}

    CallOnCountZero(const CallOnCountZero& other) = delete;
    CallOnCountZero(CallOnCountZero&& other) = delete;
    CallOnCountZero& operator=(const CallOnCountZero& other) = delete;
    CallOnCountZero& operator=(CallOnCountZero&& other) noexcept = delete;

    void call() override {
      if (*callsLeft_ > 0) {
        --*(callsLeft_);
      }
      if (*callsLeft_ == 0) {
        cb_->call();
      }
    }

   private:
    std::shared_ptr<std::atomic_size_t> callsLeft_;
    std::shared_ptr<Callable> cb_;
  };

  // This class will ensure that the contained callback is called when the
  // operator() is invoked, or when the object is destructed, whatever comes
  // first.
  class EnsureCall : public Callable {
   public:
    explicit EnsureCall(std::shared_ptr<Callable> cb)
        : cb_{std::make_shared<CallOnce>(std::move(cb))} {}

    EnsureCall(const EnsureCall& other) = delete;
    EnsureCall(EnsureCall&& other) = delete;
    EnsureCall& operator=(const EnsureCall& other) = delete;
    EnsureCall& operator=(EnsureCall&& other) noexcept = delete;

    ~EnsureCall() override {
      cb_->call();
    }

    void call() override {
      cb_->call();
    }

   private:
    std::shared_ptr<Callable> cb_;
  };

  class CountCaller {
   public:
    CountCaller(
        std::shared_ptr<Callable> cb,
        std::shared_ptr<std::atomic_size_t> callsLeft)
        : cb_{std::move(cb)} {
      ++(*callsLeft);
    }

    void operator()() {
      cb_->call();
    }

   private:
    std::shared_ptr<Callable> cb_;
  };

 public:
  explicit CallbackOnLastSignal(std::function<void()> cb)
      : callsLeft_{std::make_shared<std::atomic_size_t>(0)},
        cb_{cb ? std::make_shared<CallOnCountZero>(
                     callsLeft_,
                     std::make_shared<CallOnce>(std::make_shared<EnsureCall>(
                         std::make_shared<CallableFunction>(std::move(cb)))))
               : nullptr} {}

  std::function<void()> getCallback() const {
    if (!cb_) {
      return nullptr;
    }
    return CountCaller{
        std::make_shared<CallOnce>(std::make_shared<EnsureCall>(cb_)),
        callsLeft_};
  }

 private:
  std::shared_ptr<std::atomic_size_t> callsLeft_;
  std::shared_ptr<Callable> cb_;
};

template <typename NumRowsIter>
std::pair<uint32_t, uint64_t>
howMuchToSkip(uint64_t rowsToSkip, NumRowsIter begin, NumRowsIter end) {
  uint64_t rowsLeftToSkip = rowsToSkip;
  uint32_t unitsToSkip = 0;
  for (NumRowsIter it = begin; it != end; ++it) {
    const auto rowsInUnit = *it;
    if (rowsLeftToSkip < rowsInUnit) {
      return {unitsToSkip, rowsLeftToSkip};
    }
    rowsLeftToSkip -= rowsInUnit;
    ++unitsToSkip;
  }

  VELOX_CHECK_EQ(
      rowsLeftToSkip,
      0,
      "Can't skip more rows than all the rows in all the units");

  return {unitsToSkip, rowsLeftToSkip};
}

} // namespace facebook::velox::dwio::common::unit_loader_tools
