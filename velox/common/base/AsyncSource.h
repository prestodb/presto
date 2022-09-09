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

#include <folly/Unit.h>
#include <folly/executors/QueuedImmediateExecutor.h>
#include <folly/futures/Future.h>
#include <functional>
#include <memory>

#include "velox/common/base/Exceptions.h"
#include "velox/common/future/VeloxPromise.h"

namespace facebook::velox {

// A future-like object that prefabricates Items on an executor and
// allows consumer threads to pick items as they are ready. If the
// consumer needs the item before the executor started making it, the
// consumer will make it instead. If multiple consumers request the
// same item, exactly one gets it. Propagates exceptions to the
// consumer.
template <typename Item>
class AsyncSource {
 public:
  explicit AsyncSource(std::function<std::unique_ptr<Item>()> make)
      : make_(make) {}

  // Makes an item if it is not already made. To be called on a background
  // executor.
  void prepare() {
    std::function<std::unique_ptr<Item>()> make = nullptr;
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (!make_) {
        return;
      }
      making_ = true;
      std::swap(make, make_);
    }
    std::unique_ptr<Item> item;
    try {
      item = make();
    } catch (std::exception& e) {
      std::lock_guard<std::mutex> l(mutex_);
      exception_ = std::current_exception();
    }
    std::unique_ptr<ContinuePromise> promise;
    {
      std::lock_guard<std::mutex> l(mutex_);
      VELOX_CHECK_NULL(item_);
      if (FOLLY_LIKELY(exception_ == nullptr)) {
        item_ = std::move(item);
      }
      making_ = false;
      promise.swap(promise_);
    }
    if (promise != nullptr) {
      promise->setValue();
    }
  }

  // Returns the item to the first caller and nullptr to subsequent callers. If
  // the item is preparing on the executor, waits for the item and otherwise
  // makes it on the caller thread.
  std::unique_ptr<Item> move() {
    std::function<std::unique_ptr<Item>()> make = nullptr;
    ContinueFuture wait;
    {
      std::lock_guard<std::mutex> l(mutex_);
      // 'making_' can be read atomically, 'exception' maybe not. So test
      // 'making' so as not to read half-assigned 'exception_'.
      if (!making_ && exception_) {
        std::rethrow_exception(exception_);
      }
      if (item_) {
        return std::move(item_);
      }
      if (promise_) {
        // Somebody else is now waiting for the item to be made.
        return nullptr;
      }
      if (making_) {
        promise_ = std::make_unique<ContinuePromise>();
        wait = promise_->getSemiFuture();
      } else {
        if (!make_) {
          return nullptr;
        }
        std::swap(make, make_);
      }
    }
    // Outside of mutex_.
    if (make) {
      try {
        return make();
      } catch (const std::exception& e) {
        std::lock_guard<std::mutex> l(mutex_);
        exception_ = std::current_exception();
        throw;
      }
    }
    auto& exec = folly::QueuedImmediateExecutor::instance();
    std::move(wait).via(&exec).wait();
    std::lock_guard<std::mutex> l(mutex_);
    if (exception_) {
      std::rethrow_exception(exception_);
    }
    return std::move(item_);
  }

  // If true, move() will not block. But there is no guarantee that somebody
  // else will not get the item first.
  bool hasValue() const {
    return item_ != nullptr || exception_ != nullptr;
  }

 private:
  std::mutex mutex_;
  // True if 'prepare() is making the item.
  bool making_{false};
  std::unique_ptr<ContinuePromise> promise_;
  std::unique_ptr<Item> item_;
  std::function<std::unique_ptr<Item>()> make_;
  std::exception_ptr exception_;
};
} // namespace facebook::velox
