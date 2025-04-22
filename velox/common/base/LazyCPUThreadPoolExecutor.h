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

#include <folly/DefaultKeepAliveExecutor.h>
#include <folly/Function.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/DelayedInit.h>

namespace facebook::velox {

/// The folly cpu executor wrapper that lazily creates the executor on first
/// use. This reduces the memory consumption for cases where the executor is not
/// actually used.
class LazyCPUThreadPoolExecutor : public folly::DefaultKeepAliveExecutor {
 public:
  LazyCPUThreadPoolExecutor(size_t numThreads, std::string prefix)
      : numThreads_(numThreads), prefix_(std::move(prefix)) {}

  ~LazyCPUThreadPoolExecutor() override {
    joinKeepAlive();
  }

  // Not copyable or movable
  LazyCPUThreadPoolExecutor(const LazyCPUThreadPoolExecutor&) = delete;
  LazyCPUThreadPoolExecutor& operator=(const LazyCPUThreadPoolExecutor&) =
      delete;
  LazyCPUThreadPoolExecutor(LazyCPUThreadPoolExecutor&&) = delete;
  LazyCPUThreadPoolExecutor& operator=(LazyCPUThreadPoolExecutor&&) = delete;

  void add(folly::Func func) override {
    getInstance().add(std::move(func));
  }

  void addWithPriority(folly::Func func, int8_t priority) override {
    getInstance().addWithPriority(std::move(func), priority);
  }

 private:
  Executor& getInstance() {
    return executor_.try_emplace_with([&]() {
      return folly::CPUThreadPoolExecutor(
          numThreads_, std::make_shared<folly::NamedThreadFactory>(prefix_));
    });
  }

  const size_t numThreads_;
  const std::string prefix_;
  folly::DelayedInit<folly::CPUThreadPoolExecutor> executor_;
};

} // namespace facebook::velox
