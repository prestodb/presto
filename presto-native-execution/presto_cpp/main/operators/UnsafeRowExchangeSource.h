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

#include "presto_cpp/main/operators/ShuffleWrite.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

class UnsafeRowExchangeSource : public velox::exec::ExchangeSource {
 public:
  UnsafeRowExchangeSource(
      const std::string& taskId,
      int destination,
      std::shared_ptr<velox::exec::ExchangeQueue> queue,
      ShuffleInterface* shuffle,
      velox::memory::MemoryPool* pool)
      : ExchangeSource(taskId, destination, queue, pool), shuffle_(shuffle) {}

  bool shouldRequestLocked() override {
    return !atEnd_;
  }

  void request() override;

  void close() override {}

 private:
  ShuffleInterface* shuffle_;
};
} // namespace facebook::presto::operators