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

#include <velox/exec/VectorHasher.h>
#include "velox/core/PlanNode.h"

namespace facebook::velox::exec {

/// Assigns batches of rows to different partitions in a round-robin fashion.
class RoundRobinPartitionFunction : public core::PartitionFunction {
 public:
  explicit RoundRobinPartitionFunction(int numPartitions)
      : numPartitions_{numPartitions} {}

  std::optional<uint32_t> partition(
      const RowVector& /*input*/,
      std::vector<uint32_t>& /*partitions*/) override {
    const auto partition = counter_ % numPartitions_;
    ++counter_;
    return partition;
  }

 private:
  const int numPartitions_;
  uint32_t counter_{0};
};

class RoundRobinPartitionFunctionSpec : public core::PartitionFunctionSpec {
 public:
  std::unique_ptr<core::PartitionFunction> create(
      int numPartitions,
      bool /*localExchange*/) const override {
    return std::make_unique<velox::exec::RoundRobinPartitionFunction>(
        numPartitions);
  }

  std::string toString() const override {
    return "ROUND ROBIN";
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("RoundRobinPartitionFunctionSpec");
    return obj;
  }

  static core::PartitionFunctionSpecPtr deserialize(
      const folly::dynamic& obj,
      void* context) {
    return std::make_shared<RoundRobinPartitionFunctionSpec>();
  }
};
} // namespace facebook::velox::exec
