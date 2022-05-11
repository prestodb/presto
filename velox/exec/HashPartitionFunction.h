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

class HashPartitionFunction : public core::PartitionFunction {
 public:
  HashPartitionFunction(
      int numPartitions,
      const RowTypePtr& inputType,
      const std::vector<ChannelIndex>& keyChannels,
      const std::vector<std::shared_ptr<BaseVector>>& constValues = {});

  ~HashPartitionFunction() override = default;

  void partition(const RowVector& input, std::vector<uint32_t>& partitions)
      override;

 private:
  const int numPartitions_;
  std::vector<std::unique_ptr<VectorHasher>> hashers_;

  // Reusable memory.
  SelectivityVector rows_;
  raw_vector<uint64_t> hashes_;
};
} // namespace facebook::velox::exec
