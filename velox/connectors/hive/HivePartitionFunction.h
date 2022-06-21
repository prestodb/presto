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

#include "velox/core/PlanNode.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::connector::hive {

class HivePartitionFunction : public core::PartitionFunction {
 public:
  HivePartitionFunction(
      int numBuckets,
      std::vector<int> bucketToPartition,
      std::vector<column_index_t> keyChannels,
      const std::vector<VectorPtr>& constValues = {});

  ~HivePartitionFunction() override = default;

  void partition(const RowVector& input, std::vector<uint32_t>& partitions)
      override;

 private:
  // Precompute single value hive hash for a constant partition key.
  void precompute(const BaseVector& value, size_t column_index_t);

  const int numBuckets_;
  const std::vector<int> bucketToPartition_;
  const std::vector<column_index_t> keyChannels_;

  // Reusable memory.
  std::vector<uint32_t> hashes_;
  SelectivityVector rows_;
  std::vector<DecodedVector> decodedVectors_;
  // Precomputed hashes for constant partition keys (one per key).
  std::vector<uint32_t> precomputedHashes_;
};
} // namespace facebook::velox::connector::hive
