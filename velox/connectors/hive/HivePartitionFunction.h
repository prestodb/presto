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

  HivePartitionFunction(
      int numBuckets,
      std::vector<column_index_t> keyChannels,
      const std::vector<VectorPtr>& constValues = {})
      : HivePartitionFunction(
            numBuckets,
            {},
            std::move(keyChannels),
            constValues) {}

  ~HivePartitionFunction() override = default;

  std::optional<uint32_t> partition(
      const RowVector& input,
      std::vector<uint32_t>& partitions) override;

 private:
  // Precompute single value hive hash for a constant partition key.
  void precompute(const BaseVector& value, size_t column_index_t);

  void hash(
      const DecodedVector& values,
      TypeKind typeKind,
      const SelectivityVector& rows,
      bool mix,
      std::vector<uint32_t>& hashes,
      size_t poolIndex);

  template <TypeKind kind>
  void hashTyped(
      const DecodedVector& /* values */,
      const SelectivityVector& /* rows */,
      bool /* mix */,
      std::vector<uint32_t>& /* hashes */,
      size_t /* poolIndex */) {
    VELOX_UNSUPPORTED(
        "Hive partitioning function doesn't support {} type",
        TypeTraits<kind>::name);
  }

  // Helper functions to retrieve reusable memory from pools.
  DecodedVector& getDecodedVector(size_t poolIndex = 0);
  SelectivityVector& getRows(size_t poolIndex = 0);
  std::vector<uint32_t>& getHashes(size_t poolIndex = 0);

  const int numBuckets_;
  const std::vector<int> bucketToPartition_;
  const std::vector<column_index_t> keyChannels_;

  // Pools of reusable memory.
  std::vector<std::unique_ptr<std::vector<uint32_t>>> hashesPool_;
  std::vector<std::unique_ptr<SelectivityVector>> rowsPool_;
  std::vector<std::unique_ptr<DecodedVector>> decodedVectorsPool_;
  // Precomputed hashes for constant partition keys (one per key).
  std::vector<uint32_t> precomputedHashes_;
};
} // namespace facebook::velox::connector::hive
