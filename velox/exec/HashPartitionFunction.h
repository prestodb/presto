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

// Describes a bit range inside a 64 bit hash number for use in
// partitioning data.
class HashBitRange {
 public:
  HashBitRange(uint8_t begin, uint8_t end)
      : begin_(begin), end_(end), fieldMask_(bits::lowMask(end - begin)) {}
  HashBitRange() : HashBitRange(0, 0) {}

  int32_t partition(uint64_t hash, int32_t numPartitions) const {
    int32_t number = (hash >> begin_) & fieldMask_;
    return number < numPartitions ? number : -1;
  }

  int32_t partition(uint64_t hash) const {
    return (hash >> begin_) & fieldMask_;
  }

  int32_t numPartitions() const {
    return 1 << (end_ - begin_);
  }

 private:
  // Low bit number of hash number bit range.
  const uint8_t begin_;
  // Bit number of first bit above the hash number bit range.
  const uint8_t end_;

  const uint64_t fieldMask_;
};

class HashPartitionFunction : public core::PartitionFunction {
 public:
  HashPartitionFunction(
      int numPartitions,
      const RowTypePtr& inputType,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<VectorPtr>& constValues = {});

  HashPartitionFunction(
      const HashBitRange& hashBitRange,
      const RowTypePtr& inputType,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<VectorPtr>& constValues = {});

  ~HashPartitionFunction() override = default;

  void partition(const RowVector& input, std::vector<uint32_t>& partitions)
      override;

 private:
  void init(
      const RowTypePtr& inputType,
      const std::vector<column_index_t>& keyChannels,
      const std::vector<VectorPtr>& constValues);

  const int numPartitions_;
  const std::optional<HashBitRange> hashBitRange_ = std::nullopt;
  std::vector<std::unique_ptr<VectorHasher>> hashers_;

  // Reusable memory.
  SelectivityVector rows_;
  raw_vector<uint64_t> hashes_;
};
} // namespace facebook::velox::exec
