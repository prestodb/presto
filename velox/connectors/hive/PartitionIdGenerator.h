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

#include "velox/exec/VectorHasher.h"

namespace facebook::velox::connector::hive {
/// Generate sequential integer IDs for distinct partition values, which could
/// be used as vector index. Only single partition key is supported at the
/// moment.
class PartitionIdGenerator {
 public:
  /// @param inputType RowType of the input.
  /// @param partitionChannels Channels of partition keys in the input
  /// RowVector.
  /// @param maxPartitions The max number of distinct partitions.
  PartitionIdGenerator(
      const RowTypePtr& inputType,
      std::vector<column_index_t> partitionChannels,
      uint32_t maxPartitions);

  /// Generate sequential partition IDs for input vector.
  /// @param input Input RowVector.
  /// @param result Generated integer IDs indexed by input row number.
  void run(const RowVectorPtr& input, raw_vector<uint64_t>& result);

  /// Return the maximum partition ID generated for the most recent input.
  uint64_t recentMaxPartitionId() const {
    return recentMaxId_;
  }

 private:
  static constexpr const int32_t kHasherReservePct = 20;

  const std::vector<column_index_t> partitionChannels_;

  const uint32_t maxPartitions_;

  std::unique_ptr<exec::VectorHasher> hasher_;

  // Maximum partition ID generated for the most recent input.
  uint64_t recentMaxId_ = 0;

  // Maximum partition ID generated for all inputs received so far.
  uint64_t maxId_ = 0;

  // All rows are set valid to compute partition IDs for all input rows.
  SelectivityVector allRows_;
};

} // namespace facebook::velox::connector::hive
