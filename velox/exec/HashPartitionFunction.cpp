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
#include <velox/exec/HashPartitionFunction.h>
#include <velox/exec/VectorHasher.h>

namespace facebook::velox::exec {
HashPartitionFunction::HashPartitionFunction(
    int numPartitions,
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<VectorPtr>& constValues)
    : numPartitions_{numPartitions} {
  hashers_.reserve(keyChannels.size());
  size_t constChannel{0};
  for (const auto channel : keyChannels) {
    if (channel != kConstantChannel) {
      hashers_.emplace_back(
          VectorHasher::create(inputType->childAt(channel), channel));
    } else {
      const auto& constValue = constValues[constChannel++];
      hashers_.emplace_back(VectorHasher::create(constValue->type(), channel));
      hashers_.back()->precompute(*constValue);
    }
  }
}

void HashPartitionFunction::partition(
    const RowVector& input,
    std::vector<uint32_t>& partitions) {
  auto size = input.size();

  rows_.resize(size);
  rows_.setAll();

  hashes_.resize(size);
  for (auto i = 0; i < hashers_.size(); ++i) {
    auto& hasher = hashers_[i];
    if (hasher->channel() != kConstantChannel) {
      hashers_[i]->decode(*input.childAt(hasher->channel()), rows_);
      hashers_[i]->hash(rows_, i > 0, hashes_);
    } else {
      hashers_[i]->hashPrecomputed(rows_, i > 0, hashes_);
    }
  }

  partitions.resize(size);
  for (auto i = 0; i < size; ++i) {
    partitions[i] = hashes_[i] % numPartitions_;
  }
}
} // namespace facebook::velox::exec
