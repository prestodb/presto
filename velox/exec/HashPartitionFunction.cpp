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
    RowTypePtr inputType,
    std::vector<ChannelIndex> keyChannels)
    : numPartitions_{numPartitions}, keyChannels_{std::move(keyChannels)} {
  hashers_.reserve(keyChannels_.size());
  for (auto channel : keyChannels_) {
    hashers_.emplace_back(
        VectorHasher::create(inputType->childAt(channel), channel));
  }
}

void HashPartitionFunction::partition(
    const RowVector& input,
    std::vector<uint32_t>& partitions) {
  auto size = input.size();

  rows_.resize(size);
  rows_.setAll();

  hashes_.resize(size);
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    hashers_[i]->hash(*input.childAt(keyChannels_[i]), rows_, i > 0, hashes_);
  }

  partitions.resize(size);
  for (auto i = 0; i < size; ++i) {
    partitions[i] = hashes_[i] % numPartitions_;
  }
}
} // namespace facebook::velox::exec
