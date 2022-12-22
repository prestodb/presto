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

#include "velox/connectors/hive/PartitionIdGenerator.h"

namespace facebook::velox::connector::hive {

PartitionIdGenerator::PartitionIdGenerator(
    const RowTypePtr& inputType,
    std::vector<column_index_t> partitionChannels,
    uint32_t maxPartitions)
    : partitionChannels_(std::move(partitionChannels)),
      maxPartitions_(maxPartitions) {
  VELOX_USER_CHECK_EQ(
      partitionChannels_.size(),
      1,
      "Multiple partition keys are not supported yet.");
  hasher_ = exec::VectorHasher::create(
      inputType->childAt(partitionChannels_[0]), partitionChannels_[0]);
}

void PartitionIdGenerator::run(
    const RowVectorPtr& input,
    raw_vector<uint64_t>& result) {
  result.resize(input->size());
  allRows_.resize(input->size());
  allRows_.setAll();

  auto partitionVector = input->childAt(hasher_->channel())->loadedVector();
  hasher_->decode(*partitionVector, allRows_);

  if (!hasher_->computeValueIds(allRows_, result)) {
    uint64_t range = hasher_->enableValueIds(1, kHasherReservePct);
    VELOX_CHECK_NE(
        range,
        exec::VectorHasher::kRangeTooLarge,
        "Number of requested IDs is out of range.");

    VELOX_CHECK(
        hasher_->computeValueIds(allRows_, result),
        "Cannot assign new value IDs.");
  }

  recentMaxId_ = *std::max_element(result.begin(), result.end());
  maxId_ = std::max(maxId_, recentMaxId_);

  VELOX_USER_CHECK_LE(
      maxId_, maxPartitions_, "Exceeded limit of distinct partitions.");
}

} // namespace facebook::velox::connector::hive
