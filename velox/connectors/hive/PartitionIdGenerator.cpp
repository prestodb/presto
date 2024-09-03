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

#include "velox/connectors/hive/HivePartitionUtil.h"
#include "velox/dwio/catalog/fbhive/FileUtils.h"

using namespace facebook::velox::dwio::catalog::fbhive;

namespace facebook::velox::connector::hive {

PartitionIdGenerator::PartitionIdGenerator(
    const RowTypePtr& inputType,
    std::vector<column_index_t> partitionChannels,
    uint32_t maxPartitions,
    memory::MemoryPool* pool,
    bool partitionPathAsLowerCase)
    : partitionChannels_(std::move(partitionChannels)),
      maxPartitions_(maxPartitions),
      partitionPathAsLowerCase_(partitionPathAsLowerCase) {
  VELOX_USER_CHECK(
      !partitionChannels_.empty(), "There must be at least one partition key.");
  for (auto channel : partitionChannels_) {
    hashers_.emplace_back(
        exec::VectorHasher::create(inputType->childAt(channel), channel));
  }

  std::vector<TypePtr> partitionKeyTypes;
  std::vector<std::string> partitionKeyNames;
  for (auto channel : partitionChannels_) {
    VELOX_USER_CHECK(
        exec::VectorHasher::typeKindSupportsValueIds(
            inputType->childAt(channel)->kind()),
        "Unsupported partition type: {}.",
        inputType->childAt(channel)->toString());
    partitionKeyTypes.push_back(inputType->childAt(channel));
    partitionKeyNames.push_back(inputType->nameOf(channel));
  }

  partitionValues_ = BaseVector::create<RowVector>(
      ROW(std::move(partitionKeyNames), std::move(partitionKeyTypes)),
      maxPartitions_,
      pool);
  for (auto& key : partitionValues_->children()) {
    key->resize(maxPartitions_);
  }
}

void PartitionIdGenerator::run(
    const RowVectorPtr& input,
    raw_vector<uint64_t>& result) {
  const auto numRows = input->size();
  result.resize(numRows);

  // Compute value IDs using VectorHashers and store these in 'result'.
  computeValueIds(input, result);

  // Convert value IDs in 'result' into partition IDs using partitionIds
  // mapping. Update 'result' in place.

  // TODO Optimize common use case where all records belong to the same
  // partition. VectorHashers keep track of the number of unique values, hence,
  // we can find out if there is only one unique value for each partition key.
  for (auto i = 0; i < numRows; ++i) {
    auto valueId = result[i];
    auto it = partitionIds_.find(valueId);
    if (it != partitionIds_.end()) {
      result[i] = it->second;
    } else {
      uint64_t nextPartitionId = partitionIds_.size();
      VELOX_USER_CHECK_LT(
          nextPartitionId,
          maxPartitions_,
          "Exceeded limit of {} distinct partitions.",
          maxPartitions_);

      partitionIds_.emplace(valueId, nextPartitionId);
      savePartitionValues(nextPartitionId, input, i);

      result[i] = nextPartitionId;
    }
  }
}

std::string PartitionIdGenerator::partitionName(uint64_t partitionId) const {
  return FileUtils::makePartName(
      extractPartitionKeyValues(partitionValues_, partitionId),
      partitionPathAsLowerCase_);
}

void PartitionIdGenerator::computeValueIds(
    const RowVectorPtr& input,
    raw_vector<uint64_t>& valueIds) {
  allRows_.resize(input->size());
  allRows_.setAll();

  bool rehash = false;
  for (auto& hasher : hashers_) {
    // NOTE: for boolean column type, computeValueIds() always returns true and
    // this might cause problem in case of multiple boolean partition columns as
    // we might not set the multiplier properly.
    auto partitionVector = input->childAt(hasher->channel())->loadedVector();
    hasher->decode(*partitionVector, allRows_);
    if (!hasher->computeValueIds(allRows_, valueIds)) {
      rehash = true;
    }
  }

  if (!rehash && hasMultiplierSet_) {
    return;
  }

  uint64_t multiplier = 1;
  for (auto& hasher : hashers_) {
    hasMultiplierSet_ = true;
    multiplier = hasher->typeKind() == TypeKind::BOOLEAN
        ? hasher->enableValueRange(multiplier, 50)
        : hasher->enableValueIds(multiplier, 50);

    VELOX_CHECK_NE(
        multiplier,
        exec::VectorHasher::kRangeTooLarge,
        "Number of requested IDs is out of range.");
  }

  for (auto& hasher : hashers_) {
    const bool ok = hasher->computeValueIds(allRows_, valueIds);
    VELOX_CHECK(ok);
  }

  updateValueToPartitionIdMapping();
}

void PartitionIdGenerator::updateValueToPartitionIdMapping() {
  if (partitionIds_.empty()) {
    return;
  }

  const auto numPartitions = partitionIds_.size();

  partitionIds_.clear();

  raw_vector<uint64_t> newValueIds(numPartitions);
  SelectivityVector rows(numPartitions);
  for (auto i = 0; i < hashers_.size(); ++i) {
    auto& hasher = hashers_[i];
    hasher->decode(*partitionValues_->childAt(i), rows);
    const bool ok = hasher->computeValueIds(rows, newValueIds);
    VELOX_CHECK(ok);
  }

  for (auto i = 0; i < numPartitions; ++i) {
    partitionIds_.emplace(newValueIds[i], i);
  }
}

void PartitionIdGenerator::savePartitionValues(
    uint64_t partitionId,
    const RowVectorPtr& input,
    vector_size_t row) {
  for (auto i = 0; i < partitionChannels_.size(); ++i) {
    auto channel = partitionChannels_[i];
    partitionValues_->childAt(i)->copy(
        input->childAt(channel).get(), partitionId, row, 1);
  }
}

} // namespace facebook::velox::connector::hive
