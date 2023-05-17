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
  init(inputType, keyChannels, constValues);
}

HashPartitionFunction::HashPartitionFunction(
    const HashBitRange& hashBitRange,
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<VectorPtr>& constValues)
    : numPartitions_{hashBitRange.numPartitions()},
      hashBitRange_(hashBitRange) {
  init(inputType, keyChannels, constValues);
}

void HashPartitionFunction::init(
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<VectorPtr>& constValues) {
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
  if (hashBitRange_.has_value()) {
    for (auto i = 0; i < size; ++i) {
      partitions[i] = hashBitRange_->partition(hashes_[i]);
    }
  } else {
    for (auto i = 0; i < size; ++i) {
      partitions[i] = hashes_[i] % numPartitions_;
    }
  }
}

std::unique_ptr<core::PartitionFunction> HashPartitionFunctionSpec::create(
    int numPartitions) const {
  return std::make_unique<exec::HashPartitionFunction>(
      numPartitions, inputType_, keyChannels_, constValues_);
}

std::string HashPartitionFunctionSpec::toString() const {
  std::ostringstream keys;
  size_t constIndex = 0;
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    if (i > 0) {
      keys << ", ";
    }
    auto channel = keyChannels_[i];
    if (channel == kConstantChannel) {
      keys << "\"" << constValues_[constIndex++]->toString(0) << "\"";
    } else {
      keys << inputType_->nameOf(channel);
    }
  }

  return fmt::format("HASH({})", keys.str());
}

folly::dynamic HashPartitionFunctionSpec::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HashPartitionFunctionSpec";
  obj["inputType"] = inputType_->serialize();
  obj["keyChannels"] = ISerializable::serialize(keyChannels_);
  std::vector<velox::core::ConstantTypedExpr> constValues;
  constValues.reserve(constValues_.size());
  for (const auto& value : constValues_) {
    VELOX_CHECK_NOT_NULL(value);
    constValues.emplace_back(value);
  }
  obj["constants"] = ISerializable::serialize(constValues);
  return obj;
}

// static
core::PartitionFunctionSpecPtr HashPartitionFunctionSpec::deserialize(
    const folly::dynamic& obj,
    void* context) {
  const auto keys = ISerializable::deserialize<std::vector<column_index_t>>(
      obj["keyChannels"], context);
  const auto constTypeExprs =
      ISerializable::deserialize<std::vector<velox::core::ConstantTypedExpr>>(
          obj["constants"], context);

  auto* pool = static_cast<memory::MemoryPool*>(context);
  std::vector<VectorPtr> constValues;
  constValues.reserve(constTypeExprs.size());
  for (const auto& value : constTypeExprs) {
    constValues.emplace_back(value->toConstantVector(pool));
  }
  return std::make_shared<HashPartitionFunctionSpec>(
      ISerializable::deserialize<RowType>(obj["inputType"]), keys, constValues);
}
} // namespace facebook::velox::exec
