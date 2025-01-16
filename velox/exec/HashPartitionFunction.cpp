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

#define XXH_INLINE_ALL
#include <xxhash.h> // @manual=third-party//xxHash:xxhash

namespace facebook::velox::exec {
namespace {
// Gets the hash value for local exchange with given 'rawHash'. 'rawHash'
// is the value computed by this hash function which is used for remote
// shuffle across stages like for Prestissimo.
static inline uint32_t localExchangeHash(uint32_t rawHash) {
  // Mix the bits so we don't use the same hash used to distribute between
  // stages.
  bits::reverseBits(reinterpret_cast<uint8_t*>(&rawHash), sizeof(rawHash));
  return XXH32(&rawHash, sizeof(rawHash), 0);
}
} // namespace

HashPartitionFunction::HashPartitionFunction(
    bool localExchange,
    int numPartitions,
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<VectorPtr>& constValues)
    : localExchange_{localExchange}, numPartitions_{numPartitions} {
  init(inputType, keyChannels, constValues);
}

HashPartitionFunction::HashPartitionFunction(
    const HashBitRange& hashBitRange,
    const RowTypePtr& inputType,
    const std::vector<column_index_t>& keyChannels,
    const std::vector<VectorPtr>& constValues)
    : localExchange_{false},
      numPartitions_{hashBitRange.numPartitions()},
      hashBitRange_(hashBitRange) {
  VELOX_CHECK_GT(hashBitRange.numPartitions(), 0);
  VELOX_CHECK(!keyChannels.empty());
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

std::optional<uint32_t> HashPartitionFunction::partition(
    const RowVector& input,
    std::vector<uint32_t>& partitions) {
  if (hashers_.empty()) {
    return 0u;
  }

  const auto size = input.size();
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
    if (localExchange_) {
      for (auto i = 0; i < size; ++i) {
        partitions[i] = hashBitRange_->partition(localExchangeHash(hashes_[i]));
      }
    } else {
      for (auto i = 0; i < size; ++i) {
        partitions[i] = hashBitRange_->partition(hashes_[i]);
      }
    }
  } else {
    if (localExchange_) {
      for (auto i = 0; i < size; ++i) {
        partitions[i] = localExchangeHash(hashes_[i]) % numPartitions_;
      }
    } else {
      for (auto i = 0; i < size; ++i) {
        partitions[i] = hashes_[i] % numPartitions_;
      }
    }
  }

  return std::nullopt;
}

std::unique_ptr<core::PartitionFunction> HashPartitionFunctionSpec::create(
    int numPartitions,
    bool localExchange) const {
  return std::make_unique<exec::HashPartitionFunction>(
      localExchange, numPartitions, inputType_, keyChannels_, constValues_);
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
