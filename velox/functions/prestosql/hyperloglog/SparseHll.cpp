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
#include "velox/functions/prestosql/hyperloglog/SparseHll.h"
#include "velox/common/base/IOUtils.h"
#include "velox/functions/prestosql/hyperloglog/HllUtils.h"

namespace facebook::velox::aggregate::hll {
namespace {
const int8_t kValueBitLength = 6;
const int8_t kIndexBitLength = 26;

inline uint32_t encode(uint32_t index, uint32_t value) {
  return index << kValueBitLength | value;
}

inline uint32_t decodeIndex(uint32_t entry) {
  return entry >> kValueBitLength;
}

inline uint32_t decodeValue(uint32_t entry) {
  return entry & ((1 << kValueBitLength) - 1);
}

int searchIndex(
    uint32_t index,
    const std::vector<uint32_t, StlAllocator<uint32_t>>& entries) {
  int low = 0;
  int high = entries.size() - 1;

  while (low <= high) {
    int middle = (low + high) >> 1;

    auto middleIndex = decodeIndex(entries[middle]);

    if (index > middleIndex) {
      low = middle + 1;
    } else if (index < middleIndex) {
      high = middle - 1;
    } else {
      return middle;
    }
  }

  return -(low + 1);
}

common::InputByteStream initializeInputStream(const char* serialized) {
  common::InputByteStream stream(serialized);

  auto version = stream.read<int8_t>();
  VELOX_CHECK_EQ(kPrestoSparseV2, version);

  // Skip indexBitLength.
  stream.read<int8_t>();
  return stream;
}
} // namespace

bool SparseHll::insertHash(uint64_t hash) {
  auto index = computeIndex(hash, kIndexBitLength);
  auto value = computeValue(hash, kIndexBitLength);

  auto entry = encode(index, value);
  auto position = searchIndex(index, entries_);

  if (position >= 0) {
    if (decodeValue(entries_[position]) < value) {
      entries_[position] = entry;
    }
  } else {
    auto insertionPosition = -position - 1;
    entries_.insert(entries_.begin() + insertionPosition, entry);
  }

  return entries_.size() >= softNumEntriesLimit_;
}

int64_t SparseHll::cardinality() const {
  // Estimate the cardinality using linear counting over the theoretical
  // 2^kIndexBitLength buckets available due to the fact that we're
  // recording the raw leading kIndexBitLength of the hash. This produces
  // much better precision while in the sparse regime.
  static const int kTotalBuckets = 1 << kIndexBitLength;

  int zeroBuckets = kTotalBuckets - entries_.size();
  return std::round(linearCounting(zeroBuckets, kTotalBuckets));
}

// static
int64_t SparseHll::cardinality(const char* serialized) {
  static const int kTotalBuckets = 1 << kIndexBitLength;

  auto stream = initializeInputStream(serialized);
  auto size = stream.read<int16_t>();

  int zeroBuckets = kTotalBuckets - size;
  return std::round(linearCounting(zeroBuckets, kTotalBuckets));
}

void SparseHll::serialize(int8_t indexBitLength, char* output) const {
  common::OutputByteStream stream(output);
  stream.appendOne(kPrestoSparseV2);
  stream.appendOne(indexBitLength);
  stream.appendOne((int16_t)entries_.size());
  for (auto entry : entries_) {
    stream.appendOne(entry);
  }
}

// static
std::string SparseHll::serializeEmpty(int8_t indexBitLength) {
  static const size_t kSize = 4;

  std::string serialized;
  serialized.resize(kSize);

  common::OutputByteStream stream(serialized.data());
  stream.appendOne(kPrestoSparseV2);
  stream.appendOne(indexBitLength);
  stream.appendOne((int16_t)0);
  return serialized;
}

// static
bool SparseHll::canDeserialize(const char* input) {
  return *reinterpret_cast<const int8_t*>(input) == kPrestoSparseV2;
}

int32_t SparseHll::serializedSize() const {
  return 1 /* version */
      + 1 /* indexBitLength */
      + 2 /* number of entries */
      + entries_.size() * 4;
}

int32_t SparseHll::inMemorySize() const {
  return sizeof(uint32_t) * entries_.size();
}

SparseHll::SparseHll(const char* serialized, HashStringAllocator* allocator)
    : entries_{StlAllocator<uint32_t>(allocator)} {
  auto stream = initializeInputStream(serialized);

  auto size = stream.read<int16_t>();
  entries_.resize(size);
  for (auto i = 0; i < size; i++) {
    entries_[i] = stream.read<uint32_t>();
  }
}

void SparseHll::mergeWith(const SparseHll& other) {
  mergeWith(other.entries_.size(), other.entries_.data());
}

void SparseHll::mergeWith(const char* serialized) {
  auto stream = initializeInputStream(serialized);

  auto size = stream.read<int16_t>();
  mergeWith(
      size, reinterpret_cast<const uint32_t*>(serialized + stream.offset()));
}

void SparseHll::mergeWith(size_t otherSize, const uint32_t* otherEntries) {
  VELOX_CHECK_GT(otherSize, 0);

  auto size = entries_.size();
  std::vector<uint32_t> merged(size + otherSize);

  int pos = 0;
  int leftPos = 0;
  int rightPos = 0;

  while (leftPos < size && rightPos < otherSize) {
    auto left = decodeIndex(entries_[leftPos]);
    auto right = decodeIndex(otherEntries[rightPos]);
    if (left < right) {
      merged[pos++] = entries_[leftPos++];
    } else if (left > right) {
      merged[pos++] = otherEntries[rightPos++];
    } else {
      auto value = std::max(
          decodeValue(entries_[leftPos++]),
          decodeValue(otherEntries[rightPos++]));
      merged[pos++] = encode(left, value);
    }
  }

  while (leftPos < size) {
    merged[pos++] = entries_[leftPos++];
  }

  while (rightPos < otherSize) {
    merged[pos++] = otherEntries[rightPos++];
  }

  entries_.resize(pos);
  for (auto i = 0; i < pos; i++) {
    entries_[i] = merged[i];
  }
}

void SparseHll::verify() const {
  if (entries_.size() <= 1) {
    return;
  }

  auto prevIndex = decodeIndex(entries_[0]);
  for (auto i = 1; i < entries_.size(); i++) {
    auto index = decodeIndex(entries_[i]);
    VELOX_CHECK_LT(prevIndex, index);
    prevIndex = index;
  }
}

void SparseHll::toDense(DenseHll& denseHll) const {
  auto indexBitLength = denseHll.indexBitLength();

  for (auto i = 0; i < entries_.size(); i++) {
    auto entry = entries_[i];
    auto index = entry >> (32 - indexBitLength);

    auto zeros = __builtin_clz(entry << indexBitLength);

    // If zeros > kIndexBitLength - indexBitLength, it means all those bits were
    // zeros, so look at the entry value, which contains the number of leading 0
    // *after* kIndexBitLength.
    auto bits = kIndexBitLength - indexBitLength;
    if (zeros > bits) {
      zeros = bits + decodeValue(entry) - 1;
    }

    denseHll.insert(index, zeros + 1);
  }
}

} // namespace facebook::velox::aggregate::hll
