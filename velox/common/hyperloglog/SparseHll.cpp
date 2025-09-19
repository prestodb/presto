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
#include "velox/common/hyperloglog/SparseHll.h"
#include "velox/common/base/IOUtils.h"
#include "velox/common/hyperloglog/HllUtils.h"

namespace facebook::velox::common::hll {
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

template <typename VectorType>
int searchIndex(uint32_t index, const VectorType& entries) {
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

// Static utility functions implementation
int64_t SparseHlls::cardinality(const char* serialized) {
  static const int kTotalBuckets = 1 << kIndexBitLength;

  auto stream = initializeInputStream(serialized);
  auto size = stream.read<int16_t>();

  int zeroBuckets = kTotalBuckets - size;
  return std::round(linearCounting(zeroBuckets, kTotalBuckets));
}

std::string SparseHlls::serializeEmpty(int8_t indexBitLength) {
  static const size_t kSize = 4;

  std::string serialized;
  serialized.resize(kSize);

  common::OutputByteStream stream(serialized.data());
  stream.appendOne(kPrestoSparseV2);
  stream.appendOne(indexBitLength);
  stream.appendOne(static_cast<int16_t>(0));
  return serialized;
}

bool SparseHlls::canDeserialize(const char* input) {
  return *reinterpret_cast<const int8_t*>(input) == kPrestoSparseV2;
}

int8_t SparseHlls::deserializeIndexBitLength(const char* input) {
  common::InputByteStream stream(input);
  stream.read<int8_t>(); // Skip version
  return stream.read<int8_t>(); // Return indexBitLength
}

// Template method implementations
template <typename TAllocator>
SparseHll<TAllocator>::SparseHll(TAllocator* allocator)
    : allocator_(allocator), entries_{TStlAllocator<uint32_t>(allocator)} {}

template <typename TAllocator>
SparseHll<TAllocator>::SparseHll(const char* serialized, TAllocator* allocator)
    : allocator_(allocator), entries_{TStlAllocator<uint32_t>(allocator)} {
  common::InputByteStream stream(serialized);
  auto version = stream.read<int8_t>();
  VELOX_CHECK_EQ(kPrestoSparseV2, version);

  // Skip indexBitLength from serialized data - we use fixed kIndexBitLength
  // internally
  stream.read<int8_t>();

  auto size = stream.read<int16_t>();
  entries_.resize(size);
  for (auto i = 0; i < size; i++) {
    entries_[i] = stream.read<uint32_t>();
  }
}

template <typename TAllocator>
bool SparseHll<TAllocator>::insertHash(uint64_t hash) {
  auto index = computeIndex(hash, kIndexBitLength);
  auto value = numberOfLeadingZeros(hash, kIndexBitLength);

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

  return overLimit();
}

template <typename TAllocator>
int64_t SparseHll<TAllocator>::cardinality() const {
  // Estimate the cardinality using linear counting over the theoretical
  // 2^kIndexBitLength buckets available due to the fact that we're
  // recording the raw leading kIndexBitLength of the hash. This produces
  // much better precision while in the sparse regime.
  const int kTotalBuckets = 1 << kIndexBitLength;

  int zeroBuckets = kTotalBuckets - entries_.size();
  return std::round(linearCounting(zeroBuckets, kTotalBuckets));
}

template <typename TAllocator>
void SparseHll<TAllocator>::serialize(int8_t indexBitLength, char* output)
    const {
  common::OutputByteStream stream(output);
  stream.appendOne(kPrestoSparseV2);
  stream.appendOne(indexBitLength);
  stream.appendOne(static_cast<int16_t>(entries_.size()));
  for (auto entry : entries_) {
    stream.appendOne(entry);
  }
}

template <typename TAllocator>
int32_t SparseHll<TAllocator>::serializedSize() const {
  return 1 /* version */
      + 1 /* indexBitLength */
      + 2 /* number of entries */
      + entries_.size() * 4;
}

template <typename TAllocator>
int32_t SparseHll<TAllocator>::inMemorySize() const {
  return sizeof(uint32_t) * entries_.size();
}

template <typename TAllocator>
void SparseHll<TAllocator>::mergeWith(const SparseHll<TAllocator>& other) {
  auto size = other.entries_.size();
  // This check prevents merge aggregation from being performed on
  // empty_approx_set(), an empty HyperLogLog. The merge function typically
  // does not take an empty HyperLogLog structure as an argument.
  if (size) {
    mergeWith(size, other.entries_.data());
  }
}

template <typename TAllocator>
void SparseHll<TAllocator>::mergeWith(const char* serialized) {
  auto stream = initializeInputStream(serialized);

  auto size = stream.read<int16_t>();
  // This check prevents merge aggregation from being performed on
  // empty_approx_set(), an empty HyperLogLog. The merge function typically
  // does not take an empty HyperLogLog structure as an argument.
  if (size) {
    mergeWith(
        size, reinterpret_cast<const uint32_t*>(serialized + stream.offset()));
  }
}

template <typename TAllocator>
void SparseHll<TAllocator>::mergeWith(
    size_t otherSize,
    const uint32_t* otherEntries) {
  VELOX_CHECK_GT(otherSize, 0);

  auto size = entries_.size();

  auto merged = std::vector<uint32_t, TStlAllocator<uint32_t>>(
      size + otherSize, TStlAllocator<uint32_t>(allocator_));

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

template <typename TAllocator>
void SparseHll<TAllocator>::verify() const {
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

template <typename TAllocator>
void SparseHll<TAllocator>::toDense(DenseHll<TAllocator>& denseHll) const {
  auto indexBitLength = denseHll.indexBitLength();

  for (auto entry : entries_) {
    auto index = entry >> (32 - indexBitLength);
    auto shiftedValue = entry << indexBitLength;
    auto zeros = shiftedValue == 0 ? 32 : __builtin_clz(shiftedValue);

    // If zeros >= kIndexBitLength - indexBitLength, it means all those bits
    // were zeros, so look at the entry value, which contains the number of
    // leading 0 *after* kIndexBitLength.
    auto bits = kIndexBitLength - indexBitLength;
    if (zeros >= bits) {
      zeros = bits + decodeValue(entry);
    }

    denseHll.insert(index, zeros + 1);
  }
}

// Explicit template instantiation for HashStringAllocator (default)
template class SparseHll<HashStringAllocator>;
// Explicit template instantiation for memory::MemoryPool
template class SparseHll<memory::MemoryPool>;

} // namespace facebook::velox::common::hll
