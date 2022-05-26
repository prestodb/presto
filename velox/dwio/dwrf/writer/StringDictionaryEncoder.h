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

#include <folly/container/F14Set.h>
#include <folly/hash/Checksum.h>

#include "velox/common/base/GTestMacros.h"
#include "velox/dwio/common/DataBuffer.h"

namespace facebook::velox::dwrf {

class StringDictionaryEncoder;

namespace detail {

// Each new string inserted into dictionary is assigned an incrementing id.
// A set is maintained with all of the DictStringId created. Using
// Heterogeneous lookup techniques, incoming StringPiece is first looked for
// a match in the set. If no match exists a new id is generated and inserted
// into the set. What is Heterogeneous lookup ?
// http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p0919r1.html
// Heterogeneous lookup is not available in standard CPP and proposed for CPP20.
// Follys:F14* variant supports it, so leveraging folly for now.
struct StringLookupKey {
  StringLookupKey(folly::StringPiece sp, uint32_t index)
      : sp{sp},
        index{index},
        hash{folly::crc32c(
            reinterpret_cast<const uint8_t*>(sp.data()),
            sp.size(),
            0 /* seed */)} {}

  const folly::StringPiece sp;
  const uint32_t index;
  const uint32_t hash;
};

class DictStringId {
 public:
  explicit DictStringId(uint32_t index) : index_{index} {}

  /* implicit */ DictStringId(StringLookupKey key) : DictStringId{key.index} {}

  uint32_t getIndex() const {
    return index_;
  }

 private:
  const uint32_t index_;
};

} // namespace detail

struct DictStringIdHash {
 public:
  explicit DictStringIdHash(const StringDictionaryEncoder& encoder)
      : encoder_{encoder} {}

  uint32_t operator()(detail::DictStringId lsp) const noexcept;

  // Overload to support Heterogeneous lookup.
  uint32_t operator()(detail::StringLookupKey key) const noexcept;

 private:
  const StringDictionaryEncoder& encoder_;
};

struct DictStringIdEquality {
 public:
  explicit DictStringIdEquality(const StringDictionaryEncoder& encoder)
      : encoder_{encoder} {}

  bool operator()(detail::DictStringId lhs, detail::DictStringId rhs) const;

  // Overload to support Heterogeneous lookup.
  bool operator()(detail::StringLookupKey key, detail::DictStringId lhs) const;

 private:
  const StringDictionaryEncoder& encoder_;
};

class StringDictionaryEncoder {
 public:
  explicit StringDictionaryEncoder(
      memory::MemoryPool& dictionaryDataPool,
      memory::MemoryPool& generalPool)
      : keyBytes_{dictionaryDataPool},
        keyIndex_{
            17,
            folly::transparent<DictStringIdHash>(*this),
            folly::transparent<DictStringIdEquality>(*this),
            memory::Allocator<detail::DictStringId>{generalPool}},
        keyOffsets_{dictionaryDataPool},
        counts_{generalPool},
        firstSeenStrideIndex_{generalPool},
        hash_{generalPool} {
    keyOffsets_.append(0);
  }

  uint32_t size() const {
    return counts_.size();
  }

  uint32_t
  addKey(folly::StringPiece sp, uint32_t strideIndex, uint32_t count = 1) {
    auto newIndex = size();
    detail::StringLookupKey key{sp, newIndex};
    auto result = keyIndex_.insert(key);
    if (!result.second) {
      auto index = result.first->getIndex();
      counts_[index] += count;
      return index;
    }

    auto bytesCount = keyBytes_.size();
    if (UNLIKELY(
            newIndex == std::numeric_limits<uint32_t>::max() ||
            (std::numeric_limits<uint32_t>::max() - bytesCount <= sp.size()))) {
      DWIO_RAISE("exceeds dictionary size limit");
    }

    // append keys
    keyBytes_.extendAppend(bytesCount, sp.data(), sp.size());
    keyOffsets_.append(keyBytes_.size());
    hash_.append(key.hash);
    counts_.append(count);
    firstSeenStrideIndex_.append(strideIndex);
    return newIndex;
  }

  // Get the current frequency of a key by its index/encoded value.
  // Can throw out_of_range exception.
  uint32_t getCount(uint32_t index) const {
    return counts_[index];
  }

  uint32_t getStride(uint32_t index) const {
    return firstSeenStrideIndex_[index];
  }

  folly::StringPiece getKey(uint32_t index) const {
    DCHECK(index < keyOffsets_.size() - 1);
    auto startOffset = keyOffsets_[index];
    auto endOffset = keyOffsets_[index + 1];
    return folly::StringPiece{
        keyBytes_.data() + startOffset, endOffset - startOffset};
  }

  void clear() {
    keyIndex_.clear();
    keyBytes_.clear();
    keyOffsets_.clear();
    keyOffsets_.append(0);
    counts_.clear();
    firstSeenStrideIndex_.clear();
    hash_.clear();
  }

 private:
  VELOX_FRIEND_TEST(TestStringDictionaryEncoder, GetCount);
  VELOX_FRIEND_TEST(TestStringDictionaryEncoder, GetIndex);
  VELOX_FRIEND_TEST(TestStringDictionaryEncoder, GetStride);
  VELOX_FRIEND_TEST(TestStringDictionaryEncoder, Clear);

  // Intended for testing only.
  uint32_t getIndex(folly::StringPiece sp) {
    detail::StringLookupKey key{sp, 0};
    auto result = keyIndex_.find(key);
    if (result != keyIndex_.end()) {
      return result->getIndex();
    }
    return size();
  }

  // All keys are written in this single array.
  dwio::common::DataBuffer<char> keyBytes_;

  // Set to lookup if the String is already assigned an id.
  // An Id can only be created after appending the string, so this set
  // leverages the heterogenous lookup to check if the String already exists
  // as Creating an id is expensive.
  folly::F14FastSet<
      detail::DictStringId,
      folly::transparent<DictStringIdHash>,
      folly::transparent<DictStringIdEquality>,
      memory::Allocator<detail::DictStringId>>
      keyIndex_;
  // key index -> starting offset of key.
  dwio::common::DataBuffer<uint32_t> keyOffsets_;
  // key index -> key count.
  dwio::common::DataBuffer<uint32_t> counts_;
  // The first stride the key appears in.
  dwio::common::DataBuffer<uint32_t> firstSeenStrideIndex_;
  // key index -> cached hash
  dwio::common::DataBuffer<uint32_t> hash_;

  friend class DictStringIdHash;
};

FOLLY_ALWAYS_INLINE bool DictStringIdEquality::operator()(
    detail::DictStringId lhs,
    detail::DictStringId rhs) const {
  return encoder_.getKey(lhs.getIndex()) == encoder_.getKey(rhs.getIndex());
}

FOLLY_ALWAYS_INLINE bool DictStringIdEquality::operator()(
    detail::StringLookupKey key,
    detail::DictStringId lhs) const {
  return encoder_.getKey(lhs.getIndex()) == key.sp;
}

FOLLY_ALWAYS_INLINE uint32_t
DictStringIdHash::operator()(detail::DictStringId lsp) const noexcept {
  return encoder_.hash_[lsp.getIndex()];
}

FOLLY_ALWAYS_INLINE uint32_t
DictStringIdHash::operator()(detail::StringLookupKey key) const noexcept {
  return key.hash;
}

} // namespace facebook::velox::dwrf
