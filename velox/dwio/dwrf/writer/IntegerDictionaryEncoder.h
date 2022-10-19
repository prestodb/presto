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
#include "velox/common/base/GTestMacros.h"
#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/dwrf/common/IntEncoder.h"
#include "velox/dwio/dwrf/writer/DictionaryEncodingUtils.h"

namespace facebook::velox::dwrf {

// Empty interface as registry entries
class AbstractIntegerDictionaryEncoder {
 public:
  virtual ~AbstractIntegerDictionaryEncoder() = default;
  virtual void bumpRefCount() = 0;
  virtual uint32_t size() const = 0;
};

template <typename Integer>
class IntegerDictionaryEncoder;

namespace {

// Each new integer inserted into set is assigned an incrementing id.
// A set is maintained with all of the DictIntegerId created. Using
// Heterogeneous lookup techniques, incoming integer is first looked for
// a match in the set. If no match exists a new id is generated and inserted
// into the set. What is Heterogeneous lookup ?
// http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p0919r1.html
// Heterogeneous lookup is not available in standard CPP and proposed for CPP20.
// Follys:F14* variant supports it, so leveraging folly for now.
template <typename Integer>
struct IntegerLookupKey {
  IntegerLookupKey(Integer val, uint32_t index) : val{val}, index{index} {}

  const Integer val;
  const uint32_t index;
};

template <typename Integer>
struct DictIntegerId {
 public:
  explicit DictIntegerId(uint32_t index) : index{index} {}

  /* implicit */ DictIntegerId(IntegerLookupKey<Integer> key)
      : DictIntegerId{key.index} {}

  const uint32_t index;
};

} // namespace

template <typename Integer>
struct DictIntegerHash {
 public:
  explicit DictIntegerHash(const IntegerDictionaryEncoder<Integer>& encoder)
      : encoder_{encoder} {}

  size_t operator()(DictIntegerId<Integer> key) const noexcept;

  // Overload to support Heterogeneous lookup.
  size_t operator()(IntegerLookupKey<Integer> key) const noexcept;

 private:
  const IntegerDictionaryEncoder<Integer>& encoder_;
};

template <typename Integer>
struct DictIntegerEquality {
 public:
  explicit DictIntegerEquality(const IntegerDictionaryEncoder<Integer>& encoder)
      : encoder_{encoder} {}

  bool operator()(DictIntegerId<Integer> lhs, DictIntegerId<Integer> rhs) const;

  // Overload to support Heterogeneous lookup.
  bool operator()(IntegerLookupKey<Integer> key, DictIntegerId<Integer> lhs)
      const;

 private:
  const IntegerDictionaryEncoder<Integer>& encoder_;
};

template <typename Integer>
class IntegerDictionaryEncoder : public AbstractIntegerDictionaryEncoder {
 public:
  // Lookup table may contain unsigned dictionary offsets [0 - 2^31) or signed
  // raw values. So the type used by it need to cover both ranges.
  using LookupType =
      typename std::conditional<sizeof(Integer) < 4, int32_t, Integer>::type;

  explicit IntegerDictionaryEncoder(
      memory::MemoryPool& dictionaryDataPool,
      memory::MemoryPool& generalPool,
      bool sort = false,
      std::unique_ptr<IntEncoder<true>> dictDataWriter = nullptr)
      : generalPool_{generalPool},
        keyIndex_{
            17,
            folly::transparent<DictIntegerHash<Integer>>(*this),
            folly::transparent<DictIntegerEquality<Integer>>(*this),
            memory::Allocator<DictIntegerId<Integer>>{generalPool_}},
        keys_{dictionaryDataPool},
        counts_{dictionaryDataPool},
        totalCount_{0},
        sort_{sort},
        dictDataWriter_{std::move(dictDataWriter)},
        lookupTable_{generalPool_},
        inDict_{generalPool_} {}

  virtual ~IntegerDictionaryEncoder() override = default;

  // Add a value to the dictionary.
  uint32_t addKey(Integer value, uint32_t count = 1) {
    totalCount_ += count;
    auto newIndex = size();
    IntegerLookupKey<Integer> key{value, newIndex};
    auto result = keyIndex_.insert(key);
    if (!result.second) {
      auto index = result.first->index;
      counts_[index] += count;
      return index;
    }

    if (UNLIKELY(newIndex == std::numeric_limits<int32_t>::max())) {
      DWIO_RAISE("exceeds dictionary size limit");
    }

    keys_.append(value);
    counts_.append(count);
    return newIndex;
  }

  // Returns the num elements in the dictionary. Helps determine if we
  // should use dictionary encoding at all.
  uint32_t size() const override {
    return keys_.size();
  }

  // Get the current frequency of a key by its index/encoded value.
  // Can throw out_of_range exception.
  uint32_t getCount(uint32_t index) const {
    return counts_[index];
  }

  uint32_t getTotalCount() const {
    return totalCount_;
  }

  // Get key with index/encoded value.
  // Can throw out_of_range exception.
  Integer getKey(uint32_t index) const {
    return keys_[index];
  }

  // Sort the keys numerically, returns the number of elements in sorted
  // dictionary. The number of elements in sorted dictionary can be different
  // if we are using stride dictionary optimization.
  // Populates a lookup table that maps original indices to
  // sorted indices. In other words,
  //         lookupTable[index] = sortedIndex
  // NOTE: Dropped infrequent keys would be mapped to the actual value instead.
  template <typename TL, typename F>
  static uint32_t getSortedIndexLookupTable(
      IntegerDictionaryEncoder<Integer>& dictEncoder,
      memory::MemoryPool& pool,
      bool dropInfrequentKeys,
      bool sort,
      dwio::common::DataBuffer<TL>& lookupTable,
      dwio::common::DataBuffer<bool>& inDict,
      dwio::common::DataBuffer<char>& writeBuffer,
      F fn) {
    auto numKeys = dictEncoder.size();
    dwio::common::DataBuffer<uint32_t> sortedIndex{pool};

    if (sort) {
      sortedIndex.reserve(numKeys);
      for (uint32_t i = 0; i != numKeys; ++i) {
        sortedIndex[i] = i;
      }
      std::sort(
          sortedIndex.data(),
          sortedIndex.data() + numKeys,
          [&dictEncoder](size_t lhs, size_t rhs) {
            return dictEncoder.getKey(lhs) < dictEncoder.getKey(rhs);
          });
    }

    inDict.reserve(numKeys);
    lookupTable.reserve(numKeys);
    uint32_t newIndex = 0;

    auto dictWriter = createBufferedWriter<Integer>(writeBuffer, fn);

    for (uint32_t i = 0; i != numKeys; ++i) {
      auto origIndex = (sort ? sortedIndex[i] : i);
      if (!dropInfrequentKeys || shouldWriteKey(dictEncoder, origIndex)) {
        dictWriter.add(dictEncoder.getKey(origIndex));
        lookupTable[origIndex] = static_cast<TL>(newIndex++);
        inDict[origIndex] = true;
      } else {
        lookupTable[origIndex] = dictEncoder.getKey(origIndex);
        inDict[origIndex] = false;
      }
    }
    return newIndex;
  }

  uint32_t flush() {
    if (flushed_) {
      return finalDictionarySize_;
    }
    flushed_ = true;
    dwio::common::DataBuffer<char> writeBuffer{generalPool_};
    writeBuffer.reserve(64 * 1024);
    finalDictionarySize_ =
        IntegerDictionaryEncoder<Integer>::getSortedIndexLookupTable(
            *this,
            generalPool_,
            true,
            sort_,
            lookupTable_,
            inDict_,
            writeBuffer,
            [dictDataWriter = dictDataWriter_.get()](
                Integer* const buf, const uint32_t size) mutable {
              if (dictDataWriter) {
                dictDataWriter->add(buf, common::Ranges::of(0, size), nullptr);
                dictDataWriter->flush();
              }
            });
    return finalDictionarySize_;
  }

  dwio::common::DataBuffer<bool>& getInDict() {
    DWIO_ENSURE(flushed_);
    return inDict_;
  }

  dwio::common::DataBuffer<LookupType>& getLookupTable() {
    DWIO_ENSURE(flushed_);
    return lookupTable_;
  }

  void bumpRefCount() override {
    ++refCount_;
  }

  void clear() {
    DWIO_ENSURE(clearCount_ <= refCount_);
    if (++clearCount_ == refCount_) {
      keyIndex_.clear();
      keys_.clear();
      counts_.clear();
      totalCount_ = 0;
      flushed_ = false;
      clearCount_ = 0;
    }
  }

 private:
  VELOX_FRIEND_TEST(TestIntegerDictionaryEncoder, Clear);
  VELOX_FRIEND_TEST(TestIntegerDictionaryEncoder, GetCount);
  VELOX_FRIEND_TEST(TestWriterContext, GetIntDictionaryEncoder);

  // TODO: partially specialize for integers only.
  template <typename T>
  static bool shouldWriteKey(
      const IntegerDictionaryEncoder<T>& dictEncoder,
      size_t index) {
    return dictEncoder.getCount(index) > 1;
  }

  // Get the index/encoded value of a key.
  // Can throw out_of_range exception if key does not exist in dictionary.
  // This method is only used by tests
  uint32_t getIndex(Integer key) const {
    IntegerLookupKey<Integer> lookupKey{key, 0};
    auto ret = keyIndex_.find(lookupKey);
    if (ret != keyIndex_.end()) {
      return ret->index;
    }
    return size();
  }

  memory::MemoryPool& generalPool_;
  folly::F14FastSet<
      DictIntegerId<Integer>,
      folly::transparent<DictIntegerHash<Integer>>,
      folly::transparent<DictIntegerEquality<Integer>>,
      memory::Allocator<DictIntegerId<Integer>>>
      keyIndex_;
  // key index -> key.
  dwio::common::DataBuffer<Integer> keys_;
  // key index -> key count. We made count type uint32_t. In the unlikely
  // case that count exceeds what uint32_t can hold, it may carry incorrect
  // count information due to overflow. It's not a big problem because
  // dictionary encoder only need to tell if count is greater than 1 at the
  // moment.
  dwio::common::DataBuffer<uint32_t> counts_;
  uint32_t totalCount_;
  const bool sort_;
  std::unique_ptr<IntEncoder<true>> dictDataWriter_;
  dwio::common::DataBuffer<LookupType> lookupTable_;
  dwio::common::DataBuffer<bool> inDict_;
  uint32_t refCount_{1};
  uint32_t clearCount_{0};
  bool flushed_{false};
  size_t finalDictionarySize_;
};

template <typename Integer>
bool DictIntegerEquality<Integer>::operator()(
    DictIntegerId<Integer> lhs,
    DictIntegerId<Integer> rhs) const {
  return encoder_.getKey(lhs.index) == encoder_.getKey(rhs.index);
}

template <typename Integer>
bool DictIntegerEquality<Integer>::operator()(
    IntegerLookupKey<Integer> key,
    DictIntegerId<Integer> lhs) const {
  return encoder_.getKey(lhs.index) == key.val;
}

template <typename Integer>
size_t DictIntegerHash<Integer>::operator()(
    DictIntegerId<Integer> key) const noexcept {
  return encoder_.getKey(key.index);
}

template <typename Integer>
size_t DictIntegerHash<Integer>::operator()(
    IntegerLookupKey<Integer> key) const noexcept {
  return key.val;
}

} // namespace facebook::velox::dwrf
