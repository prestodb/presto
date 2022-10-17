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

#include "velox/common/base/BitUtil.h"
#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/dwio/dwrf/reader/ConstantColumnReader.h"
#include "velox/dwio/dwrf/reader/FlatMapHelper.h"
#include "velox/dwio/dwrf/utils/BitIterator.h"

namespace facebook::velox::dwrf {

class StringKeyBuffer;

// represent a branch of a value node in a flat map
// represent a keyed value node
template <typename T>
class KeyNode {
 private:
  std::unique_ptr<ColumnReader> reader_;
  std::unique_ptr<ByteRleDecoder> inMap_;
  dwio::common::DataBuffer<char> inMapData_;
  flatmap_helper::KeyValue<T> key_;
  uint32_t sequence_;
  VectorPtr vector_;
  memory::MemoryPool& memoryPool_;

  // key node ordinal
  mutable uint32_t ordinal_;

 public:
  KeyNode(
      std::unique_ptr<ColumnReader> valueReader,
      std::unique_ptr<ByteRleDecoder> inMapDecoder,
      const flatmap_helper::KeyValue<T>& keyValue,
      uint32_t sequence,
      memory::MemoryPool& pool)
      : reader_(std::move(valueReader)),
        inMap_(std::move(inMapDecoder)),
        inMapData_{pool},
        key_{keyValue},
        sequence_{sequence},
        memoryPool_{pool} {}
  ~KeyNode() = default;

  const flatmap_helper::KeyValue<T>& getKey() const {
    return key_;
  }

  uint32_t getSequence() const {
    return sequence_;
  }

  void setOrdinal(uint32_t ordinal) {
    ordinal_ = ordinal;
  }

  // skip number of values in this node
  void skip(uint64_t numValues);

  // try to load numValues from current position
  // return the items loaded
  BaseVector* FOLLY_NULLABLE load(uint64_t numValues);

  void loadAsChild(
      VectorPtr& vec,
      uint64_t numValues,
      BufferPtr& mergedNulls,
      uint64_t nonNullMaps,
      const uint64_t* FOLLY_NULLABLE nulls);

  const uint64_t* FOLLY_NULLABLE mergeNulls(
      uint64_t numValues,
      BufferPtr& mergedNulls,
      uint64_t nonNullMaps,
      const uint64_t* FOLLY_NULLABLE nulls);

  const uint64_t* FOLLY_NULLABLE
  getAllNulls(uint64_t numValues, BufferPtr& mergedNulls);

  uint64_t* FOLLY_NULLABLE
  ensureNullBuffer(uint64_t numValues, BufferPtr& mergedNulls);

  uint64_t readInMapData(uint64_t numValues);

  void addToBulkInMapBitIterator(utils::BulkBitIterator<char>& it) {
    it.addRawByteBuffer(inMapData_.data());
  }

  void fillKeysVector(
      VectorPtr& vector,
      vector_size_t offset,
      const StringKeyBuffer* FOLLY_NULLABLE /* unused */) {
    // Ideally, this should be dynamic_cast, but we would like to avoid the
    // cost. We cannot make vector type template variable because for string
    // type, we will have two different vector representations
    auto& flatVector = static_cast<FlatVector<T>&>(*vector);
    const_cast<T*>(flatVector.rawValues())[offset] = key_.get();
  }
};

class StringKeyBuffer {
 public:
  StringKeyBuffer(
      memory::MemoryPool& pool,
      const std::vector<std::unique_ptr<KeyNode<StringView>>>& nodes);
  ~StringKeyBuffer() = default;

  void fillKey(
      uint32_t ordinal,
      std::function<void(char* FOLLY_NONNULL, size_t)> callback) const {
    auto data = data_[ordinal];
    callback(data, data_[ordinal + 1] - data);
  }

  const BufferPtr& getBuffer() const {
    return buffer_;
  }

 private:
  BufferPtr buffer_;
  dwio::common::DataBuffer<char*> data_;
};

template <typename T>
class FlatMapColumnReader : public ColumnReader {
 public:
  FlatMapColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext);
  ~FlatMapColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(
      uint64_t numValues,
      VectorPtr& result,
      const uint64_t* FOLLY_NULLABLE nulls) override;

 private:
  const std::shared_ptr<const dwio::common::TypeWithId> requestedType_;
  std::vector<std::unique_ptr<KeyNode<T>>> keyNodes_;
  std::unique_ptr<StringKeyBuffer> stringKeyBuffer_;
  bool returnFlatVector_;

  void initStringKeyBuffer() {}

  void initKeysVector(VectorPtr& vector, vector_size_t size);
};

template <typename T>
class FlatMapStructEncodingColumnReader : public ColumnReader {
 public:
  FlatMapStructEncodingColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext);
  ~FlatMapStructEncodingColumnReader() override = default;

  uint64_t skip(uint64_t numValues) override;

  void next(
      uint64_t numValues,
      VectorPtr& result,
      const uint64_t* FOLLY_NULLABLE nulls) override;

 private:
  const std::shared_ptr<const dwio::common::TypeWithId> requestedType_;
  std::vector<std::unique_ptr<KeyNode<T>>> keyNodes_;
  std::unique_ptr<NullColumnReader> nullColumnReader_;
  BufferPtr mergedNulls_;
};

class FlatMapColumnReaderFactory {
 public:
  static std::unique_ptr<ColumnReader> create(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      StripeStreams& stripe,
      FlatMapContext flatMapContext);
};

} // namespace facebook::velox::dwrf
