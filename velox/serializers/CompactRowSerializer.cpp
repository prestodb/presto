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
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/RowSerializer.h"

#include <folly/lang/Bits.h>
#include "velox/common/base/Exceptions.h"
#include "velox/row/CompactRow.h"

namespace facebook::velox::serializer {
namespace {
using TRowSize = uint32_t;

class CompactRowVectorSerializer : public RowSerializer<row::CompactRow> {
 public:
  explicit CompactRowVectorSerializer(
      memory::MemoryPool* pool,
      const VectorSerde::Options* options)
      : RowSerializer<row::CompactRow>(pool, options) {}

 private:
  void serializeRanges(
      const row::CompactRow& row,
      const folly::Range<const IndexRange*>& ranges,
      char* rawBuffer,
      const std::vector<vector_size_t>& rowSize) override {
    size_t offset = 0;
    vector_size_t index = 0;
    for (const auto& range : ranges) {
      if (range.size == 1) {
        // Fast path for single-row serialization.
        *reinterpret_cast<TRowSize*>(rawBuffer + offset) =
            folly::Endian::big(rowSize[index]);
        auto size =
            row.serialize(range.begin, rawBuffer + offset + sizeof(TRowSize));
        offset += size + sizeof(TRowSize);
        ++index;
      } else {
        raw_vector<size_t> offsets(range.size);
        for (auto i = 0; i < range.size; ++i, ++index) {
          // Write raw size. Needs to be in big endian order.
          *(TRowSize*)(rawBuffer + offset) = folly::Endian::big(rowSize[index]);
          offsets[i] = offset + sizeof(TRowSize);
          offset += rowSize[index] + sizeof(TRowSize);
        }
        // Write row data for all rows in range.
        row.serialize(range.begin, range.size, offsets.data(), rawBuffer);
      }
    }
  }
};

} // namespace

void CompactRowVectorSerde::estimateSerializedSize(
    const row::CompactRow* compactRow,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes) {
  compactRow->serializedRowSizes(rows, sizes);
}

std::unique_ptr<IterativeVectorSerializer>
CompactRowVectorSerde::createIterativeSerializer(
    RowTypePtr /* type */,
    int32_t /* numRows */,
    StreamArena* streamArena,
    const Options* options) {
  return std::make_unique<CompactRowVectorSerializer>(
      streamArena->pool(), options);
}

void CompactRowVectorSerde::deserialize(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    const Options* options) {
  std::vector<std::string_view> serializedRows;
  std::vector<std::unique_ptr<std::string>> serializedBuffers;
  RowDeserializer<std::string_view>::deserialize(
      source, serializedRows, serializedBuffers, options);

  if (serializedRows.empty()) {
    *result = BaseVector::create<RowVector>(type, 0, pool);
    return;
  }

  *result = row::CompactRow::deserialize(serializedRows, type, pool);
}

void CompactRowVectorSerde::deserialize(
    ByteInputStream* source,
    std::unique_ptr<RowIterator>& sourceRowIterator,
    uint64_t maxRows,
    RowTypePtr type,
    RowVectorPtr* result,
    velox::memory::MemoryPool* pool,
    const Options* options) {
  std::vector<std::string_view> serializedRows;
  std::vector<std::unique_ptr<std::string>> serializedBuffers;
  RowDeserializer<std::string_view>::deserialize(
      source,
      maxRows,
      sourceRowIterator,
      serializedRows,
      serializedBuffers,
      options);

  if (serializedRows.empty()) {
    *result = BaseVector::create<RowVector>(type, 0, pool);
    return;
  }

  *result = row::CompactRow::deserialize(serializedRows, type, pool);
}

// static
void CompactRowVectorSerde::registerVectorSerde() {
  velox::registerVectorSerde(std::make_unique<CompactRowVectorSerde>());
}

// static
void CompactRowVectorSerde::registerNamedVectorSerde() {
  velox::registerNamedVectorSerde(
      VectorSerde::Kind::kCompactRow,
      std::make_unique<CompactRowVectorSerde>());
}

} // namespace facebook::velox::serializer
