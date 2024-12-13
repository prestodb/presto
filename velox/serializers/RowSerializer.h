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

#include "velox/serializers/CompactRowSerializer.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::serializer {

using TRowSize = uint32_t;

template <class Serializer>
class RowSerializer : public IterativeVectorSerializer {
 public:
  explicit RowSerializer(memory::MemoryPool* pool) : pool_(pool) {}

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& /*scratch*/) override {
    size_t totalSize = 0;
    const auto totalRows = std::accumulate(
        ranges.begin(),
        ranges.end(),
        0,
        [](vector_size_t sum, const auto& range) { return sum + range.size; });

    if (totalRows == 0) {
      return;
    }

    Serializer row(vector);
    std::vector<vector_size_t> rowSize(totalRows);
    if (auto fixedRowSize =
            Serializer::fixedRowSize(asRowType(vector->type()))) {
      totalSize += (fixedRowSize.value() + sizeof(TRowSize)) * totalRows;
      std::fill(rowSize.begin(), rowSize.end(), fixedRowSize.value());
    } else {
      vector_size_t index = 0;
      for (const auto& range : ranges) {
        for (auto i = 0; i < range.size; ++i, ++index) {
          rowSize[index] = row.rowSize(range.begin + i);
          totalSize += rowSize[index] + sizeof(TRowSize);
        }
      }
    }

    if (totalSize == 0) {
      return;
    }

    BufferPtr buffer = AlignedBuffer::allocate<char>(totalSize, pool_, 0);
    auto* rawBuffer = buffer->asMutable<char>();
    buffers_.push_back(std::move(buffer));

    serializeRanges(row, ranges, rawBuffer, rowSize);
  }

  void append(
      const Serializer& compactRow,
      const folly::Range<const vector_size_t*>& rows,
      const std::vector<vector_size_t>& sizes) override {
    size_t totalSize = 0;
    for (const auto row : rows) {
      totalSize += sizes[row];
    }
    if (totalSize == 0) {
      return;
    }

    BufferPtr buffer = AlignedBuffer::allocate<char>(totalSize, pool_, 0);
    auto* rawBuffer = buffer->asMutable<char>();
    buffers_.push_back(std::move(buffer));

    size_t offset = 0;
    for (auto& row : rows) {
      // Write row data.
      const TRowSize size =
          compactRow.serialize(row, rawBuffer + offset + sizeof(TRowSize));

      // Write raw size. Needs to be in big endian order.
      *(TRowSize*)(rawBuffer + offset) = folly::Endian::big(size);
      offset += sizeof(TRowSize) + size;
    }
  }

  size_t maxSerializedSize() const override {
    size_t totalSize = 0;
    for (const auto& buffer : buffers_) {
      totalSize += buffer->size();
    }
    return totalSize;
  }

  void flush(OutputStream* stream) override {
    for (const auto& buffer : buffers_) {
      stream->write(buffer->asMutable<char>(), buffer->size());
    }
    buffers_.clear();
  }

  void clear() override {}

 protected:
  virtual void serializeRanges(
      const Serializer& rowSerializer,
      const folly::Range<const IndexRange*>& ranges,
      char* rawBuffer,
      const std::vector<vector_size_t>& /*rowSize*/) {
    size_t offset = 0;
    for (auto& range : ranges) {
      for (auto row = range.begin; row < range.begin + range.size; ++row) {
        // Write row data.
        TRowSize size =
            rowSerializer.serialize(row, rawBuffer + offset + sizeof(TRowSize));

        // Write raw size. Needs to be in big endian order.
        *(TRowSize*)(rawBuffer + offset) = folly::Endian::big(size);
        offset += sizeof(TRowSize) + size;
      }
    }
  }

  memory::MemoryPool* const pool_;
  std::vector<BufferPtr> buffers_;
};

template <typename SerializeView>
class RowDeserializer {
 public:
  static void deserialize(
      ByteInputStream* source,
      std::vector<SerializeView>& serializedRows,
      std::vector<std::unique_ptr<std::string>>& serializedBuffers) {
    while (!source->atEnd()) {
      // First read row size in big endian order.
      const auto rowSize = folly::Endian::big(source->read<TRowSize>());
      auto serializedBuffer = std::make_unique<std::string>();
      serializedBuffer->reserve(rowSize);

      const auto row = source->nextView(rowSize);
      serializedBuffer->append(row.data(), row.size());
      // If we couldn't read the entire row at once, we need to concatenate it
      // in a different buffer.
      if (serializedBuffer->size() < rowSize) {
        concatenatePartialRow(source, rowSize, *serializedBuffer);
      }

      VELOX_CHECK_EQ(serializedBuffer->size(), rowSize);
      serializedBuffers.emplace_back(std::move(serializedBuffer));
      serializedRows.push_back(std::string_view(
          serializedBuffers.back()->data(), serializedBuffers.back()->size()));
    }
  }

 private:
  // Read from the stream until the full row is concatenated.
  static void concatenatePartialRow(
      ByteInputStream* source,
      TRowSize rowSize,
      std::string& rowBuffer) {
    while (rowBuffer.size() < rowSize) {
      const std::string_view rowFragment =
          source->nextView(rowSize - rowBuffer.size());
      VELOX_CHECK_GT(
          rowFragment.size(),
          0,
          "Unable to read full serialized row. Needed {} but read {} bytes.",
          rowSize - rowBuffer.size(),
          rowFragment.size());
      rowBuffer.append(rowFragment.data(), rowFragment.size());
    }
  }
};

} // namespace facebook::velox::serializer
