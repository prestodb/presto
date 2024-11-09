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

#include <folly/lang/Bits.h>
#include "velox/common/base/Exceptions.h"
#include "velox/row/CompactRow.h"

namespace facebook::velox::serializer {
namespace {
using TRowSize = uint32_t;

class CompactRowVectorSerializer : public IterativeVectorSerializer {
 public:
  explicit CompactRowVectorSerializer(StreamArena* streamArena)
      : pool_{streamArena->pool()} {}

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& scratch) override {
    size_t totalSize = 0;
    const auto totalRows = std::accumulate(
        ranges.begin(),
        ranges.end(),
        0,
        [](vector_size_t sum, const auto& range) { return sum + range.size; });

    if (totalRows == 0) {
      return;
    }

    row::CompactRow row(vector);
    std::vector<vector_size_t> rowSize(totalRows);
    if (auto fixedRowSize =
            row::CompactRow::fixedRowSize(asRowType(vector->type()))) {
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

    size_t offset = 0;
    vector_size_t index = 0;
    for (const auto& range : ranges) {
      if (range.size == 1) {
        // Fast path for single-row serialization.
        *(TRowSize*)(rawBuffer + offset) = folly::Endian::big(rowSize[index]);
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

  void append(
      const row::CompactRow& compactRow,
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
      stream->write(buffer->as<char>(), buffer->size());
    }
    buffers_.clear();
  }

  void clear() override {}

 private:
  memory::MemoryPool* const pool_;
  std::vector<BufferPtr> buffers_;
};

// Read from the stream until the full row is concatenated.
void concatenatePartialRow(
    ByteInputStream* source,
    TRowSize rowSize,
    std::string& rowBuffer) {
  while (rowBuffer.size() < rowSize) {
    const std::string_view rowFragment =
        source->nextView(rowSize - rowBuffer.size());
    VELOX_CHECK_GT(
        rowFragment.size(),
        0,
        "Unable to read full serialized CompactRow. Needed {} but read {} bytes.",
        rowSize - rowBuffer.size(),
        rowFragment.size());
    rowBuffer.append(rowFragment.data(), rowFragment.size());
  }
}

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
    const Options* /* options */) {
  return std::make_unique<CompactRowVectorSerializer>(streamArena);
}

void CompactRowVectorSerde::deserialize(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    const Options* /* options */) {
  std::vector<std::string_view> serializedRows;
  std::vector<std::unique_ptr<std::string>> serializedBuffers;
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

  if (serializedRows.empty()) {
    *result = BaseVector::create<RowVector>(type, 0, pool);
    return;
  }

  *result = velox::row::CompactRow::deserialize(serializedRows, type, pool);
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
