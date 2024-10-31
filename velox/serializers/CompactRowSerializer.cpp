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
#include "velox/row/CompactRow.h"

namespace facebook::velox::serializer {

void CompactRowVectorSerde::estimateSerializedSize(
    const BaseVector* /* vector */,
    const folly::Range<const IndexRange*>& /* ranges */,
    vector_size_t** /* sizes */,
    Scratch& /*scratch*/) {
  VELOX_UNSUPPORTED();
}

namespace {
class CompactRowVectorSerializer : public IterativeVectorSerializer {
 public:
  using TRowSize = uint32_t;

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

 private:
  memory::MemoryPool* const pool_;
  std::vector<BufferPtr> buffers_;
};

// Read from the stream until the full row is concatenated.
std::string concatenatePartialRow(
    ByteInputStream* source,
    std::string_view rowFragment,
    CompactRowVectorSerializer::TRowSize rowSize) {
  std::string rowBuffer;
  rowBuffer.reserve(rowSize);
  rowBuffer.append(rowFragment);

  while (rowBuffer.size() < rowSize) {
    rowFragment = source->nextView(rowSize - rowBuffer.size());
    VELOX_CHECK_GT(
        rowFragment.size(),
        0,
        "Unable to read full serialized CompactRow. Needed {} but read {} bytes.",
        rowSize - rowBuffer.size(),
        rowFragment.size());
    rowBuffer += rowFragment;
  }
  return rowBuffer;
}

} // namespace

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
  std::vector<std::string> concatenatedRows;

  while (!source->atEnd()) {
    // First read row size in big endian order.
    auto rowSize = folly::Endian::big(
        source->read<CompactRowVectorSerializer::TRowSize>());
    auto row = source->nextView(rowSize);

    // If we couldn't read the entire row at once, we need to concatenate it
    // in a different buffer.
    if (row.size() < rowSize) {
      concatenatedRows.push_back(concatenatePartialRow(source, row, rowSize));
      row = concatenatedRows.back();
    }

    VELOX_CHECK_EQ(row.size(), rowSize);
    serializedRows.push_back(row);
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

} // namespace facebook::velox::serializer
