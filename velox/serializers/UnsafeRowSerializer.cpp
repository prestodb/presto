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
#include "velox/serializers/UnsafeRowSerializer.h"
#include <folly/lang/Bits.h>
#include "velox/row/UnsafeRowDeserializers.h"
#include "velox/row/UnsafeRowFast.h"

namespace facebook::velox::serializer::spark {
namespace {
using TRowSize = uint32_t;
}

namespace {
class UnsafeRowVectorSerializer : public IterativeVectorSerializer {
 public:
  explicit UnsafeRowVectorSerializer(StreamArena* streamArena)
      : pool_{streamArena->pool()} {}

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& /*scratch*/) override {
    size_t totalSize = 0;
    row::UnsafeRowFast unsafeRow(vector);
    if (auto fixedRowSize =
            row::UnsafeRowFast::fixedRowSize(asRowType(vector->type()))) {
      for (const auto& range : ranges) {
        totalSize += (fixedRowSize.value() + sizeof(TRowSize)) * range.size;
      }

    } else {
      for (const auto& range : ranges) {
        for (auto i = range.begin; i < range.begin + range.size; ++i) {
          totalSize += unsafeRow.rowSize(i) + sizeof(TRowSize);
        }
      }
    }

    if (totalSize == 0) {
      return;
    }

    BufferPtr buffer = AlignedBuffer::allocate<char>(totalSize, pool_, 0);
    auto rawBuffer = buffer->asMutable<char>();
    buffers_.push_back(std::move(buffer));

    size_t offset = 0;
    for (auto& range : ranges) {
      for (auto i = range.begin; i < range.begin + range.size; ++i) {
        // Write row data.
        TRowSize size =
            unsafeRow.serialize(i, rawBuffer + offset + sizeof(TRowSize));

        // Write raw size. Needs to be in big endian order.
        *(TRowSize*)(rawBuffer + offset) = folly::Endian::big(size);
        offset += sizeof(TRowSize) + size;
      }
    }
  }

  void append(
      const row::UnsafeRowFast& unsafeRow,
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
          unsafeRow.serialize(row, rawBuffer + offset + sizeof(TRowSize));

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
        "Unable to read full serialized UnsafeRow. Needed {} but read {} bytes.",
        rowSize - rowBuffer.size(),
        rowFragment.size());
    rowBuffer.append(rowFragment.data(), rowFragment.size());
  }
}
} // namespace

void UnsafeRowVectorSerde::estimateSerializedSize(
    const row::UnsafeRowFast* unsafeRow,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes) {
  return unsafeRow->serializedRowSizes(rows, sizes);
}

std::unique_ptr<IterativeVectorSerializer>
UnsafeRowVectorSerde::createIterativeSerializer(
    RowTypePtr /* type */,
    int32_t /* numRows */,
    StreamArena* streamArena,
    const Options* /* options */) {
  return std::make_unique<UnsafeRowVectorSerializer>(streamArena);
}

void UnsafeRowVectorSerde::deserialize(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    const Options* /* options */) {
  std::vector<std::optional<std::string_view>> serializedRows;
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

  *result = std::dynamic_pointer_cast<RowVector>(
      velox::row::UnsafeRowDeserializer::deserialize(
          serializedRows, type, pool));
}

// static
void UnsafeRowVectorSerde::registerVectorSerde() {
  velox::registerVectorSerde(std::make_unique<UnsafeRowVectorSerde>());
}

// static
void UnsafeRowVectorSerde::registerNamedVectorSerde() {
  velox::registerNamedVectorSerde(
      VectorSerde::Kind::kUnsafeRow, std::make_unique<UnsafeRowVectorSerde>());
}

} // namespace facebook::velox::serializer::spark
