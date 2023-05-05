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
#include "velox/row/UnsafeRowSerializers.h"

namespace facebook::velox::serializer::spark {

void UnsafeRowVectorSerde::estimateSerializedSize(
    VectorPtr /* vector */,
    const folly::Range<const IndexRange*>& /* ranges */,
    vector_size_t** /* sizes */) {
  VELOX_UNSUPPORTED();
}

namespace {
class UnsafeRowVectorSerializer : public VectorSerializer {
 public:
  using TRowSize = uint32_t;

  explicit UnsafeRowVectorSerializer(StreamArena* streamArena)
      : pool_{streamArena->pool()} {}

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges) override {
    size_t totalSize = 0;
    auto fixedRowSize =
        row::UnsafeRowSerializer::getFixedSizeRow(asRowType(vector->type()));
    if (fixedRowSize.has_value()) {
      for (const auto& range : ranges) {
        totalSize += (fixedRowSize.value() + sizeof(TRowSize)) * range.size;
      }

    } else {
      for (const auto& range : ranges) {
        for (auto i = range.begin; i < range.begin + range.size; ++i) {
          auto rowSize = row::UnsafeRowSerializer::getSizeRow(vector.get(), i);
          totalSize += rowSize + sizeof(TRowSize);
        }
      }
    }

    if (totalSize == 0) {
      return;
    }

    auto* buffer = (char*)pool_->allocateZeroFilled(totalSize, 1);
    buffers_.push_back(
        ByteRange{(uint8_t*)buffer, (int32_t)totalSize, (int32_t)totalSize});

    size_t offset = 0;
    for (auto& range : ranges) {
      for (auto i = range.begin; i < range.begin + range.size; ++i) {
        // Write row data.
        auto rowSize = row::UnsafeRowSerializer::getSizeRow(vector.get(), i);
        TRowSize size = row::UnsafeRowSerializer::serialize(
                            vector, buffer + offset + sizeof(TRowSize), i)
                            .value_or(0);

        // Sanity check.
        VELOX_CHECK_EQ(rowSize, size);

        // Write raw size. Needs to be in big endian order.
        *(TRowSize*)(buffer + offset) = folly::Endian::big(size);
        offset += sizeof(TRowSize) + size;
      }
    }
  }

  void flush(OutputStream* stream) override {
    for (auto& buffer : buffers_) {
      stream->write((char*)buffer.buffer, buffer.position);
      pool_->free(buffer.buffer, buffer.size);
    }
    buffers_.clear();
  }

 private:
  memory::MemoryPool* const FOLLY_NONNULL pool_;
  std::vector<ByteRange> buffers_;
};
} // namespace

std::unique_ptr<VectorSerializer> UnsafeRowVectorSerde::createSerializer(
    RowTypePtr /* type */,
    int32_t /* numRows */,
    StreamArena* streamArena,
    const Options* /* options */) {
  return std::make_unique<UnsafeRowVectorSerializer>(streamArena);
}

void UnsafeRowVectorSerde::deserialize(
    ByteStream* source,
    velox::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    const Options* /* options */) {
  std::vector<std::optional<std::string_view>> serializedRows;
  while (!source->atEnd()) {
    // First read row size in big endian order.
    auto rowSize =
        folly::Endian::big(source->read<UnsafeRowVectorSerializer::TRowSize>());
    auto row = source->nextView(rowSize);
    VELOX_CHECK_EQ(row.size(), rowSize);
    serializedRows.push_back(row);
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

} // namespace facebook::velox::serializer::spark
