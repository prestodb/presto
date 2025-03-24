/*
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

#include "velox/core/ITypedExpr.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::presto::operators {

struct RawBuffer {
  RawBuffer(char* buffer, uint32_t size) : buffer_(buffer), size_(size) {}

  void append(int32_t value) {
    VELOX_CHECK_LE(position_, size_);
    buffer_[position_++] = value;
  }

  void reset() {
    position_ = 0;
  }

  char* data() {
    return buffer_;
  }

  uint32_t position() const {
    return position_;
  }

  char* buffer_;
  uint32_t position_ = 0;
  uint64_t size_ = 0;
};

/**
 * KeyValueSerializer is responsible for serializing sort keys from a
 * RowVector source. The key is generated so that a
 * lexicographical sort of the key will produce the ordering. 
 *
 * Based on Hive's BinarySortableSerDe:
 *
 * BinarySortableSerDe can be used to write data in a way that the data can be
 * compared byte-by-byte with the same order.
 *
 * Methods:
 * - serialize: Serializes data at a given index into a ByteOutputStream.
 * - serializeUnsafe: Serializes data at a given index into a RawBuffer without
 *   checking for buffer overflow.
 */

class KeyValueSerializer {
 public:
  KeyValueSerializer(
      const velox::RowVectorPtr& source,
      std::vector<velox::core::SortOrder> sortOrders,
      std::vector<velox::core::FieldAccessTypedExprPtr> fields)
      : rowVector_(source),
        rowType_(velox::asRowType(source->type())),
        fields_(std::move(fields)),
        sortOrders_(std::move(sortOrders)) {
    fieldChannels_.reserve(fields_.size());
    VELOX_CHECK_EQ(fields_.size(), sortOrders_.size());
    for (int32_t i = 0; i < fields_.size(); ++i) {
      fieldChannels_.emplace_back(
          i, exprToChannel(fields_[i].get(), *rowType_));
    }
  }

  void serialize(velox::vector_size_t index, velox::ByteOutputStream* out);

  // Serialize the data into an raw buffer, the caller needs to ensure there
  // serialized data won't overflow the buffer.
  void serializeUnsafe(velox::vector_size_t index, RawBuffer* out);

  std::vector<velox::column_index_t> getSortFieldChannels() {
    std::vector<velox::column_index_t> channels;
    channels.reserve(fieldChannels_.size());
    for (const auto& channel : fieldChannels_) {
      channels.push_back(channel.second);
    }
    return channels;
  }

 private:
  template <typename T>
  void serializeInternal(velox::vector_size_t index, T* out);

  velox::column_index_t exprToChannel(
      const velox::core::ITypedExpr* expr,
      const velox::RowType& type) {
    if (auto field =
            dynamic_cast<const velox::core::FieldAccessTypedExpr*>(expr)) {
      return type.getChildIdx(field->name());
    }
    if (dynamic_cast<const velox::core::ConstantTypedExpr*>(expr)) {
      return kConstantChannel;
    }
    VELOX_UNREACHABLE("Expression must be field access or constant");
    return 0; // not reached.
  }

  const velox::RowVectorPtr rowVector_;
  const velox::RowTypePtr rowType_;
  std::vector<velox::core::FieldAccessTypedExprPtr> fields_;
  std::vector<velox::core::SortOrder> sortOrders_;
  std::vector<std::pair<int32_t, velox::column_index_t>> fieldChannels_;

  static constexpr velox::column_index_t kConstantChannel =
      std::numeric_limits<velox::column_index_t>::max();
};
} // namespace facebook::presto::operators
