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
#include "velox/serializers/UnsafeRowSerde.h"
#include "velox/row/UnsafeRowDeserializer.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"

namespace facebook::velox::batch {

BatchSerdeStatus UnsafeRowKeySerializer::serializeKeys(
    const std::shared_ptr<RowVector>& vector,
    const std::vector<uint64_t>& keys,
    vector_size_t row,
    std::string_view& rowBuffer,
    std::string_view& serializedKeys,
    size_t& bufferOffset) {
  char* buffer = const_cast<char*>(rowBuffer.data());
  char* originalBuffer = buffer;
  size_t keySize = 0;
  buffer += sizeof(keySize);

  auto fieldTypes =
      std::dynamic_pointer_cast<const RowType>(vector->type())->children();
  // Traverse the keys and serialize them
  for (auto index : keys) {
    if (index >= fieldTypes.size()) {
      return BatchSerdeStatus::KeyIndexOutOfRange;
    }
    auto nextKeySize =
        velox::row::UnsafeRowDynamicSerializer::serialize(
            fieldTypes[index], vector->children()[index], buffer, row)
            .value_or(0);
    if (fieldTypes[index]->isFixedWidth()) {
      nextKeySize = velox::row::UnsafeRow::kFieldWidthBytes;
    }
    // Moving buffer pointer forward and updating the size
    keySize += nextKeySize;
    buffer += nextKeySize;
  }
  // Writing the composite key size and buffer
  *((size_t*)originalBuffer) = keySize;
  serializedKeys = std::string_view(originalBuffer + sizeof(keySize), keySize);
  bufferOffset = buffer - originalBuffer;
  return BatchSerdeStatus::Success;
}

void UnsafeRowVectorSerde::reset(const std::shared_ptr<RowVector>& vector) {
  velox::row::UnsafeRowDynamicSerializer::preloadVector(vector);
}

BatchSerdeStatus UnsafeRowVectorSerde::serializeRow(
    const std::shared_ptr<RowVector>& vector,
    const vector_size_t row,
    std::string_view& serializedKeys,
    std::string_view& serializedRow) {
  // Get the row size without writing to the buffer
  size_t rowSize = velox::row::UnsafeRowDynamicSerializer::getSizeRow(
      vector->type(), vector.get(), row);

  // Check if the current allocated buffer is large enough for this row
  if (rowBuffer_.data() == nullptr || rowSize > rowBuffer_.size()) {
    // Need more memory if we are serializing keys as well
    // We allocate at least twice the needed size so if the new rows are
    // bigger we do not have re-allocate
    // In case there are keys we allocate three times the size
    // The previously allocated buffer will be deallocated automatically
    auto allocationSize = keys_.has_value() ? rowSize * 10 : rowSize * 5;
    rowBufferPtr_ = AlignedBuffer::allocate<char>(allocationSize, pool_, true);
    VELOX_CHECK(rowBufferPtr_ != nullptr);
    rowBuffer_ =
        std::string_view(rowBufferPtr_->asMutable<char>(), allocationSize);
    std::memset(const_cast<char*>(rowBuffer_.data()), 0, allocationSize);
  }

  size_t rowOffset = 0;
  // first, writing the keys if any
  size_t offset;
  if (keys_.has_value() && keySerializer_ != nullptr) {
    auto res = keySerializer_->serializeKeys(
        vector, keys_.value(), row, rowBuffer_, serializedKeys, offset);
    if (res != BatchSerdeStatus::Success) {
      return res;
    }
    rowOffset += offset;
  }

  // Preparing the buffer location for the row itself and its size
  char* rowBuffer = const_cast<char*>(rowBuffer_.data()) + rowOffset;
  char* originalRowBuffer = rowBuffer;

  // Serialize the row and after make room for its size
  rowBuffer += sizeof(size_t);
  auto size = velox::row::UnsafeRowDynamicSerializer::serialize(
                  vector->type(), vector, rowBuffer, row)
                  .value_or(0);

  // Writing the size
  *((size_t*)originalRowBuffer) = size;

  // Return the result
  serializedRow = std::string_view(rowBuffer, size);
  return BatchSerdeStatus::Success;
}

BatchSerdeStatus UnsafeRowVectorSerde::deserializeVector(
    const std::vector<std::optional<std::string_view>>& values,
    const std::shared_ptr<const RowType> type,
    std::shared_ptr<RowVector>* result) {
  // Use row pointer to finish deserialization
  auto resultVector =
      velox::row::UnsafeRowDynamicVectorDeserializer::deserializeComplex(
          values, type, pool_);
  if (!resultVector) {
    return BatchSerdeStatus::DeserializationError;
  }

  // Assign result
  *result = std::dynamic_pointer_cast<RowVector>(resultVector);

  return BatchSerdeStatus::Success;
}
} // namespace facebook::velox::batch
