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
#include "velox/serializers/RowSerializer.h"

namespace facebook::velox::serializer::spark {

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
    const Options* options) {
  return std::make_unique<RowSerializer<row::UnsafeRowFast>>(
      streamArena->pool(), options);
}

void UnsafeRowVectorSerde::deserialize(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    const Options* options) {
  std::vector<std::optional<std::string_view>> serializedRows;
  std::vector<std::unique_ptr<std::string>> serializedBuffers;
  RowDeserializer<std::optional<std::string_view>>::deserialize(
      source, serializedRows, serializedBuffers, options);

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
