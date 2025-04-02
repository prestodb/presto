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

#include "velox/vector/fuzzer/Utils.h"
#include "velox/common/fuzzer/Utils.h"
#include "velox/expression/VectorWriters.h"
#include "velox/vector/NullsBuilder.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox {
namespace generator_spec_utils {

vector_size_t getRandomIndex(FuzzerGenerator& rng, vector_size_t maxIndex) {
  std::uniform_int_distribution<vector_size_t> indexGenerator(
      0, maxIndex); // generates index in [0, maxIndex]
  return indexGenerator(rng);
}

BufferPtr generateNullsBuffer(
    FuzzerGenerator& rng,
    memory::MemoryPool* pool,
    vector_size_t vectorSize,
    double nullProbability) {
  NullsBuilder builder{vectorSize, pool};
  for (size_t i = 0; i < vectorSize; ++i) {
    if (fuzzer::coinToss(rng, nullProbability)) {
      builder.setNull(i);
    }
  }
  return builder.build();
}

BufferPtr generateIndicesBuffer(
    FuzzerGenerator& rng,
    memory::MemoryPool* pool,
    vector_size_t bufferSize,
    vector_size_t baseVectorSize) {
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(bufferSize, pool);
  auto rawIndices = indices->asMutable<vector_size_t>();
  auto indicesGenerator =
      std::uniform_int_distribution<vector_size_t>(0, baseVectorSize - 1);

  for (size_t i = 0; i < bufferSize; ++i) {
    rawIndices[i] = indicesGenerator(rng);
  }
  return indices;
}
} // namespace generator_spec_utils

namespace fuzzer {
template <>
void writeOne<TypeKind::VARCHAR>(
    const variant& v,
    exec::GenericWriter& writer) {
  writer.template castTo<Varchar>() = v.value<TypeKind::VARCHAR>();
}

template <>
void writeOne<TypeKind::VARBINARY>(
    const variant& v,
    exec::GenericWriter& writer) {
  writer.template castTo<Varbinary>() = v.value<TypeKind::VARBINARY>();
}

template <>
void writeOne<TypeKind::ARRAY>(const variant& v, exec::GenericWriter& writer) {
  auto& writerTyped = writer.template castTo<Array<Any>>();
  const auto& elements = v.array();
  for (const auto& element : elements) {
    if (element.isNull()) {
      writerTyped.add_null();
    } else {
      VELOX_DYNAMIC_TYPE_DISPATCH(
          writeOne, element.kind(), element, writerTyped.add_item());
    }
  }
}

template <>
void writeOne<TypeKind::MAP>(const variant& v, exec::GenericWriter& writer) {
  auto& writerTyped = writer.template castTo<Map<Any, Any>>();
  const auto& map = v.map();
  for (const auto& pair : map) {
    const auto& key = pair.first;
    const auto& value = pair.second;
    VELOX_CHECK(!key.isNull());
    if (value.isNull()) {
      VELOX_DYNAMIC_TYPE_DISPATCH(
          writeOne, key.kind(), key, writerTyped.add_null());
    } else {
      auto writers = writerTyped.add_item();
      VELOX_DYNAMIC_TYPE_DISPATCH(
          writeOne, key.kind(), key, std::get<0>(writers));
      VELOX_DYNAMIC_TYPE_DISPATCH(
          writeOne, value.kind(), value, std::get<1>(writers));
    }
  }
}

template <>
void writeOne<TypeKind::ROW>(const variant& v, exec::GenericWriter& writer) {
  auto& writerTyped = writer.template castTo<DynamicRow>();
  const auto& elements = v.row();
  column_index_t i = 0;
  for (const auto& element : elements) {
    if (element.isNull()) {
      writerTyped.set_null_at(i);
    } else {
      VELOX_DYNAMIC_TYPE_DISPATCH(
          writeOne, element.kind(), element, writerTyped.get_writer_at(i));
    }
    i++;
  }
}
} // namespace fuzzer
} // namespace facebook::velox
