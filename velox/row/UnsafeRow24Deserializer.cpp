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

#include "velox/row/UnsafeRow24Deserializer.h"
#include "velox/functions/lib/DynamicFlatVector.h"

#include <vector>

namespace facebook::velox::row {
namespace {

size_t nullSizeBytes(size_t s) {
  return (s + 63) / 64 * 8;
}

bool hasNull(const std::vector<const char*>& rows) {
  return std::find(rows.begin(), rows.end(), nullptr) != rows.end();
}

BufferPtr makeNullBuffer(
    memory::MemoryPool* pool,
    const std::vector<const char*>& rows) {
  if (!hasNull(rows)) {
    return nullptr;
  }
  auto result =
      AlignedBuffer::allocate<bool>(rows.size(), pool, bits::kNotNull);
  auto* rawNulls = result->asMutable<uint64_t>();
  for (int i = 0; i < rows.size(); ++i) {
    if (!rows[i])
      bits::setNull(rawNulls, i);
  }
  return result;
}

// Decodes an uint64_t that points to variable length data.
std::pair<const char*, uint32_t> decodeVarOffset(
    const char* base,
    const char* value) {
  const uint64_t offsetAndSize = *reinterpret_cast<const uint64_t*>(value);
  return {base + (offsetAndSize >> 32), static_cast<uint32_t>(offsetAndSize)};
}

// The Deserialize family of functions returns a Vector with length equal to
// valuePointers.size() with the given type. Deserialize dispatches to the
// type-specific implementations. basePointers are used when decoding
// variable-length data (arrays, maps, structs, and strings).
VectorPtr Deserialize(
    const TypePtr& type,
    memory::MemoryPool* pool,
    const std::vector<const char*>& basePointers,
    const std::vector<const char*>& valuePointers);

// FixedWidth types mostly have the same representation in Vectors and
// UnsafeRow, with the exception of bools (bitpacked) and timestamp (micros
// only).
template <TypeKind kind>
VectorPtr DeserializeFixedWidth(
    memory::MemoryPool* pool,
    const std::vector<const char*>& valuePointers) {
  static_assert(TypeTraits<kind>::isFixedWidth);
  using T = typename TypeTraits<kind>::NativeType;
  BufferPtr nulls = makeNullBuffer(pool, valuePointers);
  BufferPtr values = AlignedBuffer::allocate<T>(valuePointers.size(), pool);
  T* data = values->template asMutable<T>();
  for (int i = 0; i < valuePointers.size(); ++i) {
    if (!valuePointers[i]) {
      continue;
    }
    if constexpr (kind == TypeKind::BOOLEAN) {
      bits::setBit(data, i, *reinterpret_cast<const bool*>(valuePointers[i]));
    } else if constexpr (kind == TypeKind::TIMESTAMP) {
      data[i] = Timestamp::fromMicros(
          *reinterpret_cast<const int64_t*>(valuePointers[i]));
    } else {
      data[i] = *reinterpret_cast<const T*>(valuePointers[i]);
    }
  }
  return std::make_shared<FlatVector<T>>(
      pool,
      ScalarType<kind>::create(),
      std::move(nulls),
      valuePointers.size(),
      std::move(values),
      /*stringBuffers=*/std::vector<BufferPtr>{});
}

// Strings are the simplest variable-length type.
FlatVectorPtr<StringView> DeserializeString(
    const TypePtr& type,
    memory::MemoryPool* pool,
    const std::vector<const char*>& basePointers,
    const std::vector<const char*>& valuePointers) {
  const vector_size_t size = valuePointers.size();
  auto result = std::dynamic_pointer_cast<FlatVector<StringView>>(
      BaseVector::create(type, size, pool));
  for (int i = 0; i < size; ++i) {
    if (valuePointers[i]) {
      auto [data, size] = decodeVarOffset(basePointers[i], valuePointers[i]);
      // This copies the data into the FlatVector's buffers if needed.
      // It might be possible to avoid this copy, but it would significantly
      // complicate lifetime management.
      result->set(i, StringView(data, size));
    } else {
      result->setNull(i, true);
    }
  }
  return result;
}

// valuePointers are resolved into pointers to the beginning of each row by
// Deserialize because this function is also the primary entry point used by
// UnsafeRow24Deserializer, where the input is pointers to the beginning of
// each row.
RowVectorPtr DeserializeRow(
    const std::shared_ptr<const RowType>& type,
    memory::MemoryPool* pool,
    const std::vector<const char*>& rows) {
  const std::vector<TypePtr>& fieldTypes = type->children();
  auto nulls = makeNullBuffer(pool, rows);
  std::vector<const char*> fieldPointers(rows.size());
  std::vector<VectorPtr> fields;
  std::size_t offset = nullSizeBytes(fieldTypes.size());
  for (int i = 0; i < fieldTypes.size(); ++i) {
    for (int row = 0; row < rows.size(); ++row) {
      // Converting fixed-width fields directly into a VectorPtr without
      // producing a valuePointer vector would yield a significant speedup.
      fieldPointers[row] = rows[row] && !bits::isBitSet(rows[row], i)
          ? rows[row] + offset
          : nullptr;
    }
    fields.push_back(Deserialize(fieldTypes[i], pool, rows, fieldPointers));
    offset += sizeof(uint64_t);
  }
  return std::make_shared<RowVector>(
      pool, type, nulls, rows.size(), std::move(fields));
}

uint8_t arrayElementStride(TypeKind kind) {
  switch (kind) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return 1;
    case TypeKind::SMALLINT:
      return 2;
    case TypeKind::INTEGER:
      return 4;
    case TypeKind::BIGINT:
      return 8;
    case TypeKind::REAL:
      return 4;
    case TypeKind::DOUBLE:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
    case TypeKind::TIMESTAMP:
      return 8;
    case TypeKind::DATE:
      return 4;
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      return 8;
    default:
      VELOX_NYI();
  }
}

ArrayVectorPtr DeserializeArray(
    const std::shared_ptr<const ArrayType>& type,
    memory::MemoryPool* pool,
    const std::vector<const char*>& basePointers,
    const std::vector<const char*>& valuePointers) {
  const vector_size_t numArrays = valuePointers.size();
  BufferPtr nulls = makeNullBuffer(pool, valuePointers);
  BufferPtr offsetsBuffer = allocateOffsets(numArrays, pool);
  BufferPtr sizesBuffer = allocateSizes(numArrays, pool);
  vector_size_t* offsets = offsetsBuffer->asMutable<vector_size_t>();
  vector_size_t* sizes = sizesBuffer->asMutable<vector_size_t>();
  std::vector<const char*> elementBases;
  std::vector<const char*> elements;
  const uint32_t elementStride =
      arrayElementStride(type->elementType()->kind());
  for (int i = 0; i < numArrays; ++i) {
    offsets[i] = elements.size();
    if (!valuePointers[i]) {
      sizes[i] = 0;
      continue;
    }
    auto [data, size] = decodeVarOffset(basePointers[i], valuePointers[i]);
    int64_t arrayLength = *reinterpret_cast<const int64_t*>(data);
    sizes[i] = arrayLength;
    VELOX_CHECK(arrayLength >= 0 && arrayLength <= size); // Sanity check.
    const char* elementNulls = data + sizeof(int64_t);
    const char* elementPointer = elementNulls + nullSizeBytes(arrayLength);
    for (int e = 0; e < arrayLength; ++e) {
      elementBases.push_back(data);
      elements.push_back(
          bits::isBitSet(elementNulls, e) ? nullptr : elementPointer);
      elementPointer += elementStride;
    }
    DCHECK_EQ(elements.size(), offsets[i] + sizes[i]);
  }
  return std::make_shared<ArrayVector>(
      pool,
      type,
      std::move(nulls),
      numArrays,
      std::move(offsetsBuffer),
      std::move(sizesBuffer),
      Deserialize(type->elementType(), pool, elementBases, elements));
}

MapVectorPtr DeserializeMap(
    const std::shared_ptr<const MapType>& type,
    memory::MemoryPool* pool,
    const std::vector<const char*>& basePointers,
    const std::vector<const char*>& valuePointers) {
  const vector_size_t numMaps = valuePointers.size();
  BufferPtr nulls = makeNullBuffer(pool, valuePointers);
  BufferPtr offsetsBuffer = allocateOffsets(numMaps, pool);
  BufferPtr sizesBuffer = allocateSizes(numMaps, pool);
  vector_size_t* offsets = offsetsBuffer->asMutable<vector_size_t>();
  vector_size_t* sizes = sizesBuffer->asMutable<vector_size_t>();
  std::vector<const char*> keyBases;
  std::vector<const char*> keys;
  std::vector<const char*> valueBases;
  std::vector<const char*> values;
  const uint32_t keyStride = arrayElementStride(type->keyType()->kind());
  const uint32_t valueStride = arrayElementStride(type->valueType()->kind());
  for (int i = 0; i < numMaps; ++i) {
    offsets[i] = keys.size();
    if (!valuePointers[i]) {
      sizes[i] = 0; // The contract is vague on whether this can be junk.
      continue;
    }
    auto [data, size] = decodeVarOffset(basePointers[i], valuePointers[i]);
    // Memory layout:
    // int64_t keysBytes
    //    Array keys (int64_t len; int64_t nulls[]; T data[])
    //    Array values (int64_t len; int64_t nulls[]; T data[])
    const char* keyBegin = data + sizeof(int64_t);
    const char* valueBegin = keyBegin + *reinterpret_cast<const int64_t*>(data);
    DCHECK_GT(valueBegin, keyBegin);
    const int64_t mapLength = *reinterpret_cast<const int64_t*>(keyBegin);
    // DCHECK_EQ(mapLength, *reinterpret_cast<const int64_t*>(valueBegin));
    sizes[i] = mapLength;
    VELOX_CHECK(mapLength >= 0 && mapLength <= size); // Sanity check.
    const char* keyNulls = keyBegin + sizeof(int64_t);
    const char* keyPointer = keyNulls + nullSizeBytes(mapLength);
    const char* valueNulls = valueBegin + sizeof(int64_t);
    const char* valuePointer = valueNulls + nullSizeBytes(mapLength);
    for (int e = 0; e < mapLength; ++e) {
      keyBases.push_back(keyBegin);
      keys.push_back(bits::isBitSet(keyNulls, e) ? nullptr : keyPointer);
      valueBases.push_back(valueBegin);
      values.push_back(bits::isBitSet(valueNulls, e) ? nullptr : valuePointer);
      keyPointer += keyStride;
      valuePointer += valueStride;
    }
    DCHECK_EQ(keys.size(), values.size());
    DCHECK_EQ(keys.size(), offsets[i] + sizes[i]);
  }
  return std::make_shared<MapVector>(
      pool,
      type,
      std::move(nulls),
      numMaps,
      std::move(offsetsBuffer),
      std::move(sizesBuffer),
      Deserialize(type->keyType(), pool, keyBases, keys),
      Deserialize(type->valueType(), pool, valueBases, values));
}

VectorPtr Deserialize(
    const TypePtr& type,
    memory::MemoryPool* pool,
    const std::vector<const char*>& basePointers,
    const std::vector<const char*>& valuePointers) {
  switch (type->kind()) {
#define SCALAR_CASE(kind) \
  case TypeKind::kind:    \
    return DeserializeFixedWidth<TypeKind::kind>(pool, valuePointers)
    SCALAR_CASE(BOOLEAN);
    SCALAR_CASE(TINYINT);
    SCALAR_CASE(SMALLINT);
    SCALAR_CASE(INTEGER);
    SCALAR_CASE(BIGINT);
    SCALAR_CASE(REAL);
    SCALAR_CASE(DOUBLE);
    SCALAR_CASE(TIMESTAMP);
    SCALAR_CASE(DATE);
#undef SCALAR_CASE
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return DeserializeString(type, pool, basePointers, valuePointers);
    case TypeKind::ARRAY:
      return DeserializeArray(
          std::dynamic_pointer_cast<const ArrayType>(type),
          pool,
          basePointers,
          valuePointers);
    case TypeKind::MAP:
      return DeserializeMap(
          std::dynamic_pointer_cast<const MapType>(type),
          pool,
          basePointers,
          valuePointers);
    case TypeKind::ROW: {
      std::vector<const char*> resolved(valuePointers.size());
      for (int i = 0; i < valuePointers.size(); ++i) {
        auto [data, size] = decodeVarOffset(basePointers[i], valuePointers[i]);
        resolved[i] = data;
      }
      return DeserializeRow(
          std::dynamic_pointer_cast<const RowType>(type), pool, resolved);
    }

    default:
      VELOX_NYI(
          "UnsafeRow deserialization of {} is not supported", type->toString());
  }
}

} // namespace

std::unique_ptr<UnsafeRow24Deserializer> UnsafeRow24Deserializer::Create(
    RowTypePtr rowType) {
  // The current implementation is stateless, so no object is really needed.
  struct Wrapper final : public UnsafeRow24Deserializer {
    explicit Wrapper(RowTypePtr rowType) : type_(std::move(rowType)) {}

    RowVectorPtr DeserializeRows(
        memory::MemoryPool* pool,
        const std::vector<std::string_view>& rows) final {
      valuePointers_.resize(rows.size());
      for (int i = 0; i < rows.size(); ++i) {
        valuePointers_[i] = rows[i].data();
      }
      auto result = DeserializeRow(type_, pool, valuePointers_);
      VELOX_CHECK(result->type()->isRow());
      return result;
    }

    RowTypePtr type_;
    std::vector<const char*> valuePointers_;
  };

  return std::make_unique<Wrapper>(rowType);
}

} // namespace facebook::velox::row
