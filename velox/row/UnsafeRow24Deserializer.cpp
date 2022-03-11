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

struct NullBuffer {
  NullBuffer(size_t size, memory::MemoryPool* pool)
      : buf_(AlignedBuffer::allocate<bool>(size, pool, bits::kNotNull)) {}

  FOLLY_ALWAYS_INLINE void setNull(int i) {
    bits::setNull(raw_, i);
  }

  BufferPtr buf_;
  uint64_t* raw_{buf_->asMutable<uint64_t>()};
};

BufferPtr makeNullBuffer(
    memory::MemoryPool* pool,
    const std::vector<const char*>& rows) {
  if (!hasNull(rows)) {
    return nullptr;
  }
  NullBuffer nulls(rows.size(), pool);
  for (int i = 0; i < rows.size(); ++i) {
    if (!rows[i])
      nulls.setNull(i);
  }
  return std::move(nulls.buf_);
}

// Decodes an uint64_t that points to variable length data.
FOLLY_ALWAYS_INLINE std::pair<const char*, uint32_t> decodeVarOffset(
    const char* base,
    const char* value) {
  const uint64_t offsetAndSize = *reinterpret_cast<const uint64_t*>(value);
  return {base + (offsetAndSize >> 32), static_cast<uint32_t>(offsetAndSize)};
}

// The Deserialize family of functions returns a Vector with length equal to
// valuePointers.size() with the given type. Deserialize dispatches to the
// type-specific implementations. basePointers are used when decoding
// variable-length data (arrays, maps, structs, and strings).

// DeserializeVariableLength is allowed to mutate the valuePointer array.
VectorPtr DeserializeVariableLength(
    const TypePtr& type,
    memory::MemoryPool* pool,
    const std::vector<const char*>& basePointers,
    std::vector<const char*>& valuePointers);

// FixedWidth types mostly have the same representation in Vectors and
// UnsafeRow, with the exception of bools (bitpacked) and timestamp (micros
// only).
template <TypeKind kind, typename ValuePointerNextCallback>
VectorPtr DeserializeFixedWidth(
    memory::MemoryPool* pool,
    std::size_t size,
    ValuePointerNextCallback valuePointers) {
  static_assert(TypeTraits<kind>::isFixedWidth);
  using T = typename TypeTraits<kind>::NativeType;
  NullBuffer nulls(size, pool);
  bool hasNull = false;
  BufferPtr values = AlignedBuffer::allocate<T>(size, pool);
  T* data = values->template asMutable<T>();
  for (int i = 0; i < size; ++i) {
    const char* valuePointer = valuePointers();
    if (UNLIKELY(!valuePointer)) {
      hasNull = true;
      nulls.setNull(i);
      continue;
    }
    if constexpr (kind == TypeKind::BOOLEAN) {
      bits::setBit(data, i, *reinterpret_cast<const bool*>(valuePointer));
    } else if constexpr (kind == TypeKind::TIMESTAMP) {
      data[i] = Timestamp::fromMicros(
          *reinterpret_cast<const int64_t*>(valuePointer));
    } else {
      data[i] = *reinterpret_cast<const T*>(valuePointer);
    }
  }
  return std::make_shared<FlatVector<T>>(
      pool,
      ScalarType<kind>::create(),
      hasNull ? std::move(nulls.buf_) : nullptr,
      size,
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

// Memory layout:
//   int64_t nulls[]
//   int64_t elements[]
// Note that elements are padded out to 8 bytes each, even for e.g. bools.
RowVectorPtr DeserializeRow(
    const std::shared_ptr<const RowType>& type,
    memory::MemoryPool* pool,
    const std::vector<const char*>& rows) {
  const std::vector<TypePtr>& fieldTypes = type->children();
  auto nulls = makeNullBuffer(pool, rows);
  std::vector<VectorPtr> fields;
  std::size_t offset = nullSizeBytes(fieldTypes.size());
  for (int field = 0; field < fieldTypes.size(); ++field) {
    auto row = rows.begin();
    auto fieldPointerCallback = [&row, field, offset] {
      const char* data = *row++;
      return data && !bits::isBitSet(data, field) ? data + offset : nullptr;
    };
    switch (fieldTypes[field]->kind()) {
#define FIXED_WIDTH(kind)                                   \
  case TypeKind::kind:                                      \
    fields.push_back(DeserializeFixedWidth<TypeKind::kind>( \
        pool, rows.size(), fieldPointerCallback));          \
    break
      FIXED_WIDTH(BOOLEAN);
      FIXED_WIDTH(TINYINT);
      FIXED_WIDTH(SMALLINT);
      FIXED_WIDTH(INTEGER);
      FIXED_WIDTH(BIGINT);
      FIXED_WIDTH(REAL);
      FIXED_WIDTH(DOUBLE);
      FIXED_WIDTH(TIMESTAMP);
      FIXED_WIDTH(DATE);
#undef FIXED_WIDTH
      default: {
        std::vector<const char*> fieldPointers(rows.size());
        for (int i = 0; i < rows.size(); ++i) {
          fieldPointers[i] = fieldPointerCallback();
        }
        fields.push_back(DeserializeVariableLength(
            fieldTypes[field], pool, rows, fieldPointers));
      }
    }
    offset += sizeof(uint64_t);
  }
  return std::make_shared<RowVector>(
      pool, type, nulls, rows.size(), std::move(fields));
}

template <TypeKind kind, int stride>
VectorPtr DeserializeFixedWidthArrayElements(
    memory::MemoryPool* pool,
    const std::vector<const char*>& arrays,
    const vector_size_t* const offsets,
    const vector_size_t* const sizes) {
  const int totalElements = offsets[arrays.size()];
  // Initial position is before the first element.
  int64_t arrayIndex = -1;
  int64_t lastElementIndex = 0;
  const char* currentNulls = nullptr;
  const char* currentData = nullptr;
  int64_t currentElementIndex = 0;
  return DeserializeFixedWidth<kind>(pool, totalElements, [&] {
    if (UNLIKELY(currentElementIndex == lastElementIndex)) {
      // Skip forward to the next non-empty array.
      do {
        ++arrayIndex;
      } while (UNLIKELY(sizes[arrayIndex] == 0));
      DCHECK_EQ(
          sizes[arrayIndex],
          *reinterpret_cast<const int64_t*>(arrays[arrayIndex]));
      currentElementIndex = 0;
      lastElementIndex = sizes[arrayIndex] - 1;
      currentNulls = arrays[arrayIndex] + sizeof(int64_t); // Skip length.
      currentData = currentNulls + nullSizeBytes(sizes[arrayIndex]);
    } else {
      ++currentElementIndex;
    }
    return bits::isBitSet(currentNulls, currentElementIndex)
        ? nullptr
        : currentData + currentElementIndex * stride;
  });
}

// Memory layout:
//   int64_t size
//   int64_t nulls[]
//   T elements[]
// Note that elements are not padded out to 8 bytes each unlike structs; bool
// arrays, for example, consume a total of 8 + size bytes.
ArrayVectorPtr DeserializeArray(
    const std::shared_ptr<const Type>& elementType,
    memory::MemoryPool* pool,
    const std::vector<const char*>& arrays) {
  const vector_size_t numArrays = arrays.size();
  BufferPtr nulls = makeNullBuffer(pool, arrays);
  BufferPtr offsetsBuffer = allocateOffsets(numArrays + 1, pool);
  BufferPtr sizesBuffer = allocateSizes(numArrays, pool);
  vector_size_t* offsets = offsetsBuffer->asMutable<vector_size_t>();
  vector_size_t* sizes = sizesBuffer->asMutable<vector_size_t>();
  // Pre-calculate sizes and offsets.
  offsets[0] = 0;
  for (int i = 0; i < numArrays; ++i) {
    int64_t arrayLength =
        arrays[i] ? *reinterpret_cast<const int64_t*>(arrays[i]) : 0;
    sizes[i] = arrayLength;
    DCHECK(arrayLength >= 0 && arrayLength <= 1 << 20); // Sanity check.
    offsets[i + 1] = offsets[i] + sizes[i];
  }
  VectorPtr elementsVector;
  switch (elementType->kind()) {
#define FIXED_WIDTH(kind, stride)                                   \
  case TypeKind::kind:                                              \
    elementsVector =                                                \
        DeserializeFixedWidthArrayElements<TypeKind::kind, stride>( \
            pool, arrays, offsets, sizes);                          \
    break;
    FIXED_WIDTH(BOOLEAN, 1);
    FIXED_WIDTH(TINYINT, 1);
    FIXED_WIDTH(SMALLINT, 2);
    FIXED_WIDTH(INTEGER, 4);
    FIXED_WIDTH(BIGINT, 8);
    FIXED_WIDTH(REAL, 4);
    FIXED_WIDTH(DOUBLE, 8);
    FIXED_WIDTH(TIMESTAMP, 8);
    FIXED_WIDTH(DATE, 4);
#undef FIXED_WIDTH
    default: {
      std::vector<const char*> elementBases(offsets[numArrays]);
      std::vector<const char*> elements(offsets[numArrays]);
      int elementCount = 0;
      for (int i = 0; i < numArrays; ++i) {
        int64_t arrayLength = sizes[i];
        const char* elementNulls = arrays[i] + sizeof(int64_t);
        const char* elementPointer = elementNulls + nullSizeBytes(arrayLength);
        for (int e = 0; e < arrayLength; ++e) {
          elementBases[elementCount] = arrays[i];
          elements[elementCount++] =
              bits::isBitSet(elementNulls, e) ? nullptr : elementPointer;
          elementPointer += sizeof(int64_t);
        }
        DCHECK_EQ(elementCount, offsets[i] + sizes[i]);
      }
      elementsVector =
          DeserializeVariableLength(elementType, pool, elementBases, elements);
    }
  }
  return std::make_shared<ArrayVector>(
      pool,
      ARRAY(elementType),
      std::move(nulls),
      numArrays,
      std::move(offsetsBuffer),
      std::move(sizesBuffer),
      std::move(elementsVector));
}

// Memory layout:
// int64_t keysBytes
//    Array keys (int64_t len; int64_t nulls[]; T data[])
//    Array values (int64_t len; int64_t nulls[]; T data[])
MapVectorPtr DeserializeMap(
    const std::shared_ptr<const MapType>& type,
    memory::MemoryPool* pool,
    std::vector<const char*>& maps) {
  const vector_size_t numMaps = maps.size();
  BufferPtr nulls = makeNullBuffer(pool, maps);
  for (int i = 0; i < numMaps; ++i) {
    maps[i] = maps[i] ? maps[i] + 8 : nullptr;
  }
  // We use std::as_const to document maps should not be modified, since we
  // calculate the value array pointers from it below.
  auto keys = DeserializeArray(type->keyType(), pool, std::as_const(maps));
  VELOX_CHECK_EQ(keys->size(), numMaps);
  for (int i = 0; i < numMaps; ++i) {
    maps[i] = maps[i] ? maps[i] + *reinterpret_cast<const int64_t*>(maps[i] - 8)
                      : nullptr;
  }
  auto values = DeserializeArray(type->valueType(), pool, maps);
  VELOX_CHECK_EQ(values->size(), numMaps);
  VELOX_CHECK_EQ(
      keys->offsetAt(numMaps - 1) + keys->sizeAt(numMaps - 1),
      values->offsetAt(numMaps - 1) + values->sizeAt(numMaps - 1));
  // Debug-only sanity checks.
  for (int i = 0; i < numMaps; ++i) {
    DCHECK_EQ(keys->isNullAt(i), values->isNullAt(i));
    // DeserializeArray sets the size to zero for null arrays and sets the
    // offset.
    DCHECK_EQ(keys->sizeAt(i), values->sizeAt(i));
    DCHECK_EQ(keys->offsetAt(i), values->offsetAt(i));
  }
  return std::make_shared<MapVector>(
      pool,
      type,
      std::move(nulls),
      numMaps,
      keys->mutableOffsets(numMaps),
      keys->mutableSizes(numMaps),
      keys->elements(),
      values->elements());
}

// Dispatches to the type-specific deserialization function. Note there are
// optimizations for fixed-width types in some cases, so not all fixed-width
// vector deserialization routes through here.
VectorPtr DeserializeVariableLength(
    const TypePtr& type,
    memory::MemoryPool* pool,
    const std::vector<const char*>& basePointers,
    std::vector<const char*>& valuePointers) {
  auto resolveValuePointers = [&]() -> std::vector<const char*>& {
    for (int i = 0; i < valuePointers.size(); ++i) {
      if (valuePointers[i]) {
        auto [data, size] = decodeVarOffset(basePointers[i], valuePointers[i]);
        valuePointers[i] = data;
      }
    }
    return valuePointers;
  };
  switch (type->kind()) {
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return DeserializeString(type, pool, basePointers, valuePointers);
    case TypeKind::ARRAY:
      return DeserializeArray(
          std::dynamic_pointer_cast<const ArrayType>(type)->elementType(),
          pool,
          resolveValuePointers());
    case TypeKind::MAP:
      return DeserializeMap(
          std::dynamic_pointer_cast<const MapType>(type),
          pool,
          resolveValuePointers());
    case TypeKind::ROW:
      return DeserializeRow(
          std::dynamic_pointer_cast<const RowType>(type),
          pool,
          resolveValuePointers());
    default:
      VELOX_CHECK(false, "Unexpected type: {}", type->toString());
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
        const std::vector<const char*>& rows) final {
      return DeserializeRow(type_, pool, rows);
    }

    RowTypePtr type_;
  };

  return std::make_unique<Wrapper>(rowType);
}

} // namespace facebook::velox::row
