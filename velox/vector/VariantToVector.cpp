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

#include "velox/vector/VariantToVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox {
namespace {

VectorPtr callMakeVector(
    TypePtr type,
    const std::vector<Variant>& data,
    memory::MemoryPool* pool);

template <TypeKind KIND, typename = void>
struct VariantToVector {
  static VectorPtr makeVector(
      TypePtr type,
      const std::vector<Variant>& /*data*/,
      memory::MemoryPool* /*pool*/) {
    VELOX_NYI("Type not supported: {}", type->toString());
  }
};

template <>
struct VariantToVector<TypeKind::HUGEINT> {
  static VectorPtr makeVector(
      TypePtr type,
      const std::vector<Variant>& /*data*/,
      memory::MemoryPool* /*pool*/) {
    VELOX_NYI("Type not supported: {}", type->toString());
  }
};

template <TypeKind KIND>
struct VariantToVector<
    KIND,
    std::enable_if_t<
        TypeTraits<KIND>::isFixedWidth || KIND == TypeKind::VARCHAR ||
            KIND == TypeKind::VARBINARY || KIND == TypeKind::OPAQUE,
        void>> {
  static constexpr bool kIsOpaque = (KIND == TypeKind::OPAQUE);
  static VectorPtr makeVector(
      TypePtr type,
      const std::vector<Variant>& data,
      memory::MemoryPool* pool) {
    using T = typename TypeTraits<KIND>::NativeType;

    // Allocate nulls and data buffers and set all values to null by default.
    const vector_size_t dataSize = data.size();
    BufferPtr valuesBuffer = AlignedBuffer::allocate<T>(dataSize, pool);
    BufferPtr nulls = allocateNulls(dataSize, pool, bits::kNull);

    // Create flat vector to store all the values.
    auto values = std::make_shared<FlatVector<T>>(
        pool,
        type,
        nulls,
        dataSize,
        std::move(valuesBuffer),
        std::vector<BufferPtr>());

    // Populate data into flat vector.
    for (size_t i = 0; i < dataSize; i++) {
      if (!data[i].isNull()) {
        if constexpr (kIsOpaque) {
          values->set(i, T(data[i].value<KIND>().obj));
        } else {
          values->set(i, T(data[i].value<KIND>()));
        }
      }
    }
    return values;
  }
};

template <>
struct VariantToVector<TypeKind::ARRAY> {
  static VectorPtr makeVector(
      TypePtr type,
      const std::vector<Variant>& data,
      memory::MemoryPool* pool) {
    // Create offsets, sizes and nulls buffers.
    vector_size_t size = data.size();
    BufferPtr offsets = allocateOffsets(size, pool);
    BufferPtr sizes = allocateSizes(size, pool);
    BufferPtr nulls = allocateNulls(size, pool);
    auto rawOffsets = offsets->asMutable<vector_size_t>();
    auto rawSizes = sizes->asMutable<vector_size_t>();
    auto rawNulls = nulls->asMutable<uint64_t>();

    // Iterate through array values and set values in offsets and sizes buffers.
    // Create container for underlying array elements to create an elements
    // vector.
    std::vector<Variant> elements;
    vector_size_t index = 0;
    vector_size_t nullCount = 0;
    for (size_t i = 0; i < data.size(); ++i) {
      auto isNull = data[i].isNull();
      *rawOffsets++ = index;
      *rawSizes++ = !isNull ? data[i].array().size() : 0;
      if (isNull) {
        ++nullCount;
        bits::setNull(rawNulls, i, true);
        continue;
      }
      for (const auto& arrayElement : data[i].array()) {
        elements.push_back(arrayElement);
        ++index;
      }
    }

    // Create child elements vector with all the array values.
    TypePtr elementType = type->childAt(0);
    auto elementsVector = callMakeVector(elementType, elements, pool);

    return std::make_shared<ArrayVector>(
        pool,
        type,
        nulls,
        size,
        offsets,
        sizes,
        std::move(elementsVector),
        nullCount);
  }
};

template <>
struct VariantToVector<TypeKind::MAP> {
  static VectorPtr makeVector(
      TypePtr type,
      const std::vector<Variant>& data,
      memory::MemoryPool* pool) {
    // Create offsets, sizes and nulls buffers.
    vector_size_t size = data.size();
    BufferPtr offsets = allocateOffsets(size, pool);
    BufferPtr sizes = allocateSizes(size, pool);
    BufferPtr nulls = allocateNulls(size, pool);
    auto rawOffsets = offsets->asMutable<vector_size_t>();
    auto rawSizes = sizes->asMutable<vector_size_t>();
    auto rawNulls = nulls->asMutable<uint64_t>();

    // Iterate through map (key,value) pairs and set offsets, sizes and nulls
    // buffers. Create container for underlying key and value elements to create
    // child vectors.
    std::vector<Variant> keys;
    std::vector<Variant> values;
    vector_size_t index = 0;
    vector_size_t nullCount = 0;
    for (size_t i = 0; i < data.size(); ++i) {
      auto isNull = data[i].isNull();
      *rawOffsets++ = index;
      *rawSizes++ = !isNull ? data[i].map().size() : 0;
      if (isNull) {
        ++nullCount;
        bits::setNull(rawNulls, i, true);
        continue;
      }
      for (const auto& [key, value] : data[i].map()) {
        keys.push_back(key);
        values.push_back(value);
        ++index;
      }
    }

    // Create keys and values vector with corresponding values.
    auto keysVector = callMakeVector(type->childAt(0), keys, pool);
    auto valuesVector = callMakeVector(type->childAt(1), values, pool);

    return std::make_shared<MapVector>(
        pool,
        type,
        nulls,
        size,
        offsets,
        sizes,
        std::move(keysVector),
        std::move(valuesVector),
        nullCount);
  }
};

template <>
struct VariantToVector<TypeKind::ROW> {
  static VectorPtr makeVector(
      TypePtr type,
      const std::vector<Variant>& data,
      memory::MemoryPool* pool) {
    vector_size_t size = data.size();
    BufferPtr nulls = allocateNulls(size, pool);
    auto rawNulls = nulls->asMutable<uint64_t>();

    auto childCount = type->size();
    std::vector<std::vector<Variant>> children;
    children.reserve(childCount);
    for (size_t i = 0; i < childCount; ++i) {
      std::vector<Variant> child;
      child.reserve(size);
      children.push_back(child);
    }

    // Populate data for each of the columns.
    for (size_t i = 0; i < data.size(); ++i) {
      if (data[i].isNull()) {
        bits::setNull(rawNulls, i, true);
        continue;
      }
      const auto& row = data[i].row();
      VELOX_CHECK_EQ(row.size(), children.size());
      for (size_t j = 0; j < row.size(); ++j) {
        children[j].push_back(row[j]);
      }
    }

    std::vector<VectorPtr> childVectors;
    childVectors.reserve(childCount);
    for (size_t i = 0; i < childCount; ++i) {
      // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
      childVectors.push_back(
          callMakeVector(type->childAt(i), children[i], pool));
    }

    return std::make_shared<RowVector>(pool, type, nulls, size, childVectors);
  }
};

VectorPtr callMakeVector(
    TypePtr type,
    const std::vector<Variant>& data,
    memory::MemoryPool* pool) {
  return VELOX_DYNAMIC_TYPE_DISPATCH_METHOD_ALL(
      VariantToVector, makeVector, type->kind(), type, data, pool);
}

} // namespace

VectorPtr variantToVector(
    const TypePtr& type,
    const Variant& value,
    memory::MemoryPool* pool) {
  if (value.isNull()) {
    return BaseVector::createNullConstant(type, 1, pool);
  } else if (type->isPrimitiveType()) {
    return BaseVector::createConstant(type, value, 1, pool);
  }

  auto variantVector = callMakeVector(type, {value}, pool);
  return BaseVector::wrapInConstant(1, 0, std::move(variantVector));
}

} // namespace facebook::velox
