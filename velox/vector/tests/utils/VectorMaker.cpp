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
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::test {

// static
std::shared_ptr<const RowType> VectorMaker::rowType(
    std::vector<std::shared_ptr<const Type>>&& types) {
  std::vector<std::string> names;
  for (int32_t i = 0; i < types.size(); ++i) {
    names.push_back(fmt::format("c{}", i));
  }
  return ROW(std::move(names), std::move(types));
}

RowVectorPtr VectorMaker::rowVector(const std::vector<VectorPtr>& children) {
  std::vector<std::string> names;
  for (int32_t i = 0; i < children.size(); ++i) {
    names.push_back(fmt::format("c{}", i));
  }

  return rowVector(std::move(names), children);
}

RowVectorPtr VectorMaker::rowVector(
    std::vector<std::string> childNames,
    const std::vector<VectorPtr>& children) {
  std::vector<std::shared_ptr<const Type>> childTypes;
  childTypes.resize(children.size());
  for (int i = 0; i < children.size(); i++) {
    childTypes[i] = children[i]->type();
  }
  auto rowType = ROW(std::move(childNames), std::move(childTypes));
  const size_t vectorSize = children.empty() ? 0 : children.front()->size();

  return std::make_shared<RowVector>(
      pool_, rowType, BufferPtr(nullptr), vectorSize, children);
}

RowVectorPtr VectorMaker::rowVector(
    const std::shared_ptr<const RowType>& rowType,
    vector_size_t size) {
  return std::make_shared<RowVector>(
      pool_, rowType, BufferPtr(nullptr), size, std::vector<VectorPtr>{});
}

vector_size_t VectorMaker::createOffsetsAndSizes(
    vector_size_t size,
    std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
    std::function<bool(vector_size_t /*row */)> isNullAt,
    BufferPtr* nulls,
    BufferPtr* offsets,
    BufferPtr* sizes) {
  *offsets = AlignedBuffer::allocate<vector_size_t>(size, pool_);
  *sizes = AlignedBuffer::allocate<vector_size_t>(size, pool_);

  auto rawOffsets = (*offsets)->asMutable<vector_size_t>();
  auto rawSizes = (*sizes)->asMutable<vector_size_t>();

  uint64_t* rawNulls;
  if (isNullAt) {
    *nulls = AlignedBuffer::allocate<bool>(size, pool_);
    rawNulls = (*nulls)->asMutable<uint64_t>();
    memset(rawNulls, bits::kNotNullByte, (*nulls)->capacity());
  }

  vector_size_t offset = 0;
  for (vector_size_t i = 0; i < size; ++i) {
    if (isNullAt && isNullAt(i)) {
      bits::setNull(rawNulls, i, true);
      rawOffsets[i] = 0;
      rawSizes[i] = 0;
      continue;
    }
    auto s = sizeAt(i);
    rawOffsets[i] = offset;
    rawSizes[i] = s;
    offset += s;
  }
  return offset;
}

ArrayVectorPtr VectorMaker::allNullArrayVector(
    vector_size_t size,
    const std::shared_ptr<const Type>& elementType) {
  BufferPtr nulls = AlignedBuffer::allocate<bool>(size, pool_);
  auto* rawNulls = nulls->asMutable<uint64_t>();
  BufferPtr offsets = AlignedBuffer::allocate<vector_size_t>(size, pool_);
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(size, pool_);
  auto* rawSizes = sizes->asMutable<vector_size_t>();

  for (vector_size_t i = 0; i < size; i++) {
    bits::setNull(rawNulls, i, true);
    rawOffsets[i] = 0;
    rawSizes[i] = 0;
  }

  return std::make_shared<ArrayVector>(
      pool_, ARRAY(elementType), nulls, size, offsets, sizes, nullptr, size);
}

MapVectorPtr VectorMaker::allNullMapVector(
    vector_size_t size,
    const std::shared_ptr<const Type>& keyType,
    const std::shared_ptr<const Type>& valueType) {
  BufferPtr nulls = AlignedBuffer::allocate<bool>(size, pool_);
  auto* rawNulls = nulls->asMutable<uint64_t>();
  BufferPtr offsets = AlignedBuffer::allocate<vector_size_t>(size, pool_);
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(size, pool_);
  auto* rawSizes = sizes->asMutable<vector_size_t>();

  for (vector_size_t i = 0; i < size; i++) {
    bits::setNull(rawNulls, i, true);
    rawOffsets[i] = 0;
    rawSizes[i] = 0;
  }

  return std::make_shared<MapVector>(
      pool_,
      MAP(keyType, valueType),
      nulls,
      size,
      offsets,
      sizes,
      nullptr,
      nullptr,
      size);
}

namespace {
template <TypeKind kind>
void setNotNullValue(
    const VectorPtr& vector,
    vector_size_t index,
    variant value) {
  using T = typename TypeTraits<kind>::NativeType;

  if constexpr (std::is_same_v<T, StringView>) {
    vector->asFlatVector<T>()->set(
        index, StringView(value.value<const char*>()));
  } else {
    vector->asFlatVector<T>()->set(index, value.value<T>());
  }
}

template <typename T, typename P>
FlatVectorPtr<T> makeDecimalFlatVector(
    const std::vector<P>& data,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  VectorPtr base = BaseVector::create(type, data.size(), pool);
  auto flatVector = std::dynamic_pointer_cast<FlatVector<T>>(base);
  using EvalType = typename CppToType<T>::NativeType;
  for (vector_size_t i = 0; i < data.size(); i++) {
    flatVector->set(i, EvalType(data[i]));
  }
  return flatVector;
}

template <typename T, typename P>
FlatVectorPtr<T> makeDecimalFlatVectorNullable(
    const std::vector<std::optional<P>>& data,
    const TypePtr& type,
    memory::MemoryPool* pool) {
  VectorPtr base = BaseVector::create(type, data.size(), pool);
  auto flatVector = std::dynamic_pointer_cast<FlatVector<T>>(base);
  using EvalType = typename CppToType<T>::NativeType;
  for (auto i = 0; i < data.size(); ++i) {
    if (data[i].has_value()) {
      flatVector->set(i, EvalType(data[i].value()));
    } else {
      flatVector->setNull(i, true);
    }
  }
  return flatVector;
}
} // namespace

template <>
FlatVectorPtr<UnscaledShortDecimal> VectorMaker::shortDecimalFlatVector(
    const std::vector<int64_t>& unscaledValues,
    const TypePtr& type) {
  VELOX_CHECK(type->isShortDecimal());
  return makeDecimalFlatVector<UnscaledShortDecimal, int64_t>(
      unscaledValues, type, pool_);
}

template <>
FlatVectorPtr<UnscaledLongDecimal> VectorMaker::longDecimalFlatVector(
    const std::vector<int64_t>& unscaledValues,
    const TypePtr& type) {
  VELOX_CHECK(type->isLongDecimal());
  return makeDecimalFlatVector<UnscaledLongDecimal, int64_t>(
      unscaledValues, type, pool_);
}

template <>
FlatVectorPtr<UnscaledLongDecimal> VectorMaker::longDecimalFlatVector(
    const std::vector<int128_t>& unscaledValues,
    const TypePtr& type) {
  VELOX_CHECK(type->isLongDecimal());
  return makeDecimalFlatVector<UnscaledLongDecimal, int128_t>(
      unscaledValues, type, pool_);
}

template <>
FlatVectorPtr<UnscaledShortDecimal> VectorMaker::shortDecimalFlatVectorNullable(
    const std::vector<std::optional<int64_t>>& unscaledValues,
    const TypePtr& type) {
  VELOX_CHECK(type->isShortDecimal());
  return makeDecimalFlatVectorNullable<UnscaledShortDecimal, int64_t>(
      unscaledValues, type, pool_);
}

template <>
FlatVectorPtr<UnscaledLongDecimal> VectorMaker::longDecimalFlatVectorNullable(
    const std::vector<std::optional<int64_t>>& unscaledValues,
    const TypePtr& type) {
  VELOX_CHECK(type->isLongDecimal());
  return makeDecimalFlatVectorNullable<UnscaledLongDecimal, int64_t>(
      unscaledValues, type, pool_);
}

template <>
FlatVectorPtr<UnscaledLongDecimal> VectorMaker::longDecimalFlatVectorNullable(
    const std::vector<std::optional<int128_t>>& unscaledValues,
    const TypePtr& type) {
  VELOX_CHECK(type->isLongDecimal());
  return makeDecimalFlatVectorNullable<UnscaledLongDecimal, int128_t>(
      unscaledValues, type, pool_);
}

ArrayVectorPtr VectorMaker::arrayOfRowVector(
    const RowTypePtr& rowType,
    const std::vector<std::vector<variant>>& data) {
  vector_size_t size = data.size();
  BufferPtr offsets = AlignedBuffer::allocate<vector_size_t>(size, pool_);
  BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(size, pool_);

  auto rawOffsets = offsets->asMutable<vector_size_t>();
  auto rawSizes = sizes->asMutable<vector_size_t>();

  // Count number of elements.
  vector_size_t numElements = 0;
  for (const auto& array : data) {
    numElements += array.size();
  }

  // Create the underlying vector.
  auto rowVector = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(rowType, numElements, pool_));
  for (auto& field : rowVector->children()) {
    field->resize(numElements);
  }

  vector_size_t currentIdx = 0;
  for (const auto& arrayValue : data) {
    *rawSizes++ = arrayValue.size();
    *rawOffsets++ = currentIdx;

    for (auto arrayElement : arrayValue) {
      if (arrayElement.isNull()) {
        rowVector->setNull(currentIdx, true);
      } else {
        for (auto i = 0; i < rowType->size(); i++) {
          const auto& value = arrayElement.row()[i];
          if (value.isNull()) {
            rowVector->childAt(i)->setNull(currentIdx, true);
          } else {
            VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
                setNotNullValue,
                rowType->childAt(i)->kind(),
                rowVector->childAt(i),
                currentIdx,
                value);
          }
        }
      }
      currentIdx++;
    }
  }

  return std::make_shared<ArrayVector>(
      pool_,
      ARRAY(rowType),
      nullptr,
      size,
      offsets,
      sizes,
      std::move(rowVector),
      0);
}

ArrayVectorPtr VectorMaker::arrayVector(
    const std::vector<vector_size_t>& offsets,
    const VectorPtr& elements,
    const std::vector<vector_size_t>& nulls) {
  const auto size = offsets.size();
  BufferPtr offsetsBuffer = allocateOffsets(size, pool_);
  BufferPtr sizesBuffer = allocateSizes(size, pool_);
  BufferPtr nullsBuffer = nullptr;
  auto rawOffsets = offsetsBuffer->asMutable<vector_size_t>();
  auto rawSizes = sizesBuffer->asMutable<vector_size_t>();

  for (int i = 0; i < size - 1; i++) {
    rawSizes[i] = offsets[i + 1] - offsets[i];
  }
  rawSizes[size - 1] = elements->size() - offsets.back();

  memcpy(rawOffsets, offsets.data(), size * sizeof(vector_size_t));

  if (!nulls.empty()) {
    nullsBuffer = AlignedBuffer::allocate<bool>(size, pool_, bits::kNotNull);
    auto rawNulls = nullsBuffer->asMutable<uint64_t>();

    for (int i = 0; i < nulls.size(); i++) {
      VELOX_CHECK_EQ(rawSizes[nulls[i]], 0)
      bits::setNull(rawNulls, nulls[i]);
    }
  }

  return std::make_shared<ArrayVector>(
      pool_,
      ARRAY(elements->type()),
      nullsBuffer,
      size,
      offsetsBuffer,
      sizesBuffer,
      elements);
}

MapVectorPtr VectorMaker::mapVector(
    const std::vector<vector_size_t>& offsets,
    const VectorPtr& keys,
    const VectorPtr& values,
    const std::vector<vector_size_t>& nulls) {
  VELOX_CHECK_EQ(keys->size(), values->size());

  const auto size = offsets.size();
  BufferPtr offsetsBuffer = allocateOffsets(size, pool_);
  BufferPtr sizesBuffer = allocateSizes(size, pool_);
  BufferPtr nullsBuffer = nullptr;
  auto rawOffsets = offsetsBuffer->asMutable<vector_size_t>();
  auto rawSizes = sizesBuffer->asMutable<vector_size_t>();

  for (int i = 0; i < size - 1; i++) {
    rawSizes[i] = offsets[i + 1] - offsets[i];
  }
  rawSizes[size - 1] = keys->size() - offsets.back();

  memcpy(rawOffsets, offsets.data(), size * sizeof(vector_size_t));

  if (!nulls.empty()) {
    nullsBuffer = AlignedBuffer::allocate<bool>(size, pool_, bits::kNotNull);
    auto rawNulls = nullsBuffer->asMutable<uint64_t>();

    for (int i = 0; i < nulls.size(); i++) {
      VELOX_CHECK_EQ(rawSizes[nulls[i]], 0)
      bits::setNull(rawNulls, nulls[i]);
    }
  }

  return std::make_shared<MapVector>(
      pool_,
      MAP(keys->type(), values->type()),
      nullsBuffer,
      size,
      offsetsBuffer,
      sizesBuffer,
      keys,
      values);
}

// static
VectorPtr VectorMaker::flatten(const VectorPtr& vector) {
  SelectivityVector allRows(vector->size());
  auto flatVector =
      BaseVector::create(vector->type(), vector->size(), vector->pool());
  flatVector->copy(vector.get(), allRows, nullptr);
  return flatVector;
}
} // namespace facebook::velox::test
