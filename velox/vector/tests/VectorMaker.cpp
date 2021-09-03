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
#include "velox/vector/tests/VectorMaker.h"

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
  std::vector<std::shared_ptr<const Type>> childTypes;
  childTypes.resize(children.size());
  for (int i = 0; i < children.size(); i++) {
    childTypes[i] = children[i]->type();
  }
  auto rowType = this->rowType(std::move(childTypes));

  return std::make_shared<RowVector>(
      pool_, rowType, BufferPtr(nullptr), children[0]->size(), children);
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
} // namespace

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

// static
VectorPtr VectorMaker::flatten(const VectorPtr& vector) {
  SelectivityVector allRows(vector->size());
  auto flatVector =
      BaseVector::create(vector->type(), vector->size(), vector->pool());
  flatVector->copy(vector.get(), allRows, nullptr);
  return flatVector;
}
} // namespace facebook::velox::test
