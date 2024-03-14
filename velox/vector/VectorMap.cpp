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

#include "velox/vector/VectorMap.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox {

VectorMap::VectorMap(BaseVector& alphabet)
    : alphabet_(&alphabet),
      isString_(
          alphabet_->typeKind() == TypeKind::VARCHAR ||
          alphabet_->typeKind() == TypeKind::VARBINARY),
      fixedWidth_(
          alphabet_->type()->isFixedWidth()
              ? alphabet_->type()->cppSizeInBytes()
              : kVariableWidth) {
  auto size = alphabet_->size();
  // We reserve the size. The assumption is that we run this on an alphabet of a
  // dictionary vector in preparation for adding elements, so the values are
  // expected to be distinct.
  if (isString_) {
    distinctStrings_.reserve(size);
  } else {
    distinctSet_.reserve(size);
  }
  for (auto i = 0; i < size; ++i) {
    addOne(*alphabet_, i, false);
  }
}

VectorMap::VectorMap(
    const TypePtr& type,
    memory::MemoryPool* pool,
    int32_t reserve)
    : isString_(
          type->kind() == TypeKind::VARCHAR ||
          type->kind() == TypeKind::VARBINARY),
      fixedWidth_(
          type->isFixedWidth() ? type->cppSizeInBytes() : kVariableWidth) {
  alphabetOwned_ = BaseVector::create(type, 0, pool);
  alphabet_ = alphabetOwned_.get();
  if (isString_) {
    distinctStrings_.reserve(reserve);
  } else {
    distinctSet_.reserve(reserve);
  }
}

vector_size_t VectorMap::addOne(
    const BaseVector& topVector,
    vector_size_t topIndex,
    bool insertToAlphabet) {
  const BaseVector* vector = &topVector;
  vector_size_t index = topIndex;
  if (topVector.encoding() != VectorEncoding::Simple::FLAT) {
    vector = topVector.wrappedVector();
    index = topVector.wrappedIndex(topIndex);
  }
  if (LIKELY(isString_)) {
    StringView string;
    bool isNull = vector->isNullAt(index);
    if (UNLIKELY(isNull)) {
      if (nullIndex_ != kNoNullIndex) {
        return nullIndex_;
      }
    } else {
      if (LIKELY(vector->encoding() == VectorEncoding::Simple::FLAT)) {
        string = vector->asUnchecked<FlatVector<StringView>>()->valueAt(index);
      } else {
        string = vector->asUnchecked<ConstantVector<StringView>>()->valueAt(0);
      }
      auto it = distinctStrings_.find(string);
      if (it != distinctStrings_.end()) {
        return it->second;
      }
    }
  } else {
    auto it = distinctSet_.find(VectorValueSetEntry{vector, index});
    if (it != distinctSet_.end()) {
      return it->index;
    }
  }
  int32_t newIndex;
  if (insertToAlphabet) {
    VELOX_CHECK(alphabet_ == alphabetOwned_.get());
    newIndex = alphabet_->size();
    alphabet_->resize(newIndex + 1);
    alphabetSizes_.resize(newIndex + 1);
    alphabet_->copy(vector, newIndex, index, 1);
  } else {
    newIndex = topIndex;
    if (alphabetSizes_.size() < newIndex + 1) {
      alphabetSizes_.resize(newIndex + 1);
    }
  }
  const bool isNull = vector->isNullAt(index);
  if (isNull) {
    alphabetSizes_[newIndex] = 0;
    nullIndex_ = newIndex;
  } else if (isString_) {
    // Add 4 for length.
    alphabetSizes_[newIndex] = alphabet_->asUnchecked<FlatVector<StringView>>()
                                   ->valueAt(newIndex)
                                   .size() +
        4;
  } else if (fixedWidth_ == kVariableWidth) {
    Scratch scratch;
    ScratchPtr<vector_size_t, 1> indicesHolder(scratch);
    ScratchPtr<vector_size_t*, 1> sizesHolder(scratch);
    auto sizeIndices = indicesHolder.get(1);
    sizeIndices[0] = newIndex;
    auto sizes = sizesHolder.get(1);
    alphabetSizes_[newIndex] = 0;
    sizes[0] = &alphabetSizes_[newIndex];
    getVectorSerde()->estimateSerializedSize(
        alphabet_,
        folly::Range<const vector_size_t*>(sizeIndices, 1),
        sizes,
        scratch);
  }
  if (isString_) {
    if (!isNull) {
      distinctStrings_[alphabet_->asUnchecked<FlatVector<StringView>>()
                           ->valueAt(newIndex)] = newIndex;
    }
  } else {
    distinctSet_.insert(VectorValueSetEntry{alphabet_, newIndex});
  }
  return newIndex;
}

void VectorMap::addMultiple(
    BaseVector& vector,
    folly::Range<const vector_size_t*> rows,
    bool insertToAlphabet,
    vector_size_t* ids) {
  auto size = rows.size();
  for (auto i = 0; i < size; ++i) {
    ids[i] = addOne(vector, rows[i], insertToAlphabet);
  }
}

} // namespace facebook::velox
