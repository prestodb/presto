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
#include "velox/serializers/PrestoSerializerEstimationUtils.h"

#include "velox/vector/FlatVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::serializer::presto::detail {
namespace {
template <TypeKind Kind>
void estimateFlattenedConstantSerializedSize(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  VELOX_CHECK_EQ(vector->encoding(), VectorEncoding::Simple::CONSTANT);
  using T = typename KindToFlatVector<Kind>::WrapperType;
  auto* constantVector = vector->as<ConstantVector<T>>();
  if (constantVector->valueVector()) {
    estimateWrapperSerializedSize(ranges, sizes, vector, scratch);
    return;
  }

  int32_t elementSize = sizeof(T);
  if (constantVector->isNullAt(0)) {
    elementSize = 1;
  } else if constexpr (std::is_same_v<T, StringView>) {
    elementSize = constantVector->valueAt(0).size();
  }
  for (int32_t i = 0; i < ranges.size(); ++i) {
    *sizes[i] += elementSize * ranges[i].size;
  }
}

void estimateBiasedSerializedSize(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  auto valueSize = vector->type()->cppSizeInBytes();
  if (vector->mayHaveNulls()) {
    auto rawNulls = vector->rawNulls();
    for (int32_t i = 0; i < ranges.size(); ++i) {
      auto end = ranges[i].begin + ranges[i].size;
      int32_t numValues = bits::countBits(rawNulls, ranges[i].begin, end);
      *(sizes[i]) += numValues * valueSize + bits::nbytes(ranges[i].size);
    }
  } else {
    for (int32_t i = 0; i < ranges.size(); ++i) {
      *(sizes[i]) += ranges[i].size * valueSize;
    }
  }
}

template <TypeKind Kind>
void estimateFlatSerializedSize(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  const auto valueSize = vector->type()->cppSizeInBytes();
  const auto numRows = rows.size();
  if (vector->mayHaveNulls()) {
    auto rawNulls = vector->rawNulls();
    ScratchPtr<uint64_t, 4> nullsHolder(scratch);
    ScratchPtr<int32_t, 64> nonNullsHolder(scratch);
    auto nulls = nullsHolder.get(bits::nwords(numRows));
    simd::gatherBits(rawNulls, rows, nulls);
    auto nonNulls = nonNullsHolder.get(numRows);
    const auto numNonNull = simd::indicesOfSetBits(nulls, 0, numRows, nonNulls);
    for (int32_t i = 0; i < numNonNull; ++i) {
      *sizes[nonNulls[i]] += valueSize;
    }
  } else {
    VELOX_UNREACHABLE("Non null fixed width case handled before this");
  }
}

void estimateFlatSerializedSizeVarcharOrVarbinary(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  const auto numRows = rows.size();
  auto strings = static_cast<const FlatVector<StringView>*>(vector);
  auto rawNulls = strings->rawNulls();
  auto rawValues = strings->rawValues();
  if (!rawNulls) {
    for (auto i = 0; i < rows.size(); ++i) {
      *sizes[i] += rawValues[rows[i]].size();
    }
  } else {
    ScratchPtr<uint64_t, 4> nullsHolder(scratch);
    ScratchPtr<int32_t, 64> nonNullsHolder(scratch);
    auto nulls = nullsHolder.get(bits::nwords(numRows));
    simd::gatherBits(rawNulls, rows, nulls);
    auto* nonNulls = nonNullsHolder.get(numRows);
    auto numNonNull = simd::indicesOfSetBits(nulls, 0, numRows, nonNulls);

    for (int32_t i = 0; i < numNonNull; ++i) {
      *sizes[nonNulls[i]] += rawValues[rows[nonNulls[i]]].size();
    }
  }
}

template <>
void estimateFlatSerializedSize<TypeKind::VARCHAR>(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  estimateFlatSerializedSizeVarcharOrVarbinary(vector, rows, sizes, scratch);
}

template <>
void estimateFlatSerializedSize<TypeKind::VARBINARY>(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  estimateFlatSerializedSizeVarcharOrVarbinary(vector, rows, sizes, scratch);
}

template <>
void estimateFlatSerializedSize<TypeKind::OPAQUE>(
    const BaseVector*,
    const folly::Range<const vector_size_t*>&,
    vector_size_t**,
    Scratch&) {
  VELOX_FAIL("Opaque type support is not implemented.");
}

template <TypeKind Kind>
void estimateFlattenedConstantSerializedSize(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  VELOX_CHECK_EQ(vector->encoding(), VectorEncoding::Simple::CONSTANT);

  using T = typename KindToFlatVector<Kind>::WrapperType;
  auto* constantVector = vector->as<ConstantVector<T>>();
  int32_t elementSize = sizeof(T);
  if (constantVector->isNullAt(0)) {
    elementSize = 1;
  } else if (vector->valueVector()) {
    const auto* values = constantVector->wrappedVector();
    vector_size_t* sizePtr = &elementSize;
    const vector_size_t singleRow = constantVector->wrappedIndex(0);
    estimateSerializedSizeInt(
        values,
        folly::Range<const vector_size_t*>(&singleRow, 1),
        &sizePtr,
        scratch);
  } else if constexpr (std::is_same_v<T, StringView>) {
    elementSize = constantVector->valueAt(0).size();
  }
  for (int32_t i = 0; i < rows.size(); ++i) {
    *sizes[i] += elementSize;
  }
}

void estimateWrapperSerializedSize(
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    const BaseVector* wrapper,
    Scratch& scratch) {
  ScratchPtr<vector_size_t, 1> innerRowsHolder(scratch);
  ScratchPtr<vector_size_t*, 1> innerSizesHolder(scratch);
  const int32_t numRows = rows.size();
  int32_t numInner = 0;
  auto* innerRows = innerRowsHolder.get(numRows);
  auto* innerSizes = sizes;
  const BaseVector* wrapped;
  if (wrapper->encoding() == VectorEncoding::Simple::DICTIONARY &&
      !wrapper->rawNulls()) {
    // Dictionary with no nulls.
    auto* indices = wrapper->wrapInfo()->as<vector_size_t>();
    wrapped = wrapper->valueVector().get();
    simd::transpose(indices, rows, innerRows);
    numInner = numRows;
  } else {
    wrapped = wrapper->wrappedVector();
    innerSizes = innerSizesHolder.get(numRows);
    for (int32_t i = 0; i < rows.size(); ++i) {
      if (!wrapper->isNullAt(rows[i])) {
        innerRows[numInner] = wrapper->wrappedIndex(rows[i]);
        innerSizes[numInner] = sizes[i];
        ++numInner;
      }
    }
  }
  if (numInner == 0) {
    return;
  }

  estimateSerializedSizeInt(
      wrapped,
      folly::Range<const vector_size_t*>(innerRows, numInner),
      innerSizes,
      scratch);
}

void estimateBiasedSerializedSize(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  VELOX_UNSUPPORTED();
}
} // namespace

void estimateFlatSerializedSizeVarcharOrVarbinary(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  auto strings = static_cast<const FlatVector<StringView>*>(vector);
  auto rawNulls = strings->rawNulls();
  auto rawValues = strings->rawValues();
  for (int32_t i = 0; i < ranges.size(); ++i) {
    auto end = ranges[i].begin + ranges[i].size;
    int32_t numNulls = 0;
    int32_t bytes = 0;
    for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
      if (rawNulls && bits::isBitNull(rawNulls, offset)) {
        ++numNulls;
      } else {
        bytes += sizeof(int32_t) + rawValues[offset].size();
      }
    }
    *(sizes[i]) += bytes + bits::nbytes(numNulls) + 4 * numNulls;
  }
}

void estimateSerializedSizeInt(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          estimateFlatSerializedSize,
          vector->typeKind(),
          vector,
          ranges,
          sizes);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          estimateFlattenedConstantSerializedSize,
          vector->typeKind(),
          vector,
          ranges,
          sizes,
          scratch);
      break;
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
      estimateWrapperSerializedSize(ranges, sizes, vector, scratch);
      break;
    case VectorEncoding::Simple::BIASED:
      estimateBiasedSerializedSize(vector, ranges, sizes);
      break;
    case VectorEncoding::Simple::ROW: {
      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      for (int32_t i = 0; i < ranges.size(); ++i) {
        auto begin = ranges[i].begin;
        auto end = begin + ranges[i].size;
        for (auto offset = begin; offset < end; ++offset) {
          *sizes[i] += sizeof(int32_t);
          if (!vector->isNullAt(offset)) {
            childRanges.push_back(IndexRange{offset, 1});
            childSizes.push_back(sizes[i]);
          }
        }
      }
      auto rowVector = vector->as<RowVector>();
      auto& children = rowVector->children();
      for (auto& child : children) {
        if (child) {
          estimateSerializedSizeInt(
              child.get(),
              folly::Range(childRanges.data(), childRanges.size()),
              childSizes.data(),
              scratch);
        }
      }
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto mapVector = vector->as<MapVector>();
      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      expandRepeatedRanges(
          mapVector,
          mapVector->rawOffsets(),
          mapVector->rawSizes(),
          ranges,
          sizes,
          &childRanges,
          &childSizes);
      estimateSerializedSizeInt(
          mapVector->mapKeys().get(), childRanges, childSizes.data(), scratch);
      estimateSerializedSizeInt(
          mapVector->mapValues().get(),
          childRanges,
          childSizes.data(),
          scratch);
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto arrayVector = vector->as<ArrayVector>();
      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      expandRepeatedRanges(
          arrayVector,
          arrayVector->rawOffsets(),
          arrayVector->rawSizes(),
          ranges,
          sizes,
          &childRanges,
          &childSizes);
      estimateSerializedSizeInt(
          arrayVector->elements().get(),
          childRanges,
          childSizes.data(),
          scratch);
      break;
    }
    case VectorEncoding::Simple::LAZY:
      estimateSerializedSizeInt(vector->loadedVector(), ranges, sizes, scratch);
      break;
    default:
      VELOX_UNSUPPORTED("Unsupported vector encoding {}", vector->encoding());
  }
}

void estimateWrapperSerializedSize(
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    const BaseVector* wrapper,
    Scratch& scratch) {
  std::vector<IndexRange> newRanges;
  std::vector<vector_size_t*> newSizes;
  const BaseVector* wrapped = wrapper->wrappedVector();
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t numNulls = 0;
    auto end = ranges[i].begin + ranges[i].size;
    for (int32_t offset = ranges[i].begin; offset < end; ++offset) {
      if (!wrapper->isNullAt(offset)) {
        newRanges.push_back(IndexRange{wrapper->wrappedIndex(offset), 1});
        newSizes.push_back(sizes[i]);
      } else {
        ++numNulls;
      }
    }
    *sizes[i] += bits::nbytes(numNulls);
  }
  estimateSerializedSizeInt(wrapped, newRanges, newSizes.data(), scratch);
}

void expandRepeatedRanges(
    const BaseVector* vector,
    const vector_size_t* rawOffsets,
    const vector_size_t* rawSizes,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    std::vector<IndexRange>* childRanges,
    std::vector<vector_size_t*>* childSizes) {
  for (int32_t i = 0; i < ranges.size(); ++i) {
    int32_t begin = ranges[i].begin;
    int32_t end = begin + ranges[i].size;
    bool hasNull = false;
    for (int32_t offset = begin; offset < end; ++offset) {
      if (vector->isNullAt(offset)) {
        hasNull = true;
      } else {
        // Add the size of the length.
        *sizes[i] += sizeof(int32_t);
        childRanges->push_back(
            IndexRange{rawOffsets[offset], rawSizes[offset]});
        childSizes->push_back(sizes[i]);
      }
    }

    if (hasNull) {
      // Add the size of the null bit mask.
      *sizes[i] += bits::nbytes(ranges[i].size);
    }
  }
}

void estimateSerializedSizeInt(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  const auto numRows = rows.size();
  if (vector->type()->isFixedWidth() && !vector->mayHaveNullsRecursive()) {
    const auto elementSize = vector->type()->cppSizeInBytes();
    for (auto i = 0; i < numRows; ++i) {
      *sizes[i] += elementSize;
    }
    return;
  }
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT: {
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          estimateFlatSerializedSize,
          vector->typeKind(),
          vector,
          rows,
          sizes,
          scratch);
      break;
    }
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          estimateFlattenedConstantSerializedSize,
          vector->typeKind(),
          vector,
          rows,
          sizes,
          scratch);
      break;
    case VectorEncoding::Simple::DICTIONARY:
    case VectorEncoding::Simple::SEQUENCE:
      estimateWrapperSerializedSize(rows, sizes, vector, scratch);
      break;
    case VectorEncoding::Simple::BIASED:
      estimateBiasedSerializedSize(vector, rows, sizes, scratch);
      break;
    case VectorEncoding::Simple::ROW: {
      ScratchPtr<vector_size_t, 1> innerRowsHolder(scratch);
      ScratchPtr<vector_size_t*, 1> innerSizesHolder(scratch);
      ScratchPtr<uint64_t, 1> nullsHolder(scratch);
      auto* innerRows = rows.data();
      auto* innerSizes = sizes;
      const auto numRows = rows.size();
      int32_t numInner = numRows;
      if (vector->mayHaveNulls()) {
        auto nulls = nullsHolder.get(bits::nwords(numRows));
        simd::gatherBits(vector->rawNulls(), rows, nulls);
        auto mutableInnerRows = innerRowsHolder.get(numRows);
        numInner = simd::indicesOfSetBits(nulls, 0, numRows, mutableInnerRows);
        innerSizes = innerSizesHolder.get(numInner);
        for (auto i = 0; i < numInner; ++i) {
          innerSizes[i] = sizes[mutableInnerRows[i]];
        }
        simd::transpose(
            rows.data(),
            folly::Range<const vector_size_t*>(mutableInnerRows, numInner),
            mutableInnerRows);
        innerRows = mutableInnerRows;
      }
      auto* rowVector = vector->as<RowVector>();
      auto& children = rowVector->children();
      for (auto& child : children) {
        if (child) {
          estimateSerializedSizeInt(
              child.get(),
              folly::Range(innerRows, numInner),
              innerSizes,
              scratch);
        }
      }
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto* mapVector = vector->asUnchecked<MapVector>();
      ScratchPtr<IndexRange> rangeHolder(scratch);
      ScratchPtr<vector_size_t*> sizesHolder(scratch);
      const auto numRanges = rowsToRanges(
          rows,
          mapVector->rawNulls(),
          mapVector->rawOffsets(),
          mapVector->rawSizes(),
          sizes,
          rangeHolder,
          &sizesHolder,
          nullptr,
          scratch);
      if (numRanges == 0) {
        return;
      }
      estimateSerializedSizeInt(
          mapVector->mapKeys().get(),
          folly::Range<const IndexRange*>(rangeHolder.get(), numRanges),
          sizesHolder.get(),
          scratch);
      estimateSerializedSizeInt(
          mapVector->mapValues().get(),
          folly::Range<const IndexRange*>(rangeHolder.get(), numRanges),
          sizesHolder.get(),
          scratch);
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto* arrayVector = vector->as<ArrayVector>();
      ScratchPtr<IndexRange> rangeHolder(scratch);
      ScratchPtr<vector_size_t*> sizesHolder(scratch);
      const auto numRanges = rowsToRanges(
          rows,
          arrayVector->rawNulls(),
          arrayVector->rawOffsets(),
          arrayVector->rawSizes(),
          sizes,
          rangeHolder,
          &sizesHolder,
          nullptr,
          scratch);
      if (numRanges == 0) {
        return;
      }
      estimateSerializedSizeInt(
          arrayVector->elements().get(),
          folly::Range<const IndexRange*>(rangeHolder.get(), numRanges),
          sizesHolder.get(),
          scratch);
      break;
    }
    case VectorEncoding::Simple::LAZY:
      estimateSerializedSizeInt(vector->loadedVector(), rows, sizes, scratch);
      break;
    default:
      VELOX_UNSUPPORTED("Unsupported vector encoding {}", vector->encoding());
  }
}
} // namespace facebook::velox::serializer::presto::detail
