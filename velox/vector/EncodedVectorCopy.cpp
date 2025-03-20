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

#include "velox/vector/EncodedVectorCopy.h"

#include "velox/vector/ConstantVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox {

namespace {

void copyImpl(
    const EncodedVectorCopyOptions& options,
    const VectorPtr& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    VectorPtr& target,
    bool targetMutable);

// Calculate the expected target size given the old target size and the copy
// ranges.
vector_size_t targetSize(
    vector_size_t size,
    const folly::Range<const BaseVector::CopyRange*>& ranges) {
  for (auto& range : ranges) {
    size = std::max(size, range.targetIndex + range.count);
  }
  return size;
}

// Calculate the expected target size for a newly created target (no old
// existing target vector).
vector_size_t newTargetSize(
    const folly::Range<const BaseVector::CopyRange*>& ranges) {
  vector_size_t size = 0;
  vector_size_t minIndex = std::numeric_limits<vector_size_t>::max();
  for (auto& range : ranges) {
    size = std::max(size, range.targetIndex + range.count);
    minIndex = std::min(minIndex, range.targetIndex);
  }
  VELOX_CHECK_EQ(
      minIndex,
      0,
      "Index from 0 to {} (exclusive) is uninitialized during the copy",
      minIndex);
  return size;
}

template <typename T>
BufferPtr tryReuse(const BufferPtr& source, vector_size_t size) {
  auto byteSize = BaseVector::byteSize<T>(size);
  if (source->isMutable() && byteSize <= source->capacity()) {
    source->setSize(byteSize);
    return source;
  }
  return nullptr;
}

void fillIndices(
    vector_size_t value,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    vector_size_t* target) {
  for (auto& range : ranges) {
    auto* t = target + range.targetIndex;
    std::fill(t, t + range.count, value);
  }
}

void copyIndices(
    const vector_size_t* source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    vector_size_t shift,
    vector_size_t* target) {
  for (auto& range : ranges) {
    auto* t = target + range.targetIndex;
    std::memcpy(
        t, source + range.sourceIndex, sizeof(vector_size_t) * range.count);
    if (shift != 0) {
      for (vector_size_t i = 0; i < range.count; ++i) {
        t[i] += shift;
      }
    }
  }
}

template <typename MutableNulls>
void setSourceNullsImpl(
    const uint64_t* source,
    const uint64_t* target,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    MutableNulls&& mutableNulls) {
  if (source) {
    BaseVector::copyNulls(mutableNulls(), source, ranges);
  } else if (target) {
    BaseVector::setNulls(mutableNulls(), ranges, false);
  }
}

void setSourceNulls(
    const uint64_t* source,
    const uint64_t* target,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    uint64_t* mutableNulls) {
  setSourceNullsImpl(source, target, ranges, [&] { return mutableNulls; });
}

void setSourceNulls(
    const uint64_t* source,
    BaseVector& target,
    const folly::Range<const BaseVector::CopyRange*>& ranges) {
  setSourceNullsImpl(source, target.rawNulls(), ranges, [&] {
    return target.mutableRawNulls();
  });
}

BufferPtr combineNulls(
    vector_size_t size,
    memory::MemoryPool* pool,
    const uint64_t* target,
    vector_size_t oldTargetSize,
    const uint64_t* source,
    const folly::Range<const BaseVector::CopyRange*>& ranges) {
  auto nulls = allocateNulls(size, pool);
  auto* rawNulls = nulls->asMutable<uint64_t>();
  if (target) {
    std::memcpy(rawNulls, target, bits::nbytes(oldTargetSize));
  }
  setSourceNulls(source, target, ranges, rawNulls);
  return nulls;
}

BufferPtr newNulls(
    vector_size_t size,
    memory::MemoryPool* pool,
    const uint64_t* source,
    const folly::Range<const BaseVector::CopyRange*>& ranges) {
  if (!source) {
    return nullptr;
  }
  return combineNulls(size, pool, nullptr, 0, source, ranges);
}

vector_size_t baseSize(DecodedVector& dictionary) {
  auto* nulls = dictionary.nulls();
  auto* indices = dictionary.indices();
  vector_size_t result = 0;
  if (nulls) {
    for (vector_size_t i = 0; i < dictionary.size(); ++i) {
      if (!bits::isBitNull(nulls, i)) {
        result = std::max(result, 1 + indices[i]);
      }
    }
  } else {
    for (vector_size_t i = 0; i < dictionary.size(); ++i) {
      result = std::max(result, 1 + indices[i]);
    }
  }
  return result;
}

template <typename NextTargetBaseIndex>
std::vector<BaseVector::CopyRange> toBaseRangesImpl(
    const DecodedVector& source,
    const folly::Range<const BaseVector::CopyRange*>& outer,
    vector_size_t* indices,
    NextTargetBaseIndex&& nextTargetBaseIndex) {
  VELOX_DCHECK(!source.isConstantMapping());
  std::vector<uint64_t> copied(bits::nwords(source.base()->size()));
  for (auto& range : outer) {
    for (vector_size_t i = 0; i < range.count; ++i) {
      auto j = range.sourceIndex + i;
      if (!source.isNullAt(j)) {
        auto k = source.index(j);
        bits::setBit(copied.data(), k);
        indices[range.targetIndex + i] = k;
      }
    }
  }
  std::vector<BaseVector::CopyRange> inner;
  // Mapping from source base indices to target base indices.
  std::vector<vector_size_t> mapping(source.base()->size());
  vector_size_t targetBaseIndex = nextTargetBaseIndex();
  bits::forEachSetBit(copied.data(), 0, source.base()->size(), [&](auto i) {
    mapping[i] = targetBaseIndex;
    if (!inner.empty() && inner.back().sourceIndex + inner.back().count == i &&
        inner.back().targetIndex + inner.back().count == targetBaseIndex) {
      ++inner.back().count;
    } else {
      auto& r = inner.emplace_back();
      r.sourceIndex = i;
      r.targetIndex = targetBaseIndex;
      r.count = 1;
    }
    targetBaseIndex = nextTargetBaseIndex();
  });
  for (auto& range : outer) {
    for (vector_size_t i = 0; i < range.count; ++i) {
      auto j = range.targetIndex + i;
      indices[j] =
          source.isNullAt(range.sourceIndex + i) ? 0 : mapping[indices[j]];
    }
  }
  return inner;
}

std::vector<BaseVector::CopyRange> toBaseRanges(
    const DecodedVector& source,
    const folly::Range<const BaseVector::CopyRange*>& outer,
    vector_size_t targetBaseIndexStart,
    vector_size_t* indices) {
  auto targetBaseIndex = targetBaseIndexStart - 1;
  return toBaseRangesImpl(
      source, outer, indices, [&] { return ++targetBaseIndex; });
}

std::vector<BaseVector::CopyRange> toBaseRanges(
    const DecodedVector& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    const DecodedVector& target,
    vector_size_t* indices) {
  std::vector<uint64_t> targetOverwritten(bits::nwords(target.size()));
  for (auto& range : ranges) {
    bits::fillBits(
        targetOverwritten.data(),
        range.targetIndex,
        std::min(target.size(), range.targetIndex + range.count),
        true);
  }
  std::vector<uint64_t> targetBaseOverwritten(
      bits::nwords(target.base()->size()), -1ll);
  bits::forEachUnsetBit(
      targetOverwritten.data(), 0, target.size(), [&](auto i) {
        if (!target.isNullAt(i)) {
          bits::clearBit(targetBaseOverwritten.data(), indices[i]);
        }
      });
  std::vector<BaseVector::CopyRange> baseRanges;
  if (source.isConstantMapping()) {
    auto& baseRange = baseRanges.emplace_back();
    baseRange.sourceIndex = source.index(0);
    baseRange.targetIndex = bits::findFirstBit(
        targetBaseOverwritten.data(), 0, target.base()->size());
    if (baseRange.targetIndex < 0) {
      baseRange.targetIndex = target.base()->size();
    }
    baseRange.count = 1;
    fillIndices(baseRange.targetIndex, ranges, indices);
  } else {
    vector_size_t begin = 0;
    baseRanges = toBaseRangesImpl(source, ranges, indices, [&] {
      if (begin < target.base()->size()) {
        begin = bits::findFirstBit(
            targetBaseOverwritten.data(), begin, target.base()->size());
        if (begin < 0) {
          begin = target.base()->size();
        }
      }
      return begin++;
    });
  }
  return baseRanges;
}

void compactNestedRangesImpl(
    std::vector<BaseVector::CopyRange>& nested,
    vector_size_t targetNestedStart,
    vector_size_t* newOffsets,
    vector_size_t* newSizes) {
  std::sort(nested.begin(), nested.end(), [](auto& x, auto& y) {
    return x.sourceIndex < y.sourceIndex;
  });
  vector_size_t size = 0;
  for (auto& r : nested) {
    newOffsets[r.targetIndex] = targetNestedStart;
    newSizes[r.targetIndex] = r.count;
    if (r.count == 0) {
      continue;
    }
    if (size > 0 &&
        nested[size - 1].sourceIndex + nested[size - 1].count ==
            r.sourceIndex) {
      nested[size - 1].count += r.count;
    } else {
      nested[size] = r;
      nested[size].targetIndex = targetNestedStart;
      ++size;
    }
    targetNestedStart += r.count;
  }
  nested.resize(size);
}

std::vector<BaseVector::CopyRange> toNestedRanges(
    const ArrayVectorBase& source,
    const folly::Range<const BaseVector::CopyRange*>& outer,
    vector_size_t targetNestedStart,
    vector_size_t* newOffsets,
    vector_size_t* newSizes) {
  std::vector<BaseVector::CopyRange> nested;
  vector_size_t size = 0;
  for (auto& range : outer) {
    size += range.count;
  }
  nested.reserve(size);
  for (auto& range : outer) {
    for (vector_size_t i = 0; i < range.count; ++i) {
      auto j = range.sourceIndex + i;
      auto& r = nested.emplace_back();
      if (source.isNullAt(j)) {
        r.count = 0;
      } else {
        r.sourceIndex = source.offsetAt(j);
        r.count = source.sizeAt(j);
      }
      // Store the outer target index temporarily.
      r.targetIndex = range.targetIndex + i;
    }
  }
  compactNestedRangesImpl(nested, targetNestedStart, newOffsets, newSizes);
  return nested;
}

bool needCompactNested(
    const EncodedVectorCopyOptions& options,
    const ArrayVectorBase& vector,
    vector_size_t nestedSize) {
  vector_size_t used = 0;
  for (vector_size_t i = 0; i < vector.size(); ++i) {
    if (!vector.isNullAt(i)) {
      used += vector.sizeAt(i);
    }
  }
  return used < options.compactNestedThreshold * nestedSize;
}

std::vector<BaseVector::CopyRange> compactNestedRanges(
    const ArrayVectorBase& vector,
    vector_size_t* newOffsets,
    vector_size_t* newSizes) {
  std::vector<BaseVector::CopyRange> nested;
  nested.reserve(vector.size());
  for (vector_size_t i = 0; i < vector.size(); ++i) {
    auto& r = nested.emplace_back();
    if (vector.isNullAt(i)) {
      r.count = 0;
    } else {
      r.sourceIndex = vector.offsetAt(i);
      r.count = vector.sizeAt(i);
    }
    r.targetIndex = i;
  }
  compactNestedRangesImpl(nested, 0, newOffsets, newSizes);
  return nested;
}

void compactNestedVector(
    const EncodedVectorCopyOptions& options,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    VectorPtr& vector,
    bool mutableVector) {
  if (mutableVector) {
    if (ranges.empty()) {
      vector->resize(0);
      return;
    }
    if (ranges.size() == 1 && ranges[0].sourceIndex == 0) {
      VELOX_CHECK_EQ(ranges[0].targetIndex, 0);
      vector->resize(ranges[0].count);
      return;
    }
  }
  VectorPtr newVector;
  copyImpl(options, vector, ranges, newVector, false);
  vector = std::move(newVector);
}

bool needFlatten(const DecodedVector& decoded) {
  auto& type = decoded.base()->type();
  return type->isFixedWidth() &&
      type->cppSizeInBytes() <= sizeof(vector_size_t);
}

template <TypeKind kKind>
VectorPtr newConstantImpl(
    const EncodedVectorCopyOptions& options,
    const DecodedVector& source,
    const VectorPtr& sourceBase,
    vector_size_t size) {
  using T = typename KindToFlatVector<kKind>::WrapperType;
  if (source.isNullAt(0)) {
    return std::make_shared<ConstantVector<T>>(
        options.pool, size, true, sourceBase->type(), T());
  }
  if constexpr (!std::is_same_v<T, ComplexType>) {
    return std::make_shared<ConstantVector<T>>(
        options.pool, size, false, sourceBase->type(), source.valueAt<T>(0));
  } else {
    VectorPtr targetBase;
    BaseVector::CopyRange range = {source.index(0), 0, 1};
    copyImpl(options, sourceBase, folly::Range(&range, 1), targetBase, false);
    return std::make_shared<ConstantVector<T>>(
        options.pool, size, 0, std::move(targetBase));
  }
}

VectorPtr newConstant(
    const EncodedVectorCopyOptions& options,
    const VectorPtr& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    DecodedVector& decodedSource,
    const VectorPtr& sourceBase) {
  auto size = newTargetSize(ranges);
  if (options.reuseSource) {
    if (size == source->size()) {
      return source;
    } else {
      return BaseVector::wrapInConstant(size, 0, source);
    }
  } else {
    return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
        newConstantImpl,
        source->typeKind(),
        options,
        decodedSource,
        sourceBase,
        size);
  }
}

VectorPtr newDictionary(
    const EncodedVectorCopyOptions& options,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    DecodedVector& decodedSource,
    const VectorPtr& sourceBase) {
  auto size = newTargetSize(ranges);
  auto nulls = newNulls(size, options.pool, decodedSource.nulls(), ranges);
  auto indices = allocateIndices(size, options.pool);
  VectorPtr alphabet;
  if (options.reuseSource) {
    copyIndices(
        decodedSource.indices(),
        ranges,
        0,
        indices->asMutable<vector_size_t>());
    alphabet = sourceBase;
  } else {
    auto baseRanges = toBaseRanges(
        decodedSource, ranges, 0, indices->asMutable<vector_size_t>());
    copyImpl(options, sourceBase, baseRanges, alphabet, false);
  }
  return BaseVector::wrapInDictionary(
      std::move(nulls), std::move(indices), size, std::move(alphabet));
}

VectorPtr newFlat(
    const EncodedVectorCopyOptions& options,
    const VectorPtr& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges) {
  if (options.reuseSource && ranges.size() == 1) {
    if (ranges[0].sourceIndex == 0 && ranges[0].count == source->size()) {
      return source;
    } else {
      return source->slice(ranges[0].sourceIndex, ranges[0].count);
    }
  } else {
    auto target =
        BaseVector::create(source->type(), newTargetSize(ranges), options.pool);
    target->copyRanges(source.get(), ranges);
    return target;
  }
}

VectorPtr newRow(
    const EncodedVectorCopyOptions& options,
    const RowVector& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges) {
  auto size = newTargetSize(ranges);
  auto nulls = newNulls(size, options.pool, source.rawNulls(), ranges);
  std::vector<VectorPtr> children(source.childrenSize());
  for (column_index_t i = 0; i < children.size(); ++i) {
    copyImpl(options, source.childAt(i), ranges, children[i], false);
  }
  return std::make_shared<RowVector>(
      options.pool, source.type(), std::move(nulls), size, std::move(children));
}

VectorPtr newMap(
    const EncodedVectorCopyOptions& options,
    const MapVector& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges) {
  auto size = newTargetSize(ranges);
  auto nulls = newNulls(size, options.pool, source.rawNulls(), ranges);
  auto offsets = allocateIndices(size, options.pool);
  auto sizes = allocateIndices(size, options.pool);
  auto nestedRanges = toNestedRanges(
      source,
      ranges,
      0,
      offsets->asMutable<vector_size_t>(),
      sizes->asMutable<vector_size_t>());
  VectorPtr keys, values;
  copyImpl(options, source.mapKeys(), nestedRanges, keys, false);
  copyImpl(options, source.mapValues(), nestedRanges, values, false);
  return std::make_shared<MapVector>(
      options.pool,
      source.type(),
      std::move(nulls),
      size,
      std::move(offsets),
      std::move(sizes),
      std::move(keys),
      std::move(values));
}

VectorPtr newArray(
    const EncodedVectorCopyOptions& options,
    const ArrayVector& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges) {
  auto size = newTargetSize(ranges);
  auto nulls = newNulls(size, options.pool, source.rawNulls(), ranges);
  auto offsets = allocateIndices(size, options.pool);
  auto sizes = allocateIndices(size, options.pool);
  auto nestedRanges = toNestedRanges(
      source,
      ranges,
      0,
      offsets->asMutable<vector_size_t>(),
      sizes->asMutable<vector_size_t>());
  VectorPtr elements;
  copyImpl(options, source.elements(), nestedRanges, elements, false);
  return std::make_shared<ArrayVector>(
      options.pool,
      source.type(),
      std::move(nulls),
      size,
      std::move(offsets),
      std::move(sizes),
      std::move(elements));
}

void copyIntoFlat(
    const EncodedVectorCopyOptions& options,
    const VectorPtr& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    VectorPtr& target,
    bool targetMutable) {
  auto size = targetSize(target->size(), ranges);
  if (!targetMutable) {
    FB_LOG_EVERY_MS(WARNING, 1000)
        << "Reallocating target vector in encodedVectorCopy";
    auto newTarget = BaseVector::create(target->type(), size, options.pool);
    newTarget->copy(target.get(), 0, 0, target->size());
    target = std::move(newTarget);
  } else if (target->size() != size) {
    target->resize(size);
  }
  target->ensureWritable(SelectivityVector::empty());
  target->copyRanges(source.get(), ranges);
}

void copyIntoConstant(
    const EncodedVectorCopyOptions& options,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    const VectorPtr& source,
    DecodedVector& decodedSource,
    const VectorPtr& sourceBase,
    DecodedVector& decodedTarget,
    VectorPtr& target,
    bool targetMutable) {
  const auto size = targetSize(target->size(), ranges);
  if (decodedSource.isConstantMapping() &&
      decodedSource.base()->equalValueAt(
          decodedTarget.base(),
          decodedSource.index(0),
          decodedTarget.index(0))) {
    if (target->size() != size) {
      if (targetMutable) {
        target->resize(size);
        target->resetDataDependentFlags(nullptr);
      } else {
        target = BaseVector::wrapInConstant(size, 0, target);
      }
    }
    return;
  }
  if (needFlatten(decodedSource)) {
    copyIntoFlat(options, source, ranges, target, false);
    return;
  }
  BufferPtr nulls;
  auto indices = allocateIndices(size, options.pool);
  auto* rawIndices = indices->asMutable<vector_size_t>();
  std::fill(rawIndices, rawIndices + target->size(), 0);
  VectorPtr alphabet;
  if (decodedSource.isConstantMapping()) {
    fillIndices(1, ranges, rawIndices);
    alphabet = BaseVector::create(target->type(), 2, options.pool);
    alphabet->copy(decodedTarget.base(), 0, decodedTarget.index(0), 1);
    alphabet->copy(decodedSource.base(), 1, decodedSource.index(0), 1);
  } else {
    nulls = newNulls(size, options.pool, decodedSource.nulls(), ranges);
    auto baseRanges = toBaseRanges(decodedSource, ranges, 1, rawIndices);
    alphabet = BaseVector::create(target->type(), 1, options.pool);
    alphabet->copy(decodedTarget.base(), 0, decodedTarget.index(0), 1);
    copyImpl(options, sourceBase, baseRanges, alphabet, true);
  }
  target = BaseVector::wrapInDictionary(
      std::move(nulls), std::move(indices), size, std::move(alphabet));
}

void copyIntoDictionary(
    const EncodedVectorCopyOptions& options,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    DecodedVector& decodedSource,
    const VectorPtr& sourceBase,
    DecodedVector& decodedTarget,
    VectorPtr&& targetBase,
    VectorPtr& target,
    bool targetMutable) {
  // DecodedVector::decodeAndGetBase returns a copy of VectorPtr, increasing the
  // reference count of targetBase by 1.
  const bool targetBaseMutable = targetMutable && targetBase.use_count() <= 2;
  const auto size = targetSize(target->size(), ranges);
  const auto targetBaseSize = targetBaseMutable ? baseSize(decodedTarget) : -1;
  BufferPtr nulls;
  auto* targetNulls = decodedTarget.nulls();
  auto* sourceNulls = decodedSource.nulls();
  if (targetNulls || sourceNulls) {
    if (targetMutable && target->nulls()) {
      nulls = tryReuse<uint64_t>(target->nulls(), bits::nwords(size));
    }
    if (!nulls) {
      nulls = allocateNulls(size, options.pool);
    }
    auto* rawNulls = nulls->asMutable<uint64_t>();
    if (targetNulls && rawNulls != targetNulls) {
      std::memcpy(rawNulls, targetNulls, bits::nbytes(decodedTarget.size()));
    }
    setSourceNulls(sourceNulls, targetNulls, ranges, rawNulls);
  }
  BufferPtr indices;
  if (targetMutable &&
      target->encoding() == VectorEncoding::Simple::DICTIONARY) {
    indices = tryReuse<vector_size_t>(target->wrapInfo(), size);
  }
  if (!indices) {
    indices = allocateIndices(size, options.pool);
  }
  auto* rawIndices = indices->asMutable<vector_size_t>();
  if (rawIndices != decodedTarget.indices()) {
    std::memcpy(
        rawIndices,
        decodedTarget.indices(),
        sizeof(vector_size_t) * decodedTarget.size());
  }
  auto baseRanges =
      toBaseRanges(decodedSource, ranges, decodedTarget, rawIndices);
  if (targetBaseMutable) {
    targetBase->resize(targetBaseSize);
  }
  copyImpl(options, sourceBase, baseRanges, targetBase, targetBaseMutable);
  target = BaseVector::wrapInDictionary(
      std::move(nulls), std::move(indices), size, std::move(targetBase));
}

void copyIntoRow(
    const EncodedVectorCopyOptions& options,
    const RowVector& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    VectorPtr& target,
    bool targetMutable) {
  const auto size = targetSize(target->size(), ranges);
  auto* targetRow = target->asUnchecked<RowVector>();
  auto* sourceNulls = source.rawNulls();
  if (targetMutable) {
    const auto oldSize = target->size();
    targetRow->unsafeResize(size);
    targetRow->resetDataDependentFlags(nullptr);
    setSourceNulls(sourceNulls, *target, ranges);
    for (column_index_t i = 0; i < targetRow->childrenSize(); ++i) {
      auto& targetChild = targetRow->childAt(i);
      bool targetChildMutable = targetChild.use_count() == 1;
      if (targetChildMutable && targetChild->size() > oldSize) {
        // Shrink the size to allow unreferenced nested rows to be released.
        targetChild->resize(oldSize);
      }
      copyImpl(
          options, source.childAt(i), ranges, targetChild, targetChildMutable);
    }
    targetRow->updateContainsLazyNotLoaded();
  } else {
    BufferPtr nulls;
    if (target->rawNulls() || sourceNulls) {
      nulls = combineNulls(
          size,
          options.pool,
          target->rawNulls(),
          target->size(),
          sourceNulls,
          ranges);
    }
    auto children = targetRow->children();
    for (column_index_t i = 0; i < children.size(); ++i) {
      copyImpl(options, source.childAt(i), ranges, children[i], false);
    }
    target = std::make_shared<RowVector>(
        options.pool,
        target->type(),
        std::move(nulls),
        size,
        std::move(children));
  }
}

void copyIntoMap(
    const EncodedVectorCopyOptions& options,
    const MapVector& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    VectorPtr& target,
    bool targetMutable) {
  const auto size = targetSize(target->size(), ranges);
  auto* targetMap = target->asUnchecked<MapVector>();
  auto* sourceNulls = source.rawNulls();

  if (targetMutable) {
    target->resize(size);
    target->resetDataDependentFlags(nullptr);
    auto offsets = targetMap->mutableOffsets(size);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    auto sizes = targetMap->mutableSizes(size);
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    bool mutableKeys = targetMap->mapKeys().use_count() == 1;
    bool mutableValues = targetMap->mapValues().use_count() == 1;
    auto keys = targetMap->mapKeys();
    auto values = targetMap->mapValues();
    auto nestedSize = std::min(keys->size(), values->size());
    if (needCompactNested(options, *targetMap, nestedSize)) {
      auto compactRanges =
          compactNestedRanges(*targetMap, rawOffsets, rawSizes);
      compactNestedVector(options, compactRanges, keys, mutableKeys);
      compactNestedVector(options, compactRanges, values, mutableValues);
      nestedSize = std::min(keys->size(), values->size());
    }
    auto nestedRanges =
        toNestedRanges(source, ranges, nestedSize, rawOffsets, rawSizes);
    copyImpl(options, source.mapKeys(), nestedRanges, keys, mutableKeys);
    copyImpl(options, source.mapValues(), nestedRanges, values, mutableValues);
    targetMap->mapKeys() = std::move(keys);
    targetMap->mapValues() = std::move(values);
    setSourceNulls(sourceNulls, *target, ranges);

  } else {
    BufferPtr nulls;
    if (target->rawNulls() || sourceNulls) {
      nulls = combineNulls(
          size,
          options.pool,
          target->rawNulls(),
          target->size(),
          sourceNulls,
          ranges);
    }
    auto offsets = allocateIndices(size, options.pool);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    std::memcpy(
        rawOffsets,
        targetMap->rawOffsets(),
        sizeof(vector_size_t) * targetMap->size());
    auto sizes = allocateIndices(size, options.pool);
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    std::memcpy(
        rawSizes,
        targetMap->rawSizes(),
        sizeof(vector_size_t) * targetMap->size());
    VectorPtr newKeys;
    VectorPtr newValues;
    {
      auto compactRanges =
          compactNestedRanges(*targetMap, rawOffsets, rawSizes);
      copyImpl(options, targetMap->mapKeys(), compactRanges, newKeys, false);
      copyImpl(
          options, targetMap->mapValues(), compactRanges, newValues, false);
    }
    auto nestedRanges = toNestedRanges(
        source,
        ranges,
        std::min(newKeys->size(), newValues->size()),
        rawOffsets,
        rawSizes);
    copyImpl(options, source.mapKeys(), nestedRanges, newKeys, false);
    copyImpl(options, source.mapValues(), nestedRanges, newValues, false);
    target = std::make_shared<MapVector>(
        options.pool,
        target->type(),
        std::move(nulls),
        size,
        std::move(offsets),
        std::move(sizes),
        std::move(newKeys),
        std::move(newValues));
  }
}

void copyIntoArray(
    const EncodedVectorCopyOptions& options,
    const ArrayVector& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    VectorPtr& target,
    bool targetMutable) {
  const auto size = targetSize(target->size(), ranges);
  auto* targetArray = target->asUnchecked<ArrayVector>();
  auto* sourceNulls = source.rawNulls();

  if (targetMutable) {
    target->resize(size);
    target->resetDataDependentFlags(nullptr);
    auto offsets = targetArray->mutableOffsets(size);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    auto sizes = targetArray->mutableSizes(size);
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    bool mutableElements = targetArray->elements().use_count() == 1;
    auto elements = targetArray->elements();
    if (needCompactNested(options, *targetArray, elements->size())) {
      auto compactRanges =
          compactNestedRanges(*targetArray, rawOffsets, rawSizes);
      compactNestedVector(options, compactRanges, elements, mutableElements);
    }
    auto nestedRanges =
        toNestedRanges(source, ranges, elements->size(), rawOffsets, rawSizes);
    copyImpl(
        options, source.elements(), nestedRanges, elements, mutableElements);
    targetArray->elements() = std::move(elements);
    setSourceNulls(sourceNulls, *target, ranges);

  } else {
    BufferPtr nulls;
    if (target->rawNulls() || sourceNulls) {
      nulls = combineNulls(
          size,
          options.pool,
          target->rawNulls(),
          target->size(),
          sourceNulls,
          ranges);
    }
    auto offsets = allocateIndices(size, options.pool);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();
    std::memcpy(
        rawOffsets,
        targetArray->rawOffsets(),
        sizeof(vector_size_t) * targetArray->size());
    auto sizes = allocateIndices(size, options.pool);
    auto* rawSizes = sizes->asMutable<vector_size_t>();
    std::memcpy(
        rawSizes,
        targetArray->rawSizes(),
        sizeof(vector_size_t) * targetArray->size());
    VectorPtr newElements;
    {
      auto compactRanges =
          compactNestedRanges(*targetArray, rawOffsets, rawSizes);
      copyImpl(
          options, targetArray->elements(), compactRanges, newElements, false);
    }
    auto nestedRanges = toNestedRanges(
        source, ranges, newElements->size(), rawOffsets, rawSizes);
    copyImpl(options, source.elements(), nestedRanges, newElements, false);
    target = std::make_shared<ArrayVector>(
        options.pool,
        target->type(),
        std::move(nulls),
        size,
        std::move(offsets),
        std::move(sizes),
        std::move(newElements));
  }
}

void copyIntoComplex(
    const EncodedVectorCopyOptions& options,
    DecodedVector& decodedSource,
    const VectorPtr& sourceBase,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    VectorPtr& target,
    bool targetMutable) {
  if (decodedSource.isIdentityMapping()) {
    switch (target->encoding()) {
      case VectorEncoding::Simple::ROW:
        copyIntoRow(
            options,
            *sourceBase->asChecked<RowVector>(),
            ranges,
            target,
            targetMutable);
        break;
      case VectorEncoding::Simple::MAP:
        copyIntoMap(
            options,
            *sourceBase->asChecked<MapVector>(),
            ranges,
            target,
            targetMutable);
        break;
      case VectorEncoding::Simple::ARRAY:
        copyIntoArray(
            options,
            *sourceBase->asChecked<ArrayVector>(),
            ranges,
            target,
            targetMutable);
        break;
      default:
        VELOX_UNREACHABLE();
    }
    return;
  }
  const auto size = targetSize(target->size(), ranges);
  BufferPtr nulls;
  auto indices = allocateIndices(size, options.pool);
  auto* rawIndices = indices->asMutable<vector_size_t>();
  std::iota(rawIndices, rawIndices + target->size(), 0);
  BaseVector::CopyRange baseRange;
  baseRange.targetIndex = target->size();
  if (decodedSource.isConstantMapping()) {
    fillIndices(target->size(), ranges, rawIndices);
    baseRange.sourceIndex = decodedSource.index(0);
    baseRange.count = 1;
  } else {
    nulls = newNulls(size, options.pool, decodedSource.nulls(), ranges);
    copyIndices(decodedSource.indices(), ranges, target->size(), rawIndices);
    baseRange.sourceIndex = 0;
    baseRange.count = sourceBase->size();
  }
  copyImpl(
      options, sourceBase, folly::Range(&baseRange, 1), target, targetMutable);
  target = BaseVector::wrapInDictionary(
      std::move(nulls), std::move(indices), size, target);
}

void copyIntoLazy(
    const EncodedVectorCopyOptions& options,
    const VectorPtr& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    VectorPtr& target,
    bool targetMutable) {
  auto* lazyTarget = target->asUnchecked<LazyVector>();
  if (!lazyTarget->isLoaded()) {
    target.reset();
    copyImpl(options, source, ranges, target, false);
  } else {
    auto& loaded = lazyTarget->loadedVectorShared();
    copyImpl(
        options,
        source,
        ranges,
        loaded,
        targetMutable && loaded.use_count() == 1);
    target = loaded;
  }
}

void copyIntoExisting(
    const EncodedVectorCopyOptions& options,
    const VectorPtr& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    DecodedVector& decodedSource,
    const VectorPtr& sourceBase,
    VectorPtr& target,
    bool targetMutable) {
  DecodedVector decodedTarget;
  auto targetBase = decodedTarget.decodeAndGetBase(target);
  if (decodedTarget.isConstantMapping()) {
    copyIntoConstant(
        options,
        ranges,
        source,
        decodedSource,
        sourceBase,
        decodedTarget,
        target,
        targetMutable);
    return;
  }
  if (!decodedTarget.isIdentityMapping()) {
    if (needFlatten(decodedTarget)) {
      auto newTarget = BaseVector::copy(*target, options.pool);
      copyIntoFlat(options, source, ranges, newTarget, true);
      target = std::move(newTarget);
    } else {
      copyIntoDictionary(
          options,
          ranges,
          decodedSource,
          sourceBase,
          decodedTarget,
          std::move(targetBase),
          target,
          targetMutable);
    }
    return;
  }
  switch (target->encoding()) {
    case VectorEncoding::Simple::FLAT:
      copyIntoFlat(options, source, ranges, target, targetMutable);
      break;
    case VectorEncoding::Simple::ROW:
    case VectorEncoding::Simple::MAP:
    case VectorEncoding::Simple::ARRAY:
      copyIntoComplex(
          options, decodedSource, sourceBase, ranges, target, targetMutable);
      break;
    case VectorEncoding::Simple::LAZY:
      copyIntoLazy(options, source, ranges, target, targetMutable);
      break;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported encoding in encodedVectorCopy: {}", source->encoding());
  }
}

void copyImpl(
    const EncodedVectorCopyOptions& options,
    const VectorPtr& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    VectorPtr& target,
    bool targetMutable) {
  if (ranges.empty()) {
    if (!target) {
      target = BaseVector::create(source->type(), 0, options.pool);
    }
    return;
  }
  VELOX_CHECK_GT(source->size(), 0);
  DecodedVector decodedSource;
  auto sourceBase = decodedSource.decodeAndGetBase(source);
  if (target) {
    copyIntoExisting(
        options,
        source,
        ranges,
        decodedSource,
        sourceBase,
        target,
        targetMutable);
    return;
  }
  if (decodedSource.isConstantMapping()) {
    target = newConstant(options, source, ranges, decodedSource, sourceBase);
    return;
  }
  if (!decodedSource.isIdentityMapping()) {
    if (needFlatten(decodedSource)) {
      target = newFlat(options, source, ranges);
    } else {
      target = newDictionary(options, ranges, decodedSource, sourceBase);
    }
    return;
  }
  switch (source->encoding()) {
    case VectorEncoding::Simple::FLAT:
      target = newFlat(options, source, ranges);
      break;
    case VectorEncoding::Simple::ROW:
      target = newRow(options, *source->asUnchecked<RowVector>(), ranges);
      break;
    case VectorEncoding::Simple::MAP:
      target = newMap(options, *source->asUnchecked<MapVector>(), ranges);
      break;
    case VectorEncoding::Simple::ARRAY:
      target = newArray(options, *source->asUnchecked<ArrayVector>(), ranges);
      break;
    case VectorEncoding::Simple::LAZY:
      VELOX_CHECK(!sourceBase->isLazy());
      copyImpl(options, sourceBase, ranges, target, targetMutable);
      break;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported encoding in encodedVectorCopy: {}", source->encoding());
  }
}

} // namespace

void encodedVectorCopy(
    const EncodedVectorCopyOptions& options,
    const VectorPtr& source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    VectorPtr& target) {
  if (options.reuseSource) {
    VELOX_CHECK(source->pool() == options.pool);
  }
  if (target) {
    VELOX_CHECK(*target->type() == *source->type());
    VELOX_CHECK(target->pool() == options.pool);
  }
  VELOX_CHECK(
      0 <= options.compactNestedThreshold &&
      options.compactNestedThreshold <= 1);
  copyImpl(options, source, ranges, target, target.use_count() == 1);
}

} // namespace facebook::velox
