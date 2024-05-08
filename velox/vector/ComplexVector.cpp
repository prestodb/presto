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

#include <optional>
#include <sstream>

#include "velox/common/base/Exceptions.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SimpleVector.h"

namespace facebook {
namespace velox {

// Up to # of elements to show as debug string for `toString()`.
constexpr vector_size_t kMaxElementsInToString = 5;

std::string stringifyTruncatedElementList(
    vector_size_t start,
    vector_size_t size,
    vector_size_t limit,
    std::string_view delimiter,
    const std::function<void(std::stringstream&, vector_size_t)>&
        stringifyElementCB) {
  std::stringstream out;
  if (size == 0) {
    return "<empty>";
  }
  out << size << " elements starting at " << start << " {";

  const vector_size_t limitedSize = std::min(size, limit);
  for (vector_size_t i = 0; i < limitedSize; ++i) {
    if (i > 0) {
      out << delimiter;
    }
    stringifyElementCB(out, start + i);
  }

  if (size > limitedSize) {
    if (limitedSize) {
      out << delimiter;
    }
    out << "...";
  }
  out << "}";
  return out.str();
}

// static
std::shared_ptr<RowVector> RowVector::createEmpty(
    std::shared_ptr<const Type> type,
    velox::memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(type, "Vector creation requires a non-null type.");
  VELOX_CHECK(type->isRow());
  return std::static_pointer_cast<RowVector>(BaseVector::create(type, 0, pool));
}

bool RowVector::containsNullAt(vector_size_t idx) const {
  if (BaseVector::isNullAt(idx)) {
    return true;
  }

  for (const auto& child : children_) {
    if (child != nullptr && child->containsNullAt(idx)) {
      return true;
    }
  }

  return false;
}

std::optional<int32_t> RowVector::compare(
    const BaseVector* other,
    vector_size_t index,
    vector_size_t otherIndex,
    CompareFlags flags) const {
  auto otherRow = other->wrappedVector()->as<RowVector>();
  if (otherRow->encoding() != VectorEncoding::Simple::ROW) {
    VELOX_CHECK(
        false,
        "Compare of ROW and non-ROW {} and {}",
        BaseVector::toString(),
        otherRow->BaseVector::toString());
  }

  bool isNull = isNullAt(index);
  bool otherNull = other->isNullAt(otherIndex);

  if (isNull || otherNull) {
    return BaseVector::compareNulls(isNull, otherNull, flags);
  }

  if (flags.equalsOnly && children_.size() != otherRow->children_.size()) {
    return 1;
  }

  auto compareSize = std::min(children_.size(), otherRow->children_.size());
  bool resultIsindeterminate = false;
  for (int32_t i = 0; i < compareSize; ++i) {
    BaseVector* child = children_[i].get();
    BaseVector* otherChild = otherRow->childAt(i).get();
    if (!child && !otherChild) {
      continue;
    }
    if (!child || !otherChild) {
      return child ? 1 : -1; // Absent child counts as less.
    }
    if (child->typeKind() != otherChild->typeKind()) {
      VELOX_CHECK(
          false,
          "Compare of different child types: {} and {}",
          BaseVector::toString(),
          other->BaseVector::toString());
    }
    auto wrappedOtherIndex = other->wrappedIndex(otherIndex);
    auto result = child->compare(
        otherChild->loadedVector(), index, wrappedOtherIndex, flags);
    if (result == kIndeterminate) {
      VELOX_DCHECK(
          flags.equalsOnly,
          "Compare should have thrown when null is encountered in child.");
      resultIsindeterminate = true;
    } else if (result.value() != 0) {
      // If values are not equal no need to continue looping.
      return result;
    }
  }

  if (resultIsindeterminate) {
    return kIndeterminate;
  }
  return children_.size() - otherRow->children_.size();
}

void RowVector::appendToChildren(
    const RowVector* source,
    vector_size_t sourceIndex,
    vector_size_t count,
    vector_size_t index) {
  for (int32_t i = 0; i < children_.size(); ++i) {
    auto& child = children_[i];
    child->copy(source->childAt(i)->loadedVector(), index, sourceIndex, count);
  }
}

void RowVector::copy(
    const BaseVector* source,
    vector_size_t targetIndex,
    vector_size_t sourceIndex,
    vector_size_t count) {
  if (count == 0) {
    return;
  }
  SelectivityVector rows(targetIndex + count);
  rows.setValidRange(0, targetIndex, false);
  rows.updateBounds();

  BufferPtr indices;
  vector_size_t* toSourceRow = nullptr;
  if (sourceIndex != targetIndex) {
    indices =
        AlignedBuffer::allocate<vector_size_t>(targetIndex + count, pool_);
    toSourceRow = indices->asMutable<vector_size_t>();
    std::iota(
        toSourceRow + targetIndex,
        toSourceRow + targetIndex + count,
        sourceIndex);
  }

  copy(source, rows, toSourceRow);
}

void RowVector::copy(
    const BaseVector* source,
    const SelectivityVector& rows,
    const vector_size_t* toSourceRow) {
  if (source->typeKind() == TypeKind::UNKNOWN) {
    rows.applyToSelected([&](auto row) { setNull(row, true); });
    return;
  }

  // Copy non-null values.
  SelectivityVector nonNullRows = rows;
  if (!toSourceRow) {
    VELOX_CHECK_GE(source->size(), rows.end());
  }

  DecodedVector decodedSource(*source);
  if (decodedSource.isIdentityMapping()) {
    if (source->mayHaveNulls()) {
      auto rawNulls = source->rawNulls();
      rows.applyToSelected([&](auto row) {
        auto idx = toSourceRow ? toSourceRow[row] : row;
        VELOX_DCHECK_GT(source->size(), idx);
        if (bits::isBitNull(rawNulls, idx)) {
          nonNullRows.setValid(row, false);
        }
      });
      nonNullRows.updateBounds();
    }

    auto rowSource = source->loadedVector()->as<RowVector>();
    for (auto i = 0; i < childrenSize_; ++i) {
      if (rowSource->childAt(i)) {
        BaseVector::ensureWritable(
            rows, type()->asRow().childAt(i), pool(), children_[i]);
        children_[i]->copy(
            rowSource->childAt(i)->loadedVector(), nonNullRows, toSourceRow);
      } else {
        children_[i].reset();
      }
    }
  } else {
    auto nulls = decodedSource.nulls(nullptr);

    if (nulls) {
      rows.applyToSelected([&](auto row) {
        auto idx = toSourceRow ? toSourceRow[row] : row;
        VELOX_DCHECK_GT(source->size(), idx);
        if (bits::isBitNull(nulls, idx)) {
          nonNullRows.setValid(row, false);
        }
      });
      nonNullRows.updateBounds();
    }

    // Copy baseSource[indices[toSource[row]]] into row.
    auto indices = decodedSource.indices();
    BufferPtr mappedIndices;
    vector_size_t* rawMappedIndices = nullptr;
    if (toSourceRow) {
      mappedIndices = AlignedBuffer::allocate<vector_size_t>(rows.end(), pool_);
      rawMappedIndices = mappedIndices->asMutable<vector_size_t>();
      nonNullRows.applyToSelected(
          [&](auto row) { rawMappedIndices[row] = indices[toSourceRow[row]]; });
    }

    auto baseSource = decodedSource.base()->as<RowVector>();
    for (auto i = 0; i < childrenSize_; ++i) {
      if (baseSource->childAt(i)) {
        BaseVector::ensureWritable(
            rows, type()->asRow().childAt(i), pool(), children_[i]);
        children_[i]->copy(
            baseSource->childAt(i)->loadedVector(),
            nonNullRows,
            rawMappedIndices ? rawMappedIndices : indices);
      } else {
        children_[i].reset();
      }
    }
  }

  if (nulls_) {
    nonNullRows.clearNulls(nulls_);
  }

  // Copy nulls.
  if (source->mayHaveNulls()) {
    SelectivityVector nullRows = rows;
    nullRows.deselect(nonNullRows);
    if (nullRows.hasSelections()) {
      ensureNulls();
      nullRows.setNulls(nulls_);
    }
  }
}

void RowVector::setType(const TypePtr& type) {
  BaseVector::setType(type);
  for (auto i = 0; i < childrenSize_; i++) {
    children_[i]->setType(type_->asRow().childAt(i));
  }
}

namespace {

// Runs quick checks to determine whether input vector has only null values.
// @return true if vector has only null values; false if vector may have
// non-null values.
bool isAllNullVector(const BaseVector& vector) {
  if (vector.typeKind() == TypeKind::UNKNOWN) {
    return true;
  }

  if (vector.isConstantEncoding()) {
    return vector.isNullAt(0);
  }

  auto leafVector = vector.wrappedVector();
  if (leafVector->isConstantEncoding()) {
    // A null constant does not have a value vector, so wrappedVector
    // returns the constant.
    VELOX_CHECK(leafVector->isNullAt(0));
    return true;
  }
  return false;
}
} // namespace

void RowVector::copyRanges(
    const BaseVector* source,
    const folly::Range<const CopyRange*>& ranges) {
  if (ranges.empty()) {
    return;
  }

  if (isAllNullVector(*source)) {
    BaseVector::setNulls(mutableRawNulls(), ranges, true);
    return;
  }

  auto minTargetIndex = std::numeric_limits<vector_size_t>::max();
  auto maxTargetIndex = std::numeric_limits<vector_size_t>::min();
  applyToEachRange(ranges, [&](auto targetIndex, auto sourceIndex, auto count) {
    minTargetIndex = std::min(minTargetIndex, targetIndex);
    maxTargetIndex = std::max(maxTargetIndex, targetIndex + count);
  });

  SelectivityVector rows(maxTargetIndex);
  rows.setValidRange(0, minTargetIndex, false);
  rows.updateBounds();
  for (auto i = 0; i < children_.size(); ++i) {
    BaseVector::ensureWritable(
        rows, type()->asRow().childAt(i), pool(), children_[i]);
  }

  DecodedVector decoded(*source);
  if (decoded.isIdentityMapping() && !decoded.mayHaveNulls()) {
    if (rawNulls_) {
      setNulls(mutableRawNulls(), ranges, false);
    }
    auto* rowSource = source->loadedVector()->as<RowVector>();
    for (int i = 0; i < children_.size(); ++i) {
      children_[i]->copyRanges(rowSource->childAt(i)->loadedVector(), ranges);
    }
  } else {
    std::vector<BaseVector::CopyRange> baseRanges;
    baseRanges.reserve(ranges.size());
    applyToEachRow(ranges, [&](auto targetIndex, auto sourceIndex) {
      bool isNull = decoded.isNullAt(sourceIndex);
      setNull(targetIndex, isNull);
      if (isNull) {
        return;
      }
      auto baseIndex = decoded.index(sourceIndex);
      if (!baseRanges.empty() &&
          baseRanges.back().sourceIndex + 1 == baseIndex &&
          baseRanges.back().targetIndex + 1 == targetIndex) {
        ++baseRanges.back().count;
      } else {
        baseRanges.push_back({
            .sourceIndex = baseIndex,
            .targetIndex = targetIndex,
            .count = 1,
        });
      }
    });

    auto* rowSource = decoded.base()->as<RowVector>();
    for (int i = 0; i < children_.size(); ++i) {
      children_[i]->copyRanges(
          rowSource->childAt(i)->loadedVector(), baseRanges);
    }
  }
}

uint64_t RowVector::hashValueAt(vector_size_t index) const {
  if (isNullAt(index)) {
    return BaseVector::kNullHash;
  }
  uint64_t hash = BaseVector::kNullHash;
  bool isFirst = true;
  for (auto i = 0; i < childrenSize(); ++i) {
    auto& child = children_[i];
    if (child) {
      auto childHash = child->hashValueAt(index);
      hash = isFirst ? childHash : bits::hashMix(hash, childHash);
      isFirst = false;
    }
  }
  return hash;
}

std::unique_ptr<SimpleVector<uint64_t>> RowVector::hashAll() const {
  VELOX_NYI();
}

std::string RowVector::toString(vector_size_t index) const {
  VELOX_CHECK_LT(index, length_, "Vector index should be less than length.");
  if (isNullAt(index)) {
    return "null";
  }
  std::stringstream out;
  out << "{";
  for (int32_t i = 0; i < children_.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << (children_[i] ? children_[i]->toString(index) : "<not set>");
  }
  out << "}";
  return out.str();
}

void RowVector::ensureWritable(const SelectivityVector& rows) {
  for (int i = 0; i < childrenSize_; i++) {
    if (children_[i]) {
      BaseVector::ensureWritable(
          rows, children_[i]->type(), BaseVector::pool_, children_[i]);
    }
  }
  BaseVector::ensureWritable(rows);
}

bool RowVector::isWritable() const {
  for (int i = 0; i < childrenSize_; i++) {
    if (children_[i]) {
      if (!BaseVector::isVectorWritable(children_[i])) {
        return false;
      }
    }
  }

  return isNullsWritable();
}

uint64_t RowVector::estimateFlatSize() const {
  uint64_t total = BaseVector::retainedSize();
  for (const auto& child : children_) {
    if (child) {
      total += child->estimateFlatSize();
    }
  }

  return total;
}

void RowVector::prepareForReuse() {
  BaseVector::prepareForReuse();
  for (auto& child : children_) {
    if (child) {
      BaseVector::prepareForReuse(child, 0);
    }
  }
  updateContainsLazyNotLoaded();
}

VectorPtr RowVector::slice(vector_size_t offset, vector_size_t length) const {
  std::vector<VectorPtr> children(children_.size());
  for (int i = 0; i < children_.size(); ++i) {
    if (children_[i]) {
      children[i] = children_[i]->slice(offset, length);
    }
  }
  return std::make_shared<RowVector>(
      pool_, type_, sliceNulls(offset, length), length, std::move(children));
}

BaseVector* RowVector::loadedVector() {
  if (childrenLoaded_) {
    return this;
  }
  containsLazyNotLoaded_ = false;
  for (auto i = 0; i < childrenSize_; ++i) {
    if (!children_[i]) {
      continue;
    }
    auto& newChild = BaseVector::loadedVectorShared(children_[i]);
    // This is not needed but can potentially optimize decoding speed later.
    if (children_[i].get() != newChild.get()) {
      children_[i] = newChild;
    }
    if (isLazyNotLoaded(*children_[i])) {
      containsLazyNotLoaded_ = true;
    }
  }
  childrenLoaded_ = true;
  return this;
}

void RowVector::updateContainsLazyNotLoaded() const {
  childrenLoaded_ = false;
  containsLazyNotLoaded_ = false;
  for (auto& child : children_) {
    if (child && isLazyNotLoaded(*child)) {
      containsLazyNotLoaded_ = true;
      break;
    }
  }
}

void ArrayVectorBase::copyRangesImpl(
    const BaseVector* source,
    const folly::Range<const BaseVector::CopyRange*>& ranges,
    VectorPtr* targetValues,
    VectorPtr* targetKeys) {
  if (isAllNullVector(*source)) {
    BaseVector::setNulls(mutableRawNulls(), ranges, true);
    return;
  }

  const BaseVector* sourceValues;
  const BaseVector* sourceKeys = nullptr;

  auto leafSource = source->wrappedVector();
  VELOX_CHECK_EQ(leafSource->encoding(), encoding());

  if (typeKind_ == TypeKind::ARRAY) {
    sourceValues = leafSource->as<ArrayVector>()->elements().get();
  } else {
    sourceValues = leafSource->as<MapVector>()->mapValues().get();
    sourceKeys = leafSource->as<MapVector>()->mapKeys().get();
  }

  if (targetKeys) {
    BaseVector::ensureWritable(
        SelectivityVector::empty(),
        targetKeys->get()->type(),
        pool(),
        *targetKeys);
  } else {
    BaseVector::ensureWritable(
        SelectivityVector::empty(),
        targetValues->get()->type(),
        pool(),
        *targetValues);
  }

  auto sourceArray = leafSource->asUnchecked<ArrayVectorBase>();
  auto setNotNulls = mayHaveNulls() || source->mayHaveNulls();
  auto* mutableOffsets =
      this->mutableOffsets(length_)->asMutable<vector_size_t>();
  auto* mutableSizes = this->mutableSizes(length_)->asMutable<vector_size_t>();
  vector_size_t childSize = targetValues->get()->size();
  if (ranges.size() == 1 && ranges.back().count == 1) {
    auto& range = ranges.back();
    if (range.count == 0) {
      return;
    }
    VELOX_DCHECK(BaseVector::length_ >= range.targetIndex + range.count);
    // Fast path if we're just copying a single array.
    if (source->isNullAt(range.sourceIndex)) {
      setNull(range.targetIndex, true);
    } else {
      if (setNotNulls) {
        setNull(range.targetIndex, false);
      }

      vector_size_t wrappedIndex = source->wrappedIndex(range.sourceIndex);
      vector_size_t copySize = sourceArray->sizeAt(wrappedIndex);

      mutableOffsets[range.targetIndex] = childSize;
      mutableSizes[range.targetIndex] = copySize;

      if (copySize > 0) {
        auto copyOffset = sourceArray->offsetAt(wrappedIndex);
        targetValues->get()->resize(childSize + copySize);
        targetValues->get()->copy(
            sourceValues, childSize, copyOffset, copySize);
        if (targetKeys) {
          targetKeys->get()->resize(childSize + copySize);
          targetKeys->get()->copy(sourceKeys, childSize, copyOffset, copySize);
        }
      }
    }
  } else {
    std::vector<CopyRange> outRanges;
    vector_size_t totalCount = 0;
    applyToEachRange(
        ranges, [&](auto targetIndex, auto sourceIndex, auto count) {
          if (count > 0) {
            VELOX_DCHECK(BaseVector::length_ >= targetIndex + count);
            totalCount += count;
          }
        });
    outRanges.reserve(totalCount);
    applyToEachRow(ranges, [&](auto targetIndex, auto sourceIndex) {
      if (source->isNullAt(sourceIndex)) {
        setNull(targetIndex, true);
      } else {
        if (setNotNulls) {
          setNull(targetIndex, false);
        }
        auto wrappedIndex = source->wrappedIndex(sourceIndex);
        auto copySize = sourceArray->sizeAt(wrappedIndex);

        if (copySize > 0) {
          auto copyOffset = sourceArray->offsetAt(wrappedIndex);

          // If we're copying two adjacent ranges, merge them.  This only
          // works if they're consecutive.
          if (!outRanges.empty() &&
              (outRanges.back().sourceIndex + outRanges.back().count ==
               copyOffset)) {
            outRanges.back().count += copySize;
          } else {
            outRanges.push_back({copyOffset, childSize, copySize});
          }
        }

        mutableOffsets[targetIndex] = childSize;
        mutableSizes[targetIndex] = copySize;
        childSize += copySize;
      }
    });

    targetValues->get()->resize(childSize);
    targetValues->get()->copyRanges(sourceValues, outRanges);
    if (targetKeys) {
      targetKeys->get()->resize(childSize);
      targetKeys->get()->copyRanges(sourceKeys, outRanges);
    }
  }
}

void RowVector::validate(const VectorValidateOptions& options) const {
  BaseVector::validate(options);
  vector_size_t lastNonNullIndex{size()};

  if (nulls_) {
    lastNonNullIndex = bits::findLastBit(nulls_->as<uint64_t>(), 0, size());
  }

  for (auto& child : children_) {
    // TODO: Currently we arent checking for null children on ROWs
    // since there are cases in SelectiveStructReader/DWIO/Koski where
    // ROW Vectors with null children are created.
    if (child != nullptr) {
      child->validate(options);
      if (child->size() < size()) {
        VELOX_CHECK_NOT_NULL(
            nulls_,
            "Child vector has size less than parent and parent has no nulls.");

        VELOX_CHECK_GT(
            child->size(),
            lastNonNullIndex,
            "Child vector has size less than last non null row.");
      }
    }
  }
}

void RowVector::unsafeResize(vector_size_t newSize, bool setNotNull) {
  BaseVector::resize(newSize, setNotNull);
}

void RowVector::resize(vector_size_t newSize, bool setNotNull) {
  const auto oldSize = size();
  BaseVector::resize(newSize, setNotNull);

  // Resize all the children.
  for (auto& child : children_) {
    if (child != nullptr) {
      if (child->isLazy()) {
        VELOX_FAIL("Resize on a lazy vector is not allowed");
      }

      // If we are just reducing the size of the vector, its safe
      // to skip uniqueness check since effectively we are just changing
      // the length.
      if (newSize > oldSize) {
        VELOX_CHECK(child.unique(), "Resizing shared child vector");
        child->resize(newSize, setNotNull);
      }
    }
  }
}

void ArrayVectorBase::checkRanges() const {
  std::unordered_map<vector_size_t, vector_size_t> seenElements;
  seenElements.reserve(size());

  for (vector_size_t i = 0; i < size(); ++i) {
    auto size = sizeAt(i);
    auto offset = offsetAt(i);

    for (vector_size_t j = 0; j < size; ++j) {
      auto it = seenElements.find(offset + j);
      if (it != seenElements.end()) {
        VELOX_FAIL(
            "checkRanges() found overlap at idx {}: element {} has offset {} "
            "and size {}, and element {} has offset {} and size {}.",
            offset + j,
            it->second,
            offsetAt(it->second),
            sizeAt(it->second),
            i,
            offset,
            size);
      }
      seenElements.emplace(offset + j, i);
    }
  }
}

void ArrayVectorBase::validateArrayVectorBase(
    const VectorValidateOptions& options,
    vector_size_t minChildVectorSize) const {
  BaseVector::validate(options);
  auto bufferByteSize = byteSize<vector_size_t>(BaseVector::length_);
  VELOX_CHECK_GE(sizes_->size(), bufferByteSize);
  VELOX_CHECK_GE(offsets_->size(), bufferByteSize);
  for (auto i = 0; i < BaseVector::length_; ++i) {
    const bool isNull =
        BaseVector::rawNulls_ && bits::isBitNull(BaseVector::rawNulls_, i);
    if (isNull || rawSizes_[i] == 0) {
      continue;
    }
    // Verify index for a non-null position. It must be >= 0 and < size of the
    // base vector.
    VELOX_CHECK_GE(
        rawSizes_[i],
        0,
        "ArrayVectorBase size must be greater than zero. Index: {}.",
        i);
    VELOX_CHECK_GE(
        rawOffsets_[i],
        0,
        "ArrayVectorBase offset must be greater than zero. Index: {}.",
        i)
    VELOX_CHECK_LT(
        rawOffsets_[i] + rawSizes_[i] - 1,
        minChildVectorSize,
        "ArrayVectorBase must only point to indices within the base "
        "vector's size. Index: {}.",
        i);
  }
}

namespace {

struct IndexRange {
  vector_size_t begin;
  vector_size_t size;
};

std::optional<int32_t> compareArrays(
    const BaseVector& left,
    const BaseVector& right,
    IndexRange leftRange,
    IndexRange rightRange,
    CompareFlags flags) {
  if (flags.equalsOnly && leftRange.size != rightRange.size) {
    // return early if not caring about collation order.
    return 1;
  }
  auto compareSize = std::min(leftRange.size, rightRange.size);
  bool resultIsindeterminate = false;
  for (auto i = 0; i < compareSize; ++i) {
    auto result =
        left.compare(&right, leftRange.begin + i, rightRange.begin + i, flags);
    if (result == kIndeterminate) {
      VELOX_DCHECK(
          flags.equalsOnly,
          "Compare should have thrown when null is encountered in child.");
      resultIsindeterminate = true;
    } else if (result.value() != 0) {
      return result;
    }
  }
  if (resultIsindeterminate) {
    return kIndeterminate;
  }

  int result = leftRange.size - rightRange.size;
  return flags.ascending ? result : result * -1;
}

std::optional<int32_t> compareArrays(
    const BaseVector& left,
    const BaseVector& right,
    folly::Range<const vector_size_t*> leftRange,
    folly::Range<const vector_size_t*> rightRange,
    CompareFlags flags) {
  if (flags.equalsOnly && leftRange.size() != rightRange.size()) {
    // return early if not caring about collation order.
    return 1;
  }

  auto compareSize = std::min(leftRange.size(), rightRange.size());

  bool resultIsindeterminate = false;
  for (auto i = 0; i < compareSize; ++i) {
    auto result = left.compare(&right, leftRange[i], rightRange[i], flags);
    if (result == kIndeterminate) {
      VELOX_DCHECK(
          flags.equalsOnly,
          "Compare should have thrown when null is encountered in child.");
      resultIsindeterminate = true;
    } else if (result.value() != 0) {
      return result;
    }
  }

  if (resultIsindeterminate) {
    return kIndeterminate;
  }

  int result = leftRange.size() - rightRange.size();
  return flags.ascending ? result : result * -1;
}
} // namespace

bool ArrayVector::containsNullAt(vector_size_t idx) const {
  if (BaseVector::isNullAt(idx)) {
    return true;
  }

  const auto offset = rawOffsets_[idx];
  const auto size = rawSizes_[idx];
  for (auto i = 0; i < size; ++i) {
    if (elements_->containsNullAt(offset + i)) {
      return true;
    }
  }

  return false;
}

std::optional<int32_t> ArrayVector::compare(
    const BaseVector* other,
    vector_size_t index,
    vector_size_t otherIndex,
    CompareFlags flags) const {
  bool isNull = isNullAt(index);
  bool otherNull = other->isNullAt(otherIndex);
  if (isNull || otherNull) {
    return BaseVector::compareNulls(isNull, otherNull, flags);
  }
  auto otherValue = other->wrappedVector();
  auto wrappedOtherIndex = other->wrappedIndex(otherIndex);
  VELOX_CHECK_EQ(
      VectorEncoding::Simple::ARRAY,
      otherValue->encoding(),
      "Compare of ARRAY and non-ARRAY: {} and {}",
      BaseVector::toString(),
      other->BaseVector::toString());

  auto otherArray = otherValue->asUnchecked<ArrayVector>();
  auto otherElements = otherArray->elements_.get();
  if (elements_->typeKind() != otherElements->typeKind()) {
    VELOX_CHECK(
        false,
        "Compare of arrays of different element type: {} and {}",
        BaseVector::toString(),
        otherArray->BaseVector::toString());
  }

  if (flags.equalsOnly &&
      rawSizes_[index] != otherArray->rawSizes_[wrappedOtherIndex]) {
    return 1;
  }
  return compareArrays(
      *elements_,
      *otherArray->elements_,
      IndexRange{rawOffsets_[index], rawSizes_[index]},
      IndexRange{
          otherArray->rawOffsets_[wrappedOtherIndex],
          otherArray->rawSizes_[wrappedOtherIndex]},
      flags);
}

void ArrayVector::setType(const TypePtr& type) {
  BaseVector::setType(type);
  elements_->setType(type_->asArray().elementType());
}

namespace {
uint64_t hashArray(
    uint64_t hash,
    const BaseVector& elements,
    vector_size_t offset,
    vector_size_t size) {
  for (auto i = 0; i < size; ++i) {
    auto elementHash = elements.hashValueAt(offset + i);
    hash = bits::commutativeHashMix(hash, elementHash);
  }
  return hash;
}
} // namespace

uint64_t ArrayVector::hashValueAt(vector_size_t index) const {
  if (isNullAt(index)) {
    return BaseVector::kNullHash;
  }
  return hashArray(
      BaseVector::kNullHash, *elements_, rawOffsets_[index], rawSizes_[index]);
}

std::unique_ptr<SimpleVector<uint64_t>> ArrayVector::hashAll() const {
  VELOX_NYI();
}

std::string ArrayVector::toString(vector_size_t index) const {
  VELOX_CHECK_LT(index, length_, "Vector index should be less than length.");
  if (isNullAt(index)) {
    return "null";
  }

  return stringifyTruncatedElementList(
      rawOffsets_[index],
      rawSizes_[index],
      kMaxElementsInToString,
      ", ",
      [this](std::stringstream& ss, vector_size_t index) {
        ss << elements_->toString(index);
      });
}

void ArrayVector::ensureWritable(const SelectivityVector& rows) {
  auto newSize = std::max<vector_size_t>(rows.end(), BaseVector::length_);
  if (offsets_ && !offsets_->unique()) {
    BufferPtr newOffsets =
        AlignedBuffer::allocate<vector_size_t>(newSize, BaseVector::pool_);
    auto rawNewOffsets = newOffsets->asMutable<vector_size_t>();

    // Copy the whole buffer. An alternative could be
    // (1) fill the buffer with zeros and copy over elements not in "rows";
    // (2) or copy over elements not in "rows" and mark "rows" elements as null
    // Leaving offsets or sizes of "rows" elements unspecified leaves the
    // vector in unusable state.
    memcpy(
        rawNewOffsets,
        rawOffsets_,
        byteSize<vector_size_t>(BaseVector::length_));

    offsets_ = std::move(newOffsets);
    rawOffsets_ = offsets_->as<vector_size_t>();
  }

  if (sizes_ && !sizes_->unique()) {
    BufferPtr newSizes =
        AlignedBuffer::allocate<vector_size_t>(newSize, BaseVector::pool_);
    auto rawNewSizes = newSizes->asMutable<vector_size_t>();
    memcpy(
        rawNewSizes, rawSizes_, byteSize<vector_size_t>(BaseVector::length_));

    sizes_ = std::move(newSizes);
    rawSizes_ = sizes_->asMutable<vector_size_t>();
  }

  // Vectors are write-once and nested elements are append only,
  // hence, all values already written must be preserved.
  BaseVector::ensureWritable(
      SelectivityVector::empty(),
      type()->childAt(0),
      BaseVector::pool_,
      elements_);
  BaseVector::ensureWritable(rows);
}

bool ArrayVector::isWritable() const {
  if (offsets_ && !offsets_->isMutable()) {
    return false;
  }

  if (sizes_ && !sizes_->isMutable()) {
    return false;
  }

  return isNullsWritable() && BaseVector::isVectorWritable(elements_);
}

uint64_t ArrayVector::estimateFlatSize() const {
  return BaseVector::retainedSize() + offsets_->capacity() +
      sizes_->capacity() + elements_->estimateFlatSize();
}

namespace {
void zeroOutBuffer(BufferPtr buffer) {
  memset(buffer->asMutable<char>(), 0, buffer->size());
}
} // namespace

void ArrayVector::prepareForReuse() {
  BaseVector::prepareForReuse();

  if (!offsets_->isMutable()) {
    offsets_ = nullptr;
  } else {
    zeroOutBuffer(offsets_);
  }

  if (!sizes_->isMutable()) {
    sizes_ = nullptr;
  } else {
    zeroOutBuffer(sizes_);
  }

  BaseVector::prepareForReuse(elements_, 0);
}

VectorPtr ArrayVector::slice(vector_size_t offset, vector_size_t length) const {
  return std::make_shared<ArrayVector>(
      pool_,
      type_,
      sliceNulls(offset, length),
      length,
      sliceBuffer(*INTEGER(), offsets_, offset, length, pool_),
      sliceBuffer(*INTEGER(), sizes_, offset, length, pool_),
      elements_);
}

void ArrayVector::validate(const VectorValidateOptions& options) const {
  ArrayVectorBase::validateArrayVectorBase(options, elements_->size());
  elements_->validate(options);
}

void ArrayVector::copyRanges(
    const BaseVector* source,
    const folly::Range<const CopyRange*>& ranges) {
  copyRangesImpl(source, ranges, &elements_, nullptr);
}

bool MapVector::containsNullAt(vector_size_t idx) const {
  if (BaseVector::isNullAt(idx)) {
    return true;
  }

  const auto offset = rawOffsets_[idx];
  const auto size = rawSizes_[idx];
  for (auto i = 0; i < size; ++i) {
    if (keys_->containsNullAt(offset + i)) {
      return true;
    }

    if (values_->containsNullAt(offset + i)) {
      return true;
    }
  }

  return false;
}

std::optional<int32_t> MapVector::compare(
    const BaseVector* other,
    vector_size_t index,
    vector_size_t otherIndex,
    CompareFlags flags) const {
  VELOX_CHECK(
      flags.nullAsValue() || flags.equalsOnly, "Map is not orderable type");

  bool isNull = isNullAt(index);
  bool otherNull = other->isNullAt(otherIndex);
  if (isNull || otherNull) {
    return BaseVector::compareNulls(isNull, otherNull, flags);
  }

  auto otherValue = other->wrappedVector();
  auto wrappedOtherIndex = other->wrappedIndex(otherIndex);
  VELOX_CHECK_EQ(
      VectorEncoding::Simple::MAP,
      otherValue->encoding(),
      "Compare of MAP and non-MAP: {} and {}",
      BaseVector::toString(),
      otherValue->BaseVector::toString());
  auto otherMap = otherValue->as<MapVector>();

  if (keys_->typeKind() != otherMap->keys_->typeKind() ||
      values_->typeKind() != otherMap->values_->typeKind()) {
    VELOX_CHECK(
        false,
        "Compare of maps of different key/value types: {} and {}",
        BaseVector::toString(),
        otherMap->BaseVector::toString());
  }

  if (flags.equalsOnly &&
      rawSizes_[index] != otherMap->rawSizes_[wrappedOtherIndex]) {
    return 1;
  }

  auto leftIndices = sortedKeyIndices(index);
  auto rightIndices = otherMap->sortedKeyIndices(wrappedOtherIndex);

  auto result =
      compareArrays(*keys_, *otherMap->keys_, leftIndices, rightIndices, flags);
  VELOX_DCHECK(result.has_value(), "Keys may not have nulls or nested nulls");

  // Keys are not the same, values not compared.
  if (result.value()) {
    return result;
  }

  return compareArrays(
      *values_, *otherMap->values_, leftIndices, rightIndices, flags);
}

uint64_t MapVector::hashValueAt(vector_size_t index) const {
  if (isNullAt(index)) {
    return BaseVector::kNullHash;
  }
  auto offset = rawOffsets_[index];
  auto size = rawSizes_[index];
  // We use a commutative hash mix, thus we do not sort first.
  return hashArray(
      hashArray(BaseVector::kNullHash, *keys_, offset, size),
      *values_,
      offset,
      size);
}

std::unique_ptr<SimpleVector<uint64_t>> MapVector::hashAll() const {
  VELOX_NYI();
}

bool MapVector::isSorted(vector_size_t index) const {
  if (isNullAt(index)) {
    return true;
  }
  auto offset = rawOffsets_[index];
  auto size = rawSizes_[index];
  for (auto i = 1; i < size; ++i) {
    if (keys_->compare(keys_.get(), offset + i - 1, offset + i) >= 0) {
      return false;
    }
  }
  return true;
}

void MapVector::setType(const TypePtr& type) {
  BaseVector::setType(type);
  const auto& mapType = type_->asMap();
  keys_->setType(mapType.keyType());
  values_->setType(mapType.valueType());
}

// static
void MapVector::canonicalize(
    const std::shared_ptr<MapVector>& map,
    bool useStableSort) {
  if (map->sortedKeys_) {
    return;
  }
  // This is not safe if 'this' is referenced from other
  // threads. The keys and values do not have to be uniquely owned
  // since they are not mutated but rather transposed, which is
  // non-destructive.
  VELOX_CHECK(map.unique());
  BufferPtr indices;
  vector_size_t* indicesRange;
  for (auto i = 0; i < map->BaseVector::length_; ++i) {
    if (map->isSorted(i)) {
      continue;
    }
    if (!indices) {
      indices = map->elementIndices();
      indicesRange = indices->asMutable<vector_size_t>();
    }
    auto offset = map->rawOffsets_[i];
    auto size = map->rawSizes_[i];
    if (useStableSort) {
      std::stable_sort(
          indicesRange + offset,
          indicesRange + offset + size,
          [&](vector_size_t left, vector_size_t right) {
            return map->keys_->compare(map->keys_.get(), left, right) < 0;
          });
    } else {
      std::sort(
          indicesRange + offset,
          indicesRange + offset + size,
          [&](vector_size_t left, vector_size_t right) {
            return map->keys_->compare(map->keys_.get(), left, right) < 0;
          });
    }
  }
  if (indices) {
    map->keys_ = BaseVector::transpose(indices, std::move(map->keys_));
    map->values_ = BaseVector::transpose(indices, std::move(map->values_));
  }
  map->sortedKeys_ = true;
}

std::vector<vector_size_t> MapVector::sortedKeyIndices(
    vector_size_t index) const {
  std::vector<vector_size_t> indices(rawSizes_[index]);
  std::iota(indices.begin(), indices.end(), rawOffsets_[index]);
  if (!sortedKeys_) {
    keys_->sortIndices(indices, CompareFlags());
  }
  return indices;
}

BufferPtr MapVector::elementIndices() const {
  auto numElements = std::min<vector_size_t>(keys_->size(), values_->size());
  BufferPtr buffer =
      AlignedBuffer::allocate<vector_size_t>(numElements, BaseVector::pool_);
  auto data = buffer->asMutable<vector_size_t>();
  auto range = folly::Range(data, numElements);
  std::iota(range.begin(), range.end(), 0);
  return buffer;
}

std::string MapVector::toString(vector_size_t index) const {
  VELOX_CHECK_LT(index, length_, "Vector index should be less than length.");
  if (isNullAt(index)) {
    return "null";
  }
  return stringifyTruncatedElementList(
      rawOffsets_[index],
      rawSizes_[index],
      kMaxElementsInToString,
      ", ",
      [this](std::stringstream& ss, vector_size_t index) {
        ss << keys_->toString(index) << " => " << values_->toString(index);
      });
}

void MapVector::ensureWritable(const SelectivityVector& rows) {
  auto newSize = std::max<vector_size_t>(rows.end(), BaseVector::length_);
  if (offsets_ && !offsets_->unique()) {
    BufferPtr newOffsets =
        AlignedBuffer::allocate<vector_size_t>(newSize, BaseVector::pool_);
    auto rawNewOffsets = newOffsets->asMutable<vector_size_t>();

    // Copy the whole buffer. An alternative could be
    // (1) fill the buffer with zeros and copy over elements not in "rows";
    // (2) or copy over elements not in "rows" and mark "rows" elements as null
    // Leaving offsets or sizes of "rows" elements unspecified leaves the
    // vector in unusable state.
    memcpy(
        rawNewOffsets,
        rawOffsets_,
        byteSize<vector_size_t>(BaseVector::length_));

    offsets_ = std::move(newOffsets);
    rawOffsets_ = offsets_->as<vector_size_t>();
  }

  if (sizes_ && !sizes_->unique()) {
    BufferPtr newSizes =
        AlignedBuffer::allocate<vector_size_t>(newSize, BaseVector::pool_);
    auto rawNewSizes = newSizes->asMutable<vector_size_t>();
    memcpy(
        rawNewSizes, rawSizes_, byteSize<vector_size_t>(BaseVector::length_));

    sizes_ = std::move(newSizes);
    rawSizes_ = sizes_->as<vector_size_t>();
  }

  // Vectors are write-once and nested elements are append only,
  // hence, all values already written must be preserved.
  BaseVector::ensureWritable(
      SelectivityVector::empty(), type()->childAt(0), BaseVector::pool_, keys_);
  BaseVector::ensureWritable(
      SelectivityVector::empty(),
      type()->childAt(1),
      BaseVector::pool_,
      values_);
  BaseVector::ensureWritable(rows);
}

bool MapVector::isWritable() const {
  if (offsets_ && !offsets_->isMutable()) {
    return false;
  }

  if (sizes_ && !sizes_->isMutable()) {
    return false;
  }

  return isNullsWritable() && BaseVector::isVectorWritable(keys_) &&
      BaseVector::isVectorWritable(values_);
}

uint64_t MapVector::estimateFlatSize() const {
  return BaseVector::retainedSize() + offsets_->capacity() +
      sizes_->capacity() + keys_->estimateFlatSize() +
      values_->estimateFlatSize();
}

void MapVector::prepareForReuse() {
  BaseVector::prepareForReuse();

  if (!offsets_->isMutable()) {
    offsets_ = nullptr;
  } else {
    zeroOutBuffer(offsets_);
  }

  if (!sizes_->isMutable()) {
    sizes_ = nullptr;
  } else {
    zeroOutBuffer(sizes_);
  }

  BaseVector::prepareForReuse(keys_, 0);
  BaseVector::prepareForReuse(values_, 0);
}

VectorPtr MapVector::slice(vector_size_t offset, vector_size_t length) const {
  return std::make_shared<MapVector>(
      pool_,
      type_,
      sliceNulls(offset, length),
      length,
      sliceBuffer(*INTEGER(), offsets_, offset, length, pool_),
      sliceBuffer(*INTEGER(), sizes_, offset, length, pool_),
      keys_,
      values_);
}

void MapVector::validate(const VectorValidateOptions& options) const {
  ArrayVectorBase::validateArrayVectorBase(
      options, std::min(keys_->size(), values_->size()));
  keys_->validate(options);
  values_->validate(options);
}

void MapVector::copyRanges(
    const BaseVector* source,
    const folly::Range<const CopyRange*>& ranges) {
  copyRangesImpl(source, ranges, &values_, &keys_);
}

namespace {

struct UpdateSource {
  vector_size_t entryIndex;
  int8_t sourceIndex;
};

template <typename T>
class UpdateMapRow {
 public:
  void insert(
      const DecodedVector* decoded,
      vector_size_t entryIndex,
      int8_t sourceIndex) {
    values_[decoded->valueAt<T>(entryIndex)] = {entryIndex, sourceIndex};
  }

  template <typename F>
  void forEachEntry(F&& func) {
    for (auto& [_, source] : values_) {
      func(source);
    }
  }

  void clear() {
    values_.clear();
  }

 private:
  folly::F14FastMap<T, UpdateSource> values_;
};

template <>
class UpdateMapRow<void> {
 public:
  void insert(
      const DecodedVector* decoded,
      vector_size_t entryIndex,
      int8_t sourceIndex) {
    references_[{decoded->base(), decoded->index(entryIndex)}] = {
        entryIndex, sourceIndex};
  }

  template <typename F>
  void forEachEntry(F&& func) {
    for (auto& [_, source] : references_) {
      func(source);
    }
  }

  void clear() {
    references_.clear();
  }

 private:
  struct Reference {
    const BaseVector* base;
    vector_size_t index;

    bool operator==(const Reference& other) const {
      return base->equalValueAt(other.base, index, other.index);
    }
  };

  struct ReferenceHasher {
    uint64_t operator()(const Reference& key) const {
      return key.base->hashValueAt(key.index);
    }
  };

  folly::F14FastMap<Reference, UpdateSource, ReferenceHasher> references_;
};

} // namespace

template <TypeKind kKeyTypeKind>
MapVectorPtr MapVector::updateImpl(
    const std::vector<MapVectorPtr>& others) const {
  auto newNulls = nulls();
  bool allocatedNewNulls = false;
  for (auto& other : others) {
    if (!other->nulls()) {
      continue;
    }
    if (!newNulls) {
      newNulls = other->nulls();
      continue;
    }
    if (!allocatedNewNulls) {
      auto* prevNewNulls = newNulls->as<uint64_t>();
      newNulls = allocateNulls(size(), pool());
      allocatedNewNulls = true;
      bits::andBits(
          newNulls->asMutable<uint64_t>(),
          prevNewNulls,
          other->rawNulls(),
          0,
          size());
    } else {
      bits::andBits(
          newNulls->asMutable<uint64_t>(), other->rawNulls(), 0, size());
    }
  }

  auto newOffsets = allocateIndices(size(), pool());
  auto* rawNewOffsets = newOffsets->asMutable<vector_size_t>();
  auto newSizes = allocateIndices(size(), pool());
  auto* rawNewSizes = newSizes->asMutable<vector_size_t>();

  std::vector<DecodedVector> keys;
  keys.reserve(1 + others.size());
  keys.emplace_back(*keys_);
  for (auto& other : others) {
    VELOX_CHECK(*keys_->type() == *other->keys_->type());
    keys.emplace_back(*other->keys_);
  }
  std::vector<std::vector<BaseVector::CopyRange>> ranges(1 + others.size());

  // Subscript symbols in this function:
  //
  // i : Top level row index.
  // j, jj : Key/value vector index.  `jj' is the offset version of `j'.
  // k : Index into `others' and `ranges' for choosing a map vector.
  UpdateMapRow<typename TypeTraits<kKeyTypeKind>::NativeType> mapRow;
  vector_size_t numEntries = 0;
  for (vector_size_t i = 0; i < size(); ++i) {
    rawNewOffsets[i] = numEntries;
    if (newNulls && bits::isBitNull(newNulls->as<uint64_t>(), i)) {
      rawNewSizes[i] = 0;
      continue;
    }
    bool needUpdate = false;
    for (auto& other : others) {
      if (other->sizeAt(i) > 0) {
        needUpdate = true;
        break;
      }
    }
    if (!needUpdate) {
      // Fast path for no update on current row.
      rawNewSizes[i] = sizeAt(i);
      if (sizeAt(i) > 0) {
        ranges[0].push_back({offsetAt(i), numEntries, sizeAt(i)});
        numEntries += sizeAt(i);
      }
      continue;
    }
    for (int k = 0; k < keys.size(); ++k) {
      auto* vector = k == 0 ? this : others[k - 1].get();
      auto offset = vector->offsetAt(i);
      auto size = vector->sizeAt(i);
      for (vector_size_t j = 0; j < size; ++j) {
        auto jj = offset + j;
        VELOX_DCHECK(!keys[k].isNullAt(jj));
        mapRow.insert(&keys[k], jj, k);
      }
    }
    vector_size_t newSize = 0;
    mapRow.forEachEntry([&](UpdateSource source) {
      ranges[source.sourceIndex].push_back(
          {source.entryIndex, numEntries + newSize, 1});
      ++newSize;
    });
    mapRow.clear();
    rawNewSizes[i] = newSize;
    numEntries += newSize;
  }

  auto newKeys = BaseVector::create(mapKeys()->type(), numEntries, pool());
  auto newValues = BaseVector::create(mapValues()->type(), numEntries, pool());
  for (int k = 0; k < ranges.size(); ++k) {
    auto* vector = k == 0 ? this : others[k - 1].get();
    newKeys->copyRanges(vector->mapKeys().get(), ranges[k]);
    newValues->copyRanges(vector->mapValues().get(), ranges[k]);
  }

  return std::make_shared<MapVector>(
      pool(),
      type(),
      std::move(newNulls),
      size(),
      std::move(newOffsets),
      std::move(newSizes),
      std::move(newKeys),
      std::move(newValues));
}

MapVectorPtr MapVector::update(const std::vector<MapVectorPtr>& others) const {
  VELOX_CHECK(!others.empty());
  VELOX_CHECK(others.size() < std::numeric_limits<int8_t>::max());
  for (auto& other : others) {
    VELOX_CHECK_EQ(size(), other->size());
  }
  return VELOX_DYNAMIC_TYPE_DISPATCH(updateImpl, keys_->typeKind(), others);
}

void RowVector::appendNulls(vector_size_t numberOfRows) {
  VELOX_CHECK_GE(numberOfRows, 0);
  if (numberOfRows == 0) {
    return;
  }
  const vector_size_t newSize = numberOfRows + BaseVector::length_;
  const vector_size_t oldSize = BaseVector::length_;
  BaseVector::resize(newSize, false);
  bits::fillBits(mutableRawNulls(), oldSize, newSize, bits::kNull);
}

} // namespace velox
} // namespace facebook
