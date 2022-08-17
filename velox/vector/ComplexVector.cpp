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
  VELOX_CHECK(type->isRow());
  return std::static_pointer_cast<RowVector>(BaseVector::create(type, 0, pool));
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
  for (int32_t i = 0; i < compareSize; ++i) {
    BaseVector* child = children_[i].get();
    BaseVector* otherChild = otherRow->childAt(i)->loadedVector();
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
    auto result = child->compare(otherChild, index, wrappedOtherIndex, flags);
    if (flags.stopAtNull && !result.has_value()) {
      return std::nullopt;
    }

    if (result.value()) {
      return result;
    }
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
  for (auto i = 0; i < children_.size(); ++i) {
    BaseVector::ensureWritable(
        rows, type()->asRow().childAt(i), pool(), &children_[i]);
  }

  // Copy non-null values.
  SelectivityVector nonNullRows = rows;

  SelectivityVector allRows(source->size());
  DecodedVector decodedSource(*source, allRows);
  if (decodedSource.isIdentityMapping()) {
    if (source->mayHaveNulls()) {
      auto rawNulls = source->rawNulls();
      rows.applyToSelected([&](auto row) {
        auto idx = toSourceRow ? toSourceRow[row] : row;
        if (bits::isBitNull(rawNulls, idx)) {
          nonNullRows.setValid(row, false);
        }
      });
      nonNullRows.updateBounds();
    }

    auto rowSource = source->loadedVector()->as<RowVector>();
    for (auto i = 0; i < childrenSize_; ++i) {
      children_[i]->copy(
          rowSource->childAt(i)->loadedVector(), nonNullRows, toSourceRow);
    }
  } else {
    auto nulls = decodedSource.nulls();

    if (nulls) {
      rows.applyToSelected([&](auto row) {
        auto idx = toSourceRow ? toSourceRow[row] : row;
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
      mappedIndices =
          AlignedBuffer::allocate<vector_size_t>(rows.size(), pool_);
      rawMappedIndices = mappedIndices->asMutable<vector_size_t>();
      nonNullRows.applyToSelected(
          [&](auto row) { rawMappedIndices[row] = indices[toSourceRow[row]]; });
    }

    auto baseSource = decodedSource.base()->as<RowVector>();
    for (auto i = 0; i < childrenSize_; ++i) {
      children_[i]->copy(
          baseSource->childAt(i)->loadedVector(),
          nonNullRows,
          rawMappedIndices ? rawMappedIndices : indices);
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
          rows, children_[i]->type(), BaseVector::pool_, &children_[i]);
    }
  }
  BaseVector::ensureWritable(rows);
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
}

VectorPtr RowVector::slice(vector_size_t offset, vector_size_t length) const {
  std::vector<VectorPtr> children(children_.size());
  for (int i = 0; i < children_.size(); ++i) {
    children[i] = children_[i]->slice(offset, length);
  }
  return std::make_shared<RowVector>(
      pool_, type_, sliceNulls(offset, length), length, std::move(children));
}

namespace {
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
  for (auto i = 0; i < compareSize; ++i) {
    auto result =
        left.compare(&right, leftRange.begin + i, rightRange.begin + i, flags);
    if (flags.stopAtNull && !result.has_value()) {
      // Null is encountered.
      return std::nullopt;
    }
    if (result.value() != 0) {
      return result;
    }
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
  for (auto i = 0; i < compareSize; ++i) {
    auto result = left.compare(&right, leftRange[i], rightRange[i], flags);
    if (flags.stopAtNull && !result.has_value()) {
      // Null is encountered.
      return std::nullopt;
    }
    if (result.value() != 0) {
      return result;
    }
  }
  int result = leftRange.size() - rightRange.size();
  return flags.ascending ? result : result * -1;
}
} // namespace

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

void ArrayVector::copy(
    const BaseVector* source,
    vector_size_t targetIndex,
    vector_size_t sourceIndex,
    vector_size_t count) {
  auto sourceValue = source->wrappedVector();
  if (sourceValue->isConstantEncoding()) {
    // A null constant does not have a value vector, so wrappedVector
    // returns the constant.
    VELOX_CHECK(sourceValue->isNullAt(0));
    for (auto i = 0; i < count; ++i) {
      setNull(targetIndex + i, true);
    }
    return;
  }
  VELOX_CHECK_EQ(sourceValue->encoding(), VectorEncoding::Simple::ARRAY);
  auto sourceArray = sourceValue->asUnchecked<ArrayVector>();
  VELOX_DCHECK(BaseVector::length_ >= targetIndex + count);
  BaseVector::ensureWritable(
      SelectivityVector::empty(), elements_->type(), pool(), &elements_);
  auto setNotNulls = mayHaveNulls() || source->mayHaveNulls();
  auto wantWidth = type()->isFixedWidth() ? type()->fixedElementsWidth() : 0;
  auto* mutableOffsets = offsets_->asMutable<vector_size_t>();
  auto* mutableSizes = sizes_->asMutable<vector_size_t>();
  vector_size_t childSize = elements_->size();
  if (count == 1) {
    // Fast path if we're just copying a single array.
    if (source->isNullAt(sourceIndex)) {
      setNull(targetIndex, true);
    } else {
      if (setNotNulls) {
        setNull(targetIndex, false);
      }

      vector_size_t wrappedIndex = source->wrappedIndex(sourceIndex);
      vector_size_t copySize = sourceArray->sizeAt(wrappedIndex);

      mutableOffsets[targetIndex] = childSize;
      mutableSizes[targetIndex] = copySize;

      if (copySize > 0) {
        // If we are populating a FixedSizeArray we validate here that
        // the entries we are populating are the correct sizes.
        if (wantWidth != 0) {
          VELOX_CHECK_EQ(
              copySize,
              wantWidth,
              "Invalid length element at wrappedIndex {}",
              wrappedIndex);
        }
        auto copyOffset = sourceArray->offsetAt(wrappedIndex);
        elements_->resize(childSize + copySize);

        elements_->copy(
            sourceArray->elements_.get(), childSize, copyOffset, copySize);
      }
    }
  } else {
    std::vector<std::pair<vector_size_t, vector_size_t>> ranges;
    ranges.reserve(count);
    for (int32_t i = 0; i < count; ++i) {
      if (source->isNullAt(sourceIndex + i)) {
        setNull(targetIndex + i, true);
      } else {
        if (setNotNulls) {
          setNull(targetIndex + i, false);
        }
        vector_size_t wrappedIndex = source->wrappedIndex(sourceIndex + i);
        vector_size_t copySize = sourceArray->sizeAt(wrappedIndex);

        if (copySize > 0) {
          // If we are populating a FixedSizeArray we validate here that
          // the entries we are populating are the correct sizes.
          if (wantWidth != 0) {
            VELOX_CHECK_EQ(
                copySize,
                wantWidth,
                "Invalid length element at index {}, wrappedIndex {}",
                i,
                wrappedIndex);
          }

          auto copyOffset = sourceArray->offsetAt(wrappedIndex);

          // If we're copying two adjacent ranges, merge them.
          // This only works if they're consecutive.
          if (!ranges.empty() &&
              (ranges.back().first + ranges.back().second == copyOffset)) {
            ranges.back().second += copySize;
          } else {
            ranges.emplace_back(copyOffset, copySize);
          }
        }

        mutableOffsets[targetIndex + i] = childSize;
        mutableSizes[targetIndex + i] = copySize;
        childSize += copySize;
      }
    }

    vector_size_t childOffset = elements_->size();
    elements_->resize(childSize);

    for (auto& range : ranges) {
      elements_->copy(
          sourceArray->elements_.get(), childOffset, range.first, range.second);
      childOffset += range.second;
    }
  }
}

std::string ArrayVector::toString(vector_size_t index) const {
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
  auto newSize = std::max<vector_size_t>(rows.size(), BaseVector::length_);
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
      &elements_);
  BaseVector::ensureWritable(rows);
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

  if (!(offsets_->unique() && offsets_->isMutable())) {
    offsets_ = nullptr;
  } else {
    zeroOutBuffer(offsets_);
  }

  if (!(sizes_->unique() && sizes_->isMutable())) {
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

std::optional<int32_t> MapVector::compare(
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
  VELOX_DCHECK(result.has_value(), "keys can not have null");

  if (flags.stopAtNull && !result.has_value()) {
    return std::nullopt;
  }

  // Keys are not the same.
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

vector_size_t MapVector::reserveMap(vector_size_t offset, vector_size_t size) {
  auto keySize = keys_->size();
  keys_->resize(keySize + size);
  values_->resize(keySize + size);
  offsets_->asMutable<vector_size_t>()[offset] = keySize;
  sizes_->asMutable<vector_size_t>()[offset] = size;
  return keySize;
}

void MapVector::copy(
    const BaseVector* source,
    vector_size_t targetIndex,
    vector_size_t sourceIndex,
    vector_size_t count) {
  auto sourceValue = source->wrappedVector();
  if (sourceValue->isConstantEncoding()) {
    // A null constant does not have a value vector, so wrappedVector
    // returns the constant.
    VELOX_CHECK(sourceValue->isNullAt(0));
    for (auto i = 0; i < count; ++i) {
      setNull(targetIndex + i, true);
    }
    return;
  }
  VELOX_CHECK_EQ(sourceValue->encoding(), VectorEncoding::Simple::MAP);
  VELOX_DCHECK(BaseVector::length_ >= targetIndex + count);
  auto sourceMap = sourceValue->asUnchecked<MapVector>();
  BaseVector::ensureWritable(
      SelectivityVector::empty(), keys_->type(), pool(), &keys_);
  auto setNotNulls = mayHaveNulls() || source->mayHaveNulls();
  auto* mutableOffsets = offsets_->asMutable<vector_size_t>();
  auto* mutableSizes = sizes_->asMutable<vector_size_t>();
  vector_size_t childSize = keys_->size();
  if (count == 1) {
    // Fast path if we're just copying a single map.
    if (source->isNullAt(sourceIndex)) {
      setNull(targetIndex, true);
    } else {
      if (setNotNulls) {
        setNull(targetIndex, false);
      }

      vector_size_t wrappedIndex = source->wrappedIndex(sourceIndex);
      vector_size_t copySize = sourceMap->sizeAt(wrappedIndex);

      mutableOffsets[targetIndex] = childSize;
      mutableSizes[targetIndex] = copySize;

      if (copySize > 0) {
        auto copyOffset = sourceMap->offsetAt(wrappedIndex);
        keys_->resize(childSize + copySize);
        values_->resize(childSize + copySize);

        keys_->copy(sourceMap->keys_.get(), childSize, copyOffset, copySize);
        values_->copy(
            sourceMap->values_.get(), childSize, copyOffset, copySize);
      }
    }
  } else {
    std::vector<std::pair<vector_size_t, vector_size_t>> ranges;
    ranges.reserve(count);
    for (int32_t i = 0; i < count; ++i) {
      if (source->isNullAt(sourceIndex + i)) {
        setNull(targetIndex + i, true);
      } else {
        if (setNotNulls) {
          setNull(targetIndex + i, false);
        }
        vector_size_t wrappedIndex = source->wrappedIndex(sourceIndex + i);
        vector_size_t copySize = sourceMap->sizeAt(wrappedIndex);

        if (copySize > 0) {
          auto copyOffset = sourceMap->offsetAt(wrappedIndex);

          // If we're copying two adjacent ranges, merge them.
          // This only works if they're consecutive.
          if (!ranges.empty() &&
              (ranges.back().first + ranges.back().second == copyOffset)) {
            ranges.back().second += copySize;
          } else {
            ranges.emplace_back(copyOffset, copySize);
          }
        }

        mutableOffsets[targetIndex + i] = childSize;
        mutableSizes[targetIndex + i] = copySize;
        childSize += copySize;
      }
    }

    vector_size_t childOffset = keys_->size();
    keys_->resize(childSize);
    values_->resize(childSize);

    for (auto& range : ranges) {
      keys_->copy(
          sourceMap->keys_.get(), childOffset, range.first, range.second);
      values_->copy(
          sourceMap->values_.get(), childOffset, range.first, range.second);
      childOffset += range.second;
    }
  }
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
  folly::Range<vector_size_t*> indicesRange;
  for (auto i = 0; i < map->BaseVector::length_; ++i) {
    if (map->isSorted(i)) {
      continue;
    }
    if (!indices) {
      indices = map->elementIndices();
      indicesRange = folly::Range<vector_size_t*>(
          indices->asMutable<vector_size_t>(), map->keys_->size());
    }
    auto offset = map->rawOffsets_[i];
    auto size = map->rawSizes_[i];
    if (useStableSort) {
      std::stable_sort(
          indicesRange.begin() + offset,
          indicesRange.begin() + offset + size,
          [&](vector_size_t left, vector_size_t right) {
            return map->keys_->compare(map->keys_.get(), left, right) < 0;
          });
    } else {
      std::sort(
          indicesRange.begin() + offset,
          indicesRange.begin() + offset + size,
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
    std::sort(
        indices.begin(),
        indices.end(),
        [&](vector_size_t left, vector_size_t right) {
          return keys_->compare(keys_.get(), left, right) < 0;
        });
  }
  return indices;
}

BufferPtr MapVector::elementIndices() const {
  auto numElements = keys_->size();
  BufferPtr buffer =
      AlignedBuffer::allocate<vector_size_t>(numElements, BaseVector::pool_);
  auto data = buffer->asMutable<vector_size_t>();
  auto range = folly::Range(data, numElements);
  std::iota(range.begin(), range.end(), 0);
  return buffer;
}

std::string MapVector::toString(vector_size_t index) const {
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
  auto newSize = std::max<vector_size_t>(rows.size(), BaseVector::length_);
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
      SelectivityVector::empty(),
      type()->childAt(0),
      BaseVector::pool_,
      &keys_);
  BaseVector::ensureWritable(
      SelectivityVector::empty(),
      type()->childAt(1),
      BaseVector::pool_,
      &values_);
  BaseVector::ensureWritable(rows);
}

uint64_t MapVector::estimateFlatSize() const {
  return BaseVector::retainedSize() + offsets_->capacity() +
      sizes_->capacity() + keys_->estimateFlatSize() +
      values_->estimateFlatSize();
}

void MapVector::prepareForReuse() {
  BaseVector::prepareForReuse();

  if (!(offsets_->unique() && offsets_->isMutable())) {
    offsets_ = nullptr;
  } else {
    zeroOutBuffer(offsets_);
  }

  if (!(sizes_->unique() && sizes_->isMutable())) {
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

} // namespace velox
} // namespace facebook
