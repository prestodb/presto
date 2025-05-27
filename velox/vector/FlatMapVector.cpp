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

#include "velox/vector/FlatMapVector.h"
#include <folly/hash/Hash.h>
#include "velox/vector/FlatVector.h"

namespace facebook::velox {
namespace {

// Up to # of elements to show as debug string for `toString()`.
constexpr vector_size_t kToStringMaxFlatMapElements = 5;
constexpr std::string_view kToStringDelimiter{", "};

template <typename T, typename TMap>
std::optional<column_index_t> getKeyChannelImpl(
    const VectorPtr& distinctKeys,
    const TMap& keyToChannel,
    T keyValue) {
  if (distinctKeys == nullptr) {
    return std::nullopt;
  }

  auto distinctFlatKeys = distinctKeys->as<FlatVector<T>>();
  VELOX_CHECK(
      distinctFlatKeys != nullptr,
      "Incompatible vector type for flat map vector keys: {}",
      distinctKeys->toString());

  uint64_t hash = folly::hasher<T>{}(keyValue);
  auto range = keyToChannel.equal_range(hash);

  // Key hash wasn't found on the map.
  if (range.first == range.second) {
    return std::nullopt;
  }

  // Here there was at least one hash match. Need to compare to the keys vector
  // to ensure it's an actual match and not a hash collision.
  for (auto it = range.first; it != range.second; ++it) {
    if (distinctFlatKeys->valueAtFast(it->second) == keyValue) {
      return it->second;
    }
  }
  return std::nullopt;
}

} // namespace

std::optional<column_index_t> FlatMapVector::getKeyChannel(
    int32_t scalarValue) const {
  return getKeyChannelImpl(distinctKeys_, keyToChannel_, scalarValue);
}

std::optional<column_index_t> FlatMapVector::getKeyChannel(
    int64_t scalarValue) const {
  return getKeyChannelImpl(distinctKeys_, keyToChannel_, scalarValue);
}

std::optional<column_index_t> FlatMapVector::getKeyChannel(
    StringView scalarValue) const {
  return getKeyChannelImpl(distinctKeys_, keyToChannel_, scalarValue);
}

std::optional<column_index_t> FlatMapVector::getKeyChannel(
    const VectorPtr& keysVector,
    vector_size_t index) const {
  uint64_t hash = keysVector->hashValueAt(index);
  auto range = keyToChannel_.equal_range(hash);

  // Key hash wasn't found on the map.
  if (range.first == range.second) {
    return std::nullopt;
  }

  for (auto it = range.first; it != range.second; ++it) {
    if (keysVector->equalValueAt(distinctKeys_.get(), index, it->second)) {
      return it->second;
    }
  }
  return std::nullopt;
}

vector_size_t FlatMapVector::sizeAt(vector_size_t index) const {
  VELOX_CHECK_LT(index, size());
  vector_size_t size = 0;

  for (vector_size_t i = 0; i < numDistinctKeys(); i++) {
    if (i < inMaps_.size() && inMaps_[i] != nullptr) {
      size += bits::isBitSet(inMaps_[i]->asMutable<uint64_t>(), index);
    } else {
      // By default assume the key exists.
      ++size;
    }
  }
  return size;
}

void FlatMapVector::resize(vector_size_t newSize, bool setNotNull) {
  const auto oldSize = size();
  BaseVector::resize(newSize, setNotNull);

  // Resize all the map values vectors.
  for (vector_size_t i = 0; i < numDistinctKeys(); i++) {
    auto& values = mapValues_[i];
    if (values != nullptr) {
      VELOX_CHECK(!values->isLazy(), "Resize on a lazy vector is not allowed.");

      // If we are just reducing the size of the vector, its safe
      // to skip uniqueness check since effectively we are just changing
      // the length.
      if (newSize > oldSize) {
        VELOX_CHECK_EQ(
            values.use_count(), 1, "Resizing shared map values vector");
        values->resize(newSize, setNotNull);

        if (i < inMaps_.size() && inMaps_[i] != nullptr) {
          VELOX_CHECK(inMaps_[i]->unique(), "Resizing shared in map vector");
          AlignedBuffer::reallocate<bool>(&inMaps_[i], newSize, 0);
        }
      }
    }
  }
}

VectorPtr FlatMapVector::slice(vector_size_t offset, vector_size_t length)
    const {
  std::vector<VectorPtr> mapValues(mapValues_.size());
  for (int i = 0; i < mapValues_.size(); ++i) {
    mapValues[i] = mapValues_[i]->slice(offset, length);
  }

  std::vector<BufferPtr> inMaps(inMaps_.size());
  for (int i = 0; i < inMaps_.size(); ++i) {
    if (inMaps_[i]) {
      inMaps[i] = Buffer::slice<bool>(inMaps_[i], offset, length, pool_);
    }
  }

  return std::make_shared<FlatMapVector>(
      pool_,
      type_,
      sliceNulls(offset, length),
      length,
      distinctKeys_,
      std::move(mapValues),
      std::move(inMaps),
      std::nullopt,
      sortedKeys_);
}

VectorPtr FlatMapVector::testingCopyPreserveEncodings(
    velox::memory::MemoryPool* pool) const {
  std::vector<VectorPtr> copiedMapValues(mapValues_.size());
  std::vector<BufferPtr> copiedInMaps(inMaps_.size());
  std::transform(
      mapValues_.begin(),
      mapValues_.end(),
      copiedMapValues.begin(),
      [&pool](const auto& mapValue) {
        return mapValue->testingCopyPreserveEncodings(pool);
      });

  auto selfPool = pool ? pool : pool_;

  for (auto i = 0; i < inMaps_.size(); ++i) {
    copiedInMaps[i] = AlignedBuffer::copy(selfPool, inMaps_[i]);
  }

  return std::make_shared<FlatMapVector>(
      selfPool,
      type_,
      AlignedBuffer::copy(selfPool, nulls_),
      length_,
      distinctKeys_->testingCopyPreserveEncodings(pool),
      std::move(copiedMapValues),
      std::move(copiedInMaps),
      nullCount_,
      sortedKeys_);
}

std::string FlatMapVector::toString(vector_size_t index) const {
  VELOX_CHECK_LT(index, length_, "Vector index should be less than length.");
  if (isNullAt(index)) {
    return "null";
  }

  vector_size_t size = sizeAt(index);

  if (size == 0) {
    return "<empty>";
  }

  std::stringstream out;
  out << size << " elements {";
  const vector_size_t limitedSize = std::min(size, kToStringMaxFlatMapElements);
  vector_size_t printedElements = 0;

  for (vector_size_t i = 0;
       i < numDistinctKeys() && printedElements < kToStringMaxFlatMapElements;
       ++i) {
    if (!isInMap(i, index)) {
      continue;
    }

    if (printedElements > 0) {
      out << kToStringDelimiter;
    }

    out << distinctKeys_->toString(i) << " => "
        << mapValues_[i]->toString(index);
    ++printedElements;
  }

  if (size > limitedSize) {
    if (limitedSize) {
      out << kToStringDelimiter;
    }
    out << "...";
  }
  out << "}";
  return out.str();
}

bool FlatMapVector::containsNullAt(vector_size_t index) const {
  if (BaseVector::isNullAt(index)) {
    return true;
  }

  // If the key/value pair exists in the map, return true if either the key or
  // value are nulls (mirroring the MapVector behavior).
  for (vector_size_t i = 0; i < numDistinctKeys(); i++) {
    if (isInMap(i, index)) {
      if (distinctKeys_->containsNullAt(i)) {
        return true;
      }

      if (mapValues_[i]->containsNullAt(i)) {
        return true;
      }
    }
  }
  return false;
}

uint64_t FlatMapVector::hashValueAt(vector_size_t index) const {
  if (isNullAt(index)) {
    return BaseVector::kNullHash;
  }

  uint64_t hash = BaseVector::kNullHash;

  for (vector_size_t i = 0; i < numDistinctKeys(); i++) {
    if (isInMap(i, index)) {
      hash = bits::commutativeHashMix(hash, distinctKeys_->hashValueAt(i));
      hash = bits::commutativeHashMix(hash, mapValues_[i]->hashValueAt(index));
    }
  }
  return hash;
}

std::unique_ptr<SimpleVector<uint64_t>> FlatMapVector::hashAll() const {
  // Method not implemented for any complex vectors in ComplexVectors.h also.
  VELOX_NYI();
}

std::vector<vector_size_t> FlatMapVector::sortedKeyIndices(
    vector_size_t index) const {
  std::vector<vector_size_t> indices;
  indices.reserve(numDistinctKeys());

  for (vector_size_t i = 0; i < numDistinctKeys(); i++) {
    if (isInMap(i, index)) {
      indices.push_back(i);
    }
  }

  if (!sortedKeys_ && distinctKeys_) {
    distinctKeys_->sortIndices(indices, CompareFlags());
  }
  return indices;
}

// This function's logic is largely based on MapVector::compare().
std::optional<int32_t> FlatMapVector::compare(
    const BaseVector* other,
    vector_size_t index,
    vector_size_t otherIndex,
    CompareFlags flags) const {
  VELOX_CHECK(
      flags.nullAsValue() || flags.equalsOnly, "Map is not orderable type");

  // If maps are null.
  bool isNull = isNullAt(index);
  bool otherNull = other->isNullAt(otherIndex);
  if (isNull || otherNull) {
    return BaseVector::compareNulls(isNull, otherNull, flags);
  }

  // Validate we have compatible map types for comparison.
  auto otherValue = other->wrappedVector();
  auto wrappedOtherIndex = other->wrappedIndex(otherIndex);
  VELOX_CHECK_EQ(
      VectorEncoding::Simple::FLAT_MAP,
      otherValue->encoding(),
      "Compare of FLAT_MAP and non-FLAT_MAP: {} and {}",
      BaseVector::toString(),
      otherValue->BaseVector::toString());
  auto otherFlatMap = otherValue->as<FlatMapVector>();

  if (keyType()->kind() != otherFlatMap->keyType()->kind() ||
      valueType()->kind() != otherFlatMap->valueType()->kind()) {
    VELOX_FAIL(
        "Compare of maps of different key/value types: {} and {}",
        BaseVector::toString(),
        otherFlatMap->BaseVector::toString());
  }

  // We first get sorted key indices for both maps.
  auto leftIndices = sortedKeyIndices(index);
  auto rightIndices = otherFlatMap->sortedKeyIndices(wrappedOtherIndex);

  // If equalsOnly and maps have different sizes, we can bail fast.
  if (flags.equalsOnly && leftIndices.size() != rightIndices.size()) {
    int result = leftIndices.size() - rightIndices.size();
    return flags.ascending ? result : result * -1;
  }

  // Compare each key value pair, using the sorted key order.
  auto compareSize = std::min(leftIndices.size(), rightIndices.size());
  bool resultIsIndeterminate = false;

  for (auto i = 0; i < compareSize; ++i) {
    // First compare the keys.
    auto result = distinctKeys_->compare(
        otherFlatMap->distinctKeys_.get(),
        leftIndices[i],
        rightIndices[i],
        flags);

    // Key mismatch; comparison can stop.
    if (result == kIndeterminate) {
      VELOX_DCHECK(
          flags.equalsOnly,
          "Compare should have thrown when null is encountered in child.");
      resultIsIndeterminate = true;
    } else if (result.value() != 0) {
      return result;
    }
    // If keys are same, compare values.
    else {
      auto valueResult = mapValues_[leftIndices[i]]->compare(
          otherFlatMap->mapValues_[rightIndices[i]].get(),
          index,
          wrappedOtherIndex,
          flags);

      // If value mismatch, comparison can also stop.
      if (valueResult == kIndeterminate) {
        VELOX_DCHECK(
            flags.equalsOnly,
            "Compare should have thrown when null is encountered in child.");
        resultIsIndeterminate = true;
      } else if (valueResult.value() != 0) {
        return valueResult;
      }
    }
  }

  if (resultIsIndeterminate) {
    return kIndeterminate;
  }

  // If one map was smaller than the other.
  int result = leftIndices.size() - rightIndices.size();
  return flags.ascending ? result : result * -1;
}

// This function will copy map value elements from the individual mapValues_
// std::vector into a single flattened one that can be used by a MapVector.
//
// It does so by copying map values in a columnar fashion, using a
// scatter-like operation. This prevents the lack of cache locality for reads in
// a gather-like operation.
MapVectorPtr FlatMapVector::toMapVector() const {
  // We first need to count how many key/value pairs per row. We do so by
  // couting how many "full" maps we have (when inMap buffers are not
  // materialized), then counting the ones that have inMap buffers.
  size_t partialKeyCount = 0;
  for (vector_size_t i = 0; i < numDistinctKeys(); i++) {
    if (i < inMaps_.size() && inMaps_[i] != nullptr) {
      ++partialKeyCount;
    }
  }
  size_t fullKeys = numDistinctKeys() - partialKeyCount;

  // Allocate the sizes vector with the minimum size of each record (the number
  // of `fullKeys`).
  auto sizes = AlignedBuffer::allocate<vector_size_t>(length_, pool_, fullKeys);
  auto* rawSizes = sizes->asMutable<vector_size_t>();

  // Then add the remaining ones.
  for (vector_size_t i = 0; i < inMaps_.size(); i++) {
    if (inMaps_[i] != nullptr) {
      bits::forEachBit(
          inMaps_[i]->as<uint64_t>(),
          0,
          length_,
          true,
          [rawSizes](int32_t idx) { ++rawSizes[idx]; });
    }
  }

  // At this point we have the accurate size for each record. Allocate and
  // populate the offsets buffer, and count how many elements the flattened
  // vectors will have (the total number of key/value pairs).
  auto offsets = allocateIndices(length_, pool_);
  auto* rawOffsets = offsets->asMutable<vector_size_t>();

  vector_size_t totalElements = 0;
  for (vector_size_t i = 0; i < length_; i++) {
    rawOffsets[i] = totalElements;
    totalElements += rawSizes[i];
  }

  // We now zero out the sizes buffer again. The sizes buffer will be used to
  // count how many elements have been added to each row, to offset the scatter
  // operation.
  std::fill(rawSizes, rawSizes + length_, 0);

  auto mapValues = BaseVector::create(valueType(), totalElements, pool_);

  // We also populate the indices that will wrap the distinct keys vector, to
  // increase its cardinality.
  auto keyDictIndices = allocateIndices(totalElements, pool_);
  auto* rawKeyDictIndices = keyDictIndices->asMutable<vector_size_t>();

  // The main scatter loop. Scan each map values vector, and copy the elements
  // to the right offsets in the flattened output.
  for (vector_size_t i = 0; i < mapValues_.size(); i++) {
    if (mapValues_[i] != nullptr) {
      for (vector_size_t j = 0; j < length_; j++) {
        // If needed, this check could be unrolled out of the loop (in many
        // cases the inMap buffer doesn't exist).
        if (isInMap(i, j)) {
          mapValues->copy(
              mapValues_[i].get(), rawOffsets[j] + rawSizes[j], j, 1);
          rawKeyDictIndices[rawOffsets[j] + rawSizes[j]] = i;
          ++rawSizes[j];
        }
      }
    }
  }

  auto mapKeys = distinctKeys_
      ? BaseVector::wrapInDictionary(
            nullptr, keyDictIndices, totalElements, distinctKeys_)
      : nullptr;
  return std::make_shared<MapVector>(
      pool_,
      type_,
      nulls_,
      length_,
      offsets,
      sizes,
      std::move(mapKeys),
      std::move(mapValues),
      nullCount_,
      sortedKeys_);
}

} // namespace facebook::velox
