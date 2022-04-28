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
#define XXH_INLINE_ALL

#include "velox/functions/prestosql/aggregates/PrestoHasher.h"
#include "velox/external/xxhash.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace facebook::velox::aggregate {

namespace {

template <typename T>
FOLLY_ALWAYS_INLINE int64_t hashInteger(const T& value) {
  return XXH64_round(0, value);
}

FOLLY_ALWAYS_INLINE int64_t
hashStringView(const DecodedVector& vector, vector_size_t row) {
  auto input = vector.valueAt<StringView>(row);
  return XXH64(input.data(), input.size(), 0);
}

template <typename Callable>
FOLLY_ALWAYS_INLINE void applyHashFunction(
    const SelectivityVector& rows,
    const DecodedVector& vector,
    BufferPtr& hashes,
    Callable func) {
  VELOX_CHECK_GE(hashes->size(), rows.end())
  auto rawHashes = hashes->asMutable<int64_t>();

  rows.applyToSelected([&](auto row) {
    if (vector.isNullAt(row)) {
      rawHashes[row] = 0;
    } else {
      rawHashes[row] = func(row);
    }
  });
}

template <typename T>
FOLLY_ALWAYS_INLINE void hashIntegral(
    const DecodedVector& vector,
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  applyHashFunction(rows, vector, hashes, [&](auto row) {
    return hashInteger<T>(vector.valueAt<T>(row));
  });
}

#if defined(__clang__)
__attribute__((no_sanitize("integer")))
#endif
FOLLY_ALWAYS_INLINE int64_t
safeHash(const int64_t& a, const int64_t& b) {
  return a * 31 + b;
}

#if defined(__clang__)
__attribute__((no_sanitize("signed-integer-overflow")))
#endif
FOLLY_ALWAYS_INLINE int64_t
safeXor(const int64_t& hash, const int64_t& a, const int64_t& b) {
  return hash + (a ^ b);
}

} // namespace

template <TypeKind kind>
FOLLY_ALWAYS_INLINE void PrestoHasher::hash(
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  using T = typename TypeTraits<kind>::NativeType;
  hashIntegral<T>(*vector_.get(), rows, hashes);
}

template <>
FOLLY_ALWAYS_INLINE void PrestoHasher::hash<TypeKind::BOOLEAN>(
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  applyHashFunction(rows, *vector_.get(), hashes, [&](auto row) {
    return vector_->valueAt<bool>(row) ? 1231 : 1237;
  });
}

template <>
FOLLY_ALWAYS_INLINE void PrestoHasher::hash<TypeKind::DATE>(
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  applyHashFunction(rows, *vector_.get(), hashes, [&](auto row) {
    return hashInteger(vector_->valueAt<Date>(row).days());
  });
}

template <>
FOLLY_ALWAYS_INLINE void PrestoHasher::hash<TypeKind::REAL>(
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  hashIntegral<int32_t>(*vector_.get(), rows, hashes);
}

template <>
FOLLY_ALWAYS_INLINE void PrestoHasher::hash<TypeKind::VARCHAR>(
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  applyHashFunction(rows, *vector_.get(), hashes, [&](auto row) {
    return hashStringView(*vector_.get(), row);
  });
}

template <>
FOLLY_ALWAYS_INLINE void PrestoHasher::hash<TypeKind::VARBINARY>(
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  applyHashFunction(rows, *vector_.get(), hashes, [&](auto row) {
    return hashStringView(*vector_.get(), row);
  });
}

template <>
FOLLY_ALWAYS_INLINE void PrestoHasher::hash<TypeKind::DOUBLE>(
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  hashIntegral<int64_t>(*vector_.get(), rows, hashes);
}

template <>
FOLLY_ALWAYS_INLINE void PrestoHasher::hash<TypeKind::TIMESTAMP>(
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  applyHashFunction(rows, *vector_.get(), hashes, [&](auto row) {
    return hashInteger((vector_->valueAt<Timestamp>(row)).toMillis());
  });
}

template <>
void PrestoHasher::hash<TypeKind::ARRAY>(
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  auto baseArray = vector_->base()->as<ArrayVector>();
  auto indices = vector_->indices();
  auto elementRows = functions::toElementRows(
      baseArray->elements()->size(), rows, baseArray, indices);

  BufferPtr elementHashes =
      AlignedBuffer::allocate<int64_t>(elementRows.end(), baseArray->pool());

  children_[0]->hash(baseArray->elements(), elementRows, elementHashes);

  auto rawSizes = baseArray->rawSizes();
  auto rawOffsets = baseArray->rawOffsets();
  auto rawNulls = baseArray->rawNulls();
  auto rawElementHashes = elementHashes->as<int64_t>();
  auto rawHashes = hashes->asMutable<int64_t>();

  rows.applyToSelected([&](auto row) {
    int64_t hash = 0;
    if (!(rawNulls && bits::isBitNull(rawNulls, indices[row]))) {
      auto size = rawSizes[indices[row]];
      auto offset = rawOffsets[indices[row]];

      for (int i = 0; i < size; i++) {
        hash = safeHash(hash, rawElementHashes[offset + i]);
      }
    }
    rawHashes[row] = hash;
  });
}

template <>
void PrestoHasher::hash<TypeKind::MAP>(
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  auto baseMap = vector_->base()->as<MapVector>();
  auto indices = vector_->indices();
  VELOX_CHECK_EQ(children_.size(), 2)

  auto elementRows = functions::toElementRows(
      baseMap->mapKeys()->size(), rows, baseMap, indices);
  BufferPtr keyHashes =
      AlignedBuffer::allocate<int64_t>(elementRows.end(), baseMap->pool());

  BufferPtr valueHashes =
      AlignedBuffer::allocate<int64_t>(elementRows.end(), baseMap->pool());

  children_[0]->hash(baseMap->mapKeys(), elementRows, keyHashes);
  children_[1]->hash(baseMap->mapValues(), elementRows, valueHashes);

  auto rawKeyHashes = keyHashes->as<int64_t>();
  auto rawValueHashes = valueHashes->as<int64_t>();
  auto rawHashes = hashes->asMutable<int64_t>();

  auto rawSizes = baseMap->rawSizes();
  auto rawOffsets = baseMap->rawOffsets();
  auto rawNulls = baseMap->rawNulls();

  rows.applyToSelected([&](auto row) {
    int64_t hash = 0;
    if (!(rawNulls && bits::isBitNull(rawNulls, indices[row]))) {
      auto size = rawSizes[indices[row]];
      auto offset = rawOffsets[indices[row]];

      for (int i = 0; i < size; i++) {
        hash =
            safeXor(hash, rawKeyHashes[offset + i], rawValueHashes[offset + i]);
      }
    }
    rawHashes[row] = hash;
  });
}

template <>
void PrestoHasher::hash<TypeKind::ROW>(
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  auto baseRow = vector_->base()->as<RowVector>();
  auto indices = vector_->indices();
  SelectivityVector elementRows;

  if (vector_->isIdentityMapping()) {
    elementRows = rows;
  } else {
    elementRows = SelectivityVector(baseRow->size(), false);
    rows.applyToSelected(
        [&](auto row) { elementRows.setValid(indices[row], true); });
    elementRows.updateBounds();
  }

  BufferPtr childHashes =
      AlignedBuffer::allocate<int64_t>(elementRows.end(), baseRow->pool());

  auto rawHashes = hashes->asMutable<int64_t>();
  auto rowChildHashes = childHashes->as<int64_t>();

  if (isTimestampWithTimeZoneType(vector_->base()->type())) {
    // Hash only timestamp value.
    children_[0]->hash(baseRow->childAt(0), elementRows, childHashes);
    rows.applyToSelected([&](auto row) {
      if (!baseRow->isNullAt(indices[row])) {
        rawHashes[row] = rowChildHashes[indices[row]];
      } else {
        rawHashes[row] = 0;
      }
    });
    return;
  }

  std::fill_n(rawHashes, rows.end(), 1);

  for (int i = 0; i < baseRow->childrenSize(); i++) {
    children_[i]->hash(baseRow->childAt(i), elementRows, childHashes);

    rows.applyToSelected([&](auto row) {
      rawHashes[row] = safeHash(rawHashes[row], rowChildHashes[indices[row]]);
    });
  }
}

void PrestoHasher::hash(
    const VectorPtr& vector,
    const SelectivityVector& rows,
    BufferPtr& hashes) {
  VELOX_CHECK(
      *vector->type() == *type_,
      "Vector type: {} != initialized type: {}",
      vector->type()->toString(),
      type_->toString())
  vector_->decode(*vector, rows);
  auto kind = vector_->base()->typeKind();
  VELOX_DYNAMIC_TYPE_DISPATCH(hash, kind, rows, hashes);
}

void PrestoHasher::createChildren() {
  auto kind = type_->kind();
  if (kind == TypeKind::ARRAY) {
    children_.push_back(std::make_unique<PrestoHasher>(type_->childAt(0)));
  } else if (kind == TypeKind::MAP) {
    // Decode key
    children_.push_back(std::make_unique<PrestoHasher>(type_->childAt(0)));
    // Decode values
    children_.push_back(std::make_unique<PrestoHasher>(type_->childAt(1)));

  } else if (kind == TypeKind::ROW) {
    children_.reserve(type_->size());
    for (int i = 0; i < type_->size(); i++) {
      children_.push_back(std::make_unique<PrestoHasher>(type_->childAt(i)));
    }
  }
}

} // namespace facebook::velox::aggregate
