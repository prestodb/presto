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

#include "velox/exec/VectorHasher.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Portability.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/common/memory/HashStringAllocator.h"

namespace facebook::velox::exec {

#define VALUE_ID_TYPE_DISPATCH(TEMPLATE_FUNC, typeKind, ...)             \
  [&]() {                                                                \
    switch (typeKind) {                                                  \
      case TypeKind::BOOLEAN: {                                          \
        return TEMPLATE_FUNC<TypeKind::BOOLEAN>(__VA_ARGS__);            \
      }                                                                  \
      case TypeKind::TINYINT: {                                          \
        return TEMPLATE_FUNC<TypeKind::TINYINT>(__VA_ARGS__);            \
      }                                                                  \
      case TypeKind::SMALLINT: {                                         \
        return TEMPLATE_FUNC<TypeKind::SMALLINT>(__VA_ARGS__);           \
      }                                                                  \
      case TypeKind::INTEGER: {                                          \
        return TEMPLATE_FUNC<TypeKind::INTEGER>(__VA_ARGS__);            \
      }                                                                  \
      case TypeKind::BIGINT: {                                           \
        return TEMPLATE_FUNC<TypeKind::BIGINT>(__VA_ARGS__);             \
      }                                                                  \
      case TypeKind::DATE: {                                             \
        return TEMPLATE_FUNC<TypeKind::DATE>(__VA_ARGS__);               \
      }                                                                  \
      case TypeKind::INTERVAL_DAY_TIME: {                                \
        return TEMPLATE_FUNC<TypeKind::INTERVAL_DAY_TIME>(__VA_ARGS__);  \
      }                                                                  \
      case TypeKind::VARCHAR:                                            \
      case TypeKind::VARBINARY: {                                        \
        return TEMPLATE_FUNC<TypeKind::VARCHAR>(__VA_ARGS__);            \
      }                                                                  \
      default:                                                           \
        VELOX_UNREACHABLE(                                               \
            "Unsupported value ID type: ", mapTypeKindToName(typeKind)); \
    }                                                                    \
  }()

namespace {
template <TypeKind Kind>
uint64_t hashOne(DecodedVector& decoded, vector_size_t index) {
  if (Kind == TypeKind::ROW || Kind == TypeKind::ARRAY ||
      Kind == TypeKind::MAP) {
    // Virtual function call for complex type.
    return decoded.base()->hashValueAt(decoded.index(index));
  }
  // Inlined for scalars.
  using T = typename KindToFlatVector<Kind>::HashRowType;
  return folly::hasher<T>()(decoded.valueAt<T>(index));
}
} // namespace

template <TypeKind Kind>
void VectorHasher::hashValues(
    const SelectivityVector& rows,
    bool mix,
    uint64_t* result) {
  using T = typename TypeTraits<Kind>::NativeType;
  if (decoded_.isConstantMapping()) {
    auto hash = decoded_.isNullAt(rows.begin())
        ? kNullHash
        : hashOne<Kind>(decoded_, rows.begin());
    rows.applyToSelected([&](vector_size_t row) {
      result[row] = mix ? bits::hashMix(result[row], hash) : hash;
    });
  } else if (decoded_.isIdentityMapping()) {
    rows.applyToSelected([&](vector_size_t row) {
      if (decoded_.isNullAt(row)) {
        result[row] = mix ? bits::hashMix(result[row], kNullHash) : kNullHash;
        return;
      }
      auto hash = hashOne<Kind>(decoded_, row);
      result[row] = mix ? bits::hashMix(result[row], hash) : hash;
    });
  } else {
    cachedHashes_.resize(decoded_.base()->size());
    std::fill(cachedHashes_.begin(), cachedHashes_.end(), kNullHash);
    rows.applyToSelected([&](vector_size_t row) {
      if (decoded_.isNullAt(row)) {
        result[row] = mix ? bits::hashMix(result[row], kNullHash) : kNullHash;
        return;
      }
      auto baseIndex = decoded_.index(row);
      uint64_t hash = cachedHashes_[baseIndex];
      if (hash == kNullHash) {
        hash = hashOne<Kind>(decoded_, row);
        cachedHashes_[baseIndex] = hash;
      }
      result[row] = mix ? bits::hashMix(result[row], hash) : hash;
    });
  }
}

template <TypeKind Kind>
bool VectorHasher::makeValueIds(
    const SelectivityVector& rows,
    uint64_t* result) {
  using T = typename TypeTraits<Kind>::NativeType;

  if (decoded_.isConstantMapping()) {
    uint64_t id = decoded_.isNullAt(rows.begin())
        ? 0
        : valueId(decoded_.valueAt<T>(rows.begin()));
    if (id == kUnmappable) {
      analyzeValue(decoded_.valueAt<T>(rows.begin()));
      return false;
    }
    rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
      result[row] = multiplier_ == 1 ? id : result[row] + multiplier_ * id;
    });
    return true;
  }

  if (decoded_.isIdentityMapping()) {
    if (decoded_.mayHaveNulls()) {
      return makeValueIdsFlatWithNulls<T>(rows, result);
    } else {
      return makeValueIdsFlatNoNulls<T>(rows, result);
    }
  }

  if (decoded_.mayHaveNulls()) {
    return makeValueIdsDecoded<T, true>(rows, result);
  } else {
    return makeValueIdsDecoded<T, false>(rows, result);
  }
}

template <>
bool VectorHasher::makeValueIdsFlatNoNulls<bool>(
    const SelectivityVector& rows,
    uint64_t* result) {
  const auto* values = decoded_.data<uint64_t>();
  rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
    bool value = bits::isBitSet(values, row);
    uint64_t id = valueId(value);
    result[row] = multiplier_ == 1 ? id : result[row] + multiplier_ * id;
  });
  return true;
}

template <>
bool VectorHasher::makeValueIdsFlatWithNulls<bool>(
    const SelectivityVector& rows,
    uint64_t* result) {
  const auto* values = decoded_.data<uint64_t>();
  const auto* nulls = decoded_.nulls();
  rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
    if (bits::isBitNull(nulls, row)) {
      if (multiplier_ == 1) {
        result[row] = 0;
      }
      return;
    }
    bool value = bits::isBitSet(values, row);
    uint64_t id = valueId(value);
    result[row] = multiplier_ == 1 ? id : result[row] + multiplier_ * id;
  });
  return true;
}

template <typename T>
bool VectorHasher::makeValueIdsFlatNoNulls(
    const SelectivityVector& rows,
    uint64_t* result) {
  const auto* values = decoded_.data<T>();
  if (isRange_ && tryMapToRange(values, rows, result)) {
    return true;
  }

  bool success = true;
  rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
    T value = values[row];
    if (!success) {
      // If all were not mappable and we do not remove unmappable,
      // we just analyze the remaining so we can decide the hash mode.
      analyzeValue(value);
      return;
    }
    uint64_t id = valueId(value);
    if (id == kUnmappable) {
      success = false;
      analyzeValue(value);
      return;
    }
    result[row] = multiplier_ == 1 ? id : result[row] + multiplier_ * id;
  });

  return success;
}

template <typename T>
bool VectorHasher::makeValueIdsFlatWithNulls(
    const SelectivityVector& rows,
    uint64_t* result) {
  const auto* values = decoded_.data<T>();
  const auto* nulls = decoded_.nulls();

  bool success = true;
  rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
    if (bits::isBitNull(nulls, row)) {
      if (multiplier_ == 1) {
        result[row] = 0;
      }
      return;
    }
    T value = values[row];
    if (!success) {
      // If all were not mappable we just analyze the remaining so we can decide
      // the hash mode.
      analyzeValue(value);
      return;
    }
    uint64_t id = valueId(value);
    if (id == kUnmappable) {
      success = false;
      analyzeValue(value);
      return;
    }
    result[row] = multiplier_ == 1 ? id : result[row] + multiplier_ * id;
  });
  return success;
}

template <typename T, bool mayHaveNulls>
bool VectorHasher::makeValueIdsDecoded(
    const SelectivityVector& rows,
    uint64_t* result) {
  cachedHashes_.resize(decoded_.base()->size());
  std::fill(cachedHashes_.begin(), cachedHashes_.end(), 0);

  auto indices = decoded_.indices();
  auto values = decoded_.data<T>();

  bool success = true;
  rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
    if constexpr (mayHaveNulls) {
      if (decoded_.isNullAt(row)) {
        if (multiplier_ == 1) {
          result[row] = 0;
        }
        return;
      }
    }
    auto baseIndex = indices[row];
    uint64_t id = cachedHashes_[baseIndex];
    if (id == 0) {
      T value = values[baseIndex];

      if (!success) {
        // If all were not mappable we just analyze the remaining so we can
        // decide the hash mode.
        analyzeValue(value);
        return;
      }
      id = valueId(value);
      if (id == kUnmappable) {
        analyzeValue(value);
        success = false;
        return;
      }
      cachedHashes_[baseIndex] = id;
    }
    result[row] = multiplier_ == 1 ? id : result[row] + multiplier_ * id;
  });
  return success;
}

template <>
bool VectorHasher::makeValueIdsDecoded<bool, true>(
    const SelectivityVector& rows,
    uint64_t* result) {
  auto indices = decoded_.indices();
  auto values = decoded_.data<uint64_t>();

  rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
    if (decoded_.isNullAt(row)) {
      if (multiplier_ == 1) {
        result[row] = 0;
      }
      return;
    }

    bool value = bits::isBitSet(values, indices[row]);
    auto id = valueId(value);
    result[row] = multiplier_ == 1 ? id : result[row] + multiplier_ * id;
  });
  return true;
}

template <>
bool VectorHasher::makeValueIdsDecoded<bool, false>(
    const SelectivityVector& rows,
    uint64_t* result) {
  auto indices = decoded_.indices();
  auto values = decoded_.data<uint64_t>();

  rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
    bool value = bits::isBitSet(values, indices[row]);
    auto id = valueId(value);
    result[row] = multiplier_ == 1 ? id : result[row] + multiplier_ * id;
  });
  return true;
}

bool VectorHasher::computeValueIds(
    const SelectivityVector& rows,
    raw_vector<uint64_t>& result) {
  return VALUE_ID_TYPE_DISPATCH(makeValueIds, typeKind_, rows, result.data());
}

bool VectorHasher::computeValueIdsForRows(
    char** groups,
    int32_t numGroups,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask,
    raw_vector<uint64_t>& result) {
  return VALUE_ID_TYPE_DISPATCH(
      makeValueIdsForRows,
      typeKind_,
      groups,
      numGroups,
      offset,
      nullByte,
      nullMask,
      result.data());
}

template <>
bool VectorHasher::makeValueIdsForRows<TypeKind::VARCHAR>(
    char** groups,
    int32_t numGroups,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask,
    uint64_t* result) {
  for (int32_t i = 0; i < numGroups; ++i) {
    if (isNullAt(groups[i], nullByte, nullMask)) {
      if (multiplier_ == 1) {
        result[i] = 0;
      }
    } else {
      std::string storage;
      auto id = valueId<StringView>(HashStringAllocator::contiguousString(
          valueAt<StringView>(groups[i], offset), storage));
      if (id == kUnmappable) {
        return false;
      }
      result[i] = multiplier_ == 1 ? id : result[i] + multiplier_ * id;
    }
  }
  return true;
}

template <TypeKind Kind>
void VectorHasher::lookupValueIdsTyped(
    const DecodedVector& decoded,
    SelectivityVector& rows,
    raw_vector<uint64_t>& hashes,
    uint64_t* result,
    bool noNulls) const {
  using T = typename TypeTraits<Kind>::NativeType;
  if (decoded.isConstantMapping()) {
    if (decoded.isNullAt(rows.begin())) {
      if (multiplier_ == 1) {
        rows.applyToSelected([&](vector_size_t row)
                                 INLINE_LAMBDA { result[row] = 0; });
      }
      return;
    }

    uint64_t id = lookupValueId(decoded.valueAt<T>(rows.begin()));
    if (id == kUnmappable) {
      rows.clearAll();
    } else {
      rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
        result[row] = multiplier_ == 1 ? id : result[row] + multiplier_ * id;
      });
    }
  } else if (decoded.isIdentityMapping()) {
    if (Kind == TypeKind::BIGINT && isRange_ && noNulls) {
      lookupIdsRangeSimd<int64_t>(decoded, rows, result);
    } else if (Kind == TypeKind::INTEGER && isRange_ && noNulls) {
      lookupIdsRangeSimd<int32_t>(decoded, rows, result);
    } else {
      rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
        if (decoded.isNullAt(row)) {
          if (multiplier_ == 1) {
            result[row] = 0;
          }
          return;
        }
        T value = decoded.valueAt<T>(row);
        uint64_t id = lookupValueId(value);
        if (id == kUnmappable) {
          rows.setValid(row, false);
          return;
        }
        result[row] = multiplier_ == 1 ? id : result[row] + multiplier_ * id;
      });
    }
    rows.updateBounds();
  } else {
    hashes.resize(decoded.base()->size());
    std::fill(hashes.begin(), hashes.end(), 0);
    rows.applyToSelected([&](vector_size_t row) INLINE_LAMBDA {
      if (decoded.isNullAt(row)) {
        if (multiplier_ == 1) {
          result[row] = 0;
        }
        return;
      }
      auto baseIndex = decoded.index(row);
      uint64_t id = hashes[baseIndex];
      if (id == 0) {
        T value = decoded.valueAt<T>(row);
        id = lookupValueId(value);
        if (id == kUnmappable) {
          rows.setValid(row, false);
          return;
        }
        hashes[baseIndex] = id;
      }
      result[row] = multiplier_ == 1 ? id : result[row] + multiplier_ * id;
    });
    rows.updateBounds();
  }
}

template <typename T>
void VectorHasher::lookupIdsRangeSimd(
    const DecodedVector& decoded,
    SelectivityVector& rows,
    uint64_t* result) const {
  static_assert(sizeof(T) == 8 || sizeof(T) == 4);
  auto lower = xsimd::batch<T>::broadcast(min_);
  auto upper = xsimd::batch<T>::broadcast(max_);
  auto data = decoded.data<T>();
  uint64_t offset = min_ - 1;
  auto bits = rows.asMutableRange().bits();
  bits::forBatches<xsimd::batch<T>::size>(
      bits, rows.begin(), rows.end(), [&](auto index, auto /*mask*/) {
        auto values = xsimd::batch<T>::load_unaligned(data + index);
        uint64_t outOfRange =
            simd::toBitMask(lower > values) | simd::toBitMask(values > upper);
        if (outOfRange) {
          bits[index / 64] &= ~(outOfRange << (index & 63));
        }
        if (outOfRange != bits::lowMask(xsimd::batch<T>::size)) {
          if constexpr (sizeof(T) == 8) {
            auto unsignedValues =
                static_cast<xsimd::batch<typename std::make_unsigned<T>::type>>(
                    values);
            if (multiplier_ == 1) {
              (unsignedValues - offset).store_unaligned(result + index);
            } else {
              (xsimd::batch<uint64_t>::load_unaligned(result + index) +
               ((unsignedValues - offset) * multiplier_))
                  .store_unaligned(result + index);
            }
          } else {
            // Widen 8 to 2 x 4 since result is always 64 wide.
            auto first4 = static_cast<xsimd::batch<uint64_t>>(
                              simd::getHalf<int64_t, 0>(values)) -
                offset;
            auto next4 = static_cast<xsimd::batch<uint64_t>>(
                             simd::getHalf<int64_t, 1>(values)) -
                offset;
            if (multiplier_ == 1) {
              first4.store_unaligned(result + index);
              next4.store_unaligned(result + index + first4.size);
            } else {
              (xsimd::batch<uint64_t>::load_unaligned(result + index) +
               (first4 * multiplier_))
                  .store_unaligned(result + index);
              (xsimd::batch<uint64_t>::load_unaligned(
                   result + index + first4.size) +
               (next4 * multiplier_))
                  .store_unaligned(result + index + first4.size);
            }
          }
        }
      });
}

void VectorHasher::lookupValueIds(
    const BaseVector& values,
    SelectivityVector& rows,
    ScratchMemory& scratchMemory,
    raw_vector<uint64_t>& result,
    bool noNulls) const {
  scratchMemory.decoded.decode(values, rows);
  VALUE_ID_TYPE_DISPATCH(
      lookupValueIdsTyped,
      typeKind_,
      scratchMemory.decoded,
      rows,
      scratchMemory.hashes,
      result.data(),
      noNulls);
}

void VectorHasher::hash(
    const SelectivityVector& rows,
    bool mix,
    raw_vector<uint64_t>& result) {
  VELOX_DYNAMIC_TYPE_DISPATCH(hashValues, typeKind_, rows, mix, result.data());
}

void VectorHasher::hashPrecomputed(
    const SelectivityVector& rows,
    bool mix,
    raw_vector<uint64_t>& result) const {
  rows.applyToSelected([&](vector_size_t row) {
    result[row] =
        mix ? bits::hashMix(result[row], precomputedHash_) : precomputedHash_;
  });
}

void VectorHasher::precompute(const BaseVector& value) {
  if (value.isNullAt(0)) {
    precomputedHash_ = kNullHash;
    return;
  }

  const SelectivityVector rows(1, true);
  decoded_.decode(value, rows);
  precomputedHash_ =
      VELOX_DYNAMIC_TYPE_DISPATCH(hashOne, typeKind_, decoded_, 0);
}

void VectorHasher::analyze(
    char** groups,
    int32_t numGroups,
    int32_t offset,
    int32_t nullByte,
    uint8_t nullMask) {
  VALUE_ID_TYPE_DISPATCH(
      analyzeTyped, typeKind_, groups, numGroups, offset, nullByte, nullMask);
}

template <>
void VectorHasher::analyzeValue(StringView value) {
  int size = value.size();
  auto data = value.data();
  if (!rangeOverflow_) {
    if (size > kStringASRangeMaxSize) {
      rangeOverflow_ = true;
    } else {
      int64_t number = stringAsNumber(data, size);
      updateRange(number);
    }
  }
  if (!distinctOverflow_) {
    UniqueValue unique(data, size);
    unique.setId(uniqueValues_.size() + 1);
    auto pair = uniqueValues_.insert(unique);
    if (pair.second) {
      if (uniqueValues_.size() > kMaxDistinct) {
        distinctOverflow_ = true;
        return;
      }
      copyStringToLocal(&*pair.first);
    }
  }
}

void VectorHasher::copyStringToLocal(const UniqueValue* unique) {
  auto size = unique->size();
  if (size <= sizeof(int64_t)) {
    return;
  }
  if (distinctStringsBytes_ > kMaxDistinctStringsBytes) {
    distinctOverflow_ = true;
    return;
  }
  if (uniqueValuesStorage_.empty()) {
    uniqueValuesStorage_.emplace_back();
    uniqueValuesStorage_.back().reserve(std::max(kStringBufferUnitSize, size));
    distinctStringsBytes_ += uniqueValuesStorage_.back().capacity();
  }
  auto str = &uniqueValuesStorage_.back();
  if (str->size() + size > str->capacity()) {
    uniqueValuesStorage_.emplace_back();
    uniqueValuesStorage_.back().reserve(std::max(kStringBufferUnitSize, size));
    distinctStringsBytes_ += uniqueValuesStorage_.back().capacity();
    str = &uniqueValuesStorage_.back();
  }
  auto start = str->size();
  str->resize(start + size);
  memcpy(str->data() + start, reinterpret_cast<char*>(unique->data()), size);
  const_cast<UniqueValue*>(unique)->setData(
      reinterpret_cast<int64_t>(str->data() + start));
}

std::unique_ptr<common::Filter> VectorHasher::getFilter(
    bool nullAllowed) const {
  switch (typeKind_) {
    case TypeKind::TINYINT:
      FOLLY_FALLTHROUGH;
    case TypeKind::SMALLINT:
      FOLLY_FALLTHROUGH;
    case TypeKind::INTEGER:
      FOLLY_FALLTHROUGH;
    case TypeKind::BIGINT:
      if (!distinctOverflow_) {
        std::vector<int64_t> values;
        values.reserve(uniqueValues_.size());
        for (const auto& value : uniqueValues_) {
          values.emplace_back(value.data());
        }

        return common::createBigintValues(values, nullAllowed);
      }
      FOLLY_FALLTHROUGH;
    default:
      // TODO Add support for strings.
      return nullptr;
  }
}

namespace {
template <typename T>
// Adds 'reserve' to either end of the range between 'min' and 'max' while
// staying in the range of T.
void extendRange(int64_t reserve, int64_t& min, int64_t& max) {
  int64_t kMin = std::numeric_limits<T>::min();
  int64_t kMax = std::numeric_limits<T>::max();
  if (kMin + reserve + 1 > min) {
    min = kMin;
  } else {
    min -= reserve;
  }
  if (kMax - reserve < max) {
    max = kMax;
  } else {
    max += reserve;
  }
}

// Adds 'reservePct' % to either end of the range between 'min' and 'max'
// while staying in the range of 'kind'.
void extendRange(
    TypeKind kind,
    int32_t reservePct,
    int64_t& min,
    int64_t& max) {
  // The reserve is 2 + reservePct % of the range. Add 2 to make sure
  // that a non-0 peercentage actually adds something for a small
  // range.
  int64_t reserve =
      reservePct == 0 ? 0 : 2 + (max - min) * (reservePct / 100.0);
  switch (kind) {
    case TypeKind::BOOLEAN:
      break;
    case TypeKind::TINYINT:
      extendRange<int8_t>(reserve, min, max);
      break;
    case TypeKind::SMALLINT:
      extendRange<int16_t>(reserve, min, max);
      break;
    case TypeKind::INTEGER:
    case TypeKind::DATE:
      extendRange<int32_t>(reserve, min, max);
      break;
    case TypeKind::BIGINT:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      extendRange<int64_t>(reserve, min, max);
      break;

    default:
      VELOX_FAIL("Unsupported VectorHasher typeKind {}", kind);
  }
}

int64_t addIdReserve(size_t numDistinct, int32_t reservePct) {
  // A merge of hashers in a hash join build may end up over the limit, so
  // return that.
  if (numDistinct > VectorHasher::kMaxDistinct) {
    return numDistinct;
  }
  if (reservePct == VectorHasher::kNoLimit) {
    return VectorHasher::kMaxDistinct;
  }
  // NOTE: 'kMaxDistinct' is a small value so no need to check overflow for
  // reservation here.
  return std::min<int64_t>(
      VectorHasher::kMaxDistinct, numDistinct * (1 + (reservePct / 100.0)));
}
} // namespace

void VectorHasher::cardinality(
    int32_t reservePct,
    uint64_t& asRange,
    uint64_t& asDistincts) {
  if (typeKind_ == TypeKind::BOOLEAN) {
    hasRange_ = true;
    asRange = 3;
    asDistincts = 3;
    return;
  }
  int64_t signedRange;
  if (!hasRange_ || rangeOverflow_) {
    asRange = kRangeTooLarge;
  } else if (__builtin_sub_overflow(max_, min_, &signedRange)) {
    rangeOverflow_ = true;
    asRange = kRangeTooLarge;
  } else if (signedRange < kMaxRange) {
    // We check that after the extension by reservePct the range of max - min
    // will still be in int64_t bounds.
    VELOX_CHECK_GE(100, reservePct);
    static_assert(kMaxRange < std::numeric_limits<uint64_t>::max() / 4);
    // We pad the range by 'reservePct'%, half below and half above,
    // while staying within bounds of the type. We do not pad the
    // limits yet, this is done only when enabling range mode.
    int64_t min = min_;
    int64_t max = max_;
    extendRange(type_->kind(), reservePct, min, max);
    asRange = (max - min) + 2;
  } else {
    rangeOverflow_ = true;
    asRange = kRangeTooLarge;
  }
  if (distinctOverflow_) {
    asDistincts = kRangeTooLarge;
    return;
  }
  // Padded count of values + 1 for null.
  asDistincts = addIdReserve(uniqueValues_.size(), reservePct) + 1;
}

uint64_t VectorHasher::enableValueIds(uint64_t multiplier, int32_t reservePct) {
  VELOX_CHECK_NE(
      typeKind_,
      TypeKind::BOOLEAN,
      "A boolean VectorHasher should  always be by range");
  multiplier_ = multiplier;
  rangeSize_ = addIdReserve(uniqueValues_.size(), reservePct) + 1;
  isRange_ = false;
  uint64_t result;
  if (__builtin_mul_overflow(multiplier_, rangeSize_, &result)) {
    return kRangeTooLarge;
  }
  return result;
}

uint64_t VectorHasher::enableValueRange(
    uint64_t multiplier,
    int32_t reservePct) {
  multiplier_ = multiplier;
  VELOX_CHECK_LE(0, reservePct);
  VELOX_CHECK(hasRange_);
  extendRange(type_->kind(), reservePct, min_, max_);
  isRange_ = true;
  // No overflow because max range is under 63 bits.
  if (typeKind_ == TypeKind::BOOLEAN) {
    rangeSize_ = 3;
  } else {
    rangeSize_ = (max_ - min_) + 2;
  }
  uint64_t result;
  if (__builtin_mul_overflow(multiplier_, rangeSize_, &result)) {
    return kRangeTooLarge;
  }
  return result;
}

void VectorHasher::copyStatsFrom(const VectorHasher& other) {
  hasRange_ = other.hasRange_;
  rangeOverflow_ = other.rangeOverflow_;
  distinctOverflow_ = other.distinctOverflow_;

  min_ = other.min_;
  max_ = other.max_;
  uniqueValues_ = other.uniqueValues_;
}

void VectorHasher::merge(const VectorHasher& other) {
  if (typeKind_ == TypeKind::BOOLEAN) {
    return;
  }
  if (other.empty()) {
    return;
  }
  if (empty()) {
    copyStatsFrom(other);
    return;
  }
  if (hasRange_ && other.hasRange_ && !rangeOverflow_ &&
      !other.rangeOverflow_) {
    min_ = std::min(min_, other.min_);
    max_ = std::max(max_, other.max_);
  } else {
    hasRange_ = false;
    rangeOverflow_ = true;
  }
  if (!distinctOverflow_ && !other.distinctOverflow_) {
    // Unique values can be merged without dispatch on type. All the
    // merged hashers must stay live for string type columns.
    for (UniqueValue value : other.uniqueValues_) {
      // Assign a new id at end of range for the case 'value' is not
      // in 'uniqueValues_'. We do not set overflow here because the
      // memory is already allocated and there is a known cap on size.
      value.setId(uniqueValues_.size() + 1);
      uniqueValues_.insert(value);
    }
  } else {
    distinctOverflow_ = true;
  }
}

std::string VectorHasher::toString() const {
  std::stringstream out;
  out << "<VectorHasher type=" << type_->toString() << "  isRange_=" << isRange_
      << " rangeSize= " << rangeSize_ << " min=" << min_ << " max=" << max_
      << " multiplier=" << multiplier_
      << " numDistinct=" << uniqueValues_.size() << ">";
  return out.str();
}

} // namespace facebook::velox::exec
