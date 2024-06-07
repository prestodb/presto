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
#include "velox/connectors/hive/HivePartitionFunction.h"

namespace facebook::velox::connector::hive {

namespace {
void mergeHash(bool mix, uint32_t oneHash, uint32_t& aggregateHash) {
  aggregateHash = mix ? aggregateHash * 31 + oneHash : oneHash;
}

int32_t hashInt64(int64_t value) {
  return ((*reinterpret_cast<uint64_t*>(&value)) >> 32) ^ value;
}

#if defined(__has_feature)
#if __has_feature(__address_sanitizer__)
__attribute__((no_sanitize("integer")))
#endif
#endif
uint32_t
hashBytes(StringView bytes, int32_t initialValue) {
  uint32_t hash = initialValue;
  auto* data = bytes.data();
  for (auto i = 0; i < bytes.size(); ++i) {
    hash = hash * 31 + *reinterpret_cast<const int8_t*>(data + i);
  }
  return hash;
}

int32_t hashTimestamp(const Timestamp& ts) {
  return hashInt64((ts.getSeconds() << 30) | ts.getNanos());
}

template <TypeKind kind>
inline uint32_t hashOne(typename TypeTraits<kind>::NativeType /* value */) {
  VELOX_UNSUPPORTED(
      "Hive partitioning function doesn't support {} type",
      TypeTraits<kind>::name);
  return 0; // Make compiler happy.
}

template <>
inline uint32_t hashOne<TypeKind::BOOLEAN>(bool value) {
  return value ? 1 : 0;
}

template <>
inline uint32_t hashOne<TypeKind::TINYINT>(int8_t value) {
  return static_cast<uint32_t>(value);
}

template <>
inline uint32_t hashOne<TypeKind::SMALLINT>(int16_t value) {
  return static_cast<uint32_t>(value);
}

template <>
inline uint32_t hashOne<TypeKind::INTEGER>(int32_t value) {
  return static_cast<uint32_t>(value);
}

template <>
inline uint32_t hashOne<TypeKind::REAL>(float value) {
  return static_cast<uint32_t>(*reinterpret_cast<const int32_t*>(&value));
}

template <>
inline uint32_t hashOne<TypeKind::BIGINT>(int64_t value) {
  return hashInt64(value);
}

template <>
inline uint32_t hashOne<TypeKind::DOUBLE>(double value) {
  return hashInt64(*reinterpret_cast<const int64_t*>(&value));
}

template <>
inline uint32_t hashOne<TypeKind::VARCHAR>(StringView value) {
  return hashBytes(value, 0);
}

template <>
inline uint32_t hashOne<TypeKind::VARBINARY>(StringView value) {
  return hashBytes(value, 0);
}

template <>
inline uint32_t hashOne<TypeKind::TIMESTAMP>(Timestamp value) {
  return hashTimestamp(value);
}

template <>
inline uint32_t hashOne<TypeKind::UNKNOWN>(UnknownValue /*value*/) {
  VELOX_FAIL("Unknown values cannot be non-NULL");
}

template <TypeKind kind>
void hashPrimitive(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes) {
  if (rows.isAllSelected()) {
    // The compiler seems to be a little fickle with optimizations.
    // Although rows.applyToSelected should do roughly the same thing, doing
    // this here along with assigning rows.size() to a variable seems to help
    // the compiler to inline hashOne showing a 50% performance improvement in
    // benchmarks.
    vector_size_t numRows = rows.size();
    for (auto i = 0; i < numRows; ++i) {
      const uint32_t hash = values.isNullAt(i)
          ? 0
          : hashOne<kind>(
                values.valueAt<typename TypeTraits<kind>::NativeType>(i));
      mergeHash(mix, hash, hashes[i]);
    }
  } else {
    rows.applyToSelected([&](auto row) INLINE_LAMBDA {
      const uint32_t hash = values.isNullAt(row)
          ? 0
          : hashOne<kind>(
                values.valueAt<typename TypeTraits<kind>::NativeType>(row));
      mergeHash(mix, hash, hashes[row]);
    });
  }
}

void hashPrecomputed(
    uint32_t precomputedHash,
    vector_size_t numRows,
    bool mix,
    std::vector<uint32_t>& hashes) {
  for (auto i = 0; i < numRows; ++i) {
    hashes[i] = mix ? hashes[i] * 31 + precomputedHash : precomputedHash;
  }
}
} // namespace

template <>
void HivePartitionFunction::hashTyped<TypeKind::BOOLEAN>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t /* poolIndex */) {
  hashPrimitive<TypeKind::BOOLEAN>(values, rows, mix, hashes);
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::TINYINT>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t /* poolIndex */) {
  hashPrimitive<TypeKind::TINYINT>(values, rows, mix, hashes);
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::SMALLINT>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t /* poolIndex */) {
  hashPrimitive<TypeKind::SMALLINT>(values, rows, mix, hashes);
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::INTEGER>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t /* poolIndex */) {
  hashPrimitive<TypeKind::INTEGER>(values, rows, mix, hashes);
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::REAL>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t /* poolIndex */) {
  hashPrimitive<TypeKind::REAL>(values, rows, mix, hashes);
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::BIGINT>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t /* poolIndex */) {
  hashPrimitive<TypeKind::BIGINT>(values, rows, mix, hashes);
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::DOUBLE>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t /* poolIndex */) {
  hashPrimitive<TypeKind::DOUBLE>(values, rows, mix, hashes);
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::VARCHAR>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t /* poolIndex */) {
  hashPrimitive<TypeKind::VARCHAR>(values, rows, mix, hashes);
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::VARBINARY>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t /* poolIndex */) {
  hashPrimitive<TypeKind::VARBINARY>(values, rows, mix, hashes);
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::TIMESTAMP>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t /* poolIndex */) {
  hashPrimitive<TypeKind::TIMESTAMP>(values, rows, mix, hashes);
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::UNKNOWN>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t /* poolIndex */) {
  hashPrimitive<TypeKind::UNKNOWN>(values, rows, mix, hashes);
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::OPAQUE>(
    const DecodedVector& /*values*/,
    const SelectivityVector& /*rows*/,
    bool /*mix*/,
    std::vector<uint32_t>& /*hashes*/,
    size_t /* poolIndex */) {
  VELOX_UNSUPPORTED("Hive partitioning function doesn't support OPAQUE type");
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::ARRAY>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t poolIndex) {
  auto& elementsDecoded = getDecodedVector(poolIndex);
  auto& elementsRows = getRows(poolIndex);
  auto& elementsHashes = getHashes(poolIndex);

  const auto* arrayVector = values.base()->as<ArrayVector>();
  const vector_size_t elementsSize = arrayVector->elements()->size();
  elementsRows.resizeFill(elementsSize, false);
  elementsHashes.resize(elementsSize);

  rows.applyToSelected([&](auto row) {
    if (!values.isNullAt(row)) {
      const auto index = values.index(row);
      const auto offset = arrayVector->offsetAt(index);
      const auto length = arrayVector->sizeAt(index);

      elementsRows.setValidRange(offset, offset + length, true);
    }
  });

  elementsRows.updateBounds();

  elementsDecoded.decode(*arrayVector->elements(), elementsRows);

  hash(
      elementsDecoded,
      elementsDecoded.base()->typeKind(),
      elementsRows,
      false,
      elementsHashes,
      poolIndex + 1);

  rows.applyToSelected([&](auto row) {
    uint32_t hash = 0;

    if (!values.isNullAt(row)) {
      const auto index = values.index(row);
      const auto offset = arrayVector->offsetAt(index);
      const auto length = arrayVector->sizeAt(index);

      for (size_t i = offset; i < offset + length; ++i) {
        mergeHash(true, elementsHashes[i], hash);
      }
    }

    mergeHash(mix, hash, hashes[row]);
  });
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::MAP>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t poolIndex) {
  auto& valuesDecoded = getDecodedVector(poolIndex);
  auto& keysDecoded = getDecodedVector(poolIndex + 1);
  auto& elementsRows = getRows(poolIndex);
  auto& valuesHashes = getHashes(poolIndex);
  auto& keysHashes = getHashes(poolIndex + 1);

  const auto* mapVector = values.base()->as<MapVector>();
  const vector_size_t elementsSize = mapVector->mapKeys()->size();
  elementsRows.resizeFill(elementsSize, false);
  keysHashes.resize(elementsSize);
  valuesHashes.resize(elementsSize);

  rows.applyToSelected([&](auto row) {
    if (!values.isNullAt(row)) {
      const auto index = values.index(row);
      const auto offset = mapVector->offsetAt(index);
      const auto length = mapVector->sizeAt(index);

      elementsRows.setValidRange(offset, offset + length, true);
    }
  });

  elementsRows.updateBounds();

  keysDecoded.decode(*mapVector->mapKeys(), elementsRows);
  valuesDecoded.decode(*mapVector->mapValues(), elementsRows);

  hash(
      keysDecoded,
      keysDecoded.base()->typeKind(),
      elementsRows,
      false,
      keysHashes,
      poolIndex + 2);

  hash(
      valuesDecoded,
      valuesDecoded.base()->typeKind(),
      elementsRows,
      false,
      valuesHashes,
      poolIndex + 2);

  rows.applyToSelected([&](auto row) {
    uint32_t hash = 0;

    if (!values.isNullAt(row)) {
      const auto index = values.index(row);
      const auto offset = mapVector->offsetAt(index);
      const auto length = mapVector->sizeAt(index);

      for (size_t i = offset; i < offset + length; ++i) {
        hash += keysHashes[i] ^ valuesHashes[i];
      }
    }

    mergeHash(mix, hash, hashes[row]);
  });
}

template <>
void HivePartitionFunction::hashTyped<TypeKind::ROW>(
    const DecodedVector& values,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t poolIndex) {
  auto& childDecodedVector = getDecodedVector(poolIndex);
  auto& childRows = getRows(poolIndex);
  auto& childHashes = getHashes(poolIndex);

  const auto* rowVector = values.base()->as<RowVector>();
  childRows.resizeFill(rowVector->size(), false);
  childHashes.resize(rowVector->size());

  rows.applyToSelected([&](auto row) {
    if (!values.isNullAt(row)) {
      childRows.setValid(values.index(row), true);
    }
  });

  childRows.updateBounds();

  for (vector_size_t i = 0; i < rowVector->childrenSize(); ++i) {
    auto& child = rowVector->childAt(i);
    childDecodedVector.decode(*child, childRows);
    hash(
        childDecodedVector,
        child->typeKind(),
        childRows,
        i > 0,
        childHashes,
        poolIndex + 1);
  }

  rows.applyToSelected([&](auto row) {
    mergeHash(
        mix,
        values.isNullAt(row) ? 0 : childHashes[values.index(row)],
        hashes[row]);
  });
}

void HivePartitionFunction::hash(
    const DecodedVector& values,
    TypeKind typeKind,
    const SelectivityVector& rows,
    bool mix,
    std::vector<uint32_t>& hashes,
    size_t poolIndex) {
  // This function mirrors the behavior of function hashCode in
  // HIVE-12025 ba83fd7bff
  // serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java
  // https://github.com/apache/hive/blob/ba83fd7bff/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspectorUtils.java

  // HIVE-7148 proposed change to bucketing hash algorithms. If that
  // gets implemented, this function will need to change
  // significantly.

  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      hashTyped, typeKind, values, rows, mix, hashes, poolIndex);
}

HivePartitionFunction::HivePartitionFunction(
    int numBuckets,
    std::vector<int> bucketToPartition,
    std::vector<column_index_t> keyChannels,
    const std::vector<VectorPtr>& constValues)
    : numBuckets_{numBuckets},
      bucketToPartition_{bucketToPartition},
      keyChannels_{std::move(keyChannels)} {
  precomputedHashes_.resize(keyChannels_.size());
  size_t constChannel{0};
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    if (keyChannels_[i] == kConstantChannel) {
      precompute(*(constValues[constChannel++]), i);
    }
  }
}

std::optional<uint32_t> HivePartitionFunction::partition(
    const RowVector& input,
    std::vector<uint32_t>& partitions) {
  const auto numRows = input.size();

  auto& decodedVector = getDecodedVector();
  auto& rows = getRows();
  auto& hashes = getHashes();
  rows.resizeFill(numRows, true);
  if (numRows > hashes.size()) {
    hashes.resize(numRows);
  }
  partitions.resize(numRows);
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    if (keyChannels_[i] != kConstantChannel) {
      const auto& keyVector = input.childAt(keyChannels_[i]);
      decodedVector.decode(*keyVector, rows);
      hash(decodedVector, keyVector->typeKind(), rows, i > 0, hashes, 1);
    } else {
      hashPrecomputed(precomputedHashes_[i], numRows, i > 0, hashes);
    }
  }

  static const int32_t kInt32Max = std::numeric_limits<int32_t>::max();

  if (bucketToPartition_.empty()) {
    // NOTE: if bucket to partition mapping is empty, then we do
    // identical mapping.
    for (auto i = 0; i < numRows; ++i) {
      partitions[i] = (hashes[i] & kInt32Max) % numBuckets_;
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      partitions[i] =
          bucketToPartition_[((hashes[i] & kInt32Max) % numBuckets_)];
    }
  }

  return std::nullopt;
}

void HivePartitionFunction::precompute(
    const BaseVector& value,
    size_t channelIndex) {
  if (value.isNullAt(0)) {
    precomputedHashes_[channelIndex] = 0;
    return;
  }

  const SelectivityVector rows(1, true);
  DecodedVector& decodedVector = getDecodedVector();
  decodedVector.decode(value, rows);

  std::vector<uint32_t> hashes{1};
  hash(decodedVector, value.typeKind(), rows, false, hashes, 1);
  precomputedHashes_[channelIndex] = hashes[0];
}

DecodedVector& HivePartitionFunction::getDecodedVector(size_t poolIndex) {
  while (poolIndex >= decodedVectorsPool_.size()) {
    decodedVectorsPool_.push_back(std::make_unique<DecodedVector>());
  }

  return *decodedVectorsPool_[poolIndex];
}

SelectivityVector& HivePartitionFunction::getRows(size_t poolIndex) {
  while (poolIndex >= rowsPool_.size()) {
    rowsPool_.push_back(std::make_unique<SelectivityVector>());
  }

  return *rowsPool_[poolIndex];
}

std::vector<uint32_t>& HivePartitionFunction::getHashes(size_t poolIndex) {
  while (poolIndex >= hashesPool_.size()) {
    hashesPool_.push_back(std::make_unique<std::vector<uint32_t>>());
  }

  return *hashesPool_[poolIndex];
}

} // namespace facebook::velox::connector::hive
