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

#include "velox/vector/tests/VectorTestUtils.h"

namespace facebook::velox::test {
namespace {

void checkBaseVectorFlagsSet(const BaseVector& vector) {
  EXPECT_TRUE(vector.getNullCount().has_value());
  EXPECT_TRUE(vector.getDistinctValueCount().has_value());
  EXPECT_TRUE(vector.representedBytes().has_value());
  EXPECT_TRUE(vector.storageBytes().has_value());
}

void checkBaseVectorFlagsCleared(const BaseVector& vector) {
  EXPECT_FALSE(vector.getNullCount().has_value());
  EXPECT_FALSE(vector.getDistinctValueCount().has_value());
  EXPECT_FALSE(vector.representedBytes().has_value());
  EXPECT_FALSE(vector.storageBytes().has_value());
}

// Forward declaration.
void checkVectorFlagsCleared(
    const BaseVector& vector,
    const SelectivityVector& asciiClearedRows,
    const SelectivityVector& asciiRemainRows);

void checkVectorFlagsSet(
    const BaseVector& vector,
    const SelectivityVector& asciiSetRows);

template <TypeKind kind>
void checkVectorFlagsClearedTyped(
    const BaseVector& vector,
    const SelectivityVector& asciiClearedRows,
    const SelectivityVector& asciiRemainRows) {
  using T = typename TypeTraits<kind>::NativeType;
  auto* simpleVector = vector.as<SimpleVector<T>>();

  checkBaseVectorFlagsCleared(vector);
  EXPECT_FALSE(simpleVector->getStats().min.has_value());
  EXPECT_FALSE(simpleVector->getStats().max.has_value());
  EXPECT_FALSE(simpleVector->isSorted().has_value());
  if constexpr (std::is_same_v<T, StringView>) {
    asciiClearedRows.applyToSelected([&](auto row) {
      EXPECT_FALSE(simpleVector->isAscii(row).has_value());
    });
    asciiRemainRows.applyToSelected([&](auto row) {
      EXPECT_TRUE(
          simpleVector->isNullAt(row) ||
          simpleVector->isAscii(row).has_value());
    });
  }
}

template <>
void checkVectorFlagsClearedTyped<TypeKind::MAP>(
    const BaseVector& vector,
    const SelectivityVector& /*asciiClearedRows*/,
    const SelectivityVector& /*asciiRemainRows*/) {
  auto* mapVector = vector.as<MapVector>();
  checkBaseVectorFlagsCleared(vector);
  EXPECT_FALSE(mapVector->hasSortedKeys());

  // MapVector is not expected to clear asciiness of children vector at existing
  // rows.
  checkVectorFlagsCleared(*mapVector->mapKeys(), {}, {});
  checkVectorFlagsCleared(*mapVector->mapValues(), {}, {});
}

template <>
void checkVectorFlagsClearedTyped<TypeKind::ARRAY>(
    const BaseVector& vector,
    const SelectivityVector& /*asciiClearedRows*/,
    const SelectivityVector& /*asciiRemainRows*/) {
  checkBaseVectorFlagsCleared(vector);

  // ArrayVector is not expected to clear asciiness of children vector at
  // existing rows.
  checkVectorFlagsCleared(*vector.as<ArrayVector>()->elements(), {}, {});
}

template <>
void checkVectorFlagsClearedTyped<TypeKind::ROW>(
    const BaseVector& vector,
    const SelectivityVector& asciiClearedRows,
    const SelectivityVector& asciiRemainRows) {
  checkBaseVectorFlagsCleared(vector);

  auto* rowVector = vector.as<RowVector>();
  for (const auto& child : rowVector->children()) {
    checkVectorFlagsCleared(*child, asciiClearedRows, asciiRemainRows);
  }
}

// Check that data-dependent flags have been reset. If the asciiness flag
// applies, check that the asciiness information is cleared at asciiClearedRows
// and remains at asciiRemainRows.
void checkVectorFlagsCleared(
    const BaseVector& vector,
    const SelectivityVector& asciiClearedRows,
    const SelectivityVector& asciiRemainRows) {
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      checkVectorFlagsClearedTyped,
      vector.typeKind(),
      vector,
      asciiClearedRows,
      asciiRemainRows);
}

template <TypeKind kind>
void checkVectorFlagsSetTyped(
    const BaseVector& vector,
    const SelectivityVector& asciiSetRows) {
  using T = typename TypeTraits<kind>::NativeType;
  auto* simpleVector = vector.as<SimpleVector<T>>();

  checkBaseVectorFlagsSet(vector);
  EXPECT_TRUE(simpleVector->getStats().min.has_value());
  EXPECT_TRUE(simpleVector->getStats().max.has_value());
  EXPECT_TRUE(simpleVector->isSorted().has_value());
  if constexpr (std::is_same_v<T, StringView>) {
    asciiSetRows.applyToSelected([&](auto row) {
      EXPECT_TRUE(
          simpleVector->isNullAt(row) ||
          simpleVector->isAscii(row).has_value());
    });
  }
}

template <>
void checkVectorFlagsSetTyped<TypeKind::MAP>(
    const BaseVector& vector,
    const SelectivityVector& asciiSetRows) {
  EXPECT_TRUE(vector.getNullCount().has_value());
  auto* mapVector = vector.as<MapVector>();
  EXPECT_TRUE(mapVector->hasSortedKeys());

  checkVectorFlagsSet(*mapVector->mapKeys(), asciiSetRows);
  checkVectorFlagsSet(*mapVector->mapValues(), asciiSetRows);
}

template <>
void checkVectorFlagsSetTyped<TypeKind::ARRAY>(
    const BaseVector& vector,
    const SelectivityVector& asciiSetRows) {
  EXPECT_TRUE(vector.getNullCount().has_value());

  checkVectorFlagsSet(*vector.as<ArrayVector>()->elements(), asciiSetRows);
}

template <>
void checkVectorFlagsSetTyped<TypeKind::ROW>(
    const BaseVector& vector,
    const SelectivityVector& asciiSetRows) {
  EXPECT_TRUE(vector.getNullCount().has_value());

  auto* rowVector = vector.as<RowVector>();
  for (const auto& child : rowVector->children()) {
    checkVectorFlagsSet(*child, asciiSetRows);
  }
}

// Check that data-dependent flags have been set and the asciiness flag has been
// set for asciiSetRows.
void checkVectorFlagsSet(
    const BaseVector& vector,
    const SelectivityVector& asciiSetRows) {
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      checkVectorFlagsSetTyped, vector.typeKind(), vector, asciiSetRows);
}

// Return an all-selected selectivity vector of the same size as a scalar or row
// vector, or of the same size as the element vectors of an array or map vector.
SelectivityVector createAllSelectedAsciiRows(const VectorPtr& vector) {
  auto size = vector->size();
  if (vector->type()->isArray()) {
    size = vector->as<ArrayVector>()->elements()->size();
  } else if (vector->type()->isMap()) {
    size = vector->as<MapVector>()->mapKeys()->size();
  }
  return SelectivityVector{size};
}

// Return a new selectivity vector of rows in oldRows but not in mutatedRows.
SelectivityVector getUpdatedAsciiRows(
    const SelectivityVector& oldRows,
    const SelectivityVector& mutatedRows) {
  auto newRows = oldRows;
  newRows.deselect(mutatedRows);
  return newRows;
}

// Return true if vector is not dictionary- or constant-encoded.
bool isFlat(const VectorPtr& vector) {
  return vector->encoding() != VectorEncoding::Simple::DICTIONARY &&
      vector->encoding() != VectorEncoding::Simple::CONSTANT;
}

} // namespace

BufferPtr makeNulls(
    vector_size_t size,
    memory::MemoryPool* pool,
    std::function<bool(vector_size_t /*row*/)> isNullAt) {
  auto nulls = allocateNulls(size, pool);
  auto* rawNulls = nulls->asMutable<uint64_t>();
  for (vector_size_t i = 0; i < size; i++) {
    if (isNullAt(i)) {
      bits::setNull(rawNulls, i, true);
    }
  }
  return nulls;
}

void checkVectorFlagsReset(
    const std::function<VectorPtr()>& createVector,
    const std::function<void(VectorPtr&)>& makeMutable,
    const SelectivityVector& mutatedRows) {
  // Vector is multiply-referenced.
  auto vector = createVector();
  auto asciiRows = createAllSelectedAsciiRows(vector);
  // Mutation of dictionary or constant vectors is expected to create a new
  // vector that doesn't keep the asciiness information.
  auto updatedAsciiRows = isFlat(vector)
      ? getUpdatedAsciiRows(asciiRows, mutatedRows)
      : SelectivityVector{};
  checkVectorFlagsSet(*vector, asciiRows);
  auto another = vector;
  makeMutable(vector);
  // When vector is multiply-referenced, mutation will create a new vector and
  // hence not expected to keep the asciiness information.
  checkVectorFlagsCleared(*vector, mutatedRows, {});

  // nulls buffer is multiply-referenced.
  vector = createVector();
  checkVectorFlagsSet(*vector, asciiRows);
  auto nulls = vector->nulls();
  makeMutable(vector);
  checkVectorFlagsCleared(*vector, mutatedRows, updatedAsciiRows);

  // values buffer is multiply-referenced.
  vector = createVector();
  if (vector->isFlatEncoding()) {
    checkVectorFlagsSet(*vector, asciiRows);
    auto values = vector->values();
    makeMutable(vector);
    checkVectorFlagsCleared(*vector, mutatedRows, updatedAsciiRows);
  }

  vector = createVector();
  checkVectorFlagsSet(*vector, asciiRows);
  makeMutable(vector);
  checkVectorFlagsCleared(*vector, mutatedRows, updatedAsciiRows);
}

} // namespace facebook::velox::test
