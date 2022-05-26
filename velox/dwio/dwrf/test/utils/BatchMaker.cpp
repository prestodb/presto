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

#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include <folly/Random.h>
#include <algorithm>
#include <random>
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::test {

using folly::Random;
using memory::MemoryPool;

bool isNotNull(
    std::mt19937& gen,
    vector_size_t i,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  // Only use the index if isNullAt is not nullptr
  if (isNullAt) {
    return !isNullAt(i);
  }
  return Random::rand32(0, 10, gen) > 0;
}

template <typename T>
VectorPtr createScalar(
    size_t size,
    std::mt19937& gen,
    std::function<T()> val,
    MemoryPool& pool,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  BufferPtr values = AlignedBuffer::allocate<T>(size, &pool);
  auto valuesPtr = values->asMutableRange<T>();

  BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
  auto* nullsPtr = nulls->asMutable<uint64_t>();

  size_t nullCount = 0;
  for (size_t i = 0; i < size; ++i) {
    auto notNull = isNotNull(gen, i, isNullAt);
    bits::setNull(nullsPtr, i, !notNull);
    if (notNull) {
      valuesPtr[i] = val();
    } else {
      nullCount++;
    }
  }

  return std::make_shared<FlatVector<T>>(
      &pool, nulls, size, values, std::vector<BufferPtr>{});
}

template <TypeKind KIND>
VectorPtr BatchMaker::createVector(
    const std::shared_ptr<const Type>& /* unused */,
    size_t /* unused */,
    memory::MemoryPool& /* unused */,
    std::mt19937& /* unused */,
    std::function<bool(vector_size_t /*index*/)> /* unused */) {
  VELOX_NYI();
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::BOOLEAN>(
    const std::shared_ptr<const Type>& /* unused */,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createScalar<bool>(
      size,
      gen,
      [&gen]() { return Random::rand32(0, 2, gen) == 0; },
      pool,
      isNullAt);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::TINYINT>(
    const std::shared_ptr<const Type>& /* unused */,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createScalar<int8_t>(
      size,
      gen,
      [&gen]() { return static_cast<int8_t>(Random::rand32(gen)); },
      pool,
      isNullAt);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::SMALLINT>(
    const std::shared_ptr<const Type>& /* unused */,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createScalar<int16_t>(
      size,
      gen,
      [&gen]() { return static_cast<int16_t>(Random::rand32(gen)); },
      pool,
      isNullAt);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::INTEGER>(
    const std::shared_ptr<const Type>& /* unused */,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createScalar<int32_t>(
      size,
      gen,
      [&gen]() { return static_cast<int32_t>(Random::rand32(gen)); },
      pool,
      isNullAt);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::BIGINT>(
    const std::shared_ptr<const Type>& /* unused */,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createScalar<int64_t>(
      size, gen, [&gen]() { return Random::rand64(gen); }, pool, isNullAt);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::REAL>(
    const std::shared_ptr<const Type>& /* unused */,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createScalar<float>(
      size,
      gen,
      [&gen]() { return static_cast<float>(Random::randDouble01(gen)); },
      pool,
      isNullAt);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::DOUBLE>(
    const std::shared_ptr<const Type>& /* unused */,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createScalar<double>(
      size,
      gen,
      [&gen]() { return Random::randDouble01(gen); },
      pool,
      isNullAt);
}

VectorPtr createBinary(
    const TypePtr& type,
    size_t size,
    std::mt19937& gen,
    MemoryPool& pool,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  auto vector = BaseVector::create(type, size, &pool);
  auto flatVector = vector->asFlatVector<StringView>();

  size_t childSize = 0;
  std::vector<int64_t> lengths(size);
  size_t nullCount = 0;
  for (size_t i = 0; i < size; ++i) {
    auto notNull = isNotNull(gen, i, isNullAt);
    vector->setNull(i, !notNull);
    if (notNull) {
      // Make sure not all strings will be inlined
      auto len = Random::rand32(0, 10, gen) + 1;
      lengths[i] = len;
      childSize += len;
    } else {
      lengths[i] = 0;
      ++nullCount;
    }
  }
  vector->setNullCount(nullCount);

  BufferPtr buf = AlignedBuffer::allocate<char>(childSize, &pool);
  auto* bufPtr = buf->asMutable<char>();
  for (size_t i = 0; i < childSize; ++i) {
    bufPtr[i] = 'a' + Random::rand32(0, 26, gen);
  }

  size_t offset = 0;
  for (size_t i = 0; i < size; ++i) {
    if (!vector->isNullAt(i)) {
      flatVector->set(i, StringView(bufPtr + offset, lengths[i]));
      offset += lengths[i];
    }
  }

  return vector;
};

template <>
VectorPtr BatchMaker::createVector<TypeKind::VARCHAR>(
    const std::shared_ptr<const Type>& /* unused */,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createBinary(VARCHAR(), size, gen, pool, isNullAt);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::VARBINARY>(
    const std::shared_ptr<const Type>& /* unused */,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createBinary(VARBINARY(), size, gen, pool, isNullAt);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::TIMESTAMP>(
    const std::shared_ptr<const Type>& /* unused */,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  constexpr int64_t TIME_OFFSET = 1420099200;
  return createScalar<Timestamp>(
      size,
      gen,
      [&gen]() {
        return Timestamp(
            TIME_OFFSET + Random::rand32(0, 60 * 60 * 24 * 365, gen),
            Random::rand32(0, 1'000'000, gen));
      },
      pool,
      isNullAt);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::DATE>(
    const std::shared_ptr<const Type>& /* unused */,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createScalar<Date>(
      size,
      gen,
      [&gen]() { return Date(Random::rand32(gen)); },
      pool,
      isNullAt);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::ROW>(
    const std::shared_ptr<const Type>& type,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt);

template <>
VectorPtr BatchMaker::createVector<TypeKind::ARRAY>(
    const std::shared_ptr<const Type>& type,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt);

template <>
VectorPtr BatchMaker::createVector<TypeKind::MAP>(
    const std::shared_ptr<const Type>& type,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt);

VectorPtr createRow(
    const std::shared_ptr<const Type>& type,
    size_t size,
    bool allowNulls,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  BufferPtr nulls;
  size_t nullCount = 0;

  if (allowNulls) {
    nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    for (size_t i = 0; i < size; ++i) {
      auto notNull = isNotNull(gen, i, isNullAt);
      bits::setNull(nullsPtr, i, !notNull);
      if (!notNull) {
        nullCount++;
      }
    }
  }

  auto& row = type->asRow();
  std::vector<VectorPtr> children(row.size());
  for (size_t i = 0; i < row.size(); ++i) {
    auto child = row.childAt(i);
    children[i] = VELOX_DYNAMIC_TYPE_DISPATCH(
        BatchMaker::createVector,
        child->kind(),
        child,
        size,
        pool,
        gen,
        isNullAt);
  }

  return std::make_shared<RowVector>(
      &pool, type, nulls, size, children, nullCount);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::ROW>(
    const std::shared_ptr<const Type>& type,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createRow(type, size, /* allowNulls */ true, pool, gen, isNullAt);
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::ARRAY>(
    const std::shared_ptr<const Type>& type,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  BufferPtr offsets = AlignedBuffer::allocate<int32_t>(size, &pool);
  auto* offsetsPtr = offsets->asMutable<int32_t>();

  BufferPtr lengths = AlignedBuffer::allocate<vector_size_t>(size, &pool);
  auto* lengthsPtr = lengths->asMutable<vector_size_t>();

  BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
  auto* nullsPtr = nulls->asMutable<uint64_t>();

  size_t nullCount = 0;
  size_t childSize = 0;
  for (size_t i = 0; i < size; ++i) {
    auto notNull = isNotNull(gen, i, isNullAt);
    bits::setNull(nullsPtr, i, !notNull);
    if (notNull) {
      auto len = Random::rand32(0, 10, gen) + 1;
      offsetsPtr[i] = childSize;
      lengthsPtr[i] = len;
      childSize += len;
    } else {
      offsetsPtr[i] = 0;
      lengthsPtr[i] = 0;
      nullCount++;
    }
  }

  auto keyType = type->asArray().childAt(0);
  auto elements = VELOX_DYNAMIC_TYPE_DISPATCH(
      BatchMaker::createVector,
      keyType->kind(),
      keyType,
      childSize,
      pool,
      gen,
      isNullAt);

  return std::make_shared<ArrayVector>(
      &pool, type, nulls, size, offsets, lengths, elements, nullCount);
}

template <typename T>
VectorPtr createScalarMapKeys(
    const vector_size_t* lengths,
    size_t totalMaps,
    size_t totalKeys,
    std::function<T(int32_t, int32_t)> val,
    MemoryPool& pool,
    std::mt19937& gen) {
  BufferPtr values = AlignedBuffer::allocate<T>(totalKeys, &pool);
  auto valuesPtr = values->asMutableRange<T>();

  size_t index = 0;
  for (size_t i = 0; i < totalMaps; i++) {
    auto seed = Random::rand32(0, 100, gen);
    for (size_t j = 0; j < lengths[i]; j++) {
      valuesPtr[index] = val(seed, index);
      index++;
    }
  }

  return std::make_shared<FlatVector<T>>(
      &pool, BufferPtr(nullptr), totalKeys, values, std::vector<BufferPtr>{});
}

VectorPtr createBinaryMapKeys(
    const TypePtr& type,
    const vector_size_t* lengths,
    size_t totalMaps,
    size_t totalKeys,
    MemoryPool& pool,
    std::mt19937& gen) {
  BufferPtr values = AlignedBuffer::allocate<StringView>(totalKeys, &pool);
  auto* valuesPtr = values->asMutable<StringView>();

  std::vector<BufferPtr> buffers;
  size_t keyIndex = 0;

  for (size_t i = 0; i < totalMaps; i++) {
    size_t keyCount = lengths[i];

    size_t bufSize = 0;
    std::vector<int64_t> stringLengths(keyCount);
    for (size_t j = 0; j < keyCount; ++j) {
      auto len = Random::rand32(0, 10, gen) + 1;
      stringLengths[j] = len;
      bufSize += len;
    }

    BufferPtr buf = AlignedBuffer::allocate<char>(bufSize, &pool);
    buffers.push_back(buf);
    auto* bufPtr = buf->asMutable<char>();
    auto fillBuffer = [&bufPtr, &gen](size_t start, size_t end) {
      for (size_t k = start; k < end; ++k) {
        bufPtr[k] = 'a' + Random::rand32(0, 26, gen);
      }
    };

    fillBuffer(0, bufSize);

    std::unordered_set<std::string> existing(keyCount);

    size_t offset = 0;
    for (size_t j = 0; j < keyCount; ++j) {
      auto len = stringLengths[j];
      auto str = std::string(bufPtr + offset, len);
      while (existing.find(str) != existing.end()) {
        fillBuffer(offset, offset + len);
        str = std::string(bufPtr + offset, len);
      }
      existing.insert(str);

      valuesPtr[keyIndex] = StringView(bufPtr + offset, len);
      keyIndex++;
      offset += len;
    }
  }

  return std::make_shared<FlatVector<StringView>>(
      &pool, type, BufferPtr(nullptr), totalKeys, values, std::move(buffers));
}

VectorPtr createMapKeys(
    const TypePtr& keyType,
    const BufferPtr& lengths,
    size_t totalMaps,
    size_t totalKeys,
    MemoryPool& pool,
    std::mt19937& gen) {
  switch (keyType->kind()) {
    case TypeKind::TINYINT: {
      return createScalarMapKeys<int8_t>(
          lengths->as<vector_size_t>(),
          totalMaps,
          totalKeys,
          [](int32_t seed, int32_t index) { return (seed + index) % 60; },
          pool,
          gen);
    }
    case TypeKind::SMALLINT: {
      return createScalarMapKeys<int16_t>(
          lengths->as<vector_size_t>(),
          totalMaps,
          totalKeys,
          [](int32_t seed, int32_t index) { return seed + index; },
          pool,
          gen);
    }
    case TypeKind::INTEGER: {
      return createScalarMapKeys<int32_t>(
          lengths->as<vector_size_t>(),
          totalMaps,
          totalKeys,
          [](int32_t seed, int32_t index) { return seed + index; },
          pool,
          gen);
    }
    case TypeKind::BIGINT: {
      return createScalarMapKeys<int64_t>(
          lengths->as<vector_size_t>(),
          totalMaps,
          totalKeys,
          [](int32_t seed, int32_t index) { return seed + index; },
          pool,
          gen);
    }
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR: {
      return createBinaryMapKeys(
          keyType,
          lengths->as<vector_size_t>(),
          totalMaps,
          totalKeys,
          pool,
          gen);
    }
    default:
      VELOX_CHECK(false, "Not supported key type: {}", keyType->kind());
      return nullptr;
  }
}

template <>
VectorPtr BatchMaker::createVector<TypeKind::MAP>(
    const std::shared_ptr<const Type>& type,
    size_t size,
    MemoryPool& pool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  BufferPtr offsets = AlignedBuffer::allocate<vector_size_t>(size, &pool);
  auto* offsetsPtr = offsets->asMutable<vector_size_t>();

  BufferPtr lengths = AlignedBuffer::allocate<vector_size_t>(size, &pool);
  auto* lengthsPtr = lengths->asMutable<vector_size_t>();

  BufferPtr nulls = AlignedBuffer::allocate<char>(bits::nbytes(size), &pool);
  auto* nullsPtr = nulls->asMutable<uint64_t>();

  size_t nullCount = 0;
  size_t childSize = 0;
  for (size_t i = 0; i < size; ++i) {
    auto notNull = isNotNull(gen, i, isNullAt);
    bits::setNull(nullsPtr, i, !notNull);
    if (notNull) {
      auto len = Random::rand32(0, 5, gen) + 1;
      offsetsPtr[i] = childSize;
      lengthsPtr[i] = len;
      childSize += len;
    } else {
      offsetsPtr[i] = 0;
      lengthsPtr[i] = 0;
      nullCount++;
    }
  }

  VectorPtr keys =
      createMapKeys(type->childAt(0), lengths, size, childSize, pool, gen);

  auto valType = type->childAt(1);
  auto values = VELOX_DYNAMIC_TYPE_DISPATCH(
      BatchMaker::createVector,
      valType->kind(),
      valType,
      childSize,
      pool,
      gen,
      isNullAt);

  return std::make_shared<MapVector>(
      &pool, type, nulls, size, offsets, lengths, keys, values, nullCount);
}

VectorPtr BatchMaker::createBatch(
    const std::shared_ptr<const Type>& type,
    uint64_t capacity,
    MemoryPool& memoryPool,
    std::mt19937& gen,
    std::function<bool(vector_size_t /*index*/)> isNullAt) {
  return createRow(
      type, capacity, /* allowNulls */ false, memoryPool, gen, isNullAt);
}

VectorPtr BatchMaker::createBatch(
    const std::shared_ptr<const Type>& type,
    uint64_t capacity,
    MemoryPool& memoryPool,
    std::function<bool(vector_size_t /*index*/)> isNullAt,
    std::mt19937::result_type seed) {
  std::mt19937 gen(seed);
  return createBatch(type, capacity, memoryPool, gen, isNullAt);
}

} // namespace facebook::velox::test
