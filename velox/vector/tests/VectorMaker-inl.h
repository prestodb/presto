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

#include <algorithm>

#include "velox/vector/SimpleVector.h"
#include "velox/vector/tests/VectorMakerStats.h"

namespace facebook::velox::test {

template <typename T, typename ShortType>
BufferPtr buildBiasedBuffer(
    const std::vector<std::optional<T>>& values,
    T bias,
    velox::memory::MemoryPool* pool) {
  BufferPtr buffer = AlignedBuffer::allocate<ShortType>(values.size(), pool);
  ShortType* data = buffer->asMutable<ShortType>();
  buffer->setSize(sizeof(ShortType) * values.size());

  for (vector_size_t i = 0; i < values.size(); ++i) {
    if (values[i] != std::nullopt) {
      data[i] = static_cast<ShortType>(*values[i] - bias);
    }
  }
  return buffer;
}

template <typename T>
BiasVectorPtr<VectorMaker::EvalType<T>> VectorMaker::biasVector(
    const std::vector<std::optional<T>>& data) {
  VELOX_CHECK_GT(data.size(), 1);
  using TEvalType = EvalType<T>;

  if constexpr (admitsBias<TEvalType>()) {
    auto stats = genVectorMakerStats(data);
    VELOX_CHECK(stats.min != std::nullopt);
    VELOX_CHECK(stats.max != std::nullopt);
    TEvalType min = *stats.min;
    TEvalType max = *stats.max;

    // This ensures the math conversions when getting a delta on signed
    // values at opposite ends of the int64 range do not overflow a
    // temporary signed value.
    uint64_t delta = max < 0 ? max - min
        : min < 0
        ? static_cast<uint64_t>(max) + static_cast<uint64_t>(std::abs(min))
        : max - min;

    VELOX_CHECK(deltaAllowsBias<TEvalType>(delta));

    // Check BiasVector.h for explanation of this calculation.
    TEvalType bias = min + static_cast<TEvalType>(std::ceil(delta / 2.0));

    BufferPtr buffer;
    TypeKind valueType;

    if (delta <= std::numeric_limits<uint8_t>::max()) {
      buffer = buildBiasedBuffer<TEvalType, int8_t>(data, bias, pool_);
      valueType = TypeKind::TINYINT;
    } else if (delta <= std::numeric_limits<uint16_t>::max()) {
      buffer = buildBiasedBuffer<TEvalType, int16_t>(data, bias, pool_);
      valueType = TypeKind::SMALLINT;
    } else {
      buffer = buildBiasedBuffer<TEvalType, int32_t>(data, bias, pool_);
      valueType = TypeKind::INTEGER;
    }

    auto biasVector = std::make_shared<BiasVector<TEvalType>>(
        pool_,
        nullptr /*nulls*/,
        data.size(),
        valueType,
        buffer,
        bias,
        stats.asSimpleVectorStats(),
        stats.distinctCount(),
        stats.nullCount,
        stats.isSorted);

    for (vector_size_t i = 0; i < data.size(); i++) {
      if (data[i] == std::nullopt) {
        biasVector->setNull(i, true);
      }
    }
    return biasVector;
  } else {
    VELOX_UNSUPPORTED("Invalid type for biasing");
  }
}

template <typename T, typename TEvalType>
void sequenceEncode(
    const std::vector<std::optional<T>>& input,
    std::vector<std::optional<TEvalType>>& encodedVals,
    std::vector<SequenceLength>& encodedLengths) {
  VELOX_CHECK_GT(input.size(), 0, "need at least one element to encode.");
  auto currentValue = input.front();
  SequenceLength currentRun = 1;

  for (vector_size_t i = 1; i < input.size(); i++) {
    if (input[i] == currentValue) {
      ++currentRun;
    } else {
      encodedVals.emplace_back(currentValue);
      encodedLengths.emplace_back(currentRun);
      currentValue = input[i];
      currentRun = 1;
    }
  }
  encodedVals.emplace_back(currentValue);
  encodedLengths.emplace_back(currentRun);
}

template <typename T>
SequenceVectorPtr<VectorMaker::EvalType<T>> VectorMaker::sequenceVector(
    const std::vector<std::optional<T>>& data) {
  using TEvalType = EvalType<T>;

  std::vector<std::optional<TEvalType>> sequenceVals;
  std::vector<SequenceLength> sequenceLengths;
  sequenceEncode(data, sequenceVals, sequenceLengths);

  auto stats = genVectorMakerStats(data);
  return std::make_unique<SequenceVector<TEvalType>>(
      pool_,
      data.size(),
      flatVectorNullable(sequenceVals),
      copyToBuffer(sequenceLengths, pool_),
      stats.asSimpleVectorStats(),
      stats.distinctCount(),
      stats.nullCount,
      stats.isSorted);
}

template <typename T>
ConstantVectorPtr<VectorMaker::EvalType<T>> VectorMaker::constantVector(
    const std::vector<std::optional<T>>& data) {
  VELOX_CHECK_GT(data.size(), 0);
  using TEvalType = EvalType<T>;

  auto stats = genVectorMakerStats(data);
  vector_size_t distinctCount = stats.distinctCount();
  vector_size_t nullCount = stats.nullCount;

  VELOX_CHECK(
      (distinctCount == 1 && nullCount == 0) || distinctCount == 0,
      "Attempting to build a constant vector with invalid entries");

  return std::make_unique<ConstantVector<TEvalType>>(
      pool_,
      data.size(),
      nullCount > 0,
      (nullCount > 0) ? TEvalType() : folly::copy(*data.front()),
      stats.asSimpleVectorStats());
}

template <typename T>
DictionaryVectorPtr<VectorMaker::EvalType<T>> VectorMaker::dictionaryVector(
    const std::vector<std::optional<T>>& data) {
  using TEvalType = EvalType<T>;

  // Encodes the data saving distinct values on `distinctValues` and their
  // respective indices on `indices`.
  std::vector<TEvalType> distinctValues;
  std::unordered_map<TEvalType, int32_t> indexMap;

  BufferPtr indices = AlignedBuffer::allocate<int32_t>(data.size(), pool_);
  auto rawIndices = indices->asMutable<int32_t>();

  BufferPtr nulls =
      AlignedBuffer::allocate<bool>(data.size(), pool_, bits::kNotNull);
  auto rawNulls = nulls->asMutable<uint64_t>();

  vector_size_t nullCount = 0;

  for (auto i = 0; i < data.size(); ++i) {
    auto val = data[i];
    if (val == std::nullopt) {
      ++nullCount;
      bits::setNull(rawNulls, i, true);
    } else {
      const auto& [it, inserted] = indexMap.emplace(*val, indexMap.size());
      if (inserted) {
        distinctValues.push_back(*val);
      }
      *rawIndices = it->second;
    }
    ++rawIndices;
  }

  auto values = flatVector(distinctValues);
  auto stats = genVectorMakerStats(data);
  auto dictionaryVector = std::make_unique<DictionaryVector<TEvalType>>(
      pool_,
      nullCount ? nulls : nullptr,
      data.size(),
      std::move(values),
      std::move(indices),
      stats.asSimpleVectorStats(),
      indexMap.size(),
      nullCount,
      stats.isSorted);

  return dictionaryVector;
}

template <typename T>
FlatVectorPtr<VectorMaker::EvalType<T>> VectorMaker::flatVectorNullable(
    const std::vector<std::optional<T>>& data,
    const TypePtr& type) {
  using TEvalType = EvalType<T>;
  BufferPtr dataBuffer = AlignedBuffer::allocate<TEvalType>(data.size(), pool_);

  auto stats = genVectorMakerStats(data);
  auto flatVector = std::make_shared<FlatVector<TEvalType>>(
      pool_,
      type,
      BufferPtr(nullptr),
      data.size(),
      std::move(dataBuffer),
      std::vector<BufferPtr>(),
      stats.asSimpleVectorStats(),
      stats.distinctCount(),
      stats.nullCount,
      stats.isSorted);

  for (vector_size_t i = 0; i < data.size(); i++) {
    if (data[i] != std::nullopt) {
      flatVector->set(i, TEvalType(*data[i]));
      flatVector->setNull(i, false);
    } else {
      flatVector->set(i, TEvalType());
      flatVector->setNull(i, true);
    }
  }
  return flatVector;
}

template <typename T>
FlatVectorPtr<VectorMaker::EvalType<T>> VectorMaker::flatVector(
    const std::vector<T>& data) {
  using TEvalType = EvalType<T>;
  BufferPtr dataBuffer = AlignedBuffer::allocate<TEvalType>(data.size(), pool_);

  auto stats = genVectorMakerStats(data);
  auto flatVector = std::make_shared<FlatVector<TEvalType>>(
      pool_,
      CppToType<T>::create(),
      BufferPtr(nullptr),
      data.size(),
      std::move(dataBuffer),
      std::vector<BufferPtr>(),
      stats.asSimpleVectorStats(),
      stats.distinctCount(),
      stats.nullCount,
      stats.isSorted);

  for (vector_size_t i = 0; i < data.size(); i++) {
    flatVector->set(i, TEvalType(data[i]));
  }
  return flatVector;
}

} // namespace facebook::velox::test
