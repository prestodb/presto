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
#pragma once

#include "velox/exec/ContainerRowSerde.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/MapAccumulator.h"

namespace facebook::velox::aggregate::prestosql {

template <typename K, typename AccumulatorType>
class MapAggregateBase : public exec::Aggregate {
 public:
  explicit MapAggregateBase(TypePtr resultType) : Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(AccumulatorType);
  }

  bool isFixedSize() const override {
    return false;
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto mapVector = (*result)->as<MapVector>();
    VELOX_CHECK(mapVector);
    mapVector->resize(numGroups);
    auto mapKeys = mapVector->mapKeys();
    auto mapValues = mapVector->mapValues();
    auto numElements = countElements(groups, numGroups);
    mapKeys->resize(numElements);
    mapValues->resize(numElements);

    auto* rawNulls = getRawNulls(mapVector);
    vector_size_t offset = 0;

    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];

      if (isNull(group)) {
        mapVector->setNull(i, true);
        mapVector->setOffsetAndSize(i, 0, 0);
        continue;
      }

      clearNull(rawNulls, i);

      auto accumulator = value<AccumulatorType>(group);
      auto mapSize = accumulator->size();
      if (mapSize) {
        accumulator->extract(mapKeys, mapValues, offset);
        mapVector->setOffsetAndSize(i, offset, mapSize);
        offset += mapSize;
      } else {
        mapVector->setOffsetAndSize(i, 0, 0);
      }
    }
  }

  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addMapInputToAccumulator(groups, rows, args, false);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    addSingleGroupMapInputToAccumulator(group, rows, args, false);
  }

 protected:
  vector_size_t countElements(char** groups, int32_t numGroups) const {
    vector_size_t size = 0;
    for (int32_t i = 0; i < numGroups; ++i) {
      size += value<AccumulatorType>(groups[i])->size();
    }
    return size;
  }

  void destroyInternal(folly::Range<char**> groups) override {
    for (auto group : groups) {
      if (isInitialized(group)) {
        auto accumulator = value<AccumulatorType>(group);
        accumulator->free(*allocator_);
      }
    }
  }

  AccumulatorType* accumulator(char* group) {
    return value<AccumulatorType>(group);
  }

  static void checkNullKeys(
      const DecodedVector& keys,
      vector_size_t offset,
      vector_size_t size) {
    static const char* kNullKey = "map key cannot be null";
    if (keys.mayHaveNulls()) {
      for (auto i = offset; i < offset + size; ++i) {
        VELOX_USER_CHECK(!keys.isNullAt(i), kNullKey);
      }
    }
  }

  void addMapInputToAccumulator(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) {
    decodedMaps_.decode(*args[0], rows);
    auto mapVector = decodedMaps_.base()->template as<MapVector>();

    decodedKeys_.decode(*mapVector->mapKeys());
    decodedValues_.decode(*mapVector->mapValues());

    VELOX_CHECK_NOT_NULL(mapVector);
    rows.applyToSelected([&](vector_size_t row) {
      auto group = groups[row];
      auto accumulator = value<AccumulatorType>(group);

      if (!decodedMaps_.isNullAt(row)) {
        clearNull(group);
        auto decodedRow = decodedMaps_.index(row);
        auto offset = mapVector->offsetAt(decodedRow);
        auto size = mapVector->sizeAt(decodedRow);
        auto tracker = trackRowSize(group);
        checkNullKeys(decodedKeys_, offset, size);
        for (auto i = offset; i < offset + size; ++i) {
          accumulator->insert(decodedKeys_, decodedValues_, i, *allocator_);
        }
      }
    });
  }

  void addSingleGroupMapInputToAccumulator(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) {
    decodedMaps_.decode(*args[0], rows);
    auto mapVector = decodedMaps_.base()->template as<MapVector>();

    decodedKeys_.decode(*mapVector->mapKeys());
    decodedValues_.decode(*mapVector->mapValues());

    auto accumulator = value<AccumulatorType>(group);

    VELOX_CHECK_NOT_NULL(mapVector);
    rows.applyToSelected([&](vector_size_t row) {
      if (!decodedMaps_.isNullAt(row)) {
        clearNull(group);
        auto decodedRow = decodedMaps_.index(row);
        auto offset = mapVector->offsetAt(decodedRow);
        auto size = mapVector->sizeAt(decodedRow);
        checkNullKeys(decodedKeys_, offset, size);
        for (auto i = offset; i < offset + size; ++i) {
          accumulator->insert(decodedKeys_, decodedValues_, i, *allocator_);
        }
      }
    });
  }

  void initializeNewGroupsInternal(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    const auto& type = resultType()->childAt(0);
    for (auto index : indices) {
      new (groups[index] + offset_) AccumulatorType(type, allocator_);
    }
    setAllNulls(groups, indices);
  }

  DecodedVector decodedKeys_;
  DecodedVector decodedValues_;
  DecodedVector decodedMaps_;
};

template <template <typename K, typename Accumulator = MapAccumulator<K>>
          class TAggregate>
std::unique_ptr<exec::Aggregate> createMapAggregate(const TypePtr& resultType) {
  auto typeKind = resultType->childAt(0)->kind();
  switch (typeKind) {
    case TypeKind::BOOLEAN:
      return std::make_unique<TAggregate<bool>>(resultType);
    case TypeKind::TINYINT:
      return std::make_unique<TAggregate<int8_t>>(resultType);
    case TypeKind::SMALLINT:
      return std::make_unique<TAggregate<int16_t>>(resultType);
    case TypeKind::INTEGER:
      return std::make_unique<TAggregate<int32_t>>(resultType);
    case TypeKind::BIGINT:
      return std::make_unique<TAggregate<int64_t>>(resultType);
    case TypeKind::REAL:
      return std::make_unique<TAggregate<float>>(resultType);
    case TypeKind::DOUBLE:
      return std::make_unique<TAggregate<double>>(resultType);
    case TypeKind::TIMESTAMP:
      return std::make_unique<TAggregate<Timestamp>>(resultType);
    case TypeKind::VARBINARY:
      [[fallthrough]];
    case TypeKind::VARCHAR:
      return std::make_unique<TAggregate<StringView>>(resultType);
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
      return std::make_unique<TAggregate<ComplexType>>(resultType);
    case TypeKind::UNKNOWN:
      return std::make_unique<TAggregate<int32_t>>(resultType);
    default:
      VELOX_UNREACHABLE("Unexpected type {}", mapTypeKindToName(typeKind));
  }
}

} // namespace facebook::velox::aggregate::prestosql
